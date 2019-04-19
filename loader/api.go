package loader

import (
	"encoding/binary"
	"fmt"

	logpb "git.sr.ht/~gabe/hod/proto"
	"git.sr.ht/~gabe/hod/turtle"
	"github.com/dgraph-io/badger"
	"github.com/golang/protobuf/proto"
	"github.com/pkg/errors"
	"github.com/zhangxinngang/murmur"
)

func (hod *HodDB) Load(bundle FileBundle) error {
	graph, err := hod.LoadFileBundle(bundle)
	if err != nil {
		return errors.Wrapf(err, "could not load file %s for graph %s", bundle.TTLFile, bundle.GraphName)
	}
	graph.ExpandTriples()

	entities := graph.CompileEntities()

	log.Println("entities compiled", len(entities))

	//TODO: do we postpone until we do extended edges?
	txn := hod.db.NewTransaction(true)

	for _, ent := range entities {
		serializedEntry, err := proto.Marshal(ent.compiled)
		if err != nil {
			txn.Discard()
			return errors.Wrap(err, "Error serializing entry")
		}
		if err := hod.setWithCommit(txn, ent.compiled.EntityKey, serializedEntry); err != nil {
			return errors.Wrap(err, "Error txn commit")
		}
	}
	if err := txn.Commit(nil); err != nil {
		txn.Discard()
		return errors.Wrap(err, "last commit")
	}

	getEntity := func(key EntityKey) *Entity {
		ent, found := entities[key]
		if !found {
			ent = newEntity(key)
			entities[key] = ent
		}
		return ent
	}

	// insert extended edges
	cursor := hod.Cursor(graph.Name, nil)
	for _, ent := range entities {
		for _, pred := range ent.GetAllPredicates() {
			e := edge{predicate: pred, pattern: logpb.Pattern_OnePlus}
			newseen, _, err := cursor.followPathFromSubject(ent, e)
			if err != nil {
				return err
			}
			for newkey := range newseen {
				ent.addOutEdge(pred, newkey, logpb.Pattern_OnePlus)
				other := getEntity(newkey)
				other.addInEdge(pred, ent.key, logpb.Pattern_OnePlus)
			}
		}
	}

	txn = hod.db.NewTransaction(true)

	for _, ent := range entities {
		serializedEntry, err := proto.Marshal(ent.compiled)
		if err != nil {
			txn.Discard()
			return errors.Wrap(err, "Error serializing entry")
		}
		if err := hod.setWithCommit(txn, ent.compiled.EntityKey, serializedEntry); err != nil {
			return errors.Wrap(err, "Error txn commit")
		}
	}
	if err := txn.Commit(nil); err != nil {
		txn.Discard()
		return errors.Wrap(err, "last commit")
	}

	return nil
}

func (hod *HodDB) setWithCommit(txn *badger.Txn, key, value []byte) error {
	if setErr := txn.Set(key, value); setErr == badger.ErrTxnTooBig {
		log.Warning("commit too big")
		if txerr := txn.Commit(nil); txerr != nil {
			txn.Discard()
			return errors.Wrap(txerr, "commit log entry")
		}
		txn = hod.db.NewTransaction(true)
		if txerr := txn.Set(key, value); txerr != nil {
			txn.Discard()
			return errors.Wrap(txerr, "set log entry")
		}
	} else if setErr != nil {
		txn.Discard()
		return errors.Wrap(setErr, "set log entry")
	}
	return nil
}

func (hod *HodDB) GetEntity(key EntityKey) (*Entity, error) {
	var entity = &Entity{
		compiled: new(logpb.Entity),
		key:      key,
	}
	err := hod.db.View(func(t *badger.Txn) error {
		it, err := t.Get(key.Bytes())
		if err != nil {
			return err
		}
		err = it.Value(func(b []byte) error {
			return proto.Unmarshal(b, entity.compiled)
		})
		if err != nil {
			return err
		}
		return nil
	})
	return entity, err
}

func (hod *HodDB) hashURI(u turtle.URI) EntityKey {

	if key, found := hod.hashes[u]; found {
		return key
	}

	var key EntityKey

	hashresult := murmur.Murmur3([]byte(u.Namespace + u.Value))
	binary.BigEndian.PutUint32(key.Hash[:], hashresult)

	if other_uri, found := hod.uris[key]; found {
		panic(fmt.Sprintf("URI for %s conflicts with %s", u, other_uri))
	}

	hod.hashes[u] = key
	hod.uris[key] = u
	return key
}
