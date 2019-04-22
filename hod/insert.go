package hod

import (
	"encoding/binary"
	logpb "git.sr.ht/~gabe/hod/proto"
	"github.com/dgraph-io/badger"
	"github.com/golang/protobuf/proto"
	"github.com/pkg/errors"
	logrus "github.com/sirupsen/logrus"
	"time"
)

func (L *Log) CreateCursor(graph string, from, to int64) (*Cursor, error) {
	var cursor *Cursor
	var found bool
	var err error

	latest, err := L.versionDB.latestVersion(graph, to)
	if err != nil {
		return nil, errors.Wrapf(err, "could not get latest version of graph %s", graph)
	}
	logrus.Debug("latest version", latest, "asked for", to)

	L.RLock()
	if _, found = L.cursorCache[graph]; found {
		L.RUnlock()
		return L.Cursor(graph, latest, nil), nil
	}
	L.RUnlock()

	// process the entities with the latest versions of the tags for the log between the two times
	entries := L.readRangeGraph(graph, from, to)
	var entities = make(map[EntityKey]*logpb.Entity)
	var dirty = make(map[EntityKey]bool)

	// get latest version of graph; we copy all keys from previous version
	// into this version
	cursor = L.Cursor(graph, latest, nil)

	if err := L.processLogEntries(cursor, entities, dirty, entries); err != nil {
		return nil, err
	}
	fromT := time.Unix(0, from)
	toT := time.Unix(0, to)
	logrus.Debugf("Processed %d entities for graph %s from %s - %s", len(entities), graph, fromT, toT)

	cursor.dropCache()

	// TODO: run the triggers in a loop until nothing is added/changed
	triggers, err := L.triggersByGraph(graph)
	if err != nil {
		return nil, errors.Wrap(err, "could not load triggers")
	}
	for _, trigger := range triggers {
		generated, err := L.runTrigger(cursor, trigger)
		if err != nil {
			return nil, errors.Wrap(err, "could not run trigger")
		}

		if err := L.processLogEntries(cursor, entities, dirty, generated); err != nil {
			return nil, err
		}
		logrus.Debugf("Loop1: Processed %d entities for graph %s from %s - %s from trigger %s", len(entities), graph, fromT, toT, trigger.Name)
	}

	for _, trigger := range triggers {
		generated, err := L.runTrigger(cursor, trigger)
		if err != nil {
			return nil, errors.Wrap(err, "could not run trigger")
		}

		if err := L.processLogEntries(cursor, entities, dirty, generated); err != nil {
			return nil, errors.Wrap(err, "could not process generated triples")
		}
		logrus.Debugf("Loop2: Processed %d entities for graph %s from %s - %s from trigger %s", len(entities), graph, fromT, toT, trigger.Name)
	}

	cursor.dropCache()

	cursor = L.Cursor(graph, latest, nil)
	L.Lock()
	L.cursorCache[graph] = cursor
	L.Unlock()

	// clear out objects
	for k := range entities {
		delete(entities, k)
	}

	return cursor, nil
}

// when transactions are looking up an Entity object, there are several places it can be. We typically request
// an object by its Key which is <graph hash, entity hash, timestamp>, but if we do not have an entity with that
// graph and hash at the given timestamp, we want to go back in time to see if there is an earlier entity that we
// can use.
// This method searches the following locations in order:
// - the transaction cache -- by exact version/timestamp
// - pull from the database -- by exact version/timestamp
// - pull from the transaction cache -- by highest available timestamp <= the given timestamp
// - pull from the database -- by highest available timstamp <= the given timestamp
func (L *Log) loadEntry(txn *badger.Txn, cache map[EntityKey]*logpb.Entity, key EntityKey) (ent *logpb.Entity, created bool) {
	var found bool

	// case 1: exists in the cache
	ent, found = cache[key]
	if found && ent != nil {
		created = false
		return
	}

	// case 2: exists in current version (key.Version)
	_ent, err := L.GetEntity(key)
	if err != nil && err != badger.ErrKeyNotFound {
		logrus.Fatal(errors.Wrapf(err, "Could not get entity at generation %d", key.Timestamp()))
	}
	if _ent != nil && err == nil {
		ent = _ent.e
		cache[key] = ent
		created = false
		return
	}

	var newk EntityKey
	for newk, ent = range cache {
		if key.HashEquals(newk) {
			// rewrite the old entity's key so it can be
			// part of this version
			ent.EntityKey = key.Bytes()
			cache[key] = ent
			created = true
			return
		}
	}
	// case 3: exists in previous version
	// create new key with new version
	// most recent version?
	_ent, err = L.GetRecentEntity(key)
	if _ent != nil && _ent.e != nil && err == nil {
		ent = _ent.e
		ent.EntityKey = key.Bytes()
		_ent.e.EntityKey = key.Bytes()
		cache[key] = _ent.e
		created = true
		return
	}

	// case 4: does not exist
	ent = new(logpb.Entity)
	ent.EntityKey = make([]byte, 16)
	copy(ent.EntityKey[:], key.Bytes())
	cache[key] = ent
	created = true
	return
}

func (L *Log) processEntry(cursor *Cursor, txn *badger.Txn, cache map[EntityKey]*logpb.Entity, dirty map[EntityKey]bool, e *logpb.LogEntry) {
	var (
		subChanged  bool = false
		predChanged bool = false
		objChanged  bool = false
	)
	if e.Op == logpb.Op_ADD {
		//var subject, predicate, reversePredicate, object EntityKey
		//var subjectEntity, predicateEntity, reversePredicateEntity, objectEntity *logpb.Entity
		var subject, predicate, object EntityKey
		var subjectEntity, predicateEntity, objectEntity *logpb.Entity
		//var found bool

		if e.Triple == nil || e.Triple.Subject == nil || e.Triple.Object == nil || len(e.Triple.Predicate) == 0 {
			return
		}

		// subject
		copy(subject.Hash[:], hashURI(e.Triple.Subject))
		copy(subject.Graph[:], hashString(e.Graph))
		binary.LittleEndian.PutUint64(subject.Version[:], uint64(cursor.timestamp))
		//copy(subjectEntity.EntityKey, subject.Bytes())
		subjectEntity, subChanged = L.loadEntry(txn, cache, subject)
		if subChanged {
			dirty[subject] = true
		}

		// predicate
		copy(predicate.Hash[:], hashURI(e.Triple.Predicate[0]))
		copy(predicate.Graph[:], hashString(e.Graph))
		binary.LittleEndian.PutUint64(predicate.Version[:], uint64(cursor.timestamp))
		//copy(predicateEntity.EntityKey, predicate.Bytes())
		predicateEntity, predChanged = L.loadEntry(txn, cache, predicate)
		if predChanged {
			dirty[predicate] = true
		}

		// object
		copy(object.Hash[:], hashURI(e.Triple.Object))
		copy(object.Graph[:], hashString(e.Graph))
		binary.LittleEndian.PutUint64(object.Version[:], uint64(cursor.timestamp))
		//copy(objectEntity.EntityKey, object.Bytes())
		objectEntity, objChanged = L.loadEntry(txn, cache, object)
		if objChanged {
			dirty[object] = true
		}

		// add edge to subject
		var toEdge = new(logpb.Entity_Edge)
		toEdge.Predicate = predicate.Bytes()
		toEdge.Value = object.Bytes()
		subjectEntity.Out, subChanged = addEdgeIfNotExist(subjectEntity.Out, toEdge)
		if subChanged {
			dirty[subject] = true
		}

		// add edge to object
		var fromEdge = new(logpb.Entity_Edge)
		fromEdge.Predicate = predicate.Bytes()
		fromEdge.Value = subject.Bytes()
		objectEntity.In, objChanged = addEdgeIfNotExist(objectEntity.In, fromEdge)
		if objChanged {
			dirty[object] = true
		}

		// add endpoints to predicate
		var endpoints = new(logpb.Entity_Endpoints)
		endpoints.Src = subject.Bytes()
		endpoints.Dst = object.Bytes()
		predicateEntity.Endpoints = append(predicateEntity.Endpoints, endpoints)
		dirty[predicate] = true

		//if reversePred, found := L.reverseEdges[uriToS(*e.Triple.Predicate[0])]; found {
		//	copy(reversePredicate.Hash[:], hashURI(&reversePred))
		//	copy(reversePredicate.Graph[:], hashString(e.Graph))
		//	binary.LittleEndian.PutUint64(reversePredicate.Version[:], uint64(e.Timestamp))
		//	reversePredicateEntity = L.loadEntry(txn, cache, reversePredicate)

		//	var rendpoints = new(logpb.Entity_Endpoints)
		//	rendpoints.Src = object.Bytes()
		//	rendpoints.Dst = subject.Bytes()
		//	reversePredicateEntity.Endpoints = append(reversePredicateEntity.Endpoints, endpoints)

		//	// add edges to entities
		//	var revToEdge = new(logpb.Entity_Edge)
		//	revToEdge.Predicate = reversePredicate.Bytes()
		//	revToEdge.Value = object.Bytes()
		//	//subjectEntity.In = append(subjectEntity.In, revToEdge)
		//	subjectEntity.In = addEdgeIfNotExist(subjectEntity.In, revToEdge)

		//	var revFromEdge = new(logpb.Entity_Edge)
		//	revFromEdge.Predicate = reversePredicate.Bytes()
		//	revFromEdge.Value = subject.Bytes()
		//	//objectEntity.Out = append(objectEntity.Out, revFromEdge)
		//	objectEntity.Out = addEdgeIfNotExist(objectEntity.Out, revFromEdge)

		//	// write to cache
		//	cache[reversePredicate] = reversePredicateEntity
		//}

		// add to cache
		cache[subject] = subjectEntity
		cache[predicate] = predicateEntity
		cache[object] = objectEntity
	}
}

func (L *Log) processLogEntries(cursor *Cursor, entities map[EntityKey]*logpb.Entity, dirty map[EntityKey]bool, entries chan *logpb.LogEntry) error {
	txn := L.db.NewTransaction(true)

	// populates the entities objects
	for entry := range entries {
		L.processEntry(cursor, txn, entities, dirty, entry)
	}
	// inserts these into the database under the current version
	for k, v := range entities {
		if changed, found := dirty[k]; found && changed {
			delete(dirty, k)
			serializedEntry, err := proto.Marshal(v)
			if err != nil {
				txn.Discard()
				return errors.Wrap(err, "Error serializing entry")
			}
			if err := L.setWithCommit(txn, k.Bytes(), serializedEntry); err != nil {
				return errors.Wrap(err, "Error txn commit")
			}
		}
	}
	if err := txn.Commit(nil); err != nil {
		txn.Discard()
		return errors.Wrap(err, "last commit")
	}

	cursor.dropCache()

	//  insert extended edges
	var changed bool
	for k, v := range entities {
		ent, err := cursor.getEntity(k)
		if err != nil {
			return errors.Wrap(err, "Could not get entity in ext")
		}

		for _, pred := range ent.GetAllPredicates() {
			e := edge{predicate: pred, pattern: logpb.Pattern_OnePlus}
			newseen, _, err := cursor.followPathFromSubject(ent, e)
			if err != nil {
				return err
			}
			for newkey := range newseen {
				var toEdge = new(logpb.Entity_Edge)
				toEdge.Predicate = pred.Bytes()
				toEdge.Value = newkey.Bytes()
				toEdge.Pattern = logpb.Pattern_OnePlus
				found := false
				v.Out, changed = addEdgeIfNotExist(v.Out, toEdge)
				if changed {
					dirty[k] = true
				}
				entities[k] = v
				obj, found := entities[newkey]
				if found {
					var invToEdge = new(logpb.Entity_Edge)
					invToEdge.Predicate = pred.Bytes()
					invToEdge.Value = k.Bytes()
					invToEdge.Pattern = logpb.Pattern_OnePlus
					found = false
					obj.In, changed = addEdgeIfNotExist(obj.In, invToEdge)
					if changed {
						dirty[newkey] = true
					}
					entities[newkey] = obj
				}

			}
		}
	}

	// re-insert these with extended edges
	txn = L.db.NewTransaction(true)
	for k, v := range entities {
		if changed, found := dirty[k]; found && changed {
			delete(dirty, k)
			serializedEntry, err := proto.Marshal(v)
			if err != nil {
				txn.Discard()
				return errors.Wrap(err, "Error serializing entry")
			}
			if err := L.setWithCommit(txn, k.Bytes(), serializedEntry); err != nil {
				return errors.Wrap(err, "Error txn commit")
			}
		}
	}
	if err := txn.Commit(nil); err != nil {
		txn.Discard()
		return errors.Wrap(err, "last commit")
	}
	return nil
}
