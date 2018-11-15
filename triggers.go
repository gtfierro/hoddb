package main

import (
	"encoding/binary"
	"github.com/dgraph-io/badger"
	"github.com/golang/protobuf/proto"
	logpb "github.com/gtfierro/hod/log/proto"
	"github.com/pkg/errors"
	logrus "github.com/sirupsen/logrus"
	"github.com/zhangxinngang/murmur"
	"os"
	"path/filepath"
)

func (L *Log) openTriggerDatabase(cfg *Config) error {
	triggerdir := filepath.Join(cfg.Database.Path, "_triggers_")
	err := os.MkdirAll(triggerdir, 0700)
	if err != nil {
		return err
	}
	opts := badger.DefaultOptions
	opts.Dir = triggerdir
	opts.ValueDir = triggerdir
	L.triggerDB, err = badger.Open(opts)
	logrus.Infof("Opened trigger DB at %s", triggerdir)
	return err
}

func (L *Log) triggersByGraph(graph string) (triggers []*logpb.Trigger, err error) {
	opt := badger.DefaultIteratorOptions
	opt.PrefetchSize = 10

	var key = make([]byte, 4)
	binary.BigEndian.PutUint32(key, murmur.Murmur3([]byte(graph)))

	err = L.triggerDB.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(opt)
		defer it.Close()
		for it.Seek(key); it.ValidForPrefix(key); it.Next() {
			item := it.Item()
			var trigger = new(logpb.Trigger)
			var unmarshalErr error
			err = item.Value(func(b []byte) {
				unmarshalErr = proto.Unmarshal(b, trigger)
			})
			if err != nil {
				return err
			}
			if unmarshalErr != nil {
				return unmarshalErr
			}
			triggers = append(triggers, trigger)
		}
		return nil
	})
	return
}

func (L *Log) triggerByName(graph, name string) (*logpb.Trigger, error) {
	var key = make([]byte, 8)
	binary.BigEndian.PutUint32(key[:4], murmur.Murmur3([]byte(graph)))
	binary.BigEndian.PutUint32(key[4:], murmur.Murmur3([]byte(name)))
	var trigger = new(logpb.Trigger)
	err := L.triggerDB.View(func(txn *badger.Txn) error {
		item, err := txn.Get(key)
		if err != nil {
			return err
		}
		var unmarshalErr error
		err = item.Value(func(b []byte) {
			unmarshalErr = proto.Unmarshal(b, trigger)
		})
		if err != nil {
			return err
		}
		return unmarshalErr
	})
	return trigger, err
}

func (L *Log) saveTrigger(trigger *logpb.Trigger) error {
	// what do the keys look like?
	// [ graph hash <4> | name hash <4> ]
	var key = make([]byte, 8)
	binary.BigEndian.PutUint32(key[:4], murmur.Murmur3([]byte(trigger.Graph)))
	binary.BigEndian.PutUint32(key[4:], murmur.Murmur3([]byte(trigger.Name)))
	serializedTrigger, err := proto.Marshal(trigger)
	if err != nil {
		return err
	}

	txn := L.triggerDB.NewTransaction(true)
	if setErr := txn.Set(key, serializedTrigger); setErr != nil {
		txn.Discard()
		return errors.Wrap(setErr, "could not set trigger")
	}
	if txerr := txn.Commit(nil); txerr != nil {
		txn.Discard()
		return errors.Wrap(txerr, "could not commit trigger")
	}
	return nil
}

func (L *Log) deleteTrigger(trigger *logpb.DeleteTriggerRequest) error {
	var key = make([]byte, 8)
	binary.BigEndian.PutUint32(key[:4], murmur.Murmur3([]byte(trigger.Graph)))
	binary.BigEndian.PutUint32(key[4:], murmur.Murmur3([]byte(trigger.Name)))

	txn := L.triggerDB.NewTransaction(true)
	if setErr := txn.Delete(key); setErr != nil {
		txn.Discard()
		return errors.Wrap(setErr, "could not set trigger")
	}
	if txerr := txn.Commit(nil); txerr != nil {
		txn.Discard()
		return errors.Wrap(txerr, "could not commit trigger")
	}
	return nil
}

func (L *Log) runTrigger(cursor *Cursor, trigger *logpb.Trigger) (chan *logpb.LogEntry, error) {
	var vars []string
	for _, triple := range trigger.Where {
		if isVariable(triple.Subject) {
			vars = append(vars, triple.Subject.Value)
		}
		if isVariable(triple.Predicate[0]) {
			vars = append(vars, triple.Predicate[0].Value)
		}
		if isVariable(triple.Object) {
			vars = append(vars, triple.Object.Value)
		}
	}
	dg := makeDependencyGraph(cursor, vars, trigger.Where)
	qp, err := formQueryPlan(dg, nil)
	if err != nil {
		err = errors.Wrap(err, "Could not form query plan")
		return nil, err
	}
	selectVars := []string{"?s", "?o"}
	qp.variables = selectVars
	cursor.addQueryPlan(qp)
	cursor.selectVars = selectVars

	for _, op := range qp.operations {
		//logrus.Info("op | ", op)
		err := op.run(cursor)
		if err != nil {
			err = errors.Wrapf(err, "Could not run op %s", op)
			return nil, err
		}
	}

	generated := cursor.generateTriples(selectVars, trigger.Insert)
	return generated, err
}

var BUILTIN_TRIGGERS = []struct {
	trigger string
	name    string
}{
	{
		"INSERT { ?o bf:isFedBy ?s } WHERE { ?s bf:feeds ?o };",
		"feeds1",
	},
	{
		"INSERT { ?o bf:feeds ?s } WHERE { ?s bf:isFedBy ?o };",
		"feeds2",
	},

	{
		"INSERT { ?o bf:hasPart ?s } WHERE { ?s bf:isPartOf ?o };",
		"haspart1",
	},
	{
		"INSERT { ?o bf:isPartOf ?s } WHERE { ?s bf:hasPart ?o };",
		"haspart2",
	},

	{
		"INSERT { ?o bf:hasPoint ?s } WHERE { ?s bf:isPointOf ?o };",
		"haspoint1",
	},
	{
		"INSERT { ?o bf:isPointOf ?s } WHERE { ?s bf:hasPoint ?o };",
		"haspoint2",
	},

	{
		"INSERT { ?o bf:isLocationOf ?s } WHERE { ?s bf:contains ?o };",
		"contains2haslocation",
	},
	{
		"INSERT { ?o bf:isLocationOf ?s } WHERE { ?s bf:hasLocation ?o };",
		"haslocation1",
	},
	{
		"INSERT { ?o bf:hasLocation ?s } WHERE { ?s bf:isLocationOf ?o };",
		"haslocation2",
	},

	{
		"INSERT { ?o bf:controls ?s } WHERE { ?s bf:isControlledBy ?o };",
		"controls1",
	},
	{
		"INSERT { ?o bf:isControlledBy ?s } WHERE { ?s bf:controls ?o };",
		"controls2",
	},
}
