package main

import (
	"context"
	"fmt"
	"github.com/dgraph-io/badger"
	"github.com/golang/protobuf/proto"
	logpb "github.com/gtfierro/hod/log/proto"
	"github.com/gtfierro/hod/turtle"
	"github.com/pkg/errors"
	logrus "github.com/sirupsen/logrus"
	"time"
)

// when we get new graphs, make sure we register the triggers
func (L *Log) TagInsert(ctx context.Context, req *logpb.TagInsertRequest) (resp *logpb.TagInsertResponse, err error) {
	L.Lock()
	defer L.Unlock()
	resp = new(logpb.TagInsertResponse)

	// get the sequence
	seq, err := L.log.GetSequence([]byte("log"), 1000)
	if err != nil {
		err = errors.Wrap(err, "get sequence")
		resp.Error = err.Error()
		return
	}

	version := time.Now().UnixNano()
	resp.Timestamp = version
	// update tag entry
	if oldversion, found := L.tagVersions[req.Tag]; found {
		if oldversion > version {
			err = errors.New(fmt.Sprintf("Newer version %d exists for tag %s", oldversion, req.Tag))
			resp.Error = err.Error()
			return
		}
	} else {
		L.tagVersions[req.Tag] = version
	}

	txn := L.log.NewTransaction(true)

	// generate a log entry for each triple in the insert request
	for _, triple := range req.Triples {
		entry := &logpb.LogEntry{
			Op:        req.Operation,
			Graph:     req.Graph,
			Tag:       req.Tag,
			Timestamp: version,
			Triple:    triple,
			Commit:    false,
		}
		newVersion, newGraph, addErr := L.versionDB.addEntry(entry)
		if addErr != nil {
			txn.Discard()
			err = errors.Wrap(addErr, "add entry to version db")
			resp.Error = err.Error()
			return
		}
		// when we get new graphs, make sure we register the triggers
		// TODO: when we trigger this ONLY when newgraph is true, not all the triggers run?
		if newGraph || newVersion {
			for _, triggerspec := range BUILTIN_TRIGGERS {
				logrus.Debugf("Adding trigger %s to new graph %s", triggerspec.name, req.Graph)
				trigger, err := L.parseTrigger(triggerspec.trigger, req.Graph, triggerspec.name)
				if err != nil {
					return nil, errors.Wrap(err, "could not parse trigger")
				}
				err = L.saveTrigger(trigger)
				if err != nil {
					return nil, errors.Wrap(err, "could not save trigger")
				}
			}
		}
		serializedEntry, serErr := proto.Marshal(entry)
		if serErr != nil {
			txn.Discard()
			err = errors.Wrap(serErr, "serialize log entry")
			resp.Error = err.Error()
			return
		}
		// keyed by the sequence
		key, seqErr := seq.Next()
		if seqErr != nil {
			txn.Discard()
			err = errors.Wrap(seqErr, "serialize log entry")
			resp.Error = err.Error()
			return
		}

		if commitErr := L.setWithCommit(txn, uint64ToBytes(key), serializedEntry); commitErr != nil {
			err = commitErr
			resp.Error = err.Error()
			return
		}
	}
	if txerr := txn.Commit(nil); txerr != nil {
		txn.Discard()
		err = errors.Wrap(txerr, "commit log entry")
		resp.Error = err.Error()
		return
	}
	logrus.Debugf("Added %d triples to graph %s (tag %s)", len(req.Triples), req.Graph, req.Tag)
	return
}

func (L *Log) RegisterTrigger(ctx context.Context, req *logpb.RegisterTriggerRequest) (resp *logpb.TriggerResponse, err error) {
	// get the sequence
	resp = new(logpb.TriggerResponse)
	err = L.saveTrigger(req.Trigger)
	resp.Error = err.Error()
	return
}

func (L *Log) DeleteTrigger(ctx context.Context, req *logpb.DeleteTriggerRequest) (resp *logpb.TriggerResponse, err error) {
	resp = new(logpb.TriggerResponse)
	err = L.deleteTrigger(req)
	resp.Error = err.Error()
	return
}

func (L *Log) ListTriggers(ctx context.Context, req *logpb.ListTriggersRequest) (resp *logpb.TriggerResponse, err error) {
	resp = new(logpb.TriggerResponse)
	opt := badger.DefaultIteratorOptions
	opt.PrefetchSize = 100
	err = L.log.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(opt)
		defer it.Close()
		for it.Rewind(); it.Valid(); it.Next() {
			item := it.Item()
			var entry = new(logpb.LogEntry)
			var unmarshalErr error
			err := item.Value(func(b []byte) {
				unmarshalErr = proto.Unmarshal(b, entry)
			})

			if err != nil {
				return err
			}
			if unmarshalErr != nil {
				return unmarshalErr
			}
			if entry.Trigger != nil {
				resp.Triggers = append(resp.Triggers, entry.Trigger)
			}
		}
		return nil
	})
	resp.Error = err.Error()
	return
}

func (L *Log) Select(ctx context.Context, query *logpb.SelectQuery) (resp *logpb.Response, err error) {

	var cursor *Cursor
	resp = new(logpb.Response)
	for _, graph := range query.Graphs {
		// TODO: check query.Filter
		//cursor = L.Cursor(graph, query.Timestamp, nil)
		cursor, err = L.createCursor(graph, 0, query.Timestamp)
		if err != nil {
			return
		}

		var vars []string
		for _, triple := range query.Where {
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
		dg := makeDependencyGraph(cursor, vars, query.Where)
		qp, err := formQueryPlan(dg, nil)
		if err != nil {
			resp.Error = err.Error()
			err = errors.Wrap(err, "Could not form query plan")
			return resp, err
		}
		qp.variables = query.Vars
		cursor.addQueryPlan(qp)
		cursor.selectVars = query.Vars

		for _, op := range qp.operations {
			//logrus.Info("op | ", op)
			err := op.run(cursor)
			if err != nil {
				err = errors.Wrapf(err, "Could not run op %s", op)
				resp.Error = err.Error()
				return resp, err
			}
		}
		resp.Variables = query.Vars
		resp.Rows = append(resp.Rows, cursor.GetRowsWithVar(query.Vars)...)
		resp.Version = query.Timestamp
		resp.Count = int64(len(resp.Rows))
		//cursor.dumpTil(len(vars))
		if debug {
			cursor.dumpResponse(resp)
		}
	}
	return

}

// if you only want count, don't need to transmit all the query contents over the network
func (L *Log) Count(ctx context.Context, query *logpb.SelectQuery) (resp *logpb.Response, err error) {
	resp, err = L.Select(ctx, query)
	if resp != nil {
		resp.Rows = resp.Rows[:0]
	}

	return
}

func (l *Log) Versions(context.Context, *logpb.VersionQuery) (resp *logpb.Response, err error) {
	// timefilter query.Filter:
	// []graphname query.Graphs:
	// int Limit :
	return
}

func (L *Log) setWithCommit(txn *badger.Txn, key, value []byte) error {
	if setErr := txn.Set(key, value); setErr == badger.ErrTxnTooBig {
		logrus.Println("commit too big")
		if txerr := txn.Commit(nil); txerr != nil {
			txn.Discard()
			return errors.Wrap(txerr, "commit log entry")
		}
		txn = L.log.NewTransaction(true)
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

func (L *Log) LoadFile(graphname, ttlfile, tag string) (int64, error) {
	p := turtle.GetParser()
	dataset, _ := p.Parse(ttlfile)
	var insert = &logpb.TagInsertRequest{
		Graph:     graphname,
		Tag:       tag,
		Operation: logpb.Op_ADD,
	}
	for abbr, full := range dataset.Namespaces {
		L.namespaces[abbr] = full
	}

	for _, _triple := range dataset.Triples {
		triple := &logpb.Triple{
			Subject:   convertURI(_triple.Subject),
			Predicate: []*logpb.URI{convertURI(_triple.Predicate)},
			Object:    convertURI(_triple.Object),
		}
		insert.Triples = append(insert.Triples, triple)
	}

	resp, e := L.TagInsert(context.Background(), insert)
	if e != nil {
		return 0, errors.Wrap(e, "insert to log")
	}
	if len(resp.Error) != 0 {
		return 0, errors.New(resp.Error)
	}
	return resp.Timestamp, nil
}
