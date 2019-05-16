package hod

import (
	"context"
	"encoding/binary"
	"fmt"
	"strings"

	query "git.sr.ht/~gabe/hod/lang"
	sparql "git.sr.ht/~gabe/hod/lang/ast"
	logpb "git.sr.ht/~gabe/hod/proto"
	turtle "git.sr.ht/~gabe/hod/turtle"
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

	hod.namespaces.Store(graph.Name, graph.Data.Namespaces)
	hod.graphs[graph.Name] = struct{}{}

	// insert extended edges
	cursor, err := hod.Cursor(graph.Name)
	if err != nil {
		return errors.Wrap(err, "get cursor")
	}
	for key, ent := range entities {

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
				entities[newkey] = other
			}
		}
		entities[key] = ent

	}

	txn = hod.db.NewTransaction(true)

	for _, ent := range entities {
		ent.Compile()
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
	//log.Debug("Set Entity ", key)
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
	//log.Debug("Get Entity ", key)
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

func (hod *HodDB) hashURI(graph string, u turtle.URI) EntityKey {

	hk := hashkeyentry{graph, u}

	hod.RLock()
	if key, found := hod.hashes[hk]; found {
		hod.RUnlock()
		return key
	}
	hod.RUnlock()

	var key EntityKey

	//hashresult := murmur.Murmur3([]byte(u.Namespace + u.Value))
	binary.BigEndian.PutUint32(key.Hash[:], murmur.Murmur3([]byte(u.Namespace+u.Value)))
	binary.BigEndian.PutUint32(key.Graph[:], murmur.Murmur3([]byte(graph)))

	if other_uri, found := hod.uris[key]; found {
		panic(fmt.Sprintf("URI for %s conflicts with %s", u, other_uri))
	}

	hod.Lock()
	hod.hashes[hk] = key
	hod.uris[key] = u
	hod.Unlock()
	return key
}

func (hod *HodDB) Versions(ctx context.Context, request *logpb.VersionQuery) (*logpb.Response, error) {
	var resp = new(logpb.Response)
	hod.RLock()
	for graph := range hod.graphs {
		resp.Rows = append(resp.Rows, &logpb.Row{
			Values: []*logpb.URI{{
				Value: graph,
			}},
		})
	}
	hod.RUnlock()
	return resp, nil
}

func (hod *HodDB) ParseQuery(qstr string, version int64) (*logpb.SelectQuery, error) {
	q, err := query.Parse(qstr)
	if err != nil {
		return nil, err
	}

	sq := &logpb.SelectQuery{
		Vars:      q.Variables,
		Graphs:    q.From.Databases,
		Timestamp: version,
		Filter:    logpb.TimeFilter_Before,
		//Where:
	}

	// TODO: expand

	for _, triple := range q.Where.Terms {
		term := &logpb.Triple{
			Subject: hod.expandURI(convertURI(triple.Subject), ""),
			Object:  hod.expandURI(convertURI(triple.Object), ""),
		}
		for _, pred := range triple.Predicates {
			// TODO: use pattern
			uri := hod.expandURI(convertURI(pred.Predicate), "")
			switch pred.Pattern {
			case sparql.PATTERN_SINGLE:
				uri.Pattern = logpb.Pattern_Single
			case sparql.PATTERN_ZERO_ONE:
				uri.Pattern = logpb.Pattern_ZeroOne
			case sparql.PATTERN_ONE_PLUS:
				uri.Pattern = logpb.Pattern_OnePlus
			case sparql.PATTERN_ZERO_PLUS:
				uri.Pattern = logpb.Pattern_ZeroPlus
			}
			term.Predicate = append(term.Predicate, uri)
		}
		sq.Where = append(sq.Where, term)
	}

	return sq, nil
}

func (hod *HodDB) expandURI(uri *logpb.URI, graphname string) *logpb.URI {
	if !strings.HasPrefix(uri.Value, "?") {
		if len(uri.Value) == 0 {
			return uri
		}

		// get random graph if one is not provided
		if graphname == "" {
			for key := range hod.graphs {
				graphname = key
				break
			}
		}

		_namespaces, ok := hod.namespaces.Load(graphname)
		if !ok {
			return nil
		}
		namespaces := _namespaces.(map[string]string)
		if uri.Namespace != "" && (uri.Value[0] != '"' && uri.Value[len(uri.Value)-1] != '"') {
			if full, found := namespaces[uri.Namespace]; found {
				uri.Namespace = full
			}
		}
	}
	return uri
}

func (hod *HodDB) Count(ctx context.Context, query *logpb.SelectQuery) (resp *logpb.Response, err error) {
	resp, err = hod.Select(ctx, query)
	if resp != nil {
		resp.Rows = resp.Rows[:0]
	}
	return
}
func (hod *HodDB) Select(ctx context.Context, query *logpb.SelectQuery) (resp *logpb.Response, err error) {

	var cursor *Cursor
	resp = new(logpb.Response)
	if len(query.Graphs) == 1 && query.Graphs[0] == "*" {
		var graphs []string
		for graph := range hod.graphs {
			graphs = append(graphs, graph)
		}
		query.Graphs = graphs
		//query.Graphs, err = hod.versionDB.listAllGraphs()
		//if err != nil {
		//	log.Error(err)
		//	return
		//}
	}

	for _, graph := range query.Graphs {
		// TODO: check query.Filter
		cursor, err = hod.Cursor(graph)
		if err != nil {
			log.Error(err)
			return
		}

		var vars []string
		for idx, triple := range query.Where {
			if isVariable(triple.Subject) {
				vars = append(vars, triple.Subject.Value)
			} else {
				query.Where[idx].Subject = hod.expandURI(triple.Subject, graph)
			}
			if isVariable(triple.Predicate[0]) {
				vars = append(vars, triple.Predicate[0].Value)
			} else {
				query.Where[idx].Predicate[0] = hod.expandURI(triple.Predicate[0], graph)
			}
			if isVariable(triple.Object) {
				vars = append(vars, triple.Object.Value)
			} else {
				query.Where[idx].Object = hod.expandURI(triple.Object, graph)
			}
		}
		dg := makeDependencyGraph(cursor, vars, query.Where)
		qp, err := formQueryPlan(dg, nil)
		if err != nil {
			resp.Error = err.Error()
			err = errors.Wrap(err, "Could not form query plan")
			log.Error(err)
			return resp, err
		}
		qp.variables = query.Vars
		cursor.addQueryPlan(qp)
		cursor.selectVars = query.Vars

		for _, op := range qp.operations {
			err := op.run(cursor)
			if err != nil {
				err = errors.Wrapf(err, "Could not run op %s", op)
				resp.Error = err.Error()
				log.Error(err)
				continue
				//return resp, err
			}
		}
		resp.Variables = query.Vars
		resp.Rows = append(resp.Rows, cursor.GetRowsWithVar(query.Vars)...)
		resp.Version = query.Timestamp
		resp.Count = int64(len(resp.Rows))
		//cursor.dumpTil(len(vars))
	}
	return

}

func (hod *HodDB) Dump(e *Entity) {
	fmt.Println("ent>", hod.s(e.key))
	for _, pred := range e.GetAllPredicates() {
		fmt.Println("  pred>", hod.s(pred))
		for _, ent := range e.InEdges(pred) {
			fmt.Println("    subject>", hod.s(ent))
		}
		for _, ent := range e.OutEdges(pred) {
			fmt.Println("    object>", hod.s(ent))
		}
		for _, ent := range e.InPlusEdges(pred) {
			fmt.Println("    subject+>", hod.s(ent))
		}
		for _, ent := range e.OutPlusEdges(pred) {
			fmt.Println("    object+>", hod.s(ent))
		}
		//predEnt, err := L.GetEntity(pred)
		//if err != nil {
		//	log.Fatal(err)
		//}
		//for _, object := range predEnt.GetAllObjects() {
		//	fmt.Println("    allobjectlist>", LOOKUPURI[object.Hash])
		//	for _, sub := range predEnt.GetSubjects(object) {
		//		fmt.Println("    allobjectlist>sub>", LOOKUPURI[sub.Hash])
		//	}
		//}
		//for _, subject := range predEnt.GetAllSubjects() {
		//	fmt.Println("    allsubjectlist>", LOOKUPURI[subject.Hash])
		//	for _, ob := range predEnt.GetObjects(subject) {
		//		fmt.Println("    allsubjectlist>obj>", LOOKUPURI[ob.Hash])
		//	}
		//}
	}
}

func (hod *HodDB) s(u EntityKey) string {
	uri := hod.uris[u]
	if uri.Namespace != "" {
		return uri.Namespace + "#" + uri.Value
	}
	return uri.Value
}

func (hod *HodDB) getURI(key EntityKey) (turtle.URI, bool) {
	hod.RLock()
	uri, found := hod.uris[key]
	hod.RUnlock()
	return uri, found
}
