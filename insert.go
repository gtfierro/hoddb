package main

import (
	"encoding/binary"
	"github.com/dgraph-io/badger"
	"github.com/golang/protobuf/proto"
	logpb "github.com/gtfierro/hodlog/proto"
	"github.com/pkg/errors"
	logrus "github.com/sirupsen/logrus"
	"time"
)

type edgecache struct {
	in  map[EntityKey]struct{}
	out map[EntityKey]struct{}
}

func newEdgeCache() edgecache {
	return edgecache{
		in:  make(map[EntityKey]struct{}),
		out: make(map[EntityKey]struct{}),
	}
}

//func addEdge(

func (L *Log) createCursor(graph string, from, to int64) (*Cursor, error) {
	var cursor *Cursor
	var found bool
	var err error

	latest, err := L.versionDB.latestVersion(graph, to)
	if err != nil {
		return nil, errors.Wrapf(err, "could not get latest version of graph %s", graph)
	}
	logrus.Debug(latest)

	L.RLock()
	if _, found = L.cursorCache[[2]int64{from, to}]; found {
		L.RUnlock()
		return L.Cursor(graph, latest, nil), nil
	}
	L.RUnlock()

	// process the entities with the latest versions of the tags for the log between the two times
	entries := L.readRangeGraph(graph, from, to)
	var entities = make(map[EntityKey]*logpb.Entity)

	//var edges = make(map[EntityKey]edgecache)

	// get latest version of graph; we copy all keys from previous version
	// into this version
	cursor = L.Cursor(graph, latest, nil)

	if err := L.processLogEntries(cursor, entities, entries); err != nil {
		return nil, err
	}
	fromT := time.Unix(0, from)
	toT := time.Unix(0, to)
	logrus.Infof("Processed %d entities for graph %s from %s - %s", len(entities), graph, fromT, toT)

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

		if err := L.processLogEntries(cursor, entities, generated); err != nil {
			return nil, err
		}
		logrus.Infof("Loop1: Processed %d entities for graph %s from %s - %s from trigger %s", len(entities), graph, fromT, toT, trigger.Name)
	}

	for _, trigger := range triggers {
		generated, err := L.runTrigger(cursor, trigger)
		if err != nil {
			return nil, errors.Wrap(err, "could not run trigger")
		}

		if err := L.processLogEntries(cursor, entities, generated); err != nil {
			return nil, err
		}
		logrus.Infof("Loop2: Processed %d entities for graph %s from %s - %s from trigger %s", len(entities), graph, fromT, toT, trigger.Name)
	}

	cursor.dropCache()

	cursor = L.Cursor(graph, latest, nil)
	L.Lock()
	L.cursorCache[[2]int64{from, to}] = cursor
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
func (L *Log) loadEntry(txn *badger.Txn, cache map[EntityKey]*logpb.Entity, key EntityKey) (ent *logpb.Entity) {
	var found bool

	// case 1: exists in the cache
	ent, found = cache[key]
	if found && ent != nil {
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
		return
	}

	var newk EntityKey
	for newk, ent = range cache {
		if key.HashEquals(newk) {
			// rewrite the old entity's key so it can be
			// part of this version
			ent.EntityKey = key.Bytes()
			cache[key] = ent
			return
		}
	}
	// case 3: exists in previous version
	// create new key with new version
	// most recent version?
	_ent, err = L.GetRecentEntity(key)
	if _ent != nil && _ent.e != nil && err == nil {
		ent = _ent.e
		logrus.Warning("Got entity @ ", _ent.key.Timestamp(), " for graph @ ", key.Timestamp())
		ent.EntityKey = key.Bytes()
		_ent.e.EntityKey = key.Bytes()
		cache[key] = _ent.e
		return
	}

	// case 4: does not exist
	ent = new(logpb.Entity)
	ent.EntityKey = make([]byte, 16)
	copy(ent.EntityKey[:], key.Bytes())
	cache[key] = ent
	return
}

func (L *Log) processEntry(txn *badger.Txn, cache map[EntityKey]*logpb.Entity, e *logpb.LogEntry) {
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
		binary.LittleEndian.PutUint64(subject.Version[:], uint64(e.Timestamp))
		subjectEntity = L.loadEntry(txn, cache, subject)

		// predicate
		copy(predicate.Hash[:], hashURI(e.Triple.Predicate[0]))
		copy(predicate.Graph[:], hashString(e.Graph))
		binary.LittleEndian.PutUint64(predicate.Version[:], uint64(e.Timestamp))
		predicateEntity = L.loadEntry(txn, cache, predicate)

		// object
		copy(object.Hash[:], hashURI(e.Triple.Object))
		copy(object.Graph[:], hashString(e.Graph))
		binary.LittleEndian.PutUint64(object.Version[:], uint64(e.Timestamp))
		objectEntity = L.loadEntry(txn, cache, object)

		// add edge to subject
		var toEdge = new(logpb.Entity_Edge)
		toEdge.Predicate = predicate.Bytes()
		toEdge.Value = object.Bytes()
		subjectEntity.Out = addEdgeIfNotExist(subjectEntity.Out, toEdge)
		//if strings.Contains(S(subject), "vav_1") {
		//	logrus.Warning("  ADD OUT EDGE ", S(predicate))
		//	for _, e := range subjectEntity.Out {
		//		logrus.Warning("     ", S(EntityKeyFromBytes(e.Predicate)))
		//	}
		//}

		// add edge to object
		var fromEdge = new(logpb.Entity_Edge)
		fromEdge.Predicate = predicate.Bytes()
		fromEdge.Value = subject.Bytes()
		//if strings.Contains(S(object), "vav_1") {
		//	logrus.Warning("  ADD IN EDGE ", S(predicate))
		//}
		objectEntity.In = addEdgeIfNotExist(objectEntity.In, fromEdge)

		// add endpoints to predicate
		var endpoints = new(logpb.Entity_Endpoints)
		endpoints.Src = subject.Bytes()
		endpoints.Dst = object.Bytes()
		predicateEntity.Endpoints = append(predicateEntity.Endpoints, endpoints)

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
		//if S(subject) == "https://brickschema.org/schema/1.0.3/Brick#Zone_Temperature_Sensor" {
		//	logrus.Warning("inserting: ", subject)
		//}
	}
}

func (L *Log) processLogEntries(cursor *Cursor, entities map[EntityKey]*logpb.Entity, entries chan *logpb.LogEntry) error {
	txn := L.db.NewTransaction(true)

	// populates the entities objects
	for entry := range entries {
		L.processEntry(txn, entities, entry)
	}
	// inserts these into the database under the current version
	for k, v := range entities {
		serializedEntry, err := proto.Marshal(v)
		if err != nil {
			txn.Discard()
			return errors.Wrap(err, "Error serializing entry")
		}
		if err := L.setWithCommit(txn, k.Bytes(), serializedEntry); err != nil {
			return errors.Wrap(err, "Error txn commit")
		}
	}
	if err := txn.Commit(nil); err != nil {
		txn.Discard()
		return errors.Wrap(err, "last commit")
	}

	cursor.dropCache()

	//  insert extended edges
	for k, v := range entities {
		ent, err := cursor.getEntity(k)
		if err != nil {
			return errors.Wrap(err, "Could not get entity in ext")
		}

		//if S(k) == "https://brickschema.org/schema/1.0.3/Brick#Zone_Temperature_Sensor" {
		//	logrus.Warning("extended for ", S(k), " ", k)
		//}
		for _, pred := range ent.GetAllPredicates() {
			e := edge{predicate: pred, pattern: logpb.Pattern_OnePlus}
			newseen, _, err := cursor.followPathFromSubject(ent, e)
			//if strings.Contains(S(k), "vav_1") {
			//	logrus.Warning("  for pred ", S(pred), " ", len(newseen))
			//}
			if err != nil {
				return err
			}
			for newkey := range newseen {
				var toEdge = new(logpb.Entity_Edge)
				toEdge.Predicate = pred.Bytes()
				toEdge.Value = newkey.Bytes()
				toEdge.Pattern = logpb.Pattern_OnePlus
				//if strings.Contains(S(k), "vav_1") {
				//	logrus.Warning("  OUT+ ", S(pred), " ", S(newkey))
				//}
				found := false
				v.Out = addEdgeIfNotExist(v.Out, toEdge)
				entities[k] = v
				obj, found := entities[newkey]
				if found {
					var invToEdge = new(logpb.Entity_Edge)
					invToEdge.Predicate = pred.Bytes()
					invToEdge.Value = k.Bytes()
					invToEdge.Pattern = logpb.Pattern_OnePlus
					//if strings.Contains(S(k), "vav_1") {
					//	logrus.Warning("  IN+ (", S(newkey), ") ", S(pred), " ", S(k))
					//}
					found = false
					obj.In = addEdgeIfNotExist(obj.In, invToEdge)
					entities[newkey] = obj
				}

			}
		}
	}

	// re-insert these with extended edges
	txn = L.db.NewTransaction(true)
	for k, v := range entities {
		serializedEntry, err := proto.Marshal(v)
		if err != nil {
			txn.Discard()
			return errors.Wrap(err, "Error serializing entry")
		}
		if err := L.setWithCommit(txn, k.Bytes(), serializedEntry); err != nil {
			return errors.Wrap(err, "Error txn commit")
		}
	}
	if err := txn.Commit(nil); err != nil {
		txn.Discard()
		return errors.Wrap(err, "last commit")
	}
	return nil
}
