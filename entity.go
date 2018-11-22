package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	logpb "github.com/gtfierro/hodlog/proto"
	"github.com/gtfierro/hodlog/turtle"
	"time"
)

func __flush() *logpb.LogEntry {
	return &logpb.LogEntry{Op: logpb.Op_FLUSH, Timestamp: time.Now().UnixNano()}

}

func timestampNowBytes() (ts [8]byte) {
	binary.LittleEndian.PutUint64(ts[:], uint64(time.Now().UnixNano()))
	return
}

func convertURI(t turtle.URI) *logpb.URI {
	return &logpb.URI{
		Namespace: t.Namespace,
		Value:     t.Value,
	}
}

type EntityKey struct {
	Graph   [4]byte
	Hash    [4]byte
	Version [8]byte
}

var _e4 = [4]byte{0, 0, 0, 0}
var _e8 = [8]byte{0, 0, 0, 0, 0, 0, 0, 0}

func (key EntityKey) Empty() bool {
	return key.Graph == _e4 && key.Hash == _e4 && key.Version == _e8
}

func (key EntityKey) Prefix() []byte {
	return append(key.Graph[:], key.Hash[:]...)
}

func (key EntityKey) String() string {
	return fmt.Sprintf("<Hash: %v, Graph: %v, Version: %v>", key.Hash, key.Graph, key.Timestamp())
}

func (key EntityKey) HashEquals(key2 EntityKey) bool {
	return bytes.Equal(key.Hash[:], key2.Hash[:])
}

func compareEntityKeyHash(key1, key2 []byte) bool {
	return bytes.Equal(key1[4:8], key2[4:8])
}

func (key *EntityKey) FromBytes(b []byte) {
	if len(b) < 12 {
		panic("too short")
	}
	if key == nil {
		key = new(EntityKey)
	}
	copy(key.Graph[:], b[:4])
	copy(key.Hash[:], b[4:8])
	copy(key.Version[:], b[8:16])
}

func EntityKeyFromBytes(b []byte) EntityKey {
	if len(b) < 12 {
		panic("too short")
	}
	var key EntityKey
	copy(key.Graph[:], b[:4])
	copy(key.Hash[:], b[4:8])
	copy(key.Version[:], b[8:16])
	return key
}

func (key EntityKey) Timestamp() int64 {
	return int64(binary.LittleEndian.Uint64(key.Version[:]))
}

// if b is nil, then return. Else, copy into b
func (key EntityKey) Bytes() (r []byte) {
	//if len(b) < 16 {
	//	b = make([]byte, 16)
	//}
	//copy(b[:], key.Graph[:])
	//copy(b[4:], key.Hash[:])
	//copy(b[8:], key.Version[:])
	//} else {
	r = make([]byte, 16)
	copy(r, key.Graph[:])
	copy(r[4:], key.Hash[:])
	copy(r[8:], key.Version[:])
	//}
	return
}

func EntityKeyFromInts(hash uint32, graph uint32, timestamp uint32) EntityKey {
	var ek = new(EntityKey)
	binary.LittleEndian.PutUint32(ek.Graph[:], graph)
	binary.LittleEndian.PutUint32(ek.Hash[:], hash)
	binary.LittleEndian.PutUint32(ek.Version[:], timestamp)
	return *ek
}

type Entity struct {
	e   *logpb.Entity
	key EntityKey
}

func (e *Entity) GetAllPredicates() (preds []EntityKey) {
edgeloop:
	for _, edge := range append(e.e.In, e.e.Out...) {
		for _, p := range preds {
			if bytes.Equal(p.Hash[:], edge.Predicate[4:8]) {
				continue edgeloop
			}
		}
		preds = append(preds, EntityKeyFromBytes(edge.Predicate))
	}
	return
}

// predicate/object, predicate/subject
func (e *Entity) GetAllEdges() (edges [][]EntityKey) {
	for _, edge := range append(e.e.In, e.e.Out...) {
		edges = append(edges, []EntityKey{EntityKeyFromBytes(edge.Predicate), EntityKeyFromBytes(edge.Value)})
	}
	return
}

func (e *Entity) GetAllInEdges() (edges [][]EntityKey) {
	for _, edge := range e.e.In {
		if edge.Pattern == logpb.Pattern_Single {
			edges = append(edges, []EntityKey{EntityKeyFromBytes(edge.Predicate), EntityKeyFromBytes(edge.Value)})
		}
	}
	return
}

func (e *Entity) GetAllOutEdges() (edges [][]EntityKey) {
	for _, edge := range e.e.Out {
		if edge.Pattern == logpb.Pattern_Single {
			edges = append(edges, []EntityKey{EntityKeyFromBytes(edge.Predicate), EntityKeyFromBytes(edge.Value)})
		}
	}
	return
}

func (e *Entity) GetAllInPlusEdges() (edges [][]EntityKey) {
	for _, edge := range e.e.In {
		if edge.Pattern != logpb.Pattern_Single {
			edges = append(edges, []EntityKey{EntityKeyFromBytes(edge.Predicate), EntityKeyFromBytes(edge.Value)})
		}
	}
	return
}

func (e *Entity) GetAllOutPlusEdges() (edges [][]EntityKey) {
	for _, edge := range e.e.Out {
		if edge.Pattern != logpb.Pattern_Single {
			edges = append(edges, []EntityKey{EntityKeyFromBytes(edge.Predicate), EntityKeyFromBytes(edge.Value)})
		}
	}
	return
}

func (e *Entity) InEdges(predicate EntityKey) (entities []EntityKey) {
	for _, edge := range append(e.e.In) {
		if edge.Pattern == logpb.Pattern_Single && bytes.Equal(predicate.Hash[:], edge.Predicate[4:8]) {
			entities = append(entities, EntityKeyFromBytes(edge.Value))
		}
	}
	return
}
func (e *Entity) OutEdges(predicate EntityKey) (entities []EntityKey) {
	for _, edge := range append(e.e.Out) {
		if edge.Pattern == logpb.Pattern_Single && bytes.Equal(predicate.Hash[:], edge.Predicate[4:8]) {
			entities = append(entities, EntityKeyFromBytes(edge.Value))
		}
	}
	return
}
func (e *Entity) InPlusEdges(predicate EntityKey) (entities []EntityKey) {
	for _, edge := range append(e.e.In) {
		if edge.Pattern != logpb.Pattern_Single && bytes.Equal(predicate.Hash[:], edge.Predicate[4:8]) {
			entities = append(entities, EntityKeyFromBytes(edge.Value))
		}
	}
	return
}
func (e *Entity) OutPlusEdges(predicate EntityKey) (entities []EntityKey) {
	for _, edge := range append(e.e.Out) {
		if edge.Pattern != logpb.Pattern_Single && bytes.Equal(predicate.Hash[:], edge.Predicate[4:8]) {
			entities = append(entities, EntityKeyFromBytes(edge.Value))
		}
	}
	return
}
func (e *Entity) GetObjects(subject EntityKey) (objects []EntityKey) {
	for _, endpoint := range e.e.Endpoints {
		if bytes.Equal(subject.Hash[:], endpoint.Src[4:8]) {
			objects = append(objects, EntityKeyFromBytes(endpoint.Dst))
		}
	}
	return
}
func (e *Entity) GetSubjects(object EntityKey) (subjects []EntityKey) {
	for _, endpoint := range e.e.Endpoints {
		if bytes.Equal(object.Hash[:], endpoint.Dst[4:8]) {
			subjects = append(subjects, EntityKeyFromBytes(endpoint.Src))
		}
	}
	return
}
func (e *Entity) GetAllObjects() (objects []EntityKey) {
endpointloop:
	for _, endpoint := range e.e.Endpoints {
		for _, o := range objects {
			if bytes.Equal(o.Hash[:], endpoint.Dst[4:8]) {
				continue endpointloop
			}
		}
		objects = append(objects, EntityKeyFromBytes(endpoint.Dst))
	}
	return
}
func (e *Entity) GetAllSubjects() (subjects []EntityKey) {
endpointloop:
	for _, endpoint := range e.e.Endpoints {
		for _, s := range subjects {
			if bytes.Equal(s.Hash[:], endpoint.Src[4:8]) {
				continue endpointloop
			}
		}
		subjects = append(subjects, EntityKeyFromBytes(endpoint.Src))
	}
	return
}
func (e *Entity) GetAllEndpoints() (endpoints [][]EntityKey) {
endpointloop:
	for _, endpoint := range e.e.Endpoints {
		for _, e := range endpoints {
			if bytes.Equal(e[1].Hash[:], endpoint.Dst[4:8]) && bytes.Equal(e[0].Hash[:], endpoint.Src[4:8]) {
				continue endpointloop
			}
		}
		endpoints = append(endpoints, []EntityKey{EntityKeyFromBytes(endpoint.Src), EntityKeyFromBytes(endpoint.Dst)})
	}
	return
}

func (L *Log) Dump(e *Entity) {
	fmt.Println("ent>", S(e.key))
	for _, pred := range e.GetAllPredicates() {
		fmt.Println("  pred>", S(pred))
		for _, ent := range e.InEdges(pred) {
			fmt.Println("    subject>", S(ent))
		}
		for _, ent := range e.OutEdges(pred) {
			fmt.Println("    object>", S(ent))
		}
		for _, ent := range e.InPlusEdges(pred) {
			fmt.Println("    subject+>", S(ent))
		}
		for _, ent := range e.OutPlusEdges(pred) {
			fmt.Println("    object+>", S(ent))
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
