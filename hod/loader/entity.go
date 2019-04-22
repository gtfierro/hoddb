package loader

import (
	"bytes"
	"encoding/binary"
	"strings"

	logpb "git.sr.ht/~gabe/hod/proto"
	"git.sr.ht/~gabe/hod/turtle"
)

var _e4 = [4]byte{0, 0, 0, 0}
var _e8 = [8]byte{0, 0, 0, 0, 0, 0, 0, 0}

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

type EntityKey struct {
	Graph   [4]byte
	Hash    [4]byte
	Version [8]byte
}

func (key EntityKey) Empty() bool {
	return key.Graph == _e4 && key.Hash == _e4 && key.Version == _e8
}

func (key EntityKey) Bytes() (r []byte) {
	r = make([]byte, 16)
	copy(r, key.Graph[:])
	copy(r[4:], key.Hash[:])
	copy(r[8:], key.Version[:])
	return
}

type Entity struct {
	key       EntityKey
	compiled  *logpb.Entity
	inedge    map[EntityKey]map[EntityKey]logpb.Pattern
	outedge   map[EntityKey]map[EntityKey]logpb.Pattern
	endpoints map[[2]EntityKey]struct{}
}

func newEntity(key EntityKey) *Entity {
	return &Entity{
		key:       key,
		inedge:    make(map[EntityKey]map[EntityKey]logpb.Pattern),
		outedge:   make(map[EntityKey]map[EntityKey]logpb.Pattern),
		endpoints: make(map[[2]EntityKey]struct{}),
	}
}

// TODO: probably need to index edges by the pattern as well as the predicate,
// otherwise we throw away old edges?
func (ent *Entity) addInEdge(pred, subject EntityKey, pattern logpb.Pattern) {
	if _, predfound := ent.inedge[pred]; !predfound {
		ent.inedge[pred] = make(map[EntityKey]logpb.Pattern)
	}
	if foundpat, foundsub := ent.inedge[pred][subject]; foundsub && foundpat == logpb.Pattern_Single {
		return
	}
	ent.inedge[pred][subject] = pattern
}

func (ent *Entity) addOutEdge(pred, object EntityKey, pattern logpb.Pattern) {
	if _, predfound := ent.outedge[pred]; !predfound {
		ent.outedge[pred] = make(map[EntityKey]logpb.Pattern)
	}
	if foundpat, foundob := ent.outedge[pred][object]; foundob && foundpat == logpb.Pattern_Single {
		return
	}
	ent.outedge[pred][object] = pattern
}

func (ent *Entity) addEndpoints(subject, object EntityKey) {
	ent.endpoints[[2]EntityKey{subject, object}] = struct{}{}
}

func (ent *Entity) Compile() {
	ent.compiled = &logpb.Entity{}
	ent.compiled.EntityKey = ent.key.Bytes()

	for inpred, inobjs := range ent.inedge {
		for inobj, pattern := range inobjs {
			edge := &logpb.Entity_Edge{Pattern: pattern}
			edge.Predicate = inpred.Bytes()
			edge.Value = inobj.Bytes()
			ent.compiled.In = append(ent.compiled.In, edge)
		}
	}

	for outpred, outobjs := range ent.outedge {
		for outobj, pattern := range outobjs {
			edge := &logpb.Entity_Edge{Pattern: pattern}
			edge.Predicate = outpred.Bytes()
			edge.Value = outobj.Bytes()
			ent.compiled.Out = append(ent.compiled.Out, edge)
		}
	}

	for endpoints := range ent.endpoints {
		e := &logpb.Entity_Endpoints{
			Src: endpoints[0].Bytes(),
			Dst: endpoints[1].Bytes(),
		}
		ent.compiled.Endpoints = append(ent.compiled.Endpoints, e)
	}
}

func (ent *Entity) GetAllPredicates() (preds []EntityKey) {
edgeloop:
	for _, edge := range append(ent.compiled.In, ent.compiled.Out...) {
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
func (ent *Entity) GetAllEdges() (edges [][]EntityKey) {
	for _, edge := range append(ent.compiled.In, ent.compiled.Out...) {
		edges = append(edges, []EntityKey{EntityKeyFromBytes(edge.Predicate), EntityKeyFromBytes(edge.Value)})
	}
	return
}

func (ent *Entity) GetAllInEdges() (edges [][]EntityKey) {
	for _, edge := range ent.compiled.In {
		if edge.Pattern == logpb.Pattern_Single {
			edges = append(edges, []EntityKey{EntityKeyFromBytes(edge.Predicate), EntityKeyFromBytes(edge.Value)})
		}
	}
	return
}

func (ent *Entity) GetAllOutEdges() (edges [][]EntityKey) {
	for _, edge := range ent.compiled.Out {
		if edge.Pattern == logpb.Pattern_Single {
			edges = append(edges, []EntityKey{EntityKeyFromBytes(edge.Predicate), EntityKeyFromBytes(edge.Value)})
		}
	}
	return
}

func (ent *Entity) GetAllInPlusEdges() (edges [][]EntityKey) {
	for _, edge := range ent.compiled.In {
		//if edge.Pattern != logpb.Pattern_Single {
		edges = append(edges, []EntityKey{EntityKeyFromBytes(edge.Predicate), EntityKeyFromBytes(edge.Value)})
		//}
	}
	return
}

func (ent *Entity) GetAllOutPlusEdges() (edges [][]EntityKey) {
	for _, edge := range ent.compiled.Out {
		//if edge.Pattern != logpb.Pattern_Single {
		edges = append(edges, []EntityKey{EntityKeyFromBytes(edge.Predicate), EntityKeyFromBytes(edge.Value)})
		//}
	}
	return
}

func (ent *Entity) InEdges(predicate EntityKey) (entities []EntityKey) {
	for _, edge := range append(ent.compiled.In) {
		if edge.Pattern == logpb.Pattern_Single && bytes.Equal(predicate.Hash[:], edge.Predicate[4:8]) {
			entities = append(entities, EntityKeyFromBytes(edge.Value))
		}
	}
	return
}
func (ent *Entity) OutEdges(predicate EntityKey) (entities []EntityKey) {
	for _, edge := range append(ent.compiled.Out) {
		if edge.Pattern == logpb.Pattern_Single && bytes.Equal(predicate.Hash[:], edge.Predicate[4:8]) {
			entities = append(entities, EntityKeyFromBytes(edge.Value))
		}
	}
	return
}
func (ent *Entity) InPlusEdges(predicate EntityKey) (entities []EntityKey) {
	for _, edge := range append(ent.compiled.In) {
		//if edge.Pattern != logpb.Pattern_Single && bytes.Equal(predicate.Hash[:], edge.Predicate[4:8]) {
		if bytes.Equal(predicate.Hash[:], edge.Predicate[4:8]) {
			entities = append(entities, EntityKeyFromBytes(edge.Value))
		}
	}
	return
}
func (ent *Entity) OutPlusEdges(predicate EntityKey) (entities []EntityKey) {
	for _, edge := range append(ent.compiled.Out) {
		//log.Warning(edge.Value, edge.Pattern)
		//if edge.Pattern != logpb.Pattern_Single && bytes.Equal(predicate.Hash[:], edge.Predicate[4:8]) {
		if bytes.Equal(predicate.Hash[:], edge.Predicate[4:8]) {
			entities = append(entities, EntityKeyFromBytes(edge.Value))
		}
	}
	return
}
func (ent *Entity) GetObjects(subject EntityKey) (objects []EntityKey) {
	for _, endpoint := range ent.compiled.Endpoints {
		if bytes.Equal(subject.Hash[:], endpoint.Src[4:8]) {
			objects = append(objects, EntityKeyFromBytes(endpoint.Dst))
		}
	}
	return
}
func (ent *Entity) GetSubjects(object EntityKey) (subjects []EntityKey) {
	for _, endpoint := range ent.compiled.Endpoints {
		if bytes.Equal(object.Hash[:], endpoint.Dst[4:8]) {
			subjects = append(subjects, EntityKeyFromBytes(endpoint.Src))
		}
	}
	return
}
func (ent *Entity) GetAllObjects() (objects []EntityKey) {
endpointloop:
	for _, endpoint := range ent.compiled.Endpoints {
		for _, o := range objects {
			if bytes.Equal(o.Hash[:], endpoint.Dst[4:8]) {
				continue endpointloop
			}
		}
		objects = append(objects, EntityKeyFromBytes(endpoint.Dst))
	}
	return
}
func (ent *Entity) GetAllSubjects() (subjects []EntityKey) {
endpointloop:
	for _, endpoint := range ent.compiled.Endpoints {
		for _, s := range subjects {
			if bytes.Equal(s.Hash[:], endpoint.Src[4:8]) {
				continue endpointloop
			}
		}
		subjects = append(subjects, EntityKeyFromBytes(endpoint.Src))
	}
	return
}
func (ent *Entity) GetAllEndpoints() (endpoints [][]EntityKey) {
endpointloop:
	for _, endpoint := range ent.compiled.Endpoints {
		for _, e := range endpoints {
			if bytes.Equal(e[1].Hash[:], endpoint.Dst[4:8]) && bytes.Equal(e[0].Hash[:], endpoint.Src[4:8]) {
				continue endpointloop
			}
		}
		endpoints = append(endpoints, []EntityKey{EntityKeyFromBytes(endpoint.Src), EntityKeyFromBytes(endpoint.Dst)})
	}
	return
}

func convertURI(t turtle.URI) *logpb.URI {
	return &logpb.URI{
		Namespace: t.Namespace,
		Value:     t.Value,
	}
}

func makeTriple(subject, predicate, object string) *logpb.Triple {
	return &logpb.Triple{
		Subject:   stringtoURI(subject),
		Predicate: []*logpb.URI{stringtoURI(predicate)},
		Object:    stringtoURI(object),
	}
}

func stringtoURI(s string) *logpb.URI {
	var ns, val string
	parts := strings.Split(s, "#")
	if len(parts) == 2 {
		ns = parts[0]
		val = parts[1]
	} else {
		val = parts[0]
	}

	var pattern logpb.Pattern = logpb.Pattern_Single
	if strings.HasSuffix(val, "*") {
		pattern = logpb.Pattern_ZeroPlus
	} else if strings.HasSuffix(val, "+") {
		pattern = logpb.Pattern_OnePlus
	} else if strings.HasSuffix(val, "?") {
		pattern = logpb.Pattern_ZeroOne
	}

	return &logpb.URI{Namespace: ns, Value: val, Pattern: pattern}
}

func EntityKeyFromInts(hash uint32, graph uint32, timestamp uint32) EntityKey {
	var ek = new(EntityKey)
	binary.LittleEndian.PutUint32(ek.Graph[:], graph)
	binary.LittleEndian.PutUint32(ek.Hash[:], hash)
	binary.LittleEndian.PutUint32(ek.Version[:], timestamp)
	return *ek
}
