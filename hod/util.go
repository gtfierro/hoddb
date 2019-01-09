package hod

import (
	"bytes"
	"encoding/binary"
	"strings"
	"sync"

	"github.com/golang/protobuf/proto"
	logpb "git.sr.ht/~gabe/hod/proto"
	"github.com/spaolacci/murmur3"
	"github.com/zhangxinngang/murmur"
)

var LOOKUPURI = make(map[[4]byte]logpb.URI)
var s sync.RWMutex

func hashURI(u *logpb.URI) []byte {
	var dest = make([]byte, 4)
	//bytes, _ := proto.Marshal(&logpb.URI{Namespace: u.Namespace, Value: u.Value})
	//binary.BigEndian.PutUint32(dest, murmur.Murmur3(bytes))
	binary.BigEndian.PutUint32(dest, murmur.Murmur3([]byte(u.Namespace+u.Value)))

	// DEBUG ONLY
	var _k [4]byte
	copy(_k[:], dest)
	s.Lock()
	if _u, found := LOOKUPURI[_k]; found && ((_u.Namespace != u.Namespace) || (_u.Value != u.Value)) {
		panic("HASH EXISTS")
	}
	LOOKUPURI[_k] = *u
	s.Unlock()
	return dest
}

func hashString(s string) []byte {
	var dest = make([]byte, 4)
	binary.BigEndian.PutUint32(dest, murmur.Murmur3([]byte(s)))
	return dest
}

func hashStringUint32(s string) uint32 {
	return murmur.Murmur3([]byte(s))
}

func uint64ToBytes(i uint64) []byte {
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], i)
	return buf[:]
}

func uint64ToBytesLE(i uint64) []byte {
	var buf [8]byte
	binary.LittleEndian.PutUint64(buf[:], i)
	return buf[:]
}

type entitystack struct {
	entities []*Entity
}

func newEntityStack() *entitystack {
	return &entitystack{}
}

func (stack *entitystack) push(e *Entity) {
	stack.entities = append(stack.entities, e)
}

func (stack *entitystack) pop() *Entity {
	if len(stack.entities) == 0 {
		return nil
	}
	e := stack.entities[0]
	stack.entities = stack.entities[1:]
	return e
}

func (stack *entitystack) len() int {
	return len(stack.entities)
}

type entityset map[EntityKey]struct{}

func newEntitySet() entityset {
	return entityset(make(map[EntityKey]struct{}))
}

func (set entityset) has(e EntityKey) bool {
	_, found := set[e]
	return found
}

func (set entityset) addIfNotHas(e EntityKey) bool {
	if _, found := set[e]; found {
		return true
	}
	set[e] = struct{}{}
	return false
}

func (set entityset) addFrom(other entityset) {
	for k := range other {
		set.add(k)
	}
}

func (set entityset) add(e EntityKey) {
	set[e] = struct{}{}
}

func generateEntitySet(numEntities int, graph int, timestamp int) entityset {
	e := newEntitySet()
	for i := 0; i < numEntities; i++ {
		e.add(EntityKeyFromInts(uint32(i), uint32(graph), uint32(timestamp)))
	}
	return e
}

func generateEntityRows(numVars int, numRows int, graph int, timestamp int) (ret [][]EntityKey) {
	hash := 0
	for r := 0; r < numRows; r++ {
		var row []EntityKey
		for i := 0; i < numVars; i++ {
			row = append(row, EntityKeyFromInts(uint32(hash), uint32(graph), uint32(timestamp)))
			hash += 1
		}
		ret = append(ret, row)
	}
	return
}

func isVariable(uri *logpb.URI) bool {
	return uri == nil || strings.HasPrefix(uri.Value, "?")
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

func uriToS(u logpb.URI) string {
	if u.Namespace != "" {
		return u.Namespace + "#" + u.Value
	}
	return u.Value
}

func makeTriple(subject, predicate, object string) *logpb.Triple {
	return &logpb.Triple{
		Subject:   stringtoURI(subject),
		Predicate: []*logpb.URI{stringtoURI(predicate)},
		Object:    stringtoURI(object),
	}
}

func reversePath(path []edge) []edge {
	newpath := make([]edge, len(path))
	// for in-place, replace newpath with path
	if len(newpath) == 1 {
		return path
	}
	for i, j := 0, len(path)-1; i < j; i, j = i+1, j-1 {
		newpath[i], newpath[j] = path[j], path[i]
	}
	return newpath
}

func edgeEqual(a, b *logpb.Entity_Edge) bool {
	return bytes.Equal(a.Predicate, b.Predicate) && bytes.Equal(a.Value, b.Value) && a.Pattern == b.Pattern
}

func S(u EntityKey) string {
	return uriToS(LOOKUPURI[u.Hash])
}

func addEdgeIfNotExist(edges []*logpb.Entity_Edge, newedge *logpb.Entity_Edge) []*logpb.Entity_Edge {
	found := false
	for _, e := range edges {
		if edgeEqual(e, newedge) {
			found = true
			break
		}
	}
	if !found {
		return append(edges, newedge)
	}
	return edges
}

func hashRow(row *logpb.Row) uint32 {
	b, _ := proto.Marshal(row)
	return murmur.Murmur3([]byte(b))
}

func hashRow2(row *logpb.Row) uint32 {
	h := murmur3.New32()
	for _, val := range row.Values {
		h.Write([]byte(val.Namespace))
		h.Write([]byte(val.Value))
	}
	return h.Sum32()
}
