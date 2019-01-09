package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"github.com/dgraph-io/badger"
	"github.com/golang/protobuf/proto"
	logpb "git.sr.ht/~gabe/hod/proto"
	"strings"
	"sync"
)

type Cursor struct {
	graphname string
	timestamp int64
	key       EntityKey
	L         *Log

	// from old query context

	// maps variable name to a position in a row
	variablePosition map[string]int
	cache            map[EntityKey]*Entity
	selectVars       []string
	rel              *relation
	plan             *queryPlan
	sync.RWMutex
}

func (L *Log) Cursor(graph string, timestamp int64, plan *queryPlan) *Cursor {
	c := &Cursor{
		graphname: graph,
		timestamp: timestamp,
		L:         L,

		variablePosition: make(map[string]int),
		cache:            make(map[EntityKey]*Entity),
		plan:             plan,
	}
	copy(c.key.Graph[:], hashString(graph))
	c.addQueryPlan(plan)
	//tsn := timestampNowBytes()
	//copy(c.key.Version[:], tsn[:])
	//copy(c.key.Version[:], uint64ToBytes(uint64(timestamp)))
	binary.LittleEndian.PutUint64(c.key.Version[:], uint64(timestamp))
	return c
}

func (c *Cursor) dropCache() {
	c.Lock()
	defer c.Unlock()
	for k := range c.cache {
		delete(c.cache, k)
	}
}

func (c *Cursor) getEntity(key EntityKey) (*Entity, error) {
	c.RLock()
	if entity, found := c.cache[key]; found {
		c.RUnlock()
		return entity, nil
	}
	c.RUnlock()

	entity, err := c.L.GetEntity(key)
	if err != nil {
		return nil, err
	}
	c.Lock()
	c.cache[key] = entity
	c.Unlock()
	return entity, nil
}

func (c *Cursor) addQueryPlan(plan *queryPlan) {
	if plan != nil {
		c.selectVars = plan.selectVars
		c.rel = newRelation(plan.variables)
		for pos, varname := range plan.variables {
			c.variablePosition[varname] = pos
		}
	}
}

func (c *Cursor) ContextualizeURI(u *logpb.URI) EntityKey {
	var key EntityKey
	copy(key.Hash[:], hashURI(u))
	copy(key.Graph[:], c.key.Graph[:])
	copy(key.Version[:], c.key.Version[:])
	return key
}

func (c *Cursor) SameVersion(key EntityKey) bool {
	return bytes.Equal(key.Version[:], c.key.Version[:])
}

func (c *Cursor) Iterate(f func(EntityKey, *Entity) bool) error {
	opt := badger.DefaultIteratorOptions
	opt.PrefetchSize = 10
	err := c.L.log.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(opt)
		defer it.Close()
		prefix := c.key.Graph[:]
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			item := it.Item()
			key := EntityKeyFromBytes(item.Key())
			if !c.SameVersion(key) {
				continue
			}

			var entity = &Entity{
				e:   new(logpb.Entity),
				key: key,
			}
			err := item.Value(func(b []byte) error {
				return proto.Unmarshal(b, entity.e)
			})
			if err != nil {
				return err
			}
			if f(entity.key, entity) {
				break
			}
		}
		return nil
	})
	return err
}

func (c *Cursor) addOrJoin(varname string, values entityset) {
	if c.hasValuesFor(varname) {
		newrel := newRelation([]string{varname})
		newrel.add1Value(varname, values)
		c.rel.join(newrel, []string{varname}, c)
	} else {
		c.rel.add1Value(varname, values)
	}

}

func (c *Cursor) hasValuesFor(varname string) bool {
	existingvalues, found := c.rel.multiindex[varname]
	return found && len(existingvalues) > 0
}

func (c *Cursor) getValuesFor(varname string) entityset {
	set := newEntitySet()
	// return 0 if no values
	if !c.hasValuesFor(varname) {
		return set
	}
	pos := c.variablePosition[varname]
	for _, row := range c.rel.rows {
		val := row.valueAt(pos)
		if !val.Empty() {
			set.add(val)
		}
	}
	return set
}

func (c *Cursor) join(other *relation, on []string) {
	c.rel.join(other, on, c)
}

func (c *Cursor) GetRowsWithVar(mandatory []string) (returnRows []*logpb.Row) {
	var seen = make(map[uint32]struct{})
	fmt.Println("dumping rows with vars ", mandatory)
rows:
	for idx, row := range c.rel.rows {
		var addRow = new(logpb.Row)
		for idx2, varname := range mandatory {
			key := row.valueAt(c.variablePosition[varname])
			if key.Empty() {
				continue rows
			}
			s.RLock()
			val := LOOKUPURI[key.Hash]
			s.RUnlock()
			fmt.Println("> ", idx, "| ", val.String(), " (", idx2, ") @ ", key.Timestamp())
			addRow.Values = append(addRow.Values, &val)
		}
		h := hashRow2(addRow)
		if _, found := seen[h]; !found {
			returnRows = append(returnRows, addRow)
			seen[h] = struct{}{}
		}
	}
	return
}

func (c *Cursor) dumpResponse(resp *logpb.Response) {
	var dmp strings.Builder
	fmt.Println("DUMP ", resp.Variables)
	var selectVars = resp.Variables //selectVars
	rowlens := make(map[string]int, len(selectVars))

	if len(resp.Rows) == 0 {
		fmt.Println("Version: ", resp.Version)
		fmt.Println("Count: ", resp.Count)
		return
	}

	for _, varname := range selectVars {
		rowlens[varname] = len(varname)
	}
	for _, row := range resp.Rows {
		for _, varname := range selectVars {
			idx := c.variablePosition[varname]
			uri := row.Values[idx].Namespace + "#" + row.Values[idx].Value
			if rowlens[varname] < len(uri) {
				rowlens[varname] = len(uri)
			}
		}
	}

	totallen := 0
	for _, length := range rowlens {
		totallen += length + 2
	}
	fmt.Fprintf(&dmp, "+%s+\n", strings.Repeat("-", totallen+len(rowlens)-1))
	// header
	fmt.Fprintf(&dmp, "|")
	for _, varname := range selectVars {
		fmt.Fprintf(&dmp, " %s%s|", varname, strings.Repeat(" ", rowlens[varname]-len(varname)+1))
	}
	fmt.Fprintf(&dmp, "\n")
	fmt.Fprintf(&dmp, "+%s+\n", strings.Repeat("-", totallen+len(rowlens)-1))

	for _, row := range resp.Rows {
		fmt.Fprintf(&dmp, "|")
		for _, varname := range selectVars {
			idx := c.variablePosition[varname]
			uri := row.Values[idx].Namespace + "#" + row.Values[idx].Value
			valuelen := len(uri)
			fmt.Fprintf(&dmp, " %s%s |", uri, strings.Repeat(" ", rowlens[varname]-valuelen))
		}
		fmt.Fprintf(&dmp, "\n")
	}
	fmt.Fprintf(&dmp, "+%s+\n", strings.Repeat("-", totallen+len(rowlens)-1))
	fmt.Println(dmp.String())

	if resp.Error != "" {
		fmt.Println("Error: ", resp.Error)
	}
	fmt.Println("Version: ", resp.Version)
	fmt.Println("Count: ", resp.Count)
}

func (c *Cursor) dump() {
	fmt.Println("---")
	for _, row := range c.rel.rows {
		fmt.Println(*row)
	}
}

func (c *Cursor) dumpTil(numVars int) {
	fmt.Println("---start")
	for _, row := range c.rel.rows {
		var v []string
		for i := 0; i < numVars; i++ {
			v = append(v, uriToS(LOOKUPURI[row.valueAt(i).Hash]))
		}
		fmt.Println(">", v)
	}
	fmt.Println("---end")
}

func (c *Cursor) dumpRelTil(rel *relation, numVars int) {
	fmt.Println("---start")
	for _, row := range rel.rows {
		var v []string
		for i := 0; i < numVars; i++ {
			v = append(v, uriToS(LOOKUPURI[row.valueAt(i).Hash]))
		}
		fmt.Println(">", v)
	}
	fmt.Println("---end")
}

// generates log entries from the contents of the cursor relation and
// the triples templates given to us
func (c *Cursor) generateTriples(vars []string, triples []*logpb.Triple) chan *logpb.LogEntry {
	generated := make(chan *logpb.LogEntry)
	go func() {
		for _, row := range c.rel.rows {

			for _, triple := range triples {
				var entry = new(logpb.LogEntry)
				var newtriple = new(logpb.Triple)
				if isVariable(triple.Subject) {
					key := row.valueAt(c.variablePosition[triple.Subject.Value])
					s.RLock()
					val := LOOKUPURI[key.Hash]
					s.RUnlock()
					newtriple.Subject = &val
				} else {
					newtriple.Subject = triple.Subject
				}

				if isVariable(triple.Predicate[0]) {
					key := row.valueAt(c.variablePosition[triple.Predicate[0].Value])
					s.RLock()
					val := LOOKUPURI[key.Hash]
					s.RUnlock()
					newtriple.Predicate[0] = &val
				} else {
					newtriple.Predicate = triple.Predicate
				}

				if isVariable(triple.Object) {
					key := row.valueAt(c.variablePosition[triple.Object.Value])
					s.RLock()
					val := LOOKUPURI[key.Hash]
					s.RUnlock()
					newtriple.Object = &val
				} else {
					newtriple.Object = triple.Object
				}
				entry.Triple = newtriple
				entry.Graph = c.graphname
				entry.Timestamp = c.timestamp
				generated <- entry
			}
		}
		close(generated)
	}()
	return generated
}

// defined
// defineVariable
// add1Value
// uniondefinitions
// rel.join
// hasJoined
// getValuesForVariable
// restrictToResolved
// cardinatityUnique
// markJoined
// validValue
// ctx.getValuesFromRelation
