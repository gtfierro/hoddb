package loader

import (
	"encoding/binary"
	"sync"

	logpb "git.sr.ht/~gabe/hod/proto"
	"git.sr.ht/~gabe/hod/turtle"
	"github.com/dgraph-io/badger"
	"github.com/golang/protobuf/proto"
	"github.com/zhangxinngang/murmur"
)

type Cursor struct {
	hod              *HodDB
	graphname        string
	key              EntityKey
	variablePosition map[string]int
	cache            map[EntityKey]*Entity
	selectVars       []string
	rel              *relation
	plan             *queryPlan
	sync.RWMutex
}

func hashString(s string) []byte {
	var dest = make([]byte, 4)
	binary.BigEndian.PutUint32(dest, murmur.Murmur3([]byte(s)))
	return dest
}

func (hod *HodDB) Cursor(graph string, plan *queryPlan) *Cursor {
	c := &Cursor{
		graphname:        graph,
		hod:              hod,
		variablePosition: make(map[string]int),
		cache:            make(map[EntityKey]*Entity),
		plan:             plan,
	}
	copy(c.key.Graph[:], hashString(graph))
	c.addQueryPlan(plan)
	return c
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
func (c *Cursor) getEntity(key EntityKey) (*Entity, error) {
	c.RLock()
	if entity, found := c.cache[key]; found {
		c.RUnlock()
		return entity, nil
	}
	c.RUnlock()

	entity, err := c.hod.GetEntity(key)
	if err != nil {
		return nil, err
	}
	c.Lock()
	c.cache[key] = entity
	c.Unlock()
	return entity, nil
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

func (c *Cursor) ContextualizeURI(u *logpb.URI) EntityKey {
	//var key EntityKey
	key := c.hod.hashURI(turtle.URI{u.Namespace, u.Value})
	//copy(key.Hash[:], c.hod.hashURI(u))
	copy(key.Graph[:], c.key.Graph[:])
	copy(key.Version[:], c.key.Version[:])
	return key
}

func (c *Cursor) Iterate(f func(EntityKey, *Entity) bool) error {
	opt := badger.DefaultIteratorOptions
	opt.PrefetchSize = 10
	err := c.hod.db.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(opt)
		defer it.Close()
		prefix := c.key.Graph[:]
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			item := it.Item()
			key := EntityKeyFromBytes(item.Key())

			var entity = &Entity{
				compiled: new(logpb.Entity),
				key:      key,
			}
			err := item.Value(func(b []byte) error {
				return proto.Unmarshal(b, entity.compiled)
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
