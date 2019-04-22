package hod

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestRelationAdd1Value(t *testing.T) {
	assert := assert.New(t)

	// test 1 var
	rel1 := newRelation([]string{"var1"})
	assert.NotNil(rel1)
	var1Vals := generateEntitySet(5, 1, 1)
	rel1.add1Value("var1", var1Vals)
	assert.Equal(0, rel1.vars["var1"])
	assert.Equal(len(rel1.rows), 5)
}

func TestRelationAdd2Value(t *testing.T) {
	assert := assert.New(t)

	rel2 := newRelation([]string{"var1", "var2"})
	rel2vals := generateEntityRows(2, 10, 1, 1)
	rel2.add2Values("var1", "var2", rel2vals)
	assert.Equal(0, rel2.vars["var1"])
	assert.Equal(1, rel2.vars["var2"])
	assert.Equal(10, len(rel2.rows))
}

func TestRelationAdd3Value(t *testing.T) {
	assert := assert.New(t)

	rel3 := newRelation([]string{"var1", "var2", "var3"})
	rel3vals := generateEntityRows(3, 10, 1, 1)
	rel3.add3Values("var1", "var2", "var3", rel3vals)
	assert.Equal(0, rel3.vars["var1"])
	assert.Equal(1, rel3.vars["var2"])
	assert.Equal(2, rel3.vars["var3"])
	assert.Equal(10, len(rel3.rows))
}

func TestRelationJoin1Value(t *testing.T) {
	assert := assert.New(t)

	// relation1 (var1)
	rel1 := newRelation([]string{"var1"})
	assert.NotNil(rel1)
	var1Vals := generateEntitySet(5, 1, 1)
	rel1.add1Value("var1", var1Vals)
	assert.Equal(0, rel1.vars["var1"])
	assert.Equal(len(rel1.rows), 5)

	// relation2 (var1, var2)
	rel2 := newRelation([]string{"var1", "var2"})
	rel2vals := generateEntityRows(2, 10, 1, 1)
	rel2.add2Values("var1", "var2", rel2vals)
	assert.Equal(1, rel2.vars["var2"])
	assert.Equal(10, len(rel2.rows))

	// inner join
	rel1.join(rel2, []string{"var1"}, nil)
	assert.Equal(3, len(rel1.rows))
}

func TestRelationJoin1ValueNoIntersection(t *testing.T) {
	assert := assert.New(t)

	// relation1 (var1)
	rel1 := newRelation([]string{"var1"})
	assert.NotNil(rel1)
	var1Vals := generateEntitySet(5, 100, 1)
	rel1.add1Value("var1", var1Vals)
	assert.Equal(0, rel1.vars["var1"])
	assert.Equal(len(rel1.rows), 5)

	// relation2 (var1, var2)
	rel2 := newRelation([]string{"var1", "var2"})
	rel2vals := generateEntityRows(2, 10, 1, 1)
	rel2.add2Values("var1", "var2", rel2vals)
	assert.Equal(1, rel2.vars["var2"])
	assert.Equal(10, len(rel2.rows))

	// inner join
	rel1.join(rel2, []string{"var1"}, nil)
	assert.Equal(0, len(rel1.rows))
}

func BenchmarkRelationAdd1Value(b *testing.B) {
	var1Vals := generateEntitySet(5, 1, 1)
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		rel1 := newRelation([]string{"var1"})
		rel1.add1Value("var1", var1Vals)
	}
}

func BenchmarkRelationAdd2Value(b *testing.B) {
	rel2vals := generateEntityRows(2, 10, 1, 1)
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		rel2 := newRelation([]string{"var1", "var2"})
		rel2.add2Values("var1", "var2", rel2vals)
	}
}

func BenchmarkRelationAdd3Value(b *testing.B) {
	rel3vals := generateEntityRows(3, 10, 1, 1)
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		rel3 := newRelation([]string{"var1", "var2", "var3"})
		rel3.add3Values("var1", "var2", "var3", rel3vals)
	}
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
