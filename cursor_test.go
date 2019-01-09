package main

import (
	"fmt"
	logpb "git.sr.ht/~gabe/hod/proto"
	"github.com/stretchr/testify/require"
	"io/ioutil"
	"os"
	"testing"
)

func TestCursor(t *testing.T) {
	require := require.New(t)

	dir, err := ioutil.TempDir("", "_log_test_")
	require.NoError(err)
	defer os.RemoveAll(dir) // clean up

	cfgStr := fmt.Sprintf(`
database:
    path: %s
    `, dir)
	cfg, err := ReadConfigFromString(cfgStr)
	require.NoError(err, "read config")
	require.NotNil(cfg, "config")

	log, err := NewLog(cfg)
	require.NoError(err, "open log")
	require.NotNil(log, "log")
	defer log.Close()

	plan := &queryPlan{
		selectVars: []string{"var1"},
		variables:  []string{"var1", "var2"},
	}
	cursor := log.Cursor("graph", int64(123456789), plan)
	require.NotNil(cursor, "cursor")

	// test contextualizeuri
	uri := &logpb.URI{Namespace: "https://brickschema.org/schema/1.0.3/Brick#", Value: "Room"}
	key := cursor.ContextualizeURI(uri)
	require.Equal(key.Graph[:], hashString("graph"))
	require.Equal(key.Hash[:], hashURI(uri))
	require.Equal(key.Version[:], uint64ToBytesLE(123456789))

	// test sameversion
	// test iterate?

	// test addorjoin
	require.False(cursor.hasValuesFor("var1"))
	require.False(cursor.hasValuesFor("var2"))
	entities := generateEntitySet(5, 1, 1)
	cursor.addOrJoin("var1", entities)
	require.Equal(len(cursor.rel.rows), 5, "rows")
	require.True(cursor.hasValuesFor("var1"))
	require.False(cursor.hasValuesFor("var2"))

	values := cursor.getValuesFor("var1")
	require.Equal(values, entities)
	values = cursor.getValuesFor("var2")
	require.Equal(0, len(values))

	newentities := generateEntityRows(2, 5, 1, 1)
	rel2 := newRelation([]string{"var1", "var2"})
	rel2.add2Values("var1", "var2", newentities)
	//for _, r := range cursor.rel.rows {
	//	fmt.Println(*r)
	//}
	cursor.join(rel2, []string{"var1"})
	require.Equal(3, len(cursor.rel.rows))
}
