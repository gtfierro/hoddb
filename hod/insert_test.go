package hod

import (
	"fmt"
	logpb "git.sr.ht/~gabe/hod/proto"
	"github.com/stretchr/testify/require"
	"io/ioutil"
	"os"
	"testing"
)

func TestInsertExample(t *testing.T) {
	require := require.New(t)
	dir, err := ioutil.TempDir("", "_log_test_")
	require.NoError(err)
	defer os.RemoveAll(dir) // clean up

	cfgStr := fmt.Sprintf(`database:
    path: %s    `, dir)
	cfg, err := ReadConfigFromString(cfgStr)
	require.NoError(err, "read config")
	require.NotNil(cfg, "config")

	hod, err := MakeHodDB(cfg)
	require.NoError(err, "open log")
	require.NotNil(hod, "log")
	//defer hod.Close()

	bundle := FileBundle{
		GraphName: "test",
		TTLFile:   "example.ttl",
		//OntologyFiles: []string{"Brick.ttl", "BrickFrame.ttl"},
		OntologyFiles: []string{"BrickFrame.ttl"},
	}
	err = hod.Load(bundle)
	require.NoError(err, "load files")

	cursor, err := hod.Cursor("test")
	require.NoError(err, "create cursor")
	require.NotNil(cursor)
	key := cursor.ContextualizeURI(&logpb.URI{
		Namespace: "http://buildsys.org/ontologies/building_example",
		Value:     "ahu_1",
	})
	require.NotNil(key)

	entity, err := cursor.getEntity(key)
	hod.Dump(entity)
	require.NoError(err)
	require.NotNil(entity)

	edges := entity.GetAllOutEdges()
	require.Equal(2, len(edges))

	edges = entity.GetAllInEdges()
	require.Equal(1, len(edges))

	edges = entity.GetAllOutPlusEdges()
	require.Equal(3, len(edges))

	edges = entity.GetAllInPlusEdges()
	require.Equal(2, len(edges))

	key = cursor.ContextualizeURI(&logpb.URI{
		Namespace: "http://buildsys.org/ontologies/building_example",
		Value:     "vav_1",
	})
	require.NotNil(key)

	entity, err = cursor.getEntity(key)
	//hod.Dump(entity)

	require.NoError(err)
	require.NotNil(entity)

	edges = entity.GetAllOutEdges()
	require.Equal(5, len(edges))

	edges = entity.GetAllInEdges()
	require.Equal(4, len(edges))

	edges = entity.GetAllOutPlusEdges()
	require.Equal(5, len(edges))

	edges = entity.GetAllInPlusEdges()
	require.Equal(4, len(edges))

}
