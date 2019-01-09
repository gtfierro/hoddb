package main

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

	L, err := NewLog(cfg)
	require.NoError(err, "open log")
	require.NotNil(L, "log")
	defer L.Close()

	//version, err := L.LoadFile("test", "Brick.ttl", "brick")
	//require.NoError(err, "load brick")
	version, err := L.LoadFile("test", "BrickFrame.ttl", "bf")
	require.NoError(err, "load brickframe")
	version, err = L.LoadFile("test", "example.ttl", "bldg")
	require.NoError(err, "load file")

	cursor, err := L.createCursor("test", 0, version)
	//cursor := L.Cursor("test", version, nil)
	require.NoError(err, "create cursor")
	require.NotNil(cursor)
	key := cursor.ContextualizeURI(&logpb.URI{
		Namespace: "http://buildsys.org/ontologies/building_example",
		Value:     "ahu_1",
	})
	require.NotNil(key)

	entity, err := cursor.getEntity(key)
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
	L.Dump(entity)

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
