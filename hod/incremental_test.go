package hod

import (
	"fmt"
	"io/ioutil"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestIncremental1(t *testing.T) {
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

	hod, err := MakeHodDB(cfg)
	require.NoError(err, "open log")
	require.NotNil(hod, "log")

	// load triples from files
	load_file := func(filename string) {
		dataset, err := LoadTriplesFromFile(filename)
		require.NoError(err, "Load "+filename)
		err = hod.expand(dataset)
		require.NoError(err, "expand "+filename)
		err = hod.AddTriples(dataset)
		require.NoError(err, "Add Dataset")
	}
	load_file("BrickFrame.ttl")
	load_file("Brick.ttl")

	// infer rules
	err = hod.inferRules()
	require.NoError(err, "Infer rule")
	require.Equal(24, len(hod.rules), "number of hod rules")

	load_file("example.ttl")

	q1 := "SELECT ?x WHERE { ?x rdf:type brick:Room };"
	rows, err := hod.run_query(q1)
	require.NoError(err, q1)
	require.Equal(1, len(rows), q1)

	q2 := "SELECT ?x ?y WHERE { ?x bf:feeds ?y};"
	rows, err = hod.run_query(q2)
	require.NoError(err, q2)
	require.Equal(2, len(rows), q2)

	q3 := "SELECT ?x ?y WHERE { ?x bf:isFedBy ?y};"
	rows, err = hod.run_query(q3)
	require.NoError(err, q3)
	require.Equal(2, len(rows), q3)

	q4 := "SELECT ?x ?y WHERE { ?x bf:feeds+ ?y};"
	rows, err = hod.run_query(q4)
	require.NoError(err, q4)
	require.Equal(2, len(rows), q4)

}
