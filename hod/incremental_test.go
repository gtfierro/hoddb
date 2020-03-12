package hod

import (
	"fmt"
	"io/ioutil"
	"os"
	"testing"

	turtle "github.com/gtfierro/hoddb/turtle"
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
		changed, err := hod.AddTriplesWithChanged("test2", dataset)
		require.NoError(err, "expand "+filename)
		require.True(changed, "adding triples updated")
	}
	load_file("BrickFrame.ttl")
	load_file("Brick.ttl")

	// infer rules
	err = hod.inferRules("test2")
	require.NoError(err, "Infer rule")
	require.Equal(36, len(hod.rules), "number of hod rules")

	load_file("example.ttl")

	q1 := "SELECT ?x WHERE { ?x rdf:type brick:Room }"
	rows, err := hod.run_query("test2", q1)
	require.NoError(err, q1)
	require.Equal(1, len(rows), q1)

	q2 := "SELECT ?x ?y WHERE { ?x bf:feeds ?y}"
	rows, err = hod.run_query("test2", q2)
	require.NoError(err, q2)
	require.Equal(2, len(rows), q2)

	q3 := "SELECT ?x ?y WHERE { ?x bf:isFedBy ?y}"
	rows, err = hod.run_query("test2", q3)
	require.NoError(err, q3)
	require.Equal(2, len(rows), q3)

	q4 := "SELECT ?x ?y WHERE { ?x bf:feeds+ ?y}"
	rows, err = hod.run_query("test2", q4)
	require.NoError(err, q4)
	require.Equal(2, len(rows), q4)

	// add new triples
	newDataset := turtle.DataSet{
		Triples: []turtle.Triple{
			{
				Subject:   turtle.ParseURI("https://buildsys.org/ontologies/building_example#vav_1"),
				Predicate: turtle.ParseURI("http://www.w3.org/1999/02/22-rdf-syntax-ns#type"),
				Object:    turtle.ParseURI("https://brickschema.org/schema/1.0.3/Brick#VAV"),
			},
			{
				Subject:   turtle.ParseURI("https://buildsys.org/ontologies/building_example#ahu_1"),
				Predicate: turtle.ParseURI("https://brickschema.org/schema/1.0.3/BrickFrame#feeds"),
				Object:    turtle.ParseURI("https://buildsys.org/ontologies/building_example#vav_2"),
			},
		},
	}
	changed, err := hod.AddTriplesWithChanged("test2", newDataset)
	require.NoError(err, "expand new triples")
	require.True(changed, "adding triples updated")

	q5 := "SELECT ?x ?y WHERE { ?x bf:feeds ?y}"
	rows, err = hod.run_query("test2", q5)
	require.NoError(err, q5)
	require.Equal(3, len(rows), q5)

	q6 := "SELECT ?x ?y WHERE { ?x bf:isFedBy ?y}"
	rows, err = hod.run_query("test2", q6)
	require.NoError(err, q6)
	require.Equal(3, len(rows), q6)
}
