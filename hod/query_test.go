package hod

import (
	"context"
	"fmt"
	logpb "git.sr.ht/~gabe/hod/proto"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"io/ioutil"
	"os"
	"testing"
)

var example_graph_test_cases = []struct {
	query   string
	results [][]*logpb.URI
}{
	{
		"SELECT ?x FROM test WHERE { ?x rdf:type brick:Room };",
		[][]*logpb.URI{{stringtoURI("http://buildsys.org/ontologies/building_example#room_1")}},
	},
	{
		"SELECT ?x FROM test WHERE { bldg:room_1 rdf:type ?x };",
		[][]*logpb.URI{{stringtoURI("https://brickschema.org/schema/1.0.3/Brick#Room")}},
	},
	{
		"SELECT ?x FROM test WHERE { bldg:room_1 ?x brick:Room };",
		[][]*logpb.URI{{stringtoURI("http://www.w3.org/1999/02/22-rdf-syntax-ns#type")}},
	},
	{
		"SELECT ?x ?y FROM test WHERE { ?x bf:feeds ?y };",
		[][]*logpb.URI{
			{stringtoURI("http://buildsys.org/ontologies/building_example#vav_1"), stringtoURI("http://buildsys.org/ontologies/building_example#hvaczone_1")},
			{stringtoURI("http://buildsys.org/ontologies/building_example#ahu_1"), stringtoURI("http://buildsys.org/ontologies/building_example#vav_1")},
		},
	},
	{
		"SELECT ?x ?y FROM test WHERE { bldg:room_1 ?x ?y };",
		[][]*logpb.URI{
			{stringtoURI("https://brickschema.org/schema/1.0.3/BrickFrame#isPartOf"), stringtoURI("http://buildsys.org/ontologies/building_example#floor_1")},
			{stringtoURI("https://brickschema.org/schema/1.0.3/BrickFrame#isPartOf"), stringtoURI("http://buildsys.org/ontologies/building_example#hvaczone_1")},
			{stringtoURI("http://www.w3.org/1999/02/22-rdf-syntax-ns#type"), stringtoURI("https://brickschema.org/schema/1.0.3/Brick#Room")},
			{stringtoURI("http://www.w3.org/2000/01/rdf-schema#label"), &logpb.URI{Value: "Room 1"}},
		},
	},
	{
		"SELECT ?x ?y FROM test WHERE { ?r rdf:type brick:Room . ?r ?x ?y };",
		[][]*logpb.URI{
			{stringtoURI("https://brickschema.org/schema/1.0.3/BrickFrame#isPartOf"), stringtoURI("http://buildsys.org/ontologies/building_example#floor_1")},
			{stringtoURI("https://brickschema.org/schema/1.0.3/BrickFrame#isPartOf"), stringtoURI("http://buildsys.org/ontologies/building_example#hvaczone_1")},
			{stringtoURI("http://www.w3.org/1999/02/22-rdf-syntax-ns#type"), stringtoURI("https://brickschema.org/schema/1.0.3/Brick#Room")},
			{stringtoURI("http://www.w3.org/2000/01/rdf-schema#label"), &logpb.URI{Value: "Room 1"}},
		},
	},
	{
		"SELECT ?x ?y FROM test WHERE { ?r rdf:type brick:Room . ?x ?y ?r };",
		[][]*logpb.URI{
			{stringtoURI("https://brickschema.org/schema/1.0.3/BrickFrame#hasPart"), stringtoURI("http://buildsys.org/ontologies/building_example#floor_1")},
			{stringtoURI("https://brickschema.org/schema/1.0.3/BrickFrame#hasPart"), stringtoURI("http://buildsys.org/ontologies/building_example#hvaczone_1")},
		},
	},
	////		{
	////			"SELECT ?x ?y WHERE { bldg:room_1 ?p bldg:floor_1 . ?x ?p ?y };",
	////			[]ResultMap{
	////				{"?y": stringtoURI("http://buildsys.org/ontologies/BrickFrame#hasPart"), "?x": stringtoURI("http://buildsys.org/ontologies/building_example#floor_1")},
	////				{"?y": stringtoURI("http://buildsys.org/ontologies/BrickFrame#hasPart"), "?x": stringtoURI("http://buildsys.org/ontologies/building_example#hvaczone_1")},
	////			},
	////		},
	{
		"SELECT ?x FROM test WHERE { ?x rdf:type <https://brickschema.org/schema/1.0.3/Brick#Room> };",
		[][]*logpb.URI{{stringtoURI("http://buildsys.org/ontologies/building_example#room_1")}},
	},
	{
		"SELECT ?x FROM test WHERE { ?ahu rdf:type brick:AHU . ?ahu bf:feeds ?x };",
		[][]*logpb.URI{{stringtoURI("http://buildsys.org/ontologies/building_example#vav_1")}},
	},
	{
		"SELECT ?x FROM test WHERE { ?ahu rdf:type brick:AHU . ?ahu bf:feeds+ ?x };",
		[][]*logpb.URI{{stringtoURI("http://buildsys.org/ontologies/building_example#hvaczone_1")}, {stringtoURI("http://buildsys.org/ontologies/building_example#vav_1")}},
	},
	{
		"SELECT ?x FROM test WHERE { ?ahu rdf:type brick:AHU . ?x bf:isFedBy+ ?ahu };",
		[][]*logpb.URI{{stringtoURI("http://buildsys.org/ontologies/building_example#hvaczone_1")}, {stringtoURI("http://buildsys.org/ontologies/building_example#vav_1")}},
	},
	{
		"SELECT ?x FROM test WHERE { ?ahu rdf:type brick:AHU . ?ahu bf:feeds/bf:feeds ?x };",
		[][]*logpb.URI{{stringtoURI("http://buildsys.org/ontologies/building_example#hvaczone_1")}},
	},
	{
		"SELECT ?x FROM test WHERE { ?ahu rdf:type brick:AHU . ?ahu bf:feeds/bf:feeds+ ?x };",
		[][]*logpb.URI{{stringtoURI("http://buildsys.org/ontologies/building_example#hvaczone_1")}},
	},
	{
		"SELECT ?x FROM test WHERE { ?ahu rdf:type brick:AHU . ?ahu bf:feeds/bf:feeds? ?x };",
		[][]*logpb.URI{{stringtoURI("http://buildsys.org/ontologies/building_example#hvaczone_1")}, {stringtoURI("http://buildsys.org/ontologies/building_example#vav_1")}},
	},
	{
		"SELECT ?x FROM test WHERE { ?ahu rdf:type brick:AHU . ?x bf:isFedBy/bf:isFedBy? ?ahu };",
		[][]*logpb.URI{{stringtoURI("http://buildsys.org/ontologies/building_example#hvaczone_1")}, {stringtoURI("http://buildsys.org/ontologies/building_example#vav_1")}},
	},
	{
		"SELECT ?x FROM test WHERE { ?ahu rdf:type brick:AHU . ?ahu bf:feeds* ?x };",
		[][]*logpb.URI{{stringtoURI("http://buildsys.org/ontologies/building_example#hvaczone_1")}, {stringtoURI("http://buildsys.org/ontologies/building_example#vav_1")}, {stringtoURI("http://buildsys.org/ontologies/building_example#ahu_1")}},
	},
	{
		"SELECT ?x FROM test WHERE { ?ahu rdf:type brick:AHU . ?x bf:isFedBy* ?ahu };",
		[][]*logpb.URI{{stringtoURI("http://buildsys.org/ontologies/building_example#hvaczone_1")}, {stringtoURI("http://buildsys.org/ontologies/building_example#vav_1")}, {stringtoURI("http://buildsys.org/ontologies/building_example#ahu_1")}},
	},
	{
		"SELECT ?vav ?room FROM test WHERE { ?vav rdf:type brick:VAV . ?room rdf:type brick:Room . ?zone rdf:type brick:HVAC_Zone . ?vav bf:feeds+ ?zone . ?room bf:isPartOf ?zone }; ",
		[][]*logpb.URI{{stringtoURI("http://buildsys.org/ontologies/building_example#room_1"), stringtoURI("http://buildsys.org/ontologies/building_example#vav_1")}},
	},
	{
		"SELECT ?sensor FROM test WHERE { ?sensor rdf:type/rdfs:subClassOf* brick:Zone_Temperature_Sensor };",
		[][]*logpb.URI{{stringtoURI("http://buildsys.org/ontologies/building_example#ztemp_1")}},
	},
	{
		"SELECT ?sensor FROM test WHERE { ?sensor rdf:type/rdfs:subClassOf* brick:Temperature_Sensor };",
		[][]*logpb.URI{{stringtoURI("http://buildsys.org/ontologies/building_example#ztemp_1")}},
	},
	{
		"SELECT ?s ?p FROM test WHERE { ?s ?p brick:Zone_Temperature_Sensor . ?s rdfs:subClassOf brick:Zone_Temperature_Sensor };",
		[][]*logpb.URI{
			{stringtoURI("https://brickschema.org/schema/1.0.3/Brick#Average_Zone_Temperature_Sensor"), stringtoURI("http://www.w3.org/2000/01/rdf-schema#subClassOf")},
			{stringtoURI("https://brickschema.org/schema/1.0.3/Brick#Coldest_Zone_Temperature_Sensor"), stringtoURI("http://www.w3.org/2000/01/rdf-schema#subClassOf")},
			{stringtoURI("https://brickschema.org/schema/1.0.3/Brick#Highest_Zone_Temperature_Sensor"), stringtoURI("http://www.w3.org/2000/01/rdf-schema#subClassOf")},
			{stringtoURI("https://brickschema.org/schema/1.0.3/Brick#Lowest_Zone_Temperature_Sensor"), stringtoURI("http://www.w3.org/2000/01/rdf-schema#subClassOf")},
			{stringtoURI("https://brickschema.org/schema/1.0.3/Brick#Warmest_Zone_Temperature_Sensor"), stringtoURI("http://www.w3.org/2000/01/rdf-schema#subClassOf")},
			{stringtoURI("https://brickschema.org/schema/1.0.3/Brick#VAV_Zone_Temperature_Sensor"), stringtoURI("http://www.w3.org/2000/01/rdf-schema#subClassOf")},
			{stringtoURI("https://brickschema.org/schema/1.0.3/Brick#AHU_Zone_Temperature_Sensor"), stringtoURI("http://www.w3.org/2000/01/rdf-schema#subClassOf")},
			{stringtoURI("https://brickschema.org/schema/1.0.3/Brick#FCU_Zone_Temperature_Sensor"), stringtoURI("http://www.w3.org/2000/01/rdf-schema#subClassOf")},
			{stringtoURI("https://brickschema.org/schema/1.0.3/Brick#Zone_Air_Temperature_Sensor"), stringtoURI("http://www.w3.org/2000/01/rdf-schema#subClassOf")},
		},
	},
}

var berkeley_graph_test_cases = []struct {
	query       string
	resultCount int
}{
	{
		"COUNT ?x FROM soda WHERE { ?x rdf:type brick:Room };",
		243,
	},
	{
		"COUNT ?x FROM soda WHERE { ?ahu rdf:type brick:AHU . ?ahu bf:feeds ?x };",
		241,
	},
	{
		"COUNT ?x FROM soda WHERE { ?ahu rdf:type brick:AHU . ?ahu bf:feeds+ ?x };",
		482,
	},
	{
		"COUNT ?x FROM soda WHERE { ?ahu rdf:type brick:AHU . ?x bf:isFedBy+ ?ahu };",
		482,
	},
	{
		"COUNT ?x FROM soda WHERE { ?ahu rdf:type brick:AHU . ?ahu bf:feeds/bf:feeds ?x };",
		241,
	},
	{
		"COUNT ?x FROM soda WHERE { ?ahu rdf:type brick:AHU . ?ahu bf:feeds/bf:feeds+ ?x };",
		241,
	},
	{
		"COUNT ?x FROM soda WHERE { ?ahu rdf:type brick:AHU . ?ahu bf:feeds/bf:feeds? ?x };",
		482,
	},
	{
		"COUNT ?x FROM soda WHERE { ?ahu rdf:type brick:AHU . ?x bf:isFedBy/bf:isFedBy? ?ahu };",
		482,
	},
	{
		"COUNT ?x FROM soda WHERE { ?ahu rdf:type brick:AHU . ?ahu bf:feeds* ?x };",
		487,
	},
	{
		"COUNT ?x FROM soda WHERE { ?ahu rdf:type brick:AHU . ?x bf:isFedBy* ?ahu };",
		487,
	},
	{
		"COUNT ?vav ?room FROM soda WHERE { ?vav rdf:type brick:VAV . ?room rdf:type brick:Room . ?zone rdf:type brick:HVAC_Zone . ?vav bf:feeds+ ?zone . ?room bf:isPartOf ?zone }; ",
		243,
	},
	{
		"COUNT ?sensor FROM soda WHERE { ?sensor rdf:type/rdfs:subClassOf* brick:Zone_Temperature_Sensor };",
		232,
	},
	{
		"COUNT ?sensor ?room FROM soda WHERE { ?sensor rdf:type/rdfs:subClassOf* brick:Zone_Temperature_Sensor . ?room rdf:type brick:Room . ?vav rdf:type brick:VAV . ?zone rdf:type brick:HVAC_Zone . ?vav bf:feeds+ ?zone . ?zone bf:hasPart ?room . ?sensor bf:isPointOf ?vav };",
		232,
	},
	//{
	//	"COUNT ?sensor ?room FROM soda WHERE { ?sensor rdf:type/rdfs:subClassOf* brick:Zone_Temperature_Sensor . ?vav rdf:type brick:VAV . ?zone rdf:type brick:HVAC_Zone . ?room rdf:type brick:Room . ?vav bf:feeds+ ?zone . ?zone bf:hasPart ?room  { ?sensor bf:isPointOf ?vav } UNION { ?sensor bf:isPointOf ?room } };",
	//	232,
	//},
	{
		"COUNT ?sensor ?room FROM soda WHERE { ?sensor rdf:type/rdfs:subClassOf* brick:Zone_Temperature_Sensor . ?room rdf:type brick:Room . ?vav rdf:type brick:VAV . ?zone rdf:type brick:HVAC_Zone . ?vav bf:feeds+ ?zone . ?zone bf:hasPart ?room . ?sensor bf:isPointOf ?room };",
		0,
	},
	{
		"COUNT ?vav ?x ?y FROM soda WHERE { ?vav rdf:type brick:VAV . ?vav bf:hasPoint ?x . ?vav bf:isFedBy ?y };",
		823,
	},
	{
		"COUNT ?ahu FROM soda WHERE { ?ahu rdf:type brick:AHU . ?ahu bf:feeds soda_hall:vav_C711 };",
		1,
	},
	{
		"COUNT ?ahu FROM soda WHERE { ?ahu bf:feeds soda_hall:vav_C711 . ?ahu rdf:type brick:AHU };",
		1,
	},
	{
		"COUNT ?vav ?x ?y ?z FROM soda WHERE { ?vav rdf:type brick:VAV . ?vav bf:feeds+ ?x . ?vav bf:isFedBy+ ?y . ?vav bf:hasPoint+ ?z };",
		823,
	},
}

func TestQueryExample(t *testing.T) {
	require := require.New(t)
	assert := assert.New(t)

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

	L, err := NewLog(cfg)
	require.NoError(err, "open log")
	require.NotNil(L, "log")
	defer L.Close()

	version, err := L.LoadFile("test", "BrickFrame.ttl", "bf")
	require.NoError(err, "load brickframe")
	version, err = L.LoadFile("test", "Brick.ttl", "brick")
	require.NoError(err, "load brick")
	version, err = L.LoadFile("test", "example.ttl", "ex")
	require.NoError(err, "load file")

	c, err := L.CreateCursor("test", 0, version)
	require.NoError(err, "creat cursor")

	for _, test := range example_graph_test_cases {
		q, err := L.ParseQuery(test.query, version)
		require.NoError(err)
		require.NotNil(q)
		resp, err := L.Select(context.Background(), q)
		require.NoError(err)
		require.NotNil(resp)
		if !assert.Equal(len(test.results), int(resp.Count), test.query) {
			c.dumpResponse(resp)
		}
		//assert.Equal(len(test.results), len(resp.Rows), test.query)
	}
}

func TestQueryBerkeley(t *testing.T) {
	require := require.New(t)
	assert := assert.New(t)

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

	L, err := NewLog(cfg)
	require.NoError(err, "open log")
	require.NotNil(L, "log")
	defer L.Close()

	version, err := L.LoadFile("soda", "BrickFrame.ttl", "bf")
	require.NoError(err, "load brickframe")
	version, err = L.LoadFile("soda", "Brick.ttl", "brick")
	require.NoError(err, "load brick")
	version, err = L.LoadFile("soda", "berkeley.ttl", "berk")
	require.NoError(err, "load file")

	c, err := L.CreateCursor("soda", 0, version)
	require.NoError(err, "cursor")

	for _, test := range berkeley_graph_test_cases {
		fmt.Println(test.query)
		q, err := L.ParseQuery(test.query, version)
		require.NoError(err)
		require.NotNil(q)
		resp, err := L.Select(context.Background(), q)
		require.NoError(err)
		require.NotNil(resp)
		if !assert.Equal(test.resultCount, int(resp.Count), test.query) {
			c.dumpResponse(resp)
		}
		assert.Equal(test.resultCount, len(resp.Rows), test.query)

	}
}

func BenchmarkQueryPerformance1(b *testing.B) {
	require := require.New(b)
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

	L, err := NewLog(cfg)
	require.NoError(err, "open log")
	require.NotNil(L, "log")
	defer L.Close()

	version, err := L.LoadFile("test", "BrickFrame.ttl", "bf")
	require.NoError(err, "load brickframe")
	version, err = L.LoadFile("test", "Brick.ttl", "brick")
	require.NoError(err, "load brick")
	version, err = L.LoadFile("test", "example.ttl", "ex")
	require.NoError(err, "load file")
	_, err = L.CreateCursor("test", 0, version)
	require.NoError(err, "cursor")

	b.ResetTimer()

	for _, test := range example_graph_test_cases {
		b.Run(test.query, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				q, err := L.ParseQuery(test.query, version)
				require.NoError(err)
				require.NotNil(q)
				_, err = L.Select(context.Background(), q)
				require.NoError(err)
			}
		})
	}
}

func BenchmarkQueryPerformanceBerkeley1(b *testing.B) {
	require := require.New(b)
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
	L, err := NewLog(cfg)
	require.NoError(err, "open log")
	require.NotNil(L, "log")
	defer L.Close()

	version, err := L.LoadFile("soda", "Brick.ttl", "br")
	require.NoError(err, "load brick")
	version, err = L.LoadFile("soda", "BrickFrame.ttl", "bf")
	require.NoError(err, "load brickframe")
	version, err = L.LoadFile("soda", "berkeley.ttl", "berk")
	require.NoError(err, "load file")

	_, err = L.CreateCursor("soda", 0, version)
	require.NoError(err, "cursor")

	b.ResetTimer()

	for _, test := range berkeley_graph_test_cases {
		b.Run(test.query, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				q, err := L.ParseQuery(test.query, version)
				require.NoError(err)
				require.NotNil(q)
				_, err = L.Select(context.Background(), q)
				require.NoError(err)
			}
		})
	}
}
