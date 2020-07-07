package hod

import (
	"fmt"
	"github.com/stretchr/testify/require"
	"io/ioutil"
	"os"
	"testing"
)

var (
	RDF_TYPE       = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type"
	BRICK_ROOM     = "https://brickschema.org/schema/1.1/Brick#Room"
	BRICK_VAV      = "https://brickschema.org/schema/1.1/Brick#VAV"
	BRICK_HVACZONE = "https://brickschema.org/schema/1.1/Brick#HVAC_Zone"
	BRICK_ZNT      = "https://brickschema.org/schema/1.1/Brick#Zone_Temperature_Sensor"
	BF_ISPARTOF    = "https://brickschema.org/schema/1.1/BrickFrame#isPartOf"
	BF_HASPART     = "https://brickschema.org/schema/1.1/BrickFrame#hasPart"
	BF_ISPOINTOF   = "https://brickschema.org/schema/1.1/BrickFrame#isPointOf"
	BF_FEEDS       = "https://brickschema.org/schema/1.1/BrickFrame#feeds"
	BF_ISFEDBY     = "https://brickschema.org/schema/1.1/BrickFrame#isFedBy"

	ROOM_1     = "http://buildsys.org/ontologies/building_example#room_1"
	VAV_1      = "http://buildsys.org/ontologies/building_example#vav_1"
	AHU_1      = "http://buildsys.org/ontologies/building_example#ahu_1"
	FLOOR_1    = "http://buildsys.org/ontologies/building_example#floor_1"
	HVACZONE_1 = "http://buildsys.org/ontologies/building_example#hvaczone_1"
)

func TestOperators(t *testing.T) {
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

	bundle := FileBundle{
		GraphName:     "example",
		TTLFile:       "example.ttl",
		OntologyFiles: []string{"BrickFrame.ttl"},
	}
	err = hod.Load(bundle)
	require.NoError(err, "load files")

	plan := &queryPlan{
		selectVars: []string{"?v1"},
		variables:  []string{"?v1"},
	}
	cursor, err := hod.Cursor("example")
	require.NoError(err, "create cursor")
	require.NotNil(cursor)
	cursor.addQueryPlan(plan)

	// test resolve subject, no prior values
	qt := makeQueryTerm(cursor, makeTriple("?v1", RDF_TYPE, BRICK_ROOM))
	rs := &resolveSubject{term: *qt}
	require.NoError(rs.run(cursor), "run resolve subject")
	require.Equal(1, len(cursor.rel.rows), "num rows")

	// should be idempotent; run again
	require.NoError(rs.run(cursor), "run resolve subject")
	require.Equal(1, len(cursor.rel.rows), "num rows")

	// intersection should be 0 when joining disjoint terms to same var
	qt = makeQueryTerm(cursor, makeTriple("?v1", RDF_TYPE, BRICK_HVACZONE))
	rs = &resolveSubject{term: *qt}
	require.NoError(rs.run(cursor), "run resolve subject")
	require.Equal(0, len(cursor.rel.rows), "num rows")

	// test resolve object, no prior values
	cursor, err = hod.Cursor("example")
	require.NoError(err, "create cursor")
	cursor.addQueryPlan(plan)
	qt = makeQueryTerm(cursor, makeTriple(HVACZONE_1, RDF_TYPE, "?v1"))
	ro := &resolveObject{term: *qt}
	require.NoError(ro.run(cursor), "run resolve object")
	require.Equal(1, len(cursor.rel.rows))

	// should be idempotent
	require.NoError(ro.run(cursor), "run resolve object")
	require.Equal(1, len(cursor.rel.rows))

	// intersection should be 0
	qt = makeQueryTerm(cursor, makeTriple(ROOM_1, RDF_TYPE, "?v1"))
	ro = &resolveObject{term: *qt}
	require.NoError(ro.run(cursor), "run resolve object")
	require.Equal(0, len(cursor.rel.rows))

	// try predicate
	cursor, err = hod.Cursor("example")
	require.NoError(err, "create cursor")
	cursor.addQueryPlan(plan)
	qt = makeQueryTerm(cursor, makeTriple(AHU_1, "?v1", VAV_1))
	rp := &resolvePredicate{term: *qt}
	require.NoError(rp.run(cursor), "run resolve predicate")
	require.Equal(1, len(cursor.rel.rows))
	// idempotent
	require.NoError(rp.run(cursor), "run resolve predicate")
	require.Equal(1, len(cursor.rel.rows))

	// intersection 0
	qt = makeQueryTerm(cursor, makeTriple(HVACZONE_1, "?v1", ROOM_1))
	rp = &resolvePredicate{term: *qt}
	require.NoError(rp.run(cursor), "run resolve predicate")
	require.Equal(0, len(cursor.rel.rows))

	////// restrictSubjectObjectByPredicate
	plan = &queryPlan{
		selectVars: []string{"?v1", "?v2"},
		variables:  []string{"?v1", "?v2"},
	}

	// case 1 (?s = 0, ?o = 0)
	cursor, err = hod.Cursor("example")
	require.NoError(err, "create cursor")
	cursor.addQueryPlan(plan)
	qt = makeQueryTerm(cursor, makeTriple("?v1", RDF_TYPE, "?v2"))
	rso := &restrictSubjectObjectByPredicate{term: *qt}
	require.NoError(rso.run(cursor), "run restrictSubjectObjectByPredicate 1")
	// TODO: this is 62 rather than 6 because we are loading in BrickFrame.ttl
	require.Equal(62, len(cursor.rel.rows))

	cursor, err = hod.Cursor("example")
	require.NoError(err, "create cursor")
	cursor.addQueryPlan(plan)
	qt = makeQueryTerm(cursor, makeTriple("?v1", BF_ISPARTOF, "?v2"))
	rso = &restrictSubjectObjectByPredicate{term: *qt}
	require.NoError(rso.run(cursor), "run restrictSubjectObjectByPredicate 1")
	require.Equal(4, len(cursor.rel.rows))

	// case 2 (?s > 0, ?o = 0)
	cursor, err = hod.Cursor("example")
	require.NoError(err, "create cursor")
	cursor.addQueryPlan(plan)
	// puts ROOM_1 into "?v1
	qt = makeQueryTerm(cursor, makeTriple("?v1", RDF_TYPE, BRICK_ROOM))
	rs = &resolveSubject{term: *qt}
	require.NoError(rs.run(cursor), "define ?v1")
	require.Equal(1, len(cursor.rel.rows))

	// puts HVACZONE_1 into "?v2"
	// this resolves room_1 partof hvaczone_1 and room_1 partof floor_1
	// and floor_1 ispartof building_1
	// the join should keep both rows for room_1
	qt = makeQueryTerm(cursor, makeTriple("?v1", BF_ISPARTOF, "?v2"))
	rso = &restrictSubjectObjectByPredicate{term: *qt}
	require.NoError(rso.run(cursor), "join on ?v1")
	require.Equal(2, len(cursor.rel.rows))

	// case 3 (?s = 0, ?o > 0)
	cursor, err = hod.Cursor("example")
	require.NoError(err, "create cursor")
	cursor.addQueryPlan(plan)
	qt = makeQueryTerm(cursor, makeTriple(ROOM_1, BF_ISPARTOF, "?v1"))
	ro = &resolveObject{term: *qt}
	require.NoError(ro.run(cursor), "define ?v1")
	require.Equal(2, len(cursor.rel.rows))

	// this should return room1 partof floor1 and floor1 partof building
	// we are joining on floor1, so we should get the floor1, building1 row
	qt = makeQueryTerm(cursor, makeTriple("?v1", BF_ISPARTOF, "?v2"))
	rso = &restrictSubjectObjectByPredicate{term: *qt}
	require.NoError(rso.run(cursor), "join on ?v1")
	require.Equal(1, len(cursor.rel.rows))
	require.Equal(cursor.ContextualizeURI(stringtoURI(FLOOR_1)), cursor.rel.rows[0].valueAt(0))

	// case 4 (?s > 0, ?o > 0)
	// start with ispartof and filter for which pairs also have ispointof
	cursor, err = hod.Cursor("example")
	require.NoError(err, "create cursor")
	cursor.addQueryPlan(plan)
	qt = makeQueryTerm(cursor, makeTriple("?v1", BF_ISPARTOF, "?v2"))
	rso = &restrictSubjectObjectByPredicate{term: *qt}
	require.NoError(rso.run(cursor), "run restrictSubjectObjectByPredicate 1")
	require.Equal(4, len(cursor.rel.rows))

	qt = makeQueryTerm(cursor, makeTriple("?v1", BF_ISPOINTOF, "?v2"))
	rso = &restrictSubjectObjectByPredicate{term: *qt}
	require.NoError(rso.run(cursor), "run restrictSubjectObjectByPredicate 1")
	require.Equal(1, len(cursor.rel.rows))

	////// resolveSubjectFromVarObject
	// ?sub pred ?obj, but we have already resolved the object
	cursor, err = hod.Cursor("example")
	require.NoError(err, "create cursor")
	cursor.addQueryPlan(plan)
	qt = makeQueryTerm(cursor, makeTriple(VAV_1, BF_FEEDS, "?v2"))
	ro = &resolveObject{term: *qt}
	require.NoError(ro.run(cursor), "run resolve object")
	require.Equal(1, len(cursor.rel.rows))
	require.Equal(cursor.ContextualizeURI(stringtoURI(HVACZONE_1)), cursor.rel.rows[0].valueAt(1))

	qt = makeQueryTerm(cursor, makeTriple("?v1", BF_ISPARTOF, "?v2"))
	rsvo := &resolveSubjectFromVarObject{term: *qt}
	require.NoError(rsvo.run(cursor), "resolveSubjectFromVarObject")
	require.Equal(1, len(cursor.rel.rows))

	////// resolveObjectFromVarSubject
	// ?sub pred ?obj, but we have already resolved the subject
	cursor, err = hod.Cursor("example")
	require.NoError(err, "create cursor")
	cursor.addQueryPlan(plan)
	qt = makeQueryTerm(cursor, makeTriple("?v1", BF_FEEDS, HVACZONE_1))
	rs = &resolveSubject{term: *qt}
	require.NoError(rs.run(cursor), "run resolve subject")
	require.Equal(1, len(cursor.rel.rows))
	require.Equal(cursor.ContextualizeURI(stringtoURI(VAV_1)), cursor.rel.rows[0].valueAt(0))

	qt = makeQueryTerm(cursor, makeTriple("?v1", RDF_TYPE, "?v2"))
	rovs := &resolveObjectFromVarSubject{term: *qt}
	require.NoError(rovs.run(cursor), "resolveObjectFromVarSubject")
	require.Equal(1, len(cursor.rel.rows))
	require.Equal(cursor.ContextualizeURI(stringtoURI(BRICK_VAV)), cursor.rel.rows[0].valueAt(1))

	////// resolveSubjectPredFromObject
	// case 1: ?s = 0, ?p = 0
	// ?s ?p HVACZONE_1
	cursor, err = hod.Cursor("example")
	require.NoError(err, "create cursor")
	cursor.addQueryPlan(plan)
	qt = makeQueryTerm(cursor, makeTriple("?v1", "?v2", HVACZONE_1))
	rspo := &resolveSubjectPredFromObject{term: *qt}
	require.NoError(rspo.run(cursor), "run resolve subject")
	require.Equal(2, len(cursor.rel.rows))

	found := false
	var idx int
	for idx = 0; idx < 2; idx++ {
		found = cursor.ContextualizeURI(stringtoURI(ROOM_1)) == cursor.rel.rows[idx].valueAt(0) && cursor.ContextualizeURI(stringtoURI(BF_ISPARTOF)) == cursor.rel.rows[idx].valueAt(1)
		if found {
			break
		}
	}
	require.Equal(cursor.ContextualizeURI(stringtoURI(ROOM_1)), cursor.rel.rows[idx].valueAt(0))
	require.Equal(cursor.ContextualizeURI(stringtoURI(BF_ISPARTOF)), cursor.rel.rows[idx].valueAt(1))

	// case 2: ?s = 0, ?p = 0
	// ?s RDF_TYPE BRICK_ROOM
	// ?s ?p HVACZONE_1
	cursor, err = hod.Cursor("example")
	require.NoError(err, "create cursor")
	cursor.addQueryPlan(plan)
	qt = makeQueryTerm(cursor, makeTriple("?v1", RDF_TYPE, BRICK_ROOM))
	rs = &resolveSubject{term: *qt}
	require.NoError(rs.run(cursor), "run resolveSubject")
	require.Equal(1, len(cursor.rel.rows))
	require.Equal(cursor.ContextualizeURI(stringtoURI(ROOM_1)), cursor.rel.rows[0].valueAt(0))

	qt = makeQueryTerm(cursor, makeTriple("?v1", "?v2", HVACZONE_1))
	rspo = &resolveSubjectPredFromObject{term: *qt}
	require.NoError(rspo.run(cursor), "run resolve subject")
	require.Equal(1, len(cursor.rel.rows))
	require.Equal(cursor.ContextualizeURI(stringtoURI(ROOM_1)), cursor.rel.rows[0].valueAt(0))
	require.Equal(cursor.ContextualizeURI(stringtoURI(BF_ISPARTOF)), cursor.rel.rows[0].valueAt(1))

	// case 3: ?s = 0, ?p > 0
	// ROOM_1 ?p HVACZONE_1
	// ?s ?p FLOOR_1
	cursor, err = hod.Cursor("example")
	require.NoError(err, "create cursor")
	cursor.addQueryPlan(plan)
	qt = makeQueryTerm(cursor, makeTriple(ROOM_1, "?v2", HVACZONE_1))
	rp = &resolvePredicate{term: *qt}
	require.NoError(rp.run(cursor), "run resolve predicate")
	require.Equal(1, len(cursor.rel.rows))
	require.Equal(cursor.ContextualizeURI(stringtoURI(BF_ISPARTOF)), cursor.rel.rows[0].valueAt(1))

	qt = makeQueryTerm(cursor, makeTriple("?v1", "?v2", FLOOR_1))
	rspo = &resolveSubjectPredFromObject{term: *qt}
	require.NoError(rspo.run(cursor), "run resolve subject")
	require.Equal(1, len(cursor.rel.rows))
	require.Equal(cursor.ContextualizeURI(stringtoURI(ROOM_1)), cursor.rel.rows[0].valueAt(0))
	require.Equal(cursor.ContextualizeURI(stringtoURI(BF_ISPARTOF)), cursor.rel.rows[0].valueAt(1))

	// case 4: ?s >0, ?p > 0
	// ?s ?p HVACZONE_1
	// ?s ?p FLOOR_1
	cursor, err = hod.Cursor("example")
	require.NoError(err, "create cursor")
	cursor.addQueryPlan(plan)
	qt = makeQueryTerm(cursor, makeTriple("?v1", "?v2", HVACZONE_1))
	rspo = &resolveSubjectPredFromObject{term: *qt}
	require.NoError(rspo.run(cursor), "run resolve subject")
	require.Equal(2, len(cursor.rel.rows))

	found = false
	for idx = 0; idx < 2; idx++ {
		found = cursor.ContextualizeURI(stringtoURI(ROOM_1)) == cursor.rel.rows[idx].valueAt(0) && cursor.ContextualizeURI(stringtoURI(BF_ISPARTOF)) == cursor.rel.rows[idx].valueAt(1)
		if found {
			break
		}
	}
	require.Equal(cursor.ContextualizeURI(stringtoURI(ROOM_1)), cursor.rel.rows[idx].valueAt(0))
	require.Equal(cursor.ContextualizeURI(stringtoURI(BF_ISPARTOF)), cursor.rel.rows[idx].valueAt(1))

	qt = makeQueryTerm(cursor, makeTriple("?v1", "?v2", FLOOR_1))
	rspo = &resolveSubjectPredFromObject{term: *qt}
	require.NoError(rspo.run(cursor), "run resolve subject")
	require.Equal(2, len(cursor.rel.rows))
	found = false
	for idx = 0; idx < 2; idx++ {
		found = cursor.ContextualizeURI(stringtoURI(ROOM_1)) == cursor.rel.rows[idx].valueAt(0) && cursor.ContextualizeURI(stringtoURI(BF_ISPARTOF)) == cursor.rel.rows[idx].valueAt(1)
		if found {
			break
		}
	}
	require.Equal(cursor.ContextualizeURI(stringtoURI(ROOM_1)), cursor.rel.rows[idx].valueAt(0))
	require.Equal(cursor.ContextualizeURI(stringtoURI(BF_ISPARTOF)), cursor.rel.rows[idx].valueAt(1))

	////// resolvePredObjectFromSubject
	// case 1: ?p = 0, ?o = 0
	// ROOM_1 ?p ?o
	cursor, err = hod.Cursor("example")
	require.NoError(err, "create cursor")
	cursor.addQueryPlan(plan)
	qt = makeQueryTerm(cursor, makeTriple(ROOM_1, "?v1", "?v2"))
	pos := &resolvePredObjectFromSubject{term: *qt}
	require.NoError(pos.run(cursor), "run resolvePredObjectFromSubject")
	require.Equal(4, len(cursor.rel.rows))

	// case 2: ?p >0, ?o = 0
	// ROOM_1 ?p BRICK_ROOM
	// HVACZONE_1 ?p ?o
	cursor, err = hod.Cursor("example")
	require.NoError(err, "create cursor")
	cursor.addQueryPlan(plan)
	qt = makeQueryTerm(cursor, makeTriple(ROOM_1, "?v1", BRICK_ROOM))
	rp = &resolvePredicate{term: *qt}
	require.NoError(rp.run(cursor), "run resolve predicate")
	require.Equal(1, len(cursor.rel.rows))
	require.Equal(cursor.ContextualizeURI(stringtoURI(RDF_TYPE)), cursor.rel.rows[0].valueAt(0))

	qt = makeQueryTerm(cursor, makeTriple(HVACZONE_1, "?v1", "?v2"))
	pos = &resolvePredObjectFromSubject{term: *qt}
	require.NoError(pos.run(cursor), "run resolvePredObjectFromSubject")
	require.Equal(1, len(cursor.rel.rows))
	require.Equal(cursor.ContextualizeURI(stringtoURI(RDF_TYPE)), cursor.rel.rows[0].valueAt(0))
	require.Equal(cursor.ContextualizeURI(stringtoURI(BRICK_HVACZONE)), cursor.rel.rows[0].valueAt(1))

	// case 3: ?p = 0, ?o > 0
	// AHU_1 BF_FEEDS ?o
	// AHU_1 ?p ?o
	cursor, err = hod.Cursor("example")
	require.NoError(err, "create cursor")
	cursor.addQueryPlan(plan)
	qt = makeQueryTerm(cursor, makeTriple(AHU_1, BF_FEEDS, "?v2"))
	ro = &resolveObject{term: *qt}
	require.NoError(ro.run(cursor), "run resolveObject")
	require.Equal(1, len(cursor.rel.rows))

	qt = makeQueryTerm(cursor, makeTriple(AHU_1, "?v1", "?v2"))
	pos = &resolvePredObjectFromSubject{term: *qt}
	require.NoError(pos.run(cursor), "run resolvePredObjectFromSubject")
	require.Equal(1, len(cursor.rel.rows))
	require.Equal(cursor.ContextualizeURI(stringtoURI(BF_FEEDS)), cursor.rel.rows[0].valueAt(0))
	require.Equal(cursor.ContextualizeURI(stringtoURI(VAV_1)), cursor.rel.rows[0].valueAt(1))

	// case 4: ?p >0, ?o > 0
	//TODO: finish

	// resolveVarTripleFromSubject
	plan = &queryPlan{
		selectVars: []string{"?v1", "?v2", "?v3"},
		variables:  []string{"?v1", "?v2", "?v3"},
	}
	cursor, err = hod.Cursor("example")
	require.NoError(err, "create cursor")
	cursor.addQueryPlan(plan)
	qt = makeQueryTerm(cursor, makeTriple("?v1", RDF_TYPE, BRICK_ROOM))
	rs = &resolveSubject{term: *qt}
	require.NoError(rs.run(cursor), "run resolve subject")
	require.Equal(1, len(cursor.rel.rows))

	qt = makeQueryTerm(cursor, makeTriple("?v1", "?v2", "?v3"))
	vtfs := &resolveVarTripleFromSubject{term: *qt}
	require.NoError(vtfs.run(cursor), "run resolveVarTripleFromSubject")
	require.Equal(4, len(cursor.rel.rows))

	// resolveVarTripleFromObject
	cursor, err = hod.Cursor("example")
	require.NoError(err, "create cursor")
	cursor.addQueryPlan(plan)
	qt = makeQueryTerm(cursor, makeTriple("?v3", RDF_TYPE, BRICK_VAV))
	rs = &resolveSubject{term: *qt}
	require.NoError(rs.run(cursor), "run resolve subject")
	require.Equal(1, len(cursor.rel.rows))

	qt = makeQueryTerm(cursor, makeTriple("?v1", "?v2", "?v3"))
	vtfo := &resolveVarTripleFromObject{term: *qt}
	require.NoError(vtfo.run(cursor), "run resolveVarTripleFromObject")
	require.Equal(4, len(cursor.rel.rows))

	// resolveVarTripleFromPredicate
	cursor, err = hod.Cursor("example")
	require.NoError(err, "create cursor")
	cursor.addQueryPlan(plan)
	qt = makeQueryTerm(cursor, makeTriple(AHU_1, "?v2", VAV_1))
	rp = &resolvePredicate{term: *qt}
	require.NoError(rp.run(cursor), "run resolve predicate")
	require.Equal(1, len(cursor.rel.rows))

	qt = makeQueryTerm(cursor, makeTriple("?v1", "?v2", "?v3"))
	vtfp := &resolveVarTripleFromPredicate{term: *qt}
	require.NoError(vtfp.run(cursor), "run resolveVarTripleFromPredicate")
	require.Equal(2, len(cursor.rel.rows))

}
