package main

import (
	"context"
	"time"
	//"fmt"
	//logpb "git.sr.ht/~gabe/hod/proto"
	"github.com/pkg/errors"
	"log"
)

var debug = false

var (
	RDF_TYPE       = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type"
	BRICK_ROOM     = "https://brickschema.org/schema/1.0.3/Brick#Room"
	BRICK_VAV      = "https://brickschema.org/schema/1.0.3/Brick#VAV"
	BRICK_HVACZONE = "https://brickschema.org/schema/1.0.3/Brick#HVAC_Zone"
	BRICK_ZNT      = "https://brickschema.org/schema/1.0.3/Brick#Zone_Temperature_Sensor"
	BF_ISPARTOF    = "https://brickschema.org/schema/1.0.3/BrickFrame#isPartOf"
	BF_HASPART     = "https://brickschema.org/schema/1.0.3/BrickFrame#hasPart"
	BF_ISPOINTOF   = "https://brickschema.org/schema/1.0.3/BrickFrame#isPointOf"
	BF_FEEDS       = "https://brickschema.org/schema/1.0.3/BrickFrame#feeds"
	BF_ISFEDBY     = "https://brickschema.org/schema/1.0.3/BrickFrame#isFedBy"

	ROOM_1     = "http://buildsys.org/ontologies/building_example#room_1"
	VAV_1      = "http://buildsys.org/ontologies/building_example#vav_1"
	AHU_1      = "http://buildsys.org/ontologies/building_example#ahu_1"
	FLOOR_1    = "http://buildsys.org/ontologies/building_example#floor_1"
	HVACZONE_1 = "http://buildsys.org/ontologies/building_example#hvaczone_1"
)

func main() {

	cfg, err := ReadConfig("hodconfig.yml")
	if err != nil {
		log.Fatal(errors.Wrap(err, "Could not load config file"))
	}
	_ = cfg

	L, err := NewLog(cfg)
	if err != nil {
		log.Fatal(errors.Wrap(err, "open log"))
	}
	defer L.Close()
	//if err := L.ServeGRPC(); err != nil {
	//	log.Fatal(errors.Wrap(err, "grpc"))
	//}
	//version, err := L.LoadFile("test", "BrickFrame.ttl", "bf")
	//log.Println("V>", version)
	//if err != nil {
	//	log.Fatal(errors.Wrap(err, "load brickframe"))
	//}
	//version, err = L.LoadFile("test", "Brick.ttl", "b")
	//log.Println("V>", version)
	//if err != nil {
	//	log.Fatal(errors.Wrap(err, "load brick"))
	//}
	//version, err = L.LoadFile("test", "example.ttl", "ex")
	//log.Println("V>", version)
	//if err != nil {
	//	log.Fatal(errors.Wrap(err, "load example"))
	//}
	//version, err = L.LoadFile("test", "Brick.ttl", "b")
	//log.Println("V>", version)
	//if err != nil {
	//	log.Fatal(errors.Wrap(err, "load brick"))
	//}
	//version, err = L.LoadFile("test", "example.ttl", "ex")
	//log.Println("V>", version)
	//if err != nil {
	//	log.Fatal(errors.Wrap(err, "load example"))
	//}

	//q := "SELECT ?vav ?room FROM test WHERE { ?vav rdf:type brick:VAV . ?room rdf:type brick:Room . ?zone rdf:type brick:HVAC_Zone . ?vav bf:feeds+ ?zone . ?room bf:isPartOf ?zone };"

	q := "SELECT ?x ?y FROM soda WHERE { ?r rdf:type brick:Room . ?x ?y ?r };"
	//version, err := L.LoadFile("soda", "BrickFrame.ttl", "brickframe")
	//if err != nil {
	//	log.Fatal(errors.Wrap(err, "load brickframe"))
	//}
	//version, err = L.LoadFile("soda", "Brick.ttl", "brick")
	//if err != nil {
	//	log.Fatal(errors.Wrap(err, "load brick"))
	//}
	//version, err = L.LoadFile("soda", "berkeley.ttl", "berkeley")
	//if err != nil {
	//	log.Fatal(errors.Wrap(err, "load berkeley"))
	//}
	//_ = version

	//q := "SELECT ?x FROM soda WHERE { ?x rdf:type brick:Room };"

	//cur, err := L.createCursor("test", 0, version)
	//if err != nil {
	//	log.Fatal(errors.Wrap(err, "create cursor"))
	//}
	//_ = cur
	////}
	//key := cur.ContextualizeURI(&logpb.URI{
	//	Namespace: "https://brickschema.org/schema/1.0.3/Brick",
	//	Value:     "Zone_Temperature_Sensor",
	//})
	//log.Println(key)
	//log.Println(S(key))
	//entity, err := cur.getEntity(key)
	//if err != nil {
	//	log.Fatal(errors.Wrap(err, "get ent"))
	//}
	//L.Dump(entity)

	selectquery, err := L.parseQuery(q, time.Now().UnixNano())
	log.Println(selectquery)

	_, err = L.Select(context.Background(), selectquery)
	if err != nil {
		log.Fatal(errors.Wrap(err, "select q1"))
	}
	//cur.dumpResponse(resp)

	_, err = L.Select(context.Background(), selectquery)
	if err != nil {
		log.Fatal(errors.Wrap(err, "select q2"))
	}
}
