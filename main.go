package main

import (
	"context"
	"time"
	//"fmt"
	"git.sr.ht/~gabe/hod/hod"
	"github.com/pkg/errors"
	"log"
)

var debug = false

func main() {

	cfg, err := hod.ReadConfig("hodconfig.yml")
	if err != nil {
		log.Fatal(errors.Wrap(err, "Could not load config file"))
	}
	_ = cfg

	L, err := hod.NewLog(cfg)
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

	selectquery, err := L.ParseQuery(q, time.Now().UnixNano())
	log.Println(selectquery)

	_, err = L.Select(context.Background(), selectquery)
	if err != nil {
		log.Println(errors.Wrap(err, "select q1"))
	} else {
		log.Println("successful q1")
	}
	//cur.dumpResponse(resp)

	_, err = L.Select(context.Background(), selectquery)
	if err != nil {
		log.Println(errors.Wrap(err, "select q2"))
	} else {
		log.Println("successful q2")
	}
}
