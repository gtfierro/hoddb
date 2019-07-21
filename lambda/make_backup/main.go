package main

import (
	"context"
	"flag"
	"fmt"
	"git.sr.ht/~gabe/hod/hod"
	"github.com/pkg/errors"
	"log"
	"os"
)

var config = flag.String("config", "hodconfig.yml", "Path to hodconfig.yml file")
var ttl = flag.String("ttl", "ciee.ttl", "Name of building")
var building = flag.String("building", "ciee.ttl", "Path to building.ttl file")

func main() {
	flag.Parse()

	cfg, err := hod.ReadConfig(*config)
	if err != nil {
		log.Fatal(errors.Wrap(err, "Could not load config file"))
	}
	_ = cfg
	cfg.Database.Buildings["bldg"] = *ttl

	hod, err := hod.MakeHodDB(cfg)
	if err != nil {
		log.Fatal(errors.Wrap(err, "open log"))
	}

	q, err := hod.ParseQuery("SELECT ?r WHERE { ?r rdf:type brick:Room };", 0)
	if err != nil {
		log.Fatal(errors.Wrap(err, "parse q"))
	}
	fmt.Printf("%+v\n", q)
	res, err := hod.Select(context.Background(), q)
	if err != nil {
		log.Fatal(errors.Wrap(err, "select q"))
	}
	fmt.Printf("%+v\n", res)

	backupfile, err := os.Create(fmt.Sprintf("%s.badger", *building))
	defer backupfile.Close()
	if err != nil {
		log.Fatal(errors.Wrap(err, "open backup file"))
	}

	if err := hod.Backup(backupfile); err != nil {
		log.Fatal(errors.Wrap(err, "open backup file"))
	}

}
