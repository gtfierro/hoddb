package main

import (
	"flag"
	"github.com/gtfierro/hoddb/hod"
	"github.com/pkg/errors"
	"log"
)

var config = flag.String("config", "hodconfig.yml", "Path to hodconfig.yml file")

func main() {
	flag.Parse()

	cfg, err := hod.ReadConfig(*config)
	if err != nil {
		log.Fatal(errors.Wrap(err, "Could not load config file"))
	}
	_ = cfg

	hod, err := hod.MakeHodDB(cfg)
	if err != nil {
		log.Fatal(errors.Wrap(err, "open log"))
	}

	log.Fatal(hod.ServeGRPC())

}
