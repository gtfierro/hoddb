package main

import (
	"git.sr.ht/~gabe/hod/hod"
	"github.com/pkg/errors"
	"log"
)

func main() {

	cfg, err := hod.ReadConfig("hodconfig.yml")
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
