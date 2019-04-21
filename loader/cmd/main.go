package main

import (
	"context"
	hod "git.sr.ht/~gabe/hod/loader"
	"github.com/pkg/errors"
	"log"
	"time"
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

	_ = hod

	qstr := `SELECT ?x FROM soda WHERE { ?x rdf:type brick:Room };`
	query, err := hod.ParseQuery(qstr, time.Now().UnixNano())
	if err != nil {
		log.Fatal(errors.Wrap(err, "parse query"))
	}
	log.Println(query)

	resp, err := hod.Select(context.Background(), query)
	if err != nil {
		log.Fatal(errors.Wrap(err, "parse query"))
	}
	log.Println(resp)
}
