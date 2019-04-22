package main

import (
	//	"context"
	//	"fmt"
	hod "git.sr.ht/~gabe/hod/loader"
	//	"github.com/chzyer/readline"
	"github.com/pkg/errors"
	//	"io"
	"log"
	//	"strings"
	//	"time"
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
