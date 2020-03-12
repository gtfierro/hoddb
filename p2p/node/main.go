package main

import (
	"flag"
	"os"

	"github.com/gtfierro/hoddb/p2p"
	"log"
)

var config = flag.String("config", "p2p.yml", "Path to p2p.yml file")

func main() {
	flag.Parse()

	f, err := os.Open(*config)
	if err != nil {
		log.Fatal(err)
	}
	cfg, err := p2p.ReadConfig(f)
	if err != nil {
		log.Fatal(err)
	}

	p2p.NewNode(&cfg)
	// Register message type to Noise.

	select {}

}
