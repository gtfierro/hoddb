package main

import (
	"flag"
	"os"

	"github.com/perlin-network/noise"
	noiselog "github.com/perlin-network/noise/log"
	logrus "github.com/sirupsen/logrus"
)

var log = logrus.New()

func init() {
	log.SetFormatter(&logrus.TextFormatter{FullTimestamp: true, ForceColors: true})
	log.SetOutput(os.Stdout)
	log.SetLevel(logrus.DebugLevel)
	noiselog.Disable()

	opcodeRequestMessage = noise.RegisterMessage(noise.NextAvailableOpcode(), (*tupleRequest)(nil))
	opcodeUpdateMessage = noise.RegisterMessage(noise.NextAvailableOpcode(), (*tupleUpdate)(nil))
}

var opcodeRequestMessage noise.Opcode
var opcodeUpdateMessage noise.Opcode

var config = flag.String("config", "p2p.yml", "Path to p2p.yml file")

func main() {
	flag.Parse()

	f, err := os.Open(*config)
	if err != nil {
		log.Fatal(err)
	}
	cfg, err := ReadConfig(f)
	if err != nil {
		log.Fatal(err)
	}

	NewNode(&cfg)
	// Register message type to Noise.

	select {}

}
