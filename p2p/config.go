package p2p

import (
	"io"
	"os"

	"github.com/BurntSushi/toml"
	"github.com/gtfierro/hoddb/hod"
	noiselog "github.com/perlin-network/noise/log"
	logrus "github.com/sirupsen/logrus"
)

var log = logrus.New()

func init() {
	log.SetFormatter(&logrus.TextFormatter{FullTimestamp: true, ForceColors: true})
	log.SetOutput(os.Stdout)
	log.SetLevel(logrus.DebugLevel)
	noiselog.Disable()
}

type Config struct {
	HodConfig *hod.Config
	//TODO: embed the configuration?

	ListenPort int

	PublicPolicy []View
	Peer         []Peer
}

type View struct {
	// list of graph names
	Graphs     []string
	Definition string
}

type Peer struct {
	// ip:port
	Address string
	//TODO: public key for routing
	Policies []View
	Wants    []View
}

func ReadConfig(r io.Reader) (Config, error) {
	var cfg Config
	_, err := toml.DecodeReader(r, &cfg)
	return cfg, err
}
