package main

import (
	"io"

	"github.com/BurntSushi/toml"
	"github.com/gtfierro/hoddb/hod"
)

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
