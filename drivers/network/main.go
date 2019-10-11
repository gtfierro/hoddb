package main

import (
	"fmt"
	"github.com/urfave/cli"
	"log"
	"os"

	"github.com/gtfierro/hoddb/p2p"
	rdf "github.com/gtfierro/hoddb/turtle"

	"github.com/dchest/uniuri"
	"github.com/google/gopacket"
	"github.com/google/gopacket/examples/util"
	"github.com/google/gopacket/layers"
	"github.com/google/gopacket/pcap"
	"github.com/pkg/errors"
)

const (
	OWL     Namespace = "http://www.w3.org/2002/07/owl#"
	RDF     Namespace = "http://www.w3.org/1999/02/22-rdf-syntax-ns#"
	RDFS    Namespace = "http://www.w3.org/2000/01/rdf-schema#"
	NETWORK Namespace = "http://xbos.io/ontologies/network#"
	MYNET   Namespace = "http://example.com/mynet#"
)

var cache = make(map[rdf.URI]rdf.URI)

//var newId(value string) rdf.URI {
//    if uri, found := cache[
//}

type Namespace string

func (ns Namespace) URI(resource string) rdf.URI {
	return rdf.ParseURI(string(ns) + resource)
}
func Value(resource string) rdf.URI {
	return rdf.URI{Value: fmt.Sprintf("\"%s\"", resource)}
}

func (mon *NetworkMonitor) TriplesFromPacket(pkt gopacket.Packet) (triples []rdf.Triple) {
	// get addresses from network layer

	// generate random identifiers; we can resolve these later
	var src = MYNET.URI(uniuri.New())
	var dst = MYNET.URI(uniuri.New())

	// add flow
	addTriple := func(defaultsub, pred, obj rdf.URI) rdf.URI {
		ents := mon.getEntitiesWithProperty(pred, obj)
		if len(ents) > 0 {
			defaultsub = ents[0]
		} else {
			triples = append(triples, rdf.Triple{
				Subject:   defaultsub,
				Predicate: RDF.URI("type"),
				Object:    NETWORK.URI("Host"),
			})
		}
		triples = append(triples, rdf.Triple{
			Subject:   defaultsub,
			Predicate: pred,
			Object:    obj,
		})
		return defaultsub
	}
	// decode link layer
	var linkflow gopacket.Flow
	if m, ok := pkt.LinkLayer().(*layers.Ethernet); ok {
		linkflow = m.LinkFlow()
	} else {
		panic("no network layer")
	}

	addTriple(src, NETWORK.URI("hasMAC"), Value(linkflow.Src().String()))
	addTriple(dst, NETWORK.URI("hasMAC"), Value(linkflow.Dst().String()))
	src_manu := mon.oid.Lookup(linkflow.Src().String())
	dst_manu := mon.oid.Lookup(linkflow.Dst().String())
	if src_manu != "unknown" {
		addTriple(src, NETWORK.URI("hasManufacturer"), Value(src_manu))
	}
	if dst_manu != "unknown" {
		addTriple(dst, NETWORK.URI("hasManufacturer"), Value(dst_manu))
	}

	// test

	// decode network layer
	var networkflow gopacket.Flow
	if net6, ok := pkt.NetworkLayer().(*layers.IPv6); ok {
		networkflow = net6.NetworkFlow()
	} else if net4, ok := pkt.NetworkLayer().(*layers.IPv4); ok {
		networkflow = net4.NetworkFlow()
	} else {
		panic("no network layer")
	}
	newsrc := addTriple(src, NETWORK.URI("hasAddress"), Value(networkflow.Src().String()))
	newdst := addTriple(dst, NETWORK.URI("hasAddress"), Value(networkflow.Dst().String()))
	triples = append(triples, rdf.Triple{
		Subject:   newsrc,
		Predicate: NETWORK.URI("talksTo"),
		Object:    newdst,
	})

	// decode transport layer
	var transportflow gopacket.Flow
	var relship rdf.URI
	if tcp, ok := pkt.TransportLayer().(*layers.TCP); ok {
		transportflow = tcp.TransportFlow()
		relship = NETWORK.URI("hasTCPPort")
	} else if udp, ok := pkt.TransportLayer().(*layers.UDP); ok {
		transportflow = udp.TransportFlow()
		relship = NETWORK.URI("hasUDPPort")
	} else {
		panic("no  transport layer")
	}
	addTriple(src, relship, Value(transportflow.Src().String()))
	addTriple(dst, relship, Value(transportflow.Dst().String()))

	return
}

// detect hosts on the network ;get the manufacturer using the OID
// detect UPNP services

// generate triples from what we notice
func doScrape(c *cli.Context) error {
	fmt.Println(c.FlagNames())

	// get config
	f, err := os.Open(c.String("config"))
	if err != nil {
		return errors.Wrap(err, "read config file")
	}
	cfg, err := p2p.ReadConfig(f)
	if err != nil {
		return errors.Wrap(err, "parse config file")
	}

	mon, err := NewNetworkMonitor(&cfg)
	if err != nil {
		return errors.Wrap(err, "create network monitor")
	}

	// dump database
	var __select_all_query = `SELECT ?s ?p ?o WHERE { ?s ?p ?o };`
	resp, err := mon.runQuery(__select_all_query)
	if err != nil {
		panic(err)
	}
	if err != nil {
		panic(err)
	}
	for _, t := range resp {
		fmt.Println(t)
	}
	fmt.Println("-------------")

	defer util.Run()()
	if handle, err := pcap.OpenLive(c.String("interface"), 1600, true, pcap.BlockForever); err != nil {
		return errors.Wrap(err, "could not open iface")
	} else if err := handle.SetBPFFilter(c.String("filter")); err != nil { // optional
		return errors.Wrap(err, "could not set bpf filter")
	} else {
		packetSource := gopacket.NewPacketSource(handle, handle.LinkType())

		for packet := range packetSource.Packets() {
			if packet.NetworkLayer() == nil || packet.TransportLayer() == nil {
				continue
			}
			pkttriples := mon.TriplesFromPacket(packet)
			for _, t := range pkttriples {
				fmt.Printf("Triple>> %s\n", t)
			}
			if err := mon.AddTriples(pkttriples); err != nil {
				panic(err)
			}

		}
	}
	return nil
}

func main() {
	app := cli.NewApp()
	app.Name = "network"
	app.Commands = []cli.Command{
		{
			Name:  "listen",
			Usage: "Scrape traffic off of an interface ",
			Flags: []cli.Flag{
				cli.StringFlag{
					Name:  "interface, i",
					Usage: "Interface to scrape packets from",
					Value: "eth0",
				},
				cli.StringFlag{
					Name:  "filter, f",
					Usage: "BPF filter",
					Value: "ip",
				},
				cli.StringFlag{
					Name:  "config, c",
					Usage: "Config file",
					Value: "networknode.toml",
				},
			},
			Action: doScrape,
		},
	}
	log.Println(os.Args)
	log.Fatal(app.Run(os.Args))
}
