package main

import (
	"context"
	"time"

	"github.com/gtfierro/hoddb/hod"
	pb "github.com/gtfierro/hoddb/proto"
	"github.com/gtfierro/hoddb/turtle"
	"github.com/pkg/errors"

	"github.com/perlin-network/noise"
	"github.com/perlin-network/noise/cipher/aead"
	"github.com/perlin-network/noise/handshake/ecdh"
	"github.com/perlin-network/noise/protocol"
)

type Node struct {
	listenPort   int
	desiredPeers []Peer
	db           *hod.HodDB
	node         *noise.Node
}

func NewNode(cfg *Config) (*Node, error) {

	var n = new(Node)

	// set up HodDB
	hcfg, err := hod.ReadConfig(cfg.HodConfig)
	if err != nil {
		log.Fatal(errors.Wrap(err, "Could not load config file"))
	}
	n.db, err = hod.MakeHodDB(hcfg)
	if err != nil {
		log.Fatal(errors.Wrap(err, "open log"))
	}
	if n.db == nil {
		panic("null db")
	}

	n.listenPort = cfg.ListenPort
	n.desiredPeers = cfg.Peer

	// initialize p2p params
	params := noise.DefaultParams()
	params.Port = uint16(n.listenPort)
	n.node, err = noise.NewNode(params)
	if err != nil {
		panic(err)
	}
	protocol.New().
		Register(ecdh.New()).
		Register(aead.New()).
		Enforce(n.node)

	log.Infof("Listening for peers on port %d", n.node.ExternalPort())

	// start listening for peers
	go n.node.Listen()
	n.node.OnPeerInit(n.peerInit)

	// start dialing peers
	go n.dialPeers()

	return n, nil
}

func (n *Node) dialPeers() {
	// TODO: do we need to attempt reconnection manually?
	for _, peer := range n.desiredPeers {
		_, err := n.node.Dial(peer.Address)
		if err != nil {
			log.Error(errors.Wrapf(err, "Could not dial peer %s. Retrying in 4 sec", peer.Address))
			time.Sleep(4 * time.Second)
		} else {
			break
		}
	}
}

// gets run when we connect to a peer
func (n *Node) peerInit(node *noise.Node, peer *noise.Peer) error {
	log.Info("Connected to peer ", peer.RemoteIP().String(), ":", string(peer.RemotePort()))
	// TODO: add context so these all get fixed up
	go n.handleRequests(peer)
	go n.handleUpdates(peer)
	for _, pcfg := range n.desiredPeers {
		go n.requestUpdates(peer, pcfg)
	}
	return nil
}

func (n *Node) handleRequests(peer *noise.Peer) {
	c := peer.Receive(opcodeRequestMessage)
	for _msg := range c {
		msg := _msg.(tupleRequest)

		// evaluate a request for tuples
		res, err := n.db.Select(context.Background(), &msg.Definition)
		if err != nil {
			log.Error(err)
			continue
		}

		// loop through results and send to the peer
		count := len(res.Rows)
		chunksize := 100
		lower := 0
		upper := chunksize
		for upper <= count {
			if upper > count {
				upper = count
			}
			response := tupleUpdate{
				Header: header{
					Timestamp: time.Now(),
					From:      []byte("put something better here"),
				},
				Rows: res.Rows[lower:upper],
				Vars: res.Variables,
			}
			if err = peer.SendMessage(response); err != nil {
				log.Error(errors.Wrap(err, "Could not send response"))
				continue
			}

			lower += chunksize
			upper += chunksize
		}
	}
}

func (n *Node) handleUpdates(peer *noise.Peer) {
	c := peer.Receive(opcodeUpdateMessage)
	var rows []*pb.Row
	var timer = time.NewTimer(10 * time.Second)

	commit := func() error {
		if len(rows) == 0 {
			return nil
		}
		log.Infof("Updating with %d rows", len(rows))
		dataset := turtle.DataSetFromRows(rows)
		update, err := n.db.MakeTripleUpdate(dataset, "test")
		if err != nil {
			return errors.Wrap(err, "Could not make update")
		}
		if err := n.db.LoadGraph(update); err != nil {
			return errors.Wrap(err, "Could not apply update")
		}
		rows = rows[:0] // saves underlying memory
		timer.Stop()
		timer.Reset(10 * time.Second)
		return nil
	}

	for {
		select {
		case msg := <-c:
			rows = append(rows, msg.(tupleUpdate).Rows...)
			// update if last commit was more than 30 seconds ago or we have 20,000 rows
			if len(rows) > 20000 {
				if err := commit(); err != nil {
					log.Error(err)
				}
			}
		case <-timer.C:
			if err := commit(); err != nil {
				log.Error(err)
			}
		}
	}
}

func (n *Node) requestUpdates(peer *noise.Peer, peercfg Peer) {
	// periodically loop through peers and request our views

	go func() {
		// check queries
		for range time.Tick(5 * time.Second) {
			//q, err := n.db.ParseQuery("SELECT ?x ?y ?z WHERE { ?x ?y ?z };", 0)
			q, err := n.db.ParseQuery("SELECT ?x WHERE { ?x rdf:type/rdfs:subClassOf* brick:Point};", 0)
			if err != nil {
				panic(err)
			}
			resp, err := n.db.Select(context.Background(), q)
			if err != nil {
				panic(err)
			}
			log.Warning("DB now has", len(resp.Rows))
		}
	}()

	// TODO: re-run periodically to check if things change
	//for range time.Tick(30 * time.Second) {
	for _, want := range peercfg.Wants {
		q, err := n.db.ParseQuery(want.Definition, 0)
		if err != nil {
			log.Error(err)
			continue
		}
		log.Println("requesting>", want)
		req := tupleRequest{
			Header: header{
				Timestamp: time.Now(),
				From:      []byte("id 1"),
			},
			Definition: *q,
		}

		err = peer.SendMessage(req)
		if err != nil {
			log.Error(err)
			continue
		}
	}
	//}
}
