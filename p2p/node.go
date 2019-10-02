package main

import (
	"context"
	"strings"
	"time"

	"github.com/gtfierro/hoddb/hod"
	//query "github.com/gtfierro/hoddb/lang"
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
	views        map[string]time.Time
}

func NewNode(cfg *Config) (*Node, error) {

	var n = new(Node)
	n.views = make(map[string]time.Time)

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

func (n *Node) Request(req *pb.TupleRequest, srv pb.P2P_RequestServer) error {

	res, err := n.db.Select(context.Background(), req.Definition)
	if err != nil {
		return err
	}
	count := len(res.Rows)
	chunksize := 100
	lower := 0
	upper := chunksize
	if upper > count {
		upper = count
	}
	for upper <= count {
		if upper > count {
			upper = count
		}
		response := tupleUpdate{
			Header: header{
				Timestamp: time.Now(),
				From:      []byte("put something better here"),
			},
			Rows:       res.Rows[lower:upper],
			Vars:       res.Variables,
			Definition: *req.Definition,
		}

		if err = srv.Send(response.ToProto()); err != nil {
			return errors.Wrap(err, "Could not send response")
		}

		lower += chunksize
		upper += chunksize
	}
	return nil
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
		for _, view := range pcfg.Wants {
			n.views[view.Definition] = time.Now()
		}
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
		log.Debug("count rows", count)
		chunksize := 100
		lower := 0
		upper := chunksize
		if upper > count {
			upper = count
		}
		for upper <= count {
			if upper > count {
				upper = count
			}
			log.Warning("update", res.Rows[lower:upper])
			response := tupleUpdate{
				Header: header{
					Timestamp: time.Now(),
					From:      []byte("put something better here"),
				},
				Rows:       res.Rows[lower:upper],
				Vars:       res.Variables,
				Definition: msg.Definition,
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

	commit := func(upd tupleUpdate) error {
		if len(upd.Rows) == 0 {
			return nil
		}
		log.Infof("Updating with %d rows", len(upd.Rows))

		// add triples by substituting the query results into the query
		var generatedRows []turtle.Triple
		for _, term := range upd.Definition.Where {
			var (
				subIdx  int = -1
				predIdx int = -1
				objIdx  int = -1
			)
			if isVariable(term.Subject) {
				subIdx = indexOf(upd.Vars, term.Subject.Value)
			}
			if isVariable(term.Predicate[0]) {
				predIdx = indexOf(upd.Vars, term.Predicate[0].Value)
			}
			if isVariable(term.Object) {
				objIdx = indexOf(upd.Vars, term.Object.Value)
			}

			for _, row := range upd.Rows {
				triple := turtle.Triple{}
				if subIdx >= 0 {
					triple.Subject = turtle.URI{Namespace: row.Values[subIdx].Namespace, Value: row.Values[subIdx].Value}
				} else {
					triple.Subject = turtle.URI{Namespace: term.Subject.Namespace, Value: term.Subject.Value}
				}

				if predIdx >= 0 {
					triple.Predicate = turtle.URI{Namespace: row.Values[predIdx].Namespace, Value: row.Values[predIdx].Value}
				} else {
					triple.Predicate = turtle.URI{Namespace: term.Predicate[0].Namespace, Value: term.Predicate[0].Value}
				}

				if objIdx >= 0 {
					triple.Object = turtle.URI{Namespace: row.Values[objIdx].Namespace, Value: row.Values[objIdx].Value}
				} else {
					triple.Object = turtle.URI{Namespace: term.Object.Namespace, Value: term.Object.Value}
				}

				log.Debug("generated> ", triple)
				generatedRows = append(generatedRows, triple)
			}
		}

		dataset := turtle.DataSet{
			Triples: generatedRows,
		}
		_ = dataset

		if err := n.db.AddTriples(dataset); err != nil {
			return errors.Wrap(err, "Could not apply update")
		}
		//rows = rows[:0] // saves underlying memory
		return nil
	}

	for msg := range c {
		upd := msg.(tupleUpdate)
		// update if last commit was more than 30 seconds ago or we have 20,000 rows
		if err := commit(upd); err != nil {
			log.Error(err)
		}
	}
}

func (n *Node) requestUpdates(peer *noise.Peer, peercfg Peer) {
	// periodically loop through peers and request our views

	go func() {
		// check queries
		for range time.Tick(5 * time.Second) {
			q, err := n.db.ParseQuery("SELECT ?x ?y ?z WHERE { ?x ?y ?z };", 0)
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
		time.Sleep(10 * time.Second)
		log.Info("requesting>", want)
		q, err := n.db.ParseQuery(want.Definition, 0)
		if err != nil {
			log.Error(err)
			continue
		}

		// TODO: need to save the query view representation inside the node.
		// When we get the contents of a view back, we can check which query it
		// is for. Use the query terms to "generate" the source triples that we
		// then insert into our local database.

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

func isVariable(uri *pb.URI) bool {
	return uri == nil || strings.HasPrefix(uri.Value, "?")
}

func indexOf(l []string, value string) int {
	for i, v := range l {
		if v == value {
			return i
		}
	}
	return -1
}
