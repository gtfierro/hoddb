//go:generate stringer -type=PeerState
package p2p

import (
	"context"
	"fmt"
	"strings"
	"sync"
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

type PeerState uint

const (
	UNKNOWN PeerState = iota
	DIRTY
	SYNCED
)

type Node struct {
	listenPort   int
	desiredPeers []Peer
	db           *hod.HodDB
	node         *noise.Node
	cfg          *Config

	// map of peer Address -> dirty
	// a peer is dirty if we have updated and haven't updated it yet
	downstream map[string]PeerState
	sync.RWMutex
}

func NewNode(cfg *Config) (*Node, error) {

	var n = new(Node)
	var err error
	n.cfg = cfg
	n.downstream = make(map[string]PeerState)

	// set up HodDB
	//hcfg, err := hod.ReadConfig(cfg.HodConfig)
	//if err != nil {
	//	log.Fatal(errors.Wrap(err, "Could not load config file"))
	//}
	n.db, err = hod.MakeHodDB(cfg.HodConfig)
	if err != nil {
		log.Fatal(errors.Wrap(err, "open log"))
	}
	if n.db == nil {
		panic("null db")
	}

	// set up views
	if err := n.db.NewGraph("public"); err != nil {
		panic(err)
	}
	for _, policy := range cfg.PublicPolicy {
		if _, err := n.updateView("public", policy); err != nil {
			panic(err)
		}
	}

	n.listenPort = cfg.ListenPort
	n.desiredPeers = cfg.Peer
	for _, pcfg := range n.desiredPeers {
		n.updatePeerState(pcfg.Address, DIRTY)
	}

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

func (n *Node) GetHodDB() *hod.HodDB {
	return n.db
}

func (n *Node) updatePeerState(peerAddress string, state PeerState) {
	n.Lock()
	n.downstream[peerAddress] = state
	n.Unlock()
}

func (n *Node) getPeerState(peerAddress string) PeerState {
	n.RLock()
	state := n.downstream[peerAddress]
	n.RUnlock()
	return state
}

func (n *Node) markAllPeers(state PeerState) {
	n.Lock()
	for addr := range n.downstream {
		n.downstream[addr] = state
	}
	n.Unlock()
}

func (n *Node) Shutdown() {
	//TODO
	n.node.Kill()
}

func (n *Node) updateView(name string, v View) (bool, error) {
	q, err := n.db.ParseQuery(v.Definition, 0)
	if err != nil {
		return false, err
	}
	q.Graphs = []string{"test"}
	resp, err := n.db.Select(context.Background(), q)
	if err != nil {
		return false, err
	}
	dataset := expandTriples(q.Where, resp.Rows, q.Vars)
	return n.db.AddTriplesWithChanged(name, dataset)
}

func (n *Node) Request(req *pb.TupleRequest, srv pb.P2P_RequestServer) error {

	res, err := n.db.Select(context.Background(), req.Definition)
	if err != nil {
		return err
	}
	count := len(res.Rows)
	chunksize := 400
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
		}
	}
}

// gets run when we connect to a peer
func (n *Node) peerInit(node *noise.Node, peer *noise.Peer) error {
	log.Infof("Connected to peer %s:%d", peer.RemoteIP().String(), peer.RemotePort())
	// TODO: add context so these all get fixed up
	go n.handleRequests(peer)
	go n.handleUpdates(peer)
	peerip := fmt.Sprintf("localhost:%d", peer.RemotePort())
	for _, pcfg := range n.desiredPeers {
		if peerip == pcfg.Address {
			pcfg := pcfg
			fmt.Println("connecting:", peerip)
			go n.requestUpdates(peer, pcfg)
		}
	}
	return nil
}

func (n *Node) handleRequests(peer *noise.Peer) {
	c := peer.Receive(opcodeRequestMessage)
	peeraddr := fmt.Sprintf("%s:%d", peer.RemoteIP(), peer.RemotePort())
	for _msg := range c {
		msg := _msg.(tupleRequest)

		log.Infof("Got request %v from %s:%d", msg, peer.RemoteIP(), peer.RemotePort())
		// evaluate a request for tuples
		res, err := n.db.Select(context.Background(), &msg.Definition)
		if err != nil {
			log.Error(err)
			continue
		}

		// loop through results and send to the peer
		count := len(res.Rows)
		chunksize := 400
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
				Definition: msg.Definition,
			}
			if err = peer.SendMessage(response); err != nil {
				log.Error(errors.Wrap(err, "Could not send response"))
				continue
			}

			lower += chunksize
			upper += chunksize
		}
		n.updatePeerState(peeraddr, SYNCED)
	}
}

func expandTriples(where []*pb.Triple, rows []*pb.Row, vars []string) turtle.DataSet {
	var generatedRows []turtle.Triple
	for _, term := range where {
		var (
			subIdx  int = -1
			predIdx int = -1
			objIdx  int = -1
		)
		if isVariable(term.Subject) {
			subIdx = indexOf(vars, term.Subject.Value)
		}
		if isVariable(term.Predicate[0]) {
			predIdx = indexOf(vars, term.Predicate[0].Value)
		}
		if isVariable(term.Object) {
			objIdx = indexOf(vars, term.Object.Value)
		}

		for _, row := range rows {
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

			//log.Debug("generated> ", triple)
			generatedRows = append(generatedRows, triple)
		}
	}

	dataset := turtle.DataSet{
		Triples: generatedRows,
	}
	return dataset
}

func (n *Node) handleUpdates(peer *noise.Peer) {
	c := peer.Receive(opcodeUpdateMessage)
	//peeraddr := fmt.Sprintf("%s:%d", peer.RemoteIP(), peer.RemotePort())

	commit := func(upd tupleUpdate) error {
		if len(upd.Rows) == 0 {
			return nil
		}
		log.Infof("Updating with %d rows", len(upd.Rows))

		// add triples by substituting the query results into the query
		dataset := expandTriples(upd.Definition.Where, upd.Rows, upd.Vars)

		err := n.db.AddTriples("test", dataset)
		if err != nil {
			return errors.Wrap(err, "Could not apply update")
			//} else if changed {
			//	n.updatePeerState(peeraddr, DIRTY)
		}
		//log.Warningf("Changed: %v from %s", changed, peeraddr)
		//rows = rows[:0] // saves underlying memory
		return nil
	}

	for msg := range c {
		upd := msg.(tupleUpdate)
		// update if last commit was more than 30 seconds ago or we have 20,000 rows
		if err := commit(upd); err != nil {
			log.Error(err)
			continue
		}
		var anychanged = false
		for _, policy := range n.cfg.PublicPolicy {
			changed, err := n.updateView("public", policy)
			if err != nil {
				log.Error(errors.Wrap(err, "Could not update view"))
			}
			anychanged = anychanged || changed
			log.Warningf("Changed: %v . (any? %v)", changed, anychanged)
		}
		if anychanged {
			n.markAllPeers(DIRTY)
		}
		//if err != nil {
		//} else if changed {
		//	n.markAllPeers(DIRTY)
		//} else {
		//	n.markAllPeers(SYNCED)
		//}
	}
}

func (n *Node) requestUpdates(peer *noise.Peer, peercfg Peer) {
	// periodically loop through peers and request our views

	//go func() {
	//	// check queries
	//	for range time.Tick(5 * time.Second) {
	//		q, err := n.db.ParseQuery("SELECT ?x ?y ?z WHERE { ?x ?y ?z };", 0)
	//		if err != nil {
	//			panic(err)
	//		}
	//		resp, err := n.db.Select(context.Background(), q)
	//		if err != nil {
	//			panic(err)
	//		}
	//		log.Warning("DB now has", len(resp.Rows))
	//	}
	//}()

	// TODO: re-run periodically to check if things change
	peeraddr := fmt.Sprintf("%s:%d", peer.RemoteIP(), peer.RemotePort())
	for range time.Tick(10 * time.Second) {
		state := n.getPeerState(peeraddr)
		if state == SYNCED {
			continue
		}
		fmt.Println(peeraddr, state)
		for _, want := range peercfg.Wants {
			log.Infof("requesting %v from %s", want, peeraddr)
			q, err := n.db.ParseQuery(want.Definition, 0)
			if err != nil {
				log.Error(err)
				continue
			}
			q.Graphs = []string{"public"}

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
			time.Sleep(1 * time.Second)
		}
		n.updatePeerState(peeraddr, SYNCED)
	}
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
