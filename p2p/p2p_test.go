package main

import (
	"context"
	"fmt"
	"io/ioutil"
	"strings"
	"testing"
	"time"

	pb "github.com/gtfierro/hoddb/proto"
	"github.com/stretchr/testify/require"
)

// port (int), building (string)
var configTemplate = `
ListenPort = %d

[HodConfig.Database]
Path = "%s"
Ontologies = ["../BrickFrame.ttl","../Brick.ttl"]
Buildings = {test="%s"}
[HodConfig.Http]
Enable = false
[HodConfig.Grpc]
Enable = false
[HodConfig.Output]
LogLevel = "info"
`
var public_policy_all = View{
	Graphs:     []string{"test"},
	Definition: `SELECT ?s ?p ?o WHERE { ?s ?p ?o };`,
}

var points_only_policy = View{
	Graphs:     []string{"test"},
	Definition: `SELECT ?s WHERE { ?s rdf:type/rdfs:subClassOf* brick:Point };`,
}

func setup_node(port int, initialfile string, publicpolicy []View, peers []Peer) (*Node, error) {
	dir, err := ioutil.TempDir("", "_log_test_")
	if err != nil {
		return nil, err
	}
	cfgStr := fmt.Sprintf(configTemplate, port, dir, initialfile)
	cfg, err := ReadConfig(strings.NewReader(cfgStr))
	if err != nil {
		return nil, err
	}
	cfg.Peer = append(cfg.Peer, peers...)
	cfg.PublicPolicy = append(cfg.PublicPolicy, publicpolicy...)
	node, err := NewNode(&cfg)
	if err != nil {
		return nil, err
	}
	return node, nil
}

func run_query(node *Node, graph, qstr string) ([]*pb.Row, error) {
	sq, err := node.db.ParseQuery(qstr, 0)
	if err != nil {
		return nil, err
	}
	sq.Graphs = []string{graph}
	resp, err := node.db.Select(context.Background(), sq)
	if err != nil {
		return nil, err
	}
	return resp.Rows, nil
}

func TestChangesPropagate(t *testing.T) {
	require := require.New(t)
	node, err := setup_node(3000, "../example.ttl", []View{public_policy_all}, nil)
	require.NoError(err, "setup node 1")
	require.NotNil(node, "setup node 1")
	defer node.Shutdown()

	peer1 := Peer{
		Address: "localhost:3000",
		Wants: []View{
			View{Definition: `SELECT ?s ?p ?o WHERE { ?s ?p ?o };`},
		},
	}
	node2, err := setup_node(3001, "ns.ttl", nil, []Peer{peer1})
	require.NoError(err, "setup node 2")
	require.NotNil(node2, "setup node 2")
	defer node2.Shutdown()

	res, err := run_query(node2, "test", "SELECT ?s ?p ?o WHERE { ?s ?p ?o };")
	require.NoError(err, "query node2")
	require.Equal(8289, len(res), "results node2")

	time.Sleep(30 * time.Second)
	res2, err := run_query(node2, "test", "SELECT ?s ?p ?o WHERE { ?s ?p ?o };")
	require.NoError(err, "query node2")
	require.Equal(8310, len(res2), "results node2")
}

func TestChangesPropagate2(t *testing.T) {
	require := require.New(t)
	node, err := setup_node(3000, "../example.ttl", []View{points_only_policy}, nil)
	require.NoError(err, "setup node 1")
	require.NotNil(node, "setup node 1")
	defer node.Shutdown()

	peer1 := Peer{
		Address: "localhost:3000",
		Wants: []View{
			View{Definition: `SELECT ?s ?p ?o WHERE { ?s ?p ?o };`},
		},
	}
	node2, err := setup_node(3001, "ns.ttl", nil, []Peer{peer1})
	require.NoError(err, "setup node 2")
	require.NotNil(node2, "setup node 2")
	defer node2.Shutdown()

	res, err := run_query(node2, "test", "SELECT ?s ?p ?o WHERE { ?s ?p ?o };")
	require.NoError(err, "query node2")
	require.Equal(8289, len(res), "results node2")

	time.Sleep(30 * time.Second)
	res2, err := run_query(node2, "test", "SELECT ?s ?p ?o WHERE { ?s ?p ?o };")
	require.NoError(err, "query node2")
	require.Equal(8290, len(res2), "results node2")
}

// root node gets populated with data from the leaf nodes
func TestStarTopo(t *testing.T) {
	require := require.New(t)

	// set up leafnodes
	leaf1, err := setup_node(3001, "test_ttl/leaf1.ttl", []View{public_policy_all}, nil)
	require.NoError(err, "setup leaf1")
	require.NotNil(leaf1, "setup leaf1")
	defer leaf1.Shutdown()

	leaf2, err := setup_node(3002, "test_ttl/leaf2.ttl", []View{public_policy_all}, nil)
	require.NoError(err, "setup leaf2")
	require.NotNil(leaf2, "setup leaf2")
	defer leaf2.Shutdown()

	// peer declarations for the root node
	leaf1_peer := Peer{
		Address: "localhost:3001",
		Wants: []View{
			View{Definition: `SELECT ?s ?p ?o WHERE { ?s ?p ?o };`},
		},
	}
	_ = leaf1_peer
	leaf2_peer := Peer{
		Address: "localhost:3002",
		Wants: []View{
			View{Definition: `SELECT ?s ?p ?o WHERE { ?s ?p ?o };`},
		},
	}

	root, err := setup_node(3000, "../example.ttl", nil, []Peer{leaf1_peer, leaf2_peer})
	require.NoError(err, "setup root")
	require.NotNil(root, "setup root")
	defer root.Shutdown()

	time.Sleep(30 * time.Second)
	res, err := run_query(root, "test", "SELECT ?s WHERE { ?s rdf:type brick:Temperature_Sensor };")
	require.NoError(err, "query root")
	require.Equal(2, len(res), "results root")
}

func TestTransitive(t *testing.T) {
	require := require.New(t)

	node1, err := setup_node(3000, "test_ttl/leaf1.ttl", []View{public_policy_all}, nil)
	require.NoError(err, "setup node1")
	node1_peer := Peer{
		Address: "localhost:3000",
		Wants: []View{
			View{Definition: `SELECT ?s ?p ?o WHERE { ?s ?p ?o };`},
		},
	}
	defer node1.Shutdown()

	node2, err := setup_node(3001, "test_ttl/leaf2.ttl", []View{public_policy_all}, []Peer{node1_peer})
	require.NoError(err, "setup node2")
	node2_peer := Peer{
		Address: "localhost:3001",
		Wants: []View{
			View{Definition: `SELECT ?s ?p ?o WHERE { ?s ?p ?o };`},
		},
	}
	defer node2.Shutdown()

	node3, err := setup_node(3002, "ns.ttl", []View{public_policy_all}, []Peer{node2_peer})
	require.NoError(err, "setup node1")
	defer node3.Shutdown()

	time.Sleep(30 * time.Second)
	res, err := run_query(node3, "test", "SELECT ?s WHERE { ?s rdf:type brick:Temperature_Sensor };")
	require.NoError(err, "query")
	require.Equal(2, len(res), "results")
}
