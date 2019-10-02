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
	node2, err := setup_node(3001, "../ns.ttl", nil, []Peer{peer1})
	require.NoError(err, "setup node 2")
	require.NotNil(node2, "setup node 2")

	res, err := run_query(node2, "test", "SELECT ?s ?p ?o WHERE { ?s ?p ?o };")
	require.NoError(err, "query node2")
	require.Equal(8289, len(res), "results node2")

	time.Sleep(20 * time.Second)
	res2, err := run_query(node2, "test", "SELECT ?s ?p ?o WHERE { ?s ?p ?o };")
	require.NoError(err, "query node2")
	require.Equal(8310, len(res2), "results node2")
	defer node2.Shutdown()
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
	node2, err := setup_node(3001, "../ns.ttl", nil, []Peer{peer1})
	require.NoError(err, "setup node 2")
	require.NotNil(node2, "setup node 2")
	defer node2.Shutdown()

	res, err := run_query(node2, "test", "SELECT ?s ?p ?o WHERE { ?s ?p ?o };")
	require.NoError(err, "query node2")
	require.Equal(8289, len(res), "results node2")

	time.Sleep(20 * time.Second)
	res2, err := run_query(node2, "test", "SELECT ?s ?p ?o WHERE { ?s ?p ?o };")
	require.NoError(err, "query node2")
	require.Equal(8290, len(res2), "results node2")
}
