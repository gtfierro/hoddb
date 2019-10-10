package main

import (
	"context"
	"fmt"
	"log"

	"github.com/gtfierro/hoddb/hod"
	"github.com/gtfierro/hoddb/p2p"
	rdf "github.com/gtfierro/hoddb/turtle"
	"github.com/pkg/errors"
)

type NetworkMonitor struct {
	node *p2p.Node
	db   *hod.HodDB
	cfg  *p2p.Config
}

func NewNetworkMonitor(cfg *p2p.Config) (*NetworkMonitor, error) {
	var err error
	mon := new(NetworkMonitor)
	mon.cfg = cfg
	mon.node, err = p2p.NewNode(cfg)
	//TODO: method
	if err != nil {
		return nil, errors.Wrap(err, "make hoddb")
	}
	mon.db = mon.node.GetHodDB()
	go func() {
		log.Fatal(mon.db.ServeGRPC())
	}()
	return mon, nil
}

func (mon *NetworkMonitor) getEntitiesWithProperty(pred, obj rdf.URI) []rdf.URI {
	q := fmt.Sprintf(`SELECT ?e WHERE { ?e <%s> <%s> };`, pred, obj)
	res, err := mon.runQuery(q)
	if err != nil {
		panic(err)
	}
	var ret []rdf.URI
	for _, r := range res {
		ret = append(ret, r[0])
	}
	return ret
}

func (mon *NetworkMonitor) runQuery(qstr string) ([][]rdf.URI, error) {
	var res [][]rdf.URI
	q, err := mon.db.ParseQuery(qstr, 0)
	if err != nil {
		return nil, err
	}
	resp, err := mon.db.Select(context.Background(), q)
	if err != nil {
		return nil, err
	}
	for _, row := range resp.Rows {
		var nr []rdf.URI
		for _, u := range row.Values {
			nr = append(nr, rdf.URI{Namespace: u.Namespace, Value: u.Value})
		}
		res = append(res, nr)
	}
	return res, nil
}

func (mon *NetworkMonitor) AddTriples(triples []rdf.Triple) error {
	var ds = rdf.NewDataSet()
	ds.Triples = triples
	ds.AddNamespace("net", string(NETWORK))
	ds.AddNamespace("mynet", string(MYNET))
	err := mon.db.AddTriples("test", *ds)
	fmt.Println("added triples", len(triples))
	return err
}
