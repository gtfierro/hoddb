package hod

import (
	"context"
	"fmt"

	"github.com/golang/protobuf/proto"
	pb "github.com/gtfierro/hoddb/proto"
	turtle "github.com/gtfierro/hoddb/turtle"
	"github.com/pkg/errors"
)

const _GRAPHNAME = "test2"

type inferenceRule2 func() []turtle.Triple

func (hod *HodDB) run_query(qstr string) ([]*pb.Row, error) {
	sq, err := hod.ParseQuery(qstr, 0)
	if err != nil {
		return nil, err
	}
	sq.Graphs = []string{_GRAPHNAME}
	resp, err := hod.Select(context.Background(), sq)
	if err != nil {
		return nil, err
	}
	return resp.Rows, nil
}

func tripleFromRow(row *pb.Row, s, p, o int) turtle.Triple {
	var t turtle.Triple
	if s >= 0 {
		t.Subject = turtle.URI{Namespace: row.Values[s].Namespace, Value: row.Values[s].Value}
	}
	if p >= 0 {
		t.Predicate = turtle.URI{Namespace: row.Values[p].Namespace, Value: row.Values[p].Value}
	}
	if o >= 0 {
		t.Object = turtle.URI{Namespace: row.Values[o].Namespace, Value: row.Values[o].Value}
	}
	return t
}

//add rules to ourself
func (hod *HodDB) inferRules() error {
	// add inverse rules
	// get all pairs of inverse edges
	q := `SELECT ?s ?o WHERE { ?s owl:inverseOf ?o };`
	rows, err := hod.run_query(q)
	if err != nil {
		return err
	}
	for _, row := range rows {
		pred := turtle.URI{Namespace: row.Values[0].Namespace, Value: row.Values[0].Value}
		invpred := turtle.URI{Namespace: row.Values[1].Namespace, Value: row.Values[1].Value}

		inv_func := func() []turtle.Triple {
			var ret []turtle.Triple
			q1 := fmt.Sprintf("SELECT ?s ?o WHERE { ?s <%s> ?o };", pred)
			resp, err := hod.run_query(q1)
			if err != nil {
				log.Error("running inv rule", err)
				return nil
			}
			for _, row := range resp {
				triple := tripleFromRow(row, 1, -1, 0)
				triple.Predicate = invpred
				ret = append(ret, triple)
			}

			return ret
		}
		hod.rules = append(hod.rules, inv_func)

		inv_func2 := func() []turtle.Triple {
			var ret []turtle.Triple
			q1 := fmt.Sprintf("SELECT ?s ?o WHERE { ?s <%s> ?o };", invpred)
			resp, err := hod.run_query(q1)
			if err != nil {
				log.Error("running inv rule", err)
				return nil
			}
			for _, row := range resp {
				triple := tripleFromRow(row, 1, -1, 0)
				triple.Predicate = pred
				ret = append(ret, triple)
			}

			return ret
		}
		hod.rules = append(hod.rules, inv_func2)
	}
	return nil
}

var __select_all_query = `SELECT ?s ?p ?o WHERE { ?s ?p ?o };`

// this is an alternative API for HodDB for incremental maintenance of views
func (hod *HodDB) all_triples() (turtle.DataSet, error) {

	_select_all_query, _ := hod.ParseQuery(__select_all_query, 0)
	resp, err := hod.Select(context.Background(), _select_all_query)
	data := turtle.DataSetFromRows(resp.Rows)
	if err != nil {
		return data, err
	}
	return data, nil
}

// Adds rules to the internal list
// Then, loop through everything to generate any new triples
func (hod *HodDB) AddRules(rules []inferenceRule2) error {
	hod.rules = append(hod.rules, rules...)

	triples, err := hod.all_triples()
	if err != nil {
		return nil
	}
	err = hod.AddTriples(triples)

	return err
}

// adds triples with no inference

// TODO: the problem is that we are overwriting entities when we have new
// tuples about them.  need to have these entities merge in
func (hod *HodDB) addTriples(ds turtle.DataSet) error {
	graph := Graph{
		Name: _GRAPHNAME,
		hod:  hod,
		Data: ds,
	}
	hod.graphs[graph.Name] = struct{}{}
	_ns, found := hod.namespaces.Load(graph.Name)
	if found {
		ns := _ns.(map[string]string)
		for k, v := range graph.Data.Namespaces {
			ns[k] = v
		}
		hod.namespaces.Store(graph.Name, ns)
	} else {
		hod.namespaces.Store(graph.Name, graph.Data.Namespaces)
	}
	entities := graph.CompileEntities()

	log.Println("entities compiled", len(entities))

	txn := hod.db.NewTransaction(true)

	for _, ent := range entities {
		serializedEntry, err := proto.Marshal(ent.compiled)
		if err != nil {
			txn.Discard()
			return errors.Wrap(err, "Error serializing entry")
		}
		if err := hod.setWithCommit(txn, ent.compiled.EntityKey, serializedEntry); err != nil {
			return errors.Wrap(err, "Error txn commit")
		}
	}
	if err := txn.Commit(); err != nil {
		txn.Discard()
		return errors.Wrap(err, "last commit")
	}

	return nil
}

func (hod *HodDB) AddTriples(dataset turtle.DataSet) error {

	//for _, pending_triple := range dataset.Triples {
	//	log.Info(pending_triple.Subject, " ", pending_triple.Predicate, " ", pending_triple.Object)
	//}
	if err := hod.addTriples(dataset); err != nil {
		return err
	}

	stable_triples := make(map[turtle.Triple]int)

	// add triples to the initial set
	for _, triple := range dataset.Triples {
		stable_triples[triple] = 0
	}

	var changed = true
	// run this until no more pending triples
	for changed {
		changed = false
		// add stable triples to database
		dataset.Triples = dataset.Triples[:0]
		for triple := range stable_triples {
			dataset.Triples = append(dataset.Triples, triple)
		}
		if err := hod.addTriples(dataset); err != nil {
			return err
		}

		// apply rules
		for _, rule := range hod.rules {
			generated := rule()
			if generated != nil {
				for _, pending_triple := range generated {
					if _, found := stable_triples[pending_triple]; !found {
						//log.Warning(pending_triple.Subject.Value, " ", pending_triple.Predicate.Value, " ", pending_triple.Object.Value)
						changed = true
						stable_triples[pending_triple] = 0
					}
				}
			}
		}

		log.Println("generated: ", len(stable_triples))
	}

	dataset.Triples = dataset.Triples[:0]
	for triple := range stable_triples {
		dataset.Triples = append(dataset.Triples, triple)
	}

	if err := hod.addTriples(dataset); err != nil {
		return err
	}

	return nil
}
