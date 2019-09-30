package hod

import (
	"crypto/sha256"
	"fmt"
	"io"
	"os"
	"sort"

	"github.com/dgraph-io/badger"
	logpb "github.com/gtfierro/hoddb/proto"
	turtle "github.com/gtfierro/hoddb/turtle"
)

const (
	OWL_NAMESPACE  = "http://www.w3.org/2002/07/owl"
	RDF_NAMESPACE  = "http://www.w3.org/1999/02/22-rdf-syntax-ns"
	RDFS_NAMESPACE = "http://www.w3.org/2000/01/rdf-schema"
)

type InferenceRule = func(triple turtle.Triple) []turtle.Triple

type FileBundle struct {
	// name of the graph
	GraphName string
	// the graph to load
	TTLFile string
	// ontology files
	OntologyFiles []string
}

func (bundle FileBundle) getKeyValue() ([]byte, []byte) {
	var files = []string{bundle.TTLFile}
	ontology_files := bundle.OntologyFiles[:]
	sort.Strings(ontology_files)
	files = append(files, ontology_files...)
	h := sha256.New()
	for _, filename := range files {
		f, err := os.Open(filename)
		if err != nil {
			log.Fatal(err)
		}
		defer f.Close()
		if _, err := io.Copy(h, f); err != nil {
			log.Fatal(err)
		}
	}
	return bundle.getKey(), h.Sum(nil)
}

func (bundle FileBundle) getKey() []byte {
	keyname := []byte("filebundle" + bundle.GraphName + bundle.TTLFile)
	ontology_files := bundle.OntologyFiles[:]
	sort.Strings(ontology_files)
	for _, file := range ontology_files {
		keyname = append(keyname, []byte(file)...)
	}
	return keyname
}

type Graph struct {
	// name of the graph
	Name string
	// loaded graph
	Data turtle.DataSet

	rules []InferenceRule
	hod   *HodDB
}

func (hod *HodDB) markBundleLoaded(bundle FileBundle) error {
	key, value := bundle.getKeyValue()
	txn := hod.db.NewTransaction(true)
	if err := txn.Set(key, value); err != nil {
		txn.Discard()
		return err
	}
	if err := txn.Commit(); err != nil {
		txn.Discard()
		return err
	}
	return nil
}

func (hod *HodDB) isFileBundleLoaded(bundle FileBundle) (bool, error) {
	key := bundle.getKey()
	txn := hod.db.NewTransaction(false)
	_, err := txn.Get(key)
	if err == badger.ErrKeyNotFound {
		return false, nil
	} else if err != nil {
		txn.Discard()
		return false, err
	}
	if err := txn.Commit(); err != nil {
		txn.Discard()
		return false, err
	}
	return true, nil
}

func (hod *HodDB) loadFileBundle(bundle FileBundle) (Graph, error) {
	g := Graph{
		Name: bundle.GraphName,
		hod:  hod,
	}

	// load graph
	dataset, err := turtle.Parse(bundle.TTLFile)
	if err != nil {
		return g, err
	}

	// load ontologies
	for _, ontology_file := range bundle.OntologyFiles {
		ontology_dataset, _ := turtle.Parse(ontology_file)
		for _, triple := range ontology_dataset.Triples {
			dataset.Triples = append(dataset.Triples, triple)
		}
	}

	g.Data = dataset

	g.getInferenceRules()

	return g, nil
}

// find some basic OWL inference instances that we can do
func (g *Graph) getInferenceRules() {
	for _, triple := range g.Data.Triples {

		// RULE: populate inverse edges
		if triple.Predicate.Namespace == OWL_NAMESPACE && triple.Predicate.Value == "inverseOf" {
			pred := triple.Subject
			invpred := triple.Object
			newrule := func(input turtle.Triple) []turtle.Triple {
				if input.Predicate == pred {
					return []turtle.Triple{{
						Subject:   input.Object,
						Predicate: invpred,
						Object:    input.Subject,
					}}
				} else if input.Predicate == invpred {
					return []turtle.Triple{{
						Subject:   input.Object,
						Predicate: pred,
						Object:    input.Subject,
					}}
				}
				return nil
			}
			g.rules = append(g.rules, newrule)
		}
	}
}

// apply rules to triples to generate new triples
func (g *Graph) ExpandTriples() {
	solid_triples := make(map[turtle.Triple]int) // stores epoch?
	// triples that we have added through processing
	added_triples := make(map[turtle.Triple]int)
	// triples we need to process
	pending_triples := make(map[turtle.Triple]int)

	// add triples to the initial set
	for _, triple := range g.Data.Triples {
		pending_triples[triple] = 0
	}

	// loop through all of the rules and generate the new triples.
	// Triples we generate go into "pending"

	// run this until no more pending triples
	for len(pending_triples) > 0 || len(added_triples) > 0 {
		// move pending triples to added triples
		for t := range added_triples {
			solid_triples[t] = 0
			delete(added_triples, t)
		}
		for t := range pending_triples {
			added_triples[t] = 0
			delete(pending_triples, t)
		}

		// apply rules
		for _, rule := range g.rules {
			for added_triple := range added_triples {
				generated := rule(added_triple)
				if generated != nil {
					for _, pending_triple := range generated {
						if _, found := solid_triples[pending_triple]; !found {
							pending_triples[pending_triple] = 0
						}
					}
				}
			}
		}

		fmt.Println("solid: ", len(solid_triples), "added: ", len(added_triples), "pending: ", len(pending_triples))
	}

	g.Data.Triples = g.Data.Triples[:0]
	for triple := range solid_triples {
		g.Data.Triples = append(g.Data.Triples, triple)
	}
}

func (g *Graph) CompileEntities() map[EntityKey]*Entity {
	entities := make(map[EntityKey]*Entity)

	getEntity := func(key EntityKey) *Entity {
		ent, found := entities[key]
		if !found {
			ent = newEntity(key)
			entities[key] = ent
		}
		return ent
	}

	// add triples
	for _, triple := range g.Data.Triples {
		subjectHash := g.hod.hashURI(g.Name, triple.Subject)
		predicateHash := g.hod.hashURI(g.Name, triple.Predicate)
		objectHash := g.hod.hashURI(g.Name, triple.Object)

		subject := getEntity(subjectHash)
		subject.addOutEdge(predicateHash, objectHash, logpb.Pattern_Single)

		object := getEntity(objectHash)
		object.addInEdge(predicateHash, subjectHash, logpb.Pattern_Single)

		predicate := getEntity(predicateHash)
		predicate.addEndpoints(subjectHash, objectHash)
	}

	// all entities are generated. compile them into protobuf compatible form
	for _, entity := range entities {
		entity.Compile()
	}

	return entities
}

// what do we need for ad-hoc update sof triples?
// - graph name
// - triples
// - ontology files?

func (hod *HodDB) MakeTripleUpdate(data turtle.DataSet, name string) (Graph, error) {
	// load ontologies
	for _, ontology_file := range hod.cfg.Database.Ontologies {
		ontology_dataset, _ := turtle.Parse(ontology_file)
		for _, triple := range ontology_dataset.Triples {
			data.Triples = append(data.Triples, triple)
		}
	}
	g := Graph{
		Name: name,
		Data: data,
		hod:  hod,
	}
	g.getInferenceRules()

	return g, nil
}

func LoadTriplesFromFile(filename string) (turtle.DataSet, error) {
	d, _ := turtle.Parse(filename)
	return d, nil
}

func LoadTriplesFromFileIntoDataSet(filename string, dataset turtle.DataSet) error {
	d, _ := turtle.Parse(filename)
	for _, triple := range d.Triples {
		dataset.Triples = append(dataset.Triples, triple)
	}
	for g, ns := range d.Namespaces {
		dataset.Namespaces[g] = ns
	}

	return nil
}
