package turtle

import (
	"strings"
)

const (
	OWL_NAMESPACE   = "http://www.w3.org/2002/07/owl"
	RDF_NAMESPACE   = "http://www.w3.org/1999/02/22-rdf-syntax-ns"
	RDFS_NAMESPACE  = "http://www.w3.org/2000/01/rdf-schema"
	BF_NAMESPACE    = "https://brickschema.org/schema/1.0.3/BrickFrame#"
	BRICK_NAMESPACE = "https://brickschema.org/schema/1.0.3/Brick#"
)

type DataSet struct {
	triplecount int
	nscount     int
	Namespaces  map[string]string
	Triples     []Triple
}

func newDataSet() *DataSet {
	return &DataSet{
		triplecount: 0,
		nscount:     0,
		Namespaces:  make(map[string]string),
		Triples:     []Triple{},
	}
}

func (d *DataSet) AddTripleStrings(subject, predicate, object string) {
	d.triplecount += 1
	d.Triples = append(d.Triples, MakeTriple(subject, predicate, object))
}

func (d *DataSet) AddTripleURIs(subject, predicate, object URI) {
	d.triplecount += 1
	d.Triples = append(d.Triples, Triple{subject, predicate, object})
}

func (d *DataSet) addNamespace(prefix, namespace string) {
	d.nscount += 1
	namespace = strings.TrimRight(namespace, "#")
	d.Namespaces[prefix] = namespace
}

func (d *DataSet) NumTriples() int {
	return d.triplecount
}

func (d *DataSet) NumNamespaces() int {
	return d.nscount
}

//func main() {
//	filename := "Brick.ttl"
//	ds, err := Parse(filename)
//	fmt.Println(ds)
//	fmt.Println(err)
//}
