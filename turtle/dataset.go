package turtle

import (
	"fmt"
	"sort"
	"strings"

	pb "github.com/gtfierro/hoddb/proto"
	"github.com/spaolacci/murmur3"
)

const (
	OWL_NAMESPACE   = "http://www.w3.org/2002/07/owl"
	RDF_NAMESPACE   = "http://www.w3.org/1999/02/22-rdf-syntax-ns"
	RDFS_NAMESPACE  = "http://www.w3.org/2000/01/rdf-schema"
	BF_NAMESPACE    = "https://brickschema.org/schema/1.0.3/BrickFrame#"
	BRICK_NAMESPACE = "https://brickschema.org/schema/1.0.3/Brick#"
)

var defaultNamespaces = map[string]string{
	"owl":   OWL_NAMESPACE,
	"rdf":   RDF_NAMESPACE,
	"rdfs":  RDFS_NAMESPACE,
	"bf":    BF_NAMESPACE,
	"brick": BRICK_NAMESPACE,
}

type DataSet struct {
	triplecount int
	nscount     int
	Namespaces  map[string]string
	Triples     []Triple
}

func NewDataSet() *DataSet {
	ds := &DataSet{
		triplecount: 0,
		nscount:     0,
		Namespaces:  make(map[string]string),
		Triples:     []Triple{},
	}
	for k, v := range defaultNamespaces {
		ds.addNamespace(k, v)
	}
	return ds
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

func (d *DataSet) Hash() []byte {
	h := murmur3.New64()
	var s = make([]string, len(d.Triples))
	for idx, triple := range d.Triples {
		s[idx] = fmt.Sprintf("%s%s%s", triple.Subject, triple.Predicate, triple.Object)
	}
	sort.Strings(s)
	for _, t := range s {
		h.Write([]byte(t))
	}
	return h.Sum(nil)
}

func DataSetFromRows(rows []*pb.Row) DataSet {
	d := NewDataSet()
	// TODO: assuming triples
	for _, row := range rows {
		s := URI{Namespace: row.Values[0].Namespace, Value: row.Values[0].Value}
		p := URI{Namespace: row.Values[1].Namespace, Value: row.Values[1].Value}
		o := URI{Namespace: row.Values[2].Namespace, Value: row.Values[2].Value}
		d.AddTripleURIs(s, p, o)
	}
	return *d
}

//func main() {
//	filename := "Brick.ttl"
//	ds, err := Parse(filename)
//	fmt.Println(ds)
//	fmt.Println(err)
//}
