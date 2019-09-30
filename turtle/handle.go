package turtle

import (
	pb "github.com/gtfierro/hoddb/proto"
	rdf "github.com/gtfierro/hoddb/turtle/rdfparser"
	"io"
	"os"
	"strings"
)

type URI struct {
	Namespace string `msg:"n"`
	Value     string `msg:"v"`
}

func (u URI) String() string {
	if u.Namespace != "" {
		return u.Namespace + "#" + u.Value
	}
	return u.Value
}

func (u URI) Bytes() []byte {
	if u.Namespace != "" {
		return []byte(u.Namespace + "#" + u.Value)
	}
	return []byte(u.Value)
}

func (u URI) IsVariable() bool {
	return strings.HasPrefix(u.Value, "?")
}

func (u URI) IsEmpty() bool {
	return len(u.Namespace) == 0 && len(u.Value) == 0
}

func ParseURI(uri string) URI {
	uri = strings.TrimLeft(uri, "<")
	uri = strings.TrimRight(uri, ">")
	parts := strings.Split(uri, "#")
	parts[0] = strings.TrimRight(parts[0], "#")
	if len(parts) != 2 {
		if strings.HasPrefix(uri, "\"") {
			uri = strings.TrimSuffix(uri, "@en")
			//uri = strings.Trim(uri, "\"")
			return URI{Value: uri}
		}
		// try to parse ":"
		parts = strings.SplitN(uri, ":", 2)
		if len(parts) > 1 {
			return URI{Namespace: parts[0], Value: parts[1]}
		}
		uri = strings.TrimSuffix(uri, "@en")
		//uri = strings.Trim(uri, "\"")
		return URI{Value: uri}
	}
	return URI{Namespace: parts[0], Value: parts[1]}
}

type Triple struct {
	Subject   URI `msg:"s"`
	Predicate URI `msg:"p"`
	Object    URI `msg:"o"`
}

func MakeTriple(sub, pred, obj string) Triple {
	s := ParseURI(sub)
	p := ParseURI(pred)
	o := ParseURI(obj)
	return Triple{
		Subject:   s,
		Predicate: p,
		Object:    o,
	}
}

func TripleFromRow(row pb.Row) Triple {
	return Triple{
		Subject:   URI{Namespace: row.Values[0].Namespace, Value: row.Values[0].Value},
		Predicate: URI{Namespace: row.Values[1].Namespace, Value: row.Values[1].Value},
		Object:    URI{Namespace: row.Values[2].Namespace, Value: row.Values[2].Value},
	}
}

// Parses the given filename using the turtle format.
// Returns the dataset, and the time elapsed in parsing
func Parse(filename string) (DataSet, error) {
	dataset := newDataSet()
	f, err := os.Open(filename)
	if err != nil {
		return *dataset, err
	}
	dec := rdf.NewTripleDecoder(f, rdf.Turtle)
	for triple, err := dec.Decode(); err != io.EOF; triple, err = dec.Decode() {
		dataset.AddTripleStrings(triple.Subj.String(), triple.Pred.String(), triple.Obj.String())
	}
	for ns, uri := range dec.Namespaces() {
		dataset.addNamespace(ns, uri)
	}

	return *dataset, nil
}
