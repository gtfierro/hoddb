package main

import (
	logpb "github.com/gtfierro/hodlog/proto"
	"testing"
)

func BenchmarkUtilHashURI(b *testing.B) {
	u := &logpb.URI{
		Namespace: "https://brickschema.org/schema/1.0.3/Brick",
		Value:     "VAV",
	}
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		hashURI(u)
	}
}
