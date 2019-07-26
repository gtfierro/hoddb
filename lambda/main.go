package main

import (
	"context"
	"fmt"
	"github.com/gtfierro/hoddb/hod"
	logpb "github.com/gtfierro/hoddb/proto"
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/pkg/errors"
	"io"
	"log"
	"os"
)

const _config = `
database:
    path: "_hod_"

http:
    enable: false
    address: localhost
    port: 47808

grpc:
    enable: false
    address: localhost
    port: 47808

profile:
    enableCpu: false
    enableMem: false
    enableBlock: false
    enableHttp: false
    httpPort: 6061

output:
    loglevel: debug`

var cfg *hod.Config

type LambdaQuery struct {
	Query string `json:"query"`
	Graph string `json:"graph"`
}

type Instance struct {
	hod          *hod.HodDB
	loaded_graph string
}

var db Instance

func LoadGraph(name string) io.Reader {
	var svc = s3.New(session.New(&aws.Config{
		Region: aws.String("us-west-1")},
	))
	result, err := svc.GetObject(&s3.GetObjectInput{
		Bucket: aws.String("hoddb"),
		Key:    aws.String(fmt.Sprintf("%s.badger", name)),
	})
	if err != nil {
		log.Fatal(errors.Wrap(err, "Could not get database from s3"))
	}
	return result.Body
}

func HandleLambdaEvent(ctx context.Context, event LambdaQuery) (*logpb.Response, error) {
	var err error
	// load graph if not here
	fmt.Printf("Handling event %+v\n", event)
	if db.loaded_graph != event.Graph {
		fmt.Println("Loading graph", event.Graph)
		db.hod.Close()
		graph := LoadGraph(event.Graph)
		db.loaded_graph = event.Graph
		db.hod, err = hod.MakeHodDBLambda(cfg, graph)
		if err != nil {
			log.Fatal(errors.Wrap(err, "open log"))
		}
	}

	q, err := db.hod.ParseQuery(event.Query, 0)
	if err != nil {
		return nil, err
	}
	fmt.Printf("%+v\n", q)
	res, err := db.hod.Select(ctx, q)
	if err != nil {
		return nil, err
	}

	fmt.Printf("Request %+v\n", event)
	return res, nil
}

func main() {

	var backup io.Reader
	var err error

	if true {
		backup = LoadGraph("ciee")
	} else {
		backup, _ = os.Open("make_backup/graphs/ACAD.badger")
	}
	// https://godoc.org/github.com/aws/aws-sdk-go/service/s3#GetObjectOutput

	cfg, err = hod.ReadConfigFromString(_config)
	if err != nil {
		log.Fatal(errors.Wrap(err, "Could not load config file"))
	}

	fmt.Printf("%+v\n", cfg)

	db.hod, err = hod.MakeHodDBLambda(cfg, backup)
	if err != nil {
		log.Fatal(errors.Wrap(err, "open log"))
	}
	fmt.Println("Done")

	a, err := HandleLambdaEvent(context.Background(), LambdaQuery{
		Query: "SELECT ?r WHERE { ?r rdf:type brick:Room };",
		Graph: "ciee",
	})
	if err != nil {
		log.Fatal(errors.Wrap(err, "fail query"))
	}
	fmt.Println(a)

	lambda.Start(HandleLambdaEvent)
}
