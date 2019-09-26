package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/pkg/errors"
	logrus "github.com/sirupsen/logrus"

	"github.com/gtfierro/hoddb/hod"
	pb "github.com/gtfierro/hoddb/proto"

	"github.com/golang/protobuf/proto"

	"github.com/perlin-network/noise"
	"github.com/perlin-network/noise/cipher/aead"
	"github.com/perlin-network/noise/handshake/ecdh"
	noiselog "github.com/perlin-network/noise/log"
	"github.com/perlin-network/noise/payload"
	"github.com/perlin-network/noise/protocol"
)

var log = logrus.New()

func init() {
	log.SetFormatter(&logrus.TextFormatter{FullTimestamp: true, ForceColors: true})
	log.SetOutput(os.Stdout)
	log.SetLevel(logrus.DebugLevel)
	noiselog.Disable()
}

var config = flag.String("config", "hodconfig.yml", "Path to hodconfig.yml file")

type header struct {
	Timestamp time.Time
	From      []byte
}

type tupleRequest struct {
	Header     header
	Definition pb.SelectQuery
}

type tupleUpdate struct {
	Header header
	Rows   []*pb.Row
}

func (tupleRequest) Read(reader payload.Reader) (noise.Message, error) {
	_req := &pb.TupleRequest{}
	b, err := reader.ReadBytes()
	if err != nil {
		return nil, err
	}
	if err := proto.Unmarshal(b, _req); err != nil {
		return nil, err
	}

	// TODO: check that all fields exist

	var req tupleRequest
	req.Header = header{
		Timestamp: time.Unix(0, _req.Header.Time),
		From:      _req.Header.From,
	}
	req.Definition = *_req.Definition

	return req, nil
}

func (req tupleRequest) Write() []byte {
	_req := &pb.TupleRequest{
		Header: &pb.P2PHeader{
			Time: req.Header.Timestamp.UnixNano(),
			From: req.Header.From,
		},
		Definition: &req.Definition,
	}

	b, err := proto.Marshal(_req)
	if err != nil {
		panic(err)
	}
	return payload.NewWriter(nil).WriteBytes(b).Bytes()
}

func (tupleUpdate) Read(reader payload.Reader) (noise.Message, error) {
	_upd := &pb.TupleUpdate{}
	b, err := reader.ReadBytes()
	if err != nil {
		return nil, err
	}
	if err := proto.Unmarshal(b, _upd); err != nil {
		return nil, err
	}

	// TODO: check that all fields exist

	var upd tupleUpdate
	upd.Header = header{
		Timestamp: time.Unix(0, _upd.Header.Time),
		From:      _upd.Header.From,
	}
	upd.Rows = _upd.Rows

	return upd, nil
}

func (upd tupleUpdate) Write() []byte {
	_upd := &pb.TupleUpdate{
		Header: &pb.P2PHeader{
			Time: upd.Header.Timestamp.UnixNano(),
			From: upd.Header.From,
		},
		Rows: upd.Rows,
	}

	b, err := proto.Marshal(_upd)
	if err != nil {
		panic(err)
	}
	return payload.NewWriter(nil).WriteBytes(b).Bytes()
}

func main() {
	flag.Parse()
	cfg, err := hod.ReadConfig(*config)
	if err != nil {
		log.Fatal(errors.Wrap(err, "Could not load config file"))
	}
	hod, err := hod.MakeHodDB(cfg)
	if err != nil {
		log.Fatal(errors.Wrap(err, "open log"))
	}

	// Register message type to Noise.
	opcodeRequestMessage := noise.RegisterMessage(noise.NextAvailableOpcode(), (*tupleRequest)(nil))
	opcodeUpdateMessage := noise.RegisterMessage(noise.NextAvailableOpcode(), (*tupleUpdate)(nil))

	params := noise.DefaultParams()

	port, _ := strconv.Atoi(flag.Args()[0])
	params.Port = uint16(port)

	node, err := noise.NewNode(params)
	if err != nil {
		panic(err)
	}

	protocol.New().
		Register(ecdh.New()).
		Register(aead.New()).
		Enforce(node)

	fmt.Printf("Listening for peers on port %d.\n", node.ExternalPort())

	go node.Listen()

	// Dial peer via TCP located at address 127.0.0.1:3001.
	go func() {
		for {
			var peer *noise.Peer
			peer, err = node.Dial(flag.Args()[1])
			if err != nil {
				fmt.Println("peer dial err", err)
				time.Sleep(4 * time.Second)
			} else {
				break
			}
			_ = peer
		}
	}()

	node.OnPeerInit(func(node *noise.Node, peer *noise.Peer) error {

		log.Info("Connected to peer ", peer.RemoteIP().String(), ":", string(peer.RemotePort()))
		// respond to requests
		go func() {
			c := peer.Receive(opcodeRequestMessage)
			for _msg := range c {
				msg := _msg.(tupleRequest)

				fmt.Println(">>>>>", msg)
				res, err := hod.Select(context.Background(), &msg.Definition)
				if err != nil {
					log.Fatal(err)
				} else {
					fmt.Println("res", res)
				}

			}
		}()

		// handle updates
		go func() {
			c := peer.Receive(opcodeUpdateMessage)
			for msg := range c {
				fmt.Println(">>>>>", msg)
			}
		}()

		go func() {
			q, err := hod.ParseQuery(`SELECT ?r WHERE { ?r rdf:type brick:Room };`, 0)
			if err != nil {
				log.Fatal(err)
			}

			for {

				req := tupleRequest{
					Header: header{
						Timestamp: time.Now(),
						From:      []byte("put something better here"),
					},
					Definition: *q,
				}

				err = peer.SendMessage(req)
				if err != nil {
					fmt.Println(err)
				}
				time.Sleep(10 * time.Second)
			}

		}()

		return nil
	})

	select {}

}
