package main

import (
	"time"

	"github.com/golang/protobuf/proto"

	pb "github.com/gtfierro/hoddb/proto"
	"github.com/perlin-network/noise"
	"github.com/perlin-network/noise/payload"
)

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
	Vars   []string
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
	upd.Vars = _upd.Vars

	return upd, nil
}

func (upd tupleUpdate) Write() []byte {
	_upd := &pb.TupleUpdate{
		Header: &pb.P2PHeader{
			Time: upd.Header.Timestamp.UnixNano(),
			From: upd.Header.From,
		},
		Rows: upd.Rows,
		Vars: upd.Vars,
	}

	b, err := proto.Marshal(_upd)
	if err != nil {
		panic(err)
	}
	return payload.NewWriter(nil).WriteBytes(b).Bytes()
}
