package p2p

import (
	"time"

	"github.com/golang/protobuf/proto"

	pb "github.com/gtfierro/hoddb/proto"
	"github.com/perlin-network/noise"
	"github.com/perlin-network/noise/payload"
)

func init() {
	opcodeRequestMessage = noise.RegisterMessage(noise.NextAvailableOpcode(), (*tupleRequest)(nil))
	opcodeUpdateMessage = noise.RegisterMessage(noise.NextAvailableOpcode(), (*tupleUpdate)(nil))
}

var opcodeRequestMessage noise.Opcode
var opcodeUpdateMessage noise.Opcode

type header struct {
	Timestamp time.Time
	From      []byte
}

type tupleRequest struct {
	Header     header
	Definition pb.SelectQuery
}

type tupleUpdate struct {
	Header     header
	Rows       []*pb.Row
	Vars       []string
	Definition pb.SelectQuery
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
func (req tupleRequest) ToProto() *pb.TupleRequest {
	return &pb.TupleRequest{
		Header: &pb.P2PHeader{
			Time: req.Header.Timestamp.UnixNano(),
			From: req.Header.From,
		},
		Definition: &req.Definition,
	}
}

func (req tupleRequest) Write() []byte {
	b, err := proto.Marshal(req.ToProto())
	if err != nil {
		panic(err)
	}
	return payload.NewWriter(nil).WriteBytes(b).Bytes()
}

func (upd tupleUpdate) ToProto() *pb.TupleUpdate {
	return &pb.TupleUpdate{
		Header: &pb.P2PHeader{
			Time: upd.Header.Timestamp.UnixNano(),
			From: upd.Header.From,
		},
		Rows:       upd.Rows,
		Vars:       upd.Vars,
		Definition: &upd.Definition,
	}
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
	upd.Definition = *_upd.Definition

	return upd, nil
}

func (upd tupleUpdate) Write() []byte {
	_upd := upd.ToProto()

	b, err := proto.Marshal(_upd)
	if err != nil {
		panic(err)
	}
	return payload.NewWriter(nil).WriteBytes(b).Bytes()
}
