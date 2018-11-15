package main

import (
	"fmt"
	logpb "github.com/gtfierro/hodlog/proto"
	logrus "github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"net"
)

func (L *Log) ServeGRPC() error {
	port := 47809
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
	if err != nil {
		logrus.Error(err)
		return err
	}
	grpcServer := grpc.NewServer()
	logpb.RegisterHodDBServer(grpcServer, L)

	return grpcServer.Serve(lis)
}
