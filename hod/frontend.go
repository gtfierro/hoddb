package hod

import (
	"context"
	"fmt"
	logpb "git.sr.ht/~gabe/hod/proto"
	"github.com/grpc-ecosystem/grpc-gateway/runtime"
	"github.com/rs/cors"
	logrus "github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"net"
	"net/http"
)

func (L *Log) ServeGRPC() error {
	port := 47808
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
	if err != nil {
		logrus.Error(err)
		return err
	}
	grpcServer := grpc.NewServer()
	logpb.RegisterHodDBServer(grpcServer, L)

	corsc := cors.New(cors.Options{
		AllowedOrigins:   []string{"*"},
		AllowedMethods:   []string{"GET", "POST"},
		AllowCredentials: true,
		Debug:            false,
	})
	mux := runtime.NewServeMux()
	opts := []grpc.DialOption{
		grpc.WithInsecure(),
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(20 * 1024 * 1024)),
	}
	if err := logpb.RegisterHodDBHandlerFromEndpoint(context.Background(), mux, ":47808", opts); err != nil {
		log.Error(err)
		panic(err)
	}
	httpmux := http.NewServeMux()
	httpmux.Handle("/", mux)
	go func() {
		log.Fatal(http.ListenAndServe(":47809", corsc.Handler(mux)))
	}()

	return grpcServer.Serve(lis)
}
