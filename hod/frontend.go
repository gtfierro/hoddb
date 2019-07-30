package hod

import (
	"context"
	"fmt"
	"github.com/grpc-ecosystem/grpc-gateway/runtime"
	logpb "github.com/gtfierro/hoddb/proto"
	"github.com/rs/cors"
	logrus "github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"net"
	"net/http"
)

func (hod *HodDB) ServeGRPC() error {
	port := 47808
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
	if err != nil {
		logrus.Error(err)
		return err
	}
	grpcServer := grpc.NewServer()
	logpb.RegisterHodDBServer(grpcServer, hod)

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
		log.Info("Serve on :47809")
		log.Fatal(http.ListenAndServe(":47809", corsc.Handler(mux)))
	}()

	return grpcServer.Serve(lis)
}
