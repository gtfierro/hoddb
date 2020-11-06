export PATH=$PATH:/usr/local/go/bin
# the following will place three binaries in your $GOBIN ($HOME/go/bin)
go get -u github.com/grpc-ecosystem/grpc-gateway/protoc-gen-grpc-gateway
go get -u github.com/grpc-ecosystem/grpc-gateway/protoc-gen-swagger
go get -u github.com/golang/protobuf/protoc-gen-go
export PATH=$PATH:$HOME/go/bin
