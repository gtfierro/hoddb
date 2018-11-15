.PHONY: proto

run: build
	rm -rf _hod_
	./log

proto: proto/log.proto
	export GOPATH=/home/gabe/go
	protoc -I proto --go_out=plugins=grpc:proto proto/log.proto 
	# protoc -I proto proto/log.proto

build:
	CGO_CFLAGS_ALLOW=.*/github.com/gtfierro/hodlog/turtle go build

test: proto
	rm -rf _log_test_
	CGO_CFLAGS_ALLOW=.*/github.com/gtfierro/hodlog/turtle go test -v  -test.run=.

test-insert: proto
	rm -rf _log_test_
	CGO_CFLAGS_ALLOW=.*/github.com/gtfierro/hodlog/turtle go test -v -test.run=TestInsert

bench: proto
	CGO_CFLAGS_ALLOW=.*/github.com/gtfierro/hodlog/turtle go test -bench=. -test.run=xxxx -v

bench-util: proto
	CGO_CFLAGS_ALLOW=.*/github.com/gtfierro/hodlog/turtle go test -bench=Util -test.run=xxxx -v

clean:
	rm -rf _hod_
