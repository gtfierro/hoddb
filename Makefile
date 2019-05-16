RELEASE?=v0.6.11
.PHONY: proto

run: build
	rm -rf _hod_
	./log

proto: proto/log.proto
	export GOPATH=/home/gabe/go
	#protoc -I proto --js_out=import_style=commonjs:viz/js --grpc-web_out=import_style=commonjs,mode=grpcwebtext:viz/js --go_out=plugins=grpc:proto proto/log.proto
	protoc -I proto -I /home/gabe/go/src/github.com/grpc-ecosystem/grpc-gateway/third_party/googleapis --go_out=plugins=grpc:proto proto/log.proto
	protoc -I proto -I /home/gabe/go/src/github.com/grpc-ecosystem/grpc-gateway/third_party/googleapis --grpc-gateway_out=logtostderr=true:proto proto/log.proto
	protoc -I proto -I /home/gabe/go/src/github.com/grpc-ecosystem/grpc-gateway/third_party/googleapis --swagger_out=logtostderr=true:viz proto/log.proto
	# protoc -I proto proto/log.proto

build:
	CGO_CFLAGS_ALLOW=.*/git.sr.ht/%7Egabe/hod/turtle go build -o log

test:
	# the -count=1 flag makes the test non-cacheable
	rm -rf _log_test_
	CGO_CFLAGS_ALLOW=.*/git.sr.ht/%7Egabe/hod/turtle go test -count=1 -v  ./...

test-insert:
	rm -rf _log_test_
	CGO_CFLAGS_ALLOW=.*/git.sr.ht/%7Egabe/hod/turtle go test -v -test.run=TestInsert ./...

bench:
	CGO_CFLAGS_ALLOW=.*/git.sr.ht/%7Egabe/hod/turtle go test -bench=. -test.run=xxxx -v ./...

bench-util:
	CGO_CFLAGS_ALLOW=.*/git.sr.ht/%7Egabe/hod/turtle go test -bench=Util -test.run=xxxx -v ./...

clean:
	rm -rf _hod_

container: build
	mv ./log containers/hoddb
	docker build -t mortar/hoddb:$(RELEASE) containers/hoddb
	docker build -t mortar/hoddb:latest containers/hoddb

viz-container:
	go build -o containers/viz/fileserver ./viz/fileserver
	cp -r viz/ containers/viz/
	docker build -t mortar/hodviz:$(RELEASE) containers/viz
	docker build -t mortar/hodviz:latest containers/viz


push-container: container
	docker push mortar/hoddb:$(RELEASE)
	docker push mortar/hoddb:latest

push-viz-container: viz-container
	docker push mortar/hodviz:$(RELEASE)
	docker push mortar/hodviz:latest
