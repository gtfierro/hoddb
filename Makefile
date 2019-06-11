RELEASE?=v0.7.2
.PHONY: proto

run: build
	rm -rf _hod_
	./log

install-python-deps:
	python -m pip install grpcio-tools googleapis-common-protos --user

proto: proto/log.proto
	export GOPATH=/home/gabe/go
	protoc -I proto -I /home/gabe/go/src/github.com/grpc-ecosystem/grpc-gateway/third_party/googleapis --go_out=plugins=grpc:proto --grpc-gateway_out=logtostderr=true:proto --swagger_out=logtostderr=true:viz proto/log.proto
	python3 -m grpc_tools.protoc -I proto -I /home/gabe/go/src/github.com/grpc-ecosystem/grpc-gateway/third_party/googleapis --python_out=. --grpc_python_out=. proto/log.proto

build:
	go build -o log

test:
	# the -count=1 flag makes the test non-cacheable
	rm -rf _log_test_
	go test -count=1 -v  ./...

test-insert:
	rm -rf _log_test_
	go test -v -test.run=TestInsert ./...

bench:
	go test -bench=. -test.run=xxxx -v ./...

bench-util:
	go test -bench=Util -test.run=xxxx -v ./...

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
