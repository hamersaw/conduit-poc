binary_dir="./bin"
lyft_image="lyft/protocgenerator:8167e11d3b3439373c2f033080a4b550078884a2"

build: generate-protos build-client build-server build-worker

.PHONY: build-client
build-client:
	[ -d $(binary_dir) ] || mkdir -p $(binary_dir)
	go build -o $(binary_dir)/client ./cmd/client.go

.PHONY: build-server
build-server:
	[ -d $(binary_dir) ] || mkdir -p $(binary_dir)
	go build -o $(binary_dir)/server ./cmd/server.go

.PHONY: build-worker
build-worker:
	[ -d $(binary_dir) ] || mkdir -p $(binary_dir)
	go build -o $(binary_dir)/worker ./cmd/worker.go

generate-protos:
	rm -r ./protos/gen
	docker run --rm -u $(shell id -u):$(shell id -g) -v $(shell pwd):/defs $(lyft_image) -i ./protos -d protos -l go --go_source_relative --validate_out
	mv ./gen ./protos

clean:
	rm -rf $(binary_dir) ./protos/gen
