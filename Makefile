binarydir="./bin"

build: build-client build-server build-worker

.PHONY: build-client
build-client:
	[ -d $(binarydir) ] || mkdir -p $(binarydir)
	go build -o $(binarydir)/client ./cmd/client.go

.PHONY: build-server
build-server:
	[ -d $(binarydir) ] || mkdir -p $(binarydir)
	go build -o $(binarydir)/server ./cmd/server.go

.PHONY: build-worker
build-worker:
	[ -d $(binarydir) ] || mkdir -p $(binarydir)
	go build -o $(binarydir)/worker ./cmd/worker.go

clean:
	rm -rf $(binarydir)
