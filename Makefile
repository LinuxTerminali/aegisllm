.PHONY: all build test test-race vet fmt fmt-check docker-build clean

BINARY_DIR := bin
AEGIS_BIN := $(BINARY_DIR)/aegis
MOCK_BIN := $(BINARY_DIR)/mockupstream
BENCHMARK_BIN := $(BINARY_DIR)/benchmark

all: fmt-check vet test-race build

build:
	@mkdir -p $(BINARY_DIR)
	go build -o $(AEGIS_BIN) ./cmd/aegis
	go build -o $(MOCK_BIN) ./cmd/mockupstream
	go build -o $(BENCHMARK_BIN) ./cmd/benchmark

test:
	go test -v ./...

test-race:
	go test -v -race -count=1 ./...

vet:
	go vet ./...

fmt:
	gofmt -s -w .

fmt-check:
	@test -z "$$(gofmt -l .)" || (echo "Code is not formatted. Run 'make fmt'" && gofmt -l . && exit 1)

docker-build:
	docker build -t aegisllm:latest .

clean:
	rm -rf $(BINARY_DIR) coverage.txt coverage.html
