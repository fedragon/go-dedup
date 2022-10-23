.PHONY: test
default: all

benchmark:
	go test -bench=. ./...

lint:
	golangci-lint run ./...

test:
	go test -v -race -count=1 ./...

build:
	go build -o bin/dedup cmd/main.go

all:  build test benchmark
