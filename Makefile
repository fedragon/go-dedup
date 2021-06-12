.PHONY: test
default: all

benchmark:
	go test -bench=. ./...

test:
	go test -v -race -count=1 ./...

build:
	go build -o bin/dedup cmd/main.go

all:  build test benchmark

run: build
	time ./bin/dedup
