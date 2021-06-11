.PHONY: test
default: build

benchmark:
	go test -bench=. ./...

test:
	go test -v -race -count=1 ./...

build: test benchmark
	go build -o bin/go-dedup cmd/main.go

run: build
	time ./bin/go-dedup
