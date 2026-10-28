.PHONY: build test fmt vet

build:
	go build -o bin/memc-load ./cmd/memc-load

test:
	go test -race ./...

fmt:
	gofmt -w $$(find . -name '*.go')

vet:
	go vet ./...
