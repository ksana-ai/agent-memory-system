SHELL := /bin/sh

export GOCACHE ?= $(CURDIR)/.cache/go-build

.PHONY: build eval fmt fmt-check test test-race verify vet

build:
	go build ./...

eval:
	go run ./cmd/eval -dataset ./datasets/retrieval-smoke-v1.json

fmt:
	gofmt -w $$(find . -name '*.go' -type f)

fmt-check:
	@test -z "$$(gofmt -l $$(find . -name '*.go' -type f))"

test:
	go test ./...

test-race:
	go test -race ./...

vet:
	go vet ./...

verify: fmt-check vet test test-race build
