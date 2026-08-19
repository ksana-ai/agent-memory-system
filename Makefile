SHELL := /bin/sh

export GOCACHE ?= $(CURDIR)/.cache/go-build

POSTGRES_DB ?= agent_memory
POSTGRES_USER ?= agent_memory
POSTGRES_PASSWORD ?= agent_memory_dev
POSTGRES_PORT ?= 55432
export POSTGRES_DB POSTGRES_USER POSTGRES_PASSWORD POSTGRES_PORT

DATABASE_URL ?= postgres://$(POSTGRES_USER):$(POSTGRES_PASSWORD)@127.0.0.1:$(POSTGRES_PORT)/$(POSTGRES_DB)?sslmode=disable
TEST_DATABASE_URL ?= $(DATABASE_URL)
export DATABASE_URL TEST_DATABASE_URL
SERVER_BINARY := $(CURDIR)/bin/agent-memory-server
EVAL_BINARY := $(CURDIR)/bin/agent-memory-eval
GO_FILES := $(shell find . -path './.cache' -prune -o -path './.git' -prune -o -name '*.go' -type f -print)

.PHONY: build build-eval build-server db-down db-up eval fmt fmt-check migrate server test test-integration test-race verify verify-postgres vet

build:
	go build ./...

build-server:
	mkdir -p $(CURDIR)/bin
	go build -o $(SERVER_BINARY) ./cmd/server

build-eval:
	mkdir -p $(CURDIR)/bin
	go build -o $(EVAL_BINARY) ./cmd/eval

db-up:
	docker compose up -d --wait postgres

db-down:
	docker compose down

eval: build-eval
	$(EVAL_BINARY) -dataset ./datasets/retrieval-smoke-v1.json

fmt:
	gofmt -w $(GO_FILES)

fmt-check:
	@test -z "$$(gofmt -l $(GO_FILES))"

test:
	go test ./...

test-integration: migrate build-server
	@TEST_SERVER_BINARY='$(SERVER_BINARY)' \
		go test -race -tags=integration -count=1 ./internal/store/postgres ./cmd/server

test-race:
	go test -race ./...

vet:
	go vet ./...

verify: fmt-check vet test test-race build

migrate: db-up
	@go run ./cmd/migrate

server: db-up
	@go run ./cmd/server

verify-postgres: test-integration
