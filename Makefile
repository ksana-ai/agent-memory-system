SHELL := /bin/sh

export GOCACHE ?= $(CURDIR)/.cache/go-build
GO_CHECKSUM_DB ?= sum.golang.org
GO := env GOSUMDB=$(GO_CHECKSUM_DB) go

POSTGRES_DB ?= agent_memory
POSTGRES_USER ?= agent_memory
POSTGRES_PASSWORD ?= agent_memory_dev
POSTGRES_PORT ?= 55432
export POSTGRES_DB POSTGRES_USER POSTGRES_PASSWORD POSTGRES_PORT

DATABASE_URL ?= postgres://$(POSTGRES_USER):$(POSTGRES_PASSWORD)@127.0.0.1:$(POSTGRES_PORT)/$(POSTGRES_DB)?sslmode=disable
TEST_DATABASE_URL ?= $(DATABASE_URL)
export DATABASE_URL TEST_DATABASE_URL
LMSTUDIO_EMBEDDINGS_URL ?= http://127.0.0.1:1234/v1/embeddings
LMSTUDIO_EMBEDDING_MODEL ?= text-embedding-bge-m3
export LMSTUDIO_EMBEDDINGS_URL LMSTUDIO_EMBEDDING_MODEL
SERVER_BINARY := $(CURDIR)/bin/agent-memory-server
EVAL_BINARY := $(CURDIR)/bin/agent-memory-eval
EVAL_V2_DATASET := $(CURDIR)/datasets/memory-lifecycle-v2.json
EVAL_SEMANTIC_V1_DATASET := $(CURDIR)/datasets/memory-semantic-extension-v1.json
EVAL_V2_MANIFEST := $(CURDIR)/artifacts/eval/memory-lifecycle-v2-latest.json
EVAL_POSTGRES_V2_MANIFEST := $(CURDIR)/artifacts/eval/memory-lifecycle-v2-postgres-fts-latest.json
EVAL_VECTOR_V2_MANIFEST := $(CURDIR)/artifacts/eval/memory-lifecycle-v2-postgres-vector-latest.json
EVAL_SEMANTIC_V1_MANIFEST := $(CURDIR)/artifacts/eval/memory-semantic-extension-v1-postgres-vector-latest.json
GO_FILES := $(shell find . -path './.cache' -prune -o -path './.git' -prune -o -name '*.go' -type f -print)

.PHONY: build build-eval build-server db-down db-up eval eval-postgres eval-postgres-recorded eval-recorded eval-semantic eval-semantic-recorded eval-v2 eval-vector eval-vector-recorded fmt fmt-check migrate semantic-frozen server test test-integration test-outbox-integration test-race test-vector-integration verify verify-postgres verify-semantic verify-vector vet

build:
	$(GO) build ./...

build-server:
	mkdir -p $(CURDIR)/bin
	$(GO) build -o $(SERVER_BINARY) ./cmd/server

build-eval:
	mkdir -p $(CURDIR)/bin
	$(GO) build -o $(EVAL_BINARY) ./cmd/eval

db-up:
	docker compose up -d --wait postgres

db-down:
	docker compose down

eval: build-eval
	$(EVAL_BINARY) -dataset ./datasets/retrieval-smoke-v1.json

eval-v2: build-eval
	@$(EVAL_BINARY) -dataset $(EVAL_V2_DATASET) -arms no-memory-v1,reviewed-cards-bm25-v1 -k 5 -ndcg-k 10 -measured-runs 1 -query-timeout 5s -require-policy-pass -quiet

eval-postgres: migrate build-eval
	@$(EVAL_BINARY) -dataset $(EVAL_V2_DATASET) -arms no-memory-v1,reviewed-cards-bm25-v1,reviewed-cards-postgres-fts-v1 -k 5 -ndcg-k 10 -measured-runs 1 -query-timeout 5s -require-policy-pass -quiet

eval-postgres-recorded: migrate build-eval
	@$(EVAL_BINARY) -dataset $(EVAL_V2_DATASET) -arms no-memory-v1,reviewed-cards-bm25-v1,reviewed-cards-postgres-fts-v1 -k 5 -ndcg-k 10 -measured-runs 3 -query-timeout 5s -require-policy-pass -require-clean -output $(EVAL_POSTGRES_V2_MANIFEST) -quiet

eval-vector: migrate build-eval
	@$(EVAL_BINARY) -dataset $(EVAL_V2_DATASET) -arms no-memory-v1,reviewed-cards-bm25-v1,reviewed-cards-postgres-fts-v1,reviewed-cards-postgres-vector-v1 -k 5 -ndcg-k 10 -warmup-runs 1 -measured-runs 1 -query-timeout 15s -require-policy-pass -quiet

eval-vector-recorded: migrate build-eval
	@$(EVAL_BINARY) -dataset $(EVAL_V2_DATASET) -arms no-memory-v1,reviewed-cards-bm25-v1,reviewed-cards-postgres-fts-v1,reviewed-cards-postgres-vector-v1 -k 5 -ndcg-k 10 -warmup-runs 1 -measured-runs 3 -query-timeout 15s -require-policy-pass -require-clean -output $(EVAL_VECTOR_V2_MANIFEST) -quiet

semantic-frozen:
	@test -z "$$(git status --porcelain)" || { echo "semantic benchmark requires a clean frozen revision" >&2; exit 1; }

eval-semantic: semantic-frozen migrate build-eval
	@$(EVAL_BINARY) -dataset $(EVAL_SEMANTIC_V1_DATASET) -arms no-memory-v1,reviewed-cards-bm25-v1,reviewed-cards-postgres-fts-v1,reviewed-cards-postgres-vector-v1 -k 5 -ndcg-k 10 -warmup-runs 1 -measured-runs 1 -query-timeout 15s -require-policy-pass -require-clean -quiet

eval-semantic-recorded: semantic-frozen migrate build-eval
	@$(EVAL_BINARY) -dataset $(EVAL_SEMANTIC_V1_DATASET) -arms no-memory-v1,reviewed-cards-bm25-v1,reviewed-cards-postgres-fts-v1,reviewed-cards-postgres-vector-v1 -k 5 -ndcg-k 10 -warmup-runs 1 -measured-runs 3 -query-timeout 15s -require-policy-pass -require-clean -output $(EVAL_SEMANTIC_V1_MANIFEST) -quiet

eval-recorded: build-eval
	@$(EVAL_BINARY) -dataset $(EVAL_V2_DATASET) -arms no-memory-v1,reviewed-cards-bm25-v1 -k 5 -ndcg-k 10 -measured-runs 3 -query-timeout 5s -require-policy-pass -require-clean -output $(EVAL_V2_MANIFEST) -quiet

fmt:
	gofmt -w $(GO_FILES)

fmt-check:
	@test -z "$$(gofmt -l $(GO_FILES))"

test:
	$(GO) test ./...

test-integration: migrate build-server
	@TEST_SERVER_BINARY='$(SERVER_BINARY)' \
		$(GO) test -p 1 -race -tags=integration -count=1 ./internal/migrations ./internal/store/postgres ./internal/eval ./cmd/server

test-outbox-integration: migrate build-server
	@TEST_SERVER_BINARY='$(SERVER_BINARY)' \
		$(GO) test -p 1 -race -tags=integration -count=3 ./internal/migrations ./internal/store/postgres ./cmd/server

test-vector-integration: migrate build-eval
	@$(GO) test -p 1 -race -tags='integration vector' -count=1 ./internal/embedding ./internal/store/postgres ./internal/eval

test-race:
	$(GO) test -race ./...

vet:
	$(GO) vet ./...

verify: fmt-check vet test test-race build eval-v2

migrate: db-up
	@$(GO) run ./cmd/migrate

server: db-up
	@$(GO) run ./cmd/server

verify-postgres: test-integration eval-postgres

verify-vector: test-vector-integration eval-vector

verify-semantic: test-vector-integration eval-semantic
