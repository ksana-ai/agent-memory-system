package postgres

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/ksana-ai/agent-memory-system/internal/domain"
)

func TestValidateServingVectorExpectation(t *testing.T) {
	t.Parallel()

	for _, valid := range []ServingVectorExpectation{
		{EmbeddingSpace: "space-a", Generation: 0},
		{EmbeddingSpace: "lmstudio:text-embedding-bge-m3:1024", Generation: 42},
	} {
		if err := validateServingVectorExpectation(valid); err != nil {
			t.Fatalf("valid expectation %#v: %v", valid, err)
		}
	}

	for name, invalid := range map[string]ServingVectorExpectation{
		"missing space":       {Generation: 1},
		"trimmed space":       {EmbeddingSpace: " space-a", Generation: 1},
		"control character":   {EmbeddingSpace: "space\nsecret", Generation: 1},
		"negative generation": {EmbeddingSpace: "space-a", Generation: -1},
	} {
		t.Run(name, func(t *testing.T) {
			if err := validateServingVectorExpectation(invalid); !errors.Is(err, domain.ErrInvalid) {
				t.Fatalf("error=%v, want invalid input", err)
			}
		})
	}
}

func TestServingVectorTransactionPinsReadCommitted(t *testing.T) {
	t.Parallel()

	options := servingVectorTxOptions()
	if options.IsoLevel != pgx.ReadCommitted {
		t.Fatalf("isolation=%q, want read committed", options.IsoLevel)
	}
	if options.AccessMode == pgx.ReadOnly {
		t.Fatal("serving vector transaction cannot be read-only because it takes row locks")
	}
}

func TestServingVectorClosedPoolIsUnavailableAndRedacted(t *testing.T) {
	const secret = "serving-pool-secret"
	config, err := pgxpool.ParseConfig("postgres://operator:" + secret + "@127.0.0.1:1/agent_memory")
	if err != nil {
		t.Fatalf("parse pool config: %v", err)
	}
	pool, err := pgxpool.NewWithConfig(context.Background(), config)
	if err != nil {
		t.Fatalf("create pool: %v", err)
	}
	pool.Close()
	storage := New(pool)

	_, currentErr := storage.CurrentServingProjection(context.Background())
	assertServingDatabaseUnavailable(t, currentErr, secret)

	_, searchErr := storage.SearchServingVector(
		context.Background(),
		"tenant-a",
		"user-a",
		ServingVectorExpectation{EmbeddingSpace: "space-a", Generation: 1},
		nil,
		0,
		time.Time{},
	)
	assertServingDatabaseUnavailable(t, searchErr, secret)
}

func assertServingDatabaseUnavailable(t *testing.T, err error, secret string) {
	t.Helper()
	if !errors.Is(err, domain.ErrUnavailable) {
		t.Fatalf("error=%v, want unavailable", err)
	}
	if errors.Is(err, domain.ErrInvariant) {
		t.Fatalf("error=%v, do not want invariant", err)
	}
	if strings.Contains(err.Error(), secret) {
		t.Fatalf("error exposed pool credential: %q", err)
	}
}

func TestServingVectorSQLFailsClosedInsideRankingQuery(t *testing.T) {
	t.Parallel()

	query := strings.ToLower(servingVectorSearchSQL)
	for _, required := range []string{
		"from agent_memory.embedding_projection_targets as target",
		"join agent_memory.embedding_projection_jobs as job",
		"join agent_memory.memory_cards as card",
		"join agent_memory.memory_embeddings as embedding",
		"target.embedding_space = $3",
		"target.state = 'serving'",
		"target.enqueue_new",
		"job.state = 'succeeded'",
		"job.expected_memory_version = card.version",
		"card.tenant_id = $1",
		"card.user_id = $2",
		"card.status = 'active'",
		"card.expires_at is null or card.expires_at > $6",
		"and exists (",
		"from agent_memory.candidate_source_events as required_source",
		"embedding.embedding <=> query_embedding.value",
		"card.created_at desc",
		"card.id asc",
	} {
		if !strings.Contains(query, required) {
			t.Errorf("serving vector SQL is missing %q", required)
		}
	}
	if strings.Contains(query, "hnsw") || strings.Contains(query, "ivfflat") {
		t.Fatal("serving vector baseline must remain exact, not ANN")
	}
}
