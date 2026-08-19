//go:build integration && vector

package eval

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/kai443/go-agent-memory-system/internal/embedding"
	"github.com/kai443/go-agent-memory-system/internal/migrations"
	"github.com/kai443/go-agent-memory-system/internal/store/postgres"
)

func TestPostgresVectorArmRunsAgainstLMStudioAndRemovesPhysicalScope(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	databaseURL := os.Getenv("TEST_DATABASE_URL")
	embeddingsURL := os.Getenv("LMSTUDIO_EMBEDDINGS_URL")
	model := os.Getenv("LMSTUDIO_EMBEDDING_MODEL")
	if databaseURL == "" || embeddingsURL == "" || model == "" {
		t.Fatal("TEST_DATABASE_URL, LMSTUDIO_EMBEDDINGS_URL, and LMSTUDIO_EMBEDDING_MODEL are required")
	}
	if err := migrations.Apply(ctx, databaseURL); err != nil {
		t.Fatalf("apply migrations: %v", err)
	}
	client, err := embedding.NewClient(embedding.Config{
		Endpoint: embeddingsURL, Model: model, ExpectedDimension: postgres.VectorDimension,
		Timeout: embedding.DefaultTimeout, MaxResponseBytes: embedding.DefaultMaxResponseBytes,
		MaxBatchSize: 1, MaxInputBytes: embedding.DefaultMaxInputBytes,
	})
	if err != nil {
		t.Fatalf("new embedding client: %v", err)
	}
	const namespaceToken = "vector_integration_repeat_namespace"
	factory, err := newPostgresVectorArmFactory(ctx, databaseURL, client, func(ctx context.Context, databaseURL string) (postgresVectorBackend, error) {
		return postgres.Open(ctx, databaseURL)
	}, func() (string, error) {
		return namespaceToken, nil
	}, func() time.Time { return time.Now().UTC() })
	if err != nil {
		t.Fatalf("new PostgreSQL vector factory: %v", err)
	}
	dataset := mustLoadDatasetV2(t, `{
  "schema_version":"2","id":"postgres-vector-integration","version":"1","description":"real LM Studio and pgvector arm",
  "cases":[{"id":"case","scopes":[{"id":"s","tenant_id":"tenant","user_id":"user"}],"timeline":[
    {"op":"memory.remember","memory_ref":"memory","scope":"s","at":"2026-08-19T10:01:00Z","review_state":"approved","memory":{"kind":"semantic","category":"deployment","key":"region","value":"Project Aurora deploy region Tokyo","person":"Project Aurora","relationship":"deployment target","backstory":"Current deployment region."},"evidence":[{"alias":"source","session_id":"session","actor":"user","content":"Project Aurora deploys in Tokyo","occurred_at":"2026-08-19T10:00:00Z"}]},
    {"op":"query","id":"q","scope":"s","at":"2026-08-19T10:02:00Z","text":"Where does Aurora deploy?","judgments":{"memory_cards":{"relevance":{"memory":3}}}}
  ]}]
}`)
	config := ConfigV2{
		RecallK: 1, NDCGK: 1, WarmupRuns: 1, MeasuredRuns: 2, QueryTimeout: embedding.DefaultTimeout,
		Arms: []ArmFactory{factory}, Timer: TimerFunc(time.Now), RequirePolicyPass: true,
	}
	physicalTenantID := "eval_" + namespaceToken + "_t_1"
	physicalUserID := "eval_" + namespaceToken + "_u_1"
	for run := 1; run <= 2; run++ {
		manifest, runErr := RunV2(ctx, dataset, config)
		if runErr != nil {
			t.Fatalf("run %d: %v", run, runErr)
		}
		aggregate := manifest.Arms[0].Aggregate
		if !aggregate.PolicyPassed || aggregate.RecallAtK != 1 || aggregate.MRR != 1 || aggregate.NDCGAtK != 1 || aggregate.QueryExecutionFailures != 0 {
			t.Fatalf("run %d aggregate = %#v", run, aggregate)
		}
		assertPostgresEvaluationScopeAbsent(t, databaseURL, physicalTenantID, physicalUserID)
		assertPostgresEvaluationEmbeddingsAbsent(t, databaseURL, physicalTenantID, physicalUserID)
	}
}

func assertPostgresEvaluationEmbeddingsAbsent(t *testing.T, databaseURL, tenantID, userID string) {
	t.Helper()
	ctx := context.Background()
	connection, err := pgx.Connect(ctx, databaseURL)
	if err != nil {
		t.Fatalf("connect for embedding cleanup assertion: %v", err)
	}
	defer connection.Close(ctx)
	var count int
	if err := connection.QueryRow(ctx, `
		SELECT count(*)
		FROM agent_memory.memory_embeddings
		WHERE tenant_id = $1 AND user_id = $2`, tenantID, userID).Scan(&count); err != nil {
		t.Fatalf("count evaluation embeddings: %v", err)
	}
	if count != 0 {
		t.Fatalf("evaluation embedding rows = %d, want 0", count)
	}
}
