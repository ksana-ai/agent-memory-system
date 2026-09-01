//go:build integration

package eval

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/ksana-ai/agent-memory-system/internal/migrations"
	"github.com/ksana-ai/agent-memory-system/internal/store/postgres"
)

func TestPostgresFTSArmRunsRepeatedlyAndRemovesPhysicalScope(t *testing.T) {
	ctx := context.Background()
	databaseURL := os.Getenv("TEST_DATABASE_URL")
	if databaseURL == "" {
		t.Fatal("TEST_DATABASE_URL is required")
	}
	if err := migrations.Apply(ctx, databaseURL); err != nil {
		t.Fatalf("apply migrations: %v", err)
	}
	const namespaceToken = "integration_repeat_namespace"
	factory, err := newPostgresFTSArmFactory(ctx, databaseURL, func(ctx context.Context, databaseURL string) (postgresFTSBackend, error) {
		return postgres.Open(ctx, databaseURL)
	}, func() (string, error) {
		return namespaceToken, nil
	})
	if err != nil {
		t.Fatalf("new PostgreSQL FTS factory: %v", err)
	}
	dataset := mustLoadDatasetV2(t, `{
  "schema_version":"2","id":"postgres-fts-integration","version":"1","description":"real PostgreSQL FTS arm",
  "cases":[{"id":"case","scopes":[{"id":"s","tenant_id":"tenant","user_id":"user"}],"timeline":[
    {"op":"memory.remember","memory_ref":"memory","scope":"s","at":"2026-08-19T10:01:00Z","review_state":"approved","memory":{"kind":"semantic","category":"deployment","key":"region","value":"Project Aurora deploy region Tokyo"},"evidence":[{"alias":"source","session_id":"session","actor":"user","content":"Project Aurora deploys in Tokyo","occurred_at":"2026-08-19T10:00:00Z"}]},
    {"op":"query","id":"q","scope":"s","at":"2026-08-19T10:02:00Z","text":"Aurora Tokyo","judgments":{"memory_cards":{"relevance":{"memory":3}}}}
  ]}]
}`)
	config := ConfigV2{
		RecallK: 1, NDCGK: 1, MeasuredRuns: 1, QueryTimeout: 5 * time.Second,
		Arms: []ArmFactory{factory}, Timer: &stepTimerV2{step: time.Millisecond}, RequirePolicyPass: true,
	}
	for run := 1; run <= 2; run++ {
		manifest, runErr := RunV2(ctx, dataset, config)
		if runErr != nil {
			t.Fatalf("run %d: %v", run, runErr)
		}
		aggregate := manifest.Arms[0].Aggregate
		if !aggregate.PolicyPassed || aggregate.RecallAtK != 1 || aggregate.MRR != 1 || aggregate.NDCGAtK != 1 {
			t.Fatalf("run %d aggregate = %#v", run, aggregate)
		}
		assertPostgresEvaluationScopeAbsent(t, databaseURL, "eval_"+namespaceToken+"_t_1", "eval_"+namespaceToken+"_u_1")
	}
}

func assertPostgresEvaluationScopeAbsent(t *testing.T, databaseURL, tenantID, userID string) {
	t.Helper()
	ctx := context.Background()
	connection, err := pgx.Connect(ctx, databaseURL)
	if err != nil {
		t.Fatalf("connect for cleanup assertion: %v", err)
	}
	defer connection.Close(ctx)
	var count int
	if err := connection.QueryRow(ctx, `
		SELECT count(*)
		FROM agent_memory.user_scope_state
		WHERE tenant_id = $1 AND user_id = $2`, tenantID, userID).Scan(&count); err != nil {
		t.Fatalf("count evaluation scope state: %v", err)
	}
	if count != 0 {
		t.Fatalf("evaluation scope state rows = %d, want 0", count)
	}
}
