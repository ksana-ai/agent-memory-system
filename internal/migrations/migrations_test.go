package migrations

import (
	"context"
	"io/fs"
	"strings"
	"testing"

	"github.com/jackc/tern/v2/migrate"
)

func TestEmbeddedMigrationsLoad(t *testing.T) {
	files, err := fs.Sub(migrationFS, "sql")
	if err != nil {
		t.Fatalf("open embedded migrations: %v", err)
	}

	migrator, err := migrate.NewMigrator(context.Background(), nil, versionTable)
	if err != nil {
		t.Fatalf("create migrator: %v", err)
	}
	if err := migrator.LoadMigrations(files); err != nil {
		t.Fatalf("load migrations: %v", err)
	}
	if len(migrator.Migrations) != 6 {
		t.Fatalf("migration count = %d, want 6", len(migrator.Migrations))
	}
	if !strings.Contains(migrator.Migrations[0].UpSQL, "CREATE TABLE agent_memory.memory_cards") {
		t.Fatal("initial migration does not create memory_cards")
	}
	if !strings.Contains(migrator.Migrations[0].DownSQL, "DROP SCHEMA IF EXISTS agent_memory CASCADE") {
		t.Fatal("initial migration does not define its rollback")
	}
	if !strings.Contains(migrator.Migrations[1].UpSQL, "ADD COLUMN expires_at timestamptz") {
		t.Fatal("expiration migration does not add expires_at")
	}
	if !strings.Contains(migrator.Migrations[1].UpSQL, "memory_cards_scope_serviceable_expiry_idx") {
		t.Fatal("expiration migration does not add its serviceability index")
	}
	if !strings.Contains(migrator.Migrations[1].DownSQL, "DROP COLUMN IF EXISTS expires_at") {
		t.Fatal("expiration migration does not define its rollback")
	}
	if !strings.Contains(migrator.Migrations[2].UpSQL, "GENERATED ALWAYS AS") {
		t.Fatal("FTS migration does not add a generated search document")
	}
	if !strings.Contains(migrator.Migrations[2].UpSQL, "'pg_catalog.simple'::regconfig") {
		t.Fatal("FTS migration does not pin the simple text search configuration")
	}
	if !strings.Contains(migrator.Migrations[2].UpSQL, "memory_cards_active_search_document_gin_idx") {
		t.Fatal("FTS migration does not add its active-card GIN index")
	}
	if !strings.Contains(migrator.Migrations[2].DownSQL, "DROP COLUMN IF EXISTS search_document") {
		t.Fatal("FTS migration does not define its rollback")
	}
	if !strings.Contains(migrator.Migrations[3].UpSQL, "embedding vector(1024) NOT NULL") {
		t.Fatal("embedding migration does not pin the measured vector dimension")
	}
	if !strings.Contains(migrator.Migrations[3].UpSQL, "CREATE TABLE agent_memory.embedding_spaces") {
		t.Fatal("embedding migration does not add the vector-space registry")
	}
	if !strings.Contains(migrator.Migrations[3].UpSQL, "embedding_spaces_dimension_pinned CHECK (dimension = 1024)") {
		t.Fatal("embedding-space registry does not pin its measured dimension")
	}
	if !strings.Contains(migrator.Migrations[3].UpSQL, "embedding_spaces_configuration_unique") {
		t.Fatal("embedding-space registry does not expose its configuration key")
	}
	if !strings.Contains(migrator.Migrations[3].UpSQL, "embedding_space, provider, model, document_version, query_version, model_fingerprint") {
		t.Fatal("memory embedding does not enforce its registry configuration")
	}
	if !strings.Contains(migrator.Migrations[3].UpSQL, "ON DELETE CASCADE") {
		t.Fatal("embedding migration does not cascade card deletion")
	}
	if !strings.Contains(migrator.Migrations[3].UpSQL, "memory_embeddings_scope_space_idx") {
		t.Fatal("embedding migration does not add its scope and space index")
	}
	if !strings.Contains(migrator.Migrations[3].DownSQL, "DROP TABLE IF EXISTS agent_memory.memory_embeddings") {
		t.Fatal("embedding migration does not define its rollback")
	}
	if !strings.Contains(migrator.Migrations[4].UpSQL, "CREATE TABLE agent_memory.embedding_projection_targets") {
		t.Fatal("projection migration does not add the target registry")
	}
	if !strings.Contains(migrator.Migrations[4].UpSQL, "state IN ('shadow', 'serving', 'blocked', 'retired')") {
		t.Fatal("projection target registry does not constrain lifecycle states")
	}
	if !strings.Contains(migrator.Migrations[4].UpSQL, "embedding_projection_targets_one_serving_idx") {
		t.Fatal("projection target registry does not enforce one serving space")
	}
	if !strings.Contains(migrator.Migrations[4].UpSQL, "CREATE TABLE agent_memory.embedding_projection_jobs") {
		t.Fatal("projection migration does not add the durable job outbox")
	}
	if !strings.Contains(migrator.Migrations[4].UpSQL, "GENERATED ALWAYS AS IDENTITY PRIMARY KEY") {
		t.Fatal("projection job outbox does not use a database identity")
	}
	if !strings.Contains(migrator.Migrations[4].UpSQL, "state IN ('pending', 'leased', 'retry', 'succeeded', 'dead', 'cancelled')") {
		t.Fatal("projection job outbox does not constrain lifecycle states")
	}
	if !strings.Contains(migrator.Migrations[4].UpSQL, "UNIQUE (tenant_id, user_id, memory_id, embedding_space)") {
		t.Fatal("projection job outbox does not enforce idempotent natural keys")
	}
	if !strings.Contains(migrator.Migrations[4].UpSQL, "embedding_projection_jobs_claim_idx") {
		t.Fatal("projection job outbox does not define its claim index")
	}
	if !strings.Contains(migrator.Migrations[4].UpSQL, "embedding_projection_jobs_lease_until_idx") {
		t.Fatal("projection job outbox does not define its lease-recovery index")
	}
	if strings.Contains(migrator.Migrations[4].UpSQL, "last_error_message") ||
		strings.Contains(migrator.Migrations[4].UpSQL, "last_error_redacted") {
		t.Fatal("projection job outbox must persist bounded error codes, not error text")
	}
	if !strings.Contains(migrator.Migrations[4].DownSQL, "DROP TABLE IF EXISTS agent_memory.embedding_projection_jobs") ||
		!strings.Contains(migrator.Migrations[4].DownSQL, "DROP TABLE IF EXISTS agent_memory.embedding_projection_targets") {
		t.Fatal("projection migration does not define its rollback")
	}
	if !strings.Contains(migrator.Migrations[5].UpSQL, "CREATE TABLE agent_memory.embedding_projection_deployment") {
		t.Fatal("deployment migration does not add its singleton state")
	}
	if !strings.Contains(migrator.Migrations[5].UpSQL, "singleton boolean PRIMARY KEY DEFAULT true") ||
		!strings.Contains(migrator.Migrations[5].UpSQL, "CHECK (singleton)") {
		t.Fatal("deployment state does not enforce its singleton key")
	}
	if !strings.Contains(migrator.Migrations[5].UpSQL, "generation bigint NOT NULL DEFAULT 0") ||
		!strings.Contains(migrator.Migrations[5].UpSQL, "CHECK (generation >= 0)") {
		t.Fatal("deployment state does not enforce a nonnegative generation")
	}
	if !strings.Contains(migrator.Migrations[5].UpSQL, "updated_at timestamptz NOT NULL DEFAULT clock_timestamp()") {
		t.Fatal("deployment state does not use the database clock")
	}
	if !strings.Contains(migrator.Migrations[5].UpSQL, "CHECK (updated_at >= created_at)") {
		t.Fatal("deployment state does not constrain its timestamps")
	}
	if !strings.Contains(migrator.Migrations[5].UpSQL, "INSERT INTO agent_memory.embedding_projection_deployment DEFAULT VALUES") {
		t.Fatal("deployment migration does not seed its one lock row")
	}
	for _, index := range []string{
		"memory_cards_active_projection_scan_idx",
		"embedding_projection_jobs_space_scope_memory_idx",
		"memory_embeddings_space_scope_memory_idx",
	} {
		if !strings.Contains(migrator.Migrations[5].UpSQL, index) {
			t.Fatalf("deployment migration does not add reconciliation index %s", index)
		}
		if !strings.Contains(migrator.Migrations[5].DownSQL, "DROP INDEX IF EXISTS agent_memory."+index) {
			t.Fatalf("deployment migration does not roll back reconciliation index %s", index)
		}
	}
	if !strings.Contains(migrator.Migrations[5].DownSQL, "DROP TABLE IF EXISTS agent_memory.embedding_projection_deployment") {
		t.Fatal("deployment migration does not define its rollback")
	}
}

func TestApplyRequiresDatabaseURL(t *testing.T) {
	if err := Apply(context.Background(), " \t\n"); err == nil {
		t.Fatal("Apply() error = nil, want missing database URL error")
	}
}
