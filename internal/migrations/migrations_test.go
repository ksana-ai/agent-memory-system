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
	if len(migrator.Migrations) != 3 {
		t.Fatalf("migration count = %d, want 3", len(migrator.Migrations))
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
}

func TestApplyRequiresDatabaseURL(t *testing.T) {
	if err := Apply(context.Background(), " \t\n"); err == nil {
		t.Fatal("Apply() error = nil, want missing database URL error")
	}
}
