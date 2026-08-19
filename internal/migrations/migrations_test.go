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
	if len(migrator.Migrations) != 1 {
		t.Fatalf("migration count = %d, want 1", len(migrator.Migrations))
	}
	if !strings.Contains(migrator.Migrations[0].UpSQL, "CREATE TABLE agent_memory.memory_cards") {
		t.Fatal("initial migration does not create memory_cards")
	}
	if !strings.Contains(migrator.Migrations[0].DownSQL, "DROP SCHEMA IF EXISTS agent_memory CASCADE") {
		t.Fatal("initial migration does not define its rollback")
	}
}

func TestApplyRequiresDatabaseURL(t *testing.T) {
	if err := Apply(context.Background(), " \t\n"); err == nil {
		t.Fatal("Apply() error = nil, want missing database URL error")
	}
}
