package migrations

import (
	"context"
	"embed"
	"fmt"
	"io/fs"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/tern/v2/migrate"
)

const versionTable = "public.agent_memory_schema_version"

// migrationFS is embedded so the server, migration command, and tests always
// execute the same versioned schema without depending on the working directory.
//
//go:embed sql/*.sql
var migrationFS embed.FS

// Apply migrates the database at databaseURL to the latest embedded schema.
// Tern serializes concurrent migrators with a PostgreSQL advisory lock and
// applies each migration in a transaction.
func Apply(ctx context.Context, databaseURL string) error {
	databaseURL = strings.TrimSpace(databaseURL)
	if databaseURL == "" {
		return fmt.Errorf("database URL is required")
	}

	conn, err := pgx.Connect(ctx, databaseURL)
	if err != nil {
		return fmt.Errorf("connect to PostgreSQL: %w", err)
	}
	defer conn.Close(context.Background())

	migrator, err := migrate.NewMigrator(ctx, conn, versionTable)
	if err != nil {
		return fmt.Errorf("create migrator: %w", err)
	}

	files, err := fs.Sub(migrationFS, "sql")
	if err != nil {
		return fmt.Errorf("open embedded migrations: %w", err)
	}
	if err := migrator.LoadMigrations(files); err != nil {
		return fmt.Errorf("load embedded migrations: %w", err)
	}
	if err := migrator.Migrate(ctx); err != nil {
		return fmt.Errorf("apply migrations: %w", err)
	}
	return nil
}
