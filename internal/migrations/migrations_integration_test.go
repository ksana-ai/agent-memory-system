//go:build integration

package migrations

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"net/url"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/tern/v2/migrate"
)

var migrationDatabaseSequence atomic.Uint64

func TestApplyFreshProjectionSchemaAndReapply(t *testing.T) {
	databaseURL := isolatedMigrationDatabase(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := Apply(ctx, databaseURL); err != nil {
		t.Fatalf("fresh Apply(): %v", err)
	}
	if err := Apply(ctx, databaseURL); err != nil {
		t.Fatalf("idempotent Apply(): %v", err)
	}

	conn, err := pgx.Connect(ctx, databaseURL)
	if err != nil {
		t.Fatalf("connect to migrated database: %v", err)
	}
	defer conn.Close(context.Background())

	var version, versionRows int
	if err := conn.QueryRow(ctx, `
		SELECT version, count(*) OVER ()
		FROM public.agent_memory_schema_version`).Scan(&version, &versionRows); err != nil {
		t.Fatalf("read schema version: %v", err)
	}
	if version != 7 || versionRows != 1 {
		t.Fatalf("schema version=%d rows=%d, want version 7 in one row", version, versionRows)
	}

	for _, table := range []string{
		"embedding_projection_targets",
		"embedding_projection_jobs",
		"embedding_projection_deployment",
		"embedding_projection_promotions",
	} {
		var exists bool
		if err := conn.QueryRow(ctx, `SELECT to_regclass($1) IS NOT NULL`, "agent_memory."+table).Scan(&exists); err != nil {
			t.Fatalf("inspect %s: %v", table, err)
		}
		if !exists {
			t.Fatalf("table agent_memory.%s does not exist", table)
		}
	}

	assertColumnCollation(t, ctx, conn, "embedding_projection_targets", []string{"embedding_space", "state"})
	assertColumnCollation(t, ctx, conn, "embedding_projection_jobs", []string{
		"tenant_id", "user_id", "memory_id", "embedding_space", "state", "lease_owner", "last_error_code",
	})

	var identity, identityGeneration string
	if err := conn.QueryRow(ctx, `
		SELECT is_identity, identity_generation
		FROM information_schema.columns
		WHERE table_schema = 'agent_memory'
		  AND table_name = 'embedding_projection_jobs'
		  AND column_name = 'id'`).Scan(&identity, &identityGeneration); err != nil {
		t.Fatalf("inspect projection job identity: %v", err)
	}
	if identity != "YES" || identityGeneration != "ALWAYS" {
		t.Fatalf("job identity=%s generation=%s, want YES/ALWAYS", identity, identityGeneration)
	}

	assertDeleteRule(t, ctx, conn, "embedding_projection_targets_space_fk", "RESTRICT")
	assertDeleteRule(t, ctx, conn, "embedding_projection_jobs_space_fk", "RESTRICT")
	assertDeleteRule(t, ctx, conn, "embedding_projection_jobs_card_fk", "CASCADE")
	assertIndexDefinition(t, ctx, conn, "embedding_projection_targets_one_serving_idx", "UNIQUE", "WHERE (state = 'serving'::text)")
	assertIndexDefinition(t, ctx, conn, "embedding_projection_jobs_claim_idx", "available_at, created_at, id", "state = ANY (ARRAY['pending'::text, 'retry'::text])")
	assertIndexDefinition(t, ctx, conn, "embedding_projection_jobs_lease_until_idx", "lease_until, id", "WHERE (state = 'leased'::text)")
	assertIndexDefinition(t, ctx, conn, "memory_cards_active_projection_scan_idx", "COLLATE \"C\"", "WHERE (status = 'active'::text)")
	assertIndexDefinition(t, ctx, conn, "embedding_projection_jobs_space_scope_memory_idx", "embedding_space, tenant_id, user_id, memory_id")
	assertIndexDefinition(t, ctx, conn, "memory_embeddings_space_scope_memory_idx", "embedding_space, tenant_id, user_id, memory_id")
	assertProjectionDeploymentState(t, ctx, conn)
	assertPromotionReceiptConstraints(t, ctx, conn)

	seedProjectionFixtures(t, ctx, conn)
	assertProjectionConstraints(t, ctx, conn)
}

func TestPromotionMigrationFailsClosedForServingTargetThatDoesNotEnqueue(t *testing.T) {
	databaseURL := isolatedMigrationDatabase(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	conn, err := pgx.Connect(ctx, databaseURL)
	if err != nil {
		t.Fatalf("connect promotion upgrade database: %v", err)
	}
	defer conn.Close(context.Background())
	migrator, err := migrate.NewMigrator(ctx, conn, versionTable)
	if err != nil {
		t.Fatalf("create promotion upgrade migrator: %v", err)
	}
	files, err := fs.Sub(migrationFS, "sql")
	if err != nil {
		t.Fatalf("open promotion upgrade migrations: %v", err)
	}
	if err := migrator.LoadMigrations(files); err != nil {
		t.Fatalf("load promotion upgrade migrations: %v", err)
	}
	if err := migrator.MigrateTo(ctx, 6); err != nil {
		t.Fatalf("migrate promotion upgrade fixture to v6: %v", err)
	}
	const fingerprint = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	if _, err := conn.Exec(ctx, `
		INSERT INTO agent_memory.embedding_spaces (
			id, provider, model, dimension, document_version, query_version,
			model_fingerprint, created_at
		) VALUES ('upgrade-space', 'fixture', 'fixture-model', 1024,
		          'memory-card-document-v1', 'raw-query-v1', $1, clock_timestamp())`, fingerprint); err != nil {
		t.Fatalf("seed v6 embedding space: %v", err)
	}
	if _, err := conn.Exec(ctx, `
		INSERT INTO agent_memory.embedding_projection_targets (
			embedding_space, state, enqueue_new
		) VALUES ('upgrade-space', 'serving', false)`); err != nil {
		t.Fatalf("seed invalid v6 serving target: %v", err)
	}
	if err := migrator.MigrateTo(ctx, 7); err == nil {
		t.Fatal("v7 migration accepted serving target with enqueue_new=false")
	}
	var version int32
	if err := conn.QueryRow(ctx, `SELECT version FROM public.agent_memory_schema_version`).Scan(&version); err != nil {
		t.Fatalf("read failed upgrade version: %v", err)
	}
	if version != 6 {
		t.Fatalf("failed upgrade version=%d, want 6", version)
	}
	if _, err := conn.Exec(ctx, `
		UPDATE agent_memory.embedding_projection_targets
		SET enqueue_new=true
		WHERE embedding_space='upgrade-space'`); err != nil {
		t.Fatalf("explicitly repair invalid v6 fixture: %v", err)
	}
	if err := migrator.MigrateTo(ctx, 7); err != nil {
		t.Fatalf("migrate valid serving target to v7: %v", err)
	}
}

func assertPromotionReceiptConstraints(t *testing.T, ctx context.Context, conn *pgx.Conn) {
	t.Helper()
	assertColumnCollation(t, ctx, conn, "embedding_projection_promotions", []string{
		"operation_id", "from_embedding_space", "to_embedding_space",
	})
	assertDeleteRule(t, ctx, conn, "embedding_projection_promotions_from_space_fk", "RESTRICT")
	assertDeleteRule(t, ctx, conn, "embedding_projection_promotions_to_space_fk", "RESTRICT")
}

func assertProjectionDeploymentState(t *testing.T, ctx context.Context, conn *pgx.Conn) {
	t.Helper()

	var singleton bool
	var generation, rows int64
	var createdAt, updatedAt time.Time
	if err := conn.QueryRow(ctx, `
		SELECT singleton, generation, created_at, updated_at, count(*) OVER ()
		FROM agent_memory.embedding_projection_deployment`,
	).Scan(&singleton, &generation, &createdAt, &updatedAt, &rows); err != nil {
		t.Fatalf("read projection deployment state: %v", err)
	}
	if !singleton || generation != 0 || rows != 1 || createdAt.IsZero() || updatedAt.Before(createdAt) {
		t.Fatalf(
			"deployment singleton=%t generation=%d rows=%d created_at=%s updated_at=%s",
			singleton, generation, rows, createdAt, updatedAt,
		)
	}

	type columnExpectation struct {
		name        string
		dataType    string
		defaultPart string
	}
	for _, column := range []columnExpectation{
		{name: "singleton", dataType: "boolean", defaultPart: "true"},
		{name: "generation", dataType: "bigint", defaultPart: "0"},
		{name: "created_at", dataType: "timestamp with time zone", defaultPart: "clock_timestamp()"},
		{name: "updated_at", dataType: "timestamp with time zone", defaultPart: "clock_timestamp()"},
	} {
		var dataType, nullable, defaultValue string
		if err := conn.QueryRow(ctx, `
			SELECT data_type, is_nullable, column_default
			FROM information_schema.columns
			WHERE table_schema = 'agent_memory'
			  AND table_name = 'embedding_projection_deployment'
			  AND column_name = $1`, column.name,
		).Scan(&dataType, &nullable, &defaultValue); err != nil {
			t.Fatalf("inspect deployment column %s: %v", column.name, err)
		}
		if dataType != column.dataType || nullable != "NO" || !strings.Contains(defaultValue, column.defaultPart) {
			t.Fatalf(
				"deployment column %s type=%q nullable=%q default=%q, want %q/NO containing %q",
				column.name, dataType, nullable, defaultValue, column.dataType, column.defaultPart,
			)
		}
	}

	assertSQLState(t, ctx, conn, `
		INSERT INTO agent_memory.embedding_projection_deployment (singleton)
		VALUES (false)`, "23514", "false deployment singleton")
	assertSQLState(t, ctx, conn, `
		INSERT INTO agent_memory.embedding_projection_deployment DEFAULT VALUES`, "23505", "second deployment singleton")
	assertSQLState(t, ctx, conn, `
		UPDATE agent_memory.embedding_projection_deployment
		SET generation = -1`, "23514", "negative deployment generation")
}

func isolatedMigrationDatabase(t *testing.T) string {
	t.Helper()
	baseURL := strings.TrimSpace(os.Getenv("TEST_DATABASE_URL"))
	if baseURL == "" {
		t.Fatal("TEST_DATABASE_URL is required for integration tests")
	}
	baseConfig, err := pgx.ParseConfig(baseURL)
	if err != nil {
		t.Fatalf("parse TEST_DATABASE_URL: %v", err)
	}

	databaseName := fmt.Sprintf(
		"agent_memory_migration_%d_%d",
		time.Now().UnixNano(),
		migrationDatabaseSequence.Add(1),
	)
	adminConfig := baseConfig.Copy()
	adminConfig.Database = "postgres"
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	admin, err := pgx.ConnectConfig(ctx, adminConfig)
	if err != nil {
		t.Fatalf("connect to PostgreSQL maintenance database: %v", err)
	}
	defer admin.Close(context.Background())

	quotedDatabase := pgx.Identifier{databaseName}.Sanitize()
	if _, err := admin.Exec(ctx, "CREATE DATABASE "+quotedDatabase); err != nil {
		t.Fatalf("create isolated migration database: %v", err)
	}
	t.Cleanup(func() {
		dropCtx, dropCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer dropCancel()
		dropConnection, err := pgx.ConnectConfig(dropCtx, adminConfig)
		if err != nil {
			t.Errorf("reconnect to drop isolated migration database: %v", err)
			return
		}
		defer dropConnection.Close(context.Background())
		if _, err := dropConnection.Exec(dropCtx, "DROP DATABASE IF EXISTS "+quotedDatabase+" WITH (FORCE)"); err != nil {
			t.Errorf("drop isolated migration database: %v", err)
		}
	})

	connectionURL, err := url.Parse(baseURL)
	if err != nil || (connectionURL.Scheme != "postgres" && connectionURL.Scheme != "postgresql") {
		t.Fatalf("TEST_DATABASE_URL must be a PostgreSQL URL: %v", err)
	}
	connectionURL.Path = "/" + databaseName
	connectionURL.RawPath = ""
	return connectionURL.String()
}

func assertColumnCollation(t *testing.T, ctx context.Context, conn *pgx.Conn, table string, columns []string) {
	t.Helper()
	for _, column := range columns {
		var collation string
		if err := conn.QueryRow(ctx, `
			SELECT collation_name
			FROM information_schema.columns
			WHERE table_schema = 'agent_memory'
			  AND table_name = $1
			  AND column_name = $2`, table, column).Scan(&collation); err != nil {
			t.Fatalf("inspect %s.%s collation: %v", table, column, err)
		}
		if collation != "C" {
			t.Fatalf("%s.%s collation=%q, want C", table, column, collation)
		}
	}
}

func assertDeleteRule(t *testing.T, ctx context.Context, conn *pgx.Conn, constraint, want string) {
	t.Helper()
	var rule string
	if err := conn.QueryRow(ctx, `
		SELECT delete_rule
		FROM information_schema.referential_constraints
		WHERE constraint_schema = 'agent_memory'
		  AND constraint_name = $1`, constraint).Scan(&rule); err != nil {
		t.Fatalf("inspect %s delete rule: %v", constraint, err)
	}
	if rule != want {
		t.Fatalf("%s delete rule=%s, want %s", constraint, rule, want)
	}
}

func assertIndexDefinition(t *testing.T, ctx context.Context, conn *pgx.Conn, index string, fragments ...string) {
	t.Helper()
	var definition string
	if err := conn.QueryRow(ctx, `
		SELECT indexdef
		FROM pg_indexes
		WHERE schemaname = 'agent_memory'
		  AND indexname = $1`, index).Scan(&definition); err != nil {
		t.Fatalf("inspect %s: %v", index, err)
	}
	for _, fragment := range fragments {
		if !strings.Contains(definition, fragment) {
			t.Fatalf("%s definition=%q, want fragment %q", index, definition, fragment)
		}
	}
}

func seedProjectionFixtures(t *testing.T, ctx context.Context, conn *pgx.Conn) {
	t.Helper()
	const fingerprint = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	for _, space := range []string{"space-a", "space-b", "space-c"} {
		if _, err := conn.Exec(ctx, `
			INSERT INTO agent_memory.embedding_spaces (
				id, provider, model, dimension, document_version, query_version,
				model_fingerprint, created_at
			) VALUES ($1, 'integration', 'embedding-model', 1024, 'document-v1', 'query-v1', $2, clock_timestamp())`,
			space, fingerprint); err != nil {
			t.Fatalf("insert embedding space %s: %v", space, err)
		}
	}

	if _, err := conn.Exec(ctx, `
		INSERT INTO agent_memory.user_scope_state (tenant_id, user_id)
		VALUES ('tenant-a', 'user-a')`); err != nil {
		t.Fatalf("seed user scope: %v", err)
	}
	if _, err := conn.Exec(ctx, `
		INSERT INTO agent_memory.memory_candidates (
			tenant_id, user_id, id, kind, category, memory_key, value,
			extractor, extractor_version, status, review_decision, reviewer_id,
			review_reason, reviewed_at, created_at
		) VALUES (
			'tenant-a', 'user-a', 'candidate-a', 'semantic', 'preference', 'key-a', 'value-a',
			'integration', 'v1', 'approved', 'approve', 'reviewer-a',
			'integration fixture', clock_timestamp(), clock_timestamp()
		)`); err != nil {
		t.Fatalf("seed memory candidate: %v", err)
	}
	if _, err := conn.Exec(ctx, `
		INSERT INTO agent_memory.memory_identity_chains (
			tenant_id, user_id, identity_key, kind, category, memory_key, latest_version
		) VALUES ('tenant-a', 'user-a', 'identity-a', 'semantic', 'preference', 'key-a', 1)`); err != nil {
		t.Fatalf("seed memory identity chain: %v", err)
	}
	if _, err := conn.Exec(ctx, `
		INSERT INTO agent_memory.memory_cards (
			tenant_id, user_id, id, candidate_id, identity_key, kind, category,
			memory_key, value, version, status, created_at
		) VALUES (
			'tenant-a', 'user-a', 'memory-a', 'candidate-a', 'identity-a', 'semantic',
			'preference', 'key-a', 'value-a', 1, 'active', clock_timestamp()
		)`); err != nil {
		t.Fatalf("seed memory card: %v", err)
	}
}

func assertProjectionConstraints(t *testing.T, ctx context.Context, conn *pgx.Conn) {
	t.Helper()
	if _, err := conn.Exec(ctx, `
		INSERT INTO agent_memory.embedding_projection_targets (embedding_space, state, enqueue_new)
		VALUES ('space-a', 'serving', true)`); err != nil {
		t.Fatalf("insert serving target: %v", err)
	}
	assertSQLState(t, ctx, conn, `
		INSERT INTO agent_memory.embedding_projection_targets (embedding_space, state, enqueue_new)
		VALUES ('space-b', 'serving', true)`, "23505", "second serving target")
	assertSQLState(t, ctx, conn, `
		INSERT INTO agent_memory.embedding_projection_targets (embedding_space, state, enqueue_new)
		VALUES ('space-b', 'blocked', true)`, "23514", "blocked enqueue target")

	var jobID, attempts int64
	var state string
	if err := conn.QueryRow(ctx, `
		INSERT INTO agent_memory.embedding_projection_jobs (
			tenant_id, user_id, memory_id, embedding_space, expected_memory_version
		) VALUES ('tenant-a', 'user-a', 'memory-a', 'space-b', 1)
		RETURNING id, state, attempt_count`,
	).Scan(&jobID, &state, &attempts); err != nil {
		t.Fatalf("insert pending job: %v", err)
	}
	if jobID <= 0 || state != "pending" || attempts != 0 {
		t.Fatalf("pending job id=%d state=%s attempts=%d, want generated/pending/0", jobID, state, attempts)
	}
	assertSQLState(t, ctx, conn, `
		INSERT INTO agent_memory.embedding_projection_jobs (
			tenant_id, user_id, memory_id, embedding_space, expected_memory_version
		) VALUES ('tenant-a', 'user-a', 'memory-a', 'space-b', 1)`, "23505", "duplicate natural job key")
	assertSQLState(t, ctx, conn, `
		INSERT INTO agent_memory.embedding_projection_jobs (
			tenant_id, user_id, memory_id, embedding_space, expected_memory_version
		) VALUES ('tenant-a', 'user-a', 'memory-a', 'space-c', 0)`, "23514", "nonpositive expected version")
	assertSQLState(t, ctx, conn, `
		INSERT INTO agent_memory.embedding_projection_jobs (
			tenant_id, user_id, memory_id, embedding_space, expected_memory_version,
			state, attempt_count, lease_version
		) VALUES ('tenant-a', 'user-a', 'memory-a', 'space-c', 1, 'leased', 1, 1)`, "23514", "leased job without owner or deadline")
	assertSQLState(t, ctx, conn, `
		INSERT INTO agent_memory.embedding_projection_jobs (
			tenant_id, user_id, memory_id, embedding_space, expected_memory_version,
			state, completed_at
		) VALUES ('tenant-a', 'user-a', 'memory-a', 'space-c', 1, 'dead', clock_timestamp())`, "23514", "dead job without error code")
	assertSQLState(t, ctx, conn, `
		INSERT INTO agent_memory.embedding_projection_jobs (
			tenant_id, user_id, memory_id, embedding_space, expected_memory_version,
			state, attempt_count, available_at, completed_at, last_error_code, last_error_at
		) VALUES (
			'tenant-a', 'user-a', 'memory-a', 'space-c', 1, 'dead', 1,
			clock_timestamp(), clock_timestamp(), 'raw error: secret=abc', clock_timestamp()
		)`, "23514", "unbounded raw error text")

	assertSQLState(t, ctx, conn, `DELETE FROM agent_memory.embedding_spaces WHERE id = 'space-b'`, "23001", "space referenced by a job")
	assertSQLState(t, ctx, conn, `DELETE FROM agent_memory.embedding_spaces WHERE id = 'space-a'`, "23001", "space referenced by a target")
	if _, err := conn.Exec(ctx, `DELETE FROM agent_memory.memory_cards WHERE tenant_id = 'tenant-a' AND user_id = 'user-a' AND id = 'memory-a'`); err != nil {
		t.Fatalf("delete projected memory card: %v", err)
	}
	var jobs int
	if err := conn.QueryRow(ctx, `SELECT count(*) FROM agent_memory.embedding_projection_jobs`).Scan(&jobs); err != nil {
		t.Fatalf("count jobs after card deletion: %v", err)
	}
	if jobs != 0 {
		t.Fatalf("job count after card deletion=%d, want cascade to zero", jobs)
	}
	if _, err := conn.Exec(ctx, `DELETE FROM agent_memory.embedding_spaces WHERE id = 'space-b'`); err != nil {
		t.Fatalf("delete space after job cascade: %v", err)
	}
}

func assertSQLState(t *testing.T, ctx context.Context, conn *pgx.Conn, statement, want, label string) {
	t.Helper()
	_, err := conn.Exec(ctx, statement)
	var pgError *pgconn.PgError
	if err == nil {
		t.Fatalf("%s error=nil, want SQLSTATE %s", label, want)
	}
	if !errors.As(err, &pgError) || pgError.Code != want {
		t.Fatalf("%s error=%v SQLSTATE=%q, want %s", label, err, pgErrorCode(pgError), want)
	}
}

func pgErrorCode(err *pgconn.PgError) string {
	if err == nil {
		return ""
	}
	return err.Code
}
