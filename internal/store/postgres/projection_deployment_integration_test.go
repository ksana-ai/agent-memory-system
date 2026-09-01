//go:build integration

package postgres_test

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/ksana-ai/agent-memory-system/internal/domain"
	"github.com/ksana-ai/agent-memory-system/internal/store/postgres"
)

func TestProjectionDeploymentGenerationAdvancesOnlyForMaterialChanges(t *testing.T) {
	ctx := context.Background()
	databaseURL := requiredDatabaseURL(t)
	applyMigrations(t, databaseURL)
	storage := openStore(t, databaseURL)
	defer storage.Close()

	space := uniqueProjectionRepositorySpace("deployment_generation")
	cleanupProjectionRepositorySpaces(t, databaseURL, space)
	initialGeneration := projectionDeploymentGeneration(t, databaseURL)
	command := projectionRepositoryRegistration(space, postgres.ProjectionTargetShadow, false, fixtureTime(1))
	target, err := storage.RegisterProjectionTarget(ctx, command)
	if err != nil {
		t.Fatalf("register material deployment target: %v", err)
	}
	assertProjectionDeploymentGeneration(t, databaseURL, initialGeneration+1)

	// Registration observation timestamps are provenance, not target shape.
	// Retrying the same immutable space and target is not a material change.
	retry := command
	retry.Space.CreatedAt = retry.Space.CreatedAt.Add(time.Hour)
	retry.CreatedAt = retry.CreatedAt.Add(time.Hour)
	if _, err := storage.RegisterProjectionTarget(ctx, retry); err != nil {
		t.Fatalf("retry exact deployment registration: %v", err)
	}
	assertProjectionDeploymentGeneration(t, databaseURL, initialGeneration+1)

	drift := command
	drift.Space.QueryVersion = "raw-query-v2"
	if _, err := storage.RegisterProjectionTarget(ctx, drift); !errors.Is(err, domain.ErrConflict) {
		t.Fatalf("registration drift error=%v, want conflict", err)
	}
	assertProjectionDeploymentGeneration(t, databaseURL, initialGeneration+1)

	setCommand := postgres.SetProjectionTargetCommand{
		EmbeddingSpace: space,
		State:          postgres.ProjectionTargetShadow,
		EnqueueNew:     true,
		UpdatedAt:      target.UpdatedAt.Add(time.Second),
	}
	updated, err := storage.SetProjectionTarget(ctx, setCommand)
	if err != nil {
		t.Fatalf("material deployment target update: %v", err)
	}
	assertProjectionDeploymentGeneration(t, databaseURL, initialGeneration+2)
	if _, err := storage.SetProjectionTarget(ctx, setCommand); err != nil {
		t.Fatalf("retry exact deployment target update: %v", err)
	}
	assertProjectionDeploymentGeneration(t, databaseURL, initialGeneration+2)

	conflict := setCommand
	conflict.EnqueueNew = false
	if _, err := storage.SetProjectionTarget(ctx, conflict); !errors.Is(err, domain.ErrConflict) {
		t.Fatalf("equal timestamp deployment conflict error=%v, want conflict", err)
	}
	assertProjectionDeploymentGeneration(t, databaseURL, initialGeneration+2)

	retire := postgres.SetProjectionTargetCommand{
		EmbeddingSpace: space,
		State:          postgres.ProjectionTargetRetired,
		EnqueueNew:     false,
		UpdatedAt:      updated.UpdatedAt.Add(time.Second),
	}
	if _, err := storage.SetProjectionTarget(ctx, retire); err != nil {
		t.Fatalf("material deployment target retirement: %v", err)
	}
	assertProjectionDeploymentGeneration(t, databaseURL, initialGeneration+3)
	if _, err := storage.SetProjectionTarget(ctx, retire); err != nil {
		t.Fatalf("retry exact deployment target retirement: %v", err)
	}
	assertProjectionDeploymentGeneration(t, databaseURL, initialGeneration+3)
}

func TestProjectionDeploymentApprovalFirstExcludesLaterRegistration(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	databaseURL := requiredDatabaseURL(t)
	applyMigrations(t, databaseURL)
	storage := openStore(t, databaseURL)
	defer storage.Close()

	space := uniqueProjectionRepositorySpace("deployment_approval_first")
	cleanupProjectionRepositorySpaces(t, databaseURL, space)
	tenantID, userID := uniqueScope("projection-deployment-approval-first")
	cleanupScopes(t, databaseURL, [][2]string{{tenantID, userID}})
	event := evidence(tenantID, userID, "event-projection-deployment-approval-first", "approval first", 10)
	mustAppend(t, storage, event)
	candidate := candidate(
		tenantID,
		userID,
		"candidate-projection-deployment-approval-first",
		"deployment-approval-first",
		"approval first",
		[]string{event.ID},
		11,
	)
	mustCreateCandidate(t, storage, candidate)
	memoryID := "memory-projection-deployment-approval-first"
	initialGeneration := projectionDeploymentGeneration(t, databaseURL)

	advisoryKey := int64(1_710_000_000 + scopeSequence.Add(1)%10_000_000)
	lockConn, err := pgx.Connect(ctx, databaseURL)
	if err != nil {
		t.Fatalf("connect approval-first barrier holder: %v", err)
	}
	defer lockConn.Close(context.Background())
	if _, err := lockConn.Exec(ctx, `SELECT pg_advisory_lock($1)`, advisoryKey); err != nil {
		t.Fatalf("hold approval-first barrier: %v", err)
	}
	locked := true
	defer func() {
		if locked {
			_, _ = lockConn.Exec(context.Background(), `SELECT pg_advisory_unlock($1)`, advisoryKey)
		}
	}()
	installProjectionApprovalAdvisoryTrigger(t, databaseURL, memoryID, advisoryKey)

	type approvalResult struct {
		card *domain.MemoryCard
		err  error
	}
	approved := make(chan approvalResult, 1)
	go func() {
		_, card, reviewErr := storage.ReviewCandidate(ctx, approval(candidate, memoryID, 12))
		approved <- approvalResult{card: card, err: reviewErr}
	}()
	waitForProjectionAdvisoryWaiter(t, databaseURL, advisoryKey)

	registrationApplication := fmt.Sprintf("projection_deployment_register_wait_%d", scopeSequence.Add(1))
	registrationStore := openProjectionDeploymentStoreWithApplicationName(t, databaseURL, registrationApplication)
	defer registrationStore.Close()
	registered := make(chan error, 1)
	go func() {
		_, registerErr := registrationStore.RegisterProjectionTarget(ctx, projectionRepositoryRegistration(
			space,
			postgres.ProjectionTargetShadow,
			true,
			fixtureTime(13),
		))
		registered <- registerErr
	}()
	waitForProjectionDeploymentLock(t, databaseURL, registrationApplication)
	assertProjectionDeploymentGeneration(t, databaseURL, initialGeneration)

	if _, err := lockConn.Exec(ctx, `SELECT pg_advisory_unlock($1)`, advisoryKey); err != nil {
		t.Fatalf("release approval-first barrier: %v", err)
	}
	locked = false
	select {
	case result := <-approved:
		if result.err != nil || result.card == nil {
			t.Fatalf("approval-first review card=%#v error=%v", result.card, result.err)
		}
	case <-ctx.Done():
		t.Fatalf("approval-first review did not finish: %v", ctx.Err())
	}
	select {
	case registerErr := <-registered:
		if registerErr != nil {
			t.Fatalf("registration after approval: %v", registerErr)
		}
	case <-ctx.Done():
		t.Fatalf("registration after approval did not finish: %v", ctx.Err())
	}

	assertProjectionDeploymentGeneration(t, databaseURL, initialGeneration+1)
	jobs, err := storage.ProjectionJobs(ctx, postgres.ProjectionJobFilter{
		EmbeddingSpace: space,
		TenantID:       tenantID,
		UserID:         userID,
		Limit:          10,
	})
	if err != nil || len(jobs) != 0 {
		t.Fatalf("approval-first jobs for later target=%#v error=%v, want none", jobs, err)
	}
}

func TestProjectionDeploymentRegistrationFirstIsIncludedByLaterApproval(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	databaseURL := requiredDatabaseURL(t)
	applyMigrations(t, databaseURL)
	storage := openStore(t, databaseURL)
	defer storage.Close()

	space := uniqueProjectionRepositorySpace("deployment_registration_first")
	cleanupProjectionRepositorySpaces(t, databaseURL, space)
	tenantID, userID := uniqueScope("projection-deployment-registration-first")
	cleanupScopes(t, databaseURL, [][2]string{{tenantID, userID}})
	event := evidence(tenantID, userID, "event-projection-deployment-registration-first", "registration first", 20)
	mustAppend(t, storage, event)
	candidate := candidate(
		tenantID,
		userID,
		"candidate-projection-deployment-registration-first",
		"deployment-registration-first",
		"registration first",
		[]string{event.ID},
		21,
	)
	mustCreateCandidate(t, storage, candidate)
	initialGeneration := projectionDeploymentGeneration(t, databaseURL)

	advisoryKey := int64(1_720_000_000 + scopeSequence.Add(1)%10_000_000)
	lockConn, err := pgx.Connect(ctx, databaseURL)
	if err != nil {
		t.Fatalf("connect registration-first barrier holder: %v", err)
	}
	defer lockConn.Close(context.Background())
	if _, err := lockConn.Exec(ctx, `SELECT pg_advisory_lock($1)`, advisoryKey); err != nil {
		t.Fatalf("hold registration-first barrier: %v", err)
	}
	locked := true
	defer func() {
		if locked {
			_, _ = lockConn.Exec(context.Background(), `SELECT pg_advisory_unlock($1)`, advisoryKey)
		}
	}()
	installProjectionTargetRegistrationAdvisoryTrigger(t, databaseURL, space, advisoryKey)

	registered := make(chan error, 1)
	go func() {
		_, registerErr := storage.RegisterProjectionTarget(ctx, projectionRepositoryRegistration(
			space,
			postgres.ProjectionTargetShadow,
			true,
			fixtureTime(22),
		))
		registered <- registerErr
	}()
	waitForProjectionDeploymentAdvisoryWaiter(t, databaseURL, advisoryKey)

	approvalApplication := fmt.Sprintf("projection_deployment_approval_wait_%d", scopeSequence.Add(1))
	approvalStore := openProjectionDeploymentStoreWithApplicationName(t, databaseURL, approvalApplication)
	defer approvalStore.Close()
	type approvalResult struct {
		card *domain.MemoryCard
		err  error
	}
	approved := make(chan approvalResult, 1)
	go func() {
		_, card, reviewErr := approvalStore.ReviewCandidate(ctx, approval(candidate,
			"memory-projection-deployment-registration-first", 23))
		approved <- approvalResult{card: card, err: reviewErr}
	}()
	waitForProjectionDeploymentLock(t, databaseURL, approvalApplication)
	assertProjectionDeploymentGeneration(t, databaseURL, initialGeneration)

	if _, err := lockConn.Exec(ctx, `SELECT pg_advisory_unlock($1)`, advisoryKey); err != nil {
		t.Fatalf("release registration-first barrier: %v", err)
	}
	locked = false
	select {
	case registerErr := <-registered:
		if registerErr != nil {
			t.Fatalf("registration-first target: %v", registerErr)
		}
	case <-ctx.Done():
		t.Fatalf("registration-first target did not finish: %v", ctx.Err())
	}
	select {
	case result := <-approved:
		if result.err != nil || result.card == nil {
			t.Fatalf("approval after registration card=%#v error=%v", result.card, result.err)
		}
	case <-ctx.Done():
		t.Fatalf("approval after registration did not finish: %v", ctx.Err())
	}

	assertProjectionDeploymentGeneration(t, databaseURL, initialGeneration+1)
	jobs, err := storage.ProjectionJobs(ctx, postgres.ProjectionJobFilter{
		EmbeddingSpace: space,
		TenantID:       tenantID,
		UserID:         userID,
		Limit:          10,
	})
	if err != nil || len(jobs) != 1 || jobs[0].State != postgres.ProjectionJobPending || jobs[0].AttemptCount != 0 {
		t.Fatalf("registration-first projection jobs=%#v error=%v, want one pending job", jobs, err)
	}
}

func TestProjectionDeploymentRejectionDoesNotWaitForDeploymentLock(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	databaseURL := requiredDatabaseURL(t)
	applyMigrations(t, databaseURL)
	storage := openStore(t, databaseURL)
	defer storage.Close()

	tenantID, userID := uniqueScope("projection-deployment-rejection")
	cleanupScopes(t, databaseURL, [][2]string{{tenantID, userID}})
	event := evidence(tenantID, userID, "event-projection-deployment-rejection", "reject without deployment", 30)
	mustAppend(t, storage, event)
	candidate := candidate(
		tenantID,
		userID,
		"candidate-projection-deployment-rejection",
		"deployment-rejection",
		"reject without deployment",
		[]string{event.ID},
		31,
	)
	mustCreateCandidate(t, storage, candidate)

	lockConn, err := pgx.Connect(ctx, databaseURL)
	if err != nil {
		t.Fatalf("connect deployment lock holder for rejection: %v", err)
	}
	defer lockConn.Close(context.Background())
	tx, err := lockConn.Begin(ctx)
	if err != nil {
		t.Fatalf("begin deployment lock holder for rejection: %v", err)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	var generation int64
	if err := tx.QueryRow(ctx, `
		SELECT generation
		FROM agent_memory.embedding_projection_deployment
		WHERE singleton
		FOR UPDATE`).Scan(&generation); err != nil {
		t.Fatalf("hold exclusive deployment lock for rejection: %v", err)
	}

	reviewCtx, reviewCancel := context.WithTimeout(ctx, 5*time.Second)
	defer reviewCancel()
	reviewed, card, err := storage.ReviewCandidate(reviewCtx, rejection(candidate, 32))
	if err != nil || card != nil || reviewed.Status != domain.CandidateRejected {
		t.Fatalf("rejection while deployment is locked candidate=%#v card=%#v error=%v", reviewed, card, err)
	}
}

func projectionDeploymentGeneration(t *testing.T, databaseURL string) int64 {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	conn, err := pgx.Connect(ctx, databaseURL)
	if err != nil {
		t.Fatalf("connect to read projection deployment generation: %v", err)
	}
	defer conn.Close(context.Background())
	var generation int64
	if err := conn.QueryRow(ctx, `
		SELECT generation
		FROM agent_memory.embedding_projection_deployment
		WHERE singleton`).Scan(&generation); err != nil {
		t.Fatalf("read projection deployment generation: %v", err)
	}
	return generation
}

func assertProjectionDeploymentGeneration(t *testing.T, databaseURL string, want int64) {
	t.Helper()
	if got := projectionDeploymentGeneration(t, databaseURL); got != want {
		t.Fatalf("projection deployment generation=%d, want %d", got, want)
	}
}

func openProjectionDeploymentStoreWithApplicationName(
	t *testing.T,
	databaseURL string,
	applicationName string,
) *postgres.Store {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	config, err := pgxpool.ParseConfig(databaseURL)
	if err != nil {
		t.Fatalf("parse projection deployment database URL: %v", err)
	}
	if config.ConnConfig.RuntimeParams == nil {
		config.ConnConfig.RuntimeParams = make(map[string]string)
	}
	config.ConnConfig.RuntimeParams["application_name"] = applicationName
	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		t.Fatalf("open projection deployment store: %v", err)
	}
	storage := postgres.New(pool)
	if err := storage.Ping(ctx); err != nil {
		storage.Close()
		t.Fatalf("ping projection deployment store: %v", err)
	}
	return storage
}

func installProjectionTargetRegistrationAdvisoryTrigger(
	t *testing.T,
	databaseURL string,
	embeddingSpace string,
	advisoryKey int64,
) {
	t.Helper()
	sequence := scopeSequence.Add(1)
	functionName := fmt.Sprintf("test_projection_deployment_wait_%d", sequence)
	triggerName := fmt.Sprintf("test_projection_deployment_wait_%d", sequence)
	qualifiedFunction := pgx.Identifier{"agent_memory", functionName}.Sanitize()
	quotedTrigger := pgx.Identifier{triggerName}.Sanitize()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	conn, err := pgx.Connect(ctx, databaseURL)
	if err != nil {
		t.Fatalf("connect to install deployment registration trigger: %v", err)
	}
	functionSQL := fmt.Sprintf(`
		CREATE FUNCTION %s() RETURNS trigger LANGUAGE plpgsql AS $body$
		BEGIN
			IF NEW.embedding_space = %s THEN
				PERFORM pg_advisory_xact_lock(%d);
			END IF;
			RETURN NEW;
		END
		$body$`, qualifiedFunction, postgresTestLiteral(embeddingSpace), advisoryKey)
	if _, err := conn.Exec(ctx, functionSQL); err != nil {
		_ = conn.Close(context.Background())
		t.Fatalf("create deployment registration wait function: %v", err)
	}
	triggerSQL := fmt.Sprintf(`
		CREATE TRIGGER %s BEFORE INSERT ON agent_memory.embedding_projection_targets
		FOR EACH ROW EXECUTE FUNCTION %s()`, quotedTrigger, qualifiedFunction)
	if _, err := conn.Exec(ctx, triggerSQL); err != nil {
		_, _ = conn.Exec(ctx, "DROP FUNCTION "+qualifiedFunction+"()")
		_ = conn.Close(context.Background())
		t.Fatalf("create deployment registration wait trigger: %v", err)
	}
	if err := conn.Close(context.Background()); err != nil {
		t.Fatalf("close deployment registration trigger connection: %v", err)
	}
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cleanupCancel()
		cleanupConn, cleanupErr := pgx.Connect(cleanupCtx, databaseURL)
		if cleanupErr != nil {
			t.Errorf("connect to remove deployment registration trigger: %v", cleanupErr)
			return
		}
		defer cleanupConn.Close(context.Background())
		if _, cleanupErr = cleanupConn.Exec(cleanupCtx, fmt.Sprintf(
			"DROP TRIGGER IF EXISTS %s ON agent_memory.embedding_projection_targets", quotedTrigger,
		)); cleanupErr != nil {
			t.Errorf("drop deployment registration trigger: %v", cleanupErr)
			return
		}
		if _, cleanupErr = cleanupConn.Exec(cleanupCtx, "DROP FUNCTION IF EXISTS "+qualifiedFunction+"()"); cleanupErr != nil {
			t.Errorf("drop deployment registration function: %v", cleanupErr)
		}
	})
}

func waitForProjectionDeploymentAdvisoryWaiter(t *testing.T, databaseURL string, advisoryKey int64) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	conn, err := pgx.Connect(ctx, databaseURL)
	if err != nil {
		t.Fatalf("connect to observe deployment advisory waiter: %v", err)
	}
	defer conn.Close(context.Background())
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		var waiting bool
		if err := conn.QueryRow(ctx, `
			SELECT EXISTS (
				SELECT 1
				FROM pg_locks
				WHERE locktype = 'advisory'
				  AND granted = false
				  AND classid = 0
				  AND objid = $1::oid
			)`, advisoryKey).Scan(&waiting); err != nil {
			t.Fatalf("observe deployment advisory waiter: %v", err)
		}
		if waiting {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatalf("deployment transaction did not reach advisory barrier: %v", ctx.Err())
		case <-ticker.C:
		}
	}
}

func waitForProjectionDeploymentLock(t *testing.T, databaseURL, applicationName string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	conn, err := pgx.Connect(ctx, databaseURL)
	if err != nil {
		t.Fatalf("connect to observe deployment lock waiter: %v", err)
	}
	defer conn.Close(context.Background())
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		var waiting bool
		if err := conn.QueryRow(ctx, `
			SELECT EXISTS (
				SELECT 1
				FROM pg_stat_activity
				WHERE application_name = $1
				  AND state = 'active'
				  AND wait_event_type = 'Lock'
				  AND query LIKE '%embedding_projection_deployment%'
				  AND cardinality(pg_blocking_pids(pid)) > 0
			)`, applicationName).Scan(&waiting); err != nil {
			t.Fatalf("observe deployment lock waiter: %v", err)
		}
		if waiting {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatalf("transaction did not wait on projection deployment lock: %v", ctx.Err())
		case <-ticker.C:
		}
	}
}
