//go:build integration && !vector

package main

import (
	"bytes"
	"context"
	"fmt"
	"net/url"
	"os"
	"os/exec"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/ksana-ai/agent-memory-system/internal/domain"
	"github.com/ksana-ai/agent-memory-system/internal/embedding"
	"github.com/ksana-ai/agent-memory-system/internal/migrations"
	storecontract "github.com/ksana-ai/agent-memory-system/internal/store"
	"github.com/ksana-ai/agent-memory-system/internal/store/postgres"
)

const (
	reconcilerProcessSecret       = "TOP_SECRET_RECONCILIATION_PROCESS"
	reconcilerProcessEvidenceText = "sensitive reconciliation integration evidence"
	reconcilerProcessCardValue    = "sensitive reconciliation integration memory"
	reconcilerAdvisoryLockKey     = int64(908_231_047)
)

var reconcilerDatabaseSequence atomic.Uint64

func TestProjectionReconcilerProcessRestartIdempotencyAndDeletionPropagation(t *testing.T) {
	binary := requiredProjectionReconcilerBinary(t)
	databaseURL := isolatedProjectionReconcilerDatabase(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	if err := migrations.Apply(ctx, databaseURL); err != nil {
		t.Fatal("apply isolated reconciler migrations")
	}
	storage, err := postgres.Open(ctx, databaseURL)
	if err != nil {
		t.Fatal("open isolated reconciler store")
	}
	defer storage.Close()

	firstTenant := fmt.Sprintf("tenant_reconciler_first_%d", time.Now().UnixNano())
	firstUser := fmt.Sprintf("user_reconciler_first_%d", time.Now().UnixNano())
	secondTenant := fmt.Sprintf("tenant_reconciler_second_%d", time.Now().UnixNano())
	secondUser := fmt.Sprintf("user_reconciler_second_%d", time.Now().UnixNano())
	firstCard := seedProjectionReconcilerCard(t, storage, firstTenant, firstUser, "first")
	secondCard := seedProjectionReconcilerCard(t, storage, secondTenant, secondUser, "second")

	space := "space_v1_reconciler_process"
	registeredAt := time.Now().UTC().Add(time.Second).Truncate(time.Microsecond)
	if _, err := storage.RegisterProjectionTarget(ctx, postgres.RegisterProjectionTargetCommand{
		Space: postgres.EmbeddingSpaceDefinition{
			ID:               space,
			Provider:         embedding.ProviderLMStudio,
			Model:            "reconciler-process-model",
			Dimension:        postgres.VectorDimension,
			DocumentVersion:  embedding.MemoryCardDocumentVersion,
			QueryVersion:     embedding.RawQueryVersion,
			ModelFingerprint: strings.Repeat("a", 64),
			CreatedAt:        registeredAt,
		},
		State:      postgres.ProjectionTargetShadow,
		EnqueueNew: true,
		CreatedAt:  registeredAt,
	}); err != nil {
		t.Fatal("register isolated reconciliation target")
	}
	if countProjectionReconcilerRows(t, databaseURL, `
		SELECT count(*) FROM agent_memory.embedding_projection_jobs
		WHERE embedding_space=$1`, space) != 0 {
		t.Fatal("pre-target reviewed cards unexpectedly had projection jobs")
	}

	holder := installProjectionReconcilerBlockingTrigger(t, databaseURL)
	child := startProjectionReconcilerChild(t, binary, projectionReconcilerChildConfig{
		databaseURL: databaseURL,
		space:       space,
		mode:        reconcilerModeBackfill,
		batchSize:   1,
	})
	waitForProjectionReconcilerBlockedInsert(t, databaseURL)
	killedOutput := child.killAndWait(t)
	assertProjectionReconcilerOutputRedacted(
		t,
		killedOutput,
		databaseURL,
		firstTenant,
		firstUser,
		firstCard.ID,
		firstCard.Key,
		firstCard.Backstory,
		secondTenant,
		secondUser,
		secondCard.ID,
		secondCard.Key,
		secondCard.Backstory,
		reconcilerProcessEvidenceText,
		reconcilerProcessCardValue,
	)
	if _, err := holder.Exec(ctx, `SELECT pg_advisory_unlock($1)`, reconcilerAdvisoryLockKey); err != nil {
		t.Fatal("release projection reconciler advisory lock")
	}
	holder.Close(ctx)
	dropProjectionReconcilerBlockingTrigger(t, databaseURL)

	restartOutput, restartErr := runProjectionReconcilerChild(t, binary, projectionReconcilerChildConfig{
		databaseURL: databaseURL,
		space:       space,
		mode:        reconcilerModeBackfill,
		batchSize:   1,
	})
	if restartErr != nil {
		t.Fatal("restarted projection reconciler failed")
	}
	assertProjectionReconcilerOutputRedacted(
		t,
		restartOutput,
		databaseURL,
		firstTenant,
		firstUser,
		firstCard.ID,
		firstCard.Key,
		firstCard.Backstory,
		secondTenant,
		secondUser,
		secondCard.ID,
		secondCard.Key,
		secondCard.Backstory,
		reconcilerProcessEvidenceText,
		reconcilerProcessCardValue,
	)
	assertProjectionReconcilerNaturalJobs(t, databaseURL, space, 2)

	idempotentOutput, idempotentErr := runProjectionReconcilerChild(t, binary, projectionReconcilerChildConfig{
		databaseURL: databaseURL,
		space:       space,
		mode:        reconcilerModeBackfill,
		batchSize:   2,
	})
	if idempotentErr != nil {
		t.Fatal("idempotent projection backfill failed")
	}
	assertProjectionReconcilerOutputRedacted(
		t,
		idempotentOutput,
		databaseURL,
		firstTenant,
		firstUser,
		firstCard.ID,
		firstCard.Key,
		firstCard.Backstory,
		secondTenant,
		secondUser,
		secondCard.ID,
		secondCard.Key,
		secondCard.Backstory,
		reconcilerProcessEvidenceText,
		reconcilerProcessCardValue,
	)
	assertProjectionReconcilerNaturalJobs(t, databaseURL, space, 2)

	auditOutput, auditErr := runProjectionReconcilerChild(t, binary, projectionReconcilerChildConfig{
		databaseURL: databaseURL,
		space:       space,
		mode:        reconcilerModeAudit,
		batchSize:   2,
	})
	if auditErr == nil || !strings.Contains(auditOutput, errProjectionCoverageIncomplete.Error()) {
		t.Fatal("audit accepted pending projections or omitted its fixed incomplete error")
	}
	assertProjectionReconcilerOutputRedacted(
		t,
		auditOutput,
		databaseURL,
		firstTenant,
		firstUser,
		firstCard.ID,
		firstCard.Key,
		firstCard.Backstory,
		secondTenant,
		secondUser,
		secondCard.ID,
		secondCard.Key,
		secondCard.Backstory,
		reconcilerProcessEvidenceText,
		reconcilerProcessCardValue,
	)

	if _, err := storage.ForgetUser(ctx, firstTenant, firstUser, time.Now().UTC()); err != nil {
		t.Fatal("forget first reconciler scope")
	}
	afterDeleteOutput, afterDeleteErr := runProjectionReconcilerChild(t, binary, projectionReconcilerChildConfig{
		databaseURL: databaseURL,
		space:       space,
		mode:        reconcilerModeBackfill,
		batchSize:   1,
	})
	if afterDeleteErr != nil {
		t.Fatal("projection backfill after deletion failed")
	}
	assertProjectionReconcilerOutputRedacted(
		t,
		afterDeleteOutput,
		databaseURL,
		firstTenant,
		firstUser,
		firstCard.ID,
		firstCard.Key,
		firstCard.Backstory,
		secondTenant,
		secondUser,
		secondCard.ID,
		secondCard.Key,
		secondCard.Backstory,
		reconcilerProcessEvidenceText,
		reconcilerProcessCardValue,
	)
	if countProjectionReconcilerRows(t, databaseURL, `
		SELECT count(*) FROM agent_memory.embedding_projection_jobs
		WHERE tenant_id=$1 AND user_id=$2`, firstTenant, firstUser) != 0 ||
		countProjectionReconcilerRows(t, databaseURL, `
			SELECT count(*) FROM agent_memory.memory_cards
			WHERE tenant_id=$1 AND user_id=$2`, firstTenant, firstUser) != 0 {
		t.Fatal("reconciliation resurrected a forgotten scope")
	}
	assertProjectionReconcilerNaturalJobs(t, databaseURL, space, 1)
}

type projectionReconcilerChildConfig struct {
	databaseURL string
	space       string
	mode        string
	batchSize   int
}

type projectionReconcilerChild struct {
	command *exec.Cmd
	output  *lockedProjectionReconcilerBuffer
}

func startProjectionReconcilerChild(
	t *testing.T,
	binary string,
	config projectionReconcilerChildConfig,
) *projectionReconcilerChild {
	t.Helper()
	command := exec.Command(binary)
	command.Env = projectionReconcilerChildEnvironment(config)
	output := &lockedProjectionReconcilerBuffer{}
	command.Stdout = output
	command.Stderr = output
	if err := command.Start(); err != nil {
		t.Fatal("start projection reconciler child")
	}
	child := &projectionReconcilerChild{command: command, output: output}
	t.Cleanup(func() {
		if child.command.ProcessState == nil {
			_ = child.command.Process.Kill()
			_ = child.command.Wait()
		}
	})
	return child
}

func runProjectionReconcilerChild(
	t *testing.T,
	binary string,
	config projectionReconcilerChildConfig,
) (string, error) {
	t.Helper()
	child := startProjectionReconcilerChild(t, binary, config)
	done := make(chan error, 1)
	go func() { done <- child.command.Wait() }()
	select {
	case err := <-done:
		return child.output.String(), err
	case <-time.After(30 * time.Second):
		_ = child.command.Process.Kill()
		<-done
		t.Fatal("projection reconciler child timed out")
		return "", context.DeadlineExceeded
	}
}

func (child *projectionReconcilerChild) killAndWait(t *testing.T) string {
	t.Helper()
	if child == nil || child.command == nil || child.command.Process == nil {
		t.Fatal("projection reconciler child is not running")
	}
	if err := child.command.Process.Kill(); err != nil {
		t.Fatal("kill projection reconciler child")
	}
	done := make(chan error, 1)
	go func() { done <- child.command.Wait() }()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("killed projection reconciler exited successfully")
		}
		return child.output.String()
	case <-time.After(10 * time.Second):
		t.Fatal("killed projection reconciler did not exit")
		return ""
	}
}

func projectionReconcilerChildEnvironment(config projectionReconcilerChildConfig) []string {
	overrides := map[string]string{
		"DATABASE_URL":                           config.databaseURL,
		"PROJECTION_RECONCILER_EMBEDDING_SPACE":  config.space,
		"PROJECTION_RECONCILER_MODE":             config.mode,
		"PROJECTION_RECONCILER_BATCH_SIZE":       fmt.Sprintf("%d", config.batchSize),
		"PROJECTION_RECONCILER_PROCESS_SENTINEL": reconcilerProcessSecret,
	}
	result := make([]string, 0, len(os.Environ())+len(overrides))
	for _, entry := range os.Environ() {
		key, _, found := strings.Cut(entry, "=")
		if _, replaced := overrides[key]; found && replaced {
			continue
		}
		result = append(result, entry)
	}
	keys := make([]string, 0, len(overrides))
	for key := range overrides {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		result = append(result, key+"="+overrides[key])
	}
	return result
}

type lockedProjectionReconcilerBuffer struct {
	mutex sync.Mutex
	data  bytes.Buffer
}

func (buffer *lockedProjectionReconcilerBuffer) Write(value []byte) (int, error) {
	buffer.mutex.Lock()
	defer buffer.mutex.Unlock()
	return buffer.data.Write(value)
}

func (buffer *lockedProjectionReconcilerBuffer) String() string {
	buffer.mutex.Lock()
	defer buffer.mutex.Unlock()
	return buffer.data.String()
}

func assertProjectionReconcilerOutputRedacted(t *testing.T, output string, forbidden ...string) {
	t.Helper()
	forbidden = append(forbidden, reconcilerProcessSecret)
	for _, value := range forbidden {
		if value != "" && strings.Contains(output, value) {
			t.Fatal("projection reconciler output leaked sensitive configuration or content")
		}
	}
}

func requiredProjectionReconcilerBinary(t *testing.T) string {
	t.Helper()
	binary := strings.TrimSpace(os.Getenv("TEST_PROJECTION_RECONCILER_BINARY"))
	if binary == "" {
		t.Fatal("TEST_PROJECTION_RECONCILER_BINARY is required")
	}
	if _, err := os.Stat(binary); err != nil {
		t.Fatal("TEST_PROJECTION_RECONCILER_BINARY is not readable")
	}
	return binary
}

func isolatedProjectionReconcilerDatabase(t *testing.T) string {
	t.Helper()
	baseURL := strings.TrimSpace(os.Getenv("TEST_DATABASE_URL"))
	if baseURL == "" {
		t.Fatal("TEST_DATABASE_URL is required")
	}
	baseConfig, err := pgx.ParseConfig(baseURL)
	if err != nil {
		t.Fatal("parse TEST_DATABASE_URL")
	}
	parsedURL, err := url.Parse(baseURL)
	if err != nil || (parsedURL.Scheme != "postgres" && parsedURL.Scheme != "postgresql") {
		t.Fatal("TEST_DATABASE_URL must be a PostgreSQL URL")
	}
	databaseName := fmt.Sprintf(
		"agent_memory_reconciler_%d_%d",
		time.Now().UnixNano(),
		reconcilerDatabaseSequence.Add(1),
	)
	adminConfig := baseConfig.Copy()
	adminConfig.Database = "postgres"
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	admin, err := pgx.ConnectConfig(ctx, adminConfig)
	if err != nil {
		t.Fatal("connect to PostgreSQL maintenance database")
	}
	defer admin.Close(context.Background())
	quotedDatabase := pgx.Identifier{databaseName}.Sanitize()
	if _, err := admin.Exec(ctx, "CREATE DATABASE "+quotedDatabase); err != nil {
		t.Fatal("create isolated projection reconciler database")
	}
	t.Cleanup(func() {
		dropContext, dropCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer dropCancel()
		dropConnection, dropErr := pgx.ConnectConfig(dropContext, adminConfig)
		if dropErr != nil {
			t.Error("reconnect to drop isolated projection reconciler database")
			return
		}
		defer dropConnection.Close(context.Background())
		if _, dropErr := dropConnection.Exec(
			dropContext,
			"DROP DATABASE IF EXISTS "+quotedDatabase+" WITH (FORCE)",
		); dropErr != nil {
			t.Error("drop isolated projection reconciler database")
		}
	})
	parsedURL.Path = "/" + databaseName
	parsedURL.RawPath = ""
	parameters := parsedURL.Query()
	parameters.Set("application_name", reconcilerProcessSecret)
	parsedURL.RawQuery = parameters.Encode()
	return parsedURL.String()
}

func seedProjectionReconcilerCard(
	t *testing.T,
	storage *postgres.Store,
	tenantID, userID, suffix string,
) domain.MemoryCard {
	t.Helper()
	ctx := context.Background()
	baseTime := time.Now().UTC().Add(-time.Minute).Truncate(time.Microsecond)
	event := domain.EvidenceEvent{
		ID:         "event-reconciler-" + suffix,
		TenantID:   tenantID,
		UserID:     userID,
		SessionID:  "session-reconciler-" + suffix,
		Actor:      domain.ActorUser,
		Content:    reconcilerProcessEvidenceText + " " + suffix,
		OccurredAt: baseTime,
		RecordedAt: baseTime.Add(time.Microsecond),
	}
	if err := storage.AppendEvidence(ctx, event); err != nil {
		t.Fatal("append reconciler evidence")
	}
	candidate := domain.MemoryCandidate{
		ID:               "candidate-reconciler-" + suffix,
		TenantID:         tenantID,
		UserID:           userID,
		Kind:             domain.MemoryKindSemantic,
		Category:         "reconciliation",
		Key:              "pre_target_" + suffix,
		Value:            reconcilerProcessCardValue + " " + suffix,
		Person:           "self",
		Relationship:     "self",
		Backstory:        "reviewed before the projection target existed",
		SourceEventIDs:   []string{event.ID},
		Extractor:        "reconciler-integration",
		ExtractorVersion: "v1",
		Status:           domain.CandidatePending,
		CreatedAt:        baseTime.Add(2 * time.Microsecond),
	}
	if err := storage.CreateCandidate(ctx, candidate); err != nil {
		t.Fatal("create reconciler candidate")
	}
	_, card, err := storage.ReviewCandidate(ctx, storecontract.CandidateReviewCommand{
		TenantID:    tenantID,
		UserID:      userID,
		CandidateID: candidate.ID,
		MemoryID:    "memory-reconciler-" + suffix,
		Review: domain.CandidateReview{
			Decision:   domain.DecisionApprove,
			ReviewerID: "reconciler-reviewer",
			Reason:     "integration fixture",
			ReviewedAt: baseTime.Add(3 * time.Microsecond),
		},
	})
	if err != nil || card == nil {
		t.Fatal("approve reconciler candidate")
	}
	return *card
}

func installProjectionReconcilerBlockingTrigger(t *testing.T, databaseURL string) *pgx.Conn {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	connection, err := pgx.Connect(ctx, databaseURL)
	if err != nil {
		t.Fatal("connect for reconciliation blocking trigger")
	}
	if _, err := connection.Exec(ctx, `
		CREATE OR REPLACE FUNCTION agent_memory.test_projection_reconciler_block()
		RETURNS trigger
		LANGUAGE plpgsql
		AS $function$
		BEGIN
			PERFORM pg_advisory_xact_lock(908231047);
			RETURN NEW;
		END
		$function$;
		CREATE TRIGGER test_projection_reconciler_block
		BEFORE INSERT ON agent_memory.embedding_projection_jobs
		FOR EACH ROW EXECUTE FUNCTION agent_memory.test_projection_reconciler_block();`); err != nil {
		connection.Close(context.Background())
		t.Fatal("install reconciliation blocking trigger")
	}
	if _, err := connection.Exec(ctx, `SELECT pg_advisory_lock($1)`, reconcilerAdvisoryLockKey); err != nil {
		connection.Close(context.Background())
		t.Fatal("hold reconciliation advisory lock")
	}
	return connection
}

func waitForProjectionReconcilerBlockedInsert(t *testing.T, databaseURL string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	connection, err := pgx.Connect(ctx, databaseURL)
	if err != nil {
		t.Fatal("connect to observe blocked reconciler")
	}
	defer connection.Close(context.Background())
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()
	for {
		var blocked bool
		err := connection.QueryRow(ctx, `
			SELECT EXISTS (
				SELECT 1
				FROM pg_stat_activity
				WHERE datname = current_database()
				  AND pid <> pg_backend_pid()
				  AND state = 'active'
				  AND wait_event_type = 'Lock'
				  AND query LIKE '%embedding_projection_jobs%'
			)`).Scan(&blocked)
		if err != nil {
			t.Fatal("inspect blocked projection reconciler")
		}
		if blocked {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatal("projection reconciler did not reach the blocked job insert")
		case <-ticker.C:
		}
	}
}

func dropProjectionReconcilerBlockingTrigger(t *testing.T, databaseURL string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	connection, err := pgx.Connect(ctx, databaseURL)
	if err != nil {
		t.Fatal("connect to drop reconciliation trigger")
	}
	defer connection.Close(context.Background())
	if _, err := connection.Exec(ctx, `
		DROP TRIGGER IF EXISTS test_projection_reconciler_block
		ON agent_memory.embedding_projection_jobs;
		DROP FUNCTION IF EXISTS agent_memory.test_projection_reconciler_block();`); err != nil {
		t.Fatal("drop reconciliation blocking trigger")
	}
}

func countProjectionReconcilerRows(t *testing.T, databaseURL, query string, arguments ...any) int64 {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	connection, err := pgx.Connect(ctx, databaseURL)
	if err != nil {
		t.Fatal("connect to count reconciliation rows")
	}
	defer connection.Close(context.Background())
	var count int64
	if err := connection.QueryRow(ctx, query, arguments...).Scan(&count); err != nil {
		t.Fatal("count reconciliation rows")
	}
	return count
}

func assertProjectionReconcilerNaturalJobs(
	t *testing.T,
	databaseURL, embeddingSpace string,
	want int64,
) {
	t.Helper()
	total := countProjectionReconcilerRows(t, databaseURL, `
		SELECT count(*)
		FROM agent_memory.embedding_projection_jobs
		WHERE embedding_space=$1`, embeddingSpace)
	distinctNaturalKeys := countProjectionReconcilerRows(t, databaseURL, `
		SELECT count(*)
		FROM (
			SELECT tenant_id, user_id, memory_id, embedding_space, count(*)
			FROM agent_memory.embedding_projection_jobs
			WHERE embedding_space=$1
			GROUP BY tenant_id, user_id, memory_id, embedding_space
		) AS natural_jobs`, embeddingSpace)
	if total != want || distinctNaturalKeys != want {
		t.Fatalf("projection jobs total/distinct=%d/%d, want %d/%d", total, distinctNaturalKeys, want, want)
	}
}
