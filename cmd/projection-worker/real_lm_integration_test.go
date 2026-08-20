//go:build integration && vector

package main

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"os/exec"
	"sort"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/kai443/go-agent-memory-system/internal/domain"
	"github.com/kai443/go-agent-memory-system/internal/embedding"
	storecontract "github.com/kai443/go-agent-memory-system/internal/store"
	"github.com/kai443/go-agent-memory-system/internal/store/postgres"
)

const (
	realWorkerEvidence = "sensitive real LM projection evidence"
	realWorkerValue    = "sensitive real LM projection memory"
)

var realWorkerDatabaseSequence atomic.Uint64

func TestProjectionWorkerProcessRealLMStudioOnceAndForget(t *testing.T) {
	binary := realWorkerRequiredEnvironment(t, "TEST_PROJECTION_WORKER_BINARY")
	endpoint := realWorkerRequiredEnvironment(t, "LMSTUDIO_EMBEDDINGS_URL")
	model := realWorkerRequiredEnvironment(t, "LMSTUDIO_EMBEDDING_MODEL")
	databaseURL := isolatedRealWorkerDatabase(t)

	client, err := embedding.NewClient(embedding.Config{
		Endpoint: endpoint, Model: model, ExpectedDimension: postgres.VectorDimension,
		Timeout: 20 * time.Second, MaxBatchSize: 2,
	})
	if err != nil {
		t.Fatal("create real LM integration client")
	}
	probeContext, cancelProbe := context.WithTimeout(context.Background(), 30*time.Second)
	probeVectors, err := client.Embed(probeContext, []string{embedding.ProbeTextV1})
	cancelProbe()
	if err != nil || len(probeVectors) != 1 || len(probeVectors[0]) != postgres.VectorDimension {
		t.Fatal("probe real LM integration model")
	}
	fingerprint := embedding.VectorSHA256(probeVectors[0])
	space, err := embedding.SpaceID(
		embedding.ProviderLMStudio,
		model,
		postgres.VectorDimension,
		embedding.MemoryCardDocumentVersion,
		embedding.RawQueryVersion,
		fingerprint,
	)
	if err != nil {
		t.Fatal("derive real LM integration space")
	}

	registerOutput := runRealWorkerChild(t, binary, realWorkerChildConfig{
		databaseURL: databaseURL, endpoint: endpoint, model: model,
		space: space, mode: workerModeRegister, once: true,
	})
	assertRealWorkerOutputRedacted(t, registerOutput, databaseURL, endpoint, realWorkerEvidence, realWorkerValue)

	storage := openRealWorkerStore(t, databaseURL)
	defer storage.Close()
	tenantID := fmt.Sprintf("tenant_real_worker_%d", time.Now().UnixNano())
	userID := fmt.Sprintf("user_real_worker_%d", time.Now().UnixNano())
	card := seedRealWorkerCard(t, storage, tenantID, userID)
	document := embedding.MemoryCardDocumentV1(card)
	stateBefore, attemptsBefore, leaseVersionBefore := loadRealWorkerJob(
		t, databaseURL, tenantID, userID, card.ID, space,
	)
	if stateBefore != string(postgres.ProjectionJobPending) || attemptsBefore != 0 || leaseVersionBefore != 0 {
		t.Fatalf(
			"real worker precondition state/attempt/version=%s/%d/%d, want pending/0/0",
			stateBefore,
			attemptsBefore,
			leaseVersionBefore,
		)
	}
	if countRealWorkerRows(t, databaseURL, `
		SELECT count(*) FROM agent_memory.memory_embeddings
		WHERE tenant_id=$1 AND user_id=$2 AND memory_id=$3 AND embedding_space=$4`,
		tenantID, userID, card.ID, space,
	) != 0 {
		t.Fatal("real worker fixture already contained a vector before the worker ran")
	}
	runOutput := runRealWorkerChild(t, binary, realWorkerChildConfig{
		databaseURL: databaseURL, endpoint: endpoint, model: model,
		space: space, mode: workerModeRun, once: true,
	})
	assertRealWorkerOutputRedacted(t, runOutput, databaseURL, endpoint, document, realWorkerEvidence, realWorkerValue)

	state, attemptCount, leaseVersion := loadRealWorkerJob(t, databaseURL, tenantID, userID, card.ID, space)
	if state != string(postgres.ProjectionJobSucceeded) || attemptCount != 1 || leaseVersion != 1 {
		t.Fatalf("real worker job state/attempt/version=%s/%d/%d, want succeeded/1/1", state, attemptCount, leaseVersion)
	}
	if countRealWorkerRows(t, databaseURL, `
		SELECT count(*) FROM agent_memory.memory_embeddings
		WHERE tenant_id=$1 AND user_id=$2 AND memory_id=$3 AND embedding_space=$4`,
		tenantID, userID, card.ID, space,
	) != 1 {
		t.Fatal("real worker did not persist exactly one vector")
	}

	if _, err := storage.ForgetUser(context.Background(), tenantID, userID, time.Now().UTC()); err != nil {
		t.Fatal("ForgetUser failed for real worker scope")
	}
	if countRealWorkerRows(t, databaseURL, `
		SELECT count(*) FROM agent_memory.embedding_projection_jobs
		WHERE tenant_id=$1 AND user_id=$2`, tenantID, userID,
	) != 0 || countRealWorkerRows(t, databaseURL, `
		SELECT count(*) FROM agent_memory.memory_embeddings
		WHERE tenant_id=$1 AND user_id=$2`, tenantID, userID,
	) != 0 {
		t.Fatal("ForgetUser did not delete real worker job and vector")
	}
}

type realWorkerChildConfig struct {
	databaseURL string
	endpoint    string
	model       string
	space       string
	mode        string
	once        bool
}

func runRealWorkerChild(t *testing.T, binary string, config realWorkerChildConfig) string {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	command := exec.CommandContext(ctx, binary)
	command.Env = realWorkerChildEnvironment(config)
	output, err := command.CombinedOutput()
	result := string(output)
	if err != nil || ctx.Err() != nil {
		assertRealWorkerOutputRedacted(t, result, config.databaseURL, config.endpoint, realWorkerEvidence, realWorkerValue)
		t.Fatal("real projection worker child exited unsuccessfully")
	}
	return result
}

func realWorkerChildEnvironment(config realWorkerChildConfig) []string {
	overrides := map[string]string{
		"DATABASE_URL":                      config.databaseURL,
		"LMSTUDIO_EMBEDDINGS_URL":           config.endpoint,
		"LMSTUDIO_EMBEDDING_MODEL":          config.model,
		"PROJECTION_WORKER_EMBEDDING_SPACE": config.space,
		"PROJECTION_WORKER_MODE":            config.mode,
		"PROJECTION_WORKER_REQUEST_TIMEOUT": "20s",
		"PROJECTION_WORKER_LEASE_DURATION":  "30s",
		"PROJECTION_WORKER_IDLE_INTERVAL":   "10ms",
		"PROJECTION_WORKER_MAX_ATTEMPTS":    "3",
		"PROJECTION_WORKER_ONCE":            fmt.Sprintf("%t", config.once),
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

func assertRealWorkerOutputRedacted(t *testing.T, output string, forbidden ...string) {
	t.Helper()
	for _, value := range forbidden {
		if value != "" && strings.Contains(output, value) {
			t.Fatal("real projection worker output leaked configuration or content")
		}
	}
}

func realWorkerRequiredEnvironment(t *testing.T, name string) string {
	t.Helper()
	value := strings.TrimSpace(os.Getenv(name))
	if value == "" {
		t.Fatalf("%s is required", name)
	}
	if name == "TEST_PROJECTION_WORKER_BINARY" {
		if _, err := os.Stat(value); err != nil {
			t.Fatal("TEST_PROJECTION_WORKER_BINARY is not readable")
		}
	}
	return value
}

func isolatedRealWorkerDatabase(t *testing.T) string {
	t.Helper()
	baseURL := realWorkerRequiredEnvironment(t, "TEST_DATABASE_URL")
	baseConfig, err := pgx.ParseConfig(baseURL)
	if err != nil {
		t.Fatal("parse TEST_DATABASE_URL")
	}
	parsedURL, err := url.Parse(baseURL)
	if err != nil || (parsedURL.Scheme != "postgres" && parsedURL.Scheme != "postgresql") {
		t.Fatal("TEST_DATABASE_URL must be a PostgreSQL URL")
	}
	databaseName := fmt.Sprintf("agent_memory_worker_real_%d_%d", time.Now().UnixNano(), realWorkerDatabaseSequence.Add(1))
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
		t.Fatal("create isolated real worker database")
	}
	t.Cleanup(func() {
		dropContext, dropCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer dropCancel()
		dropConnection, dropErr := pgx.ConnectConfig(dropContext, adminConfig)
		if dropErr != nil {
			t.Error("reconnect to drop isolated real worker database")
			return
		}
		defer dropConnection.Close(context.Background())
		if _, dropErr := dropConnection.Exec(dropContext, "DROP DATABASE IF EXISTS "+quotedDatabase+" WITH (FORCE)"); dropErr != nil {
			t.Error("drop isolated real worker database")
		}
	})
	parsedURL.Path = "/" + databaseName
	parsedURL.RawPath = ""
	return parsedURL.String()
}

func openRealWorkerStore(t *testing.T, databaseURL string) *postgres.Store {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	storage, err := postgres.Open(ctx, databaseURL)
	if err != nil {
		t.Fatal("open isolated real worker store")
	}
	return storage
}

func seedRealWorkerCard(t *testing.T, storage *postgres.Store, tenantID, userID string) domain.MemoryCard {
	t.Helper()
	ctx := context.Background()
	baseTime := time.Now().UTC().Add(-time.Second).Truncate(time.Microsecond)
	event := domain.EvidenceEvent{
		ID: "event-real-worker", TenantID: tenantID, UserID: userID,
		SessionID: "session-real-worker", Actor: domain.ActorUser,
		Content: realWorkerEvidence, OccurredAt: baseTime, RecordedAt: baseTime.Add(time.Microsecond),
	}
	if err := storage.AppendEvidence(ctx, event); err != nil {
		t.Fatal("append real worker evidence")
	}
	candidate := domain.MemoryCandidate{
		ID: "candidate-real-worker", TenantID: tenantID, UserID: userID,
		Kind: domain.MemoryKindSemantic, Category: "preference", Key: "real-worker-key",
		Value: realWorkerValue, Person: "self", Relationship: "self",
		Backstory: "real worker integration backstory", SourceEventIDs: []string{event.ID},
		Extractor: "real-worker-integration", ExtractorVersion: "v1",
		Status: domain.CandidatePending, CreatedAt: baseTime.Add(2 * time.Microsecond),
	}
	if err := storage.CreateCandidate(ctx, candidate); err != nil {
		t.Fatal("create real worker candidate")
	}
	_, card, err := storage.ReviewCandidate(ctx, storecontract.CandidateReviewCommand{
		TenantID: tenantID, UserID: userID, CandidateID: candidate.ID,
		MemoryID: "memory-real-worker",
		Review: domain.CandidateReview{
			Decision: domain.DecisionApprove, ReviewerID: "real-worker-reviewer",
			Reason: "real worker integration approval", ReviewedAt: baseTime.Add(3 * time.Microsecond),
		},
	})
	if err != nil || card == nil {
		t.Fatal("approve real worker candidate")
	}
	return *card
}

func loadRealWorkerJob(t *testing.T, databaseURL, tenantID, userID, memoryID, space string) (string, int, int64) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	conn, err := pgx.Connect(ctx, databaseURL)
	if err != nil {
		t.Fatal("connect to inspect real worker job")
	}
	defer conn.Close(context.Background())
	var state string
	var attemptCount int
	var leaseVersion int64
	if err := conn.QueryRow(ctx, `
		SELECT state, attempt_count, lease_version
		FROM agent_memory.embedding_projection_jobs
		WHERE tenant_id=$1 AND user_id=$2 AND memory_id=$3 AND embedding_space=$4`,
		tenantID, userID, memoryID, space,
	).Scan(&state, &attemptCount, &leaseVersion); err != nil {
		t.Fatal("load real worker job")
	}
	return state, attemptCount, leaseVersion
}

func countRealWorkerRows(t *testing.T, databaseURL, query string, arguments ...any) int {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	conn, err := pgx.Connect(ctx, databaseURL)
	if err != nil {
		t.Fatal("connect to count real worker rows")
	}
	defer conn.Close(context.Background())
	var count int
	if err := conn.QueryRow(ctx, query, arguments...).Scan(&count); err != nil {
		t.Fatal("count real worker rows")
	}
	return count
}
