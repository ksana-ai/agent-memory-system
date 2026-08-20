//go:build integration && !vector

package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
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
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/kai443/go-agent-memory-system/internal/domain"
	"github.com/kai443/go-agent-memory-system/internal/embedding"
	storecontract "github.com/kai443/go-agent-memory-system/internal/store"
	"github.com/kai443/go-agent-memory-system/internal/store/postgres"
)

const (
	restartWorkerModel        = "restart-integration-model"
	restartWorkerSecret       = "restart-integration-secret"
	restartWorkerCardValue    = "sensitive restart integration memory value"
	restartWorkerEvidenceText = "sensitive restart integration evidence"
)

var restartDatabaseSequence atomic.Uint64

func TestProjectionWorkerProcessRestartRecoveryAndDeletionPropagation(t *testing.T) {
	binary := requiredProjectionWorkerBinary(t)
	databaseURL := isolatedProjectionWorkerDatabase(t, "restart")
	fake := newRestartEmbeddingServer(t, restartWorkerModel)
	defer fake.Close()

	probeVector := restartBasisVector(0)
	modelFingerprint := embedding.VectorSHA256(probeVector)
	embeddingSpace, err := embedding.SpaceID(
		embedding.ProviderLMStudio,
		restartWorkerModel,
		postgres.VectorDimension,
		embedding.MemoryCardDocumentVersion,
		embedding.RawQueryVersion,
		modelFingerprint,
	)
	if err != nil {
		t.Fatal("derive restart integration embedding space")
	}

	registerOutput := runProjectionWorkerChild(t, binary, projectionWorkerChildConfig{
		databaseURL: databaseURL, endpoint: fake.URL(), model: restartWorkerModel,
		embeddingSpace: embeddingSpace, mode: workerModeRegister,
		requestTimeout: 2 * time.Second, leaseDuration: 3 * time.Second,
		once: true,
	})
	assertProjectionWorkerOutputRedacted(t, registerOutput, databaseURL, fake.URL(), restartWorkerEvidenceText, restartWorkerCardValue)

	storage := openRestartWorkerStore(t, databaseURL)
	defer storage.Close()
	tenantID := fmt.Sprintf("tenant_restart_%d", time.Now().UnixNano())
	userID := fmt.Sprintf("user_restart_%d", time.Now().UnixNano())
	card := seedRestartWorkerCard(t, storage, tenantID, userID)
	document := embedding.MemoryCardDocumentV1(card)
	fake.SetExpectedDocument(document)
	fake.BlockNextDocumentRequest()

	continuous := startProjectionWorkerChild(t, binary, projectionWorkerChildConfig{
		databaseURL: databaseURL, endpoint: fake.URL(), model: restartWorkerModel,
		embeddingSpace: embeddingSpace, mode: workerModeRun,
		requestTimeout: 2 * time.Second, leaseDuration: 2500 * time.Millisecond,
		idleInterval: 10 * time.Millisecond,
	})
	select {
	case <-fake.DocumentStarted():
	case <-time.After(15 * time.Second):
		continuous.killAndWait()
		t.Fatal("continuous worker did not reach the blocked document request")
	}

	leased := loadRestartWorkerJob(t, databaseURL, tenantID, userID, card.ID, embeddingSpace)
	if leased.state != string(postgres.ProjectionJobLeased) ||
		leased.attemptCount != 1 || leased.leaseVersion != 1 ||
		leased.leaseOwner == "" || leased.leaseUntil == nil {
		continuous.killAndWait()
		t.Fatalf("blocked worker job was not durably leased: state=%s attempt=%d version=%d", leased.state, leased.attemptCount, leased.leaseVersion)
	}

	appendContext, cancelAppend := context.WithTimeout(context.Background(), time.Second)
	appendErr := storage.AppendEvidence(appendContext, domain.EvidenceEvent{
		ID:         "event-during-blocked-http",
		TenantID:   tenantID,
		UserID:     userID,
		SessionID:  "session-during-blocked-http",
		Actor:      domain.ActorUser,
		Content:    "independent append while provider request is blocked",
		OccurredAt: time.Now().UTC(),
		RecordedAt: time.Now().UTC().Add(time.Microsecond),
	})
	cancelAppend()
	if appendErr != nil {
		continuous.killAndWait()
		t.Fatal("same-scope append was blocked by provider I/O")
	}

	continuousOutput := continuous.killAndWait()
	assertProjectionWorkerOutputRedacted(t, continuousOutput, databaseURL, fake.URL(), document, restartWorkerEvidenceText, restartWorkerCardValue)
	waitForRestartWorkerLeaseExpiry(t, databaseURL, leased.id)

	restartOutput := runProjectionWorkerChild(t, binary, projectionWorkerChildConfig{
		databaseURL: databaseURL, endpoint: fake.URL(), model: restartWorkerModel,
		embeddingSpace: embeddingSpace, mode: workerModeRun,
		requestTimeout: 2 * time.Second, leaseDuration: 3 * time.Second,
		idleInterval: 10 * time.Millisecond, once: true,
	})
	assertProjectionWorkerOutputRedacted(t, restartOutput, databaseURL, fake.URL(), document, restartWorkerEvidenceText, restartWorkerCardValue)
	recovered := loadRestartWorkerJob(t, databaseURL, tenantID, userID, card.ID, embeddingSpace)
	if recovered.state != string(postgres.ProjectionJobSucceeded) ||
		recovered.attemptCount != 2 || recovered.leaseVersion != 2 ||
		recovered.leaseOwner != "" || recovered.leaseUntil != nil {
		t.Fatalf("restarted worker did not reclaim and finalize: state=%s attempt=%d version=%d", recovered.state, recovered.attemptCount, recovered.leaseVersion)
	}
	if countRestartWorkerEmbeddings(t, databaseURL, tenantID, userID, card.ID, embeddingSpace) != 1 {
		t.Fatal("restarted worker did not persist exactly one vector")
	}

	if _, err := storage.ForgetUser(context.Background(), tenantID, userID, time.Now().UTC()); err != nil {
		t.Fatal("ForgetUser failed for restart integration scope")
	}
	if countRestartWorkerJobs(t, databaseURL, tenantID, userID) != 0 ||
		countRestartWorkerEmbeddings(t, databaseURL, tenantID, userID, card.ID, embeddingSpace) != 0 {
		t.Fatal("ForgetUser did not cascade to projection jobs and vectors")
	}
	runtimeRequestsBeforeEmptyRun := fake.RuntimeRequestCount()
	emptyOutput := runProjectionWorkerChild(t, binary, projectionWorkerChildConfig{
		databaseURL: databaseURL, endpoint: fake.URL(), model: restartWorkerModel,
		embeddingSpace: embeddingSpace, mode: workerModeRun,
		requestTimeout: 2 * time.Second, leaseDuration: 3 * time.Second,
		idleInterval: 10 * time.Millisecond, once: true,
	})
	assertProjectionWorkerOutputRedacted(t, emptyOutput, databaseURL, fake.URL(), document, restartWorkerEvidenceText, restartWorkerCardValue)
	if fake.RuntimeRequestCount() != runtimeRequestsBeforeEmptyRun ||
		countRestartWorkerJobs(t, databaseURL, tenantID, userID) != 0 ||
		countRestartWorkerEmbeddings(t, databaseURL, tenantID, userID, card.ID, embeddingSpace) != 0 {
		t.Fatal("empty restart run resurrected forgotten projection data")
	}
	fake.AssertProtocol(t, document)
}

type projectionWorkerChildConfig struct {
	databaseURL    string
	endpoint       string
	model          string
	embeddingSpace string
	mode           string
	requestTimeout time.Duration
	leaseDuration  time.Duration
	idleInterval   time.Duration
	once           bool
}

type projectionWorkerChild struct {
	command *exec.Cmd
	output  *lockedProjectionWorkerBuffer
}

func startProjectionWorkerChild(t *testing.T, binary string, config projectionWorkerChildConfig) *projectionWorkerChild {
	t.Helper()
	command := exec.Command(binary)
	command.Env = projectionWorkerChildEnvironment(config)
	output := &lockedProjectionWorkerBuffer{}
	command.Stdout = output
	command.Stderr = output
	if err := command.Start(); err != nil {
		t.Fatal("start projection worker child")
	}
	return &projectionWorkerChild{command: command, output: output}
}

func runProjectionWorkerChild(t *testing.T, binary string, config projectionWorkerChildConfig) string {
	t.Helper()
	child := startProjectionWorkerChild(t, binary, config)
	done := make(chan error, 1)
	go func() { done <- child.command.Wait() }()
	select {
	case err := <-done:
		output := child.output.String()
		if err != nil {
			assertProjectionWorkerOutputRedacted(t, output, config.databaseURL, config.endpoint, restartWorkerEvidenceText, restartWorkerCardValue)
			t.Fatal("projection worker child exited unsuccessfully")
		}
		return output
	case <-time.After(30 * time.Second):
		_ = child.command.Process.Kill()
		<-done
		t.Fatal("projection worker child timed out")
		return ""
	}
}

func (child *projectionWorkerChild) killAndWait() string {
	if child == nil || child.command == nil || child.command.Process == nil {
		return ""
	}
	_ = child.command.Process.Kill()
	_ = child.command.Wait()
	return child.output.String()
}

func projectionWorkerChildEnvironment(config projectionWorkerChildConfig) []string {
	idleInterval := config.idleInterval
	if idleInterval == 0 {
		idleInterval = 10 * time.Millisecond
	}
	overrides := map[string]string{
		"DATABASE_URL":                      config.databaseURL,
		"LMSTUDIO_EMBEDDINGS_URL":           config.endpoint,
		"LMSTUDIO_EMBEDDING_MODEL":          config.model,
		"PROJECTION_WORKER_EMBEDDING_SPACE": config.embeddingSpace,
		"PROJECTION_WORKER_MODE":            config.mode,
		"PROJECTION_WORKER_REQUEST_TIMEOUT": config.requestTimeout.String(),
		"PROJECTION_WORKER_LEASE_DURATION":  config.leaseDuration.String(),
		"PROJECTION_WORKER_IDLE_INTERVAL":   idleInterval.String(),
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

type lockedProjectionWorkerBuffer struct {
	mutex sync.Mutex
	data  bytes.Buffer
}

func (buffer *lockedProjectionWorkerBuffer) Write(value []byte) (int, error) {
	buffer.mutex.Lock()
	defer buffer.mutex.Unlock()
	return buffer.data.Write(value)
}

func (buffer *lockedProjectionWorkerBuffer) String() string {
	buffer.mutex.Lock()
	defer buffer.mutex.Unlock()
	return buffer.data.String()
}

func assertProjectionWorkerOutputRedacted(t *testing.T, output string, forbidden ...string) {
	t.Helper()
	forbidden = append(forbidden, restartWorkerSecret)
	for _, value := range forbidden {
		if value != "" && strings.Contains(output, value) {
			t.Fatal("projection worker output leaked sensitive configuration or content")
		}
	}
}

func requiredProjectionWorkerBinary(t *testing.T) string {
	t.Helper()
	binary := strings.TrimSpace(os.Getenv("TEST_PROJECTION_WORKER_BINARY"))
	if binary == "" {
		t.Fatal("TEST_PROJECTION_WORKER_BINARY is required")
	}
	if _, err := os.Stat(binary); err != nil {
		t.Fatal("TEST_PROJECTION_WORKER_BINARY is not readable")
	}
	return binary
}

func isolatedProjectionWorkerDatabase(t *testing.T, label string) string {
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
	databaseName := fmt.Sprintf("agent_memory_worker_%s_%d_%d", label, time.Now().UnixNano(), restartDatabaseSequence.Add(1))
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
		t.Fatal("create isolated projection worker database")
	}
	t.Cleanup(func() {
		dropContext, dropCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer dropCancel()
		dropConnection, dropErr := pgx.ConnectConfig(dropContext, adminConfig)
		if dropErr != nil {
			t.Error("reconnect to drop isolated projection worker database")
			return
		}
		defer dropConnection.Close(context.Background())
		if _, dropErr := dropConnection.Exec(dropContext, "DROP DATABASE IF EXISTS "+quotedDatabase+" WITH (FORCE)"); dropErr != nil {
			t.Error("drop isolated projection worker database")
		}
	})
	parsedURL.Path = "/" + databaseName
	parsedURL.RawPath = ""
	return parsedURL.String()
}

func openRestartWorkerStore(t *testing.T, databaseURL string) *postgres.Store {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	storage, err := postgres.Open(ctx, databaseURL)
	if err != nil {
		t.Fatal("open isolated restart worker store")
	}
	return storage
}

func seedRestartWorkerCard(t *testing.T, storage *postgres.Store, tenantID, userID string) domain.MemoryCard {
	t.Helper()
	ctx := context.Background()
	baseTime := time.Now().UTC().Add(-time.Second).Truncate(time.Microsecond)
	event := domain.EvidenceEvent{
		ID: "event-restart-worker", TenantID: tenantID, UserID: userID,
		SessionID: "session-restart-worker", Actor: domain.ActorUser,
		Content: restartWorkerEvidenceText, OccurredAt: baseTime, RecordedAt: baseTime.Add(time.Microsecond),
		Metadata: map[string]string{"source": "restart-integration"},
	}
	if err := storage.AppendEvidence(ctx, event); err != nil {
		t.Fatal("append restart worker evidence")
	}
	candidate := domain.MemoryCandidate{
		ID: "candidate-restart-worker", TenantID: tenantID, UserID: userID,
		Kind: domain.MemoryKindSemantic, Category: "preference", Key: "restart-key",
		Value: restartWorkerCardValue, Person: "self", Relationship: "self",
		Backstory: "restart integration backstory", SourceEventIDs: []string{event.ID},
		Extractor: "restart-integration", ExtractorVersion: "v1",
		Status: domain.CandidatePending, CreatedAt: baseTime.Add(2 * time.Microsecond),
	}
	if err := storage.CreateCandidate(ctx, candidate); err != nil {
		t.Fatal("create restart worker candidate")
	}
	_, card, err := storage.ReviewCandidate(ctx, storecontract.CandidateReviewCommand{
		TenantID: tenantID, UserID: userID, CandidateID: candidate.ID,
		MemoryID: "memory-restart-worker",
		Review: domain.CandidateReview{
			Decision: domain.DecisionApprove, ReviewerID: "restart-reviewer",
			Reason: "restart integration approval", ReviewedAt: baseTime.Add(3 * time.Microsecond),
		},
	})
	if err != nil || card == nil {
		t.Fatal("approve restart worker candidate")
	}
	return *card
}

type restartWorkerJobSnapshot struct {
	id           int64
	state        string
	attemptCount int
	leaseVersion int64
	leaseOwner   string
	leaseUntil   *time.Time
}

func loadRestartWorkerJob(t *testing.T, databaseURL, tenantID, userID, memoryID, embeddingSpace string) restartWorkerJobSnapshot {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	conn, err := pgx.Connect(ctx, databaseURL)
	if err != nil {
		t.Fatal("connect to inspect restart worker job")
	}
	defer conn.Close(context.Background())
	var result restartWorkerJobSnapshot
	var leaseUntil pgtype.Timestamptz
	if err := conn.QueryRow(ctx, `
		SELECT id, state, attempt_count, lease_version, COALESCE(lease_owner, ''), lease_until
		FROM agent_memory.embedding_projection_jobs
		WHERE tenant_id=$1 AND user_id=$2 AND memory_id=$3 AND embedding_space=$4`,
		tenantID, userID, memoryID, embeddingSpace,
	).Scan(&result.id, &result.state, &result.attemptCount, &result.leaseVersion, &result.leaseOwner, &leaseUntil); err != nil {
		t.Fatal("load restart worker job")
	}
	if leaseUntil.Valid {
		value := leaseUntil.Time.UTC().Truncate(time.Microsecond)
		result.leaseUntil = &value
	}
	return result
}

func waitForRestartWorkerLeaseExpiry(t *testing.T, databaseURL string, jobID int64) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	conn, err := pgx.Connect(ctx, databaseURL)
	if err != nil {
		t.Fatal("connect to await database lease expiry")
	}
	defer conn.Close(context.Background())
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()
	for {
		var expired bool
		if err := conn.QueryRow(ctx, `
			SELECT state='leased' AND lease_until IS NOT NULL AND clock_timestamp() >= lease_until
			FROM agent_memory.embedding_projection_jobs
			WHERE id=$1`, jobID).Scan(&expired); err != nil {
			t.Fatal("inspect lease with database clock")
		}
		if expired {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatal("database lease did not expire")
		case <-ticker.C:
		}
	}
}

func countRestartWorkerJobs(t *testing.T, databaseURL, tenantID, userID string) int {
	t.Helper()
	return restartWorkerCount(t, databaseURL, `
		SELECT count(*) FROM agent_memory.embedding_projection_jobs
		WHERE tenant_id=$1 AND user_id=$2`, tenantID, userID)
}

func countRestartWorkerEmbeddings(t *testing.T, databaseURL, tenantID, userID, memoryID, embeddingSpace string) int {
	t.Helper()
	return restartWorkerCount(t, databaseURL, `
		SELECT count(*) FROM agent_memory.memory_embeddings
		WHERE tenant_id=$1 AND user_id=$2 AND memory_id=$3 AND embedding_space=$4`,
		tenantID, userID, memoryID, embeddingSpace)
}

func restartWorkerCount(t *testing.T, databaseURL, query string, arguments ...any) int {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	conn, err := pgx.Connect(ctx, databaseURL)
	if err != nil {
		t.Fatal("connect to count restart worker rows")
	}
	defer conn.Close(context.Background())
	var count int
	if err := conn.QueryRow(ctx, query, arguments...).Scan(&count); err != nil {
		t.Fatal("count restart worker rows")
	}
	return count
}

type restartEmbeddingRequest struct {
	Model string   `json:"model"`
	Input []string `json:"input"`
}

type restartEmbeddingServer struct {
	server           *httptest.Server
	model            string
	expectedDocument string
	documentStarted  chan struct{}
	blockNext        atomic.Bool
	runtimeRequests  atomic.Int64
	mutex            sync.Mutex
	requests         []restartEmbeddingRequest
}

func newRestartEmbeddingServer(t *testing.T, model string) *restartEmbeddingServer {
	t.Helper()
	result := &restartEmbeddingServer{model: model, documentStarted: make(chan struct{}, 1)}
	result.server = httptest.NewServer(http.HandlerFunc(result.handle))
	return result
}

func (server *restartEmbeddingServer) URL() string {
	return server.server.URL + "/" + restartWorkerSecret
}
func (server *restartEmbeddingServer) Close() { server.server.Close() }
func (server *restartEmbeddingServer) DocumentStarted() <-chan struct{} {
	return server.documentStarted
}
func (server *restartEmbeddingServer) RuntimeRequestCount() int64 {
	return server.runtimeRequests.Load()
}
func (server *restartEmbeddingServer) SetExpectedDocument(document string) {
	server.mutex.Lock()
	defer server.mutex.Unlock()
	server.expectedDocument = document
}
func (server *restartEmbeddingServer) BlockNextDocumentRequest() { server.blockNext.Store(true) }

func (server *restartEmbeddingServer) handle(writer http.ResponseWriter, request *http.Request) {
	defer request.Body.Close()
	var payload restartEmbeddingRequest
	decoder := json.NewDecoder(http.MaxBytesReader(writer, request.Body, 1<<20))
	if request.Method != http.MethodPost || decoder.Decode(&payload) != nil {
		http.Error(writer, "invalid request", http.StatusBadRequest)
		return
	}
	server.mutex.Lock()
	server.requests = append(server.requests, payload)
	expectedDocument := server.expectedDocument
	server.mutex.Unlock()
	if payload.Model != server.model || len(payload.Input) < 1 || payload.Input[0] != embedding.ProbeTextV1 {
		http.Error(writer, "invalid protocol", http.StatusBadRequest)
		return
	}
	if len(payload.Input) == 2 {
		server.runtimeRequests.Add(1)
		if expectedDocument == "" || payload.Input[1] != expectedDocument {
			http.Error(writer, "invalid document", http.StatusBadRequest)
			return
		}
		if server.blockNext.CompareAndSwap(true, false) {
			select {
			case server.documentStarted <- struct{}{}:
			default:
			}
			<-request.Context().Done()
			return
		}
	} else if len(payload.Input) != 1 {
		http.Error(writer, "invalid batch", http.StatusBadRequest)
		return
	}
	data := make([]map[string]any, len(payload.Input))
	for index := range payload.Input {
		vectorIndex := 0
		if index == 1 {
			vectorIndex = 1
		}
		data[index] = map[string]any{"index": index, "embedding": restartBasisVector(vectorIndex)}
	}
	writer.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(writer).Encode(map[string]any{"model": server.model, "data": data})
}

func (server *restartEmbeddingServer) AssertProtocol(t *testing.T, document string) {
	t.Helper()
	server.mutex.Lock()
	requests := append([]restartEmbeddingRequest(nil), server.requests...)
	server.mutex.Unlock()
	probeRequests := 0
	runtimeRequests := 0
	for _, request := range requests {
		if request.Model != server.model || len(request.Input) == 0 || request.Input[0] != embedding.ProbeTextV1 {
			t.Fatal("fake embedding server observed an invalid model or probe")
		}
		switch len(request.Input) {
		case 1:
			probeRequests++
		case 2:
			runtimeRequests++
			if request.Input[1] != document {
				t.Fatal("fake embedding server observed an invalid document batch")
			}
		default:
			t.Fatal("fake embedding server observed an invalid batch size")
		}
	}
	if probeRequests != 4 || runtimeRequests != 2 {
		t.Fatalf("fake embedding protocol requests probe/runtime=%d/%d, want 4/2", probeRequests, runtimeRequests)
	}
}

func restartBasisVector(index int) []float32 {
	result := make([]float32, postgres.VectorDimension)
	result[index] = 1
	return result
}
