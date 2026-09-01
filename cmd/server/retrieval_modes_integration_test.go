//go:build integration && !vector

package main_test

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"os/exec"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/ksana-ai/agent-memory-system/internal/domain"
	"github.com/ksana-ai/agent-memory-system/internal/embedding"
	"github.com/ksana-ai/agent-memory-system/internal/migrations"
	"github.com/ksana-ai/agent-memory-system/internal/store/postgres"
)

const (
	serverRetrievalModel       = "server-retrieval-private-model"
	serverRetrievalQuery       = "server-retrieval-private-window-query"
	serverRetrievalValue       = "server-retrieval-private-window-value"
	serverRetrievalEvidence    = "server-retrieval-private-window-evidence"
	serverRetrievalApplication = "TOP_SECRET_SERVER_RETRIEVAL_PROCESS"
	serverRetrievalOperationID = "33333333-3333-4333-8333-333333333333"
)

var serverRetrievalDatabaseSequence atomic.Uint64

func TestServerServingDenseAndHybridDeletePropagationAcrossRestart(t *testing.T) {
	binary := requiredServerBinary(t)
	databaseURL := isolatedServerRetrievalDatabase(t)
	fixture := newServerEmbeddingFixture(t)
	storage := openServerRetrievalStore(t, databaseURL)
	defer storage.Close()
	definition := registerServerRetrievalTarget(t, storage, fixture)

	// FTS is the explicit default and must not touch LM Studio even when all
	// embedding settings are present. It is used here to exercise the real HTTP
	// approval path that atomically creates the durable projection job.
	fts := startRetrievalServer(t, binary, databaseURL, map[string]string{
		"LMSTUDIO_EMBEDDINGS_URL":       fixture.Endpoint(),
		"LMSTUDIO_EMBEDDING_MODEL":      serverRetrievalModel,
		"SERVER_EXPECTED_SERVING_SPACE": definition.ID,
	}, true)
	assertServerPhase(t, &http.Client{Timeout: 5 * time.Second}, fts.baseURL, "postgres-fts")
	ingestProposeApprove(
		t,
		&http.Client{Timeout: 5 * time.Second},
		fts.baseURL,
		"tenant-serving-retrieval",
		"user-serving-retrieval",
		"event-serving-retrieval",
		serverRetrievalEvidence,
		"seat_preference",
		serverRetrievalValue,
	)
	assertContextPack(
		t,
		&http.Client{Timeout: 5 * time.Second},
		fts.baseURL,
		"tenant-serving-retrieval",
		"user-serving-retrieval",
		serverRetrievalQuery,
		serverRetrievalValue,
		1,
	)
	fts.stop(t)
	if fixture.Requests() != 0 {
		t.Fatalf("FTS process made %d embedding requests, want 0", fixture.Requests())
	}

	projectServerRetrievalCard(t, storage, definition.ID, fixture.DocumentVector())
	receipt, err := storage.PromoteProjection(context.Background(), postgres.PromoteProjectionCommand{
		OperationID: serverRetrievalOperationID,
		ToSpace:     definition.ID,
	})
	if err != nil {
		t.Fatalf("promote serving retrieval target: %v", err)
	}
	if receipt.ToSpace != definition.ID || receipt.LiveCardCount != 1 || receipt.CoveredCardCount != 1 {
		t.Fatalf("promotion receipt=%#v", receipt)
	}

	client := &http.Client{Timeout: 5 * time.Second}
	dense := startRetrievalServer(t, binary, databaseURL, serverRetrievalEnvironment(fixture, definition.ID, "dense"), true)
	assertServerPhase(t, client, dense.baseURL, "postgres-dense")
	assertContextPack(
		t, client, dense.baseURL,
		"tenant-serving-retrieval", "user-serving-retrieval",
		serverRetrievalQuery, serverRetrievalValue, 1,
	)
	dense.stop(t)

	hybrid := startRetrievalServer(t, binary, databaseURL, serverRetrievalEnvironment(fixture, definition.ID, "hybrid"), true)
	assertServerPhase(t, client, hybrid.baseURL, "postgres-hybrid-rrf")
	assertContextPack(
		t, client, hybrid.baseURL,
		"tenant-serving-retrieval", "user-serving-retrieval",
		serverRetrievalQuery, serverRetrievalValue, 1,
	)
	var deletion domain.DeletionReceipt
	doServerJSON(
		t,
		client,
		http.MethodDelete,
		hybrid.baseURL+"/v1/users/user-serving-retrieval",
		"tenant-serving-retrieval",
		"",
		nil,
		http.StatusOK,
		&deletion,
	)
	if deletion.EvidenceDeleted != 1 || deletion.CandidatesDeleted != 1 || deletion.MemoriesDeleted != 1 {
		t.Fatalf("deletion receipt=%#v, want 1/1/1", deletion)
	}
	hybrid.stop(t)

	restarted := startRetrievalServer(t, binary, databaseURL, serverRetrievalEnvironment(fixture, definition.ID, "dense"), true)
	assertContextPack(
		t, client, restarted.baseURL,
		"tenant-serving-retrieval", "user-serving-retrieval",
		serverRetrievalQuery, "", 0,
	)
	restarted.stop(t)
	assertServerRetrievalDeleted(t, databaseURL, "tenant-serving-retrieval", "user-serving-retrieval")
	assertServerRetrievalLogsRedacted(t, dense, hybrid, restarted, databaseURL, fixture.Endpoint(), serverRetrievalModel, serverRetrievalEvidence, serverRetrievalValue)
}

func TestServerDenseFailsClosedWithoutServingPinOrProvider(t *testing.T) {
	binary := requiredServerBinary(t)
	databaseURL := isolatedServerRetrievalDatabase(t)
	fixture := newServerEmbeddingFixture(t)
	storage := openServerRetrievalStore(t, databaseURL)
	defer storage.Close()
	definition := registerServerRetrievalTarget(t, storage, fixture)
	client := &http.Client{Timeout: 5 * time.Second}

	withoutServing := startRetrievalServer(
		t, binary, databaseURL,
		serverRetrievalEnvironment(fixture, definition.ID, "dense"),
		false,
	)
	assertServerPhase(t, client, withoutServing.baseURL, "postgres-dense")
	assertServerNotReady(t, client, withoutServing.baseURL)
	assertServerRetrievalUnavailable(t, client, withoutServing.baseURL, "tenant-unavailable", "user-unavailable")
	withoutServing.stop(t)
	if fixture.Requests() != 0 {
		t.Fatalf("missing serving target sent %d provider requests, want 0", fixture.Requests())
	}

	if _, err := storage.PromoteProjection(context.Background(), postgres.PromoteProjectionCommand{
		OperationID: "44444444-4444-4444-8444-444444444444",
		ToSpace:     definition.ID,
		AllowEmpty:  true,
	}); err != nil {
		t.Fatalf("promote empty serving target for failure tests: %v", err)
	}

	wrongPin := "space_v1_wrong_expected_server_retrieval"
	pinMismatch := startRetrievalServer(
		t, binary, databaseURL,
		serverRetrievalEnvironment(fixture, wrongPin, "dense"),
		false,
	)
	assertServerNotReady(t, client, pinMismatch.baseURL)
	assertServerRetrievalUnavailable(t, client, pinMismatch.baseURL, "tenant-unavailable", "user-unavailable")
	pinMismatch.stop(t)
	if fixture.Requests() != 0 {
		t.Fatalf("pin mismatch sent %d provider requests, want 0", fixture.Requests())
	}

	fixture.SetProbeMismatch(true)
	probeMismatch := startRetrievalServer(
		t, binary, databaseURL,
		serverRetrievalEnvironment(fixture, definition.ID, "hybrid"),
		false,
	)
	assertServerPhase(t, client, probeMismatch.baseURL, "postgres-hybrid-rrf")
	assertServerNotReady(t, client, probeMismatch.baseURL)
	assertServerRetrievalUnavailable(t, client, probeMismatch.baseURL, "tenant-unavailable", "user-unavailable")
	probeMismatch.stop(t)
	if fixture.Requests() == 0 {
		t.Fatal("probe mismatch did not exercise the provider")
	}

	fixture.SetProbeMismatch(false)
	fixture.Close()
	providerDown := startRetrievalServer(
		t, binary, databaseURL,
		serverRetrievalEnvironment(fixture, definition.ID, "dense"),
		false,
	)
	assertServerNotReady(t, client, providerDown.baseURL)
	assertServerRetrievalUnavailable(t, client, providerDown.baseURL, "tenant-unavailable", "user-unavailable")
	providerDown.stop(t)

	assertServerRetrievalLogsRedacted(
		t,
		withoutServing,
		pinMismatch,
		probeMismatch,
		providerDown,
		databaseURL,
		fixture.Endpoint(),
		serverRetrievalModel,
		wrongPin,
		serverRetrievalQuery,
	)
}

type serverEmbeddingFixture struct {
	server         *httptest.Server
	probeVector    []float32
	documentVector []float32
	requests       atomic.Int64
	probeMismatch  atomic.Bool
}

func newServerEmbeddingFixture(t *testing.T) *serverEmbeddingFixture {
	t.Helper()
	fixture := &serverEmbeddingFixture{
		probeVector:    make([]float32, postgres.VectorDimension),
		documentVector: make([]float32, postgres.VectorDimension),
	}
	fixture.probeVector[7] = 1
	fixture.documentVector[19] = 1
	fixture.server = httptest.NewServer(http.HandlerFunc(fixture.handle))
	t.Cleanup(fixture.Close)
	return fixture
}

func (fixture *serverEmbeddingFixture) handle(writer http.ResponseWriter, request *http.Request) {
	fixture.requests.Add(1)
	var payload struct {
		Model string   `json:"model"`
		Input []string `json:"input"`
	}
	if request.Method != http.MethodPost || json.NewDecoder(request.Body).Decode(&payload) != nil ||
		payload.Model != serverRetrievalModel || len(payload.Input) != 2 ||
		payload.Input[0] != embedding.ProbeTextV1 {
		writer.WriteHeader(http.StatusBadRequest)
		return
	}
	vectors := make([][]float32, 2)
	vectors[0] = append([]float32(nil), fixture.probeVector...)
	if fixture.probeMismatch.Load() {
		vectors[0][7] = 0
		vectors[0][8] = 1
	}
	if payload.Input[1] == embedding.ProbeTextV1 {
		vectors[1] = append([]float32(nil), vectors[0]...)
	} else {
		vectors[1] = append([]float32(nil), fixture.documentVector...)
	}
	writer.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(writer).Encode(map[string]any{
		"model": serverRetrievalModel,
		"data": []any{
			map[string]any{"index": 0, "embedding": vectors[0]},
			map[string]any{"index": 1, "embedding": vectors[1]},
		},
	})
}

func (fixture *serverEmbeddingFixture) Endpoint() string {
	return fixture.server.URL + "/v1/embeddings/" + serverRetrievalApplication
}

func (fixture *serverEmbeddingFixture) Requests() int64 {
	return fixture.requests.Load()
}

func (fixture *serverEmbeddingFixture) DocumentVector() []float32 {
	return append([]float32(nil), fixture.documentVector...)
}

func (fixture *serverEmbeddingFixture) SetProbeMismatch(value bool) {
	fixture.probeMismatch.Store(value)
}

func (fixture *serverEmbeddingFixture) Close() {
	if fixture.server != nil {
		fixture.server.Close()
	}
}

func registerServerRetrievalTarget(
	t *testing.T,
	storage *postgres.Store,
	fixture *serverEmbeddingFixture,
) postgres.EmbeddingSpaceDefinition {
	t.Helper()
	fingerprint := embedding.VectorSHA256(fixture.probeVector)
	space, err := embedding.SpaceID(
		embedding.ProviderLMStudio,
		serverRetrievalModel,
		postgres.VectorDimension,
		embedding.MemoryCardDocumentVersion,
		embedding.RawQueryVersion,
		fingerprint,
	)
	if err != nil {
		t.Fatalf("derive server retrieval space: %v", err)
	}
	createdAt := time.Now().UTC().Add(-time.Minute).Truncate(time.Microsecond)
	definition := postgres.EmbeddingSpaceDefinition{
		ID:               space,
		Provider:         embedding.ProviderLMStudio,
		Model:            serverRetrievalModel,
		Dimension:        postgres.VectorDimension,
		DocumentVersion:  embedding.MemoryCardDocumentVersion,
		QueryVersion:     embedding.RawQueryVersion,
		ModelFingerprint: fingerprint,
		CreatedAt:        createdAt,
	}
	if _, err := storage.RegisterProjectionTarget(context.Background(), postgres.RegisterProjectionTargetCommand{
		Space:      definition,
		State:      postgres.ProjectionTargetShadow,
		EnqueueNew: true,
		CreatedAt:  createdAt,
	}); err != nil {
		t.Fatalf("register server retrieval target: %v", err)
	}
	return definition
}

func projectServerRetrievalCard(t *testing.T, storage *postgres.Store, space string, vector []float32) {
	t.Helper()
	work, err := storage.ClaimProjectionJobs(context.Background(), postgres.ClaimProjectionJobsCommand{
		EmbeddingSpace: space,
		LeaseOwner:     "server-retrieval-worker",
		LeaseDuration:  time.Minute,
		MaxAttempts:    3,
		Limit:          1,
	})
	if err != nil || len(work) != 1 {
		t.Fatalf("claim server retrieval projection work count=%d error=%v", len(work), err)
	}
	if work[0].Memory.Value != serverRetrievalValue || work[0].DocumentSHA256 == "" {
		t.Fatalf("projection work item=%#v", work[0])
	}
	result, err := storage.FinalizeProjectionJob(context.Background(), postgres.FinalizeProjectionJobCommand{
		JobID:          work[0].Job.ID,
		TenantID:       work[0].Job.TenantID,
		UserID:         work[0].Job.UserID,
		EmbeddingSpace: space,
		LeaseOwner:     work[0].Job.LeaseOwner,
		LeaseVersion:   work[0].Job.LeaseVersion,
		DocumentSHA256: work[0].DocumentSHA256,
		Vector:         vector,
	})
	if err != nil || result.Job.State != postgres.ProjectionJobSucceeded || !result.EmbeddingChanged || result.RevisionAdvanced {
		t.Fatalf("finalize shadow projection result=%#v error=%v", result, err)
	}
}

func serverRetrievalEnvironment(
	fixture *serverEmbeddingFixture,
	space, mode string,
) map[string]string {
	return map[string]string{
		"SERVER_RETRIEVAL_MODE":         mode,
		"LMSTUDIO_EMBEDDINGS_URL":       fixture.Endpoint(),
		"LMSTUDIO_EMBEDDING_MODEL":      serverRetrievalModel,
		"SERVER_EXPECTED_SERVING_SPACE": space,
	}
}

func startRetrievalServer(
	t *testing.T,
	binary, databaseURL string,
	overrides map[string]string,
	waitReady bool,
) *runningServer {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("reserve retrieval server address: %v", err)
	}
	address := listener.Addr().String()
	if err := listener.Close(); err != nil {
		t.Fatalf("release retrieval server address: %v", err)
	}

	logs := &synchronizedBuffer{}
	command := exec.Command(binary, "-addr", address)
	command.Env = serverRetrievalChildEnvironment(databaseURL, overrides)
	command.Stdout = logs
	command.Stderr = logs
	if err := command.Start(); err != nil {
		t.Fatalf("start retrieval server process: %v", err)
	}
	running := &runningServer{
		baseURL: "http://" + address,
		command: command,
		done:    make(chan error, 1),
		logs:    logs,
	}
	go func() { running.done <- command.Wait() }()
	t.Cleanup(running.terminateIfNeeded)

	waitServerRetrievalEndpoint(t, running, "/healthz", http.StatusOK)
	if waitReady {
		waitServerRetrievalEndpoint(t, running, "/readyz", http.StatusOK)
	}
	return running
}

func serverRetrievalChildEnvironment(databaseURL string, overrides map[string]string) []string {
	dropped := map[string]struct{}{
		"DATABASE_URL": {}, "SERVER_RETRIEVAL_MODE": {},
		"LMSTUDIO_EMBEDDINGS_URL": {}, "LMSTUDIO_EMBEDDING_MODEL": {},
		"SERVER_EXPECTED_SERVING_SPACE": {},
	}
	result := make([]string, 0, len(os.Environ())+len(overrides)+1)
	for _, item := range os.Environ() {
		key, _, found := strings.Cut(item, "=")
		if _, remove := dropped[key]; found && remove {
			continue
		}
		result = append(result, item)
	}
	result = append(result, "DATABASE_URL="+databaseURL)
	for key, value := range overrides {
		result = append(result, key+"="+value)
	}
	return result
}

func waitServerRetrievalEndpoint(t *testing.T, running *runningServer, path string, wantStatus int) {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	client := &http.Client{Timeout: 500 * time.Millisecond}
	for time.Now().Before(deadline) {
		select {
		case waitErr := <-running.done:
			running.mu.Lock()
			running.stopped = true
			running.mu.Unlock()
			t.Fatalf("retrieval server exited before %s: %v\n%s", path, waitErr, running.logs.String())
		default:
		}
		response, requestErr := client.Get(running.baseURL + path)
		if requestErr == nil {
			_, _ = io.Copy(io.Discard, response.Body)
			_ = response.Body.Close()
			if response.StatusCode == wantStatus {
				return
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	running.terminateIfNeeded()
	t.Fatalf("retrieval server endpoint %s did not return %d\n%s", path, wantStatus, running.logs.String())
}

func assertServerNotReady(t *testing.T, client *http.Client, baseURL string) {
	t.Helper()
	var body map[string]string
	doServerJSON(t, client, http.MethodGet, baseURL+"/readyz", "", "", nil, http.StatusServiceUnavailable, &body)
	if body["status"] != "not_ready" || body["storage"] != "postgresql" || body["error"] != "" {
		t.Fatalf("not-ready response=%#v", body)
	}
}

func assertServerRetrievalUnavailable(
	t *testing.T,
	client *http.Client,
	baseURL, tenantID, userID string,
) {
	t.Helper()
	var body struct {
		Error struct {
			Code    string `json:"code"`
			Message string `json:"message"`
		} `json:"error"`
	}
	doServerJSON(
		t,
		client,
		http.MethodPost,
		baseURL+"/v1/context-packs",
		tenantID,
		userID,
		map[string]any{"query": serverRetrievalQuery, "limit": 5},
		http.StatusServiceUnavailable,
		&body,
	)
	if body.Error.Code != "retrieval_unavailable" || body.Error.Message != "retrieval is temporarily unavailable" {
		t.Fatalf("retrieval unavailable response=%#v", body)
	}
}

func assertServerRetrievalDeleted(t *testing.T, databaseURL, tenantID, userID string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	connection, err := pgx.Connect(ctx, databaseURL)
	if err != nil {
		t.Fatalf("connect to inspect serving retrieval deletion: %v", err)
	}
	defer connection.Close(context.Background())
	var evidence, candidates, cards, jobs, vectors int
	if err := connection.QueryRow(ctx, `
		SELECT
		 (SELECT count(*) FROM agent_memory.evidence_events WHERE tenant_id=$1 AND user_id=$2),
		 (SELECT count(*) FROM agent_memory.memory_candidates WHERE tenant_id=$1 AND user_id=$2),
		 (SELECT count(*) FROM agent_memory.memory_cards WHERE tenant_id=$1 AND user_id=$2),
		 (SELECT count(*) FROM agent_memory.embedding_projection_jobs WHERE tenant_id=$1 AND user_id=$2),
		 (SELECT count(*) FROM agent_memory.memory_embeddings WHERE tenant_id=$1 AND user_id=$2)`,
		tenantID, userID,
	).Scan(&evidence, &candidates, &cards, &jobs, &vectors); err != nil {
		t.Fatalf("inspect serving retrieval deletion: %v", err)
	}
	if evidence != 0 || candidates != 0 || cards != 0 || jobs != 0 || vectors != 0 {
		t.Fatalf("deleted scope rows evidence/candidates/cards/jobs/vectors=%d/%d/%d/%d/%d", evidence, candidates, cards, jobs, vectors)
	}
}

func assertServerRetrievalLogsRedacted(t *testing.T, values ...any) {
	t.Helper()
	var processes []*runningServer
	var forbidden []string
	for _, value := range values {
		switch typed := value.(type) {
		case *runningServer:
			processes = append(processes, typed)
		case string:
			forbidden = append(forbidden, typed)
		}
	}
	for _, process := range processes {
		output := process.logs.String()
		for _, secret := range forbidden {
			if secret != "" && strings.Contains(output, secret) {
				t.Fatalf("server logs leaked forbidden value %q: %s", secret, output)
			}
		}
	}
}

func requiredServerBinary(t *testing.T) string {
	t.Helper()
	binary := strings.TrimSpace(os.Getenv("TEST_SERVER_BINARY"))
	if binary == "" {
		t.Fatal("TEST_SERVER_BINARY is required")
	}
	if _, err := os.Stat(binary); err != nil {
		t.Fatal("TEST_SERVER_BINARY is not readable")
	}
	return binary
}

func openServerRetrievalStore(t *testing.T, databaseURL string) *postgres.Store {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	storage, err := postgres.Open(ctx, databaseURL)
	if err != nil {
		t.Fatalf("open isolated server retrieval store: %v", err)
	}
	return storage
}

func isolatedServerRetrievalDatabase(t *testing.T) string {
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
		"agent_memory_server_retrieval_%d_%d",
		time.Now().UnixNano(),
		serverRetrievalDatabaseSequence.Add(1),
	)
	adminConfig := baseConfig.Copy()
	adminConfig.Database = "postgres"
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	admin, err := pgx.ConnectConfig(ctx, adminConfig)
	if err != nil {
		t.Fatal("connect server retrieval maintenance database")
	}
	quotedDatabase := pgx.Identifier{databaseName}.Sanitize()
	if _, err := admin.Exec(ctx, "CREATE DATABASE "+quotedDatabase); err != nil {
		_ = admin.Close(context.Background())
		t.Fatal("create isolated server retrieval database")
	}
	if err := admin.Close(context.Background()); err != nil {
		t.Fatal("close server retrieval maintenance database")
	}

	parsedURL.Path = "/" + databaseName
	parsedURL.RawPath = ""
	parameters := parsedURL.Query()
	parameters.Set("application_name", serverRetrievalApplication)
	parsedURL.RawQuery = parameters.Encode()
	databaseURL := parsedURL.String()
	if err := migrations.Apply(ctx, databaseURL); err != nil {
		t.Fatal("apply isolated server retrieval migrations")
	}
	t.Cleanup(func() {
		dropCtx, dropCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer dropCancel()
		connection, connectErr := pgx.ConnectConfig(dropCtx, adminConfig)
		if connectErr != nil {
			t.Error("reconnect to drop isolated server retrieval database")
			return
		}
		defer connection.Close(context.Background())
		if _, dropErr := connection.Exec(dropCtx, "DROP DATABASE IF EXISTS "+quotedDatabase+" WITH (FORCE)"); dropErr != nil {
			t.Error("drop isolated server retrieval database")
		}
	})
	return databaseURL
}
