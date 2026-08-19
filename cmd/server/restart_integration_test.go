//go:build integration

package main_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/kai443/go-agent-memory-system/internal/domain"
	"github.com/kai443/go-agent-memory-system/internal/migrations"
)

func TestServerProcessRestartRecoveryAndDeletePropagation(t *testing.T) {
	databaseURL := requiredServerTestDatabaseURL(t)
	serverBinary := os.Getenv("TEST_SERVER_BINARY")
	if serverBinary == "" {
		t.Fatal("TEST_SERVER_BINARY is required for process restart integration tests")
	}
	if _, err := os.Stat(serverBinary); err != nil {
		t.Fatalf("stat TEST_SERVER_BINARY %q: %v", serverBinary, err)
	}
	applyServerTestMigrations(t, databaseURL)

	suffix := fmt.Sprintf("%d", time.Now().UnixNano())
	tenantID, userID := "tenant_restart_"+suffix, "user_restart_"+suffix
	controlTenantID, controlUserID := "tenant_control_"+suffix, "user_control_"+suffix
	cleanupServerTestScopes(t, databaseURL, [][2]string{{tenantID, userID}, {controlTenantID, controlUserID}})

	client := &http.Client{Timeout: 5 * time.Second}
	first := startServer(t, serverBinary, databaseURL)
	assertServerPhase(t, client, first.baseURL, "postgres-fts")
	targetCandidate := ingestProposeApprove(t, client, first.baseURL, tenantID, userID, "event-restart", "prefers a window seat", "seat_preference", "window")
	if targetCandidate == "" {
		t.Fatal("target candidate id is empty")
	}
	controlCandidate := ingestProposeApprove(t, client, first.baseURL, controlTenantID, controlUserID, "event-control", "drinks green tea", "drink_preference", "green tea")
	if controlCandidate == "" {
		t.Fatal("control candidate id is empty")
	}
	assertContextPack(t, client, first.baseURL, tenantID, userID, "window", "window", 1)
	first.stop(t)

	second := startServer(t, serverBinary, databaseURL)
	assertContextPack(t, client, second.baseURL, tenantID, userID, "window", "window", 1)
	var receipt domain.DeletionReceipt
	doServerJSON(t, client, http.MethodDelete, second.baseURL+"/v1/users/"+url.PathEscape(userID), tenantID, "", nil, http.StatusOK, &receipt)
	if receipt.EvidenceDeleted != 1 || receipt.CandidatesDeleted != 1 || receipt.MemoriesDeleted != 1 {
		t.Fatalf("deletion receipt=%#v, want evidence/candidates/memories 1/1/1", receipt)
	}
	assertContextPack(t, client, second.baseURL, tenantID, userID, "window", "", 0)
	second.stop(t)

	third := startServer(t, serverBinary, databaseURL)
	assertContextPack(t, client, third.baseURL, tenantID, userID, "window", "", 0)
	assertContextPack(t, client, third.baseURL, controlTenantID, controlUserID, "tea", "green tea", 1)
	assertServerTestDeletedRows(t, databaseURL, tenantID, userID, 2)
	third.stop(t)
}

type synchronizedBuffer struct {
	mu     sync.Mutex
	buffer bytes.Buffer
}

func (buffer *synchronizedBuffer) Write(data []byte) (int, error) {
	buffer.mu.Lock()
	defer buffer.mu.Unlock()
	return buffer.buffer.Write(data)
}

func (buffer *synchronizedBuffer) String() string {
	buffer.mu.Lock()
	defer buffer.mu.Unlock()
	return buffer.buffer.String()
}

type runningServer struct {
	baseURL string
	command *exec.Cmd
	done    chan error
	logs    *synchronizedBuffer
	mu      sync.Mutex
	stopped bool
}

func startServer(t *testing.T, binary, databaseURL string) *runningServer {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("reserve server address: %v", err)
	}
	address := listener.Addr().String()
	if err := listener.Close(); err != nil {
		t.Fatalf("release server address: %v", err)
	}

	logs := &synchronizedBuffer{}
	command := exec.Command(binary, "-addr", address)
	command.Env = withEnvironmentVariable(os.Environ(), "DATABASE_URL", databaseURL)
	command.Stdout = logs
	command.Stderr = logs
	if err := command.Start(); err != nil {
		t.Fatalf("start server process: %v", err)
	}
	running := &runningServer{
		baseURL: "http://" + address,
		command: command,
		done:    make(chan error, 1),
		logs:    logs,
	}
	go func() {
		running.done <- command.Wait()
	}()
	t.Cleanup(func() {
		running.terminateIfNeeded()
	})

	deadline := time.Now().Add(15 * time.Second)
	client := &http.Client{Timeout: 500 * time.Millisecond}
	for time.Now().Before(deadline) {
		select {
		case waitErr := <-running.done:
			running.mu.Lock()
			running.stopped = true
			running.mu.Unlock()
			t.Fatalf("server exited before readiness: %v\n%s", waitErr, logs.String())
		default:
		}
		response, requestErr := client.Get(running.baseURL + "/readyz")
		if requestErr == nil {
			_, _ = io.Copy(io.Discard, response.Body)
			_ = response.Body.Close()
			if response.StatusCode == http.StatusOK {
				return running
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	running.terminateIfNeeded()
	t.Fatalf("server did not become ready at %s\n%s", running.baseURL, logs.String())
	return nil
}

func withEnvironmentVariable(environ []string, key, value string) []string {
	result := append([]string(nil), environ...)
	prefix := key + "="
	for index, current := range result {
		if strings.HasPrefix(current, prefix) {
			result[index] = prefix + value
			return result
		}
	}
	return append(result, prefix+value)
}

func (running *runningServer) stop(t *testing.T) {
	t.Helper()
	running.mu.Lock()
	if running.stopped {
		running.mu.Unlock()
		return
	}
	if err := running.command.Process.Signal(syscall.SIGTERM); err != nil {
		running.mu.Unlock()
		t.Fatalf("signal server process: %v\n%s", err, running.logs.String())
	}
	running.mu.Unlock()

	select {
	case err := <-running.done:
		running.mu.Lock()
		running.stopped = true
		running.mu.Unlock()
		if err != nil {
			t.Fatalf("server process did not stop cleanly: %v\n%s", err, running.logs.String())
		}
	case <-time.After(10 * time.Second):
		running.terminateIfNeeded()
		t.Fatalf("server process did not stop after SIGTERM\n%s", running.logs.String())
	}
}

func (running *runningServer) terminateIfNeeded() {
	running.mu.Lock()
	if running.stopped {
		running.mu.Unlock()
		return
	}
	running.stopped = true
	process := running.command.Process
	running.mu.Unlock()
	if process != nil {
		_ = process.Kill()
	}
	select {
	case <-running.done:
	case <-time.After(2 * time.Second):
	}
}

func ingestProposeApprove(t *testing.T, client *http.Client, baseURL, tenantID, userID, eventID, content, key, value string) string {
	t.Helper()
	var event domain.EvidenceEvent
	doServerJSON(t, client, http.MethodPost, baseURL+"/v1/evidence", tenantID, userID, map[string]any{
		"event_id": eventID, "session_id": "session-restart", "actor": "user", "content": content,
	}, http.StatusCreated, &event)
	if event.ID != eventID {
		t.Fatalf("created event id=%q, want %q", event.ID, eventID)
	}

	var candidate domain.MemoryCandidate
	doServerJSON(t, client, http.MethodPost, baseURL+"/v1/memory-candidates", tenantID, userID, map[string]any{
		"kind": "semantic", "category": "preference", "key": key, "value": value,
		"person": "self", "relationship": "self", "source_event_ids": []string{eventID},
		"extractor": "restart-integration", "extractor_version": "v1",
	}, http.StatusCreated, &candidate)

	var reviewed struct {
		Candidate domain.MemoryCandidate `json:"candidate"`
		Memory    *domain.MemoryCard     `json:"memory"`
	}
	doServerJSON(t, client, http.MethodPost, baseURL+"/v1/memory-candidates/"+url.PathEscape(candidate.ID)+"/reviews", tenantID, userID, map[string]any{
		"decision": "approve", "reviewer_id": "restart-reviewer", "reason": "supported by source evidence",
	}, http.StatusOK, &reviewed)
	if reviewed.Candidate.Status != domain.CandidateApproved || reviewed.Memory == nil || reviewed.Memory.Status != domain.MemoryActive {
		t.Fatalf("review response=%#v", reviewed)
	}
	return candidate.ID
}

func assertContextPack(t *testing.T, client *http.Client, baseURL, tenantID, userID, query, wantValue string, wantItems int) {
	t.Helper()
	var pack domain.ContextPack
	doServerJSON(t, client, http.MethodPost, baseURL+"/v1/context-packs", tenantID, userID, map[string]any{
		"query": query, "limit": 5,
	}, http.StatusOK, &pack)
	if len(pack.Items) != wantItems {
		t.Fatalf("context pack items=%d, want %d: %#v", len(pack.Items), wantItems, pack)
	}
	if wantItems > 0 {
		if pack.Items[0].Memory.Value != wantValue {
			t.Fatalf("context memory value=%q, want %q", pack.Items[0].Memory.Value, wantValue)
		}
		if len(pack.Items[0].Sources) != 1 || pack.Items[0].Sources[0].TenantID != tenantID || pack.Items[0].Sources[0].UserID != userID {
			t.Fatalf("context sources=%#v", pack.Items[0].Sources)
		}
	}
}

func assertServerPhase(t *testing.T, client *http.Client, baseURL, wantPhase string) {
	t.Helper()
	var health struct {
		Status  string `json:"status"`
		Phase   string `json:"phase"`
		Storage string `json:"storage"`
	}
	doServerJSON(t, client, http.MethodGet, baseURL+"/healthz", "", "", nil, http.StatusOK, &health)
	if health.Status != "ok" || health.Phase != wantPhase || health.Storage != "postgresql" {
		t.Fatalf("health response=%#v, want phase %q with PostgreSQL", health, wantPhase)
	}
}

func doServerJSON(t *testing.T, client *http.Client, method, endpoint, tenantID, userID string, payload any, wantStatus int, destination any) {
	t.Helper()
	var body io.Reader
	if payload != nil {
		encoded, err := json.Marshal(payload)
		if err != nil {
			t.Fatalf("encode request: %v", err)
		}
		body = bytes.NewReader(encoded)
	}
	request, err := http.NewRequest(method, endpoint, body)
	if err != nil {
		t.Fatalf("create request: %v", err)
	}
	request.Header.Set("X-Tenant-ID", tenantID)
	if userID != "" {
		request.Header.Set("X-User-ID", userID)
	}
	if payload != nil {
		request.Header.Set("Content-Type", "application/json")
	}
	response, err := client.Do(request)
	if err != nil {
		t.Fatalf("%s %s: %v", method, endpoint, err)
	}
	defer response.Body.Close()
	responseBody, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatalf("read %s %s response: %v", method, endpoint, err)
	}
	if response.StatusCode != wantStatus {
		t.Fatalf("%s %s status=%d, want %d; body=%s", method, endpoint, response.StatusCode, wantStatus, strings.TrimSpace(string(responseBody)))
	}
	if destination != nil {
		if err := json.Unmarshal(responseBody, destination); err != nil {
			t.Fatalf("decode %s %s response: %v; body=%s", method, endpoint, err, strings.TrimSpace(string(responseBody)))
		}
	}
}

func requiredServerTestDatabaseURL(t *testing.T) string {
	t.Helper()
	databaseURL := os.Getenv("TEST_DATABASE_URL")
	if databaseURL == "" {
		t.Fatal("TEST_DATABASE_URL is required for integration tests")
	}
	return databaseURL
}

func applyServerTestMigrations(t *testing.T, databaseURL string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := migrations.Apply(ctx, databaseURL); err != nil {
		t.Fatalf("apply migrations: %v", err)
	}
}

func cleanupServerTestScopes(t *testing.T, databaseURL string, scopes [][2]string) {
	t.Helper()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		conn, err := pgx.Connect(ctx, databaseURL)
		if err != nil {
			t.Errorf("connect for cleanup: %v", err)
			return
		}
		defer conn.Close(context.Background())
		for _, scope := range scopes {
			for _, table := range []string{"memory_cards", "memory_candidates", "evidence_events", "memory_identity_chains", "user_scope_state"} {
				query := fmt.Sprintf("DELETE FROM agent_memory.%s WHERE tenant_id=$1 AND user_id=$2", table)
				if _, err := conn.Exec(ctx, query, scope[0], scope[1]); err != nil {
					t.Errorf("clean %s for scope %s/%s: %v", table, scope[0], scope[1], err)
					break
				}
			}
		}
	})
}

func assertServerTestDeletedRows(t *testing.T, databaseURL, tenantID, userID string, wantRevision int64) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	conn, err := pgx.Connect(ctx, databaseURL)
	if err != nil {
		t.Fatalf("connect to inspect process-test deletion: %v", err)
	}
	defer conn.Close(context.Background())
	for _, table := range []string{"evidence_events", "memory_candidates", "candidate_source_events", "memory_cards", "memory_identity_chains"} {
		var count int
		query := fmt.Sprintf("SELECT count(*) FROM agent_memory.%s WHERE tenant_id=$1 AND user_id=$2", table)
		if err := conn.QueryRow(ctx, query, tenantID, userID).Scan(&count); err != nil {
			t.Fatalf("count %s: %v", table, err)
		}
		if count != 0 {
			t.Errorf("%s rows after process-restart deletion=%d, want 0", table, count)
		}
	}
	var stateRows int
	var revision int64
	if err := conn.QueryRow(ctx, `
		SELECT count(*), coalesce(max(context_revision), 0)
		FROM agent_memory.user_scope_state WHERE tenant_id=$1 AND user_id=$2
	`, tenantID, userID).Scan(&stateRows, &revision); err != nil {
		t.Fatalf("query retained scope state: %v", err)
	}
	if stateRows != 1 || revision != wantRevision {
		t.Fatalf("retained scope state rows/revision=%d/%d, want 1/%d", stateRows, revision, wantRevision)
	}
}
