package projectionworker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/kai443/go-agent-memory-system/internal/domain"
	"github.com/kai443/go-agent-memory-system/internal/embedding"
	"github.com/kai443/go-agent-memory-system/internal/store/postgres"
)

var workerTestNow = time.Date(2026, 8, 19, 12, 0, 0, 0, time.UTC)
var workerTestProbeVector = []float32{0.25, -0.5, 1}

func TestRunOnceEmbedsVersionedDocumentAndFinalizesAtomically(t *testing.T) {
	harness := newWorkerHarness(t, workerHarnessOptions{})
	wantVector := []float32{1, 0.5, -0.25}
	harness.embedder.embed = func(context.Context, []string) ([][]float32, error) {
		return [][]float32{workerTestProbeVector, wantVector}, nil
	}

	result, err := harness.worker.RunOnce(context.Background())
	if err != nil {
		t.Fatalf("RunOnce: %v", err)
	}
	if result != (BatchResult{Claimed: 1, Succeeded: 1}) {
		t.Fatalf("result=%#v", result)
	}
	if calls := harness.embedder.callCount(); calls != 1 {
		t.Fatalf("embed calls=%d, want 1", calls)
	}
	wantDocument := embedding.MemoryCardDocumentV1(harness.item.Memory)
	inputs := harness.embedder.lastInputs()
	if len(inputs) != 2 || inputs[0] != embedding.ProbeTextV1 || inputs[1] != wantDocument {
		t.Fatalf("embed batch does not equal probe plus memory-card-document-v1")
	}

	harness.repository.mu.Lock()
	defer harness.repository.mu.Unlock()
	if len(harness.repository.claimCommands) != 1 ||
		harness.repository.claimCommands[0].MaxAttempts != harness.config.MaxAttempts {
		t.Fatalf("claim commands=%#v, want configured max attempts", harness.repository.claimCommands)
	}
	if len(harness.repository.finalizeCommands) != 1 {
		t.Fatalf("finalize calls=%d, want 1", len(harness.repository.finalizeCommands))
	}
	command := harness.repository.finalizeCommands[0]
	if command.JobID != harness.item.Job.ID ||
		command.TenantID != harness.item.Job.TenantID ||
		command.UserID != harness.item.Job.UserID ||
		command.EmbeddingSpace != harness.item.Job.EmbeddingSpace ||
		command.LeaseOwner != harness.item.Job.LeaseOwner ||
		command.LeaseVersion != harness.item.Job.LeaseVersion ||
		command.DocumentSHA256 != embedding.DocumentSHA256(wantDocument) ||
		!equalFloat32(command.Vector, wantVector) {
		t.Fatalf("finalize command has unexpected fenced or projection fields: %#v", command)
	}
	if len(harness.repository.retryCommands) != 0 ||
		len(harness.repository.deadCommands) != 0 {
		t.Fatal("successful item used a failure transition")
	}
}

func TestRunOnceWithEmbeddingClientUsesOneProbeAndDocumentRequest(t *testing.T) {
	harness := newWorkerHarness(t, workerHarnessOptions{})
	var requestMu sync.Mutex
	requestCount := 0
	var requestInputs []string
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		var body struct {
			Model string   `json:"model"`
			Input []string `json:"input"`
		}
		if err := json.NewDecoder(request.Body).Decode(&body); err != nil {
			http.Error(response, "invalid request", http.StatusBadRequest)
			return
		}
		requestMu.Lock()
		requestCount++
		requestInputs = append([]string(nil), body.Input...)
		requestMu.Unlock()
		response.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(response).Encode(map[string]any{
			"model": body.Model,
			"data": []map[string]any{
				{"index": 0, "embedding": workerTestProbeVector},
				{"index": 1, "embedding": []float32{1, 2, 3}},
			},
		})
	}))
	defer server.Close()

	client, err := embedding.NewClient(embedding.Config{
		Endpoint:          server.URL,
		Model:             "test-embedding-model",
		ExpectedDimension: 3,
		MaxBatchSize:      EmbeddingBatchSizeV1,
	})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	worker, err := New(harness.repository, client, harness.config)
	if err != nil {
		t.Fatalf("New worker with embedding client: %v", err)
	}
	result, err := worker.RunOnce(context.Background())
	if err != nil {
		t.Fatalf("RunOnce: %v", err)
	}
	if result != (BatchResult{Claimed: 1, Succeeded: 1}) {
		t.Fatalf("result=%#v", result)
	}

	requestMu.Lock()
	defer requestMu.Unlock()
	wantDocument := embedding.MemoryCardDocumentV1(harness.item.Memory)
	if requestCount != 1 ||
		len(requestInputs) != EmbeddingBatchSizeV1 ||
		requestInputs[0] != embedding.ProbeTextV1 ||
		requestInputs[1] != wantDocument {
		t.Fatalf("requests=%d inputs=%#v", requestCount, requestInputs)
	}
}

func TestRunOnceClassifiesFailuresWithoutImplicitHTTPRetry(t *testing.T) {
	secret := "http://user:password@example.invalid/v1/embeddings raw response memory text"
	tests := []struct {
		name        string
		embed       func(context.Context, []string) ([][]float32, error)
		attempt     int
		maxAttempts int
		wantRetry   bool
		wantCode    postgres.ProjectionErrorCode
	}{
		{
			name:      "transport retries durably",
			embed:     embedFailure(fmt.Errorf("%w: %s", embedding.ErrRequestFailed, secret)),
			attempt:   1,
			wantRetry: true,
			wantCode:  postgres.ProjectionErrorTransport,
		},
		{
			name:      "provider timeout retries durably",
			embed:     embedFailure(errors.Join(embedding.ErrRequestFailed, context.DeadlineExceeded, errors.New(secret))),
			attempt:   1,
			wantRetry: true,
			wantCode:  postgres.ProjectionErrorProviderTimeout,
		},
		{
			name:      "HTTP request timeout retries durably",
			embed:     embedFailure(&embedding.HTTPStatusError{StatusCode: 408}),
			attempt:   1,
			wantRetry: true,
			wantCode:  postgres.ProjectionErrorProviderTimeout,
		},
		{
			name:      "rate limit retries durably",
			embed:     embedFailure(&embedding.HTTPStatusError{StatusCode: 429}),
			attempt:   1,
			wantRetry: true,
			wantCode:  postgres.ProjectionErrorProviderRateLimit,
		},
		{
			name:      "provider unavailable retries durably",
			embed:     embedFailure(&embedding.HTTPStatusError{StatusCode: 503}),
			attempt:   1,
			wantRetry: true,
			wantCode:  postgres.ProjectionErrorProviderUnavailable,
		},
		{
			name:     "provider rejection is terminal",
			embed:    embedFailure(&embedding.HTTPStatusError{StatusCode: 400}),
			attempt:  1,
			wantCode: postgres.ProjectionErrorProviderRejected,
		},
		{
			name:     "invalid response is terminal",
			embed:    embedFailure(fmt.Errorf("%w: %s", embedding.ErrInvalidResponse, secret)),
			attempt:  1,
			wantCode: postgres.ProjectionErrorInvalidResponse,
		},
		{
			name:     "model mismatch is terminal",
			embed:    embedFailure(errors.Join(embedding.ErrInvalidResponse, embedding.ErrModelMismatch, errors.New(secret))),
			attempt:  1,
			wantCode: postgres.ProjectionErrorModelMismatch,
		},
		{
			name: "live probe fingerprint drift is terminal",
			embed: func(context.Context, []string) ([][]float32, error) {
				return [][]float32{{0.5, 0.5, 0.5}, {1, 2, 3}}, nil
			},
			attempt:  1,
			wantCode: postgres.ProjectionErrorModelMismatch,
		},
		{
			name:     "typed dimension mismatch is terminal",
			embed:    embedFailure(errors.Join(embedding.ErrInvalidResponse, embedding.ErrDimensionMismatch, errors.New(secret))),
			attempt:  1,
			wantCode: postgres.ProjectionErrorDimensionMismatch,
		},
		{
			name: "worker dimension validation is terminal",
			embed: func(context.Context, []string) ([][]float32, error) {
				return [][]float32{workerTestProbeVector, {1, 2}}, nil
			},
			attempt:  1,
			wantCode: postgres.ProjectionErrorDimensionMismatch,
		},
		{
			name: "non finite vector is terminal",
			embed: func(context.Context, []string) ([][]float32, error) {
				return [][]float32{workerTestProbeVector, {1, float32(math.NaN()), 2}}, nil
			},
			attempt:  1,
			wantCode: postgres.ProjectionErrorNonFiniteVector,
		},
		{
			name:        "retry budget is terminal",
			embed:       embedFailure(errors.New(secret)),
			attempt:     3,
			maxAttempts: 3,
			wantCode:    postgres.ProjectionErrorAttemptsExhausted,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			harness := newWorkerHarness(t, workerHarnessOptions{
				attemptCount: test.attempt,
				maxAttempts:  test.maxAttempts,
			})
			harness.embedder.embed = test.embed

			result, err := harness.worker.RunOnce(context.Background())
			if err != nil {
				if strings.Contains(err.Error(), secret) {
					t.Fatal("RunOnce leaked provider data")
				}
				t.Fatalf("RunOnce: %v", err)
			}
			if calls := harness.embedder.callCount(); calls != 1 {
				t.Fatalf("embed calls=%d, want exactly 1", calls)
			}

			harness.repository.mu.Lock()
			defer harness.repository.mu.Unlock()
			if test.wantRetry {
				if result != (BatchResult{Claimed: 1, Retried: 1}) {
					t.Fatalf("result=%#v", result)
				}
				if len(harness.repository.retryCommands) != 1 || len(harness.repository.deadCommands) != 0 {
					t.Fatalf("retry/dead calls=%d/%d", len(harness.repository.retryCommands), len(harness.repository.deadCommands))
				}
				command := harness.repository.retryCommands[0]
				if command.ErrorCode != test.wantCode ||
					command.RetryAfter != 7*time.Second {
					t.Fatalf("retry command=%#v", command)
				}
			} else {
				if result != (BatchResult{Claimed: 1, DeadLettered: 1}) {
					t.Fatalf("result=%#v", result)
				}
				if len(harness.repository.deadCommands) != 1 || len(harness.repository.retryCommands) != 0 {
					t.Fatalf("dead/retry calls=%d/%d", len(harness.repository.deadCommands), len(harness.repository.retryCommands))
				}
				if command := harness.repository.deadCommands[0]; command.ErrorCode != test.wantCode {
					t.Fatalf("dead-letter command=%#v", command)
				}
			}
		})
	}
}

func TestRunOnceHonorsAtomicFinalizeDisposition(t *testing.T) {
	tests := []struct {
		name       string
		finalized  postgres.FinalizeProjectionJobResult
		wantResult BatchResult
		wantError  error
	}{
		{
			name:       "lifecycle cancelled",
			finalized:  postgres.FinalizeProjectionJobResult{Cancelled: true},
			wantResult: BatchResult{Claimed: 1, Cancelled: 1},
		},
		{
			name:       "blocked target requeued",
			finalized:  postgres.FinalizeProjectionJobResult{Requeued: true},
			wantResult: BatchResult{Claimed: 1, Retried: 1},
		},
		{
			name:       "conflicting disposition",
			finalized:  postgres.FinalizeProjectionJobResult{Cancelled: true, Requeued: true},
			wantResult: BatchResult{Claimed: 1},
			wantError:  ErrRepositoryInvariant,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			harness := newWorkerHarness(t, workerHarnessOptions{})
			harness.repository.finalizeResult = test.finalized
			result, err := harness.worker.RunOnce(context.Background())
			if !errors.Is(err, test.wantError) {
				t.Fatalf("error=%v, want %v", err, test.wantError)
			}
			if result != test.wantResult {
				t.Fatalf("result=%#v, want %#v", result, test.wantResult)
			}
			if calls := harness.embedder.callCount(); calls != 1 {
				t.Fatalf("embed calls=%d, want 1", calls)
			}
		})
	}
}

func TestRunOnceLeavesLeaseAndExpiryAuthorityToRepositoryClock(t *testing.T) {
	harness := newWorkerHarness(t, workerHarnessOptions{})
	oldTimestamp := workerTestNow.Add(-time.Hour)
	harness.repository.items[0].Job.LeaseUntil = &oldTimestamp
	harness.repository.items[0].Memory.ExpiresAt = &oldTimestamp
	harness.repository.items[0].Document = embedding.MemoryCardDocumentV1(harness.repository.items[0].Memory)
	harness.repository.items[0].DocumentSHA256 = embedding.DocumentSHA256(harness.repository.items[0].Document)
	harness.repository.finalizeResult = postgres.FinalizeProjectionJobResult{Cancelled: true}

	result, err := harness.worker.RunOnce(context.Background())
	if err != nil {
		t.Fatalf("RunOnce: %v", err)
	}
	if result != (BatchResult{Claimed: 1, Cancelled: 1}) {
		t.Fatalf("result=%#v", result)
	}
	if calls := harness.embedder.callCount(); calls != 1 {
		t.Fatalf("embed calls=%d, want repository to make the authoritative decision after one call", calls)
	}
}

func TestRunOnceBoundsProviderIOByMonotonicLeaseBudget(t *testing.T) {
	t.Run("claim consumed budget", func(t *testing.T) {
		monotonicNow := workerTestNow
		harness := newWorkerHarness(t, workerHarnessOptions{
			monotonicNow: func() time.Time { return monotonicNow },
		})
		harness.repository.claimHook = func() {
			monotonicNow = monotonicNow.Add(harness.config.LeaseDuration)
		}

		result, err := harness.worker.RunOnce(context.Background())
		if err != nil {
			t.Fatalf("RunOnce: %v", err)
		}
		if result != (BatchResult{Claimed: 1, LeaseLost: 1}) {
			t.Fatalf("result=%#v", result)
		}
		if calls := harness.embedder.callCount(); calls != 0 {
			t.Fatalf("embed calls=%d, want 0", calls)
		}
		harness.repository.assertNoMutations(t)
	})

	t.Run("provider returned after budget", func(t *testing.T) {
		monotonicNow := workerTestNow
		harness := newWorkerHarness(t, workerHarnessOptions{
			monotonicNow: func() time.Time { return monotonicNow },
		})
		harness.embedder.embed = func(context.Context, []string) ([][]float32, error) {
			monotonicNow = monotonicNow.Add(harness.config.LeaseDuration)
			return [][]float32{workerTestProbeVector, {1, 2, 3}}, nil
		}

		result, err := harness.worker.RunOnce(context.Background())
		if err != nil {
			t.Fatalf("RunOnce: %v", err)
		}
		if result != (BatchResult{Claimed: 1, LeaseLost: 1}) {
			t.Fatalf("result=%#v", result)
		}
		if calls := harness.embedder.callCount(); calls != 1 {
			t.Fatalf("embed calls=%d, want 1", calls)
		}
		harness.repository.assertNoMutations(t)
	})

	t.Run("provider context is capped", func(t *testing.T) {
		harness := newWorkerHarness(t, workerHarnessOptions{leaseDuration: 10 * time.Millisecond})
		harness.embedder.embed = func(ctx context.Context, _ []string) ([][]float32, error) {
			<-ctx.Done()
			return nil, ctx.Err()
		}

		result, err := harness.worker.RunOnce(context.Background())
		if err != nil {
			t.Fatalf("RunOnce: %v", err)
		}
		if result != (BatchResult{Claimed: 1, LeaseLost: 1}) {
			t.Fatalf("result=%#v", result)
		}
		if calls := harness.embedder.callCount(); calls != 1 {
			t.Fatalf("embed calls=%d, want 1", calls)
		}
		harness.repository.assertNoMutations(t)
	})
}

func TestRunOnceRejectsClaimDocumentDriftWithoutSendingContentOrMutating(t *testing.T) {
	harness := newWorkerHarness(t, workerHarnessOptions{})
	harness.repository.items[0].Document += " drifted sensitive content"
	harness.item = harness.repository.items[0]

	result, err := harness.worker.RunOnce(context.Background())
	if !errors.Is(err, ErrRepositoryInvariant) {
		t.Fatalf("RunOnce error=%v, want repository invariant", err)
	}
	if result != (BatchResult{Claimed: 1}) {
		t.Fatalf("result=%#v", result)
	}
	if calls := harness.embedder.callCount(); calls != 0 {
		t.Fatalf("embed calls=%d, want 0", calls)
	}
	harness.repository.assertNoMutations(t)
}

func TestRunOnceTreatsFencingAndConcurrentSupersedeAsTerminalForOldToken(t *testing.T) {
	t.Run("finalize fence after supersede", func(t *testing.T) {
		harness := newWorkerHarness(t, workerHarnessOptions{})
		harness.embedder.embed = func(context.Context, []string) ([][]float32, error) {
			harness.repository.mu.Lock()
			harness.repository.finalizeErr = postgres.ErrProjectionLeaseLost
			harness.repository.mu.Unlock()
			return [][]float32{workerTestProbeVector, {1, 2, 3}}, nil
		}

		result, err := harness.worker.RunOnce(context.Background())
		if err != nil {
			t.Fatalf("RunOnce: %v", err)
		}
		if result != (BatchResult{Claimed: 1, LeaseLost: 1}) {
			t.Fatalf("result=%#v", result)
		}
		if calls := harness.embedder.callCount(); calls != 1 {
			t.Fatalf("embed calls=%d, want 1", calls)
		}
		harness.repository.assertNoFailureTransitions(t)
	})

	t.Run("retry fence", func(t *testing.T) {
		harness := newWorkerHarness(t, workerHarnessOptions{})
		harness.embedder.embed = embedFailure(embedding.ErrRequestFailed)
		harness.repository.retryErr = postgres.ErrProjectionLeaseLost

		result, err := harness.worker.RunOnce(context.Background())
		if err != nil {
			t.Fatalf("RunOnce: %v", err)
		}
		if result != (BatchResult{Claimed: 1, LeaseLost: 1}) {
			t.Fatalf("result=%#v", result)
		}
		if calls := harness.embedder.callCount(); calls != 1 {
			t.Fatalf("embed calls=%d, want 1", calls)
		}
	})
}

func TestRunOnceRejectsImpossibleSupersededSnapshotWithoutMutation(t *testing.T) {
	harness := newWorkerHarness(t, workerHarnessOptions{})
	harness.repository.items[0].Memory.Status = domain.MemorySuperseded
	harness.item = harness.repository.items[0]

	result, err := harness.worker.RunOnce(context.Background())
	if !errors.Is(err, ErrRepositoryInvariant) {
		t.Fatalf("RunOnce error=%v, want repository invariant", err)
	}
	if result != (BatchResult{Claimed: 1}) {
		t.Fatalf("result=%#v", result)
	}
	if calls := harness.embedder.callCount(); calls != 0 {
		t.Fatalf("embed calls=%d, want 0", calls)
	}
	harness.repository.assertNoMutations(t)
}

func TestRunCancelsInFlightEmbedAndLeavesLeaseForRecovery(t *testing.T) {
	harness := newWorkerHarness(t, workerHarnessOptions{})
	started := make(chan struct{})
	harness.embedder.embed = func(ctx context.Context, _ []string) ([][]float32, error) {
		close(started)
		<-ctx.Done()
		return nil, fmt.Errorf("%w: %w", embedding.ErrRequestFailed, ctx.Err())
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- harness.worker.Run(ctx)
	}()
	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("embed did not start")
	}
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run after cancellation: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not stop after cancellation")
	}
	if calls := harness.embedder.callCount(); calls != 1 {
		t.Fatalf("embed calls=%d, want 1", calls)
	}
	harness.repository.assertNoMutations(t)
}

func TestRunOnceBoundsClaimAndSanitizesRepositoryErrors(t *testing.T) {
	t.Run("overlarge claim", func(t *testing.T) {
		harness := newWorkerHarness(t, workerHarnessOptions{batchSize: 1})
		harness.repository.items = append(harness.repository.items, harness.repository.items[0])
		_, err := harness.worker.RunOnce(context.Background())
		if !errors.Is(err, ErrRepositoryInvariant) {
			t.Fatalf("error=%v, want repository invariant", err)
		}
		if calls := harness.embedder.callCount(); calls != 0 {
			t.Fatalf("embed calls=%d, want 0", calls)
		}
	})

	t.Run("claim error", func(t *testing.T) {
		secret := "postgres://user:password@host/database memory text"
		harness := newWorkerHarness(t, workerHarnessOptions{})
		harness.repository.claimErr = errors.New(secret)
		_, err := harness.worker.RunOnce(context.Background())
		if !errors.Is(err, ErrRepositoryOperation) {
			t.Fatalf("error=%v, want repository operation", err)
		}
		if strings.Contains(err.Error(), secret) || strings.Contains(err.Error(), "password") {
			t.Fatal("repository error leaked secret data")
		}
	})

	t.Run("negative injected backoff", func(t *testing.T) {
		harness := newWorkerHarness(t, workerHarnessOptions{backoff: func(int) time.Duration { return -time.Second }})
		harness.embedder.embed = embedFailure(embedding.ErrRequestFailed)
		_, err := harness.worker.RunOnce(context.Background())
		if !errors.Is(err, ErrBackoff) {
			t.Fatalf("error=%v, want backoff failure", err)
		}
		harness.repository.assertNoMutations(t)
	})

	t.Run("oversized injected backoff", func(t *testing.T) {
		harness := newWorkerHarness(t, workerHarnessOptions{backoff: func(int) time.Duration {
			return MaximumLeaseDuration + time.Second
		}})
		harness.embedder.embed = embedFailure(embedding.ErrRequestFailed)
		_, err := harness.worker.RunOnce(context.Background())
		if !errors.Is(err, ErrBackoff) {
			t.Fatalf("error=%v, want backoff failure", err)
		}
		harness.repository.assertNoMutations(t)
	})
}

func TestNewRejectsUnboundOrUnboundedConfiguration(t *testing.T) {
	harness := newWorkerHarness(t, workerHarnessOptions{})
	valid := harness.config
	tests := []struct {
		name   string
		mutate func(*Config)
	}{
		{name: "space", mutate: func(config *Config) { config.EmbeddingSpace = "wrong" }},
		{name: "fingerprint", mutate: func(config *Config) { config.ModelFingerprint = strings.Repeat("z", 64) }},
		{name: "batch", mutate: func(config *Config) { config.BatchSize = MaximumBatchSize + 1 }},
		{name: "lease", mutate: func(config *Config) { config.LeaseDuration = MaximumLeaseDuration + time.Second }},
		{name: "concurrency", mutate: func(config *Config) { config.Concurrency = 2 }},
		{name: "attempts", mutate: func(config *Config) { config.MaxAttempts = -1 }},
		{name: "excess attempts", mutate: func(config *Config) { config.MaxAttempts = MaximumAttempts + 1 }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			config := valid
			test.mutate(&config)
			if _, err := New(harness.repository, harness.embedder, config); !errors.Is(err, ErrInvalidConfig) {
				t.Fatalf("New error=%v, want invalid config", err)
			}
		})
	}
}

type workerHarnessOptions struct {
	attemptCount  int
	maxAttempts   int
	batchSize     int
	backoff       Backoff
	monotonicNow  MonotonicClock
	leaseDuration time.Duration
}

type workerHarness struct {
	worker     *Worker
	repository *fakeWorkerRepository
	embedder   *fakeWorkerEmbedder
	item       postgres.ProjectionWorkItem
	config     Config
}

func newWorkerHarness(t *testing.T, options workerHarnessOptions) workerHarness {
	t.Helper()
	descriptor := embedding.Descriptor{
		Provider:        embedding.ProviderLMStudio,
		API:             embedding.APIEmbeddingsV1,
		Model:           "test-embedding-model",
		Dimension:       3,
		DocumentVersion: embedding.MemoryCardDocumentVersion,
	}
	fingerprint := embedding.VectorSHA256(workerTestProbeVector)
	spaceID, err := embedding.SpaceID(
		descriptor.Provider,
		descriptor.Model,
		descriptor.Dimension,
		descriptor.DocumentVersion,
		embedding.RawQueryVersion,
		fingerprint,
	)
	if err != nil {
		t.Fatalf("SpaceID: %v", err)
	}
	leaseUntil := workerTestNow.Add(time.Hour)
	attemptCount := options.attemptCount
	if attemptCount == 0 {
		attemptCount = 1
	}
	memory := domain.MemoryCard{
		ID:          "memory-1",
		CandidateID: "candidate-1",
		TenantID:    "tenant-1",
		UserID:      "user-1",
		Kind:        domain.MemoryKindSemantic,
		Category:    "preference",
		Key:         "editor",
		Value:       "prefers vim",
		Version:     1,
		Status:      domain.MemoryActive,
		CreatedAt:   workerTestNow.Add(-time.Hour),
	}
	document := embedding.MemoryCardDocumentV1(memory)
	item := postgres.ProjectionWorkItem{
		Job: postgres.ProjectionJob{
			ID:                    1,
			TenantID:              memory.TenantID,
			UserID:                memory.UserID,
			MemoryID:              memory.ID,
			EmbeddingSpace:        spaceID,
			ExpectedMemoryVersion: memory.Version,
			State:                 postgres.ProjectionJobLeased,
			AttemptCount:          attemptCount,
			LeaseOwner:            "worker-1",
			LeaseVersion:          7,
			LeaseUntil:            &leaseUntil,
		},
		Target: postgres.ProjectionTarget{
			Space: postgres.EmbeddingSpaceDefinition{
				ID:               spaceID,
				Provider:         descriptor.Provider,
				Model:            descriptor.Model,
				Dimension:        descriptor.Dimension,
				DocumentVersion:  descriptor.DocumentVersion,
				QueryVersion:     embedding.RawQueryVersion,
				ModelFingerprint: fingerprint,
			},
			State: postgres.ProjectionTargetShadow,
		},
		Memory:         memory,
		Document:       document,
		DocumentSHA256: embedding.DocumentSHA256(document),
	}
	repository := &fakeWorkerRepository{items: []postgres.ProjectionWorkItem{item}}
	embedder := &fakeWorkerEmbedder{descriptor: descriptor}
	maxAttempts := options.maxAttempts
	if maxAttempts == 0 {
		maxAttempts = 5
	}
	batchSize := options.batchSize
	if batchSize == 0 {
		batchSize = 1
	}
	backoff := options.backoff
	if backoff == nil {
		backoff = func(int) time.Duration { return 7 * time.Second }
	}
	monotonicNow := options.monotonicNow
	if monotonicNow == nil {
		monotonicNow = func() time.Time { return workerTestNow }
	}
	leaseDuration := options.leaseDuration
	if leaseDuration == 0 {
		leaseDuration = time.Minute
	}
	config := Config{
		EmbeddingSpace:   spaceID,
		ModelFingerprint: fingerprint,
		QueryVersion:     embedding.RawQueryVersion,
		LeaseOwner:       "worker-1",
		BatchSize:        batchSize,
		LeaseDuration:    leaseDuration,
		IdleInterval:     time.Hour,
		MaxAttempts:      maxAttempts,
		Concurrency:      1,
		MonotonicNow:     monotonicNow,
		Backoff:          backoff,
	}
	worker, err := New(repository, embedder, config)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return workerHarness{
		worker:     worker,
		repository: repository,
		embedder:   embedder,
		item:       item,
		config:     config,
	}
}

type fakeWorkerRepository struct {
	mu sync.Mutex

	items     []postgres.ProjectionWorkItem
	claimErr  error
	claimHook func()

	claimCommands    []postgres.ClaimProjectionJobsCommand
	finalizeCommands []postgres.FinalizeProjectionJobCommand
	retryCommands    []postgres.RetryProjectionJobCommand
	deadCommands     []postgres.DeadLetterProjectionJobCommand

	finalizeErr    error
	finalizeResult postgres.FinalizeProjectionJobResult
	retryErr       error
	deadErr        error
}

func (repository *fakeWorkerRepository) ClaimProjectionJobs(_ context.Context, command postgres.ClaimProjectionJobsCommand) ([]postgres.ProjectionWorkItem, error) {
	repository.mu.Lock()
	defer repository.mu.Unlock()
	repository.claimCommands = append(repository.claimCommands, command)
	if repository.claimErr != nil {
		return nil, repository.claimErr
	}
	if repository.claimHook != nil {
		repository.claimHook()
	}
	items := append([]postgres.ProjectionWorkItem(nil), repository.items...)
	repository.items = nil
	return items, nil
}

func (repository *fakeWorkerRepository) FinalizeProjectionJob(_ context.Context, command postgres.FinalizeProjectionJobCommand) (postgres.FinalizeProjectionJobResult, error) {
	repository.mu.Lock()
	defer repository.mu.Unlock()
	repository.finalizeCommands = append(repository.finalizeCommands, command)
	return repository.finalizeResult, repository.finalizeErr
}

func (repository *fakeWorkerRepository) RetryProjectionJob(_ context.Context, command postgres.RetryProjectionJobCommand) (postgres.ProjectionJob, error) {
	repository.mu.Lock()
	defer repository.mu.Unlock()
	repository.retryCommands = append(repository.retryCommands, command)
	return postgres.ProjectionJob{}, repository.retryErr
}

func (repository *fakeWorkerRepository) DeadLetterProjectionJob(_ context.Context, command postgres.DeadLetterProjectionJobCommand) (postgres.ProjectionJob, error) {
	repository.mu.Lock()
	defer repository.mu.Unlock()
	repository.deadCommands = append(repository.deadCommands, command)
	return postgres.ProjectionJob{}, repository.deadErr
}

func (repository *fakeWorkerRepository) assertNoMutations(t *testing.T) {
	t.Helper()
	repository.mu.Lock()
	defer repository.mu.Unlock()
	if len(repository.finalizeCommands) != 0 ||
		len(repository.retryCommands) != 0 ||
		len(repository.deadCommands) != 0 {
		t.Fatal("repository received a mutation")
	}
}

func (repository *fakeWorkerRepository) assertNoFailureTransitions(t *testing.T) {
	t.Helper()
	repository.mu.Lock()
	defer repository.mu.Unlock()
	if len(repository.retryCommands) != 0 || len(repository.deadCommands) != 0 {
		t.Fatal("repository received a failure transition after finalize fencing")
	}
}

type fakeWorkerEmbedder struct {
	mu         sync.Mutex
	descriptor embedding.Descriptor
	embed      func(context.Context, []string) ([][]float32, error)
	calls      [][]string
}

func (embedder *fakeWorkerEmbedder) Descriptor() embedding.Descriptor {
	return embedder.descriptor
}

func (embedder *fakeWorkerEmbedder) Embed(ctx context.Context, inputs []string) ([][]float32, error) {
	embedder.mu.Lock()
	embedder.calls = append(embedder.calls, append([]string(nil), inputs...))
	implementation := embedder.embed
	embedder.mu.Unlock()
	if implementation == nil {
		return [][]float32{workerTestProbeVector, {1, 2, 3}}, nil
	}
	return implementation(ctx, inputs)
}

func (embedder *fakeWorkerEmbedder) callCount() int {
	embedder.mu.Lock()
	defer embedder.mu.Unlock()
	return len(embedder.calls)
}

func (embedder *fakeWorkerEmbedder) lastInputs() []string {
	embedder.mu.Lock()
	defer embedder.mu.Unlock()
	if len(embedder.calls) == 0 {
		return nil
	}
	return append([]string(nil), embedder.calls[len(embedder.calls)-1]...)
}

func embedFailure(err error) func(context.Context, []string) ([][]float32, error) {
	return func(context.Context, []string) ([][]float32, error) {
		return nil, err
	}
}

func equalFloat32(left, right []float32) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index] != right[index] {
			return false
		}
	}
	return true
}
