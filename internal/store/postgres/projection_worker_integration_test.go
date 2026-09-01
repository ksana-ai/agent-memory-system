//go:build integration

package postgres_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/ksana-ai/agent-memory-system/internal/domain"
	"github.com/ksana-ai/agent-memory-system/internal/embedding"
	"github.com/ksana-ai/agent-memory-system/internal/store/postgres"
)

func TestProjectionWorkerConcurrentClaimAndRestartLeaseRecovery(t *testing.T) {
	ctx := context.Background()
	databaseURL := requiredDatabaseURL(t)
	applyMigrations(t, databaseURL)
	space := registerProjectionTarget(t, databaseURL, "worker_claim", "shadow", true)
	storage := openStore(t, databaseURL)

	tenantID, userID := uniqueScope("projection-worker-claim")
	cleanupProjectionWorkerScopeOnExit(t, databaseURL, tenantID, userID)
	cards := make([]domain.MemoryCard, 0, 4)
	for index := 0; index < 4; index++ {
		cards = append(cards, seedProjectionWorkerCard(
			t, storage, tenantID, userID,
			fmt.Sprintf("claim-%d", index), fmt.Sprintf("claim-key-%d", index), nil, 10+index*3,
		))
	}
	secondStore := openStore(t, databaseURL)

	type claimResult struct {
		items []postgres.ProjectionWorkItem
		err   error
	}
	start := make(chan struct{})
	results := make(chan claimResult, 2)
	var wait sync.WaitGroup
	for index, current := range []*postgres.Store{storage, secondStore} {
		index, current := index, current
		wait.Add(1)
		go func() {
			defer wait.Done()
			<-start
			items, err := current.ClaimProjectionJobs(context.Background(), postgres.ClaimProjectionJobsCommand{
				EmbeddingSpace: space,
				LeaseOwner:     fmt.Sprintf("worker-concurrent-%d", index),
				LeaseDuration:  50 * time.Millisecond,
				Limit:          2,
			})
			results <- claimResult{items: items, err: err}
		}()
	}
	close(start)
	wait.Wait()
	close(results)

	seen := make(map[int64]postgres.ProjectionWorkItem, 4)
	for result := range results {
		if result.err != nil {
			t.Fatalf("concurrent claim: %v", result.err)
		}
		if len(result.items) != 2 {
			t.Fatalf("concurrent claim size=%d, want 2", len(result.items))
		}
		for _, item := range result.items {
			if _, duplicate := seen[item.Job.ID]; duplicate {
				t.Fatalf("job %d was concurrently claimed twice", item.Job.ID)
			}
			if item.Job.State != postgres.ProjectionJobLeased || item.Job.AttemptCount != 1 || item.Job.LeaseVersion != 1 {
				t.Fatalf("initial claimed job=%#v", item.Job)
			}
			if item.Job.LeaseUntil == nil || item.Job.LeaseUntil.Sub(item.Job.UpdatedAt) != 50*time.Millisecond {
				t.Fatalf("lease timestamps are not derived from one database clock: %#v", item.Job)
			}
			seen[item.Job.ID] = item
		}
	}
	if len(seen) != len(cards) {
		t.Fatalf("unique claimed jobs=%d, want %d", len(seen), len(cards))
	}

	// Simulate both workers exiting without acknowledgement. A newly opened
	// store reclaims every expired lease with a strictly newer fence token.
	storage.Close()
	secondStore.Close()
	restarted := openStore(t, databaseURL)
	defer restarted.Close()
	time.Sleep(100 * time.Millisecond)
	reclaimed, err := restarted.ClaimProjectionJobs(ctx, postgres.ClaimProjectionJobsCommand{
		EmbeddingSpace: space,
		LeaseOwner:     "worker-after-restart",
		LeaseDuration:  time.Minute,
		Limit:          10,
	})
	if err != nil || len(reclaimed) != len(cards) {
		t.Fatalf("restart reclaim jobs=%d error=%v, want %d", len(reclaimed), err, len(cards))
	}
	for _, item := range reclaimed {
		if item.Job.AttemptCount != 2 || item.Job.LeaseVersion != 2 || item.Job.LeaseOwner != "worker-after-restart" {
			t.Fatalf("reclaimed job=%#v", item.Job)
		}
	}
	for _, old := range seen {
		_, err := restarted.FinalizeProjectionJob(ctx, postgres.FinalizeProjectionJobCommand{
			JobID: old.Job.ID, TenantID: old.Job.TenantID, UserID: old.Job.UserID,
			EmbeddingSpace: old.Job.EmbeddingSpace,
			LeaseOwner:     old.Job.LeaseOwner, LeaseVersion: old.Job.LeaseVersion,
			DocumentSHA256: old.DocumentSHA256, Vector: projectionWorkerVector(0),
		})
		if !errors.Is(err, postgres.ErrProjectionLeaseLost) {
			t.Fatalf("old fence finalize error=%v, want lease lost", err)
		}
		break
	}
	if _, err := restarted.ForgetUser(ctx, tenantID, userID, time.Now().UTC()); err != nil {
		t.Fatalf("cleanup restarted scope: %v", err)
	}
}

func TestProjectionWorkerCrashAttemptsAreBoundedWithoutStarvingNextJob(t *testing.T) {
	ctx := context.Background()
	databaseURL := requiredDatabaseURL(t)
	applyMigrations(t, databaseURL)
	space := registerProjectionTarget(t, databaseURL, "worker_crash_attempts", "shadow", true)
	storage := openStore(t, databaseURL)
	defer storage.Close()
	tenantID, userID := uniqueScope("projection-worker-crash-attempts")
	cleanupProjectionWorkerScopeOnExit(t, databaseURL, tenantID, userID)
	poison := seedProjectionWorkerCard(t, storage, tenantID, userID, "crash-poison", "crash-poison-key", nil, 20)
	healthy := seedProjectionWorkerCard(t, storage, tenantID, userID, "crash-healthy", "crash-healthy-key", nil, 23)

	const maxAttempts = 3
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		items, err := storage.ClaimProjectionJobs(ctx, postgres.ClaimProjectionJobsCommand{
			EmbeddingSpace: space, LeaseOwner: fmt.Sprintf("worker-crash-%d", attempt),
			LeaseDuration: 20 * time.Millisecond, MaxAttempts: maxAttempts, Limit: 1,
		})
		if err != nil || len(items) != 1 || items[0].Job.MemoryID != poison.ID ||
			items[0].Job.AttemptCount != attempt || items[0].Job.LeaseVersion != int64(attempt) {
			t.Fatalf("crash attempt %d items=%#v error=%v", attempt, items, err)
		}
		time.Sleep(40 * time.Millisecond)
	}

	items, err := storage.ClaimProjectionJobs(ctx, postgres.ClaimProjectionJobsCommand{
		EmbeddingSpace: space, LeaseOwner: "worker-after-crash-exhaustion",
		LeaseDuration: time.Minute, MaxAttempts: maxAttempts, Limit: 1,
	})
	if err != nil || len(items) != 1 || items[0].Job.MemoryID != healthy.ID {
		t.Fatalf("claim after crash exhaustion=%#v error=%v", items, err)
	}
	jobs, err := storage.ProjectionJobs(ctx, postgres.ProjectionJobFilter{
		EmbeddingSpace: space, TenantID: tenantID, UserID: userID, Limit: 10,
	})
	if err != nil {
		t.Fatalf("load crash exhaustion jobs: %v", err)
	}
	var poisonJob postgres.ProjectionJob
	for _, job := range jobs {
		if job.MemoryID == poison.ID {
			poisonJob = job
		}
	}
	if poisonJob.State != postgres.ProjectionJobDead || poisonJob.AttemptCount != maxAttempts ||
		poisonJob.LastErrorCode != postgres.ProjectionErrorAttemptsExhausted || poisonJob.CompletedAt == nil {
		t.Fatalf("crash-exhausted job=%#v", poisonJob)
	}
	if _, err := storage.FinalizeProjectionJob(ctx, postgres.FinalizeProjectionJobCommand{
		JobID: items[0].Job.ID, TenantID: tenantID, UserID: userID,
		EmbeddingSpace: items[0].Job.EmbeddingSpace,
		LeaseOwner:     items[0].Job.LeaseOwner, LeaseVersion: items[0].Job.LeaseVersion,
		DocumentSHA256: items[0].DocumentSHA256, Vector: projectionWorkerVector(0),
	}); err != nil {
		t.Fatalf("finalize healthy job after crash exhaustion: %v", err)
	}
	if _, err := storage.ForgetUser(ctx, tenantID, userID, time.Now().UTC()); err != nil {
		t.Fatalf("cleanup crash exhaustion scope: %v", err)
	}
}

func TestProjectionWorkerShadowFinalizeAndFencedTransitions(t *testing.T) {
	ctx := context.Background()
	databaseURL := requiredDatabaseURL(t)
	applyMigrations(t, databaseURL)
	space := registerProjectionTarget(t, databaseURL, "worker_transitions", "shadow", true)
	storage := openStore(t, databaseURL)
	defer storage.Close()
	tenantID, userID := uniqueScope("projection-worker-transitions")
	cleanupProjectionWorkerScopeOnExit(t, databaseURL, tenantID, userID)
	for index := 0; index < 3; index++ {
		seedProjectionWorkerCard(t, storage, tenantID, userID,
			fmt.Sprintf("transition-%d", index), fmt.Sprintf("transition-key-%d", index), nil, 30+index*3)
	}
	items, err := storage.ClaimProjectionJobs(ctx, postgres.ClaimProjectionJobsCommand{
		EmbeddingSpace: space, LeaseOwner: "worker-transition-a",
		LeaseDuration: time.Minute, Limit: 3,
	})
	if err != nil || len(items) != 3 {
		t.Fatalf("claim transition jobs=%d error=%v", len(items), err)
	}
	revisionBefore, err := storage.ContextRevision(ctx, tenantID, userID)
	if err != nil {
		t.Fatalf("revision before finalize: %v", err)
	}
	finalized, err := storage.FinalizeProjectionJob(ctx, postgres.FinalizeProjectionJobCommand{
		JobID: items[0].Job.ID, TenantID: tenantID, UserID: userID,
		EmbeddingSpace: items[0].Job.EmbeddingSpace,
		LeaseOwner:     items[0].Job.LeaseOwner, LeaseVersion: items[0].Job.LeaseVersion,
		DocumentSHA256: items[0].DocumentSHA256, Vector: projectionWorkerVector(0),
	})
	if err != nil {
		t.Fatalf("finalize shadow job: %v", err)
	}
	if finalized.Job.State != postgres.ProjectionJobSucceeded || !finalized.EmbeddingChanged ||
		finalized.RevisionAdvanced || finalized.Cancelled || finalized.Requeued {
		t.Fatalf("shadow finalize result=%#v", finalized)
	}
	if items[0].DocumentSHA256 != embedding.MemoryCardDocumentV1SHA256(items[0].Memory) {
		t.Fatalf("claim document hash does not match fixture-derived document")
	}
	assertEmbeddingCountForMemory(t, databaseURL, tenantID, userID, items[0].Job.MemoryID, 1)
	revisionAfter, err := storage.ContextRevision(ctx, tenantID, userID)
	if err != nil || revisionAfter != revisionBefore {
		t.Fatalf("shadow revision=%d error=%v, want %d", revisionAfter, err, revisionBefore)
	}
	if _, err := storage.FinalizeProjectionJob(ctx, postgres.FinalizeProjectionJobCommand{
		JobID: items[0].Job.ID, TenantID: tenantID, UserID: userID,
		EmbeddingSpace: items[0].Job.EmbeddingSpace,
		LeaseOwner:     items[0].Job.LeaseOwner, LeaseVersion: items[0].Job.LeaseVersion,
		DocumentSHA256: items[0].DocumentSHA256, Vector: projectionWorkerVector(0),
	}); !errors.Is(err, postgres.ErrProjectionLeaseLost) {
		t.Fatalf("duplicate finalize error=%v, want lease lost", err)
	}

	retried, err := storage.RetryProjectionJob(ctx, postgres.RetryProjectionJobCommand{
		JobID: items[1].Job.ID, TenantID: tenantID, UserID: userID,
		LeaseOwner: items[1].Job.LeaseOwner, LeaseVersion: items[1].Job.LeaseVersion,
		ErrorCode:  postgres.ProjectionErrorProviderTimeout,
		RetryAfter: 500 * time.Millisecond,
	})
	if err != nil || retried.State != postgres.ProjectionJobRetry ||
		retried.LastErrorCode != postgres.ProjectionErrorProviderTimeout || retried.LeaseOwner != "" {
		t.Fatalf("retry transition=%#v error=%v", retried, err)
	}
	if retried.AvailableAt.Sub(retried.UpdatedAt) != 500*time.Millisecond {
		t.Fatalf("retry schedule=%s after update, want 500ms", retried.AvailableAt.Sub(retried.UpdatedAt))
	}
	notYetRunnable, err := storage.ClaimProjectionJobs(ctx, postgres.ClaimProjectionJobsCommand{
		EmbeddingSpace: space, LeaseOwner: "worker-transition-b",
		LeaseDuration: time.Minute, Limit: 1,
	})
	if err != nil || len(notYetRunnable) != 0 {
		t.Fatalf("retry was runnable before database backoff: items=%#v error=%v", notYetRunnable, err)
	}
	time.Sleep(550 * time.Millisecond)
	reclaimed, err := storage.ClaimProjectionJobs(ctx, postgres.ClaimProjectionJobsCommand{
		EmbeddingSpace: space, LeaseOwner: "worker-transition-b",
		LeaseDuration: time.Minute, Limit: 1,
	})
	if err != nil || len(reclaimed) != 1 || reclaimed[0].Job.ID != items[1].Job.ID {
		t.Fatalf("retry reclaim=%#v error=%v", reclaimed, err)
	}
	dead, err := storage.DeadLetterProjectionJob(ctx, postgres.DeadLetterProjectionJobCommand{
		JobID: reclaimed[0].Job.ID, TenantID: tenantID, UserID: userID,
		LeaseOwner: reclaimed[0].Job.LeaseOwner, LeaseVersion: reclaimed[0].Job.LeaseVersion,
		ErrorCode: postgres.ProjectionErrorAttemptsExhausted,
	})
	if err != nil || dead.State != postgres.ProjectionJobDead || dead.CompletedAt == nil ||
		dead.LastErrorCode != postgres.ProjectionErrorAttemptsExhausted {
		t.Fatalf("dead transition=%#v error=%v", dead, err)
	}
	cancelled, err := storage.CancelProjectionJob(ctx, postgres.CancelProjectionJobCommand{
		JobID: items[2].Job.ID, TenantID: tenantID, UserID: userID,
		LeaseOwner: items[2].Job.LeaseOwner, LeaseVersion: items[2].Job.LeaseVersion,
	})
	if err != nil || cancelled.State != postgres.ProjectionJobCancelled || cancelled.CompletedAt == nil {
		t.Fatalf("cancel transition=%#v error=%v", cancelled, err)
	}
	if _, err := storage.ForgetUser(ctx, tenantID, userID, time.Now().UTC()); err != nil {
		t.Fatalf("cleanup transition scope: %v", err)
	}
}

func TestProjectionWorkerFinalizeRollsBackVectorAndRevisionBeforeServingAck(t *testing.T) {
	ctx := context.Background()
	databaseURL := requiredDatabaseURL(t)
	applyMigrations(t, databaseURL)
	space, _ := projectionServingTargetForTest(t, databaseURL, "worker_serving")
	storage := openStore(t, databaseURL)
	defer storage.Close()
	target, err := storage.ProjectionTargetBySpace(ctx, space)
	if err != nil || target.State != postgres.ProjectionTargetServing || target.Space.DocumentVersion != embedding.MemoryCardDocumentVersion {
		t.Fatalf("serving target=%#v error=%v", target, err)
	}
	tenantID, userID := uniqueScope("projection-worker-serving")
	cleanupProjectionWorkerScopeOnExit(t, databaseURL, tenantID, userID)
	card := seedProjectionWorkerCard(t, storage, tenantID, userID, "serving", "serving-key", nil, 60)
	ensureProjectionWorkerJob(t, databaseURL, card, space)
	items, err := storage.ClaimProjectionJobs(ctx, postgres.ClaimProjectionJobsCommand{
		EmbeddingSpace: space, LeaseOwner: "worker-serving",
		LeaseDuration: time.Minute, Limit: 10,
	})
	if err != nil {
		t.Fatalf("claim serving job: %v", err)
	}
	item := projectionWorkItemForMemory(t, items, card.ID)
	revisionBefore, err := storage.ContextRevision(ctx, tenantID, userID)
	if err != nil {
		t.Fatalf("revision before serving finalize: %v", err)
	}
	dropTrigger := installProjectionFinalizeFailureTrigger(t, databaseURL, item.Job.ID)
	_, err = storage.FinalizeProjectionJob(ctx, postgres.FinalizeProjectionJobCommand{
		JobID: item.Job.ID, TenantID: tenantID, UserID: userID,
		EmbeddingSpace: item.Job.EmbeddingSpace,
		LeaseOwner:     item.Job.LeaseOwner, LeaseVersion: item.Job.LeaseVersion,
		DocumentSHA256: item.DocumentSHA256, Vector: projectionWorkerVector(1),
	})
	if err == nil {
		t.Fatal("finalize unexpectedly survived acknowledgement trigger")
	}
	if strings.Contains(err.Error(), projectionWorkerRollbackSecret) {
		t.Fatalf("finalize error leaked database diagnostic: %v", err)
	}
	assertEmbeddingCountForMemory(t, databaseURL, tenantID, userID, card.ID, 0)
	revisionAfterRollback, err := storage.ContextRevision(ctx, tenantID, userID)
	if err != nil || revisionAfterRollback != revisionBefore {
		t.Fatalf("rollback revision=%d error=%v, want %d", revisionAfterRollback, err, revisionBefore)
	}
	jobs, err := storage.ProjectionJobs(ctx, postgres.ProjectionJobFilter{
		EmbeddingSpace: space, TenantID: tenantID, UserID: userID, Limit: 10,
	})
	if err != nil || len(jobs) != 1 || jobs[0].State != postgres.ProjectionJobLeased {
		t.Fatalf("job after rollback=%#v error=%v", jobs, err)
	}
	dropTrigger()
	result, err := storage.FinalizeProjectionJob(ctx, postgres.FinalizeProjectionJobCommand{
		JobID: item.Job.ID, TenantID: tenantID, UserID: userID,
		EmbeddingSpace: item.Job.EmbeddingSpace,
		LeaseOwner:     item.Job.LeaseOwner, LeaseVersion: item.Job.LeaseVersion,
		DocumentSHA256: item.DocumentSHA256, Vector: projectionWorkerVector(1),
	})
	if err != nil || !result.EmbeddingChanged || !result.RevisionAdvanced || result.Job.State != postgres.ProjectionJobSucceeded {
		t.Fatalf("serving finalize=%#v error=%v", result, err)
	}
	revisionAfterSuccess, err := storage.ContextRevision(ctx, tenantID, userID)
	if err != nil || revisionAfterSuccess != revisionBefore+1 {
		t.Fatalf("serving revision=%d error=%v, want %d", revisionAfterSuccess, err, revisionBefore+1)
	}
	if _, err := storage.ForgetUser(ctx, tenantID, userID, time.Now().UTC()); err != nil {
		t.Fatalf("cleanup serving scope: %v", err)
	}
}

func TestProjectionWorkerTargetStateUpdateSerializesWithFinalizeAndClaims(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	databaseURL := requiredDatabaseURL(t)
	applyMigrations(t, databaseURL)
	space := registerProjectionTarget(t, databaseURL, "worker_lock_order", "shadow", true)
	storage := openStore(t, databaseURL)
	defer storage.Close()
	tenantID, userID := uniqueScope("projection-worker-lock-order")
	cleanupProjectionWorkerScopeOnExit(t, databaseURL, tenantID, userID)
	first := seedProjectionWorkerCard(t, storage, tenantID, userID, "lock-first", "lock-first-key", nil, 75)
	seedProjectionWorkerCard(t, storage, tenantID, userID, "lock-second", "lock-second-key", nil, 78)
	items, err := storage.ClaimProjectionJobs(ctx, postgres.ClaimProjectionJobsCommand{
		EmbeddingSpace: space, LeaseOwner: "worker-lock-finalize",
		LeaseDuration: time.Minute, Limit: 1,
	})
	if err != nil {
		t.Fatalf("claim lock-order fixture: %v", err)
	}
	item := projectionWorkItemForMemory(t, items, first.ID)

	advisoryKey := int64(1_500_000_000 + scopeSequence.Add(1)%100_000_000)
	lockConn, err := pgx.Connect(ctx, databaseURL)
	if err != nil {
		t.Fatalf("connect advisory lock holder: %v", err)
	}
	defer lockConn.Close(context.Background())
	if _, err := lockConn.Exec(ctx, `SELECT pg_advisory_lock($1)`, advisoryKey); err != nil {
		t.Fatalf("hold finalize advisory lock: %v", err)
	}
	locked := true
	defer func() {
		if locked {
			_, _ = lockConn.Exec(context.Background(), `SELECT pg_advisory_unlock($1)`, advisoryKey)
		}
	}()
	installProjectionFinalizeAdvisoryTrigger(t, databaseURL, item.Job.MemoryID, advisoryKey)

	finalizeResult := make(chan error, 1)
	go func() {
		_, finalizeErr := storage.FinalizeProjectionJob(ctx, postgres.FinalizeProjectionJobCommand{
			JobID: item.Job.ID, TenantID: tenantID, UserID: userID,
			EmbeddingSpace: item.Job.EmbeddingSpace,
			LeaseOwner:     item.Job.LeaseOwner, LeaseVersion: item.Job.LeaseVersion,
			DocumentSHA256: item.DocumentSHA256, Vector: projectionWorkerVector(6),
		})
		finalizeResult <- finalizeErr
	}()
	waitForProjectionAdvisoryWaiter(t, databaseURL, advisoryKey)

	target, err := storage.ProjectionTargetBySpace(ctx, space)
	if err != nil {
		t.Fatalf("load lock-order target: %v", err)
	}
	setResult := make(chan error, 1)
	go func() {
		_, setErr := storage.SetProjectionTarget(ctx, postgres.SetProjectionTargetCommand{
			EmbeddingSpace: space, State: postgres.ProjectionTargetBlocked,
			EnqueueNew: false, UpdatedAt: target.UpdatedAt.Add(time.Second),
		})
		setResult <- setErr
	}()
	select {
	case setErr := <-setResult:
		t.Fatalf("target update passed finalize SHARE lock early: %v", setErr)
	case <-time.After(50 * time.Millisecond):
	}

	type claimOutcome struct {
		items []postgres.ProjectionWorkItem
		err   error
	}
	claimResult := make(chan claimOutcome, 1)
	go func() {
		claimed, claimErr := storage.ClaimProjectionJobs(ctx, postgres.ClaimProjectionJobsCommand{
			EmbeddingSpace: space, LeaseOwner: "worker-lock-claim",
			LeaseDuration: time.Minute, Limit: 1,
		})
		claimResult <- claimOutcome{items: claimed, err: claimErr}
	}()

	if _, err := lockConn.Exec(ctx, `SELECT pg_advisory_unlock($1)`, advisoryKey); err != nil {
		t.Fatalf("release finalize advisory lock: %v", err)
	}
	locked = false
	for name, result := range map[string]<-chan error{
		"finalize":   finalizeResult,
		"set target": setResult,
	} {
		select {
		case resultErr := <-result:
			if resultErr != nil {
				t.Fatalf("%s after lock release: %v", name, resultErr)
			}
		case <-ctx.Done():
			t.Fatalf("%s deadlocked: %v", name, ctx.Err())
		}
	}
	var claim claimOutcome
	select {
	case claim = <-claimResult:
		if claim.err != nil {
			t.Fatalf("claim during target serialization: %v", claim.err)
		}
		if len(claim.items) > 1 {
			t.Fatalf("claim during target serialization returned %d jobs", len(claim.items))
		}
	case <-ctx.Done():
		t.Fatalf("claim deadlocked with target update: %v", ctx.Err())
	}
	// PostgreSQL row locks do not promise queued-writer priority. The claim may
	// serialize before the blocked state update and lease one item, or after it
	// and return none. If it won first, finalization must observe the committed
	// blocked state and atomically requeue rather than persist a vector.
	if len(claim.items) == 1 {
		claimed := claim.items[0]
		result, finalizeErr := storage.FinalizeProjectionJob(ctx, postgres.FinalizeProjectionJobCommand{
			JobID: claimed.Job.ID, TenantID: claimed.Job.TenantID, UserID: claimed.Job.UserID,
			EmbeddingSpace: claimed.Job.EmbeddingSpace,
			LeaseOwner:     claimed.Job.LeaseOwner, LeaseVersion: claimed.Job.LeaseVersion,
			DocumentSHA256: claimed.DocumentSHA256, Vector: projectionWorkerVector(7),
		})
		if finalizeErr != nil || !result.Requeued || result.Cancelled || result.Job.State != postgres.ProjectionJobRetry {
			t.Fatalf("blocked target finalize=%#v error=%v", result, finalizeErr)
		}
	}
	claimedAfterUpdate, err := storage.ClaimProjectionJobs(ctx, postgres.ClaimProjectionJobsCommand{
		EmbeddingSpace: space, LeaseOwner: "worker-lock-after-block",
		LeaseDuration: time.Minute, Limit: 1,
	})
	if err != nil || len(claimedAfterUpdate) != 0 {
		t.Fatalf("claim after blocked target=%d error=%v", len(claimedAfterUpdate), err)
	}
	if _, err := storage.ForgetUser(context.Background(), tenantID, userID, time.Now().UTC()); err != nil {
		t.Fatalf("cleanup lock-order scope: %v", err)
	}
}

func TestProjectionTargetRetirementWaitsForApprovalAndCancelsItsNewJob(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	databaseURL := requiredDatabaseURL(t)
	applyMigrations(t, databaseURL)
	space := registerProjectionTarget(t, databaseURL, "worker_retire_approval", "shadow", true)
	storage := openStore(t, databaseURL)
	defer storage.Close()
	tenantID, userID := uniqueScope("projection-worker-retire-approval")
	cleanupProjectionWorkerScopeOnExit(t, databaseURL, tenantID, userID)
	event := evidence(tenantID, userID, "event-worker-retire-approval", "worker retire approval evidence", 82)
	mustAppend(t, storage, event)
	candidate := candidate(
		tenantID,
		userID,
		"candidate-worker-retire-approval",
		"retire-approval-key",
		"worker retire approval value",
		[]string{event.ID},
		83,
	)
	mustCreateCandidate(t, storage, candidate)
	memoryID := "memory-worker-retire-approval"

	advisoryKey := int64(1_600_000_000 + scopeSequence.Add(1)%100_000_000)
	lockConn, err := pgx.Connect(ctx, databaseURL)
	if err != nil {
		t.Fatalf("connect approval advisory lock holder: %v", err)
	}
	defer lockConn.Close(context.Background())
	if _, err := lockConn.Exec(ctx, `SELECT pg_advisory_lock($1)`, advisoryKey); err != nil {
		t.Fatalf("hold approval advisory lock: %v", err)
	}
	locked := true
	defer func() {
		if locked {
			_, _ = lockConn.Exec(context.Background(), `SELECT pg_advisory_unlock($1)`, advisoryKey)
		}
	}()
	installProjectionApprovalAdvisoryTrigger(t, databaseURL, memoryID, advisoryKey)

	type approvalOutcome struct {
		card *domain.MemoryCard
		err  error
	}
	approved := make(chan approvalOutcome, 1)
	go func() {
		_, card, approveErr := storage.ReviewCandidate(ctx, approval(candidate, memoryID, 84))
		approved <- approvalOutcome{card: card, err: approveErr}
	}()
	waitForProjectionAdvisoryWaiter(t, databaseURL, advisoryKey)

	target, err := storage.ProjectionTargetBySpace(ctx, space)
	if err != nil {
		t.Fatalf("load target before approval retirement: %v", err)
	}
	retireApplication := fmt.Sprintf("projection_retire_wait_%d", scopeSequence.Add(1))
	retireStore := openProjectionWorkerStoreWithApplicationName(t, databaseURL, retireApplication)
	defer retireStore.Close()
	retired := make(chan error, 1)
	go func() {
		_, retireErr := retireStore.SetProjectionTarget(ctx, postgres.SetProjectionTargetCommand{
			EmbeddingSpace: space, State: postgres.ProjectionTargetRetired,
			EnqueueNew: false, UpdatedAt: target.UpdatedAt.Add(time.Second),
		})
		retired <- retireErr
	}()
	waitForProjectionApplicationLock(t, databaseURL, retireApplication)
	if _, err := lockConn.Exec(ctx, `SELECT pg_advisory_unlock($1)`, advisoryKey); err != nil {
		t.Fatalf("release approval advisory lock: %v", err)
	}
	locked = false

	select {
	case result := <-approved:
		if result.err != nil || result.card == nil {
			t.Fatalf("approval during retirement card=%#v error=%v", result.card, result.err)
		}
	case <-ctx.Done():
		t.Fatalf("approval deadlocked with retirement: %v", ctx.Err())
	}
	select {
	case retireErr := <-retired:
		if retireErr != nil {
			t.Fatalf("retirement after approval: %v", retireErr)
		}
	case <-ctx.Done():
		t.Fatalf("retirement deadlocked after approval: %v", ctx.Err())
	}
	jobs, err := storage.ProjectionJobs(ctx, postgres.ProjectionJobFilter{
		EmbeddingSpace: space, TenantID: tenantID, UserID: userID, Limit: 10,
	})
	if err != nil || len(jobs) != 1 || jobs[0].State != postgres.ProjectionJobCancelled ||
		jobs[0].AttemptCount != 0 || jobs[0].CompletedAt == nil {
		t.Fatalf("job after approval/retirement=%#v error=%v", jobs, err)
	}
	if _, err := storage.ForgetUser(ctx, tenantID, userID, time.Now().UTC()); err != nil {
		t.Fatalf("cleanup approval retirement scope: %v", err)
	}
}

func TestProjectionWorkerExpiryTargetStateAndDeletionPropagation(t *testing.T) {
	ctx := context.Background()
	databaseURL := requiredDatabaseURL(t)
	applyMigrations(t, databaseURL)
	space := registerProjectionTarget(t, databaseURL, "worker_lifecycle", "shadow", true)
	storage := openStore(t, databaseURL)
	defer storage.Close()

	// Pending work whose card is already expired is durably cancelled by the
	// claim sweep instead of remaining permanently pending.
	expiredTenant, expiredUser := uniqueScope("projection-worker-expired-pending")
	cleanupProjectionWorkerScopeOnExit(t, databaseURL, expiredTenant, expiredUser)
	expiresAt := time.Now().UTC().Add(time.Minute).Truncate(time.Microsecond)
	expiredCard := seedProjectionWorkerCard(t, storage, expiredTenant, expiredUser,
		"expired-pending", "expired-pending-key", &expiresAt, 90)
	setProjectionWorkerMemoryExpiry(t, databaseURL, expiredCard, time.Now().UTC().Add(-time.Second))
	items, err := storage.ClaimProjectionJobs(ctx, postgres.ClaimProjectionJobsCommand{
		EmbeddingSpace: space, LeaseOwner: "worker-expiry-sweep",
		LeaseDuration: time.Minute, Limit: 10,
	})
	if err != nil {
		t.Fatalf("sweep expired pending job: %v", err)
	}
	for _, item := range items {
		if item.Job.MemoryID == expiredCard.ID {
			t.Fatalf("expired card was claimed: %#v", item.Job)
		}
	}
	expiredJobs, err := storage.ProjectionJobs(ctx, postgres.ProjectionJobFilter{
		EmbeddingSpace: space, TenantID: expiredTenant, UserID: expiredUser, Limit: 10,
	})
	if err != nil || len(expiredJobs) != 1 || expiredJobs[0].State != postgres.ProjectionJobCancelled {
		t.Fatalf("expired pending job=%#v error=%v", expiredJobs, err)
	}

	// An expiry that occurs during a valid lease is also atomically cancelled
	// by finalization, without writing a vector.
	leasedTenant, leasedUser := uniqueScope("projection-worker-expired-leased")
	cleanupProjectionWorkerScopeOnExit(t, databaseURL, leasedTenant, leasedUser)
	leaseExpiry := time.Now().UTC().Add(time.Minute).Truncate(time.Microsecond)
	leasedCard := seedProjectionWorkerCard(t, storage, leasedTenant, leasedUser,
		"expired-leased", "expired-leased-key", &leaseExpiry, 100)
	leasedItems, err := storage.ClaimProjectionJobs(ctx, postgres.ClaimProjectionJobsCommand{
		EmbeddingSpace: space, LeaseOwner: "worker-expired-lease",
		LeaseDuration: time.Minute, Limit: 10,
	})
	if err != nil {
		t.Fatalf("claim expiring lease: %v", err)
	}
	leasedItem := projectionWorkItemForMemory(t, leasedItems, leasedCard.ID)
	setProjectionWorkerMemoryExpiry(t, databaseURL, leasedCard, time.Now().UTC().Add(-time.Second))
	expiredResult, err := storage.FinalizeProjectionJob(ctx, postgres.FinalizeProjectionJobCommand{
		JobID: leasedItem.Job.ID, TenantID: leasedTenant, UserID: leasedUser,
		EmbeddingSpace: leasedItem.Job.EmbeddingSpace,
		LeaseOwner:     leasedItem.Job.LeaseOwner, LeaseVersion: leasedItem.Job.LeaseVersion,
		DocumentSHA256: leasedItem.DocumentSHA256, Vector: projectionWorkerVector(2),
	})
	if err != nil || !expiredResult.Cancelled || expiredResult.Job.State != postgres.ProjectionJobCancelled {
		t.Fatalf("expiry finalize=%#v error=%v", expiredResult, err)
	}
	assertEmbeddingCountForMemory(t, databaseURL, leasedTenant, leasedUser, leasedCard.ID, 0)

	// A blocked target releases valid work to retry. Retirement atomically
	// cancels both a newly leased job and a pending job without a live worker.
	blockedTenant, blockedUser := uniqueScope("projection-worker-blocked")
	cleanupProjectionWorkerScopeOnExit(t, databaseURL, blockedTenant, blockedUser)
	blockedCard := seedProjectionWorkerCard(t, storage, blockedTenant, blockedUser,
		"blocked", "blocked-key", nil, 110)
	blockedItems, err := storage.ClaimProjectionJobs(ctx, postgres.ClaimProjectionJobsCommand{
		EmbeddingSpace: space, LeaseOwner: "worker-before-block",
		LeaseDuration: time.Minute, Limit: 10,
	})
	if err != nil {
		t.Fatalf("claim before block: %v", err)
	}
	blockedItem := projectionWorkItemForMemory(t, blockedItems, blockedCard.ID)
	target, err := storage.ProjectionTargetBySpace(ctx, space)
	if err != nil {
		t.Fatalf("load target before block: %v", err)
	}
	blockedAt := target.UpdatedAt.Add(time.Second)
	if _, err := storage.SetProjectionTarget(ctx, postgres.SetProjectionTargetCommand{
		EmbeddingSpace: space, State: postgres.ProjectionTargetBlocked,
		EnqueueNew: false, UpdatedAt: blockedAt,
	}); err != nil {
		t.Fatalf("block target: %v", err)
	}
	blockedResult, err := storage.FinalizeProjectionJob(ctx, postgres.FinalizeProjectionJobCommand{
		JobID: blockedItem.Job.ID, TenantID: blockedTenant, UserID: blockedUser,
		EmbeddingSpace: blockedItem.Job.EmbeddingSpace,
		LeaseOwner:     blockedItem.Job.LeaseOwner, LeaseVersion: blockedItem.Job.LeaseVersion,
		DocumentSHA256: blockedItem.DocumentSHA256, Vector: projectionWorkerVector(3),
	})
	if err != nil || !blockedResult.Requeued || blockedResult.Job.State != postgres.ProjectionJobRetry {
		t.Fatalf("blocked finalize=%#v error=%v", blockedResult, err)
	}
	shadowAt := blockedAt.Add(time.Second)
	if _, err := storage.SetProjectionTarget(ctx, postgres.SetProjectionTargetCommand{
		EmbeddingSpace: space, State: postgres.ProjectionTargetShadow,
		EnqueueNew: true, UpdatedAt: shadowAt,
	}); err != nil {
		t.Fatalf("reactivate blocked target: %v", err)
	}
	reactivated, err := storage.ClaimProjectionJobs(ctx, postgres.ClaimProjectionJobsCommand{
		EmbeddingSpace: space, LeaseOwner: "worker-before-retire",
		LeaseDuration: time.Minute, Limit: 10,
	})
	if err != nil {
		t.Fatalf("claim reactivated target: %v", err)
	}
	reactivatedItem := projectionWorkItemForMemory(t, reactivated, blockedCard.ID)
	retirePendingTenant, retirePendingUser := uniqueScope("projection-worker-retire-pending")
	cleanupProjectionWorkerScopeOnExit(t, databaseURL, retirePendingTenant, retirePendingUser)
	retirePendingCard := seedProjectionWorkerCard(t, storage, retirePendingTenant, retirePendingUser,
		"retire-pending", "retire-pending-key", nil, 115)
	retiredAt := shadowAt.Add(time.Second)
	if _, err := storage.SetProjectionTarget(ctx, postgres.SetProjectionTargetCommand{
		EmbeddingSpace: space, State: postgres.ProjectionTargetRetired,
		EnqueueNew: false, UpdatedAt: retiredAt,
	}); err != nil {
		t.Fatalf("retire target: %v", err)
	}
	_, err = storage.FinalizeProjectionJob(ctx, postgres.FinalizeProjectionJobCommand{
		JobID: reactivatedItem.Job.ID, TenantID: blockedTenant, UserID: blockedUser,
		EmbeddingSpace: reactivatedItem.Job.EmbeddingSpace,
		LeaseOwner:     reactivatedItem.Job.LeaseOwner, LeaseVersion: reactivatedItem.Job.LeaseVersion,
		DocumentSHA256: reactivatedItem.DocumentSHA256, Vector: projectionWorkerVector(3),
	})
	if !errors.Is(err, postgres.ErrProjectionLeaseLost) {
		t.Fatalf("retired lease finalize error=%v, want lease lost", err)
	}
	for _, scope := range []struct {
		tenantID string
		userID   string
		memoryID string
		attempts int
	}{
		{tenantID: blockedTenant, userID: blockedUser, memoryID: blockedCard.ID, attempts: 2},
		{tenantID: retirePendingTenant, userID: retirePendingUser, memoryID: retirePendingCard.ID, attempts: 0},
	} {
		jobs, jobsErr := storage.ProjectionJobs(ctx, postgres.ProjectionJobFilter{
			EmbeddingSpace: space, TenantID: scope.tenantID, UserID: scope.userID, Limit: 10,
		})
		if jobsErr != nil || len(jobs) != 1 || jobs[0].MemoryID != scope.memoryID ||
			jobs[0].State != postgres.ProjectionJobCancelled || jobs[0].AttemptCount != scope.attempts ||
			jobs[0].CompletedAt == nil || jobs[0].LeaseOwner != "" || jobs[0].LeaseUntil != nil {
			t.Fatalf("retired job for %s=%#v error=%v", scope.memoryID, jobs, jobsErr)
		}
	}

	// Supersession deletes the old leased handoff, and ForgetUser cascades both
	// jobs and vectors. In both cases an old worker is fenced out.
	supersedeSpace := registerProjectionTarget(t, databaseURL, "worker_supersede", "shadow", true)
	supersedeTenant, supersedeUser := uniqueScope("projection-worker-supersede")
	cleanupProjectionWorkerScopeOnExit(t, databaseURL, supersedeTenant, supersedeUser)
	oldCard := seedProjectionWorkerCard(t, storage, supersedeTenant, supersedeUser,
		"supersede-old", "same-key", nil, 120)
	oldItems, err := storage.ClaimProjectionJobs(ctx, postgres.ClaimProjectionJobsCommand{
		EmbeddingSpace: supersedeSpace, LeaseOwner: "worker-before-supersede",
		LeaseDuration: time.Minute, Limit: 10,
	})
	if err != nil {
		t.Fatalf("claim before supersede: %v", err)
	}
	oldItem := projectionWorkItemForMemory(t, oldItems, oldCard.ID)
	newCard := seedProjectionWorkerCard(t, storage, supersedeTenant, supersedeUser,
		"supersede-new", "same-key", nil, 130)
	if newCard.Version != oldCard.Version+1 {
		t.Fatalf("superseding version=%d, want %d", newCard.Version, oldCard.Version+1)
	}
	if _, err := storage.FinalizeProjectionJob(ctx, postgres.FinalizeProjectionJobCommand{
		JobID: oldItem.Job.ID, TenantID: supersedeTenant, UserID: supersedeUser,
		EmbeddingSpace: oldItem.Job.EmbeddingSpace,
		LeaseOwner:     oldItem.Job.LeaseOwner, LeaseVersion: oldItem.Job.LeaseVersion,
		DocumentSHA256: oldItem.DocumentSHA256, Vector: projectionWorkerVector(4),
	}); !errors.Is(err, postgres.ErrProjectionLeaseLost) {
		t.Fatalf("superseded finalize error=%v, want lease lost", err)
	}

	forgetTenant, forgetUser := uniqueScope("projection-worker-forget")
	cleanupProjectionWorkerScopeOnExit(t, databaseURL, forgetTenant, forgetUser)
	forgetCard := seedProjectionWorkerCard(t, storage, forgetTenant, forgetUser,
		"forget", "forget-key", nil, 140)
	forgetItems, err := storage.ClaimProjectionJobs(ctx, postgres.ClaimProjectionJobsCommand{
		EmbeddingSpace: supersedeSpace, LeaseOwner: "worker-before-forget",
		LeaseDuration: time.Minute, Limit: 10,
	})
	if err != nil {
		t.Fatalf("claim before forget: %v", err)
	}
	forgetItem := projectionWorkItemForMemory(t, forgetItems, forgetCard.ID)
	if _, err := storage.ForgetUser(ctx, forgetTenant, forgetUser, time.Now().UTC()); err != nil {
		t.Fatalf("forget leased scope: %v", err)
	}
	if _, err := storage.FinalizeProjectionJob(ctx, postgres.FinalizeProjectionJobCommand{
		JobID: forgetItem.Job.ID, TenantID: forgetTenant, UserID: forgetUser,
		EmbeddingSpace: forgetItem.Job.EmbeddingSpace,
		LeaseOwner:     forgetItem.Job.LeaseOwner, LeaseVersion: forgetItem.Job.LeaseVersion,
		DocumentSHA256: forgetItem.DocumentSHA256, Vector: projectionWorkerVector(5),
	}); !errors.Is(err, postgres.ErrProjectionLeaseLost) {
		t.Fatalf("forgotten finalize error=%v, want lease lost", err)
	}
	if countProjectionJobsForScope(t, databaseURL, forgetTenant, forgetUser) != 0 {
		t.Fatal("ForgetUser left projection jobs")
	}
	assertEmbeddingCountForMemory(t, databaseURL, forgetTenant, forgetUser, forgetCard.ID, 0)

	for _, scope := range [][2]string{
		{expiredTenant, expiredUser}, {leasedTenant, leasedUser},
		{blockedTenant, blockedUser}, {retirePendingTenant, retirePendingUser},
		{supersedeTenant, supersedeUser},
	} {
		if _, err := storage.ForgetUser(ctx, scope[0], scope[1], time.Now().UTC().Add(2*time.Minute)); err != nil {
			t.Fatalf("cleanup lifecycle scope %q: %v", scope[0], err)
		}
	}
}

const projectionWorkerRollbackSecret = "projection_worker_rollback_secret"

func cleanupProjectionWorkerScopeOnExit(t *testing.T, databaseURL, tenantID, userID string) {
	t.Helper()
	t.Cleanup(func() {
		cleanupScopes(t, databaseURL, [][2]string{{tenantID, userID}})
	})
}

func openProjectionWorkerStoreWithApplicationName(t *testing.T, databaseURL, applicationName string) *postgres.Store {
	t.Helper()
	config, err := pgxpool.ParseConfig(databaseURL)
	if err != nil {
		t.Fatalf("parse worker integration database URL: %v", err)
	}
	if config.ConnConfig.RuntimeParams == nil {
		config.ConnConfig.RuntimeParams = make(map[string]string)
	}
	config.ConnConfig.RuntimeParams["application_name"] = applicationName
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		t.Fatalf("open named worker integration pool: %v", err)
	}
	storage := postgres.New(pool)
	if err := storage.Ping(ctx); err != nil {
		storage.Close()
		t.Fatalf("ping named worker integration pool: %v", err)
	}
	return storage
}

func seedProjectionWorkerCard(
	t *testing.T,
	storage *postgres.Store,
	tenantID, userID, id, key string,
	expiresAt *time.Time,
	offset int,
) domain.MemoryCard {
	t.Helper()
	event := evidence(tenantID, userID, "event-worker-"+id, "worker evidence "+id, offset)
	mustAppend(t, storage, event)
	value := candidate(tenantID, userID, "candidate-worker-"+id, key, "worker value "+id, []string{event.ID}, offset+1)
	value.ExpiresAt = cloneOptionalTime(expiresAt)
	mustCreateCandidate(t, storage, value)
	_, card, err := storage.ReviewCandidate(context.Background(), approval(value, "memory-worker-"+id, offset+2))
	if err != nil || card == nil {
		t.Fatalf("approve worker card %q: card=%#v error=%v", id, card, err)
	}
	return *card
}

func projectionWorkerVector(index int) []float32 {
	vector := make([]float32, postgres.VectorDimension)
	vector[index] = 1
	return vector
}

func projectionWorkItemForMemory(t *testing.T, items []postgres.ProjectionWorkItem, memoryID string) postgres.ProjectionWorkItem {
	t.Helper()
	for _, item := range items {
		if item.Job.MemoryID == memoryID {
			return item
		}
	}
	t.Fatalf("work item for memory %q not found in %#v", memoryID, items)
	return postgres.ProjectionWorkItem{}
}

func ensureProjectionWorkerJob(t *testing.T, databaseURL string, card domain.MemoryCard, space string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	conn, err := pgx.Connect(ctx, databaseURL)
	if err != nil {
		t.Fatalf("connect to ensure projection job: %v", err)
	}
	defer conn.Close(context.Background())
	if _, err := conn.Exec(ctx, `
		INSERT INTO agent_memory.embedding_projection_jobs (
			tenant_id, user_id, memory_id, embedding_space, expected_memory_version
		) VALUES ($1, $2, $3, $4, $5)
		ON CONFLICT (tenant_id, user_id, memory_id, embedding_space) DO NOTHING`,
		card.TenantID, card.UserID, card.ID, space, card.Version,
	); err != nil {
		t.Fatalf("ensure projection job: %v", err)
	}
}

func setProjectionWorkerMemoryExpiry(t *testing.T, databaseURL string, card domain.MemoryCard, expiresAt time.Time) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	conn, err := pgx.Connect(ctx, databaseURL)
	if err != nil {
		t.Fatalf("connect to set worker memory expiry: %v", err)
	}
	defer conn.Close(context.Background())
	commandTag, err := conn.Exec(ctx, `
		UPDATE agent_memory.memory_cards
		SET expires_at = $4
		WHERE tenant_id = $1 AND user_id = $2 AND id = $3`,
		card.TenantID, card.UserID, card.ID, canonicalWorkerTestTime(expiresAt),
	)
	if err != nil {
		t.Fatalf("set worker memory expiry: %v", err)
	}
	if commandTag.RowsAffected() != 1 {
		t.Fatalf("set worker memory expiry affected %d rows", commandTag.RowsAffected())
	}
}

func canonicalWorkerTestTime(value time.Time) time.Time {
	return value.UTC().Truncate(time.Microsecond)
}

func installProjectionFinalizeFailureTrigger(t *testing.T, databaseURL string, jobID int64) func() {
	t.Helper()
	sequence := scopeSequence.Add(1)
	functionName := fmt.Sprintf("test_projection_finalize_failure_%d", sequence)
	triggerName := fmt.Sprintf("test_projection_finalize_failure_%d", sequence)
	qualifiedFunction := pgx.Identifier{"agent_memory", functionName}.Sanitize()
	quotedTrigger := pgx.Identifier{triggerName}.Sanitize()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	conn, err := pgx.Connect(ctx, databaseURL)
	if err != nil {
		t.Fatalf("connect to install finalize trigger: %v", err)
	}
	functionSQL := fmt.Sprintf(`
		CREATE FUNCTION %s() RETURNS trigger LANGUAGE plpgsql AS $body$
		BEGIN
			IF NEW.id = %d AND NEW.state = 'succeeded' THEN
				RAISE EXCEPTION %s;
			END IF;
			RETURN NEW;
		END
		$body$`, qualifiedFunction, jobID, postgresTestLiteral(projectionWorkerRollbackSecret))
	if _, err := conn.Exec(ctx, functionSQL); err != nil {
		_ = conn.Close(context.Background())
		t.Fatalf("create finalize trigger function: %v", err)
	}
	triggerSQL := fmt.Sprintf(`
		CREATE TRIGGER %s BEFORE UPDATE ON agent_memory.embedding_projection_jobs
		FOR EACH ROW EXECUTE FUNCTION %s()`, quotedTrigger, qualifiedFunction)
	if _, err := conn.Exec(ctx, triggerSQL); err != nil {
		_, _ = conn.Exec(ctx, "DROP FUNCTION "+qualifiedFunction+"()")
		_ = conn.Close(context.Background())
		t.Fatalf("create finalize trigger: %v", err)
	}
	if err := conn.Close(context.Background()); err != nil {
		t.Fatalf("close finalize trigger connection: %v", err)
	}

	var once sync.Once
	drop := func() {
		once.Do(func() {
			dropCtx, dropCancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer dropCancel()
			dropConn, dropErr := pgx.Connect(dropCtx, databaseURL)
			if dropErr != nil {
				t.Errorf("connect to drop finalize trigger: %v", dropErr)
				return
			}
			defer dropConn.Close(context.Background())
			if _, dropErr = dropConn.Exec(dropCtx, fmt.Sprintf(
				"DROP TRIGGER IF EXISTS %s ON agent_memory.embedding_projection_jobs", quotedTrigger,
			)); dropErr != nil {
				t.Errorf("drop finalize trigger: %v", dropErr)
				return
			}
			if _, dropErr = dropConn.Exec(dropCtx, "DROP FUNCTION IF EXISTS "+qualifiedFunction+"()"); dropErr != nil {
				t.Errorf("drop finalize function: %v", dropErr)
			}
		})
	}
	t.Cleanup(drop)
	return drop
}

func installProjectionFinalizeAdvisoryTrigger(t *testing.T, databaseURL, memoryID string, advisoryKey int64) {
	t.Helper()
	sequence := scopeSequence.Add(1)
	functionName := fmt.Sprintf("test_projection_finalize_wait_%d", sequence)
	triggerName := fmt.Sprintf("test_projection_finalize_wait_%d", sequence)
	qualifiedFunction := pgx.Identifier{"agent_memory", functionName}.Sanitize()
	quotedTrigger := pgx.Identifier{triggerName}.Sanitize()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	conn, err := pgx.Connect(ctx, databaseURL)
	if err != nil {
		t.Fatalf("connect to install finalize wait trigger: %v", err)
	}
	functionSQL := fmt.Sprintf(`
		CREATE FUNCTION %s() RETURNS trigger LANGUAGE plpgsql AS $body$
		BEGIN
			IF NEW.memory_id = %s THEN
				PERFORM pg_advisory_xact_lock(%d);
			END IF;
			RETURN NEW;
		END
		$body$`, qualifiedFunction, postgresTestLiteral(memoryID), advisoryKey)
	if _, err := conn.Exec(ctx, functionSQL); err != nil {
		_ = conn.Close(context.Background())
		t.Fatalf("create finalize wait function: %v", err)
	}
	triggerSQL := fmt.Sprintf(`
		CREATE TRIGGER %s BEFORE INSERT ON agent_memory.memory_embeddings
		FOR EACH ROW EXECUTE FUNCTION %s()`, quotedTrigger, qualifiedFunction)
	if _, err := conn.Exec(ctx, triggerSQL); err != nil {
		_, _ = conn.Exec(ctx, "DROP FUNCTION "+qualifiedFunction+"()")
		_ = conn.Close(context.Background())
		t.Fatalf("create finalize wait trigger: %v", err)
	}
	if err := conn.Close(context.Background()); err != nil {
		t.Fatalf("close finalize wait trigger connection: %v", err)
	}
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cleanupCancel()
		cleanupConn, cleanupErr := pgx.Connect(cleanupCtx, databaseURL)
		if cleanupErr != nil {
			t.Errorf("connect to drop finalize wait trigger: %v", cleanupErr)
			return
		}
		defer cleanupConn.Close(context.Background())
		if _, cleanupErr = cleanupConn.Exec(cleanupCtx, fmt.Sprintf(
			"DROP TRIGGER IF EXISTS %s ON agent_memory.memory_embeddings", quotedTrigger,
		)); cleanupErr != nil {
			t.Errorf("drop finalize wait trigger: %v", cleanupErr)
			return
		}
		if _, cleanupErr = cleanupConn.Exec(cleanupCtx, "DROP FUNCTION IF EXISTS "+qualifiedFunction+"()"); cleanupErr != nil {
			t.Errorf("drop finalize wait function: %v", cleanupErr)
		}
	})
}

func installProjectionApprovalAdvisoryTrigger(t *testing.T, databaseURL, memoryID string, advisoryKey int64) {
	t.Helper()
	sequence := scopeSequence.Add(1)
	functionName := fmt.Sprintf("test_projection_approval_wait_%d", sequence)
	triggerName := fmt.Sprintf("test_projection_approval_wait_%d", sequence)
	qualifiedFunction := pgx.Identifier{"agent_memory", functionName}.Sanitize()
	quotedTrigger := pgx.Identifier{triggerName}.Sanitize()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	conn, err := pgx.Connect(ctx, databaseURL)
	if err != nil {
		t.Fatalf("connect to install approval wait trigger: %v", err)
	}
	functionSQL := fmt.Sprintf(`
		CREATE FUNCTION %s() RETURNS trigger LANGUAGE plpgsql AS $body$
		BEGIN
			IF NEW.id = %s THEN
				PERFORM pg_advisory_xact_lock(%d);
			END IF;
			RETURN NEW;
		END
		$body$`, qualifiedFunction, postgresTestLiteral(memoryID), advisoryKey)
	if _, err := conn.Exec(ctx, functionSQL); err != nil {
		_ = conn.Close(context.Background())
		t.Fatalf("create approval wait function: %v", err)
	}
	triggerSQL := fmt.Sprintf(`
		CREATE TRIGGER %s BEFORE INSERT ON agent_memory.memory_cards
		FOR EACH ROW EXECUTE FUNCTION %s()`, quotedTrigger, qualifiedFunction)
	if _, err := conn.Exec(ctx, triggerSQL); err != nil {
		_, _ = conn.Exec(ctx, "DROP FUNCTION "+qualifiedFunction+"()")
		_ = conn.Close(context.Background())
		t.Fatalf("create approval wait trigger: %v", err)
	}
	if err := conn.Close(context.Background()); err != nil {
		t.Fatalf("close approval wait trigger connection: %v", err)
	}
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cleanupCancel()
		cleanupConn, cleanupErr := pgx.Connect(cleanupCtx, databaseURL)
		if cleanupErr != nil {
			t.Errorf("connect to drop approval wait trigger: %v", cleanupErr)
			return
		}
		defer cleanupConn.Close(context.Background())
		if _, cleanupErr = cleanupConn.Exec(cleanupCtx, fmt.Sprintf(
			"DROP TRIGGER IF EXISTS %s ON agent_memory.memory_cards", quotedTrigger,
		)); cleanupErr != nil {
			t.Errorf("drop approval wait trigger: %v", cleanupErr)
			return
		}
		if _, cleanupErr = cleanupConn.Exec(cleanupCtx, "DROP FUNCTION IF EXISTS "+qualifiedFunction+"()"); cleanupErr != nil {
			t.Errorf("drop approval wait function: %v", cleanupErr)
		}
	})
}

func waitForProjectionAdvisoryWaiter(t *testing.T, databaseURL string, advisoryKey int64) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	conn, err := pgx.Connect(ctx, databaseURL)
	if err != nil {
		t.Fatalf("connect to observe advisory waiter: %v", err)
	}
	defer conn.Close(context.Background())
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		var waiting bool
		err := conn.QueryRow(ctx, `
			SELECT EXISTS (
				SELECT 1
				FROM pg_locks
				WHERE locktype = 'advisory'
				  AND granted = false
				  AND classid = 0
				  AND objid = $1::oid
			)`, advisoryKey).Scan(&waiting)
		if err != nil {
			t.Fatalf("observe advisory waiter: %v", err)
		}
		if waiting {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatalf("transaction did not reach advisory wait: %v", ctx.Err())
		case <-ticker.C:
		}
	}
}

func waitForProjectionApplicationLock(t *testing.T, databaseURL, applicationName string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	conn, err := pgx.Connect(ctx, databaseURL)
	if err != nil {
		t.Fatalf("connect to observe projection target waiter: %v", err)
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
			)`, applicationName).Scan(&waiting); err != nil {
			t.Fatalf("observe projection target waiter: %v", err)
		}
		if waiting {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatalf("target update did not reach its row lock: %v", ctx.Err())
		case <-ticker.C:
		}
	}
}
