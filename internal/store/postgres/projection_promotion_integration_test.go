//go:build integration

package postgres_test

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/ksana-ai/agent-memory-system/internal/domain"
	"github.com/ksana-ai/agent-memory-system/internal/embedding"
	"github.com/ksana-ai/agent-memory-system/internal/migrations"
	"github.com/ksana-ai/agent-memory-system/internal/store/postgres"
)

var promotionDatabaseSequence atomic.Uint64

func TestProjectionPromotionSuccessHistoricalRetryRollbackAndServingGuards(t *testing.T) {
	ctx := context.Background()
	databaseURL, storage := isolatedProjectionPromotionStore(t)

	spaceA := registerPromotionTarget(t, storage, "success_a")
	spaceB := registerPromotionTarget(t, storage, "success_b")
	spaceC := registerPromotionTarget(t, storage, "success_c")
	initial, err := storage.CurrentServingProjection(ctx)
	if err != nil || initial.Target != nil {
		t.Fatalf("initial serving state=%#v error=%v, want nil target", initial, err)
	}
	if _, err := storage.ProjectionPromotionByOperationID(ctx, promotionOperationID(1)); !errors.Is(err, domain.ErrNotFound) {
		t.Fatalf("missing promotion receipt error=%v, want not found", err)
	}

	liveTenant, liveUser := uniqueScope("promotion-success-live")
	liveCard := approveVectorCard(t, storage, liveTenant, liveUser, "promotion-success-live", "editor", "vim", 10, 12, nil)
	expiredTenant, expiredUser := uniqueScope("promotion-success-expired")
	expiredAt := time.Now().UTC().Add(-time.Hour).Truncate(time.Microsecond)
	_ = approveVectorCard(t, storage, expiredTenant, expiredUser, "promotion-success-expired", "expired", "old", 14, 16, &expiredAt)
	for _, space := range []string{spaceA, spaceB, spaceC} {
		coverPromotionCard(t, storage, databaseURL, liveCard, space)
	}
	liveRevisionBefore := mustContextRevision(t, storage, liveTenant, liveUser)
	expiredRevisionBefore := mustContextRevision(t, storage, expiredTenant, expiredUser)
	generationBefore := projectionDeploymentGeneration(t, databaseURL)

	firstCommand := postgres.PromoteProjectionCommand{
		OperationID: promotionOperationID(1), ToSpace: spaceA,
	}
	first, err := storage.PromoteProjection(ctx, firstCommand)
	if err != nil {
		t.Fatalf("initial projection promotion: %v", err)
	}
	if first.FromSpace != "" || first.ToSpace != spaceA || first.AllowEmpty ||
		first.LiveScopeCount != 1 || first.LiveCardCount != 1 || first.CoveredCardCount != 1 ||
		first.PreviousGeneration != generationBefore || first.Generation != generationBefore+1 ||
		first.CutoffAt.IsZero() || first.PromotedAt.Before(first.CutoffAt) {
		t.Fatalf("initial promotion receipt=%#v", first)
	}
	assertPromotionTargetState(t, storage, spaceA, postgres.ProjectionTargetServing, true)
	assertPromotionTargetState(t, storage, spaceB, postgres.ProjectionTargetShadow, true)
	assertContextRevision(t, storage, liveTenant, liveUser, liveRevisionBefore+1)
	assertContextRevision(t, storage, expiredTenant, expiredUser, expiredRevisionBefore)
	loaded, err := storage.ProjectionPromotionByOperationID(ctx, first.OperationID)
	if err != nil || loaded != first {
		t.Fatalf("loaded promotion receipt=%#v error=%v, want %#v", loaded, err, first)
	}

	second, err := storage.PromoteProjection(ctx, postgres.PromoteProjectionCommand{
		OperationID: promotionOperationID(2), ExpectedFrom: spaceA, ToSpace: spaceB,
	})
	if err != nil || second.FromSpace != spaceA || second.ToSpace != spaceB || second.Generation != first.Generation+1 {
		t.Fatalf("second promotion receipt=%#v error=%v", second, err)
	}
	assertPromotionTargetState(t, storage, spaceA, postgres.ProjectionTargetShadow, true)
	assertPromotionTargetState(t, storage, spaceB, postgres.ProjectionTargetServing, true)
	revisionAfterSecond := mustContextRevision(t, storage, liveTenant, liveUser)
	generationAfterSecond := projectionDeploymentGeneration(t, databaseURL)

	retried, err := storage.PromoteProjection(ctx, firstCommand)
	if err != nil || retried != first {
		t.Fatalf("historical exact retry=%#v error=%v, want first receipt", retried, err)
	}
	assertCurrentPromotionSpace(t, storage, spaceB, generationAfterSecond)
	assertContextRevision(t, storage, liveTenant, liveUser, revisionAfterSecond)
	if _, err := storage.PromoteProjection(ctx, postgres.PromoteProjectionCommand{
		OperationID: first.OperationID, ExpectedFrom: spaceB, ToSpace: spaceC,
	}); !errors.Is(err, domain.ErrConflict) {
		t.Fatalf("operation parameter drift error=%v, want conflict", err)
	}

	setPromotionEmbeddingHash(t, databaseURL, liveCard, spaceA, strings.Repeat("f", 64))
	rollbackCommand := postgres.PromoteProjectionCommand{
		OperationID: promotionOperationID(3), ExpectedFrom: spaceB, ToSpace: spaceA,
	}
	if _, err := storage.PromoteProjection(ctx, rollbackCommand); !errors.Is(err, domain.ErrConflict) {
		t.Fatalf("rollback with stale coverage error=%v, want conflict", err)
	}
	assertCurrentPromotionSpace(t, storage, spaceB, generationAfterSecond)
	assertContextRevision(t, storage, liveTenant, liveUser, revisionAfterSecond)
	if _, err := storage.ProjectionPromotionByOperationID(ctx, rollbackCommand.OperationID); !errors.Is(err, domain.ErrNotFound) {
		t.Fatalf("failed rollback receipt error=%v, want not found", err)
	}
	setPromotionEmbeddingHash(t, databaseURL, liveCard, spaceA, embedding.MemoryCardDocumentV1SHA256(liveCard))
	rolledBack, err := storage.PromoteProjection(ctx, rollbackCommand)
	if err != nil || rolledBack.FromSpace != spaceB || rolledBack.ToSpace != spaceA || rolledBack.Generation != generationAfterSecond+1 {
		t.Fatalf("covered rollback receipt=%#v error=%v", rolledBack, err)
	}
	assertPromotionTargetState(t, storage, spaceA, postgres.ProjectionTargetServing, true)
	assertPromotionTargetState(t, storage, spaceB, postgres.ProjectionTargetShadow, true)
	assertContextRevision(t, storage, expiredTenant, expiredUser, expiredRevisionBefore)

	servingA, err := storage.ProjectionTargetBySpace(ctx, spaceA)
	if err != nil {
		t.Fatalf("load serving target for generic guard: %v", err)
	}
	if _, err := storage.SetProjectionTarget(ctx, postgres.SetProjectionTargetCommand{
		EmbeddingSpace: spaceA, State: postgres.ProjectionTargetShadow, EnqueueNew: true,
		UpdatedAt: servingA.UpdatedAt.Add(time.Microsecond),
	}); !errors.Is(err, domain.ErrConflict) {
		t.Fatalf("generic serving demotion error=%v, want conflict", err)
	}
	shadowB, err := storage.ProjectionTargetBySpace(ctx, spaceB)
	if err != nil {
		t.Fatalf("load shadow target for generic guard: %v", err)
	}
	if _, err := storage.SetProjectionTarget(ctx, postgres.SetProjectionTargetCommand{
		EmbeddingSpace: spaceB, State: postgres.ProjectionTargetServing, EnqueueNew: true,
		UpdatedAt: shadowB.UpdatedAt.Add(time.Microsecond),
	}); !errors.Is(err, domain.ErrConflict) {
		t.Fatalf("generic serving entry error=%v, want conflict", err)
	}
	servingRegistration := projectionRepositoryRegistration(
		"space_promotion_forbidden_serving", postgres.ProjectionTargetServing, true, fixtureTime(90),
	)
	if _, err := storage.RegisterProjectionTarget(ctx, servingRegistration); !errors.Is(err, domain.ErrInvalid) {
		t.Fatalf("initial serving registration error=%v, want invalid", err)
	}
}

func TestProjectionPromotionCoverageBlockersHaveNoSideEffects(t *testing.T) {
	for index, blocker := range []string{
		"missing", "pending", "leased", "retry", "dead", "cancelled", "succeeded_no_vector", "hash_mismatch", "version_mismatch",
	} {
		t.Run(blocker, func(t *testing.T) {
			ctx := context.Background()
			databaseURL, storage := isolatedProjectionPromotionStore(t)
			space := registerPromotionTarget(t, storage, "blocker_"+blocker)
			tenantID, userID := uniqueScope("promotion-blocker-" + blocker)
			card := approveVectorCard(t, storage, tenantID, userID, "promotion-blocker-"+blocker, "key", "value", 100+index*3, 102+index*3, nil)
			switch blocker {
			case "missing":
				execPromotionSQL(t, databaseURL, `DELETE FROM agent_memory.embedding_projection_jobs WHERE tenant_id=$1 AND user_id=$2 AND memory_id=$3 AND embedding_space=$4`, card.TenantID, card.UserID, card.ID, space)
			case "pending":
			case "leased":
				execPromotionSQL(t, databaseURL, `UPDATE agent_memory.embedding_projection_jobs SET state='leased', attempt_count=1, lease_owner='promotion-blocker-worker', lease_version=1, lease_until=clock_timestamp()+interval '1 hour', updated_at=clock_timestamp() WHERE tenant_id=$1 AND user_id=$2 AND memory_id=$3 AND embedding_space=$4`, card.TenantID, card.UserID, card.ID, space)
			case "retry":
				execPromotionSQL(t, databaseURL, `UPDATE agent_memory.embedding_projection_jobs SET state='retry', attempt_count=1, lease_owner=NULL, lease_until=NULL, updated_at=clock_timestamp(), available_at=clock_timestamp()+interval '1 hour' WHERE tenant_id=$1 AND user_id=$2 AND memory_id=$3 AND embedding_space=$4`, card.TenantID, card.UserID, card.ID, space)
			case "dead":
				markProjectionJobState(t, databaseURL, card.TenantID, card.UserID, card.ID, space, "dead")
			case "cancelled":
				execPromotionSQL(t, databaseURL, `UPDATE agent_memory.embedding_projection_jobs SET state='cancelled', lease_owner=NULL, lease_until=NULL, updated_at=clock_timestamp(), completed_at=clock_timestamp() WHERE tenant_id=$1 AND user_id=$2 AND memory_id=$3 AND embedding_space=$4`, card.TenantID, card.UserID, card.ID, space)
			case "succeeded_no_vector":
				markProjectionJobState(t, databaseURL, card.TenantID, card.UserID, card.ID, space, "succeeded")
			case "hash_mismatch":
				coverPromotionCard(t, storage, databaseURL, card, space)
				setPromotionEmbeddingHash(t, databaseURL, card, space, strings.Repeat("f", 64))
			case "version_mismatch":
				coverPromotionCard(t, storage, databaseURL, card, space)
				execPromotionSQL(t, databaseURL, `UPDATE agent_memory.embedding_projection_jobs SET expected_memory_version=expected_memory_version+1 WHERE tenant_id=$1 AND user_id=$2 AND memory_id=$3 AND embedding_space=$4`, card.TenantID, card.UserID, card.ID, space)
			}
			generationBefore := projectionDeploymentGeneration(t, databaseURL)
			revisionBefore := mustContextRevision(t, storage, tenantID, userID)
			operationID := promotionOperationID(100 + index)
			if _, err := storage.PromoteProjection(ctx, postgres.PromoteProjectionCommand{
				OperationID: operationID, ToSpace: space,
			}); !errors.Is(err, domain.ErrConflict) {
				t.Fatalf("%s coverage error=%v, want conflict", blocker, err)
			}
			assertCurrentPromotionSpace(t, storage, "", generationBefore)
			assertPromotionTargetState(t, storage, space, postgres.ProjectionTargetShadow, true)
			assertContextRevision(t, storage, tenantID, userID, revisionBefore)
			if _, err := storage.ProjectionPromotionByOperationID(ctx, operationID); !errors.Is(err, domain.ErrNotFound) {
				t.Fatalf("%s failed receipt error=%v, want not found", blocker, err)
			}
		})
	}
}

func TestProjectionPromotionReceiptFailureRollsBackTargetRevisionAndGeneration(t *testing.T) {
	ctx := context.Background()
	databaseURL, storage := isolatedProjectionPromotionStore(t)
	spaceA := registerPromotionTarget(t, storage, "fault_a")
	spaceB := registerPromotionTarget(t, storage, "fault_b")
	tenantID, userID := uniqueScope("promotion-fault")
	card := approveVectorCard(t, storage, tenantID, userID, "promotion-fault", "key", "value", 200, 202, nil)
	coverPromotionCard(t, storage, databaseURL, card, spaceA)
	coverPromotionCard(t, storage, databaseURL, card, spaceB)
	if _, err := storage.PromoteProjection(ctx, postgres.PromoteProjectionCommand{OperationID: promotionOperationID(200), ToSpace: spaceA}); err != nil {
		t.Fatalf("seed serving promotion: %v", err)
	}
	revisionBefore := mustContextRevision(t, storage, tenantID, userID)
	generationBefore := projectionDeploymentGeneration(t, databaseURL)
	installPromotionReceiptFailureTrigger(t, databaseURL)
	operationID := promotionOperationID(201)
	_, err := storage.PromoteProjection(ctx, postgres.PromoteProjectionCommand{
		OperationID: operationID, ExpectedFrom: spaceA, ToSpace: spaceB,
	})
	if err == nil || strings.Contains(err.Error(), "promotion_receipt_failure_secret") {
		t.Fatalf("faulted promotion error=%v, want redacted failure", err)
	}
	assertCurrentPromotionSpace(t, storage, spaceA, generationBefore)
	assertPromotionTargetState(t, storage, spaceA, postgres.ProjectionTargetServing, true)
	assertPromotionTargetState(t, storage, spaceB, postgres.ProjectionTargetShadow, true)
	assertContextRevision(t, storage, tenantID, userID, revisionBefore)
	if _, err := storage.ProjectionPromotionByOperationID(ctx, operationID); !errors.Is(err, domain.ErrNotFound) {
		t.Fatalf("faulted promotion receipt error=%v, want not found", err)
	}
}

func TestProjectionPromotionEmptyDatasetRequiresExplicitAuthorization(t *testing.T) {
	ctx := context.Background()
	databaseURL, storage := isolatedProjectionPromotionStore(t)
	space := registerPromotionTarget(t, storage, "empty")
	generationBefore := projectionDeploymentGeneration(t, databaseURL)
	if _, err := storage.PromoteProjection(ctx, postgres.PromoteProjectionCommand{
		OperationID: promotionOperationID(250), ToSpace: space,
	}); !errors.Is(err, domain.ErrConflict) {
		t.Fatalf("unauthorized empty promotion error=%v, want conflict", err)
	}
	assertCurrentPromotionSpace(t, storage, "", generationBefore)
	receipt, err := storage.PromoteProjection(ctx, postgres.PromoteProjectionCommand{
		OperationID: promotionOperationID(251), ToSpace: space, AllowEmpty: true,
	})
	if err != nil || receipt.LiveScopeCount != 0 || receipt.LiveCardCount != 0 || receipt.CoveredCardCount != 0 {
		t.Fatalf("authorized empty promotion receipt=%#v error=%v", receipt, err)
	}
}

func TestProjectionPromotionUsesLogicalTargetTimestampWithoutChangingReceiptClock(t *testing.T) {
	ctx := context.Background()
	_, storage := isolatedProjectionPromotionStore(t)
	futureA := time.Now().UTC().Add(24 * time.Hour).Truncate(time.Microsecond)
	futureB := futureA.Add(time.Hour)
	spaceA := uniqueProjectionRepositorySpace("promotion_future_a")
	spaceB := uniqueProjectionRepositorySpace("promotion_future_b")
	if _, err := storage.RegisterProjectionTarget(ctx, projectionRepositoryRegistration(spaceA, postgres.ProjectionTargetShadow, true, futureA)); err != nil {
		t.Fatalf("register future target A: %v", err)
	}
	if _, err := storage.RegisterProjectionTarget(ctx, projectionRepositoryRegistration(spaceB, postgres.ProjectionTargetShadow, true, futureB)); err != nil {
		t.Fatalf("register future target B: %v", err)
	}
	first, err := storage.PromoteProjection(ctx, postgres.PromoteProjectionCommand{
		OperationID: promotionOperationID(260), ToSpace: spaceA, AllowEmpty: true,
	})
	if err != nil {
		t.Fatalf("promote future target A: %v", err)
	}
	second, err := storage.PromoteProjection(ctx, postgres.PromoteProjectionCommand{
		OperationID: promotionOperationID(261), ExpectedFrom: spaceA, ToSpace: spaceB, AllowEmpty: true,
	})
	if err != nil {
		t.Fatalf("promote future target B: %v", err)
	}
	if !first.PromotedAt.Before(futureA) || !second.PromotedAt.Before(futureA) {
		t.Fatalf("receipt DB clocks were lifted into future: first=%s second=%s future=%s", first.PromotedAt, second.PromotedAt, futureA)
	}
	targetA, err := storage.ProjectionTargetBySpace(ctx, spaceA)
	if err != nil {
		t.Fatalf("load future target A: %v", err)
	}
	targetB, err := storage.ProjectionTargetBySpace(ctx, spaceB)
	if err != nil {
		t.Fatalf("load future target B: %v", err)
	}
	wantTransition := futureB.Add(time.Microsecond)
	if !targetA.UpdatedAt.Equal(wantTransition) || !targetB.UpdatedAt.Equal(wantTransition) {
		t.Fatalf("logical transition timestamps A=%s B=%s, want %s", targetA.UpdatedAt, targetB.UpdatedAt, wantTransition)
	}
}

func TestConcurrentProjectionPromotionsFromSameGenerationHaveOneWinner(t *testing.T) {
	ctx := context.Background()
	databaseURL, storage := isolatedProjectionPromotionStore(t)
	spaceA := registerPromotionTarget(t, storage, "race_a")
	spaceB := registerPromotionTarget(t, storage, "race_b")
	spaceC := registerPromotionTarget(t, storage, "race_c")
	tenantID, userID := uniqueScope("promotion-race")
	card := approveVectorCard(t, storage, tenantID, userID, "promotion-race", "key", "value", 300, 302, nil)
	for _, space := range []string{spaceA, spaceB, spaceC} {
		coverPromotionCard(t, storage, databaseURL, card, space)
	}
	first, err := storage.PromoteProjection(ctx, postgres.PromoteProjectionCommand{OperationID: promotionOperationID(300), ToSpace: spaceA})
	if err != nil {
		t.Fatalf("seed race serving target: %v", err)
	}

	type result struct {
		receipt postgres.ProjectionPromotionReceipt
		err     error
	}
	start := make(chan struct{})
	results := make(chan result, 2)
	var group sync.WaitGroup
	for index, destination := range []string{spaceB, spaceC} {
		index, destination := index, destination
		group.Add(1)
		go func() {
			defer group.Done()
			<-start
			receipt, promoteErr := storage.PromoteProjection(ctx, postgres.PromoteProjectionCommand{
				OperationID: promotionOperationID(301 + index), ExpectedFrom: spaceA, ToSpace: destination,
			})
			results <- result{receipt: receipt, err: promoteErr}
		}()
	}
	close(start)
	group.Wait()
	close(results)
	var winner postgres.ProjectionPromotionReceipt
	wins, conflicts := 0, 0
	for result := range results {
		if result.err == nil {
			wins++
			winner = result.receipt
		} else if errors.Is(result.err, domain.ErrConflict) {
			conflicts++
		} else {
			t.Fatalf("unexpected concurrent promotion error: %v", result.err)
		}
	}
	if wins != 1 || conflicts != 1 || winner.Generation != first.Generation+1 {
		t.Fatalf("concurrent results wins=%d conflicts=%d winner=%#v", wins, conflicts, winner)
	}
	assertCurrentPromotionSpace(t, storage, winner.ToSpace, winner.Generation)
}

func TestProjectionPromotionAndApprovalDeploymentGateBothOrders(t *testing.T) {
	t.Run("approval first is included and blocks promotion", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		databaseURL, storage := isolatedProjectionPromotionStore(t)
		space := registerPromotionTarget(t, storage, "approval_first")
		tenantID, userID := uniqueScope("promotion-approval-first")
		event := evidence(tenantID, userID, "event-promotion-approval-first", "private candidate value", 400)
		mustAppend(t, storage, event)
		candidate := candidate(tenantID, userID, "candidate-promotion-approval-first", "key", "value", []string{event.ID}, 401)
		mustCreateCandidate(t, storage, candidate)
		memoryID := "memory-promotion-approval-first"

		advisoryKey := int64(1_810_000_000 + scopeSequence.Add(1)%10_000_000)
		holder, err := pgx.Connect(ctx, databaseURL)
		if err != nil {
			t.Fatalf("connect approval-first advisory holder: %v", err)
		}
		defer holder.Close(context.Background())
		if _, err := holder.Exec(ctx, `SELECT pg_advisory_lock($1)`, advisoryKey); err != nil {
			t.Fatalf("hold approval-first advisory lock: %v", err)
		}
		locked := true
		defer func() {
			if locked {
				_, _ = holder.Exec(context.Background(), `SELECT pg_advisory_unlock($1)`, advisoryKey)
			}
		}()
		installProjectionApprovalAdvisoryTrigger(t, databaseURL, memoryID, advisoryKey)

		approvalResult := make(chan error, 1)
		go func() {
			_, _, reviewErr := storage.ReviewCandidate(ctx, approval(candidate, memoryID, 402))
			approvalResult <- reviewErr
		}()
		waitForProjectionAdvisoryWaiter(t, databaseURL, advisoryKey)

		promotionApplication := "promotion_waits_for_approval"
		promotionStore := openProjectionDeploymentStoreWithApplicationName(t, databaseURL, promotionApplication)
		defer promotionStore.Close()
		promotionResult := make(chan error, 1)
		go func() {
			_, promoteErr := promotionStore.PromoteProjection(ctx, postgres.PromoteProjectionCommand{
				OperationID: promotionOperationID(400), ToSpace: space,
			})
			promotionResult <- promoteErr
		}()
		waitForProjectionDeploymentLock(t, databaseURL, promotionApplication)
		if _, err := holder.Exec(ctx, `SELECT pg_advisory_unlock($1)`, advisoryKey); err != nil {
			t.Fatalf("release approval-first advisory lock: %v", err)
		}
		locked = false
		if err := <-approvalResult; err != nil {
			t.Fatalf("approval-first review: %v", err)
		}
		if err := <-promotionResult; !errors.Is(err, domain.ErrConflict) {
			t.Fatalf("promotion after pending approval error=%v, want coverage conflict", err)
		}
		assertProjectionJobState(t, databaseURL, tenantID, userID, memoryID, space, "pending", 0)
		assertCurrentPromotionSpace(t, storage, "", projectionDeploymentGeneration(t, databaseURL))
	})

	t.Run("promotion first changes complete approval target set", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		databaseURL, storage := isolatedProjectionPromotionStore(t)
		spaceA := registerPromotionTarget(t, storage, "promotion_first_a")
		spaceB := registerPromotionTarget(t, storage, "promotion_first_b")
		first, err := storage.PromoteProjection(ctx, postgres.PromoteProjectionCommand{
			OperationID: promotionOperationID(410), ToSpace: spaceA, AllowEmpty: true,
		})
		if err != nil {
			t.Fatalf("seed empty serving target: %v", err)
		}
		tenantID, userID := uniqueScope("promotion-first-approval")
		event := evidence(tenantID, userID, "event-promotion-first-approval", "value", 410)
		mustAppend(t, storage, event)
		candidate := candidate(tenantID, userID, "candidate-promotion-first-approval", "key", "value", []string{event.ID}, 411)
		mustCreateCandidate(t, storage, candidate)
		memoryID := "memory-promotion-first-approval"

		advisoryKey := int64(1_820_000_000 + scopeSequence.Add(1)%10_000_000)
		holder, err := pgx.Connect(ctx, databaseURL)
		if err != nil {
			t.Fatalf("connect promotion-first advisory holder: %v", err)
		}
		defer holder.Close(context.Background())
		if _, err := holder.Exec(ctx, `SELECT pg_advisory_lock($1)`, advisoryKey); err != nil {
			t.Fatalf("hold promotion-first advisory lock: %v", err)
		}
		locked := true
		defer func() {
			if locked {
				_, _ = holder.Exec(context.Background(), `SELECT pg_advisory_unlock($1)`, advisoryKey)
			}
		}()
		installPromotionTargetAdvisoryTrigger(t, databaseURL, spaceB, advisoryKey)

		promoted := make(chan struct {
			receipt postgres.ProjectionPromotionReceipt
			err     error
		}, 1)
		go func() {
			receipt, promoteErr := storage.PromoteProjection(ctx, postgres.PromoteProjectionCommand{
				OperationID: promotionOperationID(411), ExpectedFrom: spaceA, ToSpace: spaceB, AllowEmpty: true,
			})
			promoted <- struct {
				receipt postgres.ProjectionPromotionReceipt
				err     error
			}{receipt: receipt, err: promoteErr}
		}()
		waitForProjectionAdvisoryWaiter(t, databaseURL, advisoryKey)

		approvalApplication := "approval_waits_for_promotion"
		approvalStore := openProjectionDeploymentStoreWithApplicationName(t, databaseURL, approvalApplication)
		defer approvalStore.Close()
		approved := make(chan error, 1)
		go func() {
			_, _, reviewErr := approvalStore.ReviewCandidate(ctx, approval(candidate, memoryID, 412))
			approved <- reviewErr
		}()
		waitForProjectionDeploymentLock(t, databaseURL, approvalApplication)
		if _, err := holder.Exec(ctx, `SELECT pg_advisory_unlock($1)`, advisoryKey); err != nil {
			t.Fatalf("release promotion-first advisory lock: %v", err)
		}
		locked = false
		promotionResult := <-promoted
		if promotionResult.err != nil || promotionResult.receipt.Generation != first.Generation+1 {
			t.Fatalf("promotion-first result=%#v error=%v", promotionResult.receipt, promotionResult.err)
		}
		if err := <-approved; err != nil {
			t.Fatalf("approval after promotion: %v", err)
		}
		assertProjectionJobState(t, databaseURL, tenantID, userID, memoryID, spaceA, "pending", 0)
		assertProjectionJobState(t, databaseURL, tenantID, userID, memoryID, spaceB, "pending", 0)
		assertPromotionTargetState(t, storage, spaceA, postgres.ProjectionTargetShadow, true)
		assertPromotionTargetState(t, storage, spaceB, postgres.ProjectionTargetServing, true)
	})
}

func TestProjectionPromotionAndForgetScopeGateBothOrders(t *testing.T) {
	t.Run("forget first removes coverage before cutoff", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		databaseURL, storage := isolatedProjectionPromotionStore(t)
		space := registerPromotionTarget(t, storage, "forget_first")
		tenantID, userID := uniqueScope("promotion-forget-first")
		card := approveVectorCard(t, storage, tenantID, userID, "promotion-forget-first", "key", "value", 500, 502, nil)
		coverPromotionCard(t, storage, databaseURL, card, space)
		revisionBefore := mustContextRevision(t, storage, tenantID, userID)

		advisoryKey := int64(1_830_000_000 + scopeSequence.Add(1)%10_000_000)
		holder := holdPromotionAdvisoryLock(t, ctx, databaseURL, advisoryKey)
		locked := true
		defer func() {
			if locked {
				_, _ = holder.Exec(context.Background(), `SELECT pg_advisory_unlock($1)`, advisoryKey)
			}
			holder.Close(context.Background())
		}()
		installPromotionCardDeleteAdvisoryTrigger(t, databaseURL, card.ID, advisoryKey)
		forgotten := make(chan error, 1)
		go func() {
			_, forgetErr := storage.ForgetUser(ctx, tenantID, userID, time.Now().UTC())
			forgotten <- forgetErr
		}()
		waitForProjectionAdvisoryWaiter(t, databaseURL, advisoryKey)

		application := "promotion_waits_for_forget"
		promotionStore := openProjectionDeploymentStoreWithApplicationName(t, databaseURL, application)
		defer promotionStore.Close()
		promoted := make(chan struct {
			receipt postgres.ProjectionPromotionReceipt
			err     error
		}, 1)
		go func() {
			receipt, promoteErr := promotionStore.PromoteProjection(ctx, postgres.PromoteProjectionCommand{
				OperationID: promotionOperationID(500), ToSpace: space, AllowEmpty: true,
			})
			promoted <- struct {
				receipt postgres.ProjectionPromotionReceipt
				err     error
			}{receipt, promoteErr}
		}()
		waitForProjectionApplicationLock(t, databaseURL, application)
		if _, err := holder.Exec(ctx, `SELECT pg_advisory_unlock($1)`, advisoryKey); err != nil {
			t.Fatalf("release forget-first advisory lock: %v", err)
		}
		locked = false
		if err := <-forgotten; err != nil {
			t.Fatalf("forget-first deletion: %v", err)
		}
		result := <-promoted
		if result.err != nil || result.receipt.LiveScopeCount != 0 || result.receipt.LiveCardCount != 0 {
			t.Fatalf("promotion after forget receipt=%#v error=%v", result.receipt, result.err)
		}
		assertContextRevision(t, storage, tenantID, userID, revisionBefore+1)
		assertNoPromotionRowsForScope(t, databaseURL, tenantID, userID)
	})

	t.Run("promotion first holds coverage until forget propagates", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		databaseURL, storage := isolatedProjectionPromotionStore(t)
		space := registerPromotionTarget(t, storage, "promotion_first_forget")
		tenantID, userID := uniqueScope("promotion-first-forget")
		card := approveVectorCard(t, storage, tenantID, userID, "promotion-first-forget", "key", "value", 510, 512, nil)
		coverPromotionCard(t, storage, databaseURL, card, space)
		revisionBefore := mustContextRevision(t, storage, tenantID, userID)

		advisoryKey := int64(1_840_000_000 + scopeSequence.Add(1)%10_000_000)
		holder := holdPromotionAdvisoryLock(t, ctx, databaseURL, advisoryKey)
		locked := true
		defer func() {
			if locked {
				_, _ = holder.Exec(context.Background(), `SELECT pg_advisory_unlock($1)`, advisoryKey)
			}
			holder.Close(context.Background())
		}()
		installPromotionTargetAdvisoryTrigger(t, databaseURL, space, advisoryKey)
		promoted := make(chan struct {
			receipt postgres.ProjectionPromotionReceipt
			err     error
		}, 1)
		go func() {
			receipt, promoteErr := storage.PromoteProjection(ctx, postgres.PromoteProjectionCommand{
				OperationID: promotionOperationID(510), ToSpace: space,
			})
			promoted <- struct {
				receipt postgres.ProjectionPromotionReceipt
				err     error
			}{receipt, promoteErr}
		}()
		waitForProjectionAdvisoryWaiter(t, databaseURL, advisoryKey)

		application := "forget_waits_for_promotion"
		forgetStore := openProjectionDeploymentStoreWithApplicationName(t, databaseURL, application)
		defer forgetStore.Close()
		forgotten := make(chan error, 1)
		go func() {
			_, forgetErr := forgetStore.ForgetUser(ctx, tenantID, userID, time.Now().UTC())
			forgotten <- forgetErr
		}()
		waitForProjectionApplicationLock(t, databaseURL, application)
		if _, err := holder.Exec(ctx, `SELECT pg_advisory_unlock($1)`, advisoryKey); err != nil {
			t.Fatalf("release promotion-first-forget advisory lock: %v", err)
		}
		locked = false
		result := <-promoted
		if result.err != nil || result.receipt.LiveScopeCount != 1 || result.receipt.LiveCardCount != 1 {
			t.Fatalf("promotion before forget receipt=%#v error=%v", result.receipt, result.err)
		}
		if err := <-forgotten; err != nil {
			t.Fatalf("forget after promotion: %v", err)
		}
		assertContextRevision(t, storage, tenantID, userID, revisionBefore+2)
		assertNoPromotionRowsForScope(t, databaseURL, tenantID, userID)
	})
}

func TestProjectionPromotionSwapIsNeverExternallyZeroOrDoubleServing(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	databaseURL, storage := isolatedProjectionPromotionStore(t)
	spaceA := registerPromotionTarget(t, storage, "visibility_a")
	spaceB := registerPromotionTarget(t, storage, "visibility_b")
	tenantID, userID := uniqueScope("promotion-visibility")
	card := approveVectorCard(t, storage, tenantID, userID, "promotion-visibility", "key", "value", 520, 522, nil)
	coverPromotionCard(t, storage, databaseURL, card, spaceA)
	coverPromotionCard(t, storage, databaseURL, card, spaceB)
	if _, err := storage.PromoteProjection(ctx, postgres.PromoteProjectionCommand{OperationID: promotionOperationID(520), ToSpace: spaceA}); err != nil {
		t.Fatalf("seed visible serving target: %v", err)
	}

	advisoryKey := int64(1_850_000_000 + scopeSequence.Add(1)%10_000_000)
	holder := holdPromotionAdvisoryLock(t, ctx, databaseURL, advisoryKey)
	locked := true
	defer func() {
		if locked {
			_, _ = holder.Exec(context.Background(), `SELECT pg_advisory_unlock($1)`, advisoryKey)
		}
		holder.Close(context.Background())
	}()
	installPromotionDemotionAdvisoryTrigger(t, databaseURL, spaceA, advisoryKey)
	result := make(chan error, 1)
	go func() {
		_, promoteErr := storage.PromoteProjection(ctx, postgres.PromoteProjectionCommand{
			OperationID: promotionOperationID(521), ExpectedFrom: spaceA, ToSpace: spaceB,
		})
		result <- promoteErr
	}()
	waitForProjectionAdvisoryWaiter(t, databaseURL, advisoryKey)
	if got := servingSpacesSnapshot(t, databaseURL); len(got) != 1 || got[0] != spaceA {
		t.Fatalf("mid-swap serving spaces=%v, want only old space", got)
	}
	if _, err := holder.Exec(ctx, `SELECT pg_advisory_unlock($1)`, advisoryKey); err != nil {
		t.Fatalf("release demotion midpoint advisory lock: %v", err)
	}
	locked = false
	if err := <-result; err != nil {
		t.Fatalf("finish visible serving swap: %v", err)
	}
	if got := servingSpacesSnapshot(t, databaseURL); len(got) != 1 || got[0] != spaceB {
		t.Fatalf("committed serving spaces=%v, want only new space", got)
	}
}

func TestProjectionPromotionAndWorkerScopeGateBothOrders(t *testing.T) {
	t.Run("worker first completes shadow coverage before promotion", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		databaseURL, storage := isolatedProjectionPromotionStore(t)
		space := registerPromotionTarget(t, storage, "worker_first")
		tenantID, userID := uniqueScope("promotion-worker-first")
		card := approveVectorCard(t, storage, tenantID, userID, "promotion-worker-first", "key", "value", 540, 542, nil)
		revisionBefore := mustContextRevision(t, storage, tenantID, userID)
		items, err := storage.ClaimProjectionJobs(ctx, postgres.ClaimProjectionJobsCommand{
			EmbeddingSpace: space, LeaseOwner: "promotion-worker-first", LeaseDuration: time.Minute, Limit: 1,
		})
		if err != nil || len(items) != 1 {
			t.Fatalf("claim worker-first item=%#v error=%v", items, err)
		}
		item := items[0]
		advisoryKey := int64(1_860_000_000 + scopeSequence.Add(1)%10_000_000)
		holder := holdPromotionAdvisoryLock(t, ctx, databaseURL, advisoryKey)
		locked := true
		defer func() {
			if locked {
				_, _ = holder.Exec(context.Background(), `SELECT pg_advisory_unlock($1)`, advisoryKey)
			}
			holder.Close(context.Background())
		}()
		installProjectionFinalizeAdvisoryTrigger(t, databaseURL, card.ID, advisoryKey)
		finalized := make(chan error, 1)
		go func() {
			_, finalizeErr := storage.FinalizeProjectionJob(ctx, postgres.FinalizeProjectionJobCommand{
				JobID: item.Job.ID, TenantID: tenantID, UserID: userID, EmbeddingSpace: space,
				LeaseOwner: item.Job.LeaseOwner, LeaseVersion: item.Job.LeaseVersion,
				DocumentSHA256: item.DocumentSHA256, Vector: projectionWorkerVector(1),
			})
			finalized <- finalizeErr
		}()
		waitForProjectionAdvisoryWaiter(t, databaseURL, advisoryKey)
		application := "promotion_waits_for_worker"
		promotionStore := openProjectionDeploymentStoreWithApplicationName(t, databaseURL, application)
		defer promotionStore.Close()
		promoted := make(chan struct {
			receipt postgres.ProjectionPromotionReceipt
			err     error
		}, 1)
		go func() {
			receipt, promoteErr := promotionStore.PromoteProjection(ctx, postgres.PromoteProjectionCommand{
				OperationID: promotionOperationID(540), ToSpace: space,
			})
			promoted <- struct {
				receipt postgres.ProjectionPromotionReceipt
				err     error
			}{receipt, promoteErr}
		}()
		waitForProjectionApplicationLock(t, databaseURL, application)
		if _, err := holder.Exec(ctx, `SELECT pg_advisory_unlock($1)`, advisoryKey); err != nil {
			t.Fatalf("release worker-first advisory lock: %v", err)
		}
		locked = false
		if err := <-finalized; err != nil {
			t.Fatalf("worker-first finalization: %v", err)
		}
		result := <-promoted
		if result.err != nil || result.receipt.LiveCardCount != 1 || result.receipt.CoveredCardCount != 1 {
			t.Fatalf("promotion after worker receipt=%#v error=%v", result.receipt, result.err)
		}
		assertContextRevision(t, storage, tenantID, userID, revisionBefore+1)
		assertProjectionJobState(t, databaseURL, tenantID, userID, card.ID, space, "succeeded", 1)
	})

	t.Run("promotion first demotes worker target before finalization", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		databaseURL, storage := isolatedProjectionPromotionStore(t)
		spaceA := registerPromotionTarget(t, storage, "promotion_first_worker_a")
		spaceB := registerPromotionTarget(t, storage, "promotion_first_worker_b")
		tenantID, userID := uniqueScope("promotion-first-worker")
		card := approveVectorCard(t, storage, tenantID, userID, "promotion-first-worker", "key", "value", 550, 552, nil)
		coverPromotionCard(t, storage, databaseURL, card, spaceA)
		coverPromotionCard(t, storage, databaseURL, card, spaceB)
		first, err := storage.PromoteProjection(ctx, postgres.PromoteProjectionCommand{OperationID: promotionOperationID(550), ToSpace: spaceA})
		if err != nil {
			t.Fatalf("seed worker serving target: %v", err)
		}
		execPromotionSQL(t, databaseURL, `DELETE FROM agent_memory.memory_embeddings WHERE tenant_id=$1 AND user_id=$2 AND memory_id=$3 AND embedding_space=$4`, tenantID, userID, card.ID, spaceA)
		execPromotionSQL(t, databaseURL, `UPDATE agent_memory.embedding_projection_jobs SET state='pending', attempt_count=0, lease_owner=NULL, lease_until=NULL, last_error_code=NULL, last_error_at=NULL, completed_at=NULL, updated_at=clock_timestamp(), available_at=clock_timestamp() WHERE tenant_id=$1 AND user_id=$2 AND memory_id=$3 AND embedding_space=$4`, tenantID, userID, card.ID, spaceA)
		items, err := storage.ClaimProjectionJobs(ctx, postgres.ClaimProjectionJobsCommand{
			EmbeddingSpace: spaceA, LeaseOwner: "promotion-first-worker", LeaseDuration: time.Minute, Limit: 1,
		})
		if err != nil || len(items) != 1 {
			t.Fatalf("claim promotion-first worker item=%#v error=%v", items, err)
		}
		item := items[0]
		revisionBefore := mustContextRevision(t, storage, tenantID, userID)

		advisoryKey := int64(1_870_000_000 + scopeSequence.Add(1)%10_000_000)
		holder := holdPromotionAdvisoryLock(t, ctx, databaseURL, advisoryKey)
		locked := true
		defer func() {
			if locked {
				_, _ = holder.Exec(context.Background(), `SELECT pg_advisory_unlock($1)`, advisoryKey)
			}
			holder.Close(context.Background())
		}()
		installPromotionTargetAdvisoryTrigger(t, databaseURL, spaceB, advisoryKey)
		promoted := make(chan struct {
			receipt postgres.ProjectionPromotionReceipt
			err     error
		}, 1)
		go func() {
			receipt, promoteErr := storage.PromoteProjection(ctx, postgres.PromoteProjectionCommand{
				OperationID: promotionOperationID(551), ExpectedFrom: spaceA, ToSpace: spaceB,
			})
			promoted <- struct {
				receipt postgres.ProjectionPromotionReceipt
				err     error
			}{receipt, promoteErr}
		}()
		waitForProjectionAdvisoryWaiter(t, databaseURL, advisoryKey)

		application := "worker_waits_for_promotion"
		workerStore := openProjectionDeploymentStoreWithApplicationName(t, databaseURL, application)
		defer workerStore.Close()
		finalized := make(chan postgres.FinalizeProjectionJobResult, 1)
		finalizeErrors := make(chan error, 1)
		go func() {
			result, finalizeErr := workerStore.FinalizeProjectionJob(ctx, postgres.FinalizeProjectionJobCommand{
				JobID: item.Job.ID, TenantID: tenantID, UserID: userID, EmbeddingSpace: spaceA,
				LeaseOwner: item.Job.LeaseOwner, LeaseVersion: item.Job.LeaseVersion,
				DocumentSHA256: item.DocumentSHA256, Vector: projectionWorkerVector(2),
			})
			finalized <- result
			finalizeErrors <- finalizeErr
		}()
		waitForProjectionApplicationLock(t, databaseURL, application)
		if _, err := holder.Exec(ctx, `SELECT pg_advisory_unlock($1)`, advisoryKey); err != nil {
			t.Fatalf("release promotion-first-worker advisory lock: %v", err)
		}
		locked = false
		promotionResult := <-promoted
		if promotionResult.err != nil || promotionResult.receipt.Generation != first.Generation+1 {
			t.Fatalf("promotion-first worker receipt=%#v error=%v", promotionResult.receipt, promotionResult.err)
		}
		finalizeResult, finalizeErr := <-finalized, <-finalizeErrors
		if finalizeErr != nil || finalizeResult.RevisionAdvanced {
			t.Fatalf("worker after demotion result=%#v error=%v", finalizeResult, finalizeErr)
		}
		assertContextRevision(t, storage, tenantID, userID, revisionBefore+1)
		assertProjectionJobState(t, databaseURL, tenantID, userID, card.ID, spaceA, "succeeded", 1)
		assertPromotionTargetState(t, storage, spaceA, postgres.ProjectionTargetShadow, true)
		assertPromotionTargetState(t, storage, spaceB, postgres.ProjectionTargetServing, true)
	})
}

func TestConcurrentExactProjectionPromotionRetryMutatesOnce(t *testing.T) {
	ctx := context.Background()
	databaseURL, storage := isolatedProjectionPromotionStore(t)
	space := registerPromotionTarget(t, storage, "exact_retry")
	command := postgres.PromoteProjectionCommand{OperationID: promotionOperationID(530), ToSpace: space, AllowEmpty: true}
	generationBefore := projectionDeploymentGeneration(t, databaseURL)
	start := make(chan struct{})
	results := make(chan struct {
		receipt postgres.ProjectionPromotionReceipt
		err     error
	}, 2)
	for range 2 {
		go func() {
			<-start
			receipt, err := storage.PromoteProjection(ctx, command)
			results <- struct {
				receipt postgres.ProjectionPromotionReceipt
				err     error
			}{receipt, err}
		}()
	}
	close(start)
	first, second := <-results, <-results
	if first.err != nil || second.err != nil || first.receipt != second.receipt {
		t.Fatalf("exact concurrent retries first=%#v/%v second=%#v/%v", first.receipt, first.err, second.receipt, second.err)
	}
	if first.receipt.Generation != generationBefore+1 || countPromotionReceipts(t, databaseURL, command.OperationID) != 1 {
		t.Fatalf("exact retry receipt=%#v count=%d", first.receipt, countPromotionReceipts(t, databaseURL, command.OperationID))
	}
}

func isolatedProjectionPromotionStore(t *testing.T) (string, *postgres.Store) {
	t.Helper()
	baseURL := strings.TrimSpace(os.Getenv("TEST_DATABASE_URL"))
	if baseURL == "" {
		t.Fatal("TEST_DATABASE_URL is required for integration tests")
	}
	baseConfig, err := pgx.ParseConfig(baseURL)
	if err != nil {
		t.Fatalf("parse promotion TEST_DATABASE_URL: %v", err)
	}
	databaseName := fmt.Sprintf("agent_memory_promotion_%d_%d", time.Now().UnixNano(), promotionDatabaseSequence.Add(1))
	adminConfig := baseConfig.Copy()
	adminConfig.Database = "postgres"
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	admin, err := pgx.ConnectConfig(ctx, adminConfig)
	if err != nil {
		t.Fatalf("connect promotion maintenance database: %v", err)
	}
	quotedDatabase := pgx.Identifier{databaseName}.Sanitize()
	if _, err := admin.Exec(ctx, "CREATE DATABASE "+quotedDatabase); err != nil {
		_ = admin.Close(context.Background())
		t.Fatalf("create promotion database: %v", err)
	}
	if err := admin.Close(context.Background()); err != nil {
		t.Fatalf("close promotion maintenance database: %v", err)
	}
	parsedURL, err := url.Parse(baseURL)
	if err != nil || (parsedURL.Scheme != "postgres" && parsedURL.Scheme != "postgresql") {
		t.Fatalf("TEST_DATABASE_URL must be a PostgreSQL URL: %v", err)
	}
	parsedURL.Path = "/" + databaseName
	parsedURL.RawPath = ""
	databaseURL := parsedURL.String()
	if err := migrations.Apply(ctx, databaseURL); err != nil {
		t.Fatalf("apply isolated promotion migrations: %v", err)
	}
	storage, err := postgres.Open(ctx, databaseURL)
	if err != nil {
		t.Fatalf("open isolated promotion store: %v", err)
	}
	t.Cleanup(func() {
		storage.Close()
		dropCtx, dropCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer dropCancel()
		connection, connectErr := pgx.ConnectConfig(dropCtx, adminConfig)
		if connectErr != nil {
			t.Errorf("reconnect to drop promotion database: %v", connectErr)
			return
		}
		defer connection.Close(context.Background())
		if _, dropErr := connection.Exec(dropCtx, "DROP DATABASE IF EXISTS "+quotedDatabase+" WITH (FORCE)"); dropErr != nil {
			t.Errorf("drop promotion database: %v", dropErr)
		}
	})
	return databaseURL, storage
}

func registerPromotionTarget(t *testing.T, storage *postgres.Store, label string) string {
	t.Helper()
	space := uniqueProjectionRepositorySpace("promotion_" + label)
	if _, err := storage.RegisterProjectionTarget(context.Background(), projectionRepositoryRegistration(
		space, postgres.ProjectionTargetShadow, true, time.Now().UTC().Truncate(time.Microsecond),
	)); err != nil {
		t.Fatalf("register promotion target: %v", err)
	}
	return space
}

func coverPromotionCard(t *testing.T, storage *postgres.Store, databaseURL string, card domain.MemoryCard, space string) {
	t.Helper()
	target, err := storage.ProjectionTargetBySpace(context.Background(), space)
	if err != nil {
		t.Fatalf("load promotion coverage target: %v", err)
	}
	if err := storage.UpsertMemoryEmbedding(context.Background(), reconciliationEmbedding(
		card, target.Space, embedding.MemoryCardDocumentV1SHA256(card),
	)); err != nil {
		t.Fatalf("upsert promotion coverage embedding: %v", err)
	}
	markProjectionJobState(t, databaseURL, card.TenantID, card.UserID, card.ID, space, "succeeded")
}

func setPromotionEmbeddingHash(t *testing.T, databaseURL string, card domain.MemoryCard, space, contentSHA256 string) {
	t.Helper()
	execPromotionSQL(t, databaseURL, `
		UPDATE agent_memory.memory_embeddings
		SET content_sha256 = $5
		WHERE tenant_id=$1 AND user_id=$2 AND memory_id=$3 AND embedding_space=$4`,
		card.TenantID, card.UserID, card.ID, space, contentSHA256)
}

func execPromotionSQL(t *testing.T, databaseURL, statement string, arguments ...any) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	conn, err := pgx.Connect(ctx, databaseURL)
	if err != nil {
		t.Fatalf("connect for promotion fixture: %v", err)
	}
	defer conn.Close(context.Background())
	if _, err := conn.Exec(ctx, statement, arguments...); err != nil {
		t.Fatalf("execute promotion fixture: %v", err)
	}
}

func promotionOperationID(value int) string {
	return fmt.Sprintf("00000000-0000-4000-8000-%012d", value)
}

func mustContextRevision(t *testing.T, storage *postgres.Store, tenantID, userID string) uint64 {
	t.Helper()
	revision, err := storage.ContextRevision(context.Background(), tenantID, userID)
	if err != nil {
		t.Fatalf("load promotion context revision: %v", err)
	}
	return revision
}

func assertContextRevision(t *testing.T, storage *postgres.Store, tenantID, userID string, want uint64) {
	t.Helper()
	if got := mustContextRevision(t, storage, tenantID, userID); got != want {
		t.Fatalf("context revision=%d, want %d", got, want)
	}
}

func assertPromotionTargetState(t *testing.T, storage *postgres.Store, space string, state postgres.ProjectionTargetState, enqueueNew bool) {
	t.Helper()
	target, err := storage.ProjectionTargetBySpace(context.Background(), space)
	if err != nil || target.State != state || target.EnqueueNew != enqueueNew {
		t.Fatalf("promotion target=%#v error=%v, want state=%s enqueue=%t", target, err, state, enqueueNew)
	}
}

func assertCurrentPromotionSpace(t *testing.T, storage *postgres.Store, wantSpace string, wantGeneration int64) {
	t.Helper()
	current, err := storage.CurrentServingProjection(context.Background())
	if err != nil || current.Generation != wantGeneration {
		t.Fatalf("current serving=%#v error=%v, want generation=%d", current, err, wantGeneration)
	}
	if wantSpace == "" {
		if current.Target != nil {
			t.Fatalf("current target=%#v, want nil", current.Target)
		}
		return
	}
	if current.Target == nil || current.Target.Space.ID != wantSpace || current.Target.State != postgres.ProjectionTargetServing {
		t.Fatalf("current target=%#v, want serving %s", current.Target, wantSpace)
	}
}

func installPromotionReceiptFailureTrigger(t *testing.T, databaseURL string) {
	t.Helper()
	const functionName = "test_projection_promotion_receipt_failure"
	const triggerName = "test_projection_promotion_receipt_failure"
	execPromotionSQL(t, databaseURL, fmt.Sprintf(`
		CREATE FUNCTION agent_memory.%s() RETURNS trigger LANGUAGE plpgsql AS $body$
		BEGIN
			RAISE EXCEPTION 'promotion_receipt_failure_secret';
		END
		$body$;
		CREATE TRIGGER %s BEFORE INSERT ON agent_memory.embedding_projection_promotions
		FOR EACH ROW EXECUTE FUNCTION agent_memory.%s()`, functionName, triggerName, functionName))
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		conn, err := pgx.Connect(ctx, databaseURL)
		if err != nil {
			t.Errorf("connect to drop promotion failure trigger: %v", err)
			return
		}
		defer conn.Close(context.Background())
		_, _ = conn.Exec(ctx, "DROP TRIGGER IF EXISTS "+triggerName+" ON agent_memory.embedding_projection_promotions")
		_, _ = conn.Exec(ctx, "DROP FUNCTION IF EXISTS agent_memory."+functionName+"()")
	})
}

func installPromotionTargetAdvisoryTrigger(t *testing.T, databaseURL, destination string, advisoryKey int64) {
	t.Helper()
	sequence := scopeSequence.Add(1)
	functionName := fmt.Sprintf("test_projection_promotion_target_wait_%d", sequence)
	triggerName := fmt.Sprintf("test_projection_promotion_target_wait_%d", sequence)
	qualifiedFunction := pgx.Identifier{"agent_memory", functionName}.Sanitize()
	quotedTrigger := pgx.Identifier{triggerName}.Sanitize()
	functionSQL := fmt.Sprintf(`
		CREATE FUNCTION %s() RETURNS trigger LANGUAGE plpgsql AS $body$
		BEGIN
			IF NEW.embedding_space = %s AND NEW.state = 'serving' THEN
				PERFORM pg_advisory_xact_lock(%d);
			END IF;
			RETURN NEW;
		END
		$body$`, qualifiedFunction, postgresTestLiteral(destination), advisoryKey)
	execPromotionSQL(t, databaseURL, functionSQL)
	execPromotionSQL(t, databaseURL, fmt.Sprintf(`
		CREATE TRIGGER %s BEFORE UPDATE ON agent_memory.embedding_projection_targets
		FOR EACH ROW EXECUTE FUNCTION %s()`, quotedTrigger, qualifiedFunction))
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		conn, err := pgx.Connect(ctx, databaseURL)
		if err != nil {
			t.Errorf("connect to drop promotion target trigger: %v", err)
			return
		}
		defer conn.Close(context.Background())
		_, _ = conn.Exec(ctx, fmt.Sprintf("DROP TRIGGER IF EXISTS %s ON agent_memory.embedding_projection_targets", quotedTrigger))
		_, _ = conn.Exec(ctx, "DROP FUNCTION IF EXISTS "+qualifiedFunction+"()")
	})
}

func holdPromotionAdvisoryLock(t *testing.T, ctx context.Context, databaseURL string, advisoryKey int64) *pgx.Conn {
	t.Helper()
	conn, err := pgx.Connect(ctx, databaseURL)
	if err != nil {
		t.Fatalf("connect promotion advisory holder: %v", err)
	}
	if _, err := conn.Exec(ctx, `SELECT pg_advisory_lock($1)`, advisoryKey); err != nil {
		_ = conn.Close(context.Background())
		t.Fatalf("hold promotion advisory lock: %v", err)
	}
	return conn
}

func installPromotionCardDeleteAdvisoryTrigger(t *testing.T, databaseURL, memoryID string, advisoryKey int64) {
	t.Helper()
	sequence := scopeSequence.Add(1)
	functionName := fmt.Sprintf("test_projection_promotion_delete_wait_%d", sequence)
	triggerName := fmt.Sprintf("test_projection_promotion_delete_wait_%d", sequence)
	qualifiedFunction := pgx.Identifier{"agent_memory", functionName}.Sanitize()
	quotedTrigger := pgx.Identifier{triggerName}.Sanitize()
	execPromotionSQL(t, databaseURL, fmt.Sprintf(`
		CREATE FUNCTION %s() RETURNS trigger LANGUAGE plpgsql AS $body$
		BEGIN
			IF OLD.id = %s THEN
				PERFORM pg_advisory_xact_lock(%d);
			END IF;
			RETURN OLD;
		END
		$body$`, qualifiedFunction, postgresTestLiteral(memoryID), advisoryKey))
	execPromotionSQL(t, databaseURL, fmt.Sprintf(`
		CREATE TRIGGER %s BEFORE DELETE ON agent_memory.memory_cards
		FOR EACH ROW EXECUTE FUNCTION %s()`, quotedTrigger, qualifiedFunction))
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		conn, err := pgx.Connect(ctx, databaseURL)
		if err != nil {
			t.Errorf("connect to drop promotion delete trigger: %v", err)
			return
		}
		defer conn.Close(context.Background())
		_, _ = conn.Exec(ctx, fmt.Sprintf("DROP TRIGGER IF EXISTS %s ON agent_memory.memory_cards", quotedTrigger))
		_, _ = conn.Exec(ctx, "DROP FUNCTION IF EXISTS "+qualifiedFunction+"()")
	})
}

func installPromotionDemotionAdvisoryTrigger(t *testing.T, databaseURL, source string, advisoryKey int64) {
	t.Helper()
	sequence := scopeSequence.Add(1)
	functionName := fmt.Sprintf("test_projection_promotion_demote_wait_%d", sequence)
	triggerName := fmt.Sprintf("test_projection_promotion_demote_wait_%d", sequence)
	qualifiedFunction := pgx.Identifier{"agent_memory", functionName}.Sanitize()
	quotedTrigger := pgx.Identifier{triggerName}.Sanitize()
	execPromotionSQL(t, databaseURL, fmt.Sprintf(`
		CREATE FUNCTION %s() RETURNS trigger LANGUAGE plpgsql AS $body$
		BEGIN
			IF OLD.embedding_space = %s AND OLD.state = 'serving' AND NEW.state = 'shadow' THEN
				PERFORM pg_advisory_xact_lock(%d);
			END IF;
			RETURN NEW;
		END
		$body$`, qualifiedFunction, postgresTestLiteral(source), advisoryKey))
	execPromotionSQL(t, databaseURL, fmt.Sprintf(`
		CREATE TRIGGER %s AFTER UPDATE ON agent_memory.embedding_projection_targets
		FOR EACH ROW EXECUTE FUNCTION %s()`, quotedTrigger, qualifiedFunction))
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		conn, err := pgx.Connect(ctx, databaseURL)
		if err != nil {
			t.Errorf("connect to drop promotion demotion trigger: %v", err)
			return
		}
		defer conn.Close(context.Background())
		_, _ = conn.Exec(ctx, fmt.Sprintf("DROP TRIGGER IF EXISTS %s ON agent_memory.embedding_projection_targets", quotedTrigger))
		_, _ = conn.Exec(ctx, "DROP FUNCTION IF EXISTS "+qualifiedFunction+"()")
	})
}

func servingSpacesSnapshot(t *testing.T, databaseURL string) []string {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	conn, err := pgx.Connect(ctx, databaseURL)
	if err != nil {
		t.Fatalf("connect for serving snapshot: %v", err)
	}
	defer conn.Close(context.Background())
	var spaces []string
	if err := conn.QueryRow(ctx, `
		SELECT COALESCE(array_agg(embedding_space ORDER BY embedding_space), ARRAY[]::text[])
		FROM agent_memory.embedding_projection_targets
		WHERE state = 'serving'`).Scan(&spaces); err != nil {
		t.Fatalf("read serving snapshot: %v", err)
	}
	return spaces
}

func assertNoPromotionRowsForScope(t *testing.T, databaseURL, tenantID, userID string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	conn, err := pgx.Connect(ctx, databaseURL)
	if err != nil {
		t.Fatalf("connect to inspect deleted promotion scope: %v", err)
	}
	defer conn.Close(context.Background())
	var cards, jobs, embeddings int
	if err := conn.QueryRow(ctx, `
		SELECT
			(SELECT count(*) FROM agent_memory.memory_cards WHERE tenant_id=$1 AND user_id=$2),
			(SELECT count(*) FROM agent_memory.embedding_projection_jobs WHERE tenant_id=$1 AND user_id=$2),
			(SELECT count(*) FROM agent_memory.memory_embeddings WHERE tenant_id=$1 AND user_id=$2)`,
		tenantID, userID).Scan(&cards, &jobs, &embeddings); err != nil {
		t.Fatalf("inspect deleted promotion scope: %v", err)
	}
	if cards != 0 || jobs != 0 || embeddings != 0 {
		t.Fatalf("deleted promotion scope cards=%d jobs=%d embeddings=%d", cards, jobs, embeddings)
	}
}

func countPromotionReceipts(t *testing.T, databaseURL, operationID string) int {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	conn, err := pgx.Connect(ctx, databaseURL)
	if err != nil {
		t.Fatalf("connect to count promotion receipts: %v", err)
	}
	defer conn.Close(context.Background())
	var count int
	if err := conn.QueryRow(ctx, `
		SELECT count(*) FROM agent_memory.embedding_projection_promotions WHERE operation_id=$1`, operationID).Scan(&count); err != nil {
		t.Fatalf("count promotion receipts: %v", err)
	}
	return count
}
