//go:build integration

package postgres_test

import (
	"context"
	"errors"
	"math"
	"testing"
	"time"

	"github.com/kai443/go-agent-memory-system/internal/domain"
	"github.com/kai443/go-agent-memory-system/internal/embedding"
	"github.com/kai443/go-agent-memory-system/internal/store/postgres"
)

func TestSearchServingVectorPinsDeploymentAndFiltersRankingSQL(t *testing.T) {
	ctx := context.Background()
	databaseURL, storage := isolatedProjectionPromotionStore(t)

	spaceA := registerPromotionTarget(t, storage, "serving_search_a")
	spaceB := registerPromotionTarget(t, storage, "serving_search_b")
	retiredSpace := registerPromotionTarget(t, storage, "serving_search_retired")
	withoutServing, err := storage.CurrentServingProjection(ctx)
	if err != nil || withoutServing.Target != nil {
		t.Fatalf("initial serving state=%#v error=%v, want no target", withoutServing, err)
	}
	if _, err := storage.SearchServingVector(ctx, "tenant", "user", postgres.ServingVectorExpectation{
		EmbeddingSpace: spaceA, Generation: withoutServing.Generation,
	}, basisVector(0), 1, time.Now().UTC()); !errors.Is(err, domain.ErrNotFound) {
		t.Fatalf("search with shadow-only targets error=%v, want not found", err)
	}
	spaceATarget, err := storage.ProjectionTargetBySpace(ctx, spaceA)
	if err != nil {
		t.Fatalf("load target before blocked-state check: %v", err)
	}
	blockedAt := spaceATarget.UpdatedAt.Add(time.Microsecond)
	if _, err := storage.SetProjectionTarget(ctx, postgres.SetProjectionTargetCommand{
		EmbeddingSpace: spaceA, State: postgres.ProjectionTargetBlocked, UpdatedAt: blockedAt,
	}); err != nil {
		t.Fatalf("block non-serving target: %v", err)
	}
	blockedState, err := storage.CurrentServingProjection(ctx)
	if err != nil {
		t.Fatalf("read blocked-only serving state: %v", err)
	}
	if _, err := storage.SearchServingVector(ctx, "tenant", "user", postgres.ServingVectorExpectation{
		EmbeddingSpace: spaceA, Generation: blockedState.Generation,
	}, basisVector(0), 1, time.Now().UTC()); !errors.Is(err, domain.ErrNotFound) {
		t.Fatalf("blocked target search error=%v, want not found", err)
	}
	if _, err := storage.SetProjectionTarget(ctx, postgres.SetProjectionTargetCommand{
		EmbeddingSpace: spaceA, State: postgres.ProjectionTargetShadow, EnqueueNew: true,
		UpdatedAt: blockedAt.Add(time.Microsecond),
	}); err != nil {
		t.Fatalf("restore blocked target to shadow: %v", err)
	}
	retiredTarget, err := storage.ProjectionTargetBySpace(ctx, retiredSpace)
	if err != nil {
		t.Fatalf("load target before retired-state check: %v", err)
	}
	if _, err := storage.SetProjectionTarget(ctx, postgres.SetProjectionTargetCommand{
		EmbeddingSpace: retiredSpace, State: postgres.ProjectionTargetRetired,
		UpdatedAt: retiredTarget.UpdatedAt.Add(time.Microsecond),
	}); err != nil {
		t.Fatalf("retire non-serving target: %v", err)
	}
	retiredState, err := storage.CurrentServingProjection(ctx)
	if err != nil {
		t.Fatalf("read retired-only serving state: %v", err)
	}
	if _, err := storage.SearchServingVector(ctx, "tenant", "user", postgres.ServingVectorExpectation{
		EmbeddingSpace: retiredSpace, Generation: retiredState.Generation,
	}, basisVector(0), 1, time.Now().UTC()); !errors.Is(err, domain.ErrNotFound) {
		t.Fatalf("retired target search error=%v, want not found", err)
	}

	tenantID, userID := uniqueScope("serving-vector-main")
	foreignTenantID, foreignUserID := uniqueScope("serving-vector-foreign")
	expiresAt := time.Now().UTC().Add(time.Hour).Truncate(time.Microsecond)

	best := approveVectorCard(t, storage, tenantID, userID, "serving-best", "best", "best payload", 10, 20, nil)
	second := approveVectorCard(t, storage, tenantID, userID, "serving-second", "second", "second payload", 11, 21, nil)
	tieA := approveVectorCard(t, storage, tenantID, userID, "serving-tie-a", "tie-a", "tie a payload", 12, 22, nil)
	tieB := approveVectorCard(t, storage, tenantID, userID, "serving-tie-b", "tie-b", "tie b payload", 13, 22, nil)
	expired := approveVectorCard(t, storage, tenantID, userID, "serving-expired", "expired", "expired payload", 14, 23, &expiresAt)
	withoutSource := approveVectorCard(t, storage, tenantID, userID, "serving-no-source", "no-source", "no source payload", 15, 24, nil)
	pending := approveVectorCard(t, storage, tenantID, userID, "serving-pending", "pending", "pending payload", 16, 25, nil)
	versionMismatch := approveVectorCard(t, storage, tenantID, userID, "serving-version", "version", "version payload", 17, 26, nil)
	superseded := approveVectorCard(t, storage, tenantID, userID, "serving-superseded", "superseded", "superseded payload", 18, 27, nil)
	missingEmbedding := approveVectorCard(t, storage, tenantID, userID, "serving-no-embedding", "no-embedding", "no embedding payload", 19, 28, nil)
	foreign := approveVectorCard(t, storage, foreignTenantID, foreignUserID, "serving-foreign", "foreign", "foreign payload", 20, 29, nil)

	extraSource := evidence(tenantID, userID, "event-serving-best-extra", "best payload corroboration", 29)
	mustAppend(t, storage, extraSource)
	execPromotionSQL(t, databaseURL, `
		INSERT INTO agent_memory.candidate_source_events (
			tenant_id, user_id, candidate_id, evidence_event_id, source_order
		) VALUES ($1, $2, $3, $4, 1)`, tenantID, userID, best.CandidateID, extraSource.ID)

	cardVectors := []struct {
		card   domain.MemoryCard
		vector []float32
	}{
		{best, basisVector(0)},
		{second, twoComponentUnitVector(0.8, 0.6)},
		{tieA, basisVector(1)},
		{tieB, basisVector(1)},
		{expired, basisVector(0)},
		{withoutSource, basisVector(0)},
		{pending, basisVector(0)},
		{versionMismatch, basisVector(0)},
		{superseded, basisVector(0)},
		{missingEmbedding, basisVector(0)},
		{foreign, basisVector(0)},
	}
	for _, fixture := range cardVectors {
		coverServingVectorCard(t, storage, databaseURL, fixture.card, spaceA, fixture.vector)
		// Keep the shadow target fully covered so the same fixtures also prove
		// that shadow embeddings cannot leak into a serving-space search.
		coverServingVectorCard(t, storage, databaseURL, fixture.card, spaceB, basisVector(1))
	}

	receipt, err := storage.PromoteProjection(ctx, postgres.PromoteProjectionCommand{
		OperationID: promotionOperationID(801), ToSpace: spaceA,
	})
	if err != nil {
		t.Fatalf("promote serving search target: %v", err)
	}
	expected := postgres.ServingVectorExpectation{EmbeddingSpace: spaceA, Generation: receipt.Generation}

	// Create serviceability drift after the coverage-backed promotion. Every
	// case must be excluded by the one ranking SQL rather than by Go filtering.
	execPromotionSQL(t, databaseURL, `
		DELETE FROM agent_memory.candidate_source_events
		WHERE tenant_id=$1 AND user_id=$2 AND candidate_id=$3`, tenantID, userID, withoutSource.CandidateID)
	execPromotionSQL(t, databaseURL, `
		UPDATE agent_memory.embedding_projection_jobs
		SET state='pending', completed_at=NULL, lease_owner=NULL, lease_until=NULL,
		    last_error_code=NULL, last_error_at=NULL, updated_at=clock_timestamp()
		WHERE tenant_id=$1 AND user_id=$2 AND memory_id=$3 AND embedding_space=$4`,
		tenantID, userID, pending.ID, spaceA)
	execPromotionSQL(t, databaseURL, `
		UPDATE agent_memory.embedding_projection_jobs
		SET expected_memory_version = expected_memory_version + 1,
		    updated_at=clock_timestamp()
		WHERE tenant_id=$1 AND user_id=$2 AND memory_id=$3 AND embedding_space=$4`,
		tenantID, userID, versionMismatch.ID, spaceA)
	execPromotionSQL(t, databaseURL, `
		UPDATE agent_memory.memory_cards
		SET status='superseded', superseded_at=clock_timestamp()
		WHERE tenant_id=$1 AND user_id=$2 AND id=$3`, tenantID, userID, superseded.ID)
	execPromotionSQL(t, databaseURL, `
		DELETE FROM agent_memory.memory_embeddings
		WHERE tenant_id=$1 AND user_id=$2 AND memory_id=$3 AND embedding_space=$4`,
		tenantID, userID, missingEmbedding.ID, spaceA)

	// Equality is intentionally expired: the serving predicate is strictly
	// expires_at > asOf, not >=.
	asOf := expiresAt
	for attempt := 0; attempt < 3; attempt++ {
		hits, searchErr := storage.SearchServingVector(
			ctx, tenantID, userID, expected, basisVector(0), 10, asOf,
		)
		if searchErr != nil {
			t.Fatalf("serving vector search attempt %d: %v", attempt, searchErr)
		}
		wantIDs := []string{best.ID, second.ID, tieA.ID, tieB.ID}
		if got := servingVectorHitIDs(hits); !equalStrings(got, wantIDs) {
			t.Fatalf("serving search ids=%v, want %v", got, wantIDs)
		}
		if math.Abs(hits[0].Score-1) > 1e-6 || math.Abs(hits[1].Score-0.8) > 1e-5 {
			t.Fatalf("serving search scores=%v, want approximately 1 and 0.8", vectorHitScores(hits))
		}
		if hits[2].Score != hits[3].Score {
			t.Fatalf("serving stable tie scores=%v", vectorHitScores(hits))
		}
		if hits[0].Memory.Value != "best payload" {
			t.Fatalf("serving payload=%#v", hits[0].Memory)
		}
		if got, want := hits[0].Memory.SourceEventIDs, []string{"event-serving-best", extraSource.ID}; !equalStrings(got, want) {
			t.Fatalf("serving source order=%v, want %v", got, want)
		}
	}

	limited, err := storage.SearchServingVector(ctx, tenantID, userID, expected, basisVector(0), 2, asOf)
	if err != nil || !equalStrings(servingVectorHitIDs(limited), []string{best.ID, second.ID}) {
		t.Fatalf("limited serving hits=%v error=%v", servingVectorHitIDs(limited), err)
	}
	empty, err := storage.SearchServingVector(ctx, tenantID, userID, expected, nil, 0, asOf)
	if err != nil || len(empty) != 0 {
		t.Fatalf("zero-limit serving hits=%#v error=%v", empty, err)
	}

	if _, err := storage.SearchServingVector(ctx, tenantID, userID, postgres.ServingVectorExpectation{
		EmbeddingSpace: spaceB, Generation: receipt.Generation,
	}, basisVector(0), 10, asOf); !errors.Is(err, domain.ErrConflict) {
		t.Fatalf("shadow expectation error=%v, want conflict", err)
	}
	if _, err := storage.SearchServingVector(ctx, tenantID, userID, postgres.ServingVectorExpectation{
		EmbeddingSpace: spaceA, Generation: receipt.Generation - 1,
	}, basisVector(0), 10, asOf); !errors.Is(err, domain.ErrConflict) {
		t.Fatalf("stale generation error=%v, want conflict", err)
	}
	if _, err := storage.SearchServingVector(ctx, tenantID, userID, postgres.ServingVectorExpectation{
		EmbeddingSpace: spaceA, Generation: receipt.Generation - 1,
	}, nil, 0, asOf); !errors.Is(err, domain.ErrConflict) {
		t.Fatalf("zero-limit stale generation error=%v, want conflict", err)
	}
}

func TestSearchServingVectorWaitsForAtomicRotationAndRejectsOldPin(t *testing.T) {
	ctx := context.Background()
	databaseURL, storage := isolatedProjectionPromotionStore(t)
	spaceA := registerPromotionTarget(t, storage, "serving_race_a")
	spaceB := registerPromotionTarget(t, storage, "serving_race_b")
	tenantID, userID := uniqueScope("serving-vector-rotation")
	card := approveVectorCard(t, storage, tenantID, userID, "serving-rotation", "rotation", "rotation payload", 40, 41, nil)
	coverServingVectorCard(t, storage, databaseURL, card, spaceA, basisVector(0))
	coverServingVectorCard(t, storage, databaseURL, card, spaceB, basisVector(0))
	first, err := storage.PromoteProjection(ctx, postgres.PromoteProjectionCommand{
		OperationID: promotionOperationID(811), ToSpace: spaceA,
	})
	if err != nil {
		t.Fatalf("seed serving rotation: %v", err)
	}

	advisoryKey := int64(1_920_000_000 + scopeSequence.Add(1)%10_000_000)
	holder := holdPromotionAdvisoryLock(t, ctx, databaseURL, advisoryKey)
	locked := true
	defer func() {
		if locked {
			_, _ = holder.Exec(context.Background(), `SELECT pg_advisory_unlock($1)`, advisoryKey)
		}
		_ = holder.Close(context.Background())
	}()
	installPromotionTargetAdvisoryTrigger(t, databaseURL, spaceB, advisoryKey)

	promotionResult := make(chan struct {
		receipt postgres.ProjectionPromotionReceipt
		err     error
	}, 1)
	go func() {
		receipt, promoteErr := storage.PromoteProjection(ctx, postgres.PromoteProjectionCommand{
			OperationID: promotionOperationID(812), ExpectedFrom: spaceA, ToSpace: spaceB,
		})
		promotionResult <- struct {
			receipt postgres.ProjectionPromotionReceipt
			err     error
		}{receipt: receipt, err: promoteErr}
	}()
	waitForProjectionAdvisoryWaiter(t, databaseURL, advisoryKey)

	const searchApplication = "serving_vector_waits_for_rotation"
	searchStore := openProjectionDeploymentStoreWithApplicationName(t, databaseURL, searchApplication)
	defer searchStore.Close()
	searchResult := make(chan struct {
		hits []domain.SearchHit
		err  error
	}, 1)
	go func() {
		hits, searchErr := searchStore.SearchServingVector(ctx, tenantID, userID, postgres.ServingVectorExpectation{
			EmbeddingSpace: spaceA, Generation: first.Generation,
		}, basisVector(0), 10, time.Now().UTC())
		searchResult <- struct {
			hits []domain.SearchHit
			err  error
		}{hits: hits, err: searchErr}
	}()
	waitForProjectionDeploymentLock(t, databaseURL, searchApplication)

	if _, err := holder.Exec(ctx, `SELECT pg_advisory_unlock($1)`, advisoryKey); err != nil {
		t.Fatalf("release serving rotation advisory lock: %v", err)
	}
	locked = false
	rotated := <-promotionResult
	if rotated.err != nil || rotated.receipt.Generation != first.Generation+1 {
		t.Fatalf("serving rotation receipt=%#v error=%v", rotated.receipt, rotated.err)
	}
	oldSearch := <-searchResult
	if len(oldSearch.hits) != 0 || !errors.Is(oldSearch.err, domain.ErrConflict) {
		t.Fatalf("old serving pin hits=%v error=%v, want conflict", servingVectorHitIDs(oldSearch.hits), oldSearch.err)
	}

	newHits, err := searchStore.SearchServingVector(ctx, tenantID, userID, postgres.ServingVectorExpectation{
		EmbeddingSpace: spaceB, Generation: rotated.receipt.Generation,
	}, basisVector(0), 10, time.Now().UTC())
	if err != nil || !equalStrings(servingVectorHitIDs(newHits), []string{card.ID}) {
		t.Fatalf("new serving pin hits=%v error=%v", servingVectorHitIDs(newHits), err)
	}
}

func coverServingVectorCard(
	t *testing.T,
	storage *postgres.Store,
	databaseURL string,
	card domain.MemoryCard,
	space string,
	vector []float32,
) {
	t.Helper()
	target, err := storage.ProjectionTargetBySpace(context.Background(), space)
	if err != nil {
		t.Fatalf("load serving vector target: %v", err)
	}
	value := reconciliationEmbedding(card, target.Space, embedding.MemoryCardDocumentV1SHA256(card))
	value.Vector = vector
	if err := storage.UpsertMemoryEmbedding(context.Background(), value); err != nil {
		t.Fatalf("upsert serving vector embedding: %v", err)
	}
	markProjectionJobState(t, databaseURL, card.TenantID, card.UserID, card.ID, space, "succeeded")
}

func servingVectorHitIDs(hits []domain.SearchHit) []string {
	result := make([]string, len(hits))
	for index := range hits {
		result[index] = hits[index].Memory.ID
	}
	return result
}
