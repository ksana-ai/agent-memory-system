package memstore_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/ksana-ai/agent-memory-system/internal/domain"
	"github.com/ksana-ai/agent-memory-system/internal/store"
	"github.com/ksana-ai/agent-memory-system/internal/store/memstore"
)

func TestEvidenceIDsAreScopedByTenantAndUser(t *testing.T) {
	storage := memstore.New()
	for _, event := range []domain.EvidenceEvent{
		{ID: "same-id", TenantID: "tenant-a", UserID: "user", SessionID: "one", Actor: domain.ActorUser, Content: "tenant a"},
		{ID: "same-id", TenantID: "tenant-b", UserID: "user", SessionID: "two", Actor: domain.ActorUser, Content: "tenant b"},
		{ID: "same-id", TenantID: "tenant-a", UserID: "other", SessionID: "three", Actor: domain.ActorUser, Content: "other user"},
	} {
		if err := storage.AppendEvidence(context.Background(), event); err != nil {
			t.Fatalf("append %#v: %v", event, err)
		}
	}
	for _, scope := range []struct{ tenant, user, want string }{
		{"tenant-a", "user", "tenant a"},
		{"tenant-b", "user", "tenant b"},
		{"tenant-a", "other", "other user"},
	} {
		event, err := storage.EvidenceByID(context.Background(), scope.tenant, scope.user, "same-id")
		if err != nil || event.Content != scope.want {
			t.Fatalf("scope %s/%s event=%#v error=%v", scope.tenant, scope.user, event, err)
		}
	}
}

func TestCreateCandidateBatchCreatesAllPendingCandidatesWithoutChangingRevision(t *testing.T) {
	storage := memstore.New()
	ctx := context.Background()
	for _, eventID := range []string{"event-batch-one", "event-batch-two"} {
		if err := storage.AppendEvidence(ctx, domain.EvidenceEvent{
			ID: eventID, TenantID: "tenant", UserID: "user", SessionID: "session",
			Actor: domain.ActorUser, Content: eventID,
		}); err != nil {
			t.Fatalf("append evidence %q: %v", eventID, err)
		}
	}
	candidates := []domain.MemoryCandidate{
		batchCandidate("candidate-batch-one", "event-batch-one"),
		batchCandidate("candidate-batch-two", "event-batch-two"),
	}
	if err := storage.CreateCandidateBatch(ctx, store.CandidateBatchCommand{
		TenantID: "tenant", UserID: "user", ExpectedRevision: 0, Candidates: candidates,
	}); err != nil {
		t.Fatalf("create candidate batch: %v", err)
	}

	// The store owns its copy of the batch values.
	candidates[0].SourceEventIDs[0] = "mutated"
	for _, candidateID := range []string{"candidate-batch-one", "candidate-batch-two"} {
		candidate, err := storage.CandidateByID(ctx, "tenant", "user", candidateID)
		if err != nil {
			t.Fatalf("load candidate %q: %v", candidateID, err)
		}
		if candidate.Status != domain.CandidatePending || candidate.Review != nil || candidate.SourceEventIDs[0] == "mutated" {
			t.Fatalf("stored candidate %q=%#v", candidateID, candidate)
		}
	}
	if revision, err := storage.ContextRevision(ctx, "tenant", "user"); err != nil || revision != 0 {
		t.Fatalf("revision after pending batch=%d error=%v, want 0", revision, err)
	}
	if err := storage.CreateCandidateBatch(ctx, store.CandidateBatchCommand{
		TenantID: "tenant", UserID: "user", ExpectedRevision: 0,
	}); err != nil {
		t.Fatalf("create current empty batch: %v", err)
	}
}

func TestCreateCandidateBatchValidationIsAtomic(t *testing.T) {
	storage := memstore.New()
	ctx := context.Background()
	if err := storage.AppendEvidence(ctx, domain.EvidenceEvent{
		ID: "event-real", TenantID: "tenant", UserID: "user", SessionID: "session",
		Actor: domain.ActorUser, Content: "real",
	}); err != nil {
		t.Fatalf("append evidence: %v", err)
	}
	first := batchCandidate("candidate-valid", "event-real")
	second := batchCandidate("candidate-missing", "event-missing")
	err := storage.CreateCandidateBatch(ctx, store.CandidateBatchCommand{
		TenantID: "tenant", UserID: "user", ExpectedRevision: 0,
		Candidates: []domain.MemoryCandidate{first, second},
	})
	if !errors.Is(err, domain.ErrNotFound) {
		t.Fatalf("create invalid batch error=%v, want not found", err)
	}
	for _, candidateID := range []string{first.ID, second.ID} {
		if _, err := storage.CandidateByID(ctx, "tenant", "user", candidateID); !errors.Is(err, domain.ErrNotFound) {
			t.Fatalf("candidate %q after failed batch error=%v, want not found", candidateID, err)
		}
	}
}

func TestCreateCandidateBatchRevisionFencePreventsEvidenceIDABA(t *testing.T) {
	storage := memstore.New()
	ctx := context.Background()
	original := domain.EvidenceEvent{
		ID: "event-aba", TenantID: "tenant", UserID: "user", SessionID: "old-session",
		Actor: domain.ActorUser, Content: "old evidence",
	}
	if err := storage.AppendEvidence(ctx, original); err != nil {
		t.Fatalf("append original evidence: %v", err)
	}
	if _, err := storage.ForgetUser(ctx, "tenant", "user", time.Now().UTC()); err != nil {
		t.Fatalf("forget user: %v", err)
	}
	replacement := original
	replacement.SessionID = "new-session"
	replacement.Content = "new evidence with reused id"
	if err := storage.AppendEvidence(ctx, replacement); err != nil {
		t.Fatalf("append replacement evidence: %v", err)
	}

	value := batchCandidate("candidate-stale", original.ID)
	err := storage.CreateCandidateBatch(ctx, store.CandidateBatchCommand{
		TenantID: "tenant", UserID: "user", ExpectedRevision: 0,
		Candidates: []domain.MemoryCandidate{value},
	})
	if !errors.Is(err, domain.ErrConflict) {
		t.Fatalf("create stale batch error=%v, want conflict", err)
	}
	if _, err := storage.CandidateByID(ctx, "tenant", "user", value.ID); !errors.Is(err, domain.ErrNotFound) {
		t.Fatalf("stale candidate after revision conflict error=%v, want not found", err)
	}
	if err := storage.CreateCandidateBatch(ctx, store.CandidateBatchCommand{
		TenantID: "tenant", UserID: "user", ExpectedRevision: 0,
	}); !errors.Is(err, domain.ErrConflict) {
		t.Fatalf("empty stale batch error=%v, want conflict", err)
	}
}

func TestCreateCandidateBatchRejectsInvalidCandidateSets(t *testing.T) {
	newFixture := func(t *testing.T) *memstore.Store {
		t.Helper()
		storage := memstore.New()
		if err := storage.AppendEvidence(context.Background(), domain.EvidenceEvent{
			ID: "event", TenantID: "tenant", UserID: "user", SessionID: "session",
			Actor: domain.ActorUser, Content: "source",
		}); err != nil {
			t.Fatalf("append evidence: %v", err)
		}
		return storage
	}
	base := batchCandidate("candidate", "event")
	reviewed := base
	reviewed.Status = domain.CandidateApproved
	duplicateSource := base
	duplicateSource.SourceEventIDs = []string{"event", "event"}
	wrongScope := base
	wrongScope.UserID = "other-user"

	for _, fixture := range []struct {
		name       string
		candidates []domain.MemoryCandidate
	}{
		{name: "cross scope", candidates: []domain.MemoryCandidate{wrongScope}},
		{name: "not pending", candidates: []domain.MemoryCandidate{reviewed}},
		{name: "duplicate source", candidates: []domain.MemoryCandidate{duplicateSource}},
		{name: "duplicate candidate id", candidates: []domain.MemoryCandidate{base, base}},
	} {
		t.Run(fixture.name, func(t *testing.T) {
			storage := newFixture(t)
			err := storage.CreateCandidateBatch(context.Background(), store.CandidateBatchCommand{
				TenantID: "tenant", UserID: "user", ExpectedRevision: 0, Candidates: fixture.candidates,
			})
			if !errors.Is(err, domain.ErrInvalid) {
				t.Fatalf("create invalid batch error=%v, want invalid", err)
			}
			if _, err := storage.CandidateByID(context.Background(), "tenant", "user", base.ID); !errors.Is(err, domain.ErrNotFound) {
				t.Fatalf("candidate after invalid batch error=%v, want not found", err)
			}
		})
	}
}

func TestConcurrentReviewHasOneWinner(t *testing.T) {
	storage := memstore.New()
	candidate := seedCandidate(t, storage, "candidate-1", "preference", "window")
	start := make(chan struct{})
	results := make(chan error, 2)
	var wait sync.WaitGroup
	for index := 0; index < 2; index++ {
		wait.Add(1)
		go func(index int) {
			defer wait.Done()
			<-start
			_, _, err := storage.ReviewCandidate(context.Background(), store.CandidateReviewCommand{
				TenantID:    candidate.TenantID,
				UserID:      candidate.UserID,
				CandidateID: candidate.ID,
				MemoryID:    "memory-" + string(rune('a'+index)),
				Review: domain.CandidateReview{
					Decision:   domain.DecisionApprove,
					ReviewerID: "reviewer",
					Reason:     "supported",
					ReviewedAt: time.Date(2026, 8, 19, 0, 0, index, 0, time.UTC),
				},
			})
			results <- err
		}(index)
	}
	close(start)
	wait.Wait()
	close(results)

	successes, conflicts := 0, 0
	for err := range results {
		switch {
		case err == nil:
			successes++
		case errors.Is(err, domain.ErrConflict):
			conflicts++
		default:
			t.Fatalf("unexpected review error: %v", err)
		}
	}
	if successes != 1 || conflicts != 1 {
		t.Fatalf("successes=%d conflicts=%d, want 1/1", successes, conflicts)
	}
	active, err := storage.ListServiceableMemories(context.Background(), candidate.TenantID, candidate.UserID, time.Now().UTC())
	if err != nil || len(active) != 1 {
		t.Fatalf("active=%#v error=%v", active, err)
	}
}

func TestConcurrentConflictingCandidatesFormVersionChain(t *testing.T) {
	storage := memstore.New()
	first := seedCandidate(t, storage, "candidate-a", "Seat_Preference", "window")
	second := seedCandidate(t, storage, "candidate-b", "seat_preference", "aisle")
	candidates := []domain.MemoryCandidate{first, second}
	start := make(chan struct{})
	versions := make(chan int, 2)
	errorsChannel := make(chan error, 2)
	var wait sync.WaitGroup
	for index, candidate := range candidates {
		wait.Add(1)
		go func(index int, candidate domain.MemoryCandidate) {
			defer wait.Done()
			<-start
			_, memory, err := storage.ReviewCandidate(context.Background(), store.CandidateReviewCommand{
				TenantID:    candidate.TenantID,
				UserID:      candidate.UserID,
				CandidateID: candidate.ID,
				MemoryID:    "memory-version-" + string(rune('a'+index)),
				Review:      domain.CandidateReview{Decision: domain.DecisionApprove, ReviewerID: "reviewer", Reason: "supported", ReviewedAt: time.Now().UTC()},
			})
			if err != nil {
				errorsChannel <- err
				return
			}
			versions <- memory.Version
		}(index, candidate)
	}
	close(start)
	wait.Wait()
	close(versions)
	close(errorsChannel)
	for err := range errorsChannel {
		t.Fatalf("review candidate: %v", err)
	}
	gotVersions := map[int]bool{}
	for version := range versions {
		gotVersions[version] = true
	}
	if !gotVersions[1] || !gotVersions[2] || len(gotVersions) != 2 {
		t.Fatalf("versions=%v, want {1,2}", gotVersions)
	}
	active, err := storage.ListServiceableMemories(context.Background(), "tenant", "user", time.Now().UTC())
	if err != nil || len(active) != 1 || active[0].Version != 2 {
		t.Fatalf("active=%#v error=%v", active, err)
	}
}

func TestListServiceableMemoriesFiltersExpirationWithoutChangingStatus(t *testing.T) {
	storage := memstore.New()
	asOf := time.Date(2026, 8, 19, 12, 0, 0, 0, time.UTC)
	past := asOf.Add(-time.Second)
	equal := asOf
	future := asOf.Add(time.Second)

	for _, fixture := range []struct {
		id        string
		expiresAt *time.Time
	}{
		{id: "past", expiresAt: &past},
		{id: "equal", expiresAt: &equal},
		{id: "future", expiresAt: &future},
		{id: "none"},
	} {
		candidate := seedCandidateWithExpiration(t, storage, "candidate-"+fixture.id, "key-"+fixture.id, fixture.id, fixture.expiresAt)
		_, memory, err := storage.ReviewCandidate(context.Background(), store.CandidateReviewCommand{
			TenantID: candidate.TenantID, UserID: candidate.UserID, CandidateID: candidate.ID, MemoryID: "memory-" + fixture.id,
			Review: domain.CandidateReview{Decision: domain.DecisionApprove, ReviewerID: "reviewer", Reason: "supported", ReviewedAt: asOf.Add(-time.Hour)},
		})
		if err != nil {
			t.Fatalf("review %s: %v", fixture.id, err)
		}
		if memory.Status != domain.MemoryActive || (fixture.expiresAt != nil && (memory.ExpiresAt == nil || !memory.ExpiresAt.Equal(*fixture.expiresAt))) {
			t.Fatalf("reviewed memory %s lost active status or expiration: %#v", fixture.id, memory)
		}
	}
	serviceable, err := storage.ListServiceableMemories(context.Background(), "tenant", "user", asOf)
	if err != nil {
		t.Fatalf("list serviceable: %v", err)
	}
	got := make(map[string]bool, len(serviceable))
	for _, memory := range serviceable {
		got[memory.ID] = true
	}
	if len(got) != 2 || !got["memory-future"] || !got["memory-none"] {
		t.Fatalf("serviceable memories = %#v, want future and none", serviceable)
	}
}

func TestCardCommitTimeIsMonotonicWhenReviewTimesArriveOutOfOrder(t *testing.T) {
	storage := memstore.New()
	first := seedCandidate(t, storage, "candidate-newer-review", "seat_preference", "window")
	second := seedCandidate(t, storage, "candidate-older-review", "seat_preference", "aisle")
	newerReviewTime := time.Date(2026, 8, 19, 12, 0, 0, 0, time.UTC)
	olderReviewTime := newerReviewTime.Add(-time.Hour)

	_, firstMemory, err := storage.ReviewCandidate(context.Background(), store.CandidateReviewCommand{
		TenantID: first.TenantID, UserID: first.UserID, CandidateID: first.ID, MemoryID: "memory-first",
		Review: domain.CandidateReview{Decision: domain.DecisionApprove, ReviewerID: "reviewer", Reason: "supported", ReviewedAt: newerReviewTime},
	})
	if err != nil {
		t.Fatalf("review first: %v", err)
	}
	_, secondMemory, err := storage.ReviewCandidate(context.Background(), store.CandidateReviewCommand{
		TenantID: second.TenantID, UserID: second.UserID, CandidateID: second.ID, MemoryID: "memory-second",
		Review: domain.CandidateReview{Decision: domain.DecisionApprove, ReviewerID: "reviewer", Reason: "supported", ReviewedAt: olderReviewTime},
	})
	if err != nil {
		t.Fatalf("review second: %v", err)
	}
	if !secondMemory.CreatedAt.After(firstMemory.CreatedAt) {
		t.Fatalf("version 2 created_at=%s must be after version 1 created_at=%s", secondMemory.CreatedAt, firstMemory.CreatedAt)
	}
}

func seedCandidate(t *testing.T, storage *memstore.Store, candidateID, key, value string) domain.MemoryCandidate {
	return seedCandidateWithExpiration(t, storage, candidateID, key, value, nil)
}

func batchCandidate(candidateID, eventID string) domain.MemoryCandidate {
	return domain.MemoryCandidate{
		ID: candidateID, TenantID: "tenant", UserID: "user", Kind: domain.MemoryKindSemantic,
		Category: "preference", Key: candidateID, Value: "value", Person: "self", Relationship: "self",
		SourceEventIDs: []string{eventID}, Extractor: "batch-test", ExtractorVersion: "v1",
		Status: domain.CandidatePending, CreatedAt: time.Date(2026, 8, 29, 0, 0, 0, 0, time.UTC),
	}
}

func seedCandidateWithExpiration(t *testing.T, storage *memstore.Store, candidateID, key, value string, expiresAt *time.Time) domain.MemoryCandidate {
	t.Helper()
	eventID := "event-" + candidateID
	event := domain.EvidenceEvent{ID: eventID, TenantID: "tenant", UserID: "user", SessionID: "session", Actor: domain.ActorUser, Content: value}
	if err := storage.AppendEvidence(context.Background(), event); err != nil {
		t.Fatalf("append evidence: %v", err)
	}
	candidate := domain.MemoryCandidate{
		ID: candidateID, TenantID: "tenant", UserID: "user", Kind: domain.MemoryKindSemantic,
		Category: "travel", Key: key, Value: value, Person: "self", Relationship: "self",
		SourceEventIDs: []string{eventID}, Status: domain.CandidatePending, ExpiresAt: expiresAt,
	}
	if err := storage.CreateCandidate(context.Background(), candidate); err != nil {
		t.Fatalf("create candidate: %v", err)
	}
	return candidate
}
