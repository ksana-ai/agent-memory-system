package app_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/kai443/go-agent-memory-system/internal/app"
	"github.com/kai443/go-agent-memory-system/internal/domain"
	"github.com/kai443/go-agent-memory-system/internal/retrieval"
	"github.com/kai443/go-agent-memory-system/internal/store/memstore"
)

func TestCandidateIsNotServedUntilApproved(t *testing.T) {
	service, _ := newTestService(t)
	event := ingest(t, service, "tenant-a", "user-a", "session-a", "I prefer a window seat when I fly.")
	candidate := propose(t, service, "tenant-a", "user-a", event.ID, "travel", "seat_preference", "window seat")

	before := buildContext(t, service, "tenant-a", "user-a", "preferred seat")
	if len(before.Items) != 0 {
		t.Fatalf("pending candidate leaked into context: %#v", before.Items)
	}

	reviewed, memory, err := service.ReviewCandidate(context.Background(), app.ReviewCandidateInput{
		TenantID:    "tenant-a",
		UserID:      "user-a",
		CandidateID: candidate.ID,
		Decision:    domain.DecisionApprove,
		ReviewerID:  "reviewer-1",
		Reason:      "The source directly states the preference.",
	})
	if err != nil {
		t.Fatalf("approve candidate: %v", err)
	}
	if reviewed.Status != domain.CandidateApproved || memory == nil || memory.Version != 1 {
		t.Fatalf("unexpected review result: candidate=%#v memory=%#v", reviewed, memory)
	}

	after := buildContext(t, service, "tenant-a", "user-a", "window seat preference")
	if len(after.Items) != 1 {
		t.Fatalf("got %d context items, want 1", len(after.Items))
	}
	if after.Items[0].Memory.Key != "seat_preference" || len(after.Items[0].Sources) != 1 {
		t.Fatalf("context did not preserve memory and source: %#v", after.Items[0])
	}
	if after.Items[0].Sources[0].ID != event.ID {
		t.Fatalf("got source %q, want %q", after.Items[0].Sources[0].ID, event.ID)
	}
}

func TestTenantAndUserScopesAreIsolated(t *testing.T) {
	service, _ := newTestService(t)
	approveMemory(t, service, "tenant-a", "shared-user", "tenant-a likes window seats", "seat_preference", "window seat")
	approveMemory(t, service, "tenant-b", "shared-user", "tenant-b uses the zebra preference marker", "seat_preference", "zebra preference")
	approveMemory(t, service, "tenant-a", "other-user", "other user uses the zebra preference marker", "seat_preference", "zebra preference")

	pack := buildContext(t, service, "tenant-a", "shared-user", "zebra")
	if len(pack.Items) != 0 {
		t.Fatalf("cross-scope memories leaked into context: %#v", pack.Items)
	}

	pack = buildContext(t, service, "tenant-b", "shared-user", "zebra")
	if len(pack.Items) != 1 || pack.Items[0].Memory.TenantID != "tenant-b" {
		t.Fatalf("expected only tenant-b memory, got %#v", pack.Items)
	}
}

func TestApprovingConflictCreatesNewActiveVersion(t *testing.T) {
	service, storage := newTestService(t)
	first := approveMemory(t, service, "tenant-a", "user-a", "I prefer window seats.", "seat_preference", "window seat")
	second := approveMemory(t, service, "tenant-a", "user-a", "I now prefer aisle seats.", "seat_preference", "aisle seat")

	if first.Version != 1 || second.Version != 2 {
		t.Fatalf("got versions %d and %d, want 1 and 2", first.Version, second.Version)
	}
	active, err := storage.ListServiceableMemories(context.Background(), "tenant-a", "user-a", time.Now().UTC())
	if err != nil {
		t.Fatalf("list active memories: %v", err)
	}
	if len(active) != 1 || active[0].ID != second.ID || active[0].Status != domain.MemoryActive {
		t.Fatalf("expected only version 2 active, got %#v", active)
	}
	oldQuery := buildContext(t, service, "tenant-a", "user-a", "window")
	if len(oldQuery.Items) != 0 {
		t.Fatalf("superseded version was retrieved: %#v", oldQuery.Items)
	}
}

func TestForgetUserRemovesAllServiceableAndSourceData(t *testing.T) {
	service, storage := newTestService(t)
	event := ingest(t, service, "tenant-a", "user-a", "session-a", "My passport expires in May 2028.")
	candidate := propose(t, service, "tenant-a", "user-a", event.ID, "travel", "passport_expiry", "May 2028")
	approve(t, service, "tenant-a", "user-a", candidate.ID)

	receipt, err := service.ForgetUser(context.Background(), "tenant-a", "user-a")
	if err != nil {
		t.Fatalf("forget user: %v", err)
	}
	if receipt.EvidenceDeleted != 1 || receipt.CandidatesDeleted != 1 || receipt.MemoriesDeleted != 1 {
		t.Fatalf("unexpected deletion receipt: %#v", receipt)
	}
	pack := buildContext(t, service, "tenant-a", "user-a", "passport expiry")
	if len(pack.Items) != 0 {
		t.Fatalf("deleted memory was retrieved: %#v", pack.Items)
	}
	if _, err := storage.EvidenceByID(context.Background(), "tenant-a", "user-a", event.ID); !errors.Is(err, domain.ErrNotFound) {
		t.Fatalf("evidence still exists or wrong error: %v", err)
	}
}

func TestCandidateCannotReferenceEvidenceFromAnotherScope(t *testing.T) {
	service, _ := newTestService(t)
	event := ingest(t, service, "tenant-a", "user-a", "session-a", "private tenant-a fact")

	_, err := service.ProposeCandidate(context.Background(), app.ProposeCandidateInput{
		TenantID:         "tenant-b",
		UserID:           "user-a",
		Kind:             domain.MemoryKindSemantic,
		Category:         "private",
		Key:              "fact",
		Value:            "private tenant-a fact",
		SourceEventIDs:   []string{event.ID},
		Extractor:        "test-fixture",
		ExtractorVersion: "v1",
	})
	if !errors.Is(err, domain.ErrNotFound) {
		t.Fatalf("got error %v, want not found", err)
	}
}

func TestRejectedCandidateNeverBecomesServiceable(t *testing.T) {
	service, storage := newTestService(t)
	event := ingest(t, service, "tenant-a", "user-a", "session-a", "Maybe I will move to Paris someday.")
	candidate := propose(t, service, "tenant-a", "user-a", event.ID, "location", "home_city", "Paris")

	reviewed, memory, err := service.ReviewCandidate(context.Background(), app.ReviewCandidateInput{
		TenantID:    "tenant-a",
		UserID:      "user-a",
		CandidateID: candidate.ID,
		Decision:    domain.DecisionReject,
		ReviewerID:  "reviewer-1",
		Reason:      "The source is a hypothetical plan, not a current fact.",
	})
	if err != nil {
		t.Fatalf("reject candidate: %v", err)
	}
	if reviewed.Status != domain.CandidateRejected || reviewed.Review == nil || memory != nil {
		t.Fatalf("unexpected rejection result: candidate=%#v memory=%#v", reviewed, memory)
	}
	stored, err := storage.CandidateByID(context.Background(), "tenant-a", "user-a", candidate.ID)
	if err != nil || stored.Status != domain.CandidateRejected {
		t.Fatalf("stored candidate=%#v error=%v", stored, err)
	}
	if pack := buildContext(t, service, "tenant-a", "user-a", "Paris home city"); len(pack.Items) != 0 {
		t.Fatalf("rejected candidate leaked into context: %#v", pack.Items)
	}
	_, _, err = service.ReviewCandidate(context.Background(), app.ReviewCandidateInput{
		TenantID:    "tenant-a",
		UserID:      "user-a",
		CandidateID: candidate.ID,
		Decision:    domain.DecisionApprove,
		ReviewerID:  "reviewer-2",
		Reason:      "Retry should not replace the first decision.",
	})
	if !errors.Is(err, domain.ErrConflict) {
		t.Fatalf("duplicate review error=%v, want conflict", err)
	}
}

func TestBuildContextRetriesWhenDeletionChangesSnapshot(t *testing.T) {
	storage := memstore.New()
	inner, err := retrieval.NewBM25(storage)
	if err != nil {
		t.Fatalf("new retriever: %v", err)
	}
	wrapper := &deleteDuringSearchRetriever{inner: inner, storage: storage}
	service, err := app.New(storage, wrapper)
	if err != nil {
		t.Fatalf("new service: %v", err)
	}
	approveMemory(t, service, "tenant-a", "user-a", "I prefer window seats.", "seat_preference", "window seat")

	pack, err := service.BuildContext(context.Background(), app.BuildContextInput{
		TenantID: "tenant-a",
		UserID:   "user-a",
		Query:    "window seat",
		Limit:    5,
	})
	if err != nil {
		t.Fatalf("build context during deletion: %v", err)
	}
	if len(pack.Items) != 0 {
		t.Fatalf("retrieval returned a stale pre-deletion snapshot: %#v", pack.Items)
	}
}

func TestBuildContextFailsClosedOnRetrieverScopeViolation(t *testing.T) {
	storage := memstore.New()
	service, err := app.New(storage, staticRetriever{hits: []domain.SearchHit{{
		Memory: domain.MemoryCard{
			ID:       "malicious-memory",
			TenantID: "other-tenant",
			UserID:   "user-a",
			Status:   domain.MemoryActive,
		},
		Score: 99,
	}}})
	if err != nil {
		t.Fatalf("new service: %v", err)
	}
	_, err = service.BuildContext(context.Background(), app.BuildContextInput{
		TenantID: "tenant-a",
		UserID:   "user-a",
		Query:    "private data",
		Limit:    5,
	})
	if !errors.Is(err, domain.ErrInvariant) {
		t.Fatalf("scope violation error=%v, want invariant violation", err)
	}
}

func TestMemoryExpirationUsesSingleBuildContextSnapshot(t *testing.T) {
	asOf := time.Date(2026, 8, 19, 12, 0, 0, 0, time.UTC)
	storage := memstore.New()
	inner, err := retrieval.NewBM25(storage)
	if err != nil {
		t.Fatalf("new retriever: %v", err)
	}
	clockCalls := 0
	service, err := app.New(storage, inner,
		app.WithClock(func() time.Time { clockCalls++; return asOf }),
		app.WithIDGenerator(sequenceIDs()),
	)
	if err != nil {
		t.Fatalf("new service: %v", err)
	}
	past := asOf.Add(-time.Second)
	equal := asOf
	future := asOf.Add(time.Second)
	for _, fixture := range []struct {
		key       string
		expiresAt *time.Time
		want      int
	}{
		{key: "pastunique", expiresAt: &past, want: 0},
		{key: "equalunique", expiresAt: &equal, want: 0},
		{key: "futureunique", expiresAt: &future, want: 1},
		{key: "persistentunique", want: 1},
	} {
		event := ingest(t, service, "tenant-a", "user-a", "session-a", fixture.key)
		candidate, proposeErr := service.ProposeCandidate(context.Background(), app.ProposeCandidateInput{
			TenantID: "tenant-a", UserID: "user-a", Kind: domain.MemoryKindSemantic,
			Category: "expiration", Key: fixture.key, Value: fixture.key, SourceEventIDs: []string{event.ID},
			Extractor: "test", ExtractorVersion: "v1", ExpiresAt: fixture.expiresAt,
		})
		if proposeErr != nil {
			t.Fatalf("propose %s: %v", fixture.key, proposeErr)
		}
		memory := approve(t, service, "tenant-a", "user-a", candidate.ID)
		if memory.Status != domain.MemoryActive {
			t.Fatalf("expired memory lifecycle status = %q, want active", memory.Status)
		}
		before := clockCalls
		pack := buildContext(t, service, "tenant-a", "user-a", fixture.key)
		if len(pack.Items) != fixture.want {
			t.Fatalf("%s context items = %d, want %d", fixture.key, len(pack.Items), fixture.want)
		}
		if clockCalls != before+1 || !pack.GeneratedAt.Equal(asOf) {
			t.Fatalf("BuildContext clock calls=%d (before %d), generated_at=%s", clockCalls, before, pack.GeneratedAt)
		}
	}
}

func TestProposeCandidateRejectsZeroExpirationButAllowsPast(t *testing.T) {
	service, _ := newTestService(t)
	event := ingest(t, service, "tenant-a", "user-a", "session-a", "temporary fact")
	zero := time.Time{}
	base := app.ProposeCandidateInput{
		TenantID: "tenant-a", UserID: "user-a", Kind: domain.MemoryKindSemantic,
		Category: "temporary", Key: "fact", Value: "value", SourceEventIDs: []string{event.ID},
		Extractor: "test", ExtractorVersion: "v1", ExpiresAt: &zero,
	}
	if _, err := service.ProposeCandidate(context.Background(), base); !errors.Is(err, domain.ErrInvalid) {
		t.Fatalf("zero expires_at error = %v, want invalid", err)
	}
	past := time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC)
	base.ExpiresAt = &past
	if _, err := service.ProposeCandidate(context.Background(), base); err != nil {
		t.Fatalf("past expires_at rejected: %v", err)
	}

	subMicrosecond := time.Date(2030, 1, 1, 0, 0, 0, 123456789, time.FixedZone("fixture", 8*60*60))
	base.ExpiresAt = &subMicrosecond
	candidate, err := service.ProposeCandidate(context.Background(), base)
	if err != nil {
		t.Fatalf("sub-microsecond expires_at rejected: %v", err)
	}
	want := subMicrosecond.UTC().Truncate(time.Microsecond)
	if candidate.ExpiresAt == nil || !candidate.ExpiresAt.Equal(want) || candidate.ExpiresAt.Location() != time.UTC {
		t.Fatalf("expires_at = %v, want canonical UTC microsecond %v", candidate.ExpiresAt, want)
	}
}

func TestBuildContextFailsClosedOnExpiredRetrieverHit(t *testing.T) {
	asOf := time.Date(2026, 8, 19, 12, 0, 0, 0, time.UTC)
	storage := memstore.New()
	service, err := app.New(storage, staticRetriever{hits: []domain.SearchHit{{
		Memory: domain.MemoryCard{
			ID: "expired-memory", TenantID: "tenant-a", UserID: "user-a", Status: domain.MemoryActive,
			ExpiresAt: &asOf, SourceEventIDs: []string{"never-loaded"},
		},
	}}}, app.WithClock(func() time.Time { return asOf }))
	if err != nil {
		t.Fatalf("new service: %v", err)
	}
	_, err = service.BuildContext(context.Background(), app.BuildContextInput{
		TenantID: "tenant-a", UserID: "user-a", Query: "expired", Limit: 5,
	})
	if !errors.Is(err, domain.ErrInvariant) {
		t.Fatalf("expired retriever hit error=%v, want invariant", err)
	}
}

func sequenceIDs() app.IDGenerator {
	counter := 0
	return func(prefix string) (string, error) {
		counter++
		return fmt.Sprintf("%s_exp_%03d", prefix, counter), nil
	}
}

type deleteDuringSearchRetriever struct {
	inner   app.Retriever
	storage *memstore.Store
	once    sync.Once
}

type staticRetriever struct {
	hits []domain.SearchHit
}

func (retriever staticRetriever) Search(context.Context, string, string, string, int, time.Time) ([]domain.SearchHit, error) {
	return append([]domain.SearchHit(nil), retriever.hits...), nil
}

func (retriever *deleteDuringSearchRetriever) Search(ctx context.Context, tenantID, userID, query string, limit int, asOf time.Time) ([]domain.SearchHit, error) {
	hits, err := retriever.inner.Search(ctx, tenantID, userID, query, limit, asOf)
	if err != nil {
		return nil, err
	}
	retriever.once.Do(func() {
		_, err = retriever.storage.ForgetUser(ctx, tenantID, userID, time.Now().UTC())
	})
	return hits, err
}

func newTestService(t *testing.T) (*app.Service, *memstore.Store) {
	t.Helper()
	storage := memstore.New()
	retriever, err := retrieval.NewBM25(storage)
	if err != nil {
		t.Fatalf("new retriever: %v", err)
	}
	counter := 0
	service, err := app.New(
		storage,
		retriever,
		app.WithClock(func() time.Time { return time.Date(2026, 8, 19, 8, 0, counter, 0, time.UTC) }),
		app.WithIDGenerator(func(prefix string) (string, error) {
			counter++
			return fmt.Sprintf("%s_%04d", prefix, counter), nil
		}),
	)
	if err != nil {
		t.Fatalf("new service: %v", err)
	}
	return service, storage
}

func ingest(t *testing.T, service *app.Service, tenantID, userID, sessionID, content string) domain.EvidenceEvent {
	t.Helper()
	event, err := service.IngestEvidence(context.Background(), app.IngestEvidenceInput{
		TenantID:  tenantID,
		UserID:    userID,
		SessionID: sessionID,
		Actor:     domain.ActorUser,
		Content:   content,
	})
	if err != nil {
		t.Fatalf("ingest evidence: %v", err)
	}
	return event
}

func propose(t *testing.T, service *app.Service, tenantID, userID, eventID, category, key, value string) domain.MemoryCandidate {
	t.Helper()
	candidate, err := service.ProposeCandidate(context.Background(), app.ProposeCandidateInput{
		TenantID:         tenantID,
		UserID:           userID,
		Kind:             domain.MemoryKindSemantic,
		Category:         category,
		Key:              key,
		Value:            value,
		Person:           "self",
		Relationship:     "self",
		Backstory:        "Directly stated by the user.",
		SourceEventIDs:   []string{eventID},
		Extractor:        "test-fixture",
		ExtractorVersion: "v1",
	})
	if err != nil {
		t.Fatalf("propose candidate: %v", err)
	}
	return candidate
}

func approve(t *testing.T, service *app.Service, tenantID, userID, candidateID string) domain.MemoryCard {
	t.Helper()
	_, memory, err := service.ReviewCandidate(context.Background(), app.ReviewCandidateInput{
		TenantID:    tenantID,
		UserID:      userID,
		CandidateID: candidateID,
		Decision:    domain.DecisionApprove,
		ReviewerID:  "reviewer-1",
		Reason:      "Source supports the candidate.",
	})
	if err != nil {
		t.Fatalf("approve candidate: %v", err)
	}
	if memory == nil {
		t.Fatal("approve candidate returned nil memory")
	}
	return *memory
}

func approveMemory(t *testing.T, service *app.Service, tenantID, userID, evidence, key, value string) domain.MemoryCard {
	t.Helper()
	event := ingest(t, service, tenantID, userID, "session-1", evidence)
	candidate := propose(t, service, tenantID, userID, event.ID, "preferences", key, value)
	return approve(t, service, tenantID, userID, candidate.ID)
}

func buildContext(t *testing.T, service *app.Service, tenantID, userID, query string) domain.ContextPack {
	t.Helper()
	pack, err := service.BuildContext(context.Background(), app.BuildContextInput{
		TenantID: tenantID,
		UserID:   userID,
		Query:    query,
		Limit:    5,
	})
	if err != nil {
		t.Fatalf("build context: %v", err)
	}
	return pack
}
