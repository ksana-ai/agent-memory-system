//go:build integration

package postgres_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/ksana-ai/agent-memory-system/internal/domain"
	"github.com/ksana-ai/agent-memory-system/internal/migrations"
	storecontract "github.com/ksana-ai/agent-memory-system/internal/store"
	"github.com/ksana-ai/agent-memory-system/internal/store/postgres"
)

var scopeSequence atomic.Uint64

func TestPostgresStoreLifecyclePreservesSourceOrderAndScope(t *testing.T) {
	ctx := context.Background()
	databaseURL := requiredDatabaseURL(t)
	applyMigrations(t, databaseURL)
	tenantID, userID := uniqueScope("lifecycle")
	otherTenantID, otherUserID := uniqueScope("same_event")
	cleanupScopes(t, databaseURL, [][2]string{{tenantID, userID}, {otherTenantID, otherUserID}})

	storage := openStore(t, databaseURL)
	defer storage.Close()

	first := evidence(tenantID, userID, "event-first", "prefers a window seat", 1)
	second := evidence(tenantID, userID, "event-second", "avoids aisle seats", 2)
	for _, event := range []domain.EvidenceEvent{first, second} {
		if err := storage.AppendEvidence(ctx, event); err != nil {
			t.Fatalf("append evidence %q: %v", event.ID, err)
		}
	}
	if err := storage.AppendEvidence(ctx, evidence(otherTenantID, otherUserID, first.ID, "same id, other scope", 3)); err != nil {
		t.Fatalf("append same event id in another scope: %v", err)
	}

	ordered, err := storage.EvidenceByIDs(ctx, tenantID, userID, []string{second.ID, first.ID})
	if err != nil {
		t.Fatalf("load ordered evidence: %v", err)
	}
	if len(ordered) != 2 || ordered[0].ID != second.ID || ordered[1].ID != first.ID {
		t.Fatalf("source order=%v, want [%s %s]", eventIDs(ordered), second.ID, first.ID)
	}
	other, err := storage.EvidenceByID(ctx, otherTenantID, otherUserID, first.ID)
	if err != nil || other.Content != "same id, other scope" {
		t.Fatalf("other-scope event=%#v error=%v", other, err)
	}

	candidate := candidate(tenantID, userID, "candidate-lifecycle", "seat_preference", "window", []string{second.ID, first.ID}, 4)
	if err := storage.CreateCandidate(ctx, candidate); err != nil {
		t.Fatalf("create candidate: %v", err)
	}
	loadedCandidate, err := storage.CandidateByID(ctx, tenantID, userID, candidate.ID)
	if err != nil {
		t.Fatalf("load candidate: %v", err)
	}
	if got := loadedCandidate.SourceEventIDs; len(got) != 2 || got[0] != second.ID || got[1] != first.ID {
		t.Fatalf("candidate source order=%v, want [%s %s]", got, second.ID, first.ID)
	}
	if loadedCandidate.Metadata["fixture"] != "postgres-integration" {
		t.Fatalf("candidate metadata=%v", loadedCandidate.Metadata)
	}

	reviewed, memory, err := storage.ReviewCandidate(ctx, approval(candidate, "memory-lifecycle", 5))
	if err != nil {
		t.Fatalf("approve candidate: %v", err)
	}
	if reviewed.Status != domain.CandidateApproved || reviewed.Review == nil || memory == nil {
		t.Fatalf("reviewed=%#v memory=%#v", reviewed, memory)
	}
	if memory.Version != 1 || memory.Status != domain.MemoryActive {
		t.Fatalf("memory version/status=%d/%s, want 1/active", memory.Version, memory.Status)
	}
	if got := memory.SourceEventIDs; len(got) != 2 || got[0] != second.ID || got[1] != first.ID {
		t.Fatalf("memory source order=%v, want [%s %s]", got, second.ID, first.ID)
	}
	active, err := storage.ListServiceableMemories(ctx, tenantID, userID, fixtureTime(100))
	if err != nil || len(active) != 1 || active[0].ID != memory.ID {
		t.Fatalf("active=%#v error=%v", active, err)
	}
	revision, err := storage.ContextRevision(ctx, tenantID, userID)
	if err != nil || revision != 1 {
		t.Fatalf("revision=%d error=%v, want 1", revision, err)
	}
}

func TestPostgresStoreConcurrentReviewOfOneCandidateHasOneWinner(t *testing.T) {
	ctx := context.Background()
	databaseURL := requiredDatabaseURL(t)
	applyMigrations(t, databaseURL)
	tenantID, userID := uniqueScope("one_winner")
	cleanupScopes(t, databaseURL, [][2]string{{tenantID, userID}})
	storage := openStore(t, databaseURL)
	defer storage.Close()

	event := evidence(tenantID, userID, "event-one-winner", "likes tea", 1)
	mustAppend(t, storage, event)
	candidate := candidate(tenantID, userID, "candidate-one-winner", "drink", "tea", []string{event.ID}, 2)
	mustCreateCandidate(t, storage, candidate)

	type result struct {
		memory *domain.MemoryCard
		err    error
	}
	start := make(chan struct{})
	results := make(chan result, 2)
	var wait sync.WaitGroup
	for index := 0; index < 2; index++ {
		index := index
		wait.Add(1)
		go func() {
			defer wait.Done()
			<-start
			_, memory, err := storage.ReviewCandidate(context.Background(), approval(candidate, fmt.Sprintf("memory-winner-%d", index), 3+index))
			results <- result{memory: memory, err: err}
		}()
	}
	close(start)
	wait.Wait()
	close(results)

	successes, conflicts := 0, 0
	for result := range results {
		switch {
		case result.err == nil:
			successes++
			if result.memory == nil || result.memory.Version != 1 {
				t.Fatalf("winning memory=%#v", result.memory)
			}
		case errors.Is(result.err, domain.ErrConflict):
			conflicts++
		default:
			t.Fatalf("unexpected concurrent review error: %v", result.err)
		}
	}
	if successes != 1 || conflicts != 1 {
		t.Fatalf("successes/conflicts=%d/%d, want 1/1", successes, conflicts)
	}
	revision, err := storage.ContextRevision(ctx, tenantID, userID)
	if err != nil || revision != 1 {
		t.Fatalf("revision=%d error=%v, want 1", revision, err)
	}
	active, err := storage.ListServiceableMemories(ctx, tenantID, userID, fixtureTime(100))
	if err != nil || len(active) != 1 {
		t.Fatalf("active=%#v error=%v", active, err)
	}
}

func TestPostgresStoreConcurrentSameIdentityFormsVersionChain(t *testing.T) {
	ctx := context.Background()
	databaseURL := requiredDatabaseURL(t)
	applyMigrations(t, databaseURL)
	tenantID, userID := uniqueScope("version_chain")
	cleanupScopes(t, databaseURL, [][2]string{{tenantID, userID}})
	storage := openStore(t, databaseURL)
	defer storage.Close()

	firstEvent := evidence(tenantID, userID, "event-version-one", "window", 1)
	secondEvent := evidence(tenantID, userID, "event-version-two", "aisle", 2)
	mustAppend(t, storage, firstEvent)
	mustAppend(t, storage, secondEvent)
	first := candidate(tenantID, userID, "candidate-version-one", "seat_preference", "window", []string{firstEvent.ID}, 3)
	second := candidate(tenantID, userID, "candidate-version-two", "seat_preference", "aisle", []string{secondEvent.ID}, 4)
	mustCreateCandidate(t, storage, first)
	mustCreateCandidate(t, storage, second)

	type result struct {
		version int
		err     error
	}
	start := make(chan struct{})
	results := make(chan result, 2)
	var wait sync.WaitGroup
	for index, current := range []domain.MemoryCandidate{first, second} {
		index, current := index, current
		wait.Add(1)
		go func() {
			defer wait.Done()
			<-start
			_, memory, err := storage.ReviewCandidate(context.Background(), approval(current, fmt.Sprintf("memory-version-%d", index+1), 5+index))
			if err != nil {
				results <- result{err: err}
				return
			}
			results <- result{version: memory.Version}
		}()
	}
	close(start)
	wait.Wait()
	close(results)

	versions := make([]int, 0, 2)
	for result := range results {
		if result.err != nil {
			t.Fatalf("approve same identity: %v", result.err)
		}
		versions = append(versions, result.version)
	}
	sort.Ints(versions)
	if len(versions) != 2 || versions[0] != 1 || versions[1] != 2 {
		t.Fatalf("versions=%v, want [1 2]", versions)
	}
	active, err := storage.ListServiceableMemories(ctx, tenantID, userID, fixtureTime(100))
	if err != nil || len(active) != 1 || active[0].Version != 2 {
		t.Fatalf("active=%#v error=%v, want only version 2", active, err)
	}
	assertCardStatuses(t, databaseURL, tenantID, userID, 1, 1)
}

func TestPostgresStoreSurvivesPoolCloseAndReopen(t *testing.T) {
	ctx := context.Background()
	databaseURL := requiredDatabaseURL(t)
	applyMigrations(t, databaseURL)
	tenantID, userID := uniqueScope("reopen")
	cleanupScopes(t, databaseURL, [][2]string{{tenantID, userID}})

	firstStore := openStore(t, databaseURL)
	event := evidence(tenantID, userID, "event-reopen", "persistent preference", 1)
	mustAppend(t, firstStore, event)
	candidate := candidate(tenantID, userID, "candidate-reopen", "preference", "persistent", []string{event.ID}, 2)
	mustCreateCandidate(t, firstStore, candidate)
	_, expectedMemory, err := firstStore.ReviewCandidate(ctx, approval(candidate, "memory-reopen", 3))
	if err != nil {
		firstStore.Close()
		t.Fatalf("approve before reopen: %v", err)
	}
	firstStore.Close()

	secondStore := openStore(t, databaseURL)
	defer secondStore.Close()
	loadedEvent, err := secondStore.EvidenceByID(ctx, tenantID, userID, event.ID)
	if err != nil || loadedEvent.Content != event.Content {
		t.Fatalf("reopened evidence=%#v error=%v", loadedEvent, err)
	}
	loadedCandidate, err := secondStore.CandidateByID(ctx, tenantID, userID, candidate.ID)
	if err != nil || loadedCandidate.Status != domain.CandidateApproved {
		t.Fatalf("reopened candidate=%#v error=%v", loadedCandidate, err)
	}
	active, err := secondStore.ListServiceableMemories(ctx, tenantID, userID, fixtureTime(100))
	if err != nil || len(active) != 1 || active[0].ID != expectedMemory.ID {
		t.Fatalf("reopened active=%#v error=%v", active, err)
	}
	revision, err := secondStore.ContextRevision(ctx, tenantID, userID)
	if err != nil || revision != 1 {
		t.Fatalf("reopened revision=%d error=%v", revision, err)
	}
}

func TestPostgresStoreExpirationPersistsFiltersReopensAndForgets(t *testing.T) {
	ctx := context.Background()
	databaseURL := requiredDatabaseURL(t)
	applyMigrations(t, databaseURL)
	tenantID, userID := uniqueScope("expiration")
	cleanupScopes(t, databaseURL, [][2]string{{tenantID, userID}})

	asOf := fixtureTime(30)
	past := asOf.Add(-time.Microsecond)
	boundary := asOf
	future := asOf.Add(time.Hour)
	type expirationFixture struct {
		id        string
		expiresAt *time.Time
	}
	fixtures := []expirationFixture{
		{id: "no-expiration"},
		{id: "future", expiresAt: &future},
		{id: "boundary", expiresAt: &boundary},
		{id: "past", expiresAt: &past},
	}

	firstStore := openStore(t, databaseURL)
	defer firstStore.Close()
	for index, fixture := range fixtures {
		event := evidence(tenantID, userID, "event-expiration-"+fixture.id, fixture.id, 1+index)
		mustAppend(t, firstStore, event)
		value := candidate(
			tenantID,
			userID,
			"candidate-expiration-"+fixture.id,
			"expiration_"+fixture.id,
			fixture.id,
			[]string{event.ID},
			5+index,
		)
		value.ExpiresAt = cloneOptionalTime(fixture.expiresAt)
		mustCreateCandidate(t, firstStore, value)

		loadedCandidate, err := firstStore.CandidateByID(ctx, tenantID, userID, value.ID)
		if err != nil {
			firstStore.Close()
			t.Fatalf("load candidate %q expiration: %v", fixture.id, err)
		}
		assertOptionalTime(t, "candidate "+fixture.id, loadedCandidate.ExpiresAt, fixture.expiresAt)

		reviewed, memory, err := firstStore.ReviewCandidate(ctx, approval(value, "memory-expiration-"+fixture.id, 10+index))
		if err != nil {
			firstStore.Close()
			t.Fatalf("approve candidate %q: %v", fixture.id, err)
		}
		assertOptionalTime(t, "reviewed candidate "+fixture.id, reviewed.ExpiresAt, fixture.expiresAt)
		if memory == nil {
			firstStore.Close()
			t.Fatalf("approved memory %q is nil", fixture.id)
		}
		assertOptionalTime(t, "approved memory "+fixture.id, memory.ExpiresAt, fixture.expiresAt)
	}
	firstStore.Close()

	// Reapplying the embedded migrations must preserve rows using the new
	// nullable column, just as restarting against an existing volume does.
	applyMigrations(t, databaseURL)
	secondStore := openStore(t, databaseURL)
	defer secondStore.Close()

	beforeExpiration, err := secondStore.ListServiceableMemories(ctx, tenantID, userID, fixtureTime(20))
	if err != nil {
		t.Fatalf("list before expiration after reopen: %v", err)
	}
	if len(beforeExpiration) != len(fixtures) {
		t.Fatalf("memories before expiration=%d, want %d", len(beforeExpiration), len(fixtures))
	}
	expectedExpiration := make(map[string]*time.Time, len(fixtures))
	for _, fixture := range fixtures {
		expectedExpiration["memory-expiration-"+fixture.id] = fixture.expiresAt
	}
	for _, memory := range beforeExpiration {
		want, exists := expectedExpiration[memory.ID]
		if !exists {
			t.Fatalf("unexpected memory after reopen: %q", memory.ID)
		}
		assertOptionalTime(t, "reopened memory "+memory.ID, memory.ExpiresAt, want)
	}

	serviceable, err := secondStore.ListServiceableMemories(ctx, tenantID, userID, asOf)
	if err != nil {
		t.Fatalf("list at expiration boundary: %v", err)
	}
	gotIDs := make([]string, len(serviceable))
	for index := range serviceable {
		gotIDs[index] = serviceable[index].ID
	}
	wantIDs := []string{"memory-expiration-no-expiration", "memory-expiration-future"}
	if len(gotIDs) != len(wantIDs) || gotIDs[0] != wantIDs[0] || gotIDs[1] != wantIDs[1] {
		t.Fatalf("serviceable memory ids at boundary=%v, want %v", gotIDs, wantIDs)
	}
	assertCardStatuses(t, databaseURL, tenantID, userID, len(fixtures), 0)

	receipt, err := secondStore.ForgetUser(ctx, tenantID, userID, asOf.Add(time.Hour))
	if err != nil {
		t.Fatalf("forget user with expired cards: %v", err)
	}
	if receipt.EvidenceDeleted != len(fixtures) || receipt.CandidatesDeleted != len(fixtures) || receipt.MemoriesDeleted != len(fixtures) {
		t.Fatalf("expiration deletion receipt=%#v, want %d evidence/candidates/memories", receipt, len(fixtures))
	}
	remaining, err := secondStore.ListServiceableMemories(ctx, tenantID, userID, fixtureTime(20))
	if err != nil || len(remaining) != 0 {
		t.Fatalf("memories after expiration-scope deletion=%#v error=%v", remaining, err)
	}
	assertDeletedScopeRows(t, databaseURL, tenantID, userID, int64(len(fixtures)+1))
}

func TestPostgresFTSSchemaAndMetadata(t *testing.T) {
	ctx := context.Background()
	databaseURL := requiredDatabaseURL(t)
	applyMigrations(t, databaseURL)
	applyMigrations(t, databaseURL)
	storage := openStore(t, databaseURL)
	defer storage.Close()

	metadata, err := storage.FTSMetadata(ctx)
	if err != nil {
		t.Fatalf("load FTS metadata: %v", err)
	}
	serverVersion, err := strconv.Atoi(metadata.ServerVersionNum)
	if err != nil || serverVersion <= 0 {
		t.Fatalf("server_version_num=%q error=%v, want positive integer", metadata.ServerVersionNum, err)
	}
	if metadata.SchemaMigrationVersion < 3 {
		t.Fatalf("schema migration version=%d, want at least 3", metadata.SchemaMigrationVersion)
	}
	if metadata.TextSearchConfig != postgres.FTSTextSearchConfig ||
		metadata.QueryStrategy != postgres.FTSQueryStrategy ||
		metadata.RankFunction != postgres.FTSRankFunction {
		t.Fatalf("unexpected FTS metadata: %#v", metadata)
	}

	conn, err := pgx.Connect(ctx, databaseURL)
	if err != nil {
		t.Fatalf("connect to inspect FTS schema: %v", err)
	}
	defer conn.Close(context.Background())
	var generated, expression string
	if err := conn.QueryRow(ctx, `
		SELECT is_generated, generation_expression
		FROM information_schema.columns
		WHERE table_schema = 'agent_memory'
		  AND table_name = 'memory_cards'
		  AND column_name = 'search_document'
	`).Scan(&generated, &expression); err != nil {
		t.Fatalf("inspect generated search document: %v", err)
	}
	lowerExpression := strings.ToLower(expression)
	if generated != "ALWAYS" ||
		!strings.Contains(lowerExpression, "to_tsvector") ||
		!strings.Contains(lowerExpression, "simple") ||
		!strings.Contains(lowerExpression, "setweight") {
		t.Fatalf("search_document generated=%q expression=%q", generated, expression)
	}
	for _, field := range []string{"memory_key", "value", "category", "person", "relationship", "backstory"} {
		if !strings.Contains(lowerExpression, field) {
			t.Fatalf("search_document expression omits %s: %q", field, expression)
		}
	}

	var indexDefinition string
	if err := conn.QueryRow(ctx, `
		SELECT indexdef
		FROM pg_indexes
		WHERE schemaname = 'agent_memory'
		  AND tablename = 'memory_cards'
		  AND indexname = 'memory_cards_active_search_document_gin_idx'
	`).Scan(&indexDefinition); err != nil {
		t.Fatalf("inspect FTS index: %v", err)
	}
	lowerIndex := strings.ToLower(indexDefinition)
	if !strings.Contains(lowerIndex, "using gin") ||
		!strings.Contains(lowerIndex, "search_document") ||
		!strings.Contains(lowerIndex, "where") ||
		!strings.Contains(lowerIndex, "status") ||
		!strings.Contains(lowerIndex, "active") {
		t.Fatalf("unexpected FTS index definition: %q", indexDefinition)
	}
}

func TestPostgresFTSSearchRanksFiltersDeterministicallyAndForgets(t *testing.T) {
	ctx := context.Background()
	databaseURL := requiredDatabaseURL(t)
	applyMigrations(t, databaseURL)
	tenantID, userID := uniqueScope("fts")
	otherTenantID, _ := uniqueScope("fts_other_tenant")
	_, otherUserID := uniqueScope("fts_other_user")
	cleanupScopes(t, databaseURL, [][2]string{
		{tenantID, userID},
		{otherTenantID, userID},
		{tenantID, otherUserID},
	})
	storage := openStore(t, databaseURL)
	defer storage.Close()

	asOf := fixtureTime(100)
	past := asOf.Add(-time.Microsecond)
	equal := asOf
	future := asOf.Add(time.Hour)
	const rankTerm = "quartzmarker"

	seedOne := func(scopeTenant, scopeUser, id, key, value string, eventOffset, reviewOffset int, configure func(*domain.MemoryCandidate)) domain.MemoryCard {
		t.Helper()
		event := evidence(scopeTenant, scopeUser, "event-"+id, "source "+id, eventOffset)
		mustAppend(t, storage, event)
		candidate := candidate(scopeTenant, scopeUser, "candidate-"+id, key, value, []string{event.ID}, eventOffset+1)
		if configure != nil {
			configure(&candidate)
		}
		mustCreateCandidate(t, storage, candidate)
		_, memory, err := storage.ReviewCandidate(ctx, approval(candidate, "memory-"+id, reviewOffset))
		if err != nil {
			t.Fatalf("approve FTS fixture %q: %v", id, err)
		}
		if memory == nil {
			t.Fatalf("approved FTS fixture %q has nil memory", id)
		}
		return *memory
	}

	firstSource := evidence(tenantID, userID, "event-rank-key-first", "first ordered source", 1)
	secondSource := evidence(tenantID, userID, "event-rank-key-second", "second ordered source", 2)
	mustAppend(t, storage, firstSource)
	mustAppend(t, storage, secondSource)
	keyCandidate := candidate(
		tenantID,
		userID,
		"candidate-rank-key",
		rankTerm,
		"key-weight match",
		[]string{secondSource.ID, firstSource.ID},
		3,
	)
	mustCreateCandidate(t, storage, keyCandidate)
	_, keyMemory, err := storage.ReviewCandidate(ctx, approval(keyCandidate, "memory-rank-key", 4))
	if err != nil || keyMemory == nil {
		t.Fatalf("approve key-rank fixture memory=%#v error=%v", keyMemory, err)
	}

	seedOne(tenantID, userID, "rank-value", "value_match", rankTerm, 5, 7, func(candidate *domain.MemoryCandidate) {
		candidate.ExpiresAt = &future
	})
	seedOne(tenantID, userID, "rank-category", "category_match", "category body", 8, 10, func(candidate *domain.MemoryCandidate) {
		candidate.Category = rankTerm
	})
	seedOne(tenantID, userID, "rank-backstory", "backstory_match", "backstory body", 11, 13, func(candidate *domain.MemoryCandidate) {
		candidate.Backstory = rankTerm
	})
	seedOne(tenantID, userID, "expired-past", "expired_past", rankTerm, 14, 16, func(candidate *domain.MemoryCandidate) {
		candidate.ExpiresAt = &past
	})
	seedOne(tenantID, userID, "expired-equal", "expired_equal", rankTerm, 17, 19, func(candidate *domain.MemoryCandidate) {
		candidate.ExpiresAt = &equal
	})
	seedOne(tenantID, userID, "version-old", "versioned_identity", rankTerm, 20, 22, nil)
	seedOne(tenantID, userID, "version-new", "versioned_identity", "replacement without search term", 23, 25, nil)
	seedOne(tenantID, userID, "tie-b", "tie_b", "tielexeme", 30, 40, nil)
	seedOne(tenantID, userID, "tie-a", "tie_a", "tielexeme", 31, 40, nil)
	seedOne(tenantID, userID, "or-alpha", "or_alpha", "alpha", 42, 44, nil)
	seedOne(tenantID, userID, "or-beta", "or_beta", "beta", 45, 47, nil)
	seedOne(otherTenantID, userID, "foreign-tenant", rankTerm, rankTerm, 50, 52, nil)
	seedOne(tenantID, otherUserID, "foreign-user", rankTerm, rankTerm, 53, 55, nil)

	rankHits, err := storage.Search(ctx, tenantID, userID, rankTerm, 20, asOf)
	if err != nil {
		t.Fatalf("rank FTS results: %v", err)
	}
	wantRankIDs := []string{"memory-rank-key", "memory-rank-value", "memory-rank-category", "memory-rank-backstory"}
	if len(rankHits) != len(wantRankIDs) {
		t.Fatalf("rank hit count=%d, want %d: %#v", len(rankHits), len(wantRankIDs), rankHits)
	}
	for index, wantID := range wantRankIDs {
		if rankHits[index].Memory.ID != wantID {
			t.Fatalf("rank hit %d id=%q, want %q: %#v", index, rankHits[index].Memory.ID, wantID, rankHits)
		}
		if index > 0 && rankHits[index-1].Score <= rankHits[index].Score {
			t.Fatalf("rank scores are not strictly descending: %#v", rankHits)
		}
	}
	if got := rankHits[0].Memory.SourceEventIDs; len(got) != 2 || got[0] != secondSource.ID || got[1] != firstSource.ID {
		t.Fatalf("FTS source order=%v, want [%s %s]", got, secondSource.ID, firstSource.ID)
	}
	limitedHits, err := storage.Search(ctx, tenantID, userID, rankTerm, 2, asOf)
	if err != nil || len(limitedHits) != 2 || limitedHits[0].Memory.ID != wantRankIDs[0] || limitedHits[1].Memory.ID != wantRankIDs[1] {
		t.Fatalf("limited rank hits=%#v error=%v", limitedHits, err)
	}

	unsafeLookingQuery := rankTerm + " | !:* '); DROP TABLE agent_memory.memory_cards; --"
	unsafeHits, err := storage.Search(ctx, tenantID, userID, unsafeLookingQuery, 20, asOf)
	if err != nil || len(unsafeHits) < len(wantRankIDs) || unsafeHits[0].Memory.ID != wantRankIDs[0] {
		t.Fatalf("plain-text query syntax isolation hits=%#v error=%v", unsafeHits, err)
	}

	orHits, err := storage.Search(ctx, tenantID, userID, "alpha beta", 20, asOf)
	if err != nil {
		t.Fatalf("OR search: %v", err)
	}
	if len(orHits) != 2 || orHits[0].Memory.ID != "memory-or-beta" || orHits[1].Memory.ID != "memory-or-alpha" {
		t.Fatalf("OR hits=%#v, want both alpha and beta ordered by recency", orHits)
	}

	for attempt := 0; attempt < 10; attempt++ {
		tieHits, err := storage.Search(ctx, tenantID, userID, "tielexeme", 20, asOf)
		if err != nil {
			t.Fatalf("tie search attempt %d: %v", attempt, err)
		}
		if len(tieHits) != 2 || tieHits[0].Memory.ID != "memory-tie-a" || tieHits[1].Memory.ID != "memory-tie-b" {
			t.Fatalf("tie search attempt %d hits=%#v", attempt, tieHits)
		}
		if tieHits[0].Score != tieHits[1].Score {
			t.Fatalf("tie scores differ: %#v", tieHits)
		}
	}
	for _, input := range []struct {
		query string
		limit int
	}{
		{query: "", limit: 20},
		{query: "   ", limit: 20},
		{query: "!!!", limit: 20},
		{query: rankTerm, limit: 0},
		{query: rankTerm, limit: -1},
	} {
		hits, err := storage.Search(ctx, tenantID, userID, input.query, input.limit, asOf)
		if err != nil || len(hits) != 0 {
			t.Fatalf("empty search contract query=%q limit=%d hits=%#v error=%v", input.query, input.limit, hits, err)
		}
	}

	receipt, err := storage.ForgetUser(ctx, tenantID, userID, asOf.Add(time.Hour))
	if err != nil {
		t.Fatalf("forget FTS scope: %v", err)
	}
	if receipt.EvidenceDeleted != 13 || receipt.CandidatesDeleted != 12 || receipt.MemoriesDeleted != 12 {
		t.Fatalf("FTS deletion receipt=%#v, want evidence/candidates/memories 13/12/12", receipt)
	}
	afterForget, err := storage.Search(ctx, tenantID, userID, rankTerm, 20, asOf)
	if err != nil || len(afterForget) != 0 {
		t.Fatalf("FTS results after forget=%#v error=%v", afterForget, err)
	}
	foreignHits, err := storage.Search(ctx, otherTenantID, userID, rankTerm, 20, asOf)
	if err != nil || len(foreignHits) != 1 || foreignHits[0].Memory.ID != "memory-foreign-tenant" {
		t.Fatalf("foreign control results=%#v error=%v", foreignHits, err)
	}
	assertDeletedScopeRows(t, databaseURL, tenantID, userID, 13)
}

func TestDeleteEvaluationScopeStateIsPrefixAndContentGuarded(t *testing.T) {
	ctx := context.Background()
	databaseURL := requiredDatabaseURL(t)
	applyMigrations(t, databaseURL)
	rawTenantID, rawUserID := uniqueScope("eval_cleanup")
	tenantID, userID := "eval_"+rawTenantID, "eval_"+rawUserID
	cleanupScopes(t, databaseURL, [][2]string{{tenantID, userID}})
	evalCleanupSpace := registerProjectionTarget(t, databaseURL, "eval_cleanup", "shadow", true)
	storage := openStore(t, databaseURL)
	defer storage.Close()

	if err := storage.DeleteEvaluationScopeState(ctx, "tenant-production", userID); !errors.Is(err, domain.ErrInvalid) {
		t.Fatalf("non-evaluation tenant cleanup error=%v, want invalid", err)
	}
	if err := storage.DeleteEvaluationScopeState(ctx, tenantID, "user-production"); !errors.Is(err, domain.ErrInvalid) {
		t.Fatalf("non-evaluation user cleanup error=%v, want invalid", err)
	}

	candidate := seedCandidate(t, storage, tenantID, userID, "eval-cleanup", "cleanup_key", "cleanup value", 1)
	if _, _, err := storage.ReviewCandidate(ctx, approval(candidate, "memory-eval-cleanup", 3)); err != nil {
		t.Fatalf("approve evaluation cleanup fixture: %v", err)
	}
	if err := storage.DeleteEvaluationScopeState(ctx, tenantID, userID); !errors.Is(err, domain.ErrConflict) {
		t.Fatalf("cleanup with lifecycle content error=%v, want conflict", err)
	}
	assertTableRowCount(t, databaseURL, "evidence_events", tenantID, userID, 1)
	assertTableRowCount(t, databaseURL, "memory_candidates", tenantID, userID, 1)
	assertTableRowCount(t, databaseURL, "candidate_source_events", tenantID, userID, 1)
	assertTableRowCount(t, databaseURL, "memory_cards", tenantID, userID, 1)
	if countProjectionJobsForMemoryAndSpace(t, databaseURL, tenantID, userID, "memory-eval-cleanup", evalCleanupSpace) != 1 {
		t.Fatal("evaluation fixture did not enqueue its projection job")
	}
	assertTableRowCount(t, databaseURL, "memory_identity_chains", tenantID, userID, 1)

	receipt, err := storage.ForgetUser(ctx, tenantID, userID, fixtureTime(4))
	if err != nil {
		t.Fatalf("forget evaluation scope: %v", err)
	}
	if receipt.EvidenceDeleted != 1 || receipt.CandidatesDeleted != 1 || receipt.MemoriesDeleted != 1 {
		t.Fatalf("evaluation cleanup receipt=%#v, want 1/1/1", receipt)
	}
	if err := storage.DeleteEvaluationScopeState(ctx, tenantID, userID); err != nil {
		t.Fatalf("delete empty evaluation scope state: %v", err)
	}
	if err := storage.DeleteEvaluationScopeState(ctx, tenantID, userID); err != nil {
		t.Fatalf("repeat evaluation scope cleanup: %v", err)
	}
	assertScopeCompletelyAbsent(t, databaseURL, tenantID, userID)
}

func TestPostgresStoreCreateCandidateWithMissingSourceRollsBack(t *testing.T) {
	ctx := context.Background()
	databaseURL := requiredDatabaseURL(t)
	applyMigrations(t, databaseURL)
	tenantID, userID := uniqueScope("missing_source")
	cleanupScopes(t, databaseURL, [][2]string{{tenantID, userID}})
	storage := openStore(t, databaseURL)
	defer storage.Close()

	realEvent := evidence(tenantID, userID, "event-real", "real source", 1)
	mustAppend(t, storage, realEvent)
	value := candidate(tenantID, userID, "candidate-missing-source", "preference", "value", []string{realEvent.ID, "event-missing"}, 2)
	if err := storage.CreateCandidate(ctx, value); !errors.Is(err, domain.ErrNotFound) {
		t.Fatalf("create candidate error=%v, want not found", err)
	}
	if _, err := storage.CandidateByID(ctx, tenantID, userID, value.ID); !errors.Is(err, domain.ErrNotFound) {
		t.Fatalf("candidate after failed create error=%v, want not found", err)
	}
	assertTableRowCount(t, databaseURL, "memory_candidates", tenantID, userID, 0)
	assertTableRowCount(t, databaseURL, "candidate_source_events", tenantID, userID, 0)
	if _, err := storage.EvidenceByID(ctx, tenantID, userID, realEvent.ID); err != nil {
		t.Fatalf("valid source was changed by failed candidate transaction: %v", err)
	}
}

func TestPostgresStoreCreateCandidateBatchIsAtomicAndRevisionFenced(t *testing.T) {
	ctx := context.Background()
	databaseURL := requiredDatabaseURL(t)
	applyMigrations(t, databaseURL)
	tenantID, userID := uniqueScope("candidate_batch")
	cleanupScopes(t, databaseURL, [][2]string{{tenantID, userID}})
	storage := openStore(t, databaseURL)
	defer storage.Close()

	firstEvent := evidence(tenantID, userID, "event-batch-first", "first source", 1)
	secondEvent := evidence(tenantID, userID, "event-batch-second", "second source", 2)
	mustAppend(t, storage, firstEvent)
	mustAppend(t, storage, secondEvent)
	first := candidate(tenantID, userID, "candidate-batch-first", "first", "one", []string{firstEvent.ID}, 3)
	second := candidate(tenantID, userID, "candidate-batch-second", "second", "two", []string{secondEvent.ID}, 4)
	if err := storage.CreateCandidateBatch(ctx, storecontract.CandidateBatchCommand{
		TenantID: tenantID, UserID: userID, ExpectedRevision: 0,
		Candidates: []domain.MemoryCandidate{first, second},
	}); err != nil {
		t.Fatalf("create candidate batch: %v", err)
	}
	for _, value := range []domain.MemoryCandidate{first, second} {
		loaded, err := storage.CandidateByID(ctx, tenantID, userID, value.ID)
		if err != nil {
			t.Fatalf("load candidate %q: %v", value.ID, err)
		}
		if loaded.Status != domain.CandidatePending || loaded.Review != nil || len(loaded.SourceEventIDs) != 1 || loaded.SourceEventIDs[0] != value.SourceEventIDs[0] {
			t.Fatalf("loaded candidate %q=%#v", value.ID, loaded)
		}
	}
	if revision, err := storage.ContextRevision(ctx, tenantID, userID); err != nil || revision != 0 {
		t.Fatalf("revision after pending batch=%d error=%v, want 0", revision, err)
	}

	valid := candidate(tenantID, userID, "candidate-batch-valid-rollback", "valid", "valid", []string{firstEvent.ID}, 5)
	missing := candidate(tenantID, userID, "candidate-batch-missing", "missing", "missing", []string{"event-missing"}, 6)
	err := storage.CreateCandidateBatch(ctx, storecontract.CandidateBatchCommand{
		TenantID: tenantID, UserID: userID, ExpectedRevision: 0,
		Candidates: []domain.MemoryCandidate{valid, missing},
	})
	if !errors.Is(err, domain.ErrNotFound) {
		t.Fatalf("create batch with missing source error=%v, want not found", err)
	}
	for _, candidateID := range []string{valid.ID, missing.ID} {
		if _, err := storage.CandidateByID(ctx, tenantID, userID, candidateID); !errors.Is(err, domain.ErrNotFound) {
			t.Fatalf("candidate %q after failed batch error=%v, want not found", candidateID, err)
		}
	}
	assertTableRowCount(t, databaseURL, "memory_candidates", tenantID, userID, 2)
	assertTableRowCount(t, databaseURL, "candidate_source_events", tenantID, userID, 2)

	insertedBeforeFailure := candidate(
		tenantID,
		userID,
		"candidate-batch-inserted-before-failure",
		"inserted-before-failure",
		"one",
		[]string{firstEvent.ID},
		7,
	)
	databaseRejected := candidate(
		tenantID,
		userID,
		"candidate-batch-database-rejected",
		"database-rejected",
		"two",
		[]string{secondEvent.ID},
		8,
	)
	databaseRejected.Kind = domain.MemoryKind("unsupported-kind")
	err = storage.CreateCandidateBatch(ctx, storecontract.CandidateBatchCommand{
		TenantID: tenantID, UserID: userID, ExpectedRevision: 0,
		Candidates: []domain.MemoryCandidate{insertedBeforeFailure, databaseRejected},
	})
	if err == nil {
		t.Fatal("database constraint failure in second insert was accepted")
	}
	for _, candidateID := range []string{insertedBeforeFailure.ID, databaseRejected.ID} {
		if _, err := storage.CandidateByID(ctx, tenantID, userID, candidateID); !errors.Is(err, domain.ErrNotFound) {
			t.Fatalf("candidate %q after mid-insert rollback error=%v, want not found", candidateID, err)
		}
	}
	assertTableRowCount(t, databaseURL, "memory_candidates", tenantID, userID, 2)
	assertTableRowCount(t, databaseURL, "candidate_source_events", tenantID, userID, 2)

	wrongScope := candidate(tenantID, "other-user", "candidate-wrong-scope", "wrong", "wrong", []string{firstEvent.ID}, 9)
	if err := storage.CreateCandidateBatch(ctx, storecontract.CandidateBatchCommand{
		TenantID: tenantID, UserID: userID, ExpectedRevision: 0,
		Candidates: []domain.MemoryCandidate{wrongScope},
	}); !errors.Is(err, domain.ErrInvalid) {
		t.Fatalf("create cross-scope batch error=%v, want invalid", err)
	}

	if _, err := storage.ForgetUser(ctx, tenantID, userID, fixtureTime(10)); err != nil {
		t.Fatalf("forget user: %v", err)
	}
	replacement := firstEvent
	replacement.SessionID = "replacement-session"
	replacement.Content = "replacement source with reused id"
	replacement.OccurredAt = fixtureTime(11)
	replacement.RecordedAt = fixtureTime(11).Add(time.Millisecond)
	mustAppend(t, storage, replacement)
	stale := candidate(tenantID, userID, "candidate-stale-batch", "stale", "stale", []string{replacement.ID}, 12)
	if err := storage.CreateCandidateBatch(ctx, storecontract.CandidateBatchCommand{
		TenantID: tenantID, UserID: userID, ExpectedRevision: 0,
		Candidates: []domain.MemoryCandidate{stale},
	}); !errors.Is(err, domain.ErrConflict) {
		t.Fatalf("create stale batch after evidence id reuse error=%v, want conflict", err)
	}
	if _, err := storage.CandidateByID(ctx, tenantID, userID, stale.ID); !errors.Is(err, domain.ErrNotFound) {
		t.Fatalf("stale candidate after revision conflict error=%v, want not found", err)
	}
	if err := storage.CreateCandidateBatch(ctx, storecontract.CandidateBatchCommand{
		TenantID: tenantID, UserID: userID, ExpectedRevision: 0,
	}); !errors.Is(err, domain.ErrConflict) {
		t.Fatalf("empty stale batch error=%v, want conflict", err)
	}
}

func TestPostgresStoreApprovalEnqueuesOnlyEligibleProjectionTargetsAndRejectionDoesNotEnqueue(t *testing.T) {
	ctx := context.Background()
	databaseURL := requiredDatabaseURL(t)
	applyMigrations(t, databaseURL)
	tenantID, userID := uniqueScope("projection_enqueue")
	cleanupScopes(t, databaseURL, [][2]string{{tenantID, userID}})
	shadowSpace := registerProjectionTarget(t, databaseURL, "projection_enqueue_shadow", "shadow", true)
	servingSpace, servingEnqueueNew := projectionServingTargetForTest(t, databaseURL, "projection_enqueue_serving")
	disabledSpace := registerProjectionTarget(t, databaseURL, "projection_enqueue_disabled", "shadow", false)
	blockedSpace := registerProjectionTarget(t, databaseURL, "projection_enqueue_blocked", "blocked", false)
	_ = registerProjectionTarget(t, databaseURL, "projection_enqueue_unrelated", "shadow", true)

	storage := openStore(t, databaseURL)
	defer storage.Close()
	approvedCandidate := seedCandidate(t, storage, tenantID, userID, "projection-approved", "projection_key", "projection value", 1)
	_, card, err := storage.ReviewCandidate(ctx, approval(approvedCandidate, "memory-projection-approved", 3))
	if err != nil || card == nil {
		t.Fatalf("approve projection candidate: card=%#v error=%v", card, err)
	}

	eligibleSpaces := []string{shadowSpace}
	excludedSpaces := []string{disabledSpace, blockedSpace}
	if servingEnqueueNew {
		eligibleSpaces = append(eligibleSpaces, servingSpace)
	} else {
		excludedSpaces = append(excludedSpaces, servingSpace)
	}
	for _, eligibleSpace := range eligibleSpaces {
		job, found := projectionJobForMemoryAndSpace(t, databaseURL, tenantID, userID, card.ID, eligibleSpace)
		if !found || job.state != "pending" || job.expectedMemoryVersion != card.Version {
			t.Fatalf("projection job for eligible space %q=%#v found=%t, want pending card version %d", eligibleSpace, job, found, card.Version)
		}
	}
	for _, excludedSpace := range excludedSpaces {
		if countProjectionJobsForMemoryAndSpace(t, databaseURL, tenantID, userID, card.ID, excludedSpace) != 0 {
			t.Fatalf("excluded projection space %q received a job", excludedSpace)
		}
	}

	beforeRejection := countProjectionJobsForScope(t, databaseURL, tenantID, userID)
	rejectedCandidate := seedCandidate(t, storage, tenantID, userID, "projection-rejected", "rejected_key", "rejected value", 4)
	if _, rejectedCard, err := storage.ReviewCandidate(ctx, rejection(rejectedCandidate, 6)); err != nil || rejectedCard != nil {
		t.Fatalf("reject projection candidate: card=%#v error=%v", rejectedCard, err)
	}
	if afterRejection := countProjectionJobsForScope(t, databaseURL, tenantID, userID); afterRejection != beforeRejection {
		t.Fatalf("projection jobs after rejection=%d, want unchanged %d", afterRejection, beforeRejection)
	}
}

func TestPostgresStoreSupersedeDeletesOldEmbeddingAndProjectionJob(t *testing.T) {
	ctx := context.Background()
	databaseURL := requiredDatabaseURL(t)
	applyMigrations(t, databaseURL)
	tenantID, userID := uniqueScope("projection_supersede")
	cleanupScopes(t, databaseURL, [][2]string{{tenantID, userID}})
	embeddingSpace := registerProjectionTarget(t, databaseURL, "projection_supersede", "shadow", true)
	storage := openStore(t, databaseURL)
	defer storage.Close()

	first := seedCandidate(t, storage, tenantID, userID, "projection-old", "versioned_projection", "old value", 1)
	_, oldCard, err := storage.ReviewCandidate(ctx, approval(first, "memory-projection-old", 3))
	if err != nil || oldCard == nil {
		t.Fatalf("approve old projection card: card=%#v error=%v", oldCard, err)
	}
	if err := storage.UpsertMemoryEmbedding(ctx, projectionTestEmbedding(*oldCard, embeddingSpace, 4)); err != nil {
		t.Fatalf("insert old projection embedding: %v", err)
	}
	if countProjectionJobsForMemoryAndSpace(t, databaseURL, tenantID, userID, oldCard.ID, embeddingSpace) != 1 {
		t.Fatal("old projection job is missing before supersede")
	}
	assertEmbeddingCountForMemory(t, databaseURL, tenantID, userID, oldCard.ID, 1)

	second := seedCandidate(t, storage, tenantID, userID, "projection-new", "versioned_projection", "new value", 5)
	_, newCard, err := storage.ReviewCandidate(ctx, approval(second, "memory-projection-new", 7))
	if err != nil || newCard == nil {
		t.Fatalf("approve replacement projection card: card=%#v error=%v", newCard, err)
	}
	if countProjectionJobsForMemoryAndSpace(t, databaseURL, tenantID, userID, oldCard.ID, embeddingSpace) != 0 {
		t.Fatal("superseded projection job remains")
	}
	assertEmbeddingCountForMemory(t, databaseURL, tenantID, userID, oldCard.ID, 0)
	job, found := projectionJobForMemoryAndSpace(t, databaseURL, tenantID, userID, newCard.ID, embeddingSpace)
	if !found || job.expectedMemoryVersion != 2 || job.state != "pending" {
		t.Fatalf("replacement projection job=%#v found=%t, want pending version-2 job", job, found)
	}
}

func TestPostgresStoreProjectionJobFailureRollsBackApproval(t *testing.T) {
	ctx := context.Background()
	databaseURL := requiredDatabaseURL(t)
	applyMigrations(t, databaseURL)
	tenantID, userID := uniqueScope("projection_job_rollback")
	cleanupScopes(t, databaseURL, [][2]string{{tenantID, userID}})
	embeddingSpace := registerProjectionTarget(t, databaseURL, "projection_job_rollback", "shadow", true)
	storage := openStore(t, databaseURL)
	defer storage.Close()

	original := seedCandidate(t, storage, tenantID, userID, "projection-job-original", "rollback_projection", "original value", 1)
	_, originalCard, err := storage.ReviewCandidate(ctx, approval(original, "memory-projection-job-original", 3))
	if err != nil || originalCard == nil {
		t.Fatalf("approve original projection card: card=%#v error=%v", originalCard, err)
	}
	if err := storage.UpsertMemoryEmbedding(ctx, projectionTestEmbedding(*originalCard, embeddingSpace, 4)); err != nil {
		t.Fatalf("insert original projection embedding: %v", err)
	}
	revisionBefore, err := storage.ContextRevision(ctx, tenantID, userID)
	if err != nil {
		t.Fatalf("load revision before injected projection failure: %v", err)
	}

	candidate := seedCandidate(t, storage, tenantID, userID, "projection-job-rollback", "rollback_projection", "replacement value", 5)
	installProjectionJobFailureTrigger(t, databaseURL, tenantID, userID)
	_, card, err := storage.ReviewCandidate(ctx, approval(candidate, "memory-projection-job-rollback", 7))
	if err == nil || card != nil {
		t.Fatalf("approval with injected projection failure: card=%#v error=%v, want failure", card, err)
	}
	if strings.Contains(err.Error(), projectionJobFailureSecret) {
		t.Fatalf("approval error exposed injected database diagnostic: %q", err)
	}
	loaded, err := storage.CandidateByID(ctx, tenantID, userID, candidate.ID)
	if err != nil {
		t.Fatalf("load candidate after projection rollback: %v", err)
	}
	if loaded.Status != domain.CandidatePending || loaded.Review != nil {
		t.Fatalf("candidate after projection rollback=%#v, want pending without review", loaded)
	}
	if revision, err := storage.ContextRevision(ctx, tenantID, userID); err != nil || revision != revisionBefore {
		t.Fatalf("revision after projection rollback=%d error=%v, want unchanged %d", revision, err, revisionBefore)
	}
	active, err := storage.ListServiceableMemories(ctx, tenantID, userID, fixtureTime(100))
	if err != nil || len(active) != 1 || active[0].ID != originalCard.ID || active[0].Status != domain.MemoryActive {
		t.Fatalf("active memories after projection rollback=%#v error=%v, want original active card", active, err)
	}
	assertTableRowCount(t, databaseURL, "memory_cards", tenantID, userID, 1)
	assertTableRowCount(t, databaseURL, "memory_identity_chains", tenantID, userID, 1)
	assertIdentityLatestVersion(t, databaseURL, tenantID, userID, 1)
	assertEmbeddingCountForMemory(t, databaseURL, tenantID, userID, originalCard.ID, 1)
	if countProjectionJobsForMemoryAndSpace(t, databaseURL, tenantID, userID, originalCard.ID, embeddingSpace) != 1 {
		t.Fatal("original projection job was not restored by rollback")
	}
	if countProjectionJobsForMemoryAndSpace(t, databaseURL, tenantID, userID, "memory-projection-job-rollback", embeddingSpace) != 0 {
		t.Fatal("failed replacement left a projection job")
	}
}

func TestPostgresStoreApprovalFailureRollsBackProjectionAndReview(t *testing.T) {
	ctx := context.Background()
	databaseURL := requiredDatabaseURL(t)
	applyMigrations(t, databaseURL)
	tenantID, userID := uniqueScope("approval_rollback")
	cleanupScopes(t, databaseURL, [][2]string{{tenantID, userID}})
	storage := openStore(t, databaseURL)
	defer storage.Close()

	first := seedCandidate(t, storage, tenantID, userID, "rollback-first", "first_identity", "one", 1)
	second := seedCandidate(t, storage, tenantID, userID, "rollback-second", "first_identity", "two", 3)
	if _, _, err := storage.ReviewCandidate(ctx, approval(first, "memory-duplicate", 5)); err != nil {
		t.Fatalf("approve first candidate: %v", err)
	}
	if _, _, err := storage.ReviewCandidate(ctx, approval(second, "memory-duplicate", 6)); !errors.Is(err, domain.ErrConflict) {
		t.Fatalf("approve with duplicate memory id error=%v, want conflict", err)
	}

	loaded, err := storage.CandidateByID(ctx, tenantID, userID, second.ID)
	if err != nil {
		t.Fatalf("load candidate after failed approval: %v", err)
	}
	if loaded.Status != domain.CandidatePending || loaded.Review != nil {
		t.Fatalf("candidate after failed approval=%#v, want pending without review", loaded)
	}
	revision, err := storage.ContextRevision(ctx, tenantID, userID)
	if err != nil || revision != 1 {
		t.Fatalf("revision after failed approval=%d error=%v, want 1", revision, err)
	}
	assertTableRowCount(t, databaseURL, "memory_cards", tenantID, userID, 1)
	assertTableRowCount(t, databaseURL, "memory_identity_chains", tenantID, userID, 1)
	assertCardStatuses(t, databaseURL, tenantID, userID, 1, 0)
	active, err := storage.ListServiceableMemories(ctx, tenantID, userID, fixtureTime(100))
	if err != nil || len(active) != 1 || active[0].ID != "memory-duplicate" || active[0].Value != "one" {
		t.Fatalf("active memory after failed supersede=%#v error=%v", active, err)
	}
	assertIdentityLatestVersion(t, databaseURL, tenantID, userID, 1)
}

func TestPostgresStoreApproveRacingForgetHasNoPartialState(t *testing.T) {
	ctx := context.Background()
	databaseURL := requiredDatabaseURL(t)
	applyMigrations(t, databaseURL)
	tenantID, userID := uniqueScope("approve_forget")
	cleanupScopes(t, databaseURL, [][2]string{{tenantID, userID}})
	registerProjectionTarget(t, databaseURL, "approve_forget", "shadow", true)
	storage := openStore(t, databaseURL)
	defer storage.Close()

	candidate := seedCandidate(t, storage, tenantID, userID, "approve-forget", "preference", "window", 1)
	type approvalResult struct {
		err error
	}
	type deletionResult struct {
		receipt domain.DeletionReceipt
		err     error
	}
	approvals := make(chan approvalResult, 1)
	deletions := make(chan deletionResult, 1)
	start := make(chan struct{})
	go func() {
		<-start
		_, _, err := storage.ReviewCandidate(context.Background(), approval(candidate, "memory-approve-forget", 3))
		approvals <- approvalResult{err: err}
	}()
	go func() {
		<-start
		receipt, err := storage.ForgetUser(context.Background(), tenantID, userID, fixtureTime(4))
		deletions <- deletionResult{receipt: receipt, err: err}
	}()
	close(start)
	approvalOutcome := <-approvals
	deletionOutcome := <-deletions
	if deletionOutcome.err != nil {
		t.Fatalf("forget racing approval: %v", deletionOutcome.err)
	}

	wantRevision := int64(1)
	switch {
	case approvalOutcome.err == nil:
		wantRevision = 2
		if deletionOutcome.receipt.MemoriesDeleted != 1 {
			t.Fatalf("approval won but deletion receipt=%#v", deletionOutcome.receipt)
		}
	case errors.Is(approvalOutcome.err, domain.ErrNotFound):
		if deletionOutcome.receipt.MemoriesDeleted != 0 {
			t.Fatalf("deletion won but receipt=%#v", deletionOutcome.receipt)
		}
	default:
		t.Fatalf("approval race error=%v, want success or not found", approvalOutcome.err)
	}
	if deletionOutcome.receipt.EvidenceDeleted != 1 || deletionOutcome.receipt.CandidatesDeleted != 1 {
		t.Fatalf("deletion receipt=%#v, want evidence/candidates 1/1", deletionOutcome.receipt)
	}
	assertDeletedScopeRows(t, databaseURL, tenantID, userID, wantRevision)
	revision, err := storage.ContextRevision(ctx, tenantID, userID)
	if err != nil || int64(revision) != wantRevision {
		t.Fatalf("revision after approve/delete race=%d error=%v, want %d", revision, err, wantRevision)
	}
}

func TestPostgresStoreRevisionDoesNotABAAfterDeleteAndRecreate(t *testing.T) {
	ctx := context.Background()
	databaseURL := requiredDatabaseURL(t)
	applyMigrations(t, databaseURL)
	tenantID, userID := uniqueScope("revision_aba")
	cleanupScopes(t, databaseURL, [][2]string{{tenantID, userID}})
	storage := openStore(t, databaseURL)
	defer storage.Close()

	first := seedCandidate(t, storage, tenantID, userID, "aba-first", "preference", "window", 1)
	if _, _, err := storage.ReviewCandidate(ctx, approval(first, "memory-aba-first", 3)); err != nil {
		t.Fatalf("approve before delete: %v", err)
	}
	if _, err := storage.ForgetUser(ctx, tenantID, userID, fixtureTime(4)); err != nil {
		t.Fatalf("forget user: %v", err)
	}
	revisionAfterDelete, err := storage.ContextRevision(ctx, tenantID, userID)
	if err != nil || revisionAfterDelete != 2 {
		t.Fatalf("revision after delete=%d error=%v, want 2", revisionAfterDelete, err)
	}

	second := seedCandidate(t, storage, tenantID, userID, "aba-second", "preference", "aisle", 5)
	_, memory, err := storage.ReviewCandidate(ctx, approval(second, "memory-aba-second", 7))
	if err != nil {
		t.Fatalf("approve after delete: %v", err)
	}
	if memory == nil || memory.Version != 1 {
		t.Fatalf("memory after delete=%#v, want a new identity chain at version 1", memory)
	}
	revisionAfterRecreate, err := storage.ContextRevision(ctx, tenantID, userID)
	if err != nil || revisionAfterRecreate != 3 {
		t.Fatalf("revision after recreate=%d error=%v, want 3", revisionAfterRecreate, err)
	}
}

func TestPostgresStoreForgetPropagatesAndRetainsMonotonicRevision(t *testing.T) {
	ctx := context.Background()
	databaseURL := requiredDatabaseURL(t)
	applyMigrations(t, databaseURL)
	tenantID, userID := uniqueScope("forget")
	controlTenantID, controlUserID := uniqueScope("forget_control")
	cleanupScopes(t, databaseURL, [][2]string{{tenantID, userID}, {controlTenantID, controlUserID}})
	registerProjectionTarget(t, databaseURL, "forget", "shadow", true)
	storage := openStore(t, databaseURL)
	defer storage.Close()

	first := seedCandidate(t, storage, tenantID, userID, "approved-one", "preference", "window", 1)
	second := seedCandidate(t, storage, tenantID, userID, "approved-two", "preference", "aisle", 3)
	if _, _, err := storage.ReviewCandidate(ctx, approval(first, "memory-approved-one", 5)); err != nil {
		t.Fatalf("approve first version: %v", err)
	}
	if _, _, err := storage.ReviewCandidate(ctx, approval(second, "memory-approved-two", 6)); err != nil {
		t.Fatalf("approve second version: %v", err)
	}
	_ = seedCandidate(t, storage, tenantID, userID, "pending", "pending_key", "pending", 7)
	rejected := seedCandidate(t, storage, tenantID, userID, "rejected", "rejected_key", "rejected", 9)
	if _, memory, err := storage.ReviewCandidate(ctx, rejection(rejected, 11)); err != nil || memory != nil {
		t.Fatalf("reject candidate memory=%#v error=%v", memory, err)
	}

	control := seedCandidate(t, storage, controlTenantID, controlUserID, "control", "control_key", "keep", 12)
	if _, _, err := storage.ReviewCandidate(ctx, approval(control, "memory-control", 14)); err != nil {
		t.Fatalf("approve control candidate: %v", err)
	}

	before, err := storage.ContextRevision(ctx, tenantID, userID)
	if err != nil || before != 2 {
		t.Fatalf("revision before forget=%d error=%v, want 2", before, err)
	}
	deletedAt := fixtureTime(15)
	receipt, err := storage.ForgetUser(ctx, tenantID, userID, deletedAt)
	if err != nil {
		t.Fatalf("forget user: %v", err)
	}
	if receipt.EvidenceDeleted != 4 || receipt.CandidatesDeleted != 4 || receipt.MemoriesDeleted != 2 {
		t.Fatalf("deletion receipt=%#v, want evidence/candidates/memories 4/4/2", receipt)
	}
	if !receipt.DeletedAt.Equal(deletedAt) {
		t.Fatalf("receipt deleted_at=%s, want %s", receipt.DeletedAt, deletedAt)
	}

	after, err := storage.ContextRevision(ctx, tenantID, userID)
	if err != nil || after != before+1 {
		t.Fatalf("revision after forget=%d error=%v, want %d", after, err, before+1)
	}
	if _, err := storage.EvidenceByID(ctx, tenantID, userID, "event-approved-one"); !errors.Is(err, domain.ErrNotFound) {
		t.Fatalf("deleted evidence error=%v, want not found", err)
	}
	if _, err := storage.CandidateByID(ctx, tenantID, userID, first.ID); !errors.Is(err, domain.ErrNotFound) {
		t.Fatalf("deleted candidate error=%v, want not found", err)
	}
	active, err := storage.ListServiceableMemories(ctx, tenantID, userID, fixtureTime(100))
	if err != nil || len(active) != 0 {
		t.Fatalf("deleted active memories=%#v error=%v", active, err)
	}
	assertDeletedScopeRows(t, databaseURL, tenantID, userID, int64(after))

	controlActive, err := storage.ListServiceableMemories(ctx, controlTenantID, controlUserID, fixtureTime(100))
	if err != nil || len(controlActive) != 1 || controlActive[0].Value != "keep" {
		t.Fatalf("control active=%#v error=%v", controlActive, err)
	}
	controlRevision, err := storage.ContextRevision(ctx, controlTenantID, controlUserID)
	if err != nil || controlRevision != 1 {
		t.Fatalf("control revision=%d error=%v", controlRevision, err)
	}
}

func requiredDatabaseURL(t *testing.T) string {
	t.Helper()
	databaseURL := os.Getenv("TEST_DATABASE_URL")
	if databaseURL == "" {
		t.Fatal("TEST_DATABASE_URL is required for integration tests")
	}
	return databaseURL
}

func applyMigrations(t *testing.T, databaseURL string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := migrations.Apply(ctx, databaseURL); err != nil {
		t.Fatalf("apply migrations: %v", err)
	}
}

func openStore(t *testing.T, databaseURL string) *postgres.Store {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	storage, err := postgres.Open(ctx, databaseURL)
	if err != nil {
		t.Fatalf("open PostgreSQL store: %v", err)
	}
	return storage
}

func uniqueScope(label string) (string, string) {
	sequence := scopeSequence.Add(1)
	suffix := fmt.Sprintf("%d_%d", time.Now().UnixNano(), sequence)
	return "tenant_" + label + "_" + suffix, "user_" + label + "_" + suffix
}

func fixtureTime(offset int) time.Time {
	return time.Date(2026, time.August, 19, 8, 0, offset, 123456000, time.UTC)
}

func cloneOptionalTime(value *time.Time) *time.Time {
	if value == nil {
		return nil
	}
	cloned := *value
	return &cloned
}

func assertOptionalTime(t *testing.T, label string, got, want *time.Time) {
	t.Helper()
	if got == nil || want == nil {
		if got != nil || want != nil {
			t.Fatalf("%s expiration=%v, want %v", label, got, want)
		}
		return
	}
	if !got.Equal(*want) {
		t.Fatalf("%s expiration=%s, want %s", label, got.Format(time.RFC3339Nano), want.Format(time.RFC3339Nano))
	}
}

func evidence(tenantID, userID, id, content string, offset int) domain.EvidenceEvent {
	return domain.EvidenceEvent{
		ID: id, TenantID: tenantID, UserID: userID, SessionID: "session-integration",
		Actor: domain.ActorUser, Content: content,
		Metadata:   map[string]string{"fixture": "postgres-integration"},
		OccurredAt: fixtureTime(offset), RecordedAt: fixtureTime(offset).Add(time.Millisecond),
	}
}

func candidate(tenantID, userID, id, key, value string, sourceEventIDs []string, offset int) domain.MemoryCandidate {
	return domain.MemoryCandidate{
		ID: id, TenantID: tenantID, UserID: userID, Kind: domain.MemoryKindSemantic,
		Category: "preference", Key: key, Value: value, Person: "self", Relationship: "self",
		Backstory: "integration fixture", SourceEventIDs: append([]string(nil), sourceEventIDs...),
		Extractor: "integration-test", ExtractorVersion: "v1", Status: domain.CandidatePending,
		CreatedAt: fixtureTime(offset), Metadata: map[string]string{"fixture": "postgres-integration"},
	}
}

func approval(candidate domain.MemoryCandidate, memoryID string, offset int) storecontract.CandidateReviewCommand {
	return storecontract.CandidateReviewCommand{
		TenantID: candidate.TenantID, UserID: candidate.UserID, CandidateID: candidate.ID, MemoryID: memoryID,
		Review: domain.CandidateReview{Decision: domain.DecisionApprove, ReviewerID: "reviewer-integration", Reason: "supported by evidence", ReviewedAt: fixtureTime(offset)},
	}
}

func rejection(candidate domain.MemoryCandidate, offset int) storecontract.CandidateReviewCommand {
	return storecontract.CandidateReviewCommand{
		TenantID: candidate.TenantID, UserID: candidate.UserID, CandidateID: candidate.ID,
		Review: domain.CandidateReview{Decision: domain.DecisionReject, ReviewerID: "reviewer-integration", Reason: "insufficient support", ReviewedAt: fixtureTime(offset)},
	}
}

func mustAppend(t *testing.T, storage *postgres.Store, event domain.EvidenceEvent) {
	t.Helper()
	if err := storage.AppendEvidence(context.Background(), event); err != nil {
		t.Fatalf("append evidence %q: %v", event.ID, err)
	}
}

func mustCreateCandidate(t *testing.T, storage *postgres.Store, value domain.MemoryCandidate) {
	t.Helper()
	if err := storage.CreateCandidate(context.Background(), value); err != nil {
		t.Fatalf("create candidate %q: %v", value.ID, err)
	}
}

func seedCandidate(t *testing.T, storage *postgres.Store, tenantID, userID, id, key, value string, offset int) domain.MemoryCandidate {
	t.Helper()
	event := evidence(tenantID, userID, "event-"+id, value, offset)
	mustAppend(t, storage, event)
	result := candidate(tenantID, userID, "candidate-"+id, key, value, []string{event.ID}, offset+1)
	mustCreateCandidate(t, storage, result)
	return result
}

func eventIDs(events []domain.EvidenceEvent) []string {
	ids := make([]string, len(events))
	for index := range events {
		ids[index] = events[index].ID
	}
	return ids
}

const (
	projectionTestProvider         = "lmstudio"
	projectionTestModel            = "text-embedding-bge-m3"
	projectionTestDocumentVersion  = "memory-card-document-v1"
	projectionTestQueryVersion     = "raw-query-v1"
	projectionTestModelFingerprint = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	projectionJobFailureSecret     = "top_secret_projection_failure"
)

type projectionJobFixture struct {
	embeddingSpace        string
	state                 string
	expectedMemoryVersion int
}

func registerProjectionTarget(t *testing.T, databaseURL, label, state string, enqueueNew bool) string {
	t.Helper()
	sequence := scopeSequence.Add(1)
	embeddingSpace := fmt.Sprintf("space_contract_%s_%d_%d", label, time.Now().UnixNano(), sequence)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	conn, err := pgx.Connect(ctx, databaseURL)
	if err != nil {
		t.Fatalf("connect to register projection target: %v", err)
	}
	timestamp := time.Now().UTC().Truncate(time.Microsecond)
	tx, err := conn.Begin(ctx)
	if err != nil {
		_ = conn.Close(context.Background())
		t.Fatalf("begin projection target registration: %v", err)
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO agent_memory.embedding_spaces (
			id, provider, model, dimension, document_version, query_version,
			model_fingerprint, created_at
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`,
		embeddingSpace, projectionTestProvider, projectionTestModel, postgres.VectorDimension,
		projectionTestDocumentVersion, projectionTestQueryVersion,
		projectionTestModelFingerprint, timestamp,
	); err != nil {
		_ = tx.Rollback(ctx)
		_ = conn.Close(context.Background())
		t.Fatalf("register embedding space %q: %v", embeddingSpace, err)
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO agent_memory.embedding_projection_targets (
			embedding_space, state, enqueue_new, created_at, updated_at
		) VALUES ($1, $2, $3, $4, $4)`, embeddingSpace, state, enqueueNew, timestamp); err != nil {
		_ = tx.Rollback(ctx)
		_ = conn.Close(context.Background())
		t.Fatalf("register projection target %q: %v", embeddingSpace, err)
	}
	if err := tx.Commit(ctx); err != nil {
		_ = conn.Close(context.Background())
		t.Fatalf("commit projection target %q: %v", embeddingSpace, err)
	}
	if err := conn.Close(context.Background()); err != nil {
		t.Fatalf("close projection target connection: %v", err)
	}

	t.Cleanup(func() {
		cleanupContext, cleanupCancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cleanupCancel()
		cleanupConnection, cleanupErr := pgx.Connect(cleanupContext, databaseURL)
		if cleanupErr != nil {
			t.Errorf("connect to clean projection target %q: %v", embeddingSpace, cleanupErr)
			return
		}
		defer cleanupConnection.Close(context.Background())
		cleanupTransaction, cleanupErr := cleanupConnection.Begin(cleanupContext)
		if cleanupErr != nil {
			t.Errorf("begin cleanup for projection target %q: %v", embeddingSpace, cleanupErr)
			return
		}
		for _, statement := range []string{
			"DELETE FROM agent_memory.embedding_projection_targets WHERE embedding_space=$1",
			"DELETE FROM agent_memory.embedding_projection_jobs WHERE embedding_space=$1",
			"DELETE FROM agent_memory.memory_embeddings WHERE embedding_space=$1",
			"DELETE FROM agent_memory.embedding_spaces WHERE id=$1",
		} {
			if _, cleanupErr = cleanupTransaction.Exec(cleanupContext, statement, embeddingSpace); cleanupErr != nil {
				_ = cleanupTransaction.Rollback(cleanupContext)
				t.Errorf("clean projection target %q: %v", embeddingSpace, cleanupErr)
				return
			}
		}
		if cleanupErr = cleanupTransaction.Commit(cleanupContext); cleanupErr != nil {
			t.Errorf("commit cleanup for projection target %q: %v", embeddingSpace, cleanupErr)
		}
	})
	return embeddingSpace
}

// projectionServingTargetForTest avoids assuming the shared development
// database has no legitimate serving target. If one already exists, the test
// observes its enqueue_new contract without mutating or deleting it; otherwise
// it creates and cleans a test-owned serving target.
func projectionServingTargetForTest(t *testing.T, databaseURL, label string) (string, bool) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	conn, err := pgx.Connect(ctx, databaseURL)
	if err != nil {
		t.Fatalf("connect to inspect serving projection target: %v", err)
	}
	var embeddingSpace string
	var enqueueNew bool
	err = conn.QueryRow(ctx, `
		SELECT embedding_space, enqueue_new
		FROM agent_memory.embedding_projection_targets
		WHERE state='serving'`).Scan(&embeddingSpace, &enqueueNew)
	if closeErr := conn.Close(context.Background()); closeErr != nil {
		t.Fatalf("close serving projection target connection: %v", closeErr)
	}
	if err == nil {
		return embeddingSpace, enqueueNew
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		t.Fatalf("inspect serving projection target: %v", err)
	}
	return registerProjectionTarget(t, databaseURL, label, "serving", true), true
}

func projectionJobsForMemory(t *testing.T, databaseURL, tenantID, userID, memoryID string) []projectionJobFixture {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	conn, err := pgx.Connect(ctx, databaseURL)
	if err != nil {
		t.Fatalf("connect to inspect projection jobs: %v", err)
	}
	defer conn.Close(context.Background())
	rows, err := conn.Query(ctx, `
		SELECT embedding_space, state, expected_memory_version
		FROM agent_memory.embedding_projection_jobs
		WHERE tenant_id=$1 AND user_id=$2 AND memory_id=$3
		ORDER BY embedding_space`, tenantID, userID, memoryID)
	if err != nil {
		t.Fatalf("query projection jobs: %v", err)
	}
	defer rows.Close()
	var jobs []projectionJobFixture
	for rows.Next() {
		var job projectionJobFixture
		if err := rows.Scan(&job.embeddingSpace, &job.state, &job.expectedMemoryVersion); err != nil {
			t.Fatalf("scan projection job: %v", err)
		}
		jobs = append(jobs, job)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate projection jobs: %v", err)
	}
	return jobs
}

func projectionJobForMemoryAndSpace(
	t *testing.T,
	databaseURL, tenantID, userID, memoryID, embeddingSpace string,
) (projectionJobFixture, bool) {
	t.Helper()
	for _, job := range projectionJobsForMemory(t, databaseURL, tenantID, userID, memoryID) {
		if job.embeddingSpace == embeddingSpace {
			return job, true
		}
	}
	return projectionJobFixture{}, false
}

func countProjectionJobsForScope(t *testing.T, databaseURL, tenantID, userID string) int {
	t.Helper()
	return countProjectionJobs(t, databaseURL, tenantID, userID, "", "")
}

func countProjectionJobsForMemoryAndSpace(t *testing.T, databaseURL, tenantID, userID, memoryID, embeddingSpace string) int {
	t.Helper()
	return countProjectionJobs(t, databaseURL, tenantID, userID, memoryID, embeddingSpace)
}

func countProjectionJobs(t *testing.T, databaseURL, tenantID, userID, memoryID, embeddingSpace string) int {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	conn, err := pgx.Connect(ctx, databaseURL)
	if err != nil {
		t.Fatalf("connect to count projection jobs: %v", err)
	}
	defer conn.Close(context.Background())
	query := `
		SELECT count(*)
		FROM agent_memory.embedding_projection_jobs
		WHERE tenant_id=$1 AND user_id=$2`
	arguments := []any{tenantID, userID}
	if memoryID != "" {
		query += " AND memory_id=$3"
		arguments = append(arguments, memoryID)
	}
	if embeddingSpace != "" {
		query += fmt.Sprintf(" AND embedding_space=$%d", len(arguments)+1)
		arguments = append(arguments, embeddingSpace)
	}
	var count int
	if err := conn.QueryRow(ctx, query, arguments...).Scan(&count); err != nil {
		t.Fatalf("count projection jobs: %v", err)
	}
	return count
}

func projectionTestEmbedding(card domain.MemoryCard, embeddingSpace string, offset int) postgres.MemoryEmbedding {
	vector := make([]float32, postgres.VectorDimension)
	vector[0] = 1
	return postgres.MemoryEmbedding{
		TenantID:         card.TenantID,
		UserID:           card.UserID,
		MemoryID:         card.ID,
		EmbeddingSpace:   embeddingSpace,
		Provider:         projectionTestProvider,
		Model:            projectionTestModel,
		DocumentVersion:  projectionTestDocumentVersion,
		QueryVersion:     projectionTestQueryVersion,
		ModelFingerprint: projectionTestModelFingerprint,
		ContentSHA256:    strings.Repeat("c", 64),
		Vector:           vector,
		CreatedAt:        fixtureTime(offset),
	}
}

func assertEmbeddingCountForMemory(t *testing.T, databaseURL, tenantID, userID, memoryID string, want int) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	conn, err := pgx.Connect(ctx, databaseURL)
	if err != nil {
		t.Fatalf("connect to count memory embeddings: %v", err)
	}
	defer conn.Close(context.Background())
	var count int
	if err := conn.QueryRow(ctx, `
		SELECT count(*) FROM agent_memory.memory_embeddings
		WHERE tenant_id=$1 AND user_id=$2 AND memory_id=$3`, tenantID, userID, memoryID).Scan(&count); err != nil {
		t.Fatalf("count memory embeddings: %v", err)
	}
	if count != want {
		t.Fatalf("memory embedding rows for %q=%d, want %d", memoryID, count, want)
	}
}

func installProjectionJobFailureTrigger(t *testing.T, databaseURL, tenantID, userID string) {
	t.Helper()
	sequence := scopeSequence.Add(1)
	functionName := fmt.Sprintf("test_projection_job_failure_%d", sequence)
	triggerName := fmt.Sprintf("test_projection_job_failure_%d", sequence)
	qualifiedFunction := pgx.Identifier{"agent_memory", functionName}.Sanitize()
	quotedTrigger := pgx.Identifier{triggerName}.Sanitize()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	conn, err := pgx.Connect(ctx, databaseURL)
	if err != nil {
		t.Fatalf("connect to install projection failure trigger: %v", err)
	}
	functionSQL := fmt.Sprintf(`
		CREATE FUNCTION %s() RETURNS trigger LANGUAGE plpgsql AS $body$
		BEGIN
			IF NEW.tenant_id = %s AND NEW.user_id = %s THEN
				RAISE EXCEPTION %s;
			END IF;
			RETURN NEW;
		END
		$body$`, qualifiedFunction, postgresTestLiteral(tenantID), postgresTestLiteral(userID), postgresTestLiteral(projectionJobFailureSecret))
	if _, err := conn.Exec(ctx, functionSQL); err != nil {
		_ = conn.Close(context.Background())
		t.Fatalf("create projection failure function: %v", err)
	}
	triggerSQL := fmt.Sprintf(`
		CREATE TRIGGER %s
		BEFORE INSERT ON agent_memory.embedding_projection_jobs
		FOR EACH ROW EXECUTE FUNCTION %s()`, quotedTrigger, qualifiedFunction)
	if _, err := conn.Exec(ctx, triggerSQL); err != nil {
		_, _ = conn.Exec(ctx, "DROP FUNCTION "+qualifiedFunction+"()")
		_ = conn.Close(context.Background())
		t.Fatalf("create projection failure trigger: %v", err)
	}
	if err := conn.Close(context.Background()); err != nil {
		t.Fatalf("close projection failure connection: %v", err)
	}

	t.Cleanup(func() {
		cleanupContext, cleanupCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cleanupCancel()
		cleanupConnection, cleanupErr := pgx.Connect(cleanupContext, databaseURL)
		if cleanupErr != nil {
			t.Errorf("connect to remove projection failure trigger: %v", cleanupErr)
			return
		}
		defer cleanupConnection.Close(context.Background())
		if _, cleanupErr = cleanupConnection.Exec(cleanupContext, fmt.Sprintf(
			"DROP TRIGGER IF EXISTS %s ON agent_memory.embedding_projection_jobs", quotedTrigger,
		)); cleanupErr != nil {
			t.Errorf("drop projection failure trigger: %v", cleanupErr)
			return
		}
		if _, cleanupErr = cleanupConnection.Exec(cleanupContext, "DROP FUNCTION IF EXISTS "+qualifiedFunction+"()"); cleanupErr != nil {
			t.Errorf("drop projection failure function: %v", cleanupErr)
		}
	})
}

func postgresTestLiteral(value string) string {
	return "'" + strings.ReplaceAll(value, "'", "''") + "'"
}

func cleanupScopes(t *testing.T, databaseURL string, scopes [][2]string) {
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
			tx, err := conn.Begin(ctx)
			if err != nil {
				t.Errorf("begin cleanup for scope %s/%s: %v", scope[0], scope[1], err)
				continue
			}
			failed := false
			for _, table := range []string{"memory_cards", "memory_candidates", "evidence_events", "memory_identity_chains", "user_scope_state"} {
				query := fmt.Sprintf("DELETE FROM agent_memory.%s WHERE tenant_id=$1 AND user_id=$2", table)
				if _, err := tx.Exec(ctx, query, scope[0], scope[1]); err != nil {
					t.Errorf("clean %s for scope %s/%s: %v", table, scope[0], scope[1], err)
					failed = true
					break
				}
			}
			if failed {
				_ = tx.Rollback(ctx)
				continue
			}
			if err := tx.Commit(ctx); err != nil {
				t.Errorf("commit cleanup for scope %s/%s: %v", scope[0], scope[1], err)
			}
		}
	})
}

func assertCardStatuses(t *testing.T, databaseURL, tenantID, userID string, wantActive, wantSuperseded int) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	conn, err := pgx.Connect(ctx, databaseURL)
	if err != nil {
		t.Fatalf("connect to inspect cards: %v", err)
	}
	defer conn.Close(context.Background())
	var active, superseded int
	err = conn.QueryRow(ctx, `
		SELECT count(*) FILTER (WHERE status='active'), count(*) FILTER (WHERE status='superseded')
		FROM agent_memory.memory_cards WHERE tenant_id=$1 AND user_id=$2
	`, tenantID, userID).Scan(&active, &superseded)
	if err != nil {
		t.Fatalf("query card statuses: %v", err)
	}
	if active != wantActive || superseded != wantSuperseded {
		t.Fatalf("active/superseded=%d/%d, want %d/%d", active, superseded, wantActive, wantSuperseded)
	}
}

func assertTableRowCount(t *testing.T, databaseURL, table, tenantID, userID string, want int) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	conn, err := pgx.Connect(ctx, databaseURL)
	if err != nil {
		t.Fatalf("connect to count %s: %v", table, err)
	}
	defer conn.Close(context.Background())
	var count int
	query := fmt.Sprintf("SELECT count(*) FROM agent_memory.%s WHERE tenant_id=$1 AND user_id=$2", table)
	if err := conn.QueryRow(ctx, query, tenantID, userID).Scan(&count); err != nil {
		t.Fatalf("count %s: %v", table, err)
	}
	if count != want {
		t.Fatalf("%s rows=%d, want %d", table, count, want)
	}
}

func assertIdentityLatestVersion(t *testing.T, databaseURL, tenantID, userID string, want int) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	conn, err := pgx.Connect(ctx, databaseURL)
	if err != nil {
		t.Fatalf("connect to inspect identity chain: %v", err)
	}
	defer conn.Close(context.Background())
	var latestVersion int
	if err := conn.QueryRow(ctx, `
		SELECT latest_version
		FROM agent_memory.memory_identity_chains
		WHERE tenant_id=$1 AND user_id=$2
	`, tenantID, userID).Scan(&latestVersion); err != nil {
		t.Fatalf("query identity latest version: %v", err)
	}
	if latestVersion != want {
		t.Fatalf("identity latest version=%d, want %d", latestVersion, want)
	}
}

func assertDeletedScopeRows(t *testing.T, databaseURL, tenantID, userID string, wantRevision int64) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	conn, err := pgx.Connect(ctx, databaseURL)
	if err != nil {
		t.Fatalf("connect to inspect deletion: %v", err)
	}
	defer conn.Close(context.Background())
	for _, table := range []string{"evidence_events", "memory_candidates", "candidate_source_events", "memory_cards", "memory_embeddings", "embedding_projection_jobs", "memory_identity_chains"} {
		var count int
		query := fmt.Sprintf("SELECT count(*) FROM agent_memory.%s WHERE tenant_id=$1 AND user_id=$2", table)
		if err := conn.QueryRow(ctx, query, tenantID, userID).Scan(&count); err != nil {
			t.Fatalf("count %s: %v", table, err)
		}
		if count != 0 {
			t.Errorf("%s rows after deletion=%d, want 0", table, count)
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
		t.Fatalf("retained state rows/revision=%d/%d, want 1/%d", stateRows, revision, wantRevision)
	}
}

func assertScopeCompletelyAbsent(t *testing.T, databaseURL, tenantID, userID string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	conn, err := pgx.Connect(ctx, databaseURL)
	if err != nil {
		t.Fatalf("connect to inspect complete scope cleanup: %v", err)
	}
	defer conn.Close(context.Background())
	for _, table := range []string{
		"evidence_events",
		"memory_candidates",
		"candidate_source_events",
		"memory_cards",
		"memory_embeddings",
		"embedding_projection_jobs",
		"memory_identity_chains",
		"user_scope_state",
	} {
		var count int
		query := fmt.Sprintf("SELECT count(*) FROM agent_memory.%s WHERE tenant_id=$1 AND user_id=$2", table)
		if err := conn.QueryRow(ctx, query, tenantID, userID).Scan(&count); err != nil {
			t.Fatalf("count %s after complete scope cleanup: %v", table, err)
		}
		if count != 0 {
			t.Errorf("%s rows after complete scope cleanup=%d, want 0", table, count)
		}
	}
}
