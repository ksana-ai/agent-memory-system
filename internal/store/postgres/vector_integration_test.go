//go:build integration

package postgres_test

import (
	"context"
	"errors"
	"math"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/kai443/go-agent-memory-system/internal/domain"
	storecontract "github.com/kai443/go-agent-memory-system/internal/store"
	"github.com/kai443/go-agent-memory-system/internal/store/postgres"
)

const vectorTestSpace = "lmstudio:text-embedding-bge-m3:memory-card-v1"

func TestPostgresVectorSchemaAndMetadata(t *testing.T) {
	ctx := context.Background()
	databaseURL := requiredDatabaseURL(t)
	applyMigrations(t, databaseURL)
	storage := openStore(t, databaseURL)
	defer storage.Close()

	metadata, err := storage.VectorMetadata(ctx)
	if err != nil {
		t.Fatalf("load vector metadata: %v", err)
	}
	if metadata.ServerVersionNum == "" || metadata.ExtensionVersion == "" {
		t.Fatalf("incomplete vector metadata: %#v", metadata)
	}
	if metadata.SchemaMigrationVersion != 6 ||
		metadata.Dimension != postgres.VectorDimension ||
		metadata.DistanceMetric != postgres.VectorDistanceMetric ||
		metadata.SearchStrategy != postgres.VectorSearchStrategy ||
		metadata.ApproximateIndexCount != 0 {
		t.Fatalf("vector metadata=%#v, want schema=6 dimension=%d exact cosine", metadata, postgres.VectorDimension)
	}

	conn, err := pgx.Connect(ctx, databaseURL)
	if err != nil {
		t.Fatalf("connect to inspect vector schema: %v", err)
	}
	defer conn.Close(context.Background())

	var columnType string
	err = conn.QueryRow(ctx, `
		SELECT format_type(attribute.atttypid, attribute.atttypmod)
		FROM pg_attribute AS attribute
		JOIN pg_class AS relation ON relation.oid = attribute.attrelid
		JOIN pg_namespace AS namespace ON namespace.oid = relation.relnamespace
		WHERE namespace.nspname = 'agent_memory'
		  AND relation.relname = 'memory_embeddings'
		  AND attribute.attname = 'embedding'
		  AND NOT attribute.attisdropped`).Scan(&columnType)
	if err != nil || columnType != "vector(1024)" {
		t.Fatalf("embedding column type=%q error=%v, want vector(1024)", columnType, err)
	}

	var deleteRule string
	err = conn.QueryRow(ctx, `
		SELECT delete_rule
		FROM information_schema.referential_constraints
		WHERE constraint_schema = 'agent_memory'
		  AND constraint_name = 'memory_embeddings_card_fk'`).Scan(&deleteRule)
	if err != nil || deleteRule != "CASCADE" {
		t.Fatalf("embedding FK delete rule=%q error=%v, want CASCADE", deleteRule, err)
	}
	var registryKeyColumns int
	err = conn.QueryRow(ctx, `
		SELECT array_length(constraint_row.conkey, 1)
		FROM pg_constraint AS constraint_row
		JOIN pg_class AS relation ON relation.oid = constraint_row.conrelid
		JOIN pg_namespace AS namespace ON namespace.oid = relation.relnamespace
		WHERE namespace.nspname = 'agent_memory'
		  AND relation.relname = 'memory_embeddings'
		  AND constraint_row.conname = 'memory_embeddings_space_fk'`).Scan(&registryKeyColumns)
	if err != nil || registryKeyColumns != 6 {
		t.Fatalf("embedding registry FK columns=%d error=%v, want six-field configuration key", registryKeyColumns, err)
	}

	var indexDefinition string
	err = conn.QueryRow(ctx, `
		SELECT indexdef
		FROM pg_indexes
		WHERE schemaname = 'agent_memory'
		  AND indexname = 'memory_embeddings_scope_space_idx'`).Scan(&indexDefinition)
	if err != nil || !strings.Contains(indexDefinition, "USING btree (tenant_id, user_id, embedding_space, memory_id)") {
		t.Fatalf("scope/space index=%q error=%v", indexDefinition, err)
	}
	var annIndexes int
	err = conn.QueryRow(ctx, `
		SELECT count(*)
		FROM pg_indexes
		WHERE schemaname = 'agent_memory'
		  AND tablename = 'memory_embeddings'
		  AND (lower(indexdef) LIKE '%using hnsw%' OR lower(indexdef) LIKE '%using ivfflat%')`).Scan(&annIndexes)
	if err != nil || annIndexes != 0 {
		t.Fatalf("ANN index count=%d error=%v, want exact-search baseline without ANN", annIndexes, err)
	}
}

func TestPostgresVectorMetadataRejectsApproximateIndexDrift(t *testing.T) {
	ctx := context.Background()
	databaseURL := requiredDatabaseURL(t)
	applyMigrations(t, databaseURL)
	storage := openStore(t, databaseURL)
	defer storage.Close()

	conn, err := pgx.Connect(ctx, databaseURL)
	if err != nil {
		t.Fatalf("connect to create drift index: %v", err)
	}
	defer conn.Close(context.Background())
	const indexName = "memory_embeddings_test_hnsw_idx"
	_, _ = conn.Exec(ctx, "DROP INDEX IF EXISTS agent_memory."+indexName)
	defer func() {
		_, _ = conn.Exec(context.Background(), "DROP INDEX IF EXISTS agent_memory."+indexName)
	}()
	if _, err := conn.Exec(ctx, "CREATE INDEX "+indexName+" ON agent_memory.memory_embeddings USING hnsw (embedding vector_cosine_ops)"); err != nil {
		t.Fatalf("create approximate drift index: %v", err)
	}
	if _, err := storage.VectorMetadata(ctx); !errors.Is(err, domain.ErrInvariant) {
		t.Fatalf("metadata with approximate index error=%v, want invariant failure", err)
	}
	if _, err := conn.Exec(ctx, "DROP INDEX agent_memory."+indexName); err != nil {
		t.Fatalf("drop approximate drift index: %v", err)
	}
	if metadata, err := storage.VectorMetadata(ctx); err != nil || metadata.ApproximateIndexCount != 0 {
		t.Fatalf("metadata after drift cleanup=%#v error=%v", metadata, err)
	}
}

func TestPostgresVectorUpsertRanksHydratesPersistsAndDeletes(t *testing.T) {
	ctx := context.Background()
	databaseURL := requiredDatabaseURL(t)
	applyMigrations(t, databaseURL)
	tenantID, userID := uniqueScope("vector_rank")
	cleanupScopes(t, databaseURL, [][2]string{{tenantID, userID}})
	storage := openStore(t, databaseURL)

	firstSource := evidence(tenantID, userID, "event-vector-rank-best-source-first", "first source", 1)
	secondSource := evidence(tenantID, userID, "event-vector-rank-best-source-second", "second source", 2)
	mustAppend(t, storage, firstSource)
	mustAppend(t, storage, secondSource)
	bestCandidate := candidate(
		tenantID,
		userID,
		"candidate-vector-rank-best",
		"rank_best",
		"best vector memory",
		[]string{secondSource.ID, firstSource.ID},
		3,
	)
	mustCreateCandidate(t, storage, bestCandidate)
	_, best, err := storage.ReviewCandidate(ctx, approval(bestCandidate, "memory-vector-rank-best", 4))
	if err != nil || best == nil {
		t.Fatalf("approve best vector card: memory=%#v error=%v", best, err)
	}
	second := approveVectorCard(t, storage, tenantID, userID, "vector-rank-second", "rank_second", "second vector memory", 5, 8, nil)
	tieA := approveVectorCard(t, storage, tenantID, userID, "vector-rank-tie-a", "rank_tie_a", "tie vector a", 9, 20, nil)
	tieB := approveVectorCard(t, storage, tenantID, userID, "vector-rank-tie-b", "rank_tie_b", "tie vector b", 11, 20, nil)

	initial := vectorEmbedding(*best, vectorTestSpace, basisVector(1), 21)
	revisionBeforeEmbedding, err := storage.ContextRevision(ctx, tenantID, userID)
	if err != nil {
		t.Fatalf("load revision before embedding: %v", err)
	}
	if err := storage.UpsertMemoryEmbedding(ctx, initial); err != nil {
		t.Fatalf("initial upsert: %v", err)
	}
	revisionAfterInsert, err := storage.ContextRevision(ctx, tenantID, userID)
	if err != nil || revisionAfterInsert != revisionBeforeEmbedding+1 {
		t.Fatalf("revision after embedding insert=%d error=%v, want %d", revisionAfterInsert, err, revisionBeforeEmbedding+1)
	}
	if err := storage.UpsertMemoryEmbedding(ctx, initial); err != nil {
		t.Fatalf("exact idempotent upsert: %v", err)
	}
	revisionAfterNoop, err := storage.ContextRevision(ctx, tenantID, userID)
	if err != nil || revisionAfterNoop != revisionAfterInsert {
		t.Fatalf("revision after embedding no-op=%d error=%v, want %d", revisionAfterNoop, err, revisionAfterInsert)
	}
	retryWithNewTimestamp := initial
	retryWithNewTimestamp.CreatedAt = fixtureTime(22)
	if err := storage.UpsertMemoryEmbedding(ctx, retryWithNewTimestamp); err != nil {
		t.Fatalf("idempotent upsert with a new attempt timestamp: %v", err)
	}
	revisionAfterTimestampOnlyRetry, err := storage.ContextRevision(ctx, tenantID, userID)
	if err != nil || revisionAfterTimestampOnlyRetry != revisionAfterInsert {
		t.Fatalf("revision after timestamp-only retry=%d error=%v, want %d", revisionAfterTimestampOnlyRetry, err, revisionAfterInsert)
	}
	replacement := vectorEmbedding(*best, vectorTestSpace, basisVector(0), 22)
	replacement.ContentSHA256 = strings.ToUpper(replacement.ContentSHA256)
	if err := storage.UpsertMemoryEmbedding(ctx, replacement); err != nil {
		t.Fatalf("idempotent replacement upsert: %v", err)
	}
	revisionAfterReplacement, err := storage.ContextRevision(ctx, tenantID, userID)
	if err != nil || revisionAfterReplacement != revisionAfterInsert+1 {
		t.Fatalf("revision after embedding replacement=%d error=%v, want %d", revisionAfterReplacement, err, revisionAfterInsert+1)
	}
	contentConflict := replacement
	contentConflict.ContentSHA256 = strings.Repeat("c", 64)
	contentConflict.CreatedAt = fixtureTime(23)
	if err := storage.UpsertMemoryEmbedding(ctx, contentConflict); !errors.Is(err, domain.ErrConflict) {
		t.Fatalf("content-changing upsert error=%v, want conflict", err)
	}
	revisionAfterConflict, err := storage.ContextRevision(ctx, tenantID, userID)
	if err != nil || revisionAfterConflict != revisionAfterReplacement {
		t.Fatalf("revision after rejected content change=%d error=%v, want %d", revisionAfterConflict, err, revisionAfterReplacement)
	}
	spaceConflict := vectorEmbedding(second, vectorTestSpace, twoComponentUnitVector(0.8, 0.6), 23)
	spaceConflict.QueryVersion = "query-v2"
	if err := storage.UpsertMemoryEmbedding(ctx, spaceConflict); !errors.Is(err, domain.ErrConflict) {
		t.Fatalf("mixed embedding-space config error=%v, want conflict", err)
	}
	for _, value := range []postgres.MemoryEmbedding{
		vectorEmbedding(second, vectorTestSpace, twoComponentUnitVector(0.8, 0.6), 24),
		vectorEmbedding(tieA, vectorTestSpace, basisVector(1), 25),
		vectorEmbedding(tieB, vectorTestSpace, basisVector(1), 26),
	} {
		if err := storage.UpsertMemoryEmbedding(ctx, value); err != nil {
			t.Fatalf("upsert %s: %v", value.MemoryID, err)
		}
	}

	assertTableRowCount(t, databaseURL, "memory_embeddings", tenantID, userID, 4)
	conn, err := pgx.Connect(ctx, databaseURL)
	if err != nil {
		t.Fatalf("connect to inspect vector upsert: %v", err)
	}
	var provider, contentHash string
	var rows int
	err = conn.QueryRow(ctx, `
		SELECT count(*), max(provider), max(content_sha256)
		FROM agent_memory.memory_embeddings
		WHERE tenant_id=$1 AND user_id=$2 AND memory_id=$3 AND embedding_space=$4`,
		tenantID, userID, best.ID, vectorTestSpace,
	).Scan(&rows, &provider, &contentHash)
	if err == nil {
		_, mutationErr := conn.Exec(ctx, `
			UPDATE agent_memory.memory_embeddings
			SET provider = 'incompatible-provider'
			WHERE tenant_id=$1 AND user_id=$2 AND memory_id=$3 AND embedding_space=$4`,
			tenantID, userID, best.ID, vectorTestSpace,
		)
		if mutationErr == nil {
			t.Fatal("database accepted embedding metadata that disagrees with its registered space")
		}
	}
	_ = conn.Close(ctx)
	if err != nil || rows != 1 || provider != replacement.Provider || contentHash != strings.ToLower(replacement.ContentSHA256) {
		t.Fatalf("upserted row count/provider/hash=%d/%q/%q error=%v", rows, provider, contentHash, err)
	}

	assertVectorRanking := func(label string, current *postgres.Store) {
		t.Helper()
		for attempt := 0; attempt < 5; attempt++ {
			hits, err := current.SearchVector(ctx, tenantID, userID, vectorTestSpace, basisVector(0), 10, fixtureTime(100))
			if err != nil {
				t.Fatalf("%s search attempt %d: %v", label, attempt, err)
			}
			wantIDs := []string{best.ID, second.ID, tieA.ID, tieB.ID}
			if got := vectorHitIDs(hits); !equalStrings(got, wantIDs) {
				t.Fatalf("%s search attempt %d ids=%v, want %v", label, attempt, got, wantIDs)
			}
			if math.Abs(hits[0].Score-1) > 1e-6 || math.Abs(hits[1].Score-0.8) > 1e-5 {
				t.Fatalf("%s scores=%v, want approximately 1 and 0.8 first", label, vectorHitScores(hits))
			}
			if hits[2].Score != hits[3].Score {
				t.Fatalf("%s tie scores differ: %v", label, vectorHitScores(hits))
			}
			if got := hits[0].Memory.SourceEventIDs; !equalStrings(got, []string{secondSource.ID, firstSource.ID}) {
				t.Fatalf("%s source order=%v, want [%s %s]", label, got, secondSource.ID, firstSource.ID)
			}
		}
		limited, err := current.SearchVector(ctx, tenantID, userID, vectorTestSpace, basisVector(0), 2, fixtureTime(100))
		if err != nil || !equalStrings(vectorHitIDs(limited), []string{best.ID, second.ID}) {
			t.Fatalf("%s limited hits=%v error=%v", label, vectorHitIDs(limited), err)
		}
	}
	assertVectorRanking("before reopen", storage)
	storage.Close()

	reopened := openStore(t, databaseURL)
	defer reopened.Close()
	assertVectorRanking("after reopen", reopened)
	receipt, err := reopened.ForgetUser(ctx, tenantID, userID, fixtureTime(101))
	if err != nil {
		t.Fatalf("forget vector scope: %v", err)
	}
	if receipt.MemoriesDeleted != 4 {
		t.Fatalf("forget receipt=%#v, want four deleted memories", receipt)
	}
	assertTableRowCount(t, databaseURL, "memory_embeddings", tenantID, userID, 0)
	afterDelete, err := reopened.SearchVector(ctx, tenantID, userID, vectorTestSpace, basisVector(0), 10, fixtureTime(102))
	if err != nil || len(afterDelete) != 0 {
		t.Fatalf("search after deletion=%#v error=%v", afterDelete, err)
	}
}

func TestPostgresVectorSearchFiltersScopeSpaceAndLifecycle(t *testing.T) {
	ctx := context.Background()
	databaseURL := requiredDatabaseURL(t)
	applyMigrations(t, databaseURL)
	tenantID, userID := uniqueScope("vector_filter")
	otherTenantID, otherUserID := uniqueScope("vector_filter_foreign")
	cleanupScopes(t, databaseURL, [][2]string{{tenantID, userID}, {otherTenantID, userID}, {tenantID, otherUserID}})
	storage := openStore(t, databaseURL)
	defer storage.Close()
	asOf := fixtureTime(80)

	active := approveVectorCard(t, storage, tenantID, userID, "vector-filter-active", "active", "active", 1, 3, nil)
	altOnly := approveVectorCard(t, storage, tenantID, userID, "vector-filter-space", "alt", "alt", 4, 6, nil)
	expired := approveVectorCard(t, storage, tenantID, userID, "vector-filter-expired", "expired", "expired", 7, 9, &asOf)
	old := approveVectorCard(t, storage, tenantID, userID, "vector-filter-old", "versioned", "old", 10, 12, nil)
	if err := storage.UpsertMemoryEmbedding(ctx, vectorEmbedding(old, vectorTestSpace, basisVector(0), 13)); err != nil {
		t.Fatalf("upsert vector before supersede: %v", err)
	}
	_ = approveVectorCard(t, storage, tenantID, userID, "vector-filter-new", "versioned", "new", 14, 16, nil)
	foreignTenant := approveVectorCard(t, storage, otherTenantID, userID, "vector-filter-foreign-tenant", "foreign_tenant", "foreign tenant", 16, 18, nil)
	foreignUser := approveVectorCard(t, storage, tenantID, otherUserID, "vector-filter-foreign-user", "foreign_user", "foreign user", 19, 21, nil)

	for _, value := range []postgres.MemoryEmbedding{
		vectorEmbedding(active, vectorTestSpace, basisVector(0), 30),
		vectorEmbedding(altOnly, "other-space", basisVector(0), 31),
		vectorEmbedding(expired, vectorTestSpace, basisVector(0), 32),
		vectorEmbedding(foreignTenant, vectorTestSpace, basisVector(0), 34),
		vectorEmbedding(foreignUser, vectorTestSpace, basisVector(0), 35),
	} {
		if err := storage.UpsertMemoryEmbedding(ctx, value); err != nil {
			t.Fatalf("upsert filter fixture %s: %v", value.MemoryID, err)
		}
	}
	if err := storage.UpsertMemoryEmbedding(ctx, vectorEmbedding(old, vectorTestSpace, basisVector(0), 36)); !errors.Is(err, domain.ErrNotFound) {
		t.Fatalf("late superseded-card upsert error=%v, want not found", err)
	}
	assertMemoryEmbeddingCount(t, databaseURL, tenantID, userID, old.ID, 0)

	hits, err := storage.SearchVector(ctx, tenantID, userID, vectorTestSpace, basisVector(0), 20, asOf)
	if err != nil || !equalStrings(vectorHitIDs(hits), []string{active.ID}) {
		t.Fatalf("filtered hits=%v error=%v, want only %s", vectorHitIDs(hits), err, active.ID)
	}
	foreignHits, err := storage.SearchVector(ctx, otherTenantID, userID, vectorTestSpace, basisVector(0), 20, asOf)
	if err != nil || !equalStrings(vectorHitIDs(foreignHits), []string{foreignTenant.ID}) {
		t.Fatalf("foreign tenant hits=%v error=%v", vectorHitIDs(foreignHits), err)
	}
	altHits, err := storage.SearchVector(ctx, tenantID, userID, "other-space", basisVector(0), 20, asOf)
	if err != nil || !equalStrings(vectorHitIDs(altHits), []string{altOnly.ID}) {
		t.Fatalf("alternate-space hits=%v error=%v", vectorHitIDs(altHits), err)
	}
	emptyHits, err := storage.SearchVector(ctx, tenantID, userID, vectorTestSpace, nil, 0, asOf)
	if err != nil || len(emptyHits) != 0 {
		t.Fatalf("zero-limit search hits=%#v error=%v, want empty without vector parsing", emptyHits, err)
	}

	wrongScope := vectorEmbedding(active, vectorTestSpace, basisVector(0), 40)
	wrongScope.UserID = "user-vector-missing-scope"
	if err := storage.UpsertMemoryEmbedding(ctx, wrongScope); !errors.Is(err, domain.ErrNotFound) {
		t.Fatalf("wrong-scope upsert error=%v, want not found", err)
	}
	conn, err := pgx.Connect(ctx, databaseURL)
	if err != nil {
		t.Fatalf("connect to inspect wrong scope: %v", err)
	}
	defer conn.Close(context.Background())
	var wrongStateRows int
	if err := conn.QueryRow(ctx, `
		SELECT count(*) FROM agent_memory.user_scope_state
		WHERE tenant_id=$1 AND user_id=$2`, tenantID, wrongScope.UserID).Scan(&wrongStateRows); err != nil {
		t.Fatalf("count wrong scope state: %v", err)
	}
	if wrongStateRows != 0 {
		t.Fatalf("wrong-scope upsert created %d state rows, want zero", wrongStateRows)
	}
}

func TestPostgresVectorUpsertRacingForgetLeavesNoOrphan(t *testing.T) {
	databaseURL := requiredDatabaseURL(t)
	applyMigrations(t, databaseURL)
	storage := openStore(t, databaseURL)
	defer storage.Close()

	for attempt := 0; attempt < 12; attempt++ {
		tenantID, userID := uniqueScope("vector_upsert_forget")
		cleanupScopes(t, databaseURL, [][2]string{{tenantID, userID}})
		card := approveVectorCard(t, storage, tenantID, userID, "vector-race", "race", "race", 1, 3, nil)
		value := vectorEmbedding(card, vectorTestSpace, basisVector(0), 4)

		start := make(chan struct{})
		var wait sync.WaitGroup
		wait.Add(2)
		var upsertErr, forgetErr error
		go func() {
			defer wait.Done()
			<-start
			upsertErr = storage.UpsertMemoryEmbedding(context.Background(), value)
		}()
		go func() {
			defer wait.Done()
			<-start
			_, forgetErr = storage.ForgetUser(context.Background(), tenantID, userID, fixtureTime(5))
		}()
		close(start)
		wait.Wait()

		if upsertErr != nil && !errors.Is(upsertErr, domain.ErrNotFound) {
			t.Fatalf("attempt %d upsert error=%v, want success or not found", attempt, upsertErr)
		}
		if forgetErr != nil {
			t.Fatalf("attempt %d forget error=%v", attempt, forgetErr)
		}
		assertTableRowCount(t, databaseURL, "memory_embeddings", tenantID, userID, 0)
		assertTableRowCount(t, databaseURL, "memory_cards", tenantID, userID, 0)
	}
}

func TestPostgresVectorUpsertRacingSupersedeLeavesNoStaleProjection(t *testing.T) {
	databaseURL := requiredDatabaseURL(t)
	applyMigrations(t, databaseURL)
	storage := openStore(t, databaseURL)
	defer storage.Close()

	for attempt := 0; attempt < 12; attempt++ {
		tenantID, userID := uniqueScope("vector_upsert_supersede")
		cleanupScopes(t, databaseURL, [][2]string{{tenantID, userID}})
		old := approveVectorCard(t, storage, tenantID, userID, "vector-supersede-old", "versioned", "old", 1, 3, nil)
		replacementEvent := evidence(tenantID, userID, "event-vector-supersede-new", "new", 4)
		mustAppend(t, storage, replacementEvent)
		replacementCandidate := candidate(
			tenantID,
			userID,
			"candidate-vector-supersede-new",
			"versioned",
			"new",
			[]string{replacementEvent.ID},
			5,
		)
		mustCreateCandidate(t, storage, replacementCandidate)
		value := vectorEmbedding(old, vectorTestSpace, basisVector(0), 6)
		reviewCommand := approval(replacementCandidate, "memory-vector-supersede-new", 7)

		start := make(chan struct{})
		var wait sync.WaitGroup
		wait.Add(2)
		var upsertErr, reviewErr error
		go func() {
			defer wait.Done()
			<-start
			upsertErr = storage.UpsertMemoryEmbedding(context.Background(), value)
		}()
		go func() {
			defer wait.Done()
			<-start
			_, _, reviewErr = storage.ReviewCandidate(context.Background(), reviewCommand)
		}()
		close(start)
		wait.Wait()

		if upsertErr != nil && !errors.Is(upsertErr, domain.ErrNotFound) {
			t.Fatalf("attempt %d upsert error=%v, want success or not found", attempt, upsertErr)
		}
		if reviewErr != nil {
			t.Fatalf("attempt %d replacement review error=%v", attempt, reviewErr)
		}
		assertMemoryEmbeddingCount(t, databaseURL, tenantID, userID, old.ID, 0)
		active, err := storage.ListServiceableMemories(context.Background(), tenantID, userID, fixtureTime(100))
		if err != nil || len(active) != 1 || active[0].ID != reviewCommand.MemoryID {
			t.Fatalf("attempt %d active memories=%#v error=%v", attempt, active, err)
		}
		revision, err := storage.ContextRevision(context.Background(), tenantID, userID)
		wantRevision := uint64(2)
		if upsertErr == nil {
			wantRevision = 3
		}
		if err != nil || revision != wantRevision {
			t.Fatalf("attempt %d revision=%d error=%v, want %d", attempt, revision, err, wantRevision)
		}
	}
}

func TestPostgresVectorEvaluationCleanupIncludesEmbeddings(t *testing.T) {
	ctx := context.Background()
	databaseURL := requiredDatabaseURL(t)
	applyMigrations(t, databaseURL)
	rawTenantID, rawUserID := uniqueScope("vector_eval_cleanup")
	tenantID, userID := "eval_"+rawTenantID, "eval_"+rawUserID
	cleanupScopes(t, databaseURL, [][2]string{{tenantID, userID}})
	storage := openStore(t, databaseURL)
	defer storage.Close()

	card := approveVectorCard(t, storage, tenantID, userID, "vector-eval", "eval", "eval", 1, 3, nil)
	if err := storage.UpsertMemoryEmbedding(ctx, vectorEmbedding(card, vectorTestSpace, basisVector(0), 4)); err != nil {
		t.Fatalf("upsert evaluation vector: %v", err)
	}
	if err := storage.DeleteEvaluationScopeState(ctx, tenantID, userID); !errors.Is(err, domain.ErrConflict) {
		t.Fatalf("cleanup with embedding content error=%v, want conflict", err)
	}
	assertTableRowCount(t, databaseURL, "memory_embeddings", tenantID, userID, 1)
	if _, err := storage.ForgetUser(ctx, tenantID, userID, fixtureTime(5)); err != nil {
		t.Fatalf("forget evaluation vector scope: %v", err)
	}
	assertTableRowCount(t, databaseURL, "memory_embeddings", tenantID, userID, 0)
	if err := storage.DeleteEvaluationScopeState(ctx, tenantID, userID); err != nil {
		t.Fatalf("delete empty evaluation vector scope: %v", err)
	}
	assertScopeCompletelyAbsent(t, databaseURL, tenantID, userID)
}

func approveVectorCard(
	t *testing.T,
	storage *postgres.Store,
	tenantID, userID, label, key, value string,
	candidateOffset, reviewOffset int,
	expiresAt *time.Time,
) domain.MemoryCard {
	t.Helper()
	event := evidence(tenantID, userID, "event-"+label, value, candidateOffset)
	mustAppend(t, storage, event)
	candidate := candidate(tenantID, userID, "candidate-"+label, key, value, []string{event.ID}, candidateOffset+1)
	candidate.ExpiresAt = cloneOptionalTime(expiresAt)
	mustCreateCandidate(t, storage, candidate)
	_, memory, err := storage.ReviewCandidate(context.Background(), storecontract.CandidateReviewCommand{
		TenantID:    tenantID,
		UserID:      userID,
		CandidateID: candidate.ID,
		MemoryID:    "memory-" + label,
		Review: domain.CandidateReview{
			Decision:   domain.DecisionApprove,
			ReviewerID: "reviewer-vector",
			Reason:     "supported by vector fixture evidence",
			ReviewedAt: fixtureTime(reviewOffset),
		},
	})
	if err != nil || memory == nil {
		t.Fatalf("approve vector card %s: memory=%#v error=%v", label, memory, err)
	}
	return *memory
}

func vectorEmbedding(card domain.MemoryCard, space string, vector []float32, offset int) postgres.MemoryEmbedding {
	return postgres.MemoryEmbedding{
		TenantID:         card.TenantID,
		UserID:           card.UserID,
		MemoryID:         card.ID,
		EmbeddingSpace:   space,
		Provider:         "lmstudio",
		Model:            "text-embedding-bge-m3",
		DocumentVersion:  "memory-card-v1",
		QueryVersion:     "query-v1",
		ModelFingerprint: strings.Repeat("b", 64),
		ContentSHA256:    strings.Repeat("a", 64),
		Vector:           vector,
		CreatedAt:        fixtureTime(offset),
	}
}

func assertMemoryEmbeddingCount(t *testing.T, databaseURL, tenantID, userID, memoryID string, want int) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	conn, err := pgx.Connect(ctx, databaseURL)
	if err != nil {
		t.Fatalf("connect to count memory embedding: %v", err)
	}
	defer conn.Close(context.Background())
	var count int
	if err := conn.QueryRow(ctx, `
		SELECT count(*) FROM agent_memory.memory_embeddings
		WHERE tenant_id=$1 AND user_id=$2 AND memory_id=$3`, tenantID, userID, memoryID).Scan(&count); err != nil {
		t.Fatalf("count memory embedding: %v", err)
	}
	if count != want {
		t.Fatalf("memory embedding rows=%d, want %d", count, want)
	}
}

func basisVector(index int) []float32 {
	result := make([]float32, postgres.VectorDimension)
	result[index] = 1
	return result
}

func twoComponentUnitVector(first, second float32) []float32 {
	result := make([]float32, postgres.VectorDimension)
	result[0] = first
	result[1] = second
	return result
}

func vectorHitIDs(hits []domain.SearchHit) []string {
	result := make([]string, len(hits))
	for index := range hits {
		result[index] = hits[index].Memory.ID
	}
	return result
}

func vectorHitScores(hits []domain.SearchHit) []float64 {
	result := make([]float64, len(hits))
	for index := range hits {
		result[index] = hits[index].Score
	}
	return result
}

func equalStrings(left, right []string) bool {
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
