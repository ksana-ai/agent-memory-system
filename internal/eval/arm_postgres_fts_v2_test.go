package eval

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/kai443/go-agent-memory-system/internal/domain"
	"github.com/kai443/go-agent-memory-system/internal/retrieval"
	"github.com/kai443/go-agent-memory-system/internal/store"
	"github.com/kai443/go-agent-memory-system/internal/store/memstore"
	"github.com/kai443/go-agent-memory-system/internal/store/postgres"
)

func TestPostgresFTSNamespaceRestoresLogicalScopeAndCleansPhysicalScope(t *testing.T) {
	ctx := context.Background()
	backend := newFakePostgresFTSBackend(t)
	namespace := newPostgresFTSNamespace(backend, "opaque_case_namespace")
	logicalTenantID, logicalUserID := "logical-tenant", "logical-user"
	physical := namespace.physicalScope(logicalTenantID, logicalUserID)
	if physical.tenantID == logicalTenantID || physical.userID == logicalUserID ||
		strings.Contains(physical.tenantID, logicalTenantID) || strings.Contains(physical.userID, logicalUserID) {
		t.Fatalf("physical scope is not opaque: %#v", physical)
	}

	event := domain.EvidenceEvent{
		ID: "event-stable", TenantID: logicalTenantID, UserID: logicalUserID,
		SessionID: "session", Actor: domain.ActorUser, Content: "Project Aurora deploys in Tokyo.",
		OccurredAt: time.Date(2026, 8, 19, 10, 0, 0, 0, time.UTC), RecordedAt: time.Date(2026, 8, 19, 10, 1, 0, 0, time.UTC),
	}
	if err := namespace.AppendEvidence(ctx, event); err != nil {
		t.Fatalf("append evidence: %v", err)
	}
	rawEvent, err := backend.Store.EvidenceByID(ctx, physical.tenantID, physical.userID, event.ID)
	if err != nil || rawEvent.TenantID != physical.tenantID || rawEvent.UserID != physical.userID {
		t.Fatalf("raw physical evidence = %#v, error=%v", rawEvent, err)
	}
	logicalEvent, err := namespace.EvidenceByID(ctx, logicalTenantID, logicalUserID, event.ID)
	if err != nil || logicalEvent.TenantID != logicalTenantID || logicalEvent.UserID != logicalUserID {
		t.Fatalf("restored logical evidence = %#v, error=%v", logicalEvent, err)
	}

	expiresAt := time.Date(2026, 8, 20, 0, 0, 0, 0, time.UTC)
	candidate := domain.MemoryCandidate{
		ID: "candidate-stable", TenantID: logicalTenantID, UserID: logicalUserID,
		Kind: domain.MemoryKindSemantic, Category: "deployment", Key: "region", Value: "Project Aurora deploys in Tokyo",
		SourceEventIDs: []string{event.ID}, Extractor: "fixture", ExtractorVersion: "v1",
		Status: domain.CandidatePending, CreatedAt: event.RecordedAt, ExpiresAt: &expiresAt,
	}
	if err := namespace.CreateCandidate(ctx, candidate); err != nil {
		t.Fatalf("create candidate: %v", err)
	}
	batchCandidate := candidate
	batchCandidate.ID = "candidate-batch-stable"
	batchCandidate.Key = "batch-region"
	batchCandidate.Value = "Project Aurora also has a batch candidate"
	if err := namespace.CreateCandidateBatch(ctx, store.CandidateBatchCommand{
		TenantID: logicalTenantID, UserID: logicalUserID, ExpectedRevision: 0,
		Candidates: []domain.MemoryCandidate{batchCandidate},
	}); err != nil {
		t.Fatalf("create candidate batch: %v", err)
	}
	if batchCandidate.TenantID != logicalTenantID || batchCandidate.UserID != logicalUserID {
		t.Fatalf("batch input was mutated: %#v", batchCandidate)
	}
	rawBatchCandidate, err := backend.Store.CandidateByID(ctx, physical.tenantID, physical.userID, batchCandidate.ID)
	if err != nil || rawBatchCandidate.TenantID != physical.tenantID || rawBatchCandidate.UserID != physical.userID {
		t.Fatalf("raw physical batch candidate = %#v, error=%v", rawBatchCandidate, err)
	}
	logicalBatchCandidate, err := namespace.CandidateByID(ctx, logicalTenantID, logicalUserID, batchCandidate.ID)
	if err != nil || logicalBatchCandidate.TenantID != logicalTenantID || logicalBatchCandidate.UserID != logicalUserID {
		t.Fatalf("restored logical batch candidate = %#v, error=%v", logicalBatchCandidate, err)
	}
	reviewed, memory, err := namespace.ReviewCandidate(ctx, store.CandidateReviewCommand{
		TenantID: logicalTenantID, UserID: logicalUserID, CandidateID: candidate.ID, MemoryID: "memory-stable",
		Review: domain.CandidateReview{Decision: domain.DecisionApprove, ReviewerID: "reviewer", Reason: "supported", ReviewedAt: event.RecordedAt.Add(time.Minute)},
	})
	if err != nil {
		t.Fatalf("review candidate: %v", err)
	}
	if reviewed.TenantID != logicalTenantID || reviewed.UserID != logicalUserID || memory == nil ||
		memory.TenantID != logicalTenantID || memory.UserID != logicalUserID || memory.ExpiresAt == nil || !memory.ExpiresAt.Equal(expiresAt) {
		t.Fatalf("review did not restore logical scope/payload: candidate=%#v memory=%#v", reviewed, memory)
	}
	hits, err := namespace.Search(ctx, logicalTenantID, logicalUserID, "Aurora Tokyo", 5, event.RecordedAt)
	if err != nil || len(hits) != 1 || hits[0].Memory.TenantID != logicalTenantID || hits[0].Memory.UserID != logicalUserID {
		t.Fatalf("logical search hits = %#v, error=%v", hits, err)
	}

	if err := namespace.Cleanup(ctx); err != nil {
		t.Fatalf("cleanup: %v", err)
	}
	if backend.closeCalls != 1 || len(backend.forgotten) != 1 || backend.forgotten[0] != physical ||
		len(backend.deletedScopeStates) != 1 || backend.deletedScopeStates[0] != physical {
		t.Fatalf("cleanup close/forgotten/deleted states = %d/%#v/%#v", backend.closeCalls, backend.forgotten, backend.deletedScopeStates)
	}
	if _, err := backend.Store.EvidenceByID(ctx, physical.tenantID, physical.userID, event.ID); !errors.Is(err, domain.ErrNotFound) {
		t.Fatalf("physical evidence survived cleanup: %v", err)
	}
	if err := namespace.Cleanup(ctx); err != nil || backend.closeCalls != 1 || len(backend.forgotten) != 1 {
		t.Fatalf("second cleanup was not idempotent: error=%v close=%d forgotten=%d", err, backend.closeCalls, len(backend.forgotten))
	}
}

func TestPostgresFTSFactoryManifestContainsComponentsButNoSecret(t *testing.T) {
	const secretURL = "postgres://eval-user:manifest-secret@db.example.invalid/agent_memory"
	opened := 0
	opener := func(context.Context, string) (postgresFTSBackend, error) {
		opened++
		return newFakePostgresFTSBackend(t), nil
	}
	factory, err := newPostgresFTSArmFactory(context.Background(), secretURL, opener, func() (string, error) {
		return "manifest_namespace", nil
	})
	if err != nil {
		t.Fatalf("new factory: %v", err)
	}
	descriptor := factory.Descriptor()
	if descriptor.ID != ArmReviewedCardsPostgresFTSV1 || len(descriptor.ConfigHash) != 64 ||
		descriptor.Metadata["text_search_config"] != postgres.FTSTextSearchConfig ||
		descriptor.Metadata["schema_migration_version"] != "3" {
		t.Fatalf("descriptor metadata = %#v", descriptor)
	}
	descriptor.Metadata["text_search_config"] = "mutated"
	if factory.Descriptor().Metadata["text_search_config"] != postgres.FTSTextSearchConfig {
		t.Fatal("descriptor metadata was mutable through the returned map")
	}
	changedFactory, err := newPostgresFTSArmFactory(context.Background(), secretURL, func(context.Context, string) (postgresFTSBackend, error) {
		backend := newFakePostgresFTSBackend(t)
		backend.metadata.SchemaMigrationVersion++
		return backend, nil
	}, func() (string, error) { return "unused_namespace", nil })
	if err != nil {
		t.Fatalf("new changed-component factory: %v", err)
	}
	if changedFactory.Descriptor().ConfigHash == factory.Descriptor().ConfigHash {
		t.Fatal("component metadata did not participate in the descriptor config hash")
	}

	dataset := mustLoadDatasetV2(t, `{
  "schema_version":"2","id":"postgres-manifest","version":"1","description":"postgres manifest secrecy",
  "cases":[{"id":"case","scopes":[{"id":"s","tenant_id":"tenant","user_id":"user"}],"timeline":[
    {"op":"memory.remember","memory_ref":"memory","scope":"s","at":"2026-08-19T10:01:00Z","review_state":"approved","memory":{"kind":"semantic","category":"deployment","key":"region","value":"Aurora Tokyo"},"evidence":[{"alias":"source","session_id":"session","actor":"user","content":"Aurora deploys in Tokyo","occurred_at":"2026-08-19T10:00:00Z"}]},
    {"op":"query","id":"q","scope":"s","at":"2026-08-19T10:02:00Z","text":"Aurora Tokyo","judgments":{"memory_cards":{"relevance":{"memory":3}}}}
  ]}]
}`)
	manifest, err := RunV2(context.Background(), dataset, ConfigV2{
		RecallK: 1, NDCGK: 1, MeasuredRuns: 1, QueryTimeout: time.Second,
		Arms: []ArmFactory{factory}, Timer: &stepTimerV2{step: time.Millisecond},
	})
	if err != nil {
		t.Fatalf("run manifest: %v", err)
	}
	encoded, err := json.Marshal(manifest)
	if err != nil {
		t.Fatalf("marshal manifest: %v", err)
	}
	if strings.Contains(string(encoded), secretURL) || strings.Contains(string(encoded), "manifest-secret") {
		t.Fatalf("manifest leaked database URL: %s", encoded)
	}
	if opened != 2 {
		t.Fatalf("backend open count = %d, want probe plus one case", opened)
	}
}

func TestPostgresFTSFactoryErrorsDoNotContainDatabaseURL(t *testing.T) {
	const secretURL = "postgres://eval-user:error-secret@db.example.invalid/agent_memory"
	_, err := newPostgresFTSArmFactory(context.Background(), secretURL, func(context.Context, string) (postgresFTSBackend, error) {
		return nil, errors.New("driver echoed " + secretURL)
	}, func() (string, error) { return "unused", nil })
	if err == nil {
		t.Fatal("factory error = nil")
	}
	if strings.Contains(err.Error(), secretURL) || strings.Contains(err.Error(), "error-secret") {
		t.Fatalf("factory error leaked database URL: %v", err)
	}

	openCalls := 0
	factory, err := newPostgresFTSArmFactory(context.Background(), secretURL, func(context.Context, string) (postgresFTSBackend, error) {
		openCalls++
		if openCalls == 1 {
			return newFakePostgresFTSBackend(t), nil
		}
		return nil, errors.New("runtime driver echoed " + secretURL)
	}, func() (string, error) { return "unused", nil })
	if err != nil {
		t.Fatalf("construct factory before runtime error: %v", err)
	}
	if _, err = factory.NewRuntime(context.Background()); err == nil {
		t.Fatal("runtime error = nil")
	}
	if strings.Contains(err.Error(), secretURL) || strings.Contains(err.Error(), "error-secret") {
		t.Fatalf("runtime error leaked database URL: %v", err)
	}
}

type fakePostgresFTSBackend struct {
	*memstore.Store
	retriever          *retrieval.BM25
	metadata           postgres.FTSMetadata
	forgotten          []postgresPhysicalScope
	deletedScopeStates []postgresPhysicalScope
	closeCalls         int
}

func newFakePostgresFTSBackend(t *testing.T) *fakePostgresFTSBackend {
	t.Helper()
	storage := memstore.New()
	retriever, err := retrieval.NewBM25(storage)
	if err != nil {
		t.Fatalf("new fake retriever: %v", err)
	}
	return &fakePostgresFTSBackend{
		Store:     storage,
		retriever: retriever,
		metadata: postgres.FTSMetadata{
			ServerVersionNum: "180000", SchemaMigrationVersion: 3,
			TextSearchConfig: postgres.FTSTextSearchConfig, QueryStrategy: postgres.FTSQueryStrategy, RankFunction: postgres.FTSRankFunction,
		},
	}
}

func (backend *fakePostgresFTSBackend) Search(ctx context.Context, tenantID, userID, query string, limit int, asOf time.Time) ([]domain.SearchHit, error) {
	return backend.retriever.Search(ctx, tenantID, userID, query, limit, asOf)
}

func (backend *fakePostgresFTSBackend) FTSMetadata(context.Context) (postgres.FTSMetadata, error) {
	return backend.metadata, nil
}

func (backend *fakePostgresFTSBackend) ForgetUser(ctx context.Context, tenantID, userID string, deletedAt time.Time) (domain.DeletionReceipt, error) {
	backend.forgotten = append(backend.forgotten, postgresPhysicalScope{tenantID: tenantID, userID: userID})
	return backend.Store.ForgetUser(ctx, tenantID, userID, deletedAt)
}

func (backend *fakePostgresFTSBackend) Close() {
	backend.closeCalls++
}

func (backend *fakePostgresFTSBackend) DeleteEvaluationScopeState(_ context.Context, tenantID, userID string) error {
	backend.deletedScopeStates = append(backend.deletedScopeStates, postgresPhysicalScope{tenantID: tenantID, userID: userID})
	return nil
}
