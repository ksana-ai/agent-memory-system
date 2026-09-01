package eval

import (
	"context"
	"encoding/json"
	"errors"
	"math"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ksana-ai/agent-memory-system/internal/domain"
	"github.com/ksana-ai/agent-memory-system/internal/embedding"
	"github.com/ksana-ai/agent-memory-system/internal/store/postgres"
)

func TestPostgresVectorArmProjectsOracleDocumentEmbedsEveryQueryAndCleans(t *testing.T) {
	ctx := context.Background()
	embedder := newFakePostgresVectorEmbedder()
	var backends []*fakePostgresVectorBackend
	opener := func(context.Context, string) (postgresVectorBackend, error) {
		backend := newFakePostgresVectorBackend(t)
		backends = append(backends, backend)
		return backend, nil
	}
	fixedNow := time.Date(2026, 8, 19, 12, 0, 0, 0, time.UTC)
	factory, err := newPostgresVectorArmFactory(ctx, "postgres://private-dsn", embedder, opener, func() (string, error) {
		return "vector_case_namespace", nil
	}, func() time.Time { return fixedNow })
	if err != nil {
		t.Fatalf("new PostgreSQL vector factory: %v", err)
	}

	dataset := mustLoadDatasetV2(t, `{
  "schema_version":"2","id":"postgres-vector-unit","version":"1","description":"vector arm lifecycle",
  "cases":[{"id":"case","scopes":[{"id":"s","tenant_id":"tenant","user_id":"user"}],"timeline":[
    {"op":"memory.remember","memory_ref":"memory","scope":"s","at":"2026-08-19T10:01:00Z","review_state":"approved","memory":{"kind":"semantic","category":"deployment","key":"region","value":"Project Aurora deploy region Tokyo","person":"Project Aurora","relationship":"deployment target","backstory":"Current region."},"evidence":[{"alias":"source","session_id":"session","actor":"user","content":"Project Aurora deploys in Tokyo","occurred_at":"2026-08-19T10:00:00Z"}]},
    {"op":"query","id":"q","scope":"s","at":"2026-08-19T10:02:00Z","text":"Aurora Tokyo","judgments":{"memory_cards":{"relevance":{"memory":3}}}}
  ]}]
}`)
	manifest, err := RunV2(ctx, dataset, ConfigV2{
		RecallK: 1, NDCGK: 1, WarmupRuns: 1, MeasuredRuns: 2, QueryTimeout: time.Second,
		Arms: []ArmFactory{factory}, Timer: &stepTimerV2{step: time.Millisecond}, RequirePolicyPass: true,
	})
	if err != nil {
		t.Fatalf("run PostgreSQL vector arm: %v", err)
	}
	arm := manifest.Arms[0]
	if arm.Descriptor.ID != ArmReviewedCardsPostgresVectorV1 ||
		arm.Aggregate.RecallAtK != 1 || arm.Aggregate.MRR != 1 || arm.Aggregate.NDCGAtK != 1 || !arm.Aggregate.PolicyPassed {
		t.Fatalf("vector arm result = %#v", arm)
	}
	wantFingerprint := embedding.VectorSHA256(unitPostgresVector())
	if arm.Descriptor.Metadata["embedding_behavior_sha256"] != wantFingerprint ||
		arm.Descriptor.Metadata["embedding_space"] == "" ||
		arm.Descriptor.Metadata["vector_approximate_index_count"] != "0" ||
		arm.Descriptor.Metadata["retrieval_latency_scope"] != postgresVectorLatencyScope {
		t.Fatalf("vector descriptor metadata = %#v", arm.Descriptor.Metadata)
	}

	wantDocument := embedding.MemoryCardDocumentV1(domain.MemoryCard{
		Kind: domain.MemoryKindSemantic, Category: "deployment", Key: "region",
		Value: "Project Aurora deploy region Tokyo", Person: "Project Aurora",
		Relationship: "deployment target", Backstory: "Current region.",
	})
	inputs := embedder.Inputs()
	if countStrings(inputs, embedding.ProbeTextV1) != 2 {
		t.Fatalf("probe calls = %d, want factory plus case probe; inputs=%q", countStrings(inputs, embedding.ProbeTextV1), inputs)
	}
	if countStrings(inputs, wantDocument) != 1 {
		t.Fatalf("document calls = %d, want one oracle projection; inputs=%q", countStrings(inputs, wantDocument), inputs)
	}
	if countStrings(inputs, "Aurora Tokyo") != 3 {
		t.Fatalf("query calls = %d, want warmup plus two measured calls; inputs=%q", countStrings(inputs, "Aurora Tokyo"), inputs)
	}
	if len(backends) != 2 {
		t.Fatalf("opened backends = %d, want component probe plus case runtime", len(backends))
	}
	runtimeBackend := backends[1]
	if runtimeBackend.embeddingCount() != 0 || len(runtimeBackend.forgotten) != 1 || len(runtimeBackend.deletedScopeStates) != 1 || runtimeBackend.closeCalls != 1 {
		t.Fatalf("runtime cleanup: embeddings=%d forgotten=%#v states=%#v closes=%d", runtimeBackend.embeddingCount(), runtimeBackend.forgotten, runtimeBackend.deletedScopeStates, runtimeBackend.closeCalls)
	}
}

func TestPostgresVectorArmRuntimeProbeDriftFailsClosed(t *testing.T) {
	embedder := newFakePostgresVectorEmbedder()
	embedder.probeVectors = [][]float32{unitPostgresVector(), alternateUnitPostgresVector()}
	factory, err := newPostgresVectorArmFactory(context.Background(), "postgres://private-dsn", embedder, func(context.Context, string) (postgresVectorBackend, error) {
		return newFakePostgresVectorBackend(t), nil
	}, func() (string, error) { return "unused", nil }, time.Now)
	if err != nil {
		t.Fatalf("new factory: %v", err)
	}
	if _, err := factory.NewRuntime(context.Background()); err == nil || !strings.Contains(err.Error(), "components changed") {
		t.Fatalf("runtime drift error = %v", err)
	}
}

func TestPostgresVectorArmDescriptorAndErrorsExcludeConnectionAndInputSecrets(t *testing.T) {
	const databaseSecret = "postgres://user:database-secret@example.invalid/database"
	embedder := newFakePostgresVectorEmbedder()
	factory, err := newPostgresVectorArmFactory(context.Background(), databaseSecret, embedder, func(context.Context, string) (postgresVectorBackend, error) {
		return newFakePostgresVectorBackend(t), nil
	}, func() (string, error) { return "secret_test_namespace", nil }, time.Now)
	if err != nil {
		t.Fatalf("new factory: %v", err)
	}
	encoded, err := json.Marshal(factory.Descriptor())
	if err != nil {
		t.Fatalf("marshal descriptor: %v", err)
	}
	if strings.Contains(string(encoded), databaseSecret) || strings.Contains(string(encoded), "database-secret") {
		t.Fatalf("descriptor leaked database URL: %s", encoded)
	}

	const rawInputSecret = "raw-input-secret"
	embedder.failInput = rawInputSecret
	embedder.failError = errors.New("dependency echoed " + databaseSecret + " " + rawInputSecret)
	runtime, err := factory.NewRuntime(context.Background())
	if err != nil {
		t.Fatalf("new runtime: %v", err)
	}
	_, err = runtime.Retriever.Search(context.Background(), "tenant", "user", rawInputSecret, 1, time.Now())
	if err == nil {
		t.Fatal("query error = nil")
	}
	if strings.Contains(err.Error(), databaseSecret) || strings.Contains(err.Error(), "database-secret") || strings.Contains(err.Error(), rawInputSecret) {
		t.Fatalf("query error leaked dependency details: %v", err)
	}
	if cleanupErr := runtime.Cleanup(context.Background()); cleanupErr != nil {
		t.Fatalf("cleanup: %v", cleanupErr)
	}
}

func TestOnePostgresVectorRejectsInvalidShapeAndNumbers(t *testing.T) {
	if _, err := onePostgresVector(nil, postgres.VectorDimension); err == nil {
		t.Fatal("missing vector was accepted")
	}
	if _, err := onePostgresVector([][]float32{make([]float32, postgres.VectorDimension)}, postgres.VectorDimension); err == nil {
		t.Fatal("zero vector was accepted")
	}
	invalid := unitPostgresVector()
	invalid[1] = float32(math.NaN())
	if _, err := onePostgresVector([][]float32{invalid}, postgres.VectorDimension); err == nil {
		t.Fatal("NaN vector was accepted")
	}
}

type fakePostgresVectorEmbedder struct {
	mu           sync.Mutex
	inputs       []string
	probeVectors [][]float32
	probeCalls   int
	failInput    string
	failError    error
}

func newFakePostgresVectorEmbedder() *fakePostgresVectorEmbedder {
	return &fakePostgresVectorEmbedder{}
}

func (*fakePostgresVectorEmbedder) Descriptor() embedding.Descriptor {
	return embedding.Descriptor{
		Provider: embedding.ProviderLMStudio, API: embedding.APIEmbeddingsV1,
		Model: "text-embedding-bge-m3", Dimension: postgres.VectorDimension,
		DocumentVersion: embedding.MemoryCardDocumentVersion,
	}
}

func (embedder *fakePostgresVectorEmbedder) Embed(_ context.Context, inputs []string) ([][]float32, error) {
	embedder.mu.Lock()
	defer embedder.mu.Unlock()
	result := make([][]float32, len(inputs))
	for index, input := range inputs {
		embedder.inputs = append(embedder.inputs, input)
		if embedder.failInput != "" && input == embedder.failInput {
			return nil, embedder.failError
		}
		if input == embedding.ProbeTextV1 && embedder.probeCalls < len(embedder.probeVectors) {
			result[index] = append([]float32(nil), embedder.probeVectors[embedder.probeCalls]...)
			embedder.probeCalls++
			continue
		}
		if input == embedding.ProbeTextV1 {
			embedder.probeCalls++
		}
		result[index] = unitPostgresVector()
	}
	return result, nil
}

func (embedder *fakePostgresVectorEmbedder) Inputs() []string {
	embedder.mu.Lock()
	defer embedder.mu.Unlock()
	return append([]string(nil), embedder.inputs...)
}

type fakePostgresVectorBackend struct {
	*fakePostgresFTSBackend
	vectorMetadata postgres.VectorMetadata
	mu             sync.Mutex
	embeddings     map[string]postgres.MemoryEmbedding
}

func newFakePostgresVectorBackend(t *testing.T) *fakePostgresVectorBackend {
	return &fakePostgresVectorBackend{
		fakePostgresFTSBackend: newFakePostgresFTSBackend(t),
		vectorMetadata: postgres.VectorMetadata{
			ServerVersionNum: "180000", ExtensionVersion: "0.8.6", SchemaMigrationVersion: 4,
			Dimension: postgres.VectorDimension, DistanceMetric: postgres.VectorDistanceMetric, SearchStrategy: postgres.VectorSearchStrategy,
		},
		embeddings: make(map[string]postgres.MemoryEmbedding),
	}
}

func (backend *fakePostgresVectorBackend) VectorMetadata(context.Context) (postgres.VectorMetadata, error) {
	return backend.vectorMetadata, nil
}

func (backend *fakePostgresVectorBackend) UpsertMemoryEmbedding(_ context.Context, value postgres.MemoryEmbedding) error {
	backend.mu.Lock()
	defer backend.mu.Unlock()
	value.Vector = append([]float32(nil), value.Vector...)
	backend.embeddings[postgresVectorEmbeddingKey(value.TenantID, value.UserID, value.MemoryID, value.EmbeddingSpace)] = value
	return nil
}

func (backend *fakePostgresVectorBackend) SearchVector(
	ctx context.Context,
	tenantID, userID, embeddingSpace string,
	query []float32,
	limit int,
	asOf time.Time,
) ([]domain.SearchHit, error) {
	memories, err := backend.Store.ListServiceableMemories(ctx, tenantID, userID, asOf)
	if err != nil {
		return nil, err
	}
	backend.mu.Lock()
	defer backend.mu.Unlock()
	hits := make([]domain.SearchHit, 0, len(memories))
	for _, memory := range memories {
		projection, exists := backend.embeddings[postgresVectorEmbeddingKey(tenantID, userID, memory.ID, embeddingSpace)]
		if !exists {
			continue
		}
		hits = append(hits, domain.SearchHit{Memory: memory, Score: cosineScoreV2(projection.Vector, query)})
	}
	sort.Slice(hits, func(left, right int) bool {
		if hits[left].Score != hits[right].Score {
			return hits[left].Score > hits[right].Score
		}
		if !hits[left].Memory.CreatedAt.Equal(hits[right].Memory.CreatedAt) {
			return hits[left].Memory.CreatedAt.After(hits[right].Memory.CreatedAt)
		}
		return hits[left].Memory.ID < hits[right].Memory.ID
	})
	return append([]domain.SearchHit(nil), hits[:min(limit, len(hits))]...), nil
}

func (backend *fakePostgresVectorBackend) ForgetUser(ctx context.Context, tenantID, userID string, deletedAt time.Time) (domain.DeletionReceipt, error) {
	receipt, err := backend.fakePostgresFTSBackend.ForgetUser(ctx, tenantID, userID, deletedAt)
	if err != nil {
		return domain.DeletionReceipt{}, err
	}
	backend.mu.Lock()
	defer backend.mu.Unlock()
	prefix := tenantID + "\x00" + userID + "\x00"
	for key := range backend.embeddings {
		if strings.HasPrefix(key, prefix) {
			delete(backend.embeddings, key)
		}
	}
	return receipt, nil
}

func (backend *fakePostgresVectorBackend) embeddingCount() int {
	backend.mu.Lock()
	defer backend.mu.Unlock()
	return len(backend.embeddings)
}

func postgresVectorEmbeddingKey(tenantID, userID, memoryID, embeddingSpace string) string {
	return tenantID + "\x00" + userID + "\x00" + memoryID + "\x00" + embeddingSpace
}

func cosineScoreV2(left, right []float32) float64 {
	var dot, leftNorm, rightNorm float64
	for index := range left {
		leftValue, rightValue := float64(left[index]), float64(right[index])
		dot += leftValue * rightValue
		leftNorm += leftValue * leftValue
		rightNorm += rightValue * rightValue
	}
	return dot / (math.Sqrt(leftNorm) * math.Sqrt(rightNorm))
}

func unitPostgresVector() []float32 {
	vector := make([]float32, postgres.VectorDimension)
	vector[0] = 1
	return vector
}

func alternateUnitPostgresVector() []float32 {
	vector := make([]float32, postgres.VectorDimension)
	vector[1] = 1
	return vector
}

func countStrings(values []string, target string) int {
	count := 0
	for _, value := range values {
		if value == target {
			count++
		}
	}
	return count
}
