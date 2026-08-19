package eval

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/kai443/go-agent-memory-system/internal/domain"
	"github.com/kai443/go-agent-memory-system/internal/retrieval"
	"github.com/kai443/go-agent-memory-system/internal/store"
	"github.com/kai443/go-agent-memory-system/internal/store/memstore"
)

func TestRunV2ExecutesLifecycleForEveryArmAndSeparatesQualityFromPolicy(t *testing.T) {
	dataset := mustLoadDatasetV2(t, `{
  "schema_version":"2",
  "id":"runner-lifecycle",
  "version":"1",
  "description":"runner lifecycle fixture",
  "cases":[{
    "id":"lifecycle",
    "scopes":[{"id":"owner","tenant_id":"tenant-a","user_id":"user-a"}],
    "timeline":[
      {"op":"memory.remember","memory_ref":"coffee-memory","scope":"owner","at":"2026-01-01T00:01:00Z","review_state":"approved","memory":{"kind":"semantic","category":"preference","key":"drink","value":"Ethiopian coffee"},"evidence":[{"alias":"coffee-source","session_id":"session-1","actor":"user","content":"I prefer Ethiopian coffee","occurred_at":"2026-01-01T00:00:00Z"}]},
      {"op":"query","id":"positive","scope":"owner","at":"2026-01-01T00:02:00Z","text":"Ethiopian coffee drink","judgments":{"memory_cards":{"relevance":{"coffee-memory":3}}}},
      {"op":"memory.remember","memory_ref":"rejected-memory","scope":"owner","at":"2026-01-01T00:03:00Z","review_state":"rejected","memory":{"kind":"semantic","category":"private","key":"codeword","value":"red lantern"},"evidence":[{"alias":"rejected-source","session_id":"session-2","actor":"agent","content":"unsupported red lantern guess","occurred_at":"2026-01-01T00:02:30Z"}]},
      {"op":"query","id":"rejected","scope":"owner","at":"2026-01-01T00:04:00Z","text":"red lantern codeword","judgments":{"memory_cards":{"forbidden":["rejected-memory"],"require_empty":true}}},
      {"op":"forget_user","scope":"owner","at":"2026-01-01T00:05:00Z"},
      {"op":"query","id":"forgotten","scope":"owner","at":"2026-01-01T00:06:00Z","text":"Ethiopian coffee drink","judgments":{"memory_cards":{"forbidden":["coffee-memory"],"require_empty":true}}}
    ]
  }]
}`)
	generatedAt := time.Date(2026, 1, 2, 0, 0, 0, 0, time.UTC)
	config := ConfigV2{
		RecallK: 2, NDCGK: 3, WarmupRuns: 1, MeasuredRuns: 2,
		QueryTimeout: time.Second, Arms: BuiltinArmFactories(),
		Timer:       &stepTimerV2{step: 5 * time.Millisecond},
		GeneratedAt: func() time.Time { return generatedAt },
	}

	manifest, err := RunV2(context.Background(), dataset, config)
	if err != nil {
		t.Fatalf("RunV2(): %v", err)
	}
	if manifest.Dataset.Cases != 1 || manifest.Dataset.Queries != 3 || manifest.System.RetrievalDepth != 3 {
		t.Fatalf("unexpected manifest metadata: %#v %#v", manifest.Dataset, manifest.System)
	}
	if len(manifest.Arms) != 2 {
		t.Fatalf("arm count = %d, want 2", len(manifest.Arms))
	}

	noMemory := findArmResultV2(t, manifest, ArmNoMemoryV1)
	if noMemory.Aggregate.QualityQueryCount != 1 || noMemory.Aggregate.RecallAtK != 0 {
		t.Fatalf("no-memory quality = %#v", noMemory.Aggregate)
	}
	if noMemory.Aggregate.PolicyPassRate != 1 || noMemory.Aggregate.NonRecallPassRate != 1 {
		t.Fatalf("no-memory policy rates = %#v", noMemory.Aggregate)
	}

	bm25 := findArmResultV2(t, manifest, ArmReviewedCardsBM25V1)
	if bm25.Aggregate.RecallAtK != 1 || bm25.Aggregate.MRR != 1 || bm25.Aggregate.NDCGAtK != 1 || bm25.Aggregate.PassRate != 1 {
		t.Fatalf("bm25 quality = %#v", bm25.Aggregate)
	}
	if !bm25.Aggregate.PolicyPassed || bm25.Aggregate.PolicyPassRate != 1 || bm25.Aggregate.NonRecallPassRate != 1 {
		t.Fatalf("bm25 policy = %#v", bm25.Aggregate)
	}
	if bm25.Aggregate.LatencySampleCount != 6 || bm25.Aggregate.LatencyP50Nanoseconds != int64(5*time.Millisecond) || bm25.Aggregate.LatencyMaxNanoseconds != int64(5*time.Millisecond) {
		t.Fatalf("bm25 latency = %#v", bm25.Aggregate)
	}
	positive := findQueryResultV2(t, bm25, "positive")
	if len(positive.Hits) != 1 || positive.Hits[0].Alias != "coffee-memory" {
		t.Fatalf("positive hits = %#v", positive.Hits)
	}
	if len(positive.Hits[0].SourceAliases) != 1 || positive.Hits[0].SourceAliases[0] != "coffee-source" {
		t.Fatalf("source aliases = %#v", positive.Hits[0].SourceAliases)
	}
	if len(positive.DurationsNanoseconds) != 2 {
		t.Fatalf("measured durations = %v, want two samples", positive.DurationsNanoseconds)
	}
	if len(findQueryResultV2(t, bm25, "rejected").Hits) != 0 || len(findQueryResultV2(t, bm25, "forgotten").Hits) != 0 {
		t.Fatal("rejected or forgotten memory was retrieved")
	}

	secondConfig := config
	secondConfig.Timer = &stepTimerV2{step: 91 * time.Millisecond}
	second, err := RunV2(context.Background(), dataset, secondConfig)
	if err != nil {
		t.Fatalf("second RunV2(): %v", err)
	}
	secondBM25 := findArmResultV2(t, second, ArmReviewedCardsBM25V1)
	if positive.Hits[0].MemoryID != findQueryResultV2(t, secondBM25, "positive").Hits[0].MemoryID {
		t.Fatal("alias-derived memory ID changed across runs")
	}
	if bm25.Aggregate.QualityResultSHA256 != secondBM25.Aggregate.QualityResultSHA256 {
		t.Fatal("quality result hash changed with latency")
	}
}

func TestRunV2DoesNotPutJudgmentAliasesIntoSearchableFields(t *testing.T) {
	dataset := mustLoadDatasetV2(t, `{
  "schema_version":"2","id":"qrel-isolation","version":"1","description":"qrels stay outside documents",
  "cases":[{"id":"qrel-case","scopes":[{"id":"s","tenant_id":"t","user_id":"u"}],"timeline":[
    {"op":"memory.remember","memory_ref":"logical-qrel-7f9","scope":"s","at":"2026-01-01T00:01:00Z","review_state":"approved","memory":{"kind":"semantic","category":"food","key":"breakfast","value":"oatmeal"},"evidence":[{"alias":"e","session_id":"x","actor":"user","content":"oatmeal for breakfast","occurred_at":"2026-01-01T00:00:00Z"}]},
    {"op":"query","id":"q","scope":"s","at":"2026-01-01T00:02:00Z","text":"logical qrel 7f9","judgments":{"memory_cards":{"relevance":{"logical-qrel-7f9":3}}}}
  ]}]
}`)
	factory, _ := BuiltinArmFactory(ArmReviewedCardsBM25V1)
	manifest, err := RunV2(context.Background(), dataset, ConfigV2{
		RecallK: 5, NDCGK: 10, MeasuredRuns: 1, QueryTimeout: time.Second,
		Arms: []ArmFactory{factory}, Timer: &stepTimerV2{step: time.Millisecond},
	})
	if err != nil {
		t.Fatalf("RunV2(): %v", err)
	}
	if hits := manifest.Arms[0].Queries[0].Hits; len(hits) != 0 {
		t.Fatalf("judgment alias leaked into searchable memory fields: %#v", hits)
	}
}

func TestRunV2MakesRetrieverPolicyViolationsVisibleAndCleansUp(t *testing.T) {
	dataset := mustLoadDatasetV2(t, `{
  "schema_version":"2","id":"policy","version":"1","description":"malicious retriever",
  "cases":[{"id":"policy-case","scopes":[{"id":"s","tenant_id":"tenant-a","user_id":"user-a"}],"timeline":[
    {"op":"memory.remember","memory_ref":"known","scope":"s","at":"2026-01-01T00:01:00Z","review_state":"approved","memory":{"kind":"semantic","category":"c","key":"k","value":"v"},"evidence":[{"alias":"source","session_id":"session","actor":"user","content":"v","occurred_at":"2026-01-01T00:00:00Z"}]},
    {"op":"query","id":"q","scope":"s","at":"2026-01-01T00:02:00Z","text":"anything","judgments":{"memory_cards":{"forbidden":["known"]}}}
  ]}]
}`)
	known := domain.MemoryCard{
		ID: stableArtifactIDV2("mem", "policy-case", "known"), TenantID: "tenant-a", UserID: "user-a",
		Kind: domain.MemoryKindSemantic, Category: "c", Key: "k", Value: "v", Status: domain.MemoryActive,
		SourceEventIDs: []string{stableArtifactIDV2("evt", "policy-case", "source")},
	}
	duplicate := known
	nonActive := known
	nonActive.Status = domain.MemorySuperseded
	unknown := known
	unknown.ID = "mem_unknown"
	unknown.TenantID = "tenant-other"
	cleanups := 0
	factory := armFactory{
		descriptor: ArmDescriptor{ID: "malicious-v1", Version: "1", JudgmentProfile: "reviewed-memory-alias-v1", ResultKind: "memory-card", ConfigHash: hashArmConfig("malicious")},
		newRuntime: func(context.Context) (ArmRuntime, error) {
			return ArmRuntime{
				Store: memstore.New(), Retriever: staticRetrieverV2{hits: []domain.SearchHit{{Memory: known}, {Memory: duplicate}, {Memory: nonActive}, {Memory: unknown}}},
				Cleanup: func(context.Context) error { cleanups++; return nil },
			}, nil
		},
	}
	manifest, err := RunV2(context.Background(), dataset, ConfigV2{
		RecallK: 2, NDCGK: 4, MeasuredRuns: 1, QueryTimeout: time.Second,
		Arms: []ArmFactory{factory}, Timer: &stepTimerV2{step: time.Millisecond},
	})
	if err != nil {
		t.Fatalf("RunV2(): %v", err)
	}
	if cleanups != 1 {
		t.Fatalf("cleanup calls = %d, want 1", cleanups)
	}
	query := manifest.Arms[0].Queries[0]
	if query.ExecutionError == "" || query.Policy.Passed {
		t.Fatalf("execution/policy failure not visible: %#v", query)
	}
	if query.Policy.ForbiddenHits != 3 || query.Policy.DuplicateHits != 2 || query.Policy.NonActiveHits != 1 || query.Policy.UnknownHits != 1 || query.Policy.ScopeViolations != 1 {
		t.Fatalf("policy counts = %#v", query.Policy)
	}
	if manifest.Arms[0].Aggregate.PolicyPassed || manifest.Arms[0].Aggregate.QueryExecutionFailures != 1 || manifest.Arms[0].Aggregate.PolicyPassRate != 0 {
		t.Fatalf("aggregate policy = %#v", manifest.Arms[0].Aggregate)
	}
}

func TestRunV2RequiresQueryTimeout(t *testing.T) {
	dataset := mustLoadDatasetV2(t, `{"schema_version":"2","id":"timeout","version":"1","description":"timeout","cases":[{"id":"c","scopes":[{"id":"s","tenant_id":"t","user_id":"u"}],"timeline":[{"op":"query","id":"q","scope":"s","at":"2026-01-01T00:00:00Z","text":"x","judgments":{"memory_cards":{"require_empty":true}}}]}]}`)
	_, err := RunV2(context.Background(), dataset, ConfigV2{RecallK: 1, NDCGK: 1, MeasuredRuns: 1, Arms: BuiltinArmFactories()})
	if err == nil {
		t.Fatal("missing query timeout error = nil")
	}
}

func TestRunV2RetainsTransientWarmupPolicyViolation(t *testing.T) {
	dataset := mustLoadDatasetV2(t, `{
  "schema_version":"2","id":"transient","version":"1","description":"transient leak",
  "cases":[{"id":"transient-case","scopes":[{"id":"s","tenant_id":"t","user_id":"u"}],"timeline":[
    {"op":"memory.remember","memory_ref":"forbidden","scope":"s","at":"2026-01-01T00:01:00Z","review_state":"approved","memory":{"kind":"semantic","category":"c","key":"k","value":"v"},"evidence":[{"alias":"source","session_id":"session","actor":"user","content":"v","occurred_at":"2026-01-01T00:00:00Z"}]},
    {"op":"query","id":"q","scope":"s","at":"2026-01-01T00:02:00Z","text":"x","judgments":{"memory_cards":{"forbidden":["forbidden"]}}}
  ]}]
}`)
	card := domain.MemoryCard{
		ID: stableArtifactIDV2("mem", "transient-case", "forbidden"), TenantID: "t", UserID: "u",
		CandidateID: stableArtifactIDV2("cand", "transient-case", "\x00compact-candidate:forbidden"),
		Kind:        domain.MemoryKindSemantic, Category: "c", Key: "k", Value: "v", Status: domain.MemoryActive,
		SourceEventIDs: []string{stableArtifactIDV2("evt", "transient-case", "source")},
		Version:        1, CreatedAt: time.Date(2026, 1, 1, 0, 1, 0, 0, time.UTC),
	}
	factory := armFactory{
		descriptor: ArmDescriptor{ID: "sequence-v1", Version: "1", JudgmentProfile: "reviewed-memory-alias-v1", ResultKind: "memory-card", ConfigHash: hashArmConfig("sequence")},
		newRuntime: func(context.Context) (ArmRuntime, error) {
			return ArmRuntime{Store: memstore.New(), Retriever: &sequenceRetrieverV2{results: [][]domain.SearchHit{{{Memory: card}}, {}}}}, nil
		},
	}
	manifest, err := RunV2(context.Background(), dataset, ConfigV2{
		RecallK: 1, NDCGK: 1, WarmupRuns: 1, MeasuredRuns: 1, QueryTimeout: time.Second,
		Arms: []ArmFactory{factory}, Timer: &stepTimerV2{step: time.Millisecond},
	})
	if err != nil {
		t.Fatalf("RunV2(): %v", err)
	}
	query := manifest.Arms[0].Queries[0]
	if query.Policy.ForbiddenHits != 1 || query.Policy.Passed || query.ExecutionError != "" || len(query.Hits) != 0 {
		t.Fatalf("transient warmup leak was erased or entered quality ranking: %#v", query)
	}
	measuredManifest, err := RunV2(context.Background(), dataset, ConfigV2{
		RecallK: 1, NDCGK: 1, MeasuredRuns: 2, QueryTimeout: time.Second,
		Arms: []ArmFactory{factory}, Timer: &stepTimerV2{step: time.Millisecond},
	})
	if err != nil {
		t.Fatalf("measured RunV2(): %v", err)
	}
	measured := measuredManifest.Arms[0].Queries[0]
	if measured.Policy.ForbiddenHits != 1 || measured.Policy.Passed || measured.ExecutionError == "" {
		t.Fatalf("transient measured leak or ranking instability was erased: %#v", measured)
	}
}

func TestRunV2RejectsKnownIDWithSubstitutedMemoryPayload(t *testing.T) {
	dataset := mustLoadDatasetV2(t, `{
  "schema_version":"2","id":"payload","version":"1","description":"payload substitution",
  "cases":[{"id":"payload-case","scopes":[{"id":"s","tenant_id":"t","user_id":"u"}],"timeline":[
    {"op":"memory.remember","memory_ref":"known","scope":"s","at":"2026-01-01T00:01:00Z","review_state":"approved","memory":{"kind":"semantic","category":"c","key":"k","value":"trusted"},"evidence":[{"alias":"source","session_id":"session","actor":"user","content":"trusted","occurred_at":"2026-01-01T00:00:00Z"}]},
    {"op":"query","id":"q","scope":"s","at":"2026-01-01T00:02:00Z","text":"trusted","judgments":{"memory_cards":{"relevance":{"known":3}}}}
  ]}]
}`)
	expected := domain.MemoryCard{
		ID:          stableArtifactIDV2("mem", "payload-case", "known"),
		CandidateID: stableArtifactIDV2("cand", "payload-case", "\x00compact-candidate:known"),
		TenantID:    "t", UserID: "u", Kind: domain.MemoryKindSemantic, Category: "c", Key: "k", Value: "trusted",
		SourceEventIDs: []string{stableArtifactIDV2("evt", "payload-case", "source")},
		Version:        1, Status: domain.MemoryActive, CreatedAt: time.Date(2026, 1, 1, 0, 1, 0, 0, time.UTC),
	}
	tampered := expected
	tampered.Value = "substituted"
	factory := armFactory{
		descriptor: ArmDescriptor{ID: "payload-substitution-v1", Version: "1", JudgmentProfile: "reviewed-memory-alias-v1", ResultKind: "memory-card", ConfigHash: hashArmConfig("payload")},
		newRuntime: func(context.Context) (ArmRuntime, error) {
			return ArmRuntime{Store: memstore.New(), Retriever: staticRetrieverV2{hits: []domain.SearchHit{{Memory: tampered, Score: 1}}}}, nil
		},
	}
	manifest, err := RunV2(context.Background(), dataset, ConfigV2{
		RecallK: 1, NDCGK: 1, MeasuredRuns: 1, QueryTimeout: time.Second,
		Arms: []ArmFactory{factory}, Timer: &stepTimerV2{step: time.Millisecond},
	})
	if err != nil {
		t.Fatalf("RunV2(): %v", err)
	}
	query := manifest.Arms[0].Queries[0]
	if query.Quality == nil || query.Quality.RecallAtK != 1 {
		t.Fatalf("known ID should still demonstrate metric spoofing pressure: %#v", query.Quality)
	}
	if query.Policy.MemoryPayloadViolations == 0 || query.Policy.Passed {
		t.Fatalf("payload substitution passed policy: %#v", query.Policy)
	}
	if query.Hits[0].PayloadSHA256 == memoryPayloadHashV2(expected) {
		t.Fatal("hit payload hash did not expose substituted content")
	}
}

func TestRunV2RejectsHitsBeyondDeclaredRetrievalDepth(t *testing.T) {
	dataset := mustLoadDatasetV2(t, `{
  "schema_version":"2","id":"over-limit","version":"1","description":"retriever limit contract",
  "cases":[{"id":"over-limit-case","scopes":[{"id":"s","tenant_id":"t","user_id":"u"}],"timeline":[
    {"op":"memory.remember","memory_ref":"distractor","scope":"s","at":"2026-01-01T00:01:00Z","review_state":"approved","memory":{"kind":"semantic","category":"c","key":"distractor","value":"alpha"},"evidence":[{"alias":"source-a","session_id":"session","actor":"user","content":"alpha","occurred_at":"2026-01-01T00:00:00Z"}]},
    {"op":"memory.remember","memory_ref":"relevant","scope":"s","at":"2026-01-01T00:03:00Z","review_state":"approved","memory":{"kind":"semantic","category":"c","key":"relevant","value":"omega"},"evidence":[{"alias":"source-b","session_id":"session","actor":"user","content":"omega","occurred_at":"2026-01-01T00:02:00Z"}]},
    {"op":"query","id":"q","scope":"s","at":"2026-01-01T00:04:00Z","text":"omega","judgments":{"memory_cards":{"relevance":{"relevant":3}}}}
  ]}]
}`)
	card := func(alias, key, value, source string, createdAt time.Time) domain.MemoryCard {
		return domain.MemoryCard{
			ID:          stableArtifactIDV2("mem", "over-limit-case", alias),
			CandidateID: stableArtifactIDV2("cand", "over-limit-case", "\x00compact-candidate:"+alias),
			TenantID:    "t", UserID: "u", Kind: domain.MemoryKindSemantic, Category: "c", Key: key, Value: value,
			SourceEventIDs: []string{stableArtifactIDV2("evt", "over-limit-case", source)},
			Version:        1, Status: domain.MemoryActive, CreatedAt: createdAt,
		}
	}
	factory := armFactory{
		descriptor: ArmDescriptor{ID: "over-limit-v1", Version: "1", JudgmentProfile: "reviewed-memory-alias-v1", ResultKind: "memory-card", ConfigHash: hashArmConfig("over-limit")},
		newRuntime: func(context.Context) (ArmRuntime, error) {
			return ArmRuntime{Store: memstore.New(), Retriever: staticRetrieverV2{hits: []domain.SearchHit{
				{Memory: card("distractor", "distractor", "alpha", "source-a", time.Date(2026, 1, 1, 0, 1, 0, 0, time.UTC))},
				{Memory: card("relevant", "relevant", "omega", "source-b", time.Date(2026, 1, 1, 0, 3, 0, 0, time.UTC))},
			}}}, nil
		},
	}
	manifest, err := RunV2(context.Background(), dataset, ConfigV2{
		RecallK: 1, NDCGK: 1, MeasuredRuns: 1, QueryTimeout: time.Second,
		Arms: []ArmFactory{factory}, Timer: &stepTimerV2{step: time.Millisecond},
	})
	if err != nil {
		t.Fatalf("RunV2(): %v", err)
	}
	query := manifest.Arms[0].Queries[0]
	if query.Policy.OverLimitHits != 1 || query.Policy.Passed {
		t.Fatalf("over-limit result passed policy: %#v", query.Policy)
	}
	if query.Quality == nil || query.Quality.RecallAtK != 0 || query.Quality.MRR != 0 || query.Quality.NDCGAtK != 0 {
		t.Fatalf("out-of-depth hit entered quality metrics: %#v", query.Quality)
	}
	if len(query.Hits) != 1 || query.Hits[0].Alias != "distractor" {
		t.Fatalf("manifest exposed hits beyond retrieval depth: %#v", query.Hits)
	}
	if manifest.Arms[0].Aggregate.OverLimitHits != 1 || manifest.Arms[0].Aggregate.PolicyPassed {
		t.Fatalf("aggregate over-limit violation missing: %#v", manifest.Arms[0].Aggregate)
	}
}

func TestRunV2ChecksReturnedSourceAliasOrder(t *testing.T) {
	dataset := mustLoadDatasetV2(t, `{
  "schema_version":"2","id":"sources","version":"1","description":"source order",
  "cases":[{"id":"sources-case","scopes":[{"id":"s","tenant_id":"t","user_id":"u"}],"timeline":[
    {"op":"evidence.append","as":"first","scope":"s","session_id":"session","at":"2026-01-01T00:00:00Z","actor":"user","content":"alpha"},
    {"op":"evidence.append","as":"second","scope":"s","session_id":"session","at":"2026-01-01T00:01:00Z","actor":"user","content":"beta"},
    {"op":"candidate.propose","as":"candidate","scope":"s","at":"2026-01-01T00:02:00Z","source_event_ids":["first","second"],"memory":{"kind":"semantic","category":"c","key":"alpha","value":"beta"},"extractor":"fixture","extractor_version":"v2"},
    {"op":"candidate.review","candidate":"candidate","scope":"s","at":"2026-01-01T00:03:00Z","decision":"approve","memory_as":"memory","reviewer_id":"reviewer","reason":"supported"},
    {"op":"query","id":"q","scope":"s","at":"2026-01-01T00:04:00Z","text":"alpha beta","judgments":{"memory_cards":{"relevance":{"memory":3}}}}
  ]}]
}`)
	factory := armFactory{
		descriptor: ArmDescriptor{ID: "reordered-source-v1", Version: "1", JudgmentProfile: "reviewed-memory-alias-v1", ResultKind: "memory-card", ConfigHash: hashArmConfig("reordered")},
		newRuntime: func(context.Context) (ArmRuntime, error) {
			base := memstore.New()
			wrapped := reorderingStoreV2{Store: base}
			retriever, err := retrieval.NewBM25(wrapped)
			return ArmRuntime{Store: wrapped, Retriever: retriever}, err
		},
	}
	manifest, err := RunV2(context.Background(), dataset, ConfigV2{
		RecallK: 1, NDCGK: 1, MeasuredRuns: 1, QueryTimeout: time.Second,
		Arms: []ArmFactory{factory}, Timer: &stepTimerV2{step: time.Millisecond},
	})
	if err != nil {
		t.Fatalf("RunV2(): %v", err)
	}
	query := manifest.Arms[0].Queries[0]
	if query.Policy.ReorderedSources != 1 || query.Policy.Passed {
		t.Fatalf("source order violation not visible: %#v", query.Policy)
	}
	if got := query.Hits[0].SourceAliases; len(got) != 2 || got[0] != "second" || got[1] != "first" {
		t.Fatalf("manifest source aliases = %v", got)
	}
}

func TestRunV2RejectsEvidencePayloadSubstitution(t *testing.T) {
	dataset := mustLoadDatasetV2(t, `{
  "schema_version":"2","id":"evidence-payload","version":"1","description":"evidence substitution",
  "cases":[{"id":"evidence-case","scopes":[{"id":"s","tenant_id":"t","user_id":"u"}],"timeline":[
    {"op":"memory.remember","memory_ref":"memory","scope":"s","at":"2026-01-01T00:01:00Z","review_state":"approved","memory":{"kind":"semantic","category":"c","key":"k","value":"trusted"},"evidence":[{"alias":"source","session_id":"session","actor":"user","content":"trusted evidence","occurred_at":"2026-01-01T00:00:00Z"}]},
    {"op":"query","id":"q","scope":"s","at":"2026-01-01T00:02:00Z","text":"trusted","judgments":{"memory_cards":{"relevance":{"memory":3}}}}
  ]}]
}`)
	factory := armFactory{
		descriptor: ArmDescriptor{ID: "evidence-substitution-v1", Version: "1", JudgmentProfile: "reviewed-memory-alias-v1", ResultKind: "memory-card", ConfigHash: hashArmConfig("evidence")},
		newRuntime: func(context.Context) (ArmRuntime, error) {
			base := memstore.New()
			wrapped := tamperingEvidenceStoreV2{Store: base}
			retriever, err := retrieval.NewBM25(wrapped)
			return ArmRuntime{Store: wrapped, Retriever: retriever}, err
		},
	}
	manifest, err := RunV2(context.Background(), dataset, ConfigV2{
		RecallK: 1, NDCGK: 1, MeasuredRuns: 1, QueryTimeout: time.Second,
		Arms: []ArmFactory{factory}, Timer: &stepTimerV2{step: time.Millisecond},
	})
	if err != nil {
		t.Fatalf("RunV2(): %v", err)
	}
	query := manifest.Arms[0].Queries[0]
	if query.Policy.EvidencePayloadViolations != 1 || query.Policy.Passed {
		t.Fatalf("evidence payload substitution passed policy: %#v", query.Policy)
	}
	expected := domain.EvidenceEvent{
		ID: stableArtifactIDV2("evt", "evidence-case", "source"), TenantID: "t", UserID: "u", SessionID: "session",
		Actor: domain.ActorUser, Content: "trusted evidence", OccurredAt: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC), RecordedAt: time.Date(2026, 1, 1, 0, 1, 0, 0, time.UTC),
	}
	if query.Hits[0].SourcePayloadSHA256[0] == evidencePayloadHashV2(expected) {
		t.Fatal("source payload hash did not expose substituted evidence")
	}
}

func TestRunV2DoesNotLetStoreReturnValueDefineExpectedMemory(t *testing.T) {
	dataset := mustLoadDatasetV2(t, `{
  "schema_version":"2","id":"review-oracle","version":"1","description":"review return corruption",
  "cases":[{"id":"review-case","scopes":[{"id":"s","tenant_id":"t","user_id":"u"}],"timeline":[
    {"op":"memory.remember","memory_ref":"memory","scope":"s","at":"2026-01-01T00:01:00Z","review_state":"approved","memory":{"kind":"semantic","category":"c","key":"k","value":"trusted"},"evidence":[{"alias":"source","session_id":"session","actor":"user","content":"trusted","occurred_at":"2026-01-01T00:00:00Z"}]},
    {"op":"query","id":"q","scope":"s","at":"2026-01-01T00:02:00Z","text":"trusted","judgments":{"memory_cards":{"relevance":{"memory":3}}}}
  ]}]
}`)
	factory := armFactory{
		descriptor: ArmDescriptor{ID: "corrupt-review-v1", Version: "1", JudgmentProfile: "reviewed-memory-alias-v1", ResultKind: "memory-card", ConfigHash: hashArmConfig("review")},
		newRuntime: func(context.Context) (ArmRuntime, error) {
			base := memstore.New()
			wrapped := corruptReviewStoreV2{Store: base}
			retriever, err := retrieval.NewBM25(wrapped)
			return ArmRuntime{Store: wrapped, Retriever: retriever}, err
		},
	}
	_, err := RunV2(context.Background(), dataset, ConfigV2{
		RecallK: 1, NDCGK: 1, MeasuredRuns: 1, QueryTimeout: time.Second,
		Arms: []ArmFactory{factory}, Timer: &stepTimerV2{step: time.Millisecond},
	})
	if err == nil || !strings.Contains(err.Error(), "differs from authored lifecycle") {
		t.Fatalf("corrupt review return error = %v", err)
	}
}

type stepTimerV2 struct {
	now  time.Time
	step time.Duration
}

func (timer *stepTimerV2) Now() time.Time {
	value := timer.now
	timer.now = timer.now.Add(timer.step)
	return value
}

type staticRetrieverV2 struct{ hits []domain.SearchHit }

func (retriever staticRetrieverV2) Search(context.Context, string, string, string, int) ([]domain.SearchHit, error) {
	return append([]domain.SearchHit(nil), retriever.hits...), nil
}

type sequenceRetrieverV2 struct {
	results [][]domain.SearchHit
	next    int
}

func (retriever *sequenceRetrieverV2) Search(context.Context, string, string, string, int) ([]domain.SearchHit, error) {
	index := retriever.next
	if index >= len(retriever.results) {
		index = len(retriever.results) - 1
	}
	retriever.next++
	return append([]domain.SearchHit(nil), retriever.results[index]...), nil
}

type reorderingStoreV2 struct{ store.Store }

func (storage reorderingStoreV2) EvidenceByIDs(ctx context.Context, tenantID, userID string, eventIDs []string) ([]domain.EvidenceEvent, error) {
	events, err := storage.Store.EvidenceByIDs(ctx, tenantID, userID, eventIDs)
	if err != nil {
		return nil, err
	}
	for left, right := 0, len(events)-1; left < right; left, right = left+1, right-1 {
		events[left], events[right] = events[right], events[left]
	}
	return events, nil
}

type tamperingEvidenceStoreV2 struct{ store.Store }

func (storage tamperingEvidenceStoreV2) EvidenceByIDs(ctx context.Context, tenantID, userID string, eventIDs []string) ([]domain.EvidenceEvent, error) {
	events, err := storage.Store.EvidenceByIDs(ctx, tenantID, userID, eventIDs)
	if err == nil && len(events) > 0 {
		events[0].Content = "substituted evidence"
	}
	return events, err
}

type corruptReviewStoreV2 struct{ store.Store }

func (storage corruptReviewStoreV2) ReviewCandidate(ctx context.Context, command store.CandidateReviewCommand) (domain.MemoryCandidate, *domain.MemoryCard, error) {
	candidate, card, err := storage.Store.ReviewCandidate(ctx, command)
	if err == nil && card != nil {
		card.Value = "substituted memory"
	}
	return candidate, card, err
}

func mustLoadDatasetV2(t *testing.T, data string) DatasetV2 {
	t.Helper()
	dataset, err := LoadV2([]byte(data))
	if err != nil {
		t.Fatalf("LoadV2(): %v", err)
	}
	return dataset
}

func findArmResultV2(t *testing.T, manifest ManifestV2, id string) ArmResultV2 {
	t.Helper()
	for _, arm := range manifest.Arms {
		if arm.Descriptor.ID == id {
			return arm
		}
	}
	t.Fatalf("arm %q not found", id)
	return ArmResultV2{}
}

func findQueryResultV2(t *testing.T, arm ArmResultV2, id string) QueryResultV2 {
	t.Helper()
	for _, query := range arm.Queries {
		if query.QueryID == id {
			return query
		}
	}
	t.Fatalf("query %q not found", id)
	return QueryResultV2{}
}
