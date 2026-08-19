package eval

import (
	"context"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"
)

func TestRepositoryLifecycleDatasetV2RunsEveryBuiltinArm(t *testing.T) {
	_, filename, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("runtime.Caller failed")
	}
	data, err := os.ReadFile(filepath.Join(filepath.Dir(filename), "..", "..", "datasets", "memory-lifecycle-v2.json"))
	if err != nil {
		t.Fatalf("read repository v2 dataset: %v", err)
	}
	dataset, err := LoadV2(data)
	if err != nil {
		t.Fatalf("load repository v2 dataset: %v", err)
	}
	if dataset.ID != "memory-lifecycle-hard-v2" || dataset.Version != "2.0.0" || len(dataset.Cases) != 28 || countQueriesV2(dataset) != 28 {
		t.Fatalf("unexpected repository dataset metadata: id=%q version=%q cases=%d queries=%d", dataset.ID, dataset.Version, len(dataset.Cases), countQueriesV2(dataset))
	}

	tagCounts := make(map[string]int)
	for _, testCase := range dataset.Cases {
		for _, tag := range testCase.Tags {
			tagCounts[tag]++
		}
	}
	for tag, want := range map[string]int{
		"direct": 6, "multi_session_entity": 6, "update_conflict": 6,
		"language_hard": 4, "lifecycle_non_recall": 2, "scope_adversarial": 4,
		"lang_en": 9, "lang_zh": 10, "lang_mixed": 9,
	} {
		if tagCounts[tag] != want {
			t.Errorf("tag %q count = %d, want %d", tag, tagCounts[tag], want)
		}
	}

	config := ConfigV2{
		RecallK: 5, NDCGK: 10, MeasuredRuns: 2, QueryTimeout: 2 * time.Second,
		Arms: BuiltinArmFactories(), Timer: &stepTimerV2{step: time.Millisecond},
		GeneratedAt:       func() time.Time { return time.Date(2026, 8, 19, 12, 0, 0, 0, time.UTC) },
		RequirePolicyPass: true,
	}
	manifest, err := RunV2(context.Background(), dataset, config)
	if err != nil {
		t.Fatalf("run repository v2 dataset: %v", err)
	}
	if !manifest.Acceptance.PolicyPassRequired || !manifest.Acceptance.PolicyPassVerified {
		t.Fatalf("policy acceptance is incomplete: %#v", manifest.Acceptance)
	}

	noMemory := findArmResultV2(t, manifest, ArmNoMemoryV1)
	if noMemory.Aggregate.RecallAtK != 0 || !noMemory.Aggregate.PolicyPassed {
		t.Fatalf("unexpected no-memory baseline: %#v", noMemory.Aggregate)
	}
	bm25 := findArmResultV2(t, manifest, ArmReviewedCardsBM25V1)
	if bm25.Aggregate.QueryCount != 28 || bm25.Aggregate.QualityQueryCount != 23 || bm25.Aggregate.NonRecallQueryCount != 5 {
		t.Fatalf("unexpected BM25 query counts: %#v", bm25.Aggregate)
	}
	if bm25.Aggregate.RecallAtK < 0.95 || bm25.Aggregate.MRR < 0.95 || bm25.Aggregate.NDCGAtK < 0.95 || bm25.Aggregate.PassRate < 0.95 {
		t.Fatalf("BM25 dropped below the v2 quality floor: %#v", bm25.Aggregate)
	}
	if !bm25.Aggregate.PolicyPassed || bm25.Aggregate.PolicyPassRate != 1 || bm25.Aggregate.NonRecallPassRate != 1 {
		t.Fatalf("BM25 policy gate failed: %#v", bm25.Aggregate)
	}
	if bm25.Aggregate.ForbiddenHits != 0 || bm25.Aggregate.RequireEmptyFailures != 0 ||
		bm25.Aggregate.ScopeViolations != 0 || bm25.Aggregate.NonActiveHits != 0 ||
		bm25.Aggregate.UnknownHits != 0 || bm25.Aggregate.DuplicateHits != 0 || bm25.Aggregate.OverLimitHits != 0 ||
		bm25.Aggregate.UnknownSourceIDs != 0 || bm25.Aggregate.MissingSources != 0 ||
		bm25.Aggregate.ReorderedSources != 0 || bm25.Aggregate.SourceScopeViolations != 0 ||
		bm25.Aggregate.MemoryPayloadViolations != 0 || bm25.Aggregate.EvidencePayloadViolations != 0 ||
		bm25.Aggregate.QueryExecutionFailures != 0 {
		t.Fatalf("BM25 emitted a policy violation: %#v", bm25.Aggregate)
	}

	secondConfig := config
	secondConfig.Timer = &stepTimerV2{step: 17 * time.Millisecond}
	secondConfig.GeneratedAt = func() time.Time { return time.Date(2026, 8, 20, 12, 0, 0, 0, time.UTC) }
	second, err := RunV2(context.Background(), dataset, secondConfig)
	if err != nil {
		t.Fatalf("run repository v2 dataset again: %v", err)
	}
	if bm25.Aggregate.QualityResultSHA256 != findArmResultV2(t, second, ArmReviewedCardsBM25V1).Aggregate.QualityResultSHA256 {
		t.Fatal("quality result hash changed with timestamp and latency only")
	}
}
