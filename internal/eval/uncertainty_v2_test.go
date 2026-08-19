package eval

import (
	"encoding/json"
	"math"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestQualityIntervalsV2AreDeterministicQueryLevelPercentiles(t *testing.T) {
	samples := []QueryQualityV2{
		{RelevantCount: 2, RecallAtK: 0, MRR: 0, NDCGAtK: 0.1, Passed: false},
		{RelevantCount: 2, RecallAtK: 0, MRR: 0.5, NDCGAtK: 0.3, Passed: false},
		{RelevantCount: 2, RecallAtK: 1, MRR: 0.5, NDCGAtK: 0.7, Passed: false},
		{RelevantCount: 2, RecallAtK: 1, MRR: 1, NDCGAtK: 0.9, Passed: true},
	}

	first, err := qualityIntervalsV2(samples)
	if err != nil {
		t.Fatalf("qualityIntervalsV2(): %v", err)
	}
	second, err := qualityIntervalsV2(samples)
	if err != nil {
		t.Fatalf("second qualityIntervalsV2(): %v", err)
	}
	if !reflect.DeepEqual(first, second) {
		t.Fatalf("bootstrap changed across identical runs: first=%#v second=%#v", first, second)
	}
	firstJSON, err := json.Marshal(first)
	if err != nil {
		t.Fatalf("marshal first intervals: %v", err)
	}
	secondJSON, err := json.Marshal(second)
	if err != nil {
		t.Fatalf("marshal second intervals: %v", err)
	}
	if string(firstJSON) != string(secondJSON) {
		t.Fatalf("bootstrap JSON is not byte stable: first=%s second=%s", firstJSON, secondJSON)
	}

	want := QualityIntervalsV2{
		RecallAtK: ConfidenceIntervalV2{Estimate: 0.5, Lower: 0, Upper: 1},
		MRR:       ConfidenceIntervalV2{Estimate: 0.5, Lower: 0.125, Upper: 0.875},
		NDCGAtK:   ConfidenceIntervalV2{Estimate: 0.5, Lower: 0.19999999999999998, Upper: 0.8},
		PassRate:  ConfidenceIntervalV2{Estimate: 0.25, Lower: 0, Upper: 0.75},
	}
	if !reflect.DeepEqual(first, want) {
		t.Fatalf("intervals = %#v, want %#v", first, want)
	}
	assertIntervalInvariantV2(t, first.RecallAtK)
	assertIntervalInvariantV2(t, first.MRR)
	assertIntervalInvariantV2(t, first.NDCGAtK)
	assertIntervalInvariantV2(t, first.PassRate)
}

func TestQualityIntervalsV2SingleQueryCollapsesToEstimate(t *testing.T) {
	intervals, err := qualityIntervalsV2([]QueryQualityV2{{
		RelevantCount: 3, RecallAtK: 2.0 / 3.0, MRR: 0.5, NDCGAtK: 0.75, Passed: false,
	}})
	if err != nil {
		t.Fatalf("qualityIntervalsV2(): %v", err)
	}
	for name, interval := range map[string]ConfidenceIntervalV2{
		"recall": intervals.RecallAtK, "mrr": intervals.MRR, "ndcg": intervals.NDCGAtK, "pass": intervals.PassRate,
	} {
		if interval.Lower != interval.Estimate || interval.Upper != interval.Estimate {
			t.Errorf("%s interval did not collapse: %#v", name, interval)
		}
	}
	if intervals.NDCGAtK.BoundaryDegenerate {
		t.Fatal("a constant non-boundary interval was marked boundary-degenerate")
	}
	if !intervals.PassRate.BoundaryDegenerate {
		t.Fatal("a constant zero pass-rate interval was not marked boundary-degenerate")
	}
}

func TestQualityIntervalsV2RejectsEmptyAndInvalidSamples(t *testing.T) {
	if _, err := qualityIntervalsV2(nil); err == nil || !strings.Contains(err.Error(), "at least one") {
		t.Fatalf("empty samples error = %v", err)
	}

	tests := []struct {
		name   string
		sample QueryQualityV2
	}{
		{name: "no gold", sample: QueryQualityV2{}},
		{name: "recall NaN", sample: QueryQualityV2{RelevantCount: 1, RecallAtK: math.NaN()}},
		{name: "recall infinity", sample: QueryQualityV2{RelevantCount: 1, RecallAtK: math.Inf(1)}},
		{name: "recall below zero", sample: QueryQualityV2{RelevantCount: 1, RecallAtK: -0.01}},
		{name: "mrr above one", sample: QueryQualityV2{RelevantCount: 1, MRR: 1.01}},
		{name: "ndcg NaN", sample: QueryQualityV2{RelevantCount: 1, NDCGAtK: math.NaN()}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if _, err := qualityIntervalsV2([]QueryQualityV2{test.sample}); err == nil {
				t.Fatal("qualityIntervalsV2() accepted an invalid sample")
			}
		})
	}
}

func TestSummarizeArmV2IntervalsExcludeNoGoldQueriesAndMatchAggregates(t *testing.T) {
	qualityOne := QueryQualityV2{RelevantCount: 2, RecallAtK: 0.5, MRR: 1, NDCGAtK: 0.75, Passed: false}
	qualityTwo := QueryQualityV2{RelevantCount: 1, RecallAtK: 1, MRR: 0.5, NDCGAtK: 1, Passed: true}
	queries := []QueryResultV2{
		{CaseID: "c", QueryID: "gold-one", Quality: &qualityOne, Policy: QueryPolicyV2{Passed: true}},
		{CaseID: "c", QueryID: "no-gold", Quality: nil, Policy: QueryPolicyV2{Passed: true}},
		{CaseID: "c", QueryID: "gold-two", Quality: &qualityTwo, Policy: QueryPolicyV2{Passed: true}},
	}

	aggregate, err := summarizeArmV2(queries)
	if err != nil {
		t.Fatalf("summarizeArmV2(): %v", err)
	}
	if aggregate.QualityQueryCount != 2 || aggregate.NonRecallQueryCount != 1 {
		t.Fatalf("query counts = quality %d no-gold %d", aggregate.QualityQueryCount, aggregate.NonRecallQueryCount)
	}
	if aggregate.QualityIntervals == nil {
		t.Fatal("quality intervals are missing")
	}
	if aggregate.QualityIntervals.RecallAtK.Estimate != aggregate.RecallAtK ||
		aggregate.QualityIntervals.MRR.Estimate != aggregate.MRR ||
		aggregate.QualityIntervals.NDCGAtK.Estimate != aggregate.NDCGAtK ||
		aggregate.QualityIntervals.PassRate.Estimate != aggregate.PassRate {
		t.Fatalf("interval estimates do not exactly match aggregates: aggregate=%#v intervals=%#v", aggregate, aggregate.QualityIntervals)
	}

	policyOnly, err := summarizeArmV2([]QueryResultV2{{CaseID: "c", QueryID: "policy", Policy: QueryPolicyV2{Passed: true}}})
	if err != nil {
		t.Fatalf("summarize policy-only arm: %v", err)
	}
	if policyOnly.QualityIntervals != nil || policyOnly.QualityQueryCount != 0 || policyOnly.NonRecallQueryCount != 1 {
		t.Fatalf("policy-only aggregate emitted quality uncertainty: %#v", policyOnly)
	}
}

func TestQualityUncertaintyMetadataAndConfigHashAreVersioned(t *testing.T) {
	metadata := qualityUncertaintyMetadataV2()
	if runnerVersionV2 != "evaluation-runner-v2.1" || metadata.Algorithm != "percentile-bootstrap-query-mean-splitmix64-v1" ||
		metadata.ConfidenceLevel != 0.95 || metadata.Resamples != 10_000 || metadata.Seed != 2_026_081_901 ||
		metadata.SamplingUnit != "quality_query" || metadata.Paired || metadata.Stratified {
		t.Fatalf("unexpected fixed uncertainty contract: runner=%q metadata=%#v", runnerVersionV2, metadata)
	}
	if nearestRankIndexV2(10_000, 250, 10_000) != 249 || nearestRankIndexV2(10_000, 9750, 10_000) != 9749 {
		t.Fatal("95% nearest-rank percentile endpoints changed")
	}

	config := ConfigV2{
		RecallK: 5, NDCGK: 10, WarmupRuns: 1, MeasuredRuns: 3, QueryTimeout: 15 * time.Second,
		Arms: BuiltinArmFactories(), Source: SourceProofV2{CleanRequired: true}, RequirePolicyPass: true,
	}
	const wantConfigHash = "21d731d740cbc2e8d51506e4fd9888d3cff4aed52e71e68fdf8ec613e7176018"
	if got := configHashV2(config); got != wantConfigHash {
		t.Fatalf("config hash = %q, want %q", got, wantConfigHash)
	}
}

func TestSplitMix64V2GoldenSequenceAndUnbiasedRejection(t *testing.T) {
	random := splitMix64V2{state: qualityBootstrapSeedV2}
	wantNext := []uint64{
		16810279466179616732, 7151007997394725314, 18259025032012800547,
		12666003978370311287, 16698532482929470562, 736423274849594595,
	}
	for index, want := range wantNext {
		if got := random.next(); got != want {
			t.Fatalf("next[%d]=%d, want %d", index, got, want)
		}
	}

	random = splitMix64V2{state: qualityBootstrapSeedV2}
	wantMod4 := []int{0, 2, 3, 3, 2, 3, 1, 0, 2, 1, 3, 2}
	for index, want := range wantMod4 {
		if got := random.intn(4); got != want {
			t.Fatalf("intn4[%d]=%d, want %d", index, got, want)
		}
	}

	// This state makes the next output fall below the rejection threshold for
	// bound 2^62+1. The expected result comes from the following accepted draw,
	// proving that intn did not use biased value%%bound directly.
	random = splitMix64V2{state: 1663341877513419478}
	const largeBound = uint64(4611686018427387905)
	if got, want := random.intn(largeBound), 3657680391930858827; got != want {
		t.Fatalf("rejection-path intn=%d, want %d", got, want)
	}
}

func TestQualityIntervalsV2MarksBoundaryDegenerateSamples(t *testing.T) {
	intervals, err := qualityIntervalsV2([]QueryQualityV2{
		{RelevantCount: 1, RecallAtK: 1, MRR: 1, NDCGAtK: 1, Passed: true},
		{RelevantCount: 1, RecallAtK: 1, MRR: 1, NDCGAtK: 1, Passed: true},
	})
	if err != nil {
		t.Fatalf("qualityIntervalsV2(): %v", err)
	}
	for name, interval := range map[string]ConfidenceIntervalV2{
		"recall": intervals.RecallAtK, "mrr": intervals.MRR, "ndcg": intervals.NDCGAtK, "pass": intervals.PassRate,
	} {
		if !interval.BoundaryDegenerate || interval.Lower != 1 || interval.Upper != 1 {
			t.Errorf("%s interval=%#v, want explicit boundary degeneration", name, interval)
		}
	}
}

func TestQualityResultHashV2DoesNotIncludeUncertaintyOutput(t *testing.T) {
	quality := QueryQualityV2{RelevantCount: 2, RecallAtK: 0.5, MRR: 1, NDCGAtK: 0.75, Passed: false}
	queries := []QueryResultV2{{
		CaseID: "case", QueryID: "query", Scope: "scope", RetrievalDepth: 10,
		Hits:                 []HitResultV2{{Rank: 1, Alias: "memory", MemoryID: "memory-id", Score: 0.75}},
		DurationsNanoseconds: []int64{1, 2, 3}, Quality: &quality, Policy: QueryPolicyV2{Passed: true},
	}}
	const wantQualityHash = "1d44e0e9c3465a430c6212cc29d407294d66ab73e3a7c0963b38fd998ef86c70"
	if got := qualityResultHashV2(queries); got != wantQualityHash {
		t.Fatalf("quality hash = %q, want %q", got, wantQualityHash)
	}
	queries[0].DurationsNanoseconds = []int64{999}
	if got := qualityResultHashV2(queries); got != wantQualityHash {
		t.Fatalf("quality hash changed with latency: %q", got)
	}
}

func assertIntervalInvariantV2(t *testing.T, interval ConfidenceIntervalV2) {
	t.Helper()
	if interval.Lower > interval.Upper || math.IsNaN(interval.Estimate) || math.IsInf(interval.Estimate, 0) {
		t.Fatalf("invalid interval order: %#v", interval)
	}
}
