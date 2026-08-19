package eval

import (
	"math"
	"slices"
	"testing"
	"time"
)

func TestNDCGAtKPerfectAndMissingRelevantResult(t *testing.T) {
	relevance := map[string]float64{
		"most-relevant":   3,
		"also-relevant":   2,
		"barely-relevant": 1,
	}

	perfect, err := NDCGAtK([]string{"most-relevant", "also-relevant", "barely-relevant"}, relevance, 3)
	if err != nil {
		t.Fatalf("perfect nDCG: %v", err)
	}
	if perfect != 1 {
		t.Fatalf("perfect nDCG = %v, want 1", perfect)
	}

	missing, err := NDCGAtK([]string{"most-relevant", "unjudged", "barely-relevant"}, relevance, 3)
	if err != nil {
		t.Fatalf("missing-result nDCG: %v", err)
	}
	if !(missing > 0 && missing < 1) {
		t.Fatalf("missing-result nDCG = %v, want between 0 and 1", missing)
	}

	want := (7 + 1/math.Log2(4)) / (7 + 3/math.Log2(3) + 1/math.Log2(4))
	if math.Abs(missing-want) > 1e-12 {
		t.Fatalf("missing-result nDCG = %.12f, want %.12f", missing, want)
	}
}

func TestNDCGAtKUsesAllJudgmentsForIdealRanking(t *testing.T) {
	got, err := NDCGAtK(
		[]string{"retrieved-relevant"},
		map[string]float64{"retrieved-relevant": 1, "missed-more-relevant": 3},
		1,
	)
	if err != nil {
		t.Fatalf("nDCG: %v", err)
	}
	want := 1.0 / 7.0
	if math.Abs(got-want) > 1e-12 {
		t.Fatalf("nDCG = %.12f, want %.12f", got, want)
	}
}

func TestNDCGAtKNoPositiveJudgmentsIsZero(t *testing.T) {
	got, err := NDCGAtK([]string{"unknown"}, map[string]float64{"known": 0}, 5)
	if err != nil {
		t.Fatalf("nDCG: %v", err)
	}
	if got != 0 {
		t.Fatalf("nDCG = %v, want 0", got)
	}
}

func TestNDCGAtKRejectsInvalidInput(t *testing.T) {
	tests := []struct {
		name      string
		ranked    []string
		relevance map[string]float64
		k         int
	}{
		{name: "cutoff", k: 0},
		{name: "empty judgment key", relevance: map[string]float64{"": 1}, k: 1},
		{name: "negative relevance", relevance: map[string]float64{"item": -1}, k: 1},
		{name: "non-finite relevance", relevance: map[string]float64{"item": math.Inf(1)}, k: 1},
		{name: "duplicate ranked key", ranked: []string{"item", "item"}, relevance: map[string]float64{"item": 1}, k: 2},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if _, err := NDCGAtK(test.ranked, test.relevance, test.k); err == nil {
				t.Fatal("NDCGAtK() error = nil, want invalid-input error")
			}
		})
	}
}

func TestSummarizeLatencyUsesNearestRankWithoutMutatingSamples(t *testing.T) {
	samples := []time.Duration{
		100 * time.Millisecond,
		10 * time.Millisecond,
		40 * time.Millisecond,
		20 * time.Millisecond,
		90 * time.Millisecond,
	}
	original := append([]time.Duration(nil), samples...)

	summary, err := SummarizeLatency(samples)
	if err != nil {
		t.Fatalf("SummarizeLatency(): %v", err)
	}
	if summary.P50 != 40*time.Millisecond {
		t.Fatalf("p50 = %s, want 40ms", summary.P50)
	}
	if summary.P95 != 100*time.Millisecond {
		t.Fatalf("p95 = %s, want 100ms", summary.P95)
	}
	if !slices.Equal(samples, original) {
		t.Fatalf("samples mutated: got %v want %v", samples, original)
	}
}

func TestSummarizeLatencySingleSample(t *testing.T) {
	summary, err := SummarizeLatency([]time.Duration{17 * time.Millisecond})
	if err != nil {
		t.Fatalf("SummarizeLatency(): %v", err)
	}
	if summary.P50 != 17*time.Millisecond || summary.P95 != 17*time.Millisecond {
		t.Fatalf("summary = %#v, want both percentiles at 17ms", summary)
	}
}

func TestSummarizeLatencyRejectsInvalidSamples(t *testing.T) {
	if _, err := SummarizeLatency(nil); err == nil {
		t.Fatal("empty sample error = nil")
	}
	if _, err := SummarizeLatency([]time.Duration{time.Millisecond, -time.Nanosecond}); err == nil {
		t.Fatal("negative sample error = nil")
	}
}
