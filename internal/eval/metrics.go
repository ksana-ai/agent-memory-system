package eval

import (
	"errors"
	"fmt"
	"math"
	"sort"
	"time"
)

// NDCGAtK computes normalized discounted cumulative gain for a ranked list.
// relevanceByKey contains non-negative graded relevance judgments; an
// unjudged ranked key has zero relevance. The ideal ranking is derived from all
// judgments, so relevant items missing from rankedKeys still reduce the score.
// Duplicate ranked keys are rejected because counting the same item twice can
// inflate a retrieval metric.
func NDCGAtK(rankedKeys []string, relevanceByKey map[string]float64, k int) (float64, error) {
	if k <= 0 {
		return 0, errors.New("nDCG cutoff must be positive")
	}

	idealRelevance := make([]float64, 0, len(relevanceByKey))
	for key, relevance := range relevanceByKey {
		if key == "" {
			return 0, errors.New("nDCG relevance key must not be empty")
		}
		if relevance < 0 || math.IsNaN(relevance) || math.IsInf(relevance, 0) {
			return 0, fmt.Errorf("nDCG relevance for %q must be finite and non-negative", key)
		}
		idealRelevance = append(idealRelevance, relevance)
	}
	sort.Slice(idealRelevance, func(i, j int) bool {
		return idealRelevance[i] > idealRelevance[j]
	})

	seen := make(map[string]struct{}, min(k, len(rankedKeys)))
	actualRelevance := make([]float64, 0, min(k, len(rankedKeys)))
	for rank, key := range rankedKeys {
		if rank >= k {
			break
		}
		if _, exists := seen[key]; exists {
			return 0, fmt.Errorf("nDCG ranked key %q is duplicated within top %d", key, k)
		}
		seen[key] = struct{}{}
		actualRelevance = append(actualRelevance, relevanceByKey[key])
	}

	idealDCG, err := discountedCumulativeGain(idealRelevance, k)
	if err != nil {
		return 0, err
	}
	if idealDCG == 0 {
		return 0, nil
	}
	actualDCG, err := discountedCumulativeGain(actualRelevance, k)
	if err != nil {
		return 0, err
	}
	return actualDCG / idealDCG, nil
}

// LatencyPercentiles contains nearest-rank latency percentiles.
type LatencyPercentiles struct {
	P50 time.Duration
	P95 time.Duration
}

// SummarizeLatency returns nearest-rank p50 and p95 values. It rejects an
// empty sample or negative durations and does not mutate the input slice.
func SummarizeLatency(samples []time.Duration) (LatencyPercentiles, error) {
	if len(samples) == 0 {
		return LatencyPercentiles{}, errors.New("latency samples must not be empty")
	}
	ordered := append([]time.Duration(nil), samples...)
	for _, sample := range ordered {
		if sample < 0 {
			return LatencyPercentiles{}, errors.New("latency samples must be non-negative")
		}
	}
	sort.Slice(ordered, func(i, j int) bool {
		return ordered[i] < ordered[j]
	})
	return LatencyPercentiles{
		P50: nearestRank(ordered, 0.50),
		P95: nearestRank(ordered, 0.95),
	}, nil
}

func discountedCumulativeGain(relevance []float64, k int) (float64, error) {
	limit := min(k, len(relevance))
	dcg := 0.0
	for index := 0; index < limit; index++ {
		gain := math.Exp2(relevance[index]) - 1
		if math.IsInf(gain, 0) || math.IsNaN(gain) {
			return 0, fmt.Errorf("nDCG relevance at rank %d produces a non-finite gain", index+1)
		}
		dcg += gain / math.Log2(float64(index)+2)
	}
	return dcg, nil
}

func nearestRank(ordered []time.Duration, percentile float64) time.Duration {
	rank := int(math.Ceil(percentile * float64(len(ordered))))
	if rank < 1 {
		rank = 1
	}
	return ordered[rank-1]
}
