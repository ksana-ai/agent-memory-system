package eval

import (
	"errors"
	"fmt"
	"math"
	"sort"
)

const (
	qualityBootstrapAlgorithmV2              = "percentile-bootstrap-query-mean-splitmix64-v1"
	qualityBootstrapConfidenceLevelV2        = 0.95
	qualityBootstrapConfidenceBPSV2          = 9500
	qualityBootstrapResamplesV2              = 10_000
	qualityBootstrapSeedV2            uint64 = 2_026_081_901
	qualityBootstrapBasisV2                  = 10_000
	qualityBootstrapSamplingUnitV2           = "quality_query"
)

func qualityUncertaintyMetadataV2() QualityUncertaintyMetadataV2 {
	return QualityUncertaintyMetadataV2{
		Algorithm:       qualityBootstrapAlgorithmV2,
		ConfidenceLevel: qualityBootstrapConfidenceLevelV2,
		Resamples:       qualityBootstrapResamplesV2,
		Seed:            qualityBootstrapSeedV2,
		SamplingUnit:    qualityBootstrapSamplingUnitV2,
		Paired:          false,
		Stratified:      false,
	}
}

// qualityIntervalsV2 applies a percentile bootstrap to the query-level metric
// vectors. All four metrics share each resampled query index so their marginal
// intervals preserve the dataset's query-level coupling. The fixed SplitMix64
// implementation, nearest-rank quantiles, resample count, and seed make the
// result independent of math/rand and stable across Go releases.
func qualityIntervalsV2(quality []QueryQualityV2) (QualityIntervalsV2, error) {
	if len(quality) == 0 {
		return QualityIntervalsV2{}, errors.New("quality bootstrap requires at least one quality query")
	}

	estimates := [4]float64{}
	for index, sample := range quality {
		if sample.RelevantCount < 1 {
			return QualityIntervalsV2{}, fmt.Errorf("quality bootstrap sample %d has no relevant judgments", index)
		}
		values := [4]float64{sample.RecallAtK, sample.MRR, sample.NDCGAtK, boolFloatV2(sample.Passed)}
		for metric, value := range values {
			if math.IsNaN(value) || math.IsInf(value, 0) || value < 0 || value > 1 {
				return QualityIntervalsV2{}, fmt.Errorf("quality bootstrap sample %d metric %d must be finite and between zero and one", index, metric)
			}
			estimates[metric] += value
		}
	}
	for metric := range estimates {
		estimates[metric] /= float64(len(quality))
	}

	bootstrapMeans := [4][]float64{}
	for metric := range bootstrapMeans {
		bootstrapMeans[metric] = make([]float64, qualityBootstrapResamplesV2)
	}
	random := splitMix64V2{state: qualityBootstrapSeedV2}
	for replicate := 0; replicate < qualityBootstrapResamplesV2; replicate++ {
		sums := [4]float64{}
		for range quality {
			sample := quality[random.intn(uint64(len(quality)))]
			sums[0] += sample.RecallAtK
			sums[1] += sample.MRR
			sums[2] += sample.NDCGAtK
			sums[3] += boolFloatV2(sample.Passed)
		}
		for metric := range sums {
			bootstrapMeans[metric][replicate] = sums[metric] / float64(len(quality))
		}
	}

	intervals := [4]ConfidenceIntervalV2{}
	for metric := range bootstrapMeans {
		sort.Float64s(bootstrapMeans[metric])
		lower := bootstrapMeans[metric][nearestRankIndexV2(len(bootstrapMeans[metric]), (qualityBootstrapBasisV2-qualityBootstrapConfidenceBPSV2)/2, qualityBootstrapBasisV2)]
		upper := bootstrapMeans[metric][nearestRankIndexV2(len(bootstrapMeans[metric]), (qualityBootstrapBasisV2+qualityBootstrapConfidenceBPSV2)/2, qualityBootstrapBasisV2)]
		intervals[metric] = ConfidenceIntervalV2{
			Estimate: estimates[metric], Lower: lower, Upper: upper,
			BoundaryDegenerate: lower == upper && (lower == 0 || lower == 1),
		}
	}

	return QualityIntervalsV2{
		RecallAtK: intervals[0],
		MRR:       intervals[1],
		NDCGAtK:   intervals[2],
		PassRate:  intervals[3],
	}, nil
}

func boolFloatV2(value bool) float64 {
	if value {
		return 1
	}
	return 0
}

// nearestRankIndexV2 returns the zero-based nearest-rank quantile index for a
// rational probability numerator/denominator. Integer arithmetic fixes the
// endpoint definition and avoids floating-point rounding at 2.5% and 97.5%.
func nearestRankIndexV2(length, numerator, denominator int) int {
	rank := (length*numerator + denominator - 1) / denominator
	if rank < 1 {
		return 0
	}
	if rank > length {
		return length - 1
	}
	return rank - 1
}

type splitMix64V2 struct {
	state uint64
}

func (random *splitMix64V2) next() uint64 {
	random.state += 0x9e3779b97f4a7c15
	value := random.state
	value = (value ^ (value >> 30)) * 0xbf58476d1ce4e5b9
	value = (value ^ (value >> 27)) * 0x94d049bb133111eb
	return value ^ (value >> 31)
}

func (random *splitMix64V2) intn(bound uint64) int {
	// Reject the short prefix so the accepted uint64 range has a size exactly
	// divisible by bound. This avoids modulo bias while remaining deterministic.
	threshold := -bound % bound
	for {
		value := random.next()
		if value >= threshold {
			return int(value % bound)
		}
	}
}
