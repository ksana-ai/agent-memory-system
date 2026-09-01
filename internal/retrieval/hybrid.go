package retrieval

import (
	"context"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/ksana-ai/agent-memory-system/internal/app"
	"github.com/ksana-ai/agent-memory-system/internal/domain"
)

const (
	hybridRRFK              = 60
	hybridMinimumCandidates = 20
	hybridMaximumCandidates = 100
)

// Hybrid combines lexical and dense rankings with strict reciprocal-rank
// fusion. It intentionally has no one-branch fallback: silently changing the
// retrieval policy during an outage would make serving and evaluation
// behavior incomparable.
type Hybrid struct {
	lexical app.Retriever
	dense   app.Retriever
}

func NewHybrid(lexical, dense app.Retriever) (*Hybrid, error) {
	if lexical == nil || dense == nil {
		return nil, fmt.Errorf("hybrid retrieval branches are required: %w", domain.ErrInvalid)
	}
	return &Hybrid{lexical: lexical, dense: dense}, nil
}

func (retriever *Hybrid) Search(
	ctx context.Context,
	tenantID, userID, query string,
	limit int,
	asOf time.Time,
) ([]domain.SearchHit, error) {
	if limit <= 0 || strings.TrimSpace(query) == "" {
		return []domain.SearchHit{}, nil
	}
	depth := hybridCandidateDepth(limit)

	type branchResult struct {
		hits []domain.SearchHit
		err  error
	}
	results := [2]branchResult{}
	var wait sync.WaitGroup
	wait.Add(2)
	go func() {
		defer wait.Done()
		results[0].hits, results[0].err = retriever.lexical.Search(ctx, tenantID, userID, query, depth, asOf)
	}()
	go func() {
		defer wait.Done()
		results[1].hits, results[1].err = retriever.dense.Search(ctx, tenantID, userID, query, depth, asOf)
	}()
	wait.Wait()

	// Deterministic error precedence avoids making simultaneous failures depend
	// on goroutine scheduling.
	if results[0].err != nil {
		return nil, fmt.Errorf("hybrid lexical branch failed: %w", results[0].err)
	}
	if results[1].err != nil {
		return nil, fmt.Errorf("hybrid dense branch failed: %w", results[1].err)
	}

	type fusedHit struct {
		memory   domain.MemoryCard
		score    float64
		bestRank int
	}
	fused := make(map[string]*fusedHit, len(results[0].hits)+len(results[1].hits))
	for _, result := range results {
		if len(result.hits) > depth {
			return nil, hybridInvariant("hybrid branch exceeded its candidate limit")
		}
		seen := make(map[string]struct{}, len(result.hits))
		for index, hit := range result.hits {
			if err := validateHybridHit(hit, tenantID, userID, asOf); err != nil {
				return nil, err
			}
			if _, duplicate := seen[hit.Memory.ID]; duplicate {
				return nil, hybridInvariant("hybrid branch returned a duplicate memory")
			}
			seen[hit.Memory.ID] = struct{}{}

			rank := index + 1
			entry, exists := fused[hit.Memory.ID]
			if !exists {
				fused[hit.Memory.ID] = &fusedHit{
					memory:   hit.Memory,
					score:    1 / float64(hybridRRFK+rank),
					bestRank: rank,
				}
				continue
			}
			if !reflect.DeepEqual(entry.memory, hit.Memory) {
				return nil, hybridInvariant("hybrid branches disagreed on a memory payload")
			}
			entry.score += 1 / float64(hybridRRFK+rank)
			if rank < entry.bestRank {
				entry.bestRank = rank
			}
		}
	}

	ranked := make([]fusedHit, 0, len(fused))
	for _, hit := range fused {
		ranked = append(ranked, *hit)
	}
	sort.Slice(ranked, func(i, j int) bool {
		if ranked[i].score != ranked[j].score {
			return ranked[i].score > ranked[j].score
		}
		if ranked[i].bestRank != ranked[j].bestRank {
			return ranked[i].bestRank < ranked[j].bestRank
		}
		if !ranked[i].memory.CreatedAt.Equal(ranked[j].memory.CreatedAt) {
			return ranked[i].memory.CreatedAt.After(ranked[j].memory.CreatedAt)
		}
		return ranked[i].memory.ID < ranked[j].memory.ID
	})
	if len(ranked) > limit {
		ranked = ranked[:limit]
	}
	result := make([]domain.SearchHit, len(ranked))
	for index, hit := range ranked {
		result[index] = domain.SearchHit{Memory: hit.memory, Score: hit.score}
	}
	return result, nil
}

func hybridCandidateDepth(limit int) int {
	if limit > hybridMaximumCandidates/4 {
		return hybridMaximumCandidates
	}
	depth := 4 * limit
	if depth < hybridMinimumCandidates {
		return hybridMinimumCandidates
	}
	return depth
}

func validateHybridHit(hit domain.SearchHit, tenantID, userID string, asOf time.Time) error {
	memory := hit.Memory
	if strings.TrimSpace(memory.ID) == "" {
		return hybridInvariant("hybrid branch returned a memory without an ID")
	}
	if memory.TenantID != tenantID || memory.UserID != userID {
		return hybridInvariant("hybrid branch returned a memory outside the requested scope")
	}
	if memory.Status != domain.MemoryActive ||
		(memory.ExpiresAt != nil && !memory.ExpiresAt.After(asOf)) {
		return hybridInvariant("hybrid branch returned a non-serviceable memory")
	}
	if len(memory.SourceEventIDs) == 0 {
		return hybridInvariant("hybrid branch returned a memory without provenance")
	}
	seenSources := make(map[string]struct{}, len(memory.SourceEventIDs))
	for _, sourceID := range memory.SourceEventIDs {
		normalizedSourceID := strings.TrimSpace(sourceID)
		if normalizedSourceID == "" {
			return hybridInvariant("hybrid branch returned empty provenance")
		}
		if _, duplicate := seenSources[normalizedSourceID]; duplicate {
			return hybridInvariant("hybrid branch returned duplicate provenance")
		}
		seenSources[normalizedSourceID] = struct{}{}
	}
	return nil
}

func hybridInvariant(operation string) error {
	return fmt.Errorf("%s: %w", operation, domain.ErrInvariant)
}
