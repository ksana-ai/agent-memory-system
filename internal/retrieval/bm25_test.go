package retrieval

import (
	"context"
	"slices"
	"testing"
	"time"

	"github.com/kai443/go-agent-memory-system/internal/domain"
)

type staticMemorySource []domain.MemoryCard

func (source staticMemorySource) ListActiveMemories(context.Context, string, string) ([]domain.MemoryCard, error) {
	return append([]domain.MemoryCard(nil), source...), nil
}

func TestBM25RanksMatchingCardFirst(t *testing.T) {
	now := time.Date(2026, 8, 19, 0, 0, 0, 0, time.UTC)
	retriever, err := NewBM25(staticMemorySource{
		{ID: "one", Key: "seat_preference", Value: "window seat", Category: "travel", Status: domain.MemoryActive, CreatedAt: now},
		{ID: "two", Key: "meal_preference", Value: "vegetarian meal", Category: "travel", Status: domain.MemoryActive, CreatedAt: now},
	})
	if err != nil {
		t.Fatalf("new BM25: %v", err)
	}
	hits, err := retriever.Search(context.Background(), "tenant", "user", "preferred vegetarian meal", 2)
	if err != nil {
		t.Fatalf("search: %v", err)
	}
	if len(hits) != 1 || hits[0].Memory.ID != "two" {
		t.Fatalf("unexpected hits: %#v", hits)
	}
}

func TestTokenizeIncludesChineseBigrams(t *testing.T) {
	tokens := tokenize("偏好无糖拿铁")
	wanted := map[string]bool{"偏好": false, "无糖": false, "拿铁": false}
	for _, token := range tokens {
		if _, ok := wanted[token]; ok {
			wanted[token] = true
		}
	}
	for token, found := range wanted {
		if !found {
			t.Errorf("tokenize did not emit %q: %#v", token, tokens)
		}
	}
}

func TestBM25SearchIsDeterministicAcrossRuns(t *testing.T) {
	now := time.Date(2026, 8, 19, 0, 0, 0, 0, time.UTC)
	retriever, err := NewBM25(staticMemorySource{
		{ID: "card-b", Key: "alpha_beta", Value: "alpha beta", Status: domain.MemoryActive, CreatedAt: now},
		{ID: "card-a", Key: "beta_alpha", Value: "beta alpha", Status: domain.MemoryActive, CreatedAt: now},
		{ID: "card-c", Key: "gamma", Value: "gamma", Status: domain.MemoryActive, CreatedAt: now},
	})
	if err != nil {
		t.Fatalf("new BM25: %v", err)
	}

	var baselineIDs []string
	var baselineScores []float64
	for run := 0; run < 100; run++ {
		hits, err := retriever.Search(context.Background(), "tenant", "user", "alpha beta", 3)
		if err != nil {
			t.Fatalf("search run %d: %v", run, err)
		}
		ids := make([]string, 0, len(hits))
		scores := make([]float64, 0, len(hits))
		for _, hit := range hits {
			ids = append(ids, hit.Memory.ID)
			scores = append(scores, hit.Score)
		}
		if run == 0 {
			baselineIDs = ids
			baselineScores = scores
			continue
		}
		if !slices.Equal(ids, baselineIDs) || !slices.Equal(scores, baselineScores) {
			t.Fatalf("run %d changed ranking: ids=%v scores=%v, want ids=%v scores=%v", run, ids, scores, baselineIDs, baselineScores)
		}
	}
}
