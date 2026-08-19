package retrieval

import (
	"context"
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
