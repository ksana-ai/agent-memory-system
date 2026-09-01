package postgres

import (
	"context"
	"testing"
	"time"

	"github.com/ksana-ai/agent-memory-system/internal/domain"
)

func TestSearchEmptyInputDoesNotRequireDatabase(t *testing.T) {
	storage := &Store{}
	for _, input := range []struct {
		query string
		limit int
	}{
		{query: "", limit: 5},
		{query: "   ", limit: 5},
		{query: "memory", limit: 0},
		{query: "memory", limit: -1},
	} {
		hits, err := storage.Search(context.Background(), "tenant", "user", input.query, input.limit, time.Now())
		if err != nil || len(hits) != 0 {
			t.Fatalf("Search(query=%q, limit=%d) hits=%#v error=%v, want empty success", input.query, input.limit, hits, err)
		}
	}
}

func TestEncodeIdentityUsesDomainNormalization(t *testing.T) {
	first := domain.MemoryCard{
		Kind:         domain.MemoryKind("Semantic"),
		Category:     " Travel ",
		Key:          "Seat_Preference",
		Person:       " SELF ",
		Relationship: "Self",
	}.Identity()
	second := domain.MemoryCard{
		Kind:         domain.MemoryKindSemantic,
		Category:     "travel",
		Key:          "seat_preference",
		Person:       "self",
		Relationship: "self",
	}.Identity()

	if first != second {
		t.Fatalf("domain identities differ: first=%#v second=%#v", first, second)
	}
	if encodeIdentity(first) != encodeIdentity(second) {
		t.Fatal("equivalent normalized identities produced different keys")
	}
}

func TestEncodeIdentityKeepsFieldBoundaries(t *testing.T) {
	first := domain.MemoryIdentity{Kind: domain.MemoryKindSemantic, Category: "ab", Key: "c"}
	second := domain.MemoryIdentity{Kind: domain.MemoryKindSemantic, Category: "a", Key: "bc"}
	if encodeIdentity(first) == encodeIdentity(second) {
		t.Fatal("distinct identities produced the same key")
	}
}

func TestNormalizedProjectionTimeClampsAndTruncates(t *testing.T) {
	candidateTime := time.Date(2026, 8, 19, 12, 0, 0, 123456789, time.FixedZone("test", 8*60*60))
	reviewTime := candidateTime.Add(-time.Hour)

	got := normalizedProjectionTime(reviewTime, candidateTime)
	want := candidateTime.UTC().Truncate(time.Microsecond)
	if !got.Equal(want) {
		t.Fatalf("normalized projection time = %s, want %s", got, want)
	}
	if got.Location() != time.UTC {
		t.Fatalf("normalized projection location = %s, want UTC", got.Location())
	}
}

func TestFirstDuplicate(t *testing.T) {
	if got := firstDuplicate([]string{"one", "two", "one"}); got != "one" {
		t.Fatalf("firstDuplicate() = %q, want one", got)
	}
	if got := firstDuplicate([]string{"one", "two"}); got != "" {
		t.Fatalf("firstDuplicate() = %q, want empty", got)
	}
}
