package retrieval

import (
	"context"
	"errors"
	"fmt"
	"math"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/ksana-ai/agent-memory-system/internal/domain"
)

type hybridRetrieverFunc func(context.Context, string, string, string, int, time.Time) ([]domain.SearchHit, error)

func (function hybridRetrieverFunc) Search(
	ctx context.Context,
	tenantID, userID, query string,
	limit int,
	asOf time.Time,
) ([]domain.SearchHit, error) {
	return function(ctx, tenantID, userID, query, limit, asOf)
}

func TestHybridUsesExactReciprocalRankFusion(t *testing.T) {
	asOf := time.Date(2026, 8, 20, 12, 0, 0, 0, time.UTC)
	cards := map[string]domain.MemoryCard{}
	for _, id := range []string{"a", "b", "c", "d"} {
		cards[id] = hybridCard(id, asOf.Add(-time.Duration(id[0])*time.Minute))
	}
	lexical := hybridStatic([]domain.SearchHit{{Memory: cards["a"]}, {Memory: cards["b"]}, {Memory: cards["c"]}}, nil)
	dense := hybridStatic([]domain.SearchHit{{Memory: cards["b"]}, {Memory: cards["d"]}, {Memory: cards["a"]}}, nil)
	retriever, err := NewHybrid(lexical, dense)
	if err != nil {
		t.Fatalf("NewHybrid: %v", err)
	}

	hits, err := retriever.Search(context.Background(), "tenant", "user", "query", 4, asOf)
	if err != nil {
		t.Fatalf("Search: %v", err)
	}
	wantIDs := []string{"b", "a", "d", "c"}
	gotIDs := make([]string, len(hits))
	for index := range hits {
		gotIDs[index] = hits[index].Memory.ID
	}
	if !reflect.DeepEqual(gotIDs, wantIDs) {
		t.Fatalf("IDs = %v, want %v", gotIDs, wantIDs)
	}
	wantScores := map[string]float64{
		"a": 1.0/61 + 1.0/63,
		"b": 1.0/62 + 1.0/61,
		"c": 1.0 / 63,
		"d": 1.0 / 62,
	}
	for _, hit := range hits {
		if math.Abs(hit.Score-wantScores[hit.Memory.ID]) > 1e-15 {
			t.Errorf("score[%s] = %.18f, want %.18f", hit.Memory.ID, hit.Score, wantScores[hit.Memory.ID])
		}
	}
}

func TestHybridTieBreaksByBestRankThenCreatedAtThenID(t *testing.T) {
	asOf := time.Date(2026, 8, 20, 12, 0, 0, 0, time.UTC)

	// 1/63 + 1/105 == 1/70 + 1/90. The first item has best rank 3,
	// the second best rank 10, so recency must not move the second ahead.
	olderBestRank := hybridCard("best-rank", asOf.Add(-time.Hour))
	newerWorseRank := hybridCard("worse-rank", asOf.Add(-time.Minute))
	lexical := makeRankedHybridHits("lexical", 45, map[int]domain.MemoryCard{
		3: olderBestRank, 10: newerWorseRank,
	}, asOf)
	dense := makeRankedHybridHits("dense", 45, map[int]domain.MemoryCard{
		45: olderBestRank, 30: newerWorseRank,
	}, asOf)
	retriever, err := NewHybrid(hybridStatic(lexical, nil), hybridStatic(dense, nil))
	if err != nil {
		t.Fatalf("NewHybrid: %v", err)
	}
	hits, err := retriever.Search(context.Background(), "tenant", "user", "query", 100, asOf)
	if err != nil {
		t.Fatalf("Search: %v", err)
	}
	if hybridHitIndex(hits, "best-rank") >= hybridHitIndex(hits, "worse-rank") {
		t.Fatalf("best-rank did not win exact fused-score tie")
	}

	created := asOf.Add(-time.Hour)
	old := hybridCard("old", created.Add(-time.Second))
	newer := hybridCard("new", created)
	retriever, _ = NewHybrid(
		hybridStatic([]domain.SearchHit{{Memory: old}}, nil),
		hybridStatic([]domain.SearchHit{{Memory: newer}}, nil),
	)
	hits, err = retriever.Search(context.Background(), "tenant", "user", "query", 2, asOf)
	if err != nil || len(hits) != 2 || hits[0].Memory.ID != "new" {
		t.Fatalf("created-at tie = %#v, %v", hits, err)
	}

	cardZ := hybridCard("z", created)
	cardA := hybridCard("a", created)
	retriever, _ = NewHybrid(
		hybridStatic([]domain.SearchHit{{Memory: cardZ}}, nil),
		hybridStatic([]domain.SearchHit{{Memory: cardA}}, nil),
	)
	hits, err = retriever.Search(context.Background(), "tenant", "user", "query", 2, asOf)
	if err != nil || len(hits) != 2 || hits[0].Memory.ID != "a" {
		t.Fatalf("ID tie = %#v, %v", hits, err)
	}
}

func TestHybridRunsBranchesConcurrentlyWithSameBoundary(t *testing.T) {
	asOf := time.Date(2026, 8, 20, 12, 0, 0, 123, time.UTC)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	wantDeadline, _ := ctx.Deadline()

	var mutex sync.Mutex
	entered := 0
	release := make(chan struct{})
	observations := make([]struct {
		deadline time.Time
		asOf     time.Time
		limit    int
	}, 0, 2)
	branch := func(ctx context.Context, _, _ string, _ string, limit int, gotAsOf time.Time) ([]domain.SearchHit, error) {
		deadline, _ := ctx.Deadline()
		mutex.Lock()
		entered++
		observations = append(observations, struct {
			deadline time.Time
			asOf     time.Time
			limit    int
		}{deadline: deadline, asOf: gotAsOf, limit: limit})
		if entered == 2 {
			close(release)
		}
		mutex.Unlock()
		select {
		case <-release:
			return []domain.SearchHit{}, nil
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	retriever, err := NewHybrid(hybridRetrieverFunc(branch), hybridRetrieverFunc(branch))
	if err != nil {
		t.Fatalf("NewHybrid: %v", err)
	}
	if _, err := retriever.Search(ctx, "tenant", "user", "query", 3, asOf); err != nil {
		t.Fatalf("Search: %v", err)
	}
	if len(observations) != 2 {
		t.Fatalf("observations = %#v", observations)
	}
	for _, observation := range observations {
		if !observation.deadline.Equal(wantDeadline) || !observation.asOf.Equal(asOf) || observation.limit != 20 {
			t.Errorf("boundary = %#v, want deadline=%v asOf=%v depth=20", observation, wantDeadline, asOf)
		}
	}
}

func TestHybridRejectsEveryMalformedRawHit(t *testing.T) {
	asOf := time.Date(2026, 8, 20, 12, 0, 0, 0, time.UTC)
	valid := hybridCard("card", asOf.Add(-time.Minute))

	tests := []struct {
		name    string
		lexical []domain.SearchHit
		dense   []domain.SearchHit
	}{
		{name: "branch duplicate", lexical: []domain.SearchHit{{Memory: valid}, {Memory: valid}}},
		{name: "same ID payload drift", lexical: []domain.SearchHit{{Memory: valid}}, dense: []domain.SearchHit{{Memory: func() domain.MemoryCard { value := valid; value.Value = "changed"; return value }()}}},
		{name: "tenant scope", lexical: []domain.SearchHit{{Memory: func() domain.MemoryCard { value := valid; value.TenantID = "other"; return value }()}}},
		{name: "user scope", lexical: []domain.SearchHit{{Memory: func() domain.MemoryCard { value := valid; value.UserID = "other"; return value }()}}},
		{name: "status", lexical: []domain.SearchHit{{Memory: func() domain.MemoryCard { value := valid; value.Status = domain.MemorySuperseded; return value }()}}},
		{name: "expiry equality", lexical: []domain.SearchHit{{Memory: func() domain.MemoryCard { value := valid; value.ExpiresAt = &asOf; return value }()}}},
		{name: "expiry past", lexical: []domain.SearchHit{{Memory: func() domain.MemoryCard {
			value := valid
			expired := asOf.Add(-time.Nanosecond)
			value.ExpiresAt = &expired
			return value
		}()}}},
		{name: "source missing", lexical: []domain.SearchHit{{Memory: func() domain.MemoryCard { value := valid; value.SourceEventIDs = nil; return value }()}}},
		{name: "source blank", lexical: []domain.SearchHit{{Memory: func() domain.MemoryCard { value := valid; value.SourceEventIDs = []string{"  "}; return value }()}}},
		{name: "source duplicate", lexical: []domain.SearchHit{{Memory: func() domain.MemoryCard {
			value := valid
			value.SourceEventIDs = []string{"source", " source "}
			return value
		}()}}},
		{name: "empty ID", lexical: []domain.SearchHit{{Memory: func() domain.MemoryCard { value := valid; value.ID = ""; return value }()}}},
		{name: "over limit", lexical: makeHybridHits(21, asOf)},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			retriever, err := NewHybrid(hybridStatic(test.lexical, nil), hybridStatic(test.dense, nil))
			if err != nil {
				t.Fatalf("NewHybrid: %v", err)
			}
			_, err = retriever.Search(context.Background(), "tenant", "user", "query", 1, asOf)
			if !errors.Is(err, domain.ErrInvariant) {
				t.Fatalf("error = %v, want invariant", err)
			}
		})
	}
}

func TestHybridStrictlyPropagatesEitherBranchFailure(t *testing.T) {
	asOf := time.Date(2026, 8, 20, 12, 0, 0, 0, time.UTC)
	sentinel := errors.New("branch unavailable")
	valid := []domain.SearchHit{{Memory: hybridCard("valid", asOf)}}
	for _, test := range []struct {
		name       string
		lexicalErr error
		denseErr   error
	}{
		{name: "lexical", lexicalErr: sentinel},
		{name: "dense", denseErr: sentinel},
	} {
		t.Run(test.name, func(t *testing.T) {
			retriever, _ := NewHybrid(hybridStatic(valid, test.lexicalErr), hybridStatic(valid, test.denseErr))
			hits, err := retriever.Search(context.Background(), "tenant", "user", "query", 1, asOf)
			if !errors.Is(err, sentinel) || hits != nil {
				t.Fatalf("hits=%#v error=%v", hits, err)
			}
		})
	}
}

func TestHybridCandidateDepthBoundsAndNoWork(t *testing.T) {
	for _, test := range []struct {
		limit int
		want  int
	}{{limit: 1, want: 20}, {limit: 5, want: 20}, {limit: 6, want: 24}, {limit: 25, want: 100}, {limit: 26, want: 100}, {limit: math.MaxInt, want: 100}} {
		if got := hybridCandidateDepth(test.limit); got != test.want {
			t.Errorf("depth(%d)=%d, want %d", test.limit, got, test.want)
		}
	}
	called := 0
	branch := hybridRetrieverFunc(func(context.Context, string, string, string, int, time.Time) ([]domain.SearchHit, error) {
		called++
		return nil, nil
	})
	retriever, _ := NewHybrid(branch, branch)
	for _, input := range []struct {
		query string
		limit int
	}{{query: "query", limit: 0}, {query: " ", limit: 1}} {
		hits, err := retriever.Search(context.Background(), "tenant", "user", input.query, input.limit, time.Now())
		if err != nil || len(hits) != 0 {
			t.Fatalf("Search = %#v, %v", hits, err)
		}
	}
	if called != 0 {
		t.Fatalf("no-work branch calls = %d", called)
	}
}

func TestNewHybridRequiresBothBranches(t *testing.T) {
	branch := hybridStatic(nil, nil)
	if _, err := NewHybrid(nil, branch); !errors.Is(err, domain.ErrInvalid) {
		t.Fatalf("nil lexical error = %v", err)
	}
	if _, err := NewHybrid(branch, nil); !errors.Is(err, domain.ErrInvalid) {
		t.Fatalf("nil dense error = %v", err)
	}
}

func hybridStatic(hits []domain.SearchHit, err error) hybridRetrieverFunc {
	return func(context.Context, string, string, string, int, time.Time) ([]domain.SearchHit, error) {
		return append([]domain.SearchHit(nil), hits...), err
	}
}

func hybridCard(id string, createdAt time.Time) domain.MemoryCard {
	return domain.MemoryCard{
		ID:             id,
		TenantID:       "tenant",
		UserID:         "user",
		Value:          "value-" + id,
		Status:         domain.MemoryActive,
		SourceEventIDs: []string{"source-" + id},
		CreatedAt:      createdAt,
	}
}

func makeHybridHits(count int, asOf time.Time) []domain.SearchHit {
	result := make([]domain.SearchHit, count)
	for index := range result {
		result[index] = domain.SearchHit{Memory: hybridCard(fmt.Sprintf("card-%03d", index), asOf.Add(-time.Duration(index)*time.Second))}
	}
	return result
}

func makeRankedHybridHits(prefix string, count int, replacements map[int]domain.MemoryCard, asOf time.Time) []domain.SearchHit {
	result := make([]domain.SearchHit, count)
	for rank := 1; rank <= count; rank++ {
		memory, exists := replacements[rank]
		if !exists {
			memory = hybridCard(fmt.Sprintf("%s-%03d", prefix, rank), asOf.Add(-time.Duration(rank)*time.Second))
		}
		result[rank-1] = domain.SearchHit{Memory: memory}
	}
	return result
}

func hybridHitIndex(hits []domain.SearchHit, id string) int {
	for index := range hits {
		if hits[index].Memory.ID == id {
			return index
		}
	}
	return len(hits)
}
