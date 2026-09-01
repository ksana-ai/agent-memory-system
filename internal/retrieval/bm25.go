package retrieval

import (
	"context"
	"fmt"
	"math"
	"sort"
	"strings"
	"time"
	"unicode"

	"github.com/ksana-ai/agent-memory-system/internal/domain"
)

const (
	bm25K1 = 1.2
	bm25B  = 0.75
)

type MemorySource interface {
	ListServiceableMemories(context.Context, string, string, time.Time) ([]domain.MemoryCard, error)
}

// BM25 is a deterministic lexical baseline. It deliberately has no embedding
// fallback: later retrieval adapters can be compared against this exact arm.
type BM25 struct {
	source MemorySource
}

func NewBM25(source MemorySource) (*BM25, error) {
	if source == nil {
		return nil, fmt.Errorf("memory source is required")
	}
	return &BM25{source: source}, nil
}

func (retriever *BM25) Search(ctx context.Context, tenantID, userID, query string, limit int, asOf time.Time) ([]domain.SearchHit, error) {
	queryTokens := tokenize(query)
	if len(queryTokens) == 0 || limit <= 0 {
		return []domain.SearchHit{}, nil
	}

	memories, err := retriever.source.ListServiceableMemories(ctx, tenantID, userID, asOf)
	if err != nil {
		return nil, err
	}
	if len(memories) == 0 {
		return []domain.SearchHit{}, nil
	}

	type document struct {
		memory domain.MemoryCard
		terms  map[string]int
		length int
	}
	documents := make([]document, 0, len(memories))
	documentFrequency := make(map[string]int)
	totalLength := 0
	for _, memory := range memories {
		tokens := memoryTokens(memory)
		termFrequency := make(map[string]int, len(tokens))
		for _, token := range tokens {
			termFrequency[token]++
		}
		for token := range termFrequency {
			documentFrequency[token]++
		}
		documents = append(documents, document{memory: memory, terms: termFrequency, length: len(tokens)})
		totalLength += len(tokens)
	}

	averageLength := float64(totalLength) / float64(len(documents))
	queryFrequency := make(map[string]int, len(queryTokens))
	for _, token := range queryTokens {
		queryFrequency[token]++
	}
	queryTerms := make([]string, 0, len(queryFrequency))
	for token := range queryFrequency {
		queryTerms = append(queryTerms, token)
	}
	sort.Strings(queryTerms)

	hits := make([]domain.SearchHit, 0, len(documents))
	for _, document := range documents {
		score := 0.0
		for _, token := range queryTerms {
			queryCount := queryFrequency[token]
			termCount := document.terms[token]
			if termCount == 0 {
				continue
			}
			df := documentFrequency[token]
			idf := math.Log(1 + (float64(len(documents)-df)+0.5)/(float64(df)+0.5))
			normalization := bm25K1 * (1 - bm25B + bm25B*float64(document.length)/averageLength)
			score += float64(queryCount) * idf * (float64(termCount) * (bm25K1 + 1) / (float64(termCount) + normalization))
		}
		if score > 0 {
			hits = append(hits, domain.SearchHit{Memory: document.memory, Score: score})
		}
	}

	sort.Slice(hits, func(i, j int) bool {
		if hits[i].Score != hits[j].Score {
			return hits[i].Score > hits[j].Score
		}
		if !hits[i].Memory.CreatedAt.Equal(hits[j].Memory.CreatedAt) {
			return hits[i].Memory.CreatedAt.After(hits[j].Memory.CreatedAt)
		}
		return hits[i].Memory.ID < hits[j].Memory.ID
	})
	if len(hits) > limit {
		hits = hits[:limit]
	}
	return hits, nil
}

func memoryTokens(memory domain.MemoryCard) []string {
	var tokens []string
	appendWeighted := func(value string, weight int) {
		fieldTokens := tokenize(value)
		for range weight {
			tokens = append(tokens, fieldTokens...)
		}
	}

	appendWeighted(memory.Key, 3)
	appendWeighted(memory.Value, 2)
	appendWeighted(memory.Category, 1)
	appendWeighted(memory.Person, 1)
	appendWeighted(memory.Relationship, 1)
	appendWeighted(memory.Backstory, 1)
	return tokens
}

func tokenize(input string) []string {
	input = strings.ToLower(input)
	tokens := make([]string, 0)
	word := make([]rune, 0)
	var previousHan rune

	flushWord := func() {
		if len(word) > 0 {
			tokens = append(tokens, normalizeWord(string(word)))
			word = word[:0]
		}
	}
	for _, current := range input {
		if unicode.Is(unicode.Han, current) {
			flushWord()
			tokens = append(tokens, string(current))
			if previousHan != 0 {
				tokens = append(tokens, string([]rune{previousHan, current}))
			}
			previousHan = current
			continue
		}
		previousHan = 0
		if unicode.IsLetter(current) || unicode.IsDigit(current) {
			word = append(word, current)
			continue
		}
		flushWord()
	}
	flushWord()
	return tokens
}

func normalizeWord(word string) string {
	if len(word) > 4 && strings.HasSuffix(word, "s") && !strings.HasSuffix(word, "ss") && !strings.HasSuffix(word, "us") {
		return strings.TrimSuffix(word, "s")
	}
	return word
}
