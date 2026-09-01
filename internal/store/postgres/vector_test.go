package postgres

import (
	"errors"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/ksana-ai/agent-memory-system/internal/domain"
)

func TestValidateMemoryEmbeddingCanonicalizesMetadata(t *testing.T) {
	value := validMemoryEmbeddingForUnitTest()
	value.ContentSHA256 = strings.ToUpper(value.ContentSHA256)
	value.CreatedAt = time.Date(2026, time.August, 19, 8, 0, 0, 123456789, time.FixedZone("test", 8*60*60))

	normalized, encoded, err := validateMemoryEmbedding(value)
	if err != nil {
		t.Fatalf("validate embedding: %v", err)
	}
	if normalized.ContentSHA256 != strings.ToLower(value.ContentSHA256) {
		t.Fatalf("content hash=%q, want lowercase", normalized.ContentSHA256)
	}
	if normalized.CreatedAt.Location() != time.UTC || normalized.CreatedAt.Nanosecond()%1000 != 0 {
		t.Fatalf("created_at=%s, want canonical UTC microseconds", normalized.CreatedAt.Format(time.RFC3339Nano))
	}
	if !strings.HasPrefix(encoded, "[1,") || !strings.HasSuffix(encoded, "]") {
		t.Fatalf("encoded vector has unexpected form: %.40q", encoded)
	}
}

func TestValidateMemoryEmbeddingRejectsInvalidValues(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*MemoryEmbedding)
	}{
		{name: "blank scope", mutate: func(value *MemoryEmbedding) { value.TenantID = "  " }},
		{name: "blank space", mutate: func(value *MemoryEmbedding) { value.EmbeddingSpace = "" }},
		{name: "blank provider", mutate: func(value *MemoryEmbedding) { value.Provider = "" }},
		{name: "blank model", mutate: func(value *MemoryEmbedding) { value.Model = "" }},
		{name: "blank document version", mutate: func(value *MemoryEmbedding) { value.DocumentVersion = "" }},
		{name: "blank query version", mutate: func(value *MemoryEmbedding) { value.QueryVersion = "" }},
		{name: "bad model fingerprint", mutate: func(value *MemoryEmbedding) { value.ModelFingerprint = "not-a-sha" }},
		{name: "bad hash length", mutate: func(value *MemoryEmbedding) { value.ContentSHA256 = "ab" }},
		{name: "bad hash alphabet", mutate: func(value *MemoryEmbedding) { value.ContentSHA256 = strings.Repeat("z", 64) }},
		{name: "wrong dimension", mutate: func(value *MemoryEmbedding) { value.Vector = value.Vector[:VectorDimension-1] }},
		{name: "zero vector", mutate: func(value *MemoryEmbedding) { value.Vector = make([]float32, VectorDimension) }},
		{name: "nan vector", mutate: func(value *MemoryEmbedding) { value.Vector[3] = float32(math.NaN()) }},
		{name: "positive infinity", mutate: func(value *MemoryEmbedding) { value.Vector[4] = float32(math.Inf(1)) }},
		{name: "missing created at", mutate: func(value *MemoryEmbedding) { value.CreatedAt = time.Time{} }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			value := validMemoryEmbeddingForUnitTest()
			test.mutate(&value)
			if _, _, err := validateMemoryEmbedding(value); !errors.Is(err, domain.ErrInvalid) {
				t.Fatalf("validation error=%v, want domain.ErrInvalid", err)
			}
		})
	}
}

func validMemoryEmbeddingForUnitTest() MemoryEmbedding {
	vector := make([]float32, VectorDimension)
	vector[0] = 1
	return MemoryEmbedding{
		TenantID:         "tenant-unit",
		UserID:           "user-unit",
		MemoryID:         "memory-unit",
		EmbeddingSpace:   "lmstudio:text-embedding-bge-m3:memory-card-v1",
		Provider:         "lmstudio",
		Model:            "text-embedding-bge-m3",
		DocumentVersion:  "memory-card-v1",
		QueryVersion:     "query-v1",
		ModelFingerprint: strings.Repeat("b", 64),
		ContentSHA256:    strings.Repeat("a", 64),
		Vector:           vector,
		CreatedAt:        time.Date(2026, time.August, 19, 8, 0, 0, 0, time.UTC),
	}
}
