//go:build integration && vector

package embedding

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"
)

func TestClientProbeFingerprintStableAcrossBatchShapes(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	endpoint := os.Getenv("LMSTUDIO_EMBEDDINGS_URL")
	model := os.Getenv("LMSTUDIO_EMBEDDING_MODEL")
	if endpoint == "" || model == "" {
		t.Fatal("LMSTUDIO_EMBEDDINGS_URL and LMSTUDIO_EMBEDDING_MODEL are required")
	}
	client, err := NewClient(Config{
		Endpoint:          endpoint,
		Model:             model,
		ExpectedDimension: DefaultDimension,
		Timeout:           DefaultTimeout,
		MaxResponseBytes:  DefaultMaxResponseBytes,
		MaxBatchSize:      2,
		MaxInputBytes:     DefaultMaxInputBytes,
	})
	if err != nil {
		t.Fatalf("new embedding client: %v", err)
	}

	baseline, err := client.Embed(ctx, []string{ProbeTextV1})
	if err != nil {
		t.Fatalf("embed single probe baseline: %v", err)
	}
	if len(baseline) != 1 {
		t.Fatalf("single probe result count=%d, want 1", len(baseline))
	}
	baselineHash := VectorSHA256(baseline[0])

	documents := []struct {
		name string
		text string
	}{
		{name: "short", text: "x"},
		{
			name: "multilingual-medium",
			text: "这是公开的批处理形状验证文本，用于检查中英文混合输入。 " +
				"This synthetic sentence contains no user memory and verifies a medium token shape.",
		},
		{
			name: "long",
			text: strings.Repeat("public batch shape validation token ", 512),
		},
	}
	const rounds = 2
	observations := 0
	for _, document := range documents {
		for round := 1; round <= rounds; round++ {
			vectors, embedErr := client.Embed(ctx, []string{ProbeTextV1, document.text})
			if embedErr != nil {
				t.Fatalf("embed paired probe shape=%s round=%d: %v", document.name, round, embedErr)
			}
			if len(vectors) != 2 {
				t.Fatalf("paired result count shape=%s round=%d is %d, want 2", document.name, round, len(vectors))
			}
			observedHash := VectorSHA256(vectors[0])
			if observedHash != baselineHash {
				t.Fatalf(
					"probe fingerprint drift shape=%s round=%d baseline=%s observed=%s",
					document.name,
					round,
					baselineHash,
					observedHash,
				)
			}
			observations++
		}
	}
	t.Logf(
		"probe_vector_sha256=%s paired_observations=%d document_shapes=%d rounds_per_shape=%d",
		baselineHash,
		observations,
		len(documents),
		rounds,
	)
}
