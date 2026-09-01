package retrieval

import (
	"context"
	"errors"
	"fmt"
	"math"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/ksana-ai/agent-memory-system/internal/domain"
	"github.com/ksana-ai/agent-memory-system/internal/embedding"
	"github.com/ksana-ai/agent-memory-system/internal/store/postgres"
)

type denseTestEmbedder struct {
	descriptor embedding.Descriptor
	vectors    [][]float32
	err        error
	inputs     [][]string
}

func (embedder *denseTestEmbedder) Descriptor() embedding.Descriptor {
	return embedder.descriptor
}

func (embedder *denseTestEmbedder) Embed(_ context.Context, inputs []string) ([][]float32, error) {
	embedder.inputs = append(embedder.inputs, append([]string(nil), inputs...))
	if embedder.err != nil {
		return nil, embedder.err
	}
	result := make([][]float32, len(embedder.vectors))
	for index := range embedder.vectors {
		result[index] = append([]float32(nil), embedder.vectors[index]...)
	}
	return result, nil
}

type denseSearchCall struct {
	tenantID    string
	userID      string
	expectation postgres.ServingVectorExpectation
	query       []float32
	limit       int
	asOf        time.Time
}

type denseTestBackend struct {
	states       []postgres.ServingProjectionState
	currentErr   error
	currentCalls int
	searchErr    error
	hits         []domain.SearchHit
	searchCalls  []denseSearchCall
}

func (backend *denseTestBackend) CurrentServingProjection(context.Context) (postgres.ServingProjectionState, error) {
	backend.currentCalls++
	if backend.currentErr != nil {
		return postgres.ServingProjectionState{}, backend.currentErr
	}
	if len(backend.states) == 0 {
		return postgres.ServingProjectionState{}, nil
	}
	index := backend.currentCalls - 1
	if index >= len(backend.states) {
		index = len(backend.states) - 1
	}
	return backend.states[index], nil
}

func (backend *denseTestBackend) SearchServingVector(
	_ context.Context,
	tenantID, userID string,
	expectation postgres.ServingVectorExpectation,
	query []float32,
	limit int,
	asOf time.Time,
) ([]domain.SearchHit, error) {
	backend.searchCalls = append(backend.searchCalls, denseSearchCall{
		tenantID: tenantID, userID: userID, expectation: expectation,
		query: append([]float32(nil), query...), limit: limit, asOf: asOf,
	})
	if backend.searchErr != nil {
		return nil, backend.searchErr
	}
	return append([]domain.SearchHit(nil), backend.hits...), nil
}

func TestDenseSearchPinsProviderAndServingGeneration(t *testing.T) {
	descriptor := denseDescriptor()
	probe := denseVector(1)
	queryVector := denseVector(2)
	space := denseSpace(t, descriptor, probe)
	target := denseTarget(descriptor, probe, space)
	asOf := time.Date(2026, 8, 20, 10, 0, 0, 0, time.UTC)
	wantHits := []domain.SearchHit{{Memory: domain.MemoryCard{ID: "memory"}, Score: 0.75}}
	backend := &denseTestBackend{
		states: []postgres.ServingProjectionState{{Target: &target, Generation: 7}},
		hits:   wantHits,
	}
	embedder := &denseTestEmbedder{descriptor: descriptor, vectors: [][]float32{probe, queryVector}}
	retriever, err := NewDense(backend, embedder, space)
	if err != nil {
		t.Fatalf("NewDense: %v", err)
	}

	hits, err := retriever.Search(context.Background(), "tenant", "user", "  private query  ", 9, asOf)
	if err != nil {
		t.Fatalf("Search: %v", err)
	}
	if !reflect.DeepEqual(hits, wantHits) {
		t.Fatalf("hits = %#v, want %#v", hits, wantHits)
	}
	if len(embedder.inputs) != 1 || !reflect.DeepEqual(embedder.inputs[0], []string{embedding.ProbeTextV1, "private query"}) {
		t.Fatalf("embedding inputs = %#v", embedder.inputs)
	}
	if backend.currentCalls != 1 || len(backend.searchCalls) != 1 {
		t.Fatalf("backend calls current=%d search=%d", backend.currentCalls, len(backend.searchCalls))
	}
	call := backend.searchCalls[0]
	if call.tenantID != "tenant" || call.userID != "user" ||
		call.expectation != (postgres.ServingVectorExpectation{EmbeddingSpace: space, Generation: 7}) ||
		call.limit != 9 || !call.asOf.Equal(asOf) || !reflect.DeepEqual(call.query, queryVector) {
		t.Fatalf("search call = %#v", call)
	}
}

func TestDenseSearchFailsClosedOnStaticTargetAndDescriptorDrift(t *testing.T) {
	descriptor := denseDescriptor()
	probe := denseVector(1)
	space := denseSpace(t, descriptor, probe)
	baseline := denseTarget(descriptor, probe, space)

	tests := []struct {
		name   string
		mutate func(*postgres.ServingProjectionState, *denseTestEmbedder)
	}{
		{name: "missing target", mutate: func(state *postgres.ServingProjectionState, _ *denseTestEmbedder) { state.Target = nil }},
		{name: "shadow target", mutate: func(state *postgres.ServingProjectionState, _ *denseTestEmbedder) {
			state.Target.State = postgres.ProjectionTargetShadow
		}},
		{name: "enqueue disabled", mutate: func(state *postgres.ServingProjectionState, _ *denseTestEmbedder) { state.Target.EnqueueNew = false }},
		{name: "space pin", mutate: func(state *postgres.ServingProjectionState, _ *denseTestEmbedder) {
			state.Target.Space.ID = "other-space"
		}},
		{name: "provider metadata", mutate: func(state *postgres.ServingProjectionState, _ *denseTestEmbedder) {
			state.Target.Space.Provider = "other-provider"
		}},
		{name: "model metadata", mutate: func(state *postgres.ServingProjectionState, _ *denseTestEmbedder) {
			state.Target.Space.Model = "other-model"
		}},
		{name: "dimension metadata", mutate: func(state *postgres.ServingProjectionState, _ *denseTestEmbedder) { state.Target.Space.Dimension-- }},
		{name: "document metadata", mutate: func(state *postgres.ServingProjectionState, _ *denseTestEmbedder) {
			state.Target.Space.DocumentVersion = "other-document"
		}},
		{name: "query metadata", mutate: func(state *postgres.ServingProjectionState, _ *denseTestEmbedder) {
			state.Target.Space.QueryVersion = "other-query"
		}},
		{name: "negative generation", mutate: func(state *postgres.ServingProjectionState, _ *denseTestEmbedder) { state.Generation = -1 }},
		{name: "descriptor model rotation", mutate: func(_ *postgres.ServingProjectionState, embedder *denseTestEmbedder) {
			embedder.descriptor.Model = "rotated-model"
		}},
		{name: "descriptor dimension rotation", mutate: func(_ *postgres.ServingProjectionState, embedder *denseTestEmbedder) {
			embedder.descriptor.Dimension--
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			target := baseline
			state := postgres.ServingProjectionState{Target: &target, Generation: 4}
			embedder := &denseTestEmbedder{descriptor: descriptor, vectors: [][]float32{probe, denseVector(2)}}
			backend := &denseTestBackend{states: []postgres.ServingProjectionState{state}}
			retriever, err := NewDense(backend, embedder, space)
			if err != nil {
				t.Fatalf("NewDense: %v", err)
			}
			test.mutate(&backend.states[0], embedder)
			_, err = retriever.Search(context.Background(), "tenant", "user", "query", 1, time.Now())
			if !errors.Is(err, domain.ErrUnavailable) {
				t.Fatalf("error = %v, want unavailable", err)
			}
			if len(embedder.inputs) != 0 {
				t.Fatalf("provider called for static drift: %#v", embedder.inputs)
			}
			if len(backend.searchCalls) != 0 {
				t.Fatal("vector search called for static drift")
			}
		})
	}
}

func TestDenseSearchRejectsProbeAndVectorShapeDrift(t *testing.T) {
	descriptor := denseDescriptor()
	probe := denseVector(1)
	space := denseSpace(t, descriptor, probe)
	target := denseTarget(descriptor, probe, space)
	tests := []struct {
		name    string
		vectors [][]float32
	}{
		{name: "probe behavior", vectors: [][]float32{denseVector(9), denseVector(2)}},
		{name: "count", vectors: [][]float32{probe}},
		{name: "probe dimension", vectors: [][]float32{probe[:len(probe)-1], denseVector(2)}},
		{name: "query dimension", vectors: [][]float32{probe, denseVector(2)[:postgres.VectorDimension-1]}},
		{name: "non finite", vectors: [][]float32{probe, func() []float32 { value := denseVector(2); value[5] = float32(math.NaN()); return value }()}},
		{name: "zero", vectors: [][]float32{probe, make([]float32, postgres.VectorDimension)}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			backend := &denseTestBackend{states: []postgres.ServingProjectionState{{Target: &target, Generation: 2}}}
			embedder := &denseTestEmbedder{descriptor: descriptor, vectors: test.vectors}
			retriever, err := NewDense(backend, embedder, space)
			if err != nil {
				t.Fatalf("NewDense: %v", err)
			}
			_, err = retriever.Search(context.Background(), "tenant", "user", "query", 1, time.Now())
			if !errors.Is(err, domain.ErrUnavailable) || len(backend.searchCalls) != 0 {
				t.Fatalf("error=%v search calls=%d", err, len(backend.searchCalls))
			}
		})
	}
}

func TestDenseErrorsAreRedactedAndInvariantIsPreserved(t *testing.T) {
	descriptor := denseDescriptor()
	probe := denseVector(1)
	space := denseSpace(t, descriptor, probe)
	target := denseTarget(descriptor, probe, space)

	for _, test := range []struct {
		name       string
		currentErr error
		embedErr   error
		searchErr  error
		want       error
		unwanted   error
		secret     string
	}{
		{name: "provider", embedErr: errors.New("http://secret-host body=private-query"), want: domain.ErrUnavailable, secret: "secret-host"},
		{name: "provider classified not found", embedErr: fmt.Errorf("private provider body: %w", domain.ErrNotFound), want: domain.ErrUnavailable, secret: "private provider"},
		{name: "current not found", currentErr: fmt.Errorf("private target: %w", domain.ErrNotFound), want: domain.ErrUnavailable, secret: "private target"},
		{name: "current conflict", currentErr: fmt.Errorf("private generation: %w", domain.ErrConflict), want: domain.ErrUnavailable, secret: "private generation"},
		{name: "current network", currentErr: errors.New("tcp secret-current-host"), want: domain.ErrUnavailable, secret: "secret-current-host"},
		{name: "generation rotation", searchErr: fmt.Errorf("postgres://user:password@host: %w", domain.ErrUnavailable), want: domain.ErrUnavailable, secret: "password"},
		{name: "search not found", searchErr: fmt.Errorf("private row: %w", domain.ErrNotFound), want: domain.ErrUnavailable, secret: "private row"},
		{name: "search conflict", searchErr: fmt.Errorf("private fence: %w", domain.ErrConflict), want: domain.ErrUnavailable, secret: "private fence"},
		{name: "search network", searchErr: errors.New("tcp secret-search-host"), want: domain.ErrUnavailable, secret: "secret-search-host"},
		{name: "backend invariant", searchErr: fmt.Errorf("private row: %w", domain.ErrInvariant), want: domain.ErrInvariant, unwanted: domain.ErrUnavailable, secret: "private row"},
	} {
		t.Run(test.name, func(t *testing.T) {
			backend := &denseTestBackend{
				states:     []postgres.ServingProjectionState{{Target: &target, Generation: 2}},
				currentErr: test.currentErr,
				searchErr:  test.searchErr,
			}
			embedder := &denseTestEmbedder{descriptor: descriptor, vectors: [][]float32{probe, denseVector(2)}, err: test.embedErr}
			retriever, err := NewDense(backend, embedder, space)
			if err != nil {
				t.Fatalf("NewDense: %v", err)
			}
			_, err = retriever.Search(context.Background(), "tenant", "user", "query", 1, time.Now())
			if !errors.Is(err, test.want) || (test.unwanted != nil && errors.Is(err, test.unwanted)) {
				t.Fatalf("error = %v", err)
			}
			if strings.Contains(err.Error(), test.secret) || strings.Contains(err.Error(), "private-query") || strings.Contains(err.Error(), "postgres://") {
				t.Fatalf("error leaked sensitive detail: %q", err)
			}
		})
	}
}

func TestDenseReadyUsesOnlyPublicProbeAndDetectsGenerationRotation(t *testing.T) {
	descriptor := denseDescriptor()
	probe := denseVector(1)
	space := denseSpace(t, descriptor, probe)
	target := denseTarget(descriptor, probe, space)

	for _, test := range []struct {
		name    string
		states  []postgres.ServingProjectionState
		second  []float32
		wantErr bool
	}{
		{
			name: "ready",
			states: []postgres.ServingProjectionState{
				{Target: &target, Generation: 8}, {Target: &target, Generation: 8},
			},
			second: probe,
		},
		{
			name: "provider nondeterminism",
			states: []postgres.ServingProjectionState{
				{Target: &target, Generation: 8},
			},
			second: denseVector(3), wantErr: true,
		},
		{
			name: "generation rotation",
			states: []postgres.ServingProjectionState{
				{Target: &target, Generation: 8}, {Target: &target, Generation: 9},
			},
			second: probe, wantErr: true,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			backend := &denseTestBackend{states: test.states}
			embedder := &denseTestEmbedder{descriptor: descriptor, vectors: [][]float32{probe, test.second}}
			retriever, err := NewDense(backend, embedder, space)
			if err != nil {
				t.Fatalf("NewDense: %v", err)
			}
			err = retriever.Ready(context.Background())
			if test.wantErr != errors.Is(err, domain.ErrUnavailable) {
				t.Fatalf("Ready error = %v", err)
			}
			if len(embedder.inputs) != 1 || !reflect.DeepEqual(embedder.inputs[0], []string{embedding.ProbeTextV1, embedding.ProbeTextV1}) {
				t.Fatalf("readiness inputs = %#v", embedder.inputs)
			}
			if len(backend.searchCalls) != 0 {
				t.Fatal("readiness performed a tenant vector search")
			}
		})
	}
}

func TestNewDenseValidatesDependenciesAndDescriptor(t *testing.T) {
	descriptor := denseDescriptor()
	backend := &denseTestBackend{}
	embedder := &denseTestEmbedder{descriptor: descriptor}
	if _, err := NewDense(nil, embedder, "space"); !errors.Is(err, domain.ErrInvalid) {
		t.Fatalf("nil backend error = %v", err)
	}
	if _, err := NewDense(backend, nil, "space"); !errors.Is(err, domain.ErrInvalid) {
		t.Fatalf("nil embedder error = %v", err)
	}
	if _, err := NewDense(backend, embedder, " "); !errors.Is(err, domain.ErrInvalid) {
		t.Fatalf("empty space error = %v", err)
	}
	embedder.descriptor.Dimension--
	if _, err := NewDense(backend, embedder, "space"); !errors.Is(err, domain.ErrInvalid) {
		t.Fatalf("dimension error = %v", err)
	}
}

func TestDenseSearchWithNoWorkAvoidsExternalCalls(t *testing.T) {
	descriptor := denseDescriptor()
	embedder := &denseTestEmbedder{descriptor: descriptor}
	probe := denseVector(1)
	space := denseSpace(t, descriptor, probe)
	target := denseTarget(descriptor, probe, space)
	backend := &denseTestBackend{states: []postgres.ServingProjectionState{{Target: &target, Generation: 1}}}
	retriever, err := NewDense(backend, embedder, space)
	if err != nil {
		t.Fatalf("NewDense: %v", err)
	}
	for _, input := range []struct {
		query string
		limit int
	}{{query: "query", limit: 0}, {query: "  ", limit: 1}} {
		hits, err := retriever.Search(context.Background(), "tenant", "user", input.query, input.limit, time.Now())
		if err != nil || len(hits) != 0 {
			t.Fatalf("Search(%q,%d) = %#v, %v", input.query, input.limit, hits, err)
		}
	}
	if backend.currentCalls != 2 || len(embedder.inputs) != 0 {
		t.Fatalf("external calls current=%d embed=%d", backend.currentCalls, len(embedder.inputs))
	}
}

func denseDescriptor() embedding.Descriptor {
	return embedding.Descriptor{
		Provider:        embedding.ProviderLMStudio,
		API:             embedding.APIEmbeddingsV1,
		Model:           "test-model",
		Dimension:       postgres.VectorDimension,
		DocumentVersion: embedding.MemoryCardDocumentVersion,
	}
}

func denseVector(seed float32) []float32 {
	result := make([]float32, postgres.VectorDimension)
	for index := range result {
		result[index] = seed + float32(index%13)/17
	}
	return result
}

func denseSpace(t *testing.T, descriptor embedding.Descriptor, probe []float32) string {
	t.Helper()
	space, err := embedding.SpaceID(
		descriptor.Provider,
		descriptor.Model,
		descriptor.Dimension,
		embedding.MemoryCardDocumentVersion,
		embedding.RawQueryVersion,
		embedding.VectorSHA256(probe),
	)
	if err != nil {
		t.Fatalf("SpaceID: %v", err)
	}
	return space
}

func denseTarget(descriptor embedding.Descriptor, probe []float32, space string) postgres.ProjectionTarget {
	return postgres.ProjectionTarget{
		Space: postgres.EmbeddingSpaceDefinition{
			ID:               space,
			Provider:         descriptor.Provider,
			Model:            descriptor.Model,
			Dimension:        descriptor.Dimension,
			DocumentVersion:  descriptor.DocumentVersion,
			QueryVersion:     embedding.RawQueryVersion,
			ModelFingerprint: embedding.VectorSHA256(probe),
		},
		State:      postgres.ProjectionTargetServing,
		EnqueueNew: true,
	}
}
