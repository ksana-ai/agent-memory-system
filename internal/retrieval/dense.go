package retrieval

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/kai443/go-agent-memory-system/internal/domain"
	"github.com/kai443/go-agent-memory-system/internal/embedding"
	"github.com/kai443/go-agent-memory-system/internal/store/postgres"
)

// DenseBackend is the serving-only database contract used by Dense. The
// expectation passed to SearchServingVector fences a query against a
// concurrent projection rotation after the provider request has completed.
type DenseBackend interface {
	CurrentServingProjection(context.Context) (postgres.ServingProjectionState, error)
	SearchServingVector(
		context.Context,
		string,
		string,
		postgres.ServingVectorExpectation,
		[]float32,
		int,
		time.Time,
	) ([]domain.SearchHit, error)
}

// Embedder is the narrow, endpoint-free provider contract needed for serving.
// Descriptor must not contain a URL or credentials.
type Embedder interface {
	Embed(context.Context, []string) ([][]float32, error)
	Descriptor() embedding.Descriptor
}

// Dense serves only one explicitly pinned embedding space. It does not choose
// whichever target happens to be serving: a deployment must configure the
// expected space and restart/reconfigure this component deliberately.
type Dense struct {
	backend       DenseBackend
	embedder      Embedder
	expectedSpace string
	descriptor    embedding.Descriptor
}

func NewDense(backend DenseBackend, embedder Embedder, expectedSpace string) (*Dense, error) {
	if backend == nil {
		return nil, fmt.Errorf("dense backend is required: %w", domain.ErrInvalid)
	}
	if embedder == nil {
		return nil, fmt.Errorf("dense embedder is required: %w", domain.ErrInvalid)
	}
	expectedSpace = strings.TrimSpace(expectedSpace)
	if expectedSpace == "" {
		return nil, fmt.Errorf("dense expected space is required: %w", domain.ErrInvalid)
	}
	descriptor := embedder.Descriptor()
	if err := validateDenseDescriptor(descriptor); err != nil {
		return nil, err
	}
	return &Dense{
		backend:       backend,
		embedder:      embedder,
		expectedSpace: expectedSpace,
		descriptor:    descriptor,
	}, nil
}

// Ready verifies the serving pin with a fixed public behavioral probe. The two
// identical inputs are sent in one request so a non-deterministic provider is
// rejected without sending tenant content. Readiness is an instantaneous
// check, not a model-weights identity proof.
func (retriever *Dense) Ready(ctx context.Context) error {
	state, target, err := retriever.currentServing(ctx)
	if err != nil {
		return err
	}

	vectors, err := retriever.embedder.Embed(ctx, []string{embedding.ProbeTextV1, embedding.ProbeTextV1})
	if err != nil {
		return denseExternalFailure(ctx, "dense provider probe failed", err)
	}
	if retriever.embedder.Descriptor() != retriever.descriptor {
		return denseUnavailable("dense provider descriptor changed")
	}
	validated, err := validateDenseVectors(vectors, 2, retriever.descriptor.Dimension)
	if err != nil {
		return denseUnavailable("dense provider probe was invalid")
	}
	firstFingerprint := embedding.VectorSHA256(validated[0])
	if firstFingerprint != embedding.VectorSHA256(validated[1]) {
		return denseUnavailable("dense provider probe was not stable")
	}
	if err := retriever.validateTarget(target, firstFingerprint); err != nil {
		return err
	}

	// Re-read the deployment fence after provider I/O. Search uses the stronger
	// SearchServingVector expectation in the same position; readiness has no
	// tenant query with which to perform that operation.
	after, afterTarget, err := retriever.currentServing(ctx)
	if err != nil {
		return err
	}
	if after.Generation != state.Generation || afterTarget.Space.ID != target.Space.ID {
		return denseUnavailable("dense serving projection changed during probe")
	}
	if err := retriever.validateTarget(afterTarget, firstFingerprint); err != nil {
		return err
	}
	return nil
}

func (retriever *Dense) Search(
	ctx context.Context,
	tenantID, userID, query string,
	limit int,
	asOf time.Time,
) ([]domain.SearchHit, error) {
	state, target, err := retriever.currentServing(ctx)
	if err != nil {
		return nil, err
	}
	if limit <= 0 || strings.TrimSpace(query) == "" {
		return []domain.SearchHit{}, nil
	}
	// Validate every persisted field that can be checked before provider I/O.
	// The fingerprint and derived space are checked after the public probe.
	if err := retriever.validateStaticTarget(target); err != nil {
		return nil, err
	}

	trimmedQuery := strings.TrimSpace(query)
	vectors, err := retriever.embedder.Embed(ctx, []string{embedding.ProbeTextV1, trimmedQuery})
	if err != nil {
		return nil, denseExternalFailure(ctx, "dense query embedding failed", err)
	}
	if retriever.embedder.Descriptor() != retriever.descriptor {
		return nil, denseUnavailable("dense provider descriptor changed")
	}
	validated, err := validateDenseVectors(vectors, 2, retriever.descriptor.Dimension)
	if err != nil {
		return nil, denseUnavailable("dense query embedding response was invalid")
	}
	if err := retriever.validateTarget(target, embedding.VectorSHA256(validated[0])); err != nil {
		return nil, err
	}

	hits, err := retriever.backend.SearchServingVector(
		ctx,
		tenantID,
		userID,
		postgres.ServingVectorExpectation{
			EmbeddingSpace: retriever.expectedSpace,
			Generation:     state.Generation,
		},
		validated[1],
		limit,
		asOf,
	)
	if err != nil {
		return nil, denseExternalFailure(ctx, "dense serving vector search failed", err)
	}
	return hits, nil
}

func (retriever *Dense) currentServing(ctx context.Context) (postgres.ServingProjectionState, postgres.ProjectionTarget, error) {
	state, err := retriever.backend.CurrentServingProjection(ctx)
	if err != nil {
		return postgres.ServingProjectionState{}, postgres.ProjectionTarget{}, denseExternalFailure(ctx, "dense serving projection read failed", err)
	}
	if state.Target == nil {
		return postgres.ServingProjectionState{}, postgres.ProjectionTarget{}, denseUnavailable("dense serving projection is not configured")
	}
	target := *state.Target
	if state.Generation < 0 {
		return postgres.ServingProjectionState{}, postgres.ProjectionTarget{}, denseUnavailable("dense serving generation is invalid")
	}
	if err := retriever.validateStaticTarget(target); err != nil {
		return postgres.ServingProjectionState{}, postgres.ProjectionTarget{}, err
	}
	return state, target, nil
}

func (retriever *Dense) validateStaticTarget(target postgres.ProjectionTarget) error {
	if retriever.embedder.Descriptor() != retriever.descriptor {
		return denseUnavailable("dense provider descriptor changed")
	}
	space := target.Space
	if target.State != postgres.ProjectionTargetServing || !target.EnqueueNew ||
		space.ID != retriever.expectedSpace ||
		space.Provider != retriever.descriptor.Provider ||
		space.Model != retriever.descriptor.Model ||
		space.Dimension != retriever.descriptor.Dimension ||
		space.DocumentVersion != embedding.MemoryCardDocumentVersion ||
		space.DocumentVersion != retriever.descriptor.DocumentVersion ||
		space.QueryVersion != embedding.RawQueryVersion ||
		strings.TrimSpace(space.ModelFingerprint) == "" {
		return denseUnavailable("dense serving projection does not match the configured pin")
	}
	return nil
}

func (retriever *Dense) validateTarget(target postgres.ProjectionTarget, fingerprint string) error {
	if err := retriever.validateStaticTarget(target); err != nil {
		return err
	}
	space, err := embedding.SpaceID(
		retriever.descriptor.Provider,
		retriever.descriptor.Model,
		retriever.descriptor.Dimension,
		embedding.MemoryCardDocumentVersion,
		embedding.RawQueryVersion,
		fingerprint,
	)
	if err != nil {
		return denseUnavailable("dense embedding space could not be verified")
	}
	if target.Space.ModelFingerprint != fingerprint ||
		space != retriever.expectedSpace ||
		target.Space.ID != space {
		return denseUnavailable("dense embedding behavior does not match the serving projection")
	}
	return nil
}

func validateDenseDescriptor(descriptor embedding.Descriptor) error {
	if strings.TrimSpace(descriptor.Provider) == "" ||
		strings.TrimSpace(descriptor.API) == "" ||
		strings.TrimSpace(descriptor.Model) == "" ||
		descriptor.Dimension != postgres.VectorDimension ||
		descriptor.DocumentVersion != embedding.MemoryCardDocumentVersion {
		return fmt.Errorf("dense embedding descriptor is incompatible: %w", domain.ErrInvalid)
	}
	return nil
}

func validateDenseVectors(vectors [][]float32, count, dimension int) ([][]float32, error) {
	if len(vectors) != count {
		return nil, errors.New("unexpected vector count")
	}
	result := make([][]float32, count)
	for index, vector := range vectors {
		if len(vector) != dimension {
			return nil, errors.New("unexpected vector dimension")
		}
		copyVector := append([]float32(nil), vector...)
		nonzero := false
		for _, component := range copyVector {
			if math.IsNaN(float64(component)) || math.IsInf(float64(component), 0) {
				return nil, errors.New("non-finite vector")
			}
			nonzero = nonzero || component != 0
		}
		if !nonzero {
			return nil, errors.New("zero vector")
		}
		result[index] = copyVector
	}
	return result, nil
}

func denseExternalFailure(ctx context.Context, operation string, err error) error {
	if ctxErr := ctx.Err(); ctxErr != nil {
		return ctxErr
	}
	if errors.Is(err, domain.ErrInvariant) {
		return fmt.Errorf("%s: %w", operation, domain.ErrInvariant)
	}
	return denseUnavailable(operation)
}

func denseUnavailable(operation string) error {
	return fmt.Errorf("%s: %w", operation, domain.ErrUnavailable)
}
