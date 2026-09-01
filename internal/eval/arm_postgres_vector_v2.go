package eval

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/ksana-ai/agent-memory-system/internal/domain"
	"github.com/ksana-ai/agent-memory-system/internal/embedding"
	"github.com/ksana-ai/agent-memory-system/internal/store/postgres"
)

const ArmReviewedCardsPostgresVectorV1 = "reviewed-cards-postgres-vector-v1"

const (
	postgresVectorProbeID      = "memory-embedding-behavioral-probe-v1"
	postgresVectorRetryCount   = 0
	postgresVectorLatencyScope = "query_embedding_plus_postgres_vector_search"
)

type postgresVectorEmbedder interface {
	Descriptor() embedding.Descriptor
	Embed(context.Context, []string) ([][]float32, error)
}

type postgresVectorBackend interface {
	postgresFTSBackend
	UpsertMemoryEmbedding(context.Context, postgres.MemoryEmbedding) error
	SearchVector(context.Context, string, string, string, []float32, int, time.Time) ([]domain.SearchHit, error)
	VectorMetadata(context.Context) (postgres.VectorMetadata, error)
}

type postgresVectorOpener func(context.Context, string) (postgresVectorBackend, error)

// NewPostgresVectorArmFactory constructs the real LM Studio plus pgvector
// evaluation arm. Network locations remain closure-only configuration and are
// never added to descriptors, manifests, or returned errors.
func NewPostgresVectorArmFactory(ctx context.Context, databaseURL, embeddingsURL, model string) (ArmFactory, error) {
	client, err := embedding.NewClient(embedding.Config{
		Endpoint:          embeddingsURL,
		Model:             model,
		ExpectedDimension: postgres.VectorDimension,
		Timeout:           embedding.DefaultTimeout,
		MaxResponseBytes:  embedding.DefaultMaxResponseBytes,
		MaxBatchSize:      1,
		MaxInputBytes:     embedding.DefaultMaxInputBytes,
	})
	if err != nil {
		return nil, errors.New("configure LM Studio embedding component failed")
	}
	return newPostgresVectorArmFactory(
		ctx,
		databaseURL,
		client,
		func(ctx context.Context, databaseURL string) (postgresVectorBackend, error) {
			return postgres.Open(ctx, databaseURL)
		},
		randomPostgresNamespaceToken,
		func() time.Time { return time.Now().UTC() },
	)
}

func newPostgresVectorArmFactory(
	ctx context.Context,
	databaseURL string,
	embedder postgresVectorEmbedder,
	opener postgresVectorOpener,
	namespaceToken func() (string, error),
	now func() time.Time,
) (ArmFactory, error) {
	if strings.TrimSpace(databaseURL) == "" {
		return nil, errors.New("PostgreSQL URL is required for the PostgreSQL vector evaluation arm")
	}
	if embedder == nil || opener == nil || namespaceToken == nil || now == nil {
		return nil, errors.New("PostgreSQL vector evaluation dependencies are incomplete")
	}

	embeddingDescriptor := embedder.Descriptor()
	if err := validatePostgresVectorEmbeddingDescriptor(embeddingDescriptor); err != nil {
		return nil, err
	}
	modelFingerprint, err := probePostgresVectorEmbedder(ctx, embedder, embeddingDescriptor.Dimension)
	if err != nil {
		return nil, errors.New("probe LM Studio embedding component failed")
	}
	embeddingSpace, err := embedding.SpaceID(
		embeddingDescriptor.Provider,
		embeddingDescriptor.Model,
		embeddingDescriptor.Dimension,
		embedding.MemoryCardDocumentVersion,
		embedding.RawQueryVersion,
		modelFingerprint,
	)
	if err != nil {
		return nil, errors.New("construct embedding space identity failed")
	}

	probe, err := opener(ctx, databaseURL)
	if err != nil {
		return nil, errors.New("connect to PostgreSQL for vector component probe failed")
	}
	postgresMetadata, metadataErr := probe.VectorMetadata(ctx)
	probe.Close()
	if metadataErr != nil {
		return nil, errors.New("read PostgreSQL vector component metadata failed")
	}
	descriptorMetadata := postgresVectorDescriptorMetadata(postgresMetadata, embeddingDescriptor, modelFingerprint, embeddingSpace)
	config := struct {
		Storage    string            `json:"storage"`
		Retriever  string            `json:"retriever"`
		Components map[string]string `json:"components"`
	}{Storage: "postgresql", Retriever: "lmstudio-postgres-vector-v1", Components: descriptorMetadata}

	return armFactory{
		descriptor: ArmDescriptor{
			ID:              ArmReviewedCardsPostgresVectorV1,
			Version:         "1",
			JudgmentProfile: "reviewed-memory-alias-v1",
			ResultKind:      "memory-card",
			ConfigHash:      hashArmConfig(config),
			Metadata:        descriptorMetadata,
		},
		newRuntime: func(ctx context.Context) (ArmRuntime, error) {
			backend, openErr := opener(ctx, databaseURL)
			if openErr != nil {
				return ArmRuntime{}, errors.New("open PostgreSQL vector case runtime failed")
			}
			runtimePostgresMetadata, runtimeMetadataErr := backend.VectorMetadata(ctx)
			if runtimeMetadataErr != nil {
				backend.Close()
				return ArmRuntime{}, errors.New("read PostgreSQL vector case component metadata failed")
			}
			runtimeFingerprint, runtimeProbeErr := probePostgresVectorEmbedder(ctx, embedder, embeddingDescriptor.Dimension)
			if runtimeProbeErr != nil {
				backend.Close()
				return ArmRuntime{}, errors.New("probe LM Studio embedding case component failed")
			}
			runtimeSpace, runtimeSpaceErr := embedding.SpaceID(
				embeddingDescriptor.Provider,
				embeddingDescriptor.Model,
				embeddingDescriptor.Dimension,
				embedding.MemoryCardDocumentVersion,
				embedding.RawQueryVersion,
				runtimeFingerprint,
			)
			if runtimeSpaceErr != nil || !maps.Equal(
				postgresVectorDescriptorMetadata(runtimePostgresMetadata, embeddingDescriptor, runtimeFingerprint, runtimeSpace),
				descriptorMetadata,
			) {
				backend.Close()
				return ArmRuntime{}, errors.New("PostgreSQL vector or embedding components changed after factory construction")
			}
			token, tokenErr := namespaceToken()
			if tokenErr != nil {
				backend.Close()
				return ArmRuntime{}, errors.New("create PostgreSQL vector case namespace failed")
			}
			namespace := newPostgresFTSNamespace(backend, token)
			runtime := &postgresVectorRuntime{
				backend:          backend,
				namespace:        namespace,
				embedder:         embedder,
				descriptor:       embeddingDescriptor,
				embeddingSpace:   embeddingSpace,
				modelFingerprint: modelFingerprint,
				now:              now,
			}
			return ArmRuntime{
				Store: namespace, Retriever: runtime,
				ProjectApprovedMemory: runtime.ProjectApprovedMemory,
				Cleanup:               namespace.Cleanup,
			}, nil
		},
	}, nil
}

func validatePostgresVectorEmbeddingDescriptor(descriptor embedding.Descriptor) error {
	if strings.TrimSpace(descriptor.Provider) == "" || strings.TrimSpace(descriptor.API) == "" || strings.TrimSpace(descriptor.Model) == "" {
		return errors.New("LM Studio embedding descriptor is incomplete")
	}
	if descriptor.Dimension != postgres.VectorDimension {
		return fmt.Errorf("LM Studio embedding dimension is %d, PostgreSQL requires %d", descriptor.Dimension, postgres.VectorDimension)
	}
	if descriptor.DocumentVersion != embedding.MemoryCardDocumentVersion {
		return errors.New("LM Studio embedding document version is incompatible")
	}
	return nil
}

func probePostgresVectorEmbedder(ctx context.Context, embedder postgresVectorEmbedder, dimension int) (string, error) {
	vectors, err := embedder.Embed(ctx, []string{embedding.ProbeTextV1})
	if err != nil {
		return "", err
	}
	vector, err := onePostgresVector(vectors, dimension)
	if err != nil {
		return "", err
	}
	return embedding.VectorSHA256(vector), nil
}

func postgresVectorDescriptorMetadata(
	postgresMetadata postgres.VectorMetadata,
	embeddingDescriptor embedding.Descriptor,
	modelFingerprint, embeddingSpace string,
) map[string]string {
	return map[string]string{
		"postgres_server_version_num":           postgresMetadata.ServerVersionNum,
		"schema_migration_version":              strconv.Itoa(postgresMetadata.SchemaMigrationVersion),
		"pgvector_extension_version":            postgresMetadata.ExtensionVersion,
		"vector_dimension":                      strconv.Itoa(postgresMetadata.Dimension),
		"vector_distance_metric":                postgresMetadata.DistanceMetric,
		"vector_search_strategy":                postgresMetadata.SearchStrategy,
		"vector_approximate_index_count":        strconv.Itoa(postgresMetadata.ApproximateIndexCount),
		"embedding_provider":                    embeddingDescriptor.Provider,
		"embedding_api":                         embeddingDescriptor.API,
		"embedding_model_requested":             embeddingDescriptor.Model,
		"embedding_model_returned":              embeddingDescriptor.Model,
		"embedding_document_version":            embedding.MemoryCardDocumentVersion,
		"embedding_query_version":               embedding.RawQueryVersion,
		"embedding_probe_id":                    postgresVectorProbeID,
		"embedding_probe_text_sha256":           embedding.ProbeTextV1SHA256,
		"embedding_behavior_sha256":             modelFingerprint,
		"embedding_space":                       embeddingSpace,
		"embedding_vector_fingerprint_encoding": "float32_ieee754_big_endian_sha256_v1",
		"embedding_client_normalization":        "none",
		"embedding_retry_count":                 strconv.Itoa(postgresVectorRetryCount),
		"embedding_request_timeout_nanoseconds": strconv.FormatInt(embedding.DefaultTimeout.Nanoseconds(), 10),
		"embedding_max_response_bytes":          strconv.FormatInt(embedding.DefaultMaxResponseBytes, 10),
		"embedding_max_batch_size":              "1",
		"embedding_max_input_bytes":             strconv.Itoa(embedding.DefaultMaxInputBytes),
		"retrieval_latency_scope":               postgresVectorLatencyScope,
	}
}

type postgresVectorRuntime struct {
	backend          postgresVectorBackend
	namespace        *postgresFTSNamespace
	embedder         postgresVectorEmbedder
	descriptor       embedding.Descriptor
	embeddingSpace   string
	modelFingerprint string
	now              func() time.Time
}

func (runtime *postgresVectorRuntime) ProjectApprovedMemory(ctx context.Context, memory domain.MemoryCard) error {
	document := embedding.MemoryCardDocumentV1(memory)
	vectors, err := runtime.embedder.Embed(ctx, []string{document})
	if err != nil {
		return errors.New("embed reviewed memory document failed")
	}
	vector, err := onePostgresVector(vectors, runtime.descriptor.Dimension)
	if err != nil {
		return errors.New("validate reviewed memory embedding failed")
	}
	physical := runtime.namespace.physicalScope(memory.TenantID, memory.UserID)
	if err := runtime.backend.UpsertMemoryEmbedding(ctx, postgres.MemoryEmbedding{
		TenantID:         physical.tenantID,
		UserID:           physical.userID,
		MemoryID:         memory.ID,
		EmbeddingSpace:   runtime.embeddingSpace,
		Provider:         runtime.descriptor.Provider,
		Model:            runtime.descriptor.Model,
		DocumentVersion:  embedding.MemoryCardDocumentVersion,
		QueryVersion:     embedding.RawQueryVersion,
		ModelFingerprint: runtime.modelFingerprint,
		ContentSHA256:    embedding.DocumentSHA256(document),
		Vector:           vector,
		CreatedAt:        runtime.now().UTC(),
	}); err != nil {
		return errors.New("store reviewed memory embedding failed")
	}
	return nil
}

func (runtime *postgresVectorRuntime) Search(
	ctx context.Context,
	tenantID, userID, query string,
	limit int,
	asOf time.Time,
) ([]domain.SearchHit, error) {
	if limit <= 0 {
		return []domain.SearchHit{}, nil
	}
	vectors, err := runtime.embedder.Embed(ctx, []string{query})
	if err != nil {
		return nil, errors.New("embed vector query failed")
	}
	vector, err := onePostgresVector(vectors, runtime.descriptor.Dimension)
	if err != nil {
		return nil, errors.New("validate vector query embedding failed")
	}
	physical := runtime.namespace.physicalScope(tenantID, userID)
	hits, err := runtime.backend.SearchVector(ctx, physical.tenantID, physical.userID, runtime.embeddingSpace, vector, limit, asOf)
	if err != nil {
		return nil, errors.New("search PostgreSQL memory vectors failed")
	}
	for index := range hits {
		if err := restorePostgresScope(&hits[index].Memory.TenantID, &hits[index].Memory.UserID, tenantID, userID, physical); err != nil {
			return nil, err
		}
	}
	return hits, nil
}

func onePostgresVector(vectors [][]float32, dimension int) ([]float32, error) {
	if len(vectors) != 1 || len(vectors[0]) != dimension {
		return nil, errors.New("embedding component returned an unexpected vector shape")
	}
	vector := append([]float32(nil), vectors[0]...)
	nonzero := false
	for _, component := range vector {
		if math.IsNaN(float64(component)) || math.IsInf(float64(component), 0) {
			return nil, errors.New("embedding component returned a non-finite vector")
		}
		nonzero = nonzero || component != 0
	}
	if !nonzero {
		return nil, errors.New("embedding component returned an all-zero vector")
	}
	return vector, nil
}
