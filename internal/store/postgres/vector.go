package postgres

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/kai443/go-agent-memory-system/internal/domain"
)

const (
	// VectorDimension is the measured output dimension of the configured
	// text-embedding-bge-m3 endpoint. The database typmod pins the same value.
	VectorDimension = 1024

	VectorDistanceMetric = "cosine"
	VectorSearchStrategy = "exact_cosine_v1"
)

// MemoryEmbedding is a versioned vector projection of one reviewed memory
// card. EmbeddingSpace identifies a mutually comparable vector space; callers
// must change it whenever model or document construction semantics change.
type MemoryEmbedding struct {
	TenantID         string
	UserID           string
	MemoryID         string
	EmbeddingSpace   string
	Provider         string
	Model            string
	DocumentVersion  string
	QueryVersion     string
	ModelFingerprint string
	ContentSHA256    string
	Vector           []float32
	CreatedAt        time.Time
}

// VectorMetadata contains only reproducibility facts and intentionally omits
// connection strings, database names, hosts, and credentials.
type VectorMetadata struct {
	ServerVersionNum       string `json:"server_version_num"`
	ExtensionVersion       string `json:"extension_version"`
	SchemaMigrationVersion int    `json:"schema_migration_version"`
	Dimension              int    `json:"dimension"`
	DistanceMetric         string `json:"distance_metric"`
	SearchStrategy         string `json:"search_strategy"`
	ApproximateIndexCount  int    `json:"approximate_index_count"`
}

// UpsertMemoryEmbedding atomically inserts or replaces one memory/space
// projection. The existing scope row is locked so it serializes with
// ForgetUser without recreating deleted lifecycle content or leaving orphans.
func (s *Store) UpsertMemoryEmbedding(ctx context.Context, value MemoryEmbedding) error {
	normalized, vectorText, err := validateMemoryEmbedding(value)
	if err != nil {
		return err
	}
	if err := s.ready(); err != nil {
		return err
	}

	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return fmt.Errorf("begin memory embedding upsert: %w", err)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()

	if err := lockExistingScope(ctx, tx, normalized.TenantID, normalized.UserID); err != nil {
		return err
	}
	var exists int
	err = tx.QueryRow(ctx, `
		SELECT 1
		FROM agent_memory.memory_cards
		WHERE tenant_id = $1 AND user_id = $2 AND id = $3 AND status = 'active'
		FOR KEY SHARE`, normalized.TenantID, normalized.UserID, normalized.MemoryID).Scan(&exists)
	if errors.Is(err, pgx.ErrNoRows) {
		return fmt.Errorf("active memory %q: %w", normalized.MemoryID, domain.ErrNotFound)
	}
	if err != nil {
		return mapPostgresError("lock memory card for embedding", err)
	}

	if err := ensureEmbeddingSpace(ctx, tx, normalized); err != nil {
		return err
	}

	var existingContentSHA256 string
	err = tx.QueryRow(ctx, `
		SELECT content_sha256
		FROM agent_memory.memory_embeddings
		WHERE tenant_id = $1 AND user_id = $2 AND memory_id = $3 AND embedding_space = $4`,
		normalized.TenantID,
		normalized.UserID,
		normalized.MemoryID,
		normalized.EmbeddingSpace,
	).Scan(&existingContentSHA256)
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return mapPostgresError("load existing memory embedding", err)
	}
	if err == nil && existingContentSHA256 != normalized.ContentSHA256 {
		return fmt.Errorf("memory %q content changed within embedding space %q: %w", normalized.MemoryID, normalized.EmbeddingSpace, domain.ErrConflict)
	}

	commandTag, err := tx.Exec(ctx, `
		INSERT INTO agent_memory.memory_embeddings (
			tenant_id, user_id, memory_id, embedding_space, provider, model,
			document_version, query_version, model_fingerprint, content_sha256,
			embedding, created_at
		) VALUES (
			$1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11::vector(1024), $12
		)
		ON CONFLICT (tenant_id, user_id, memory_id, embedding_space)
		DO UPDATE SET
			provider = EXCLUDED.provider,
			model = EXCLUDED.model,
			document_version = EXCLUDED.document_version,
			query_version = EXCLUDED.query_version,
			model_fingerprint = EXCLUDED.model_fingerprint,
			content_sha256 = EXCLUDED.content_sha256,
			embedding = EXCLUDED.embedding,
			created_at = EXCLUDED.created_at
		WHERE memory_embeddings.content_sha256 = EXCLUDED.content_sha256
		  AND (
			memory_embeddings.provider,
			memory_embeddings.model,
			memory_embeddings.document_version,
			memory_embeddings.query_version,
			memory_embeddings.model_fingerprint,
			memory_embeddings.content_sha256,
			memory_embeddings.embedding
		  ) IS DISTINCT FROM (
			EXCLUDED.provider,
			EXCLUDED.model,
			EXCLUDED.document_version,
			EXCLUDED.query_version,
			EXCLUDED.model_fingerprint,
			EXCLUDED.content_sha256,
			EXCLUDED.embedding
		  )`,
		normalized.TenantID, normalized.UserID, normalized.MemoryID,
		normalized.EmbeddingSpace, normalized.Provider, normalized.Model,
		normalized.DocumentVersion, normalized.QueryVersion,
		normalized.ModelFingerprint, normalized.ContentSHA256, vectorText,
		normalized.CreatedAt,
	)
	if err != nil {
		return mapPostgresError("upsert memory embedding", err)
	}
	if commandTag.RowsAffected() > 1 {
		return fmt.Errorf("memory embedding upsert affected %d rows: %w", commandTag.RowsAffected(), domain.ErrInvariant)
	}
	if commandTag.RowsAffected() == 1 {
		commandTag, err = tx.Exec(ctx, `
			UPDATE agent_memory.user_scope_state
			SET context_revision = context_revision + 1,
			    updated_at = CURRENT_TIMESTAMP
			WHERE tenant_id = $1 AND user_id = $2`, normalized.TenantID, normalized.UserID)
		if err != nil {
			return mapPostgresError("advance embedding context revision", err)
		}
		if commandTag.RowsAffected() != 1 {
			return fmt.Errorf("user scope state disappeared during embedding upsert: %w", domain.ErrInvariant)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return mapPostgresError("commit memory embedding upsert", err)
	}
	return nil
}

// SearchVector performs an exact tenant-scoped cosine search. Card lifecycle,
// expiration, embedding-space, and scope filters all execute in the ranking
// SQL so non-serviceable rows fail closed.
func (s *Store) SearchVector(
	ctx context.Context,
	tenantID, userID, embeddingSpace string,
	query []float32,
	limit int,
	asOf time.Time,
) ([]domain.SearchHit, error) {
	if err := validateRequired("tenant id", tenantID); err != nil {
		return nil, err
	}
	if err := validateRequired("user id", userID); err != nil {
		return nil, err
	}
	if err := validateRequired("embedding space", embeddingSpace); err != nil {
		return nil, err
	}
	if limit <= 0 {
		return []domain.SearchHit{}, nil
	}
	queryText, err := encodeVector(query)
	if err != nil {
		return nil, fmt.Errorf("query vector: %w", err)
	}
	if err := s.ready(); err != nil {
		return nil, err
	}

	rows, err := s.pool.Query(ctx, `
		WITH query_embedding AS (
			SELECT $4::vector(1024) AS value
		)
		SELECT card.id, card.candidate_id, card.tenant_id, card.user_id, card.kind,
		       card.category, card.memory_key, card.value, card.person,
		       card.relationship, card.backstory,
		       COALESCE(ARRAY(
		           SELECT source.evidence_event_id
		           FROM agent_memory.candidate_source_events AS source
		           WHERE source.tenant_id = card.tenant_id
		             AND source.user_id = card.user_id
		             AND source.candidate_id = card.candidate_id
		           ORDER BY source.source_order
		       ), ARRAY[]::text[]),
		       card.version, card.status, card.created_at, card.expires_at,
		       card.superseded_at,
		       (1 - (embedding.embedding <=> query_embedding.value))::double precision
		FROM agent_memory.memory_embeddings AS embedding
		JOIN agent_memory.memory_cards AS card
		  ON card.tenant_id = embedding.tenant_id
		 AND card.user_id = embedding.user_id
		 AND card.id = embedding.memory_id
		CROSS JOIN query_embedding
		WHERE embedding.tenant_id = $1
		  AND embedding.user_id = $2
		  AND embedding.embedding_space = $3
		  AND card.status = 'active'
		  AND (card.expires_at IS NULL OR card.expires_at > $6)
		ORDER BY embedding.embedding <=> query_embedding.value ASC,
		         card.created_at DESC,
		         card.id ASC
		LIMIT $5`, tenantID, userID, embeddingSpace, queryText, limit, asOf)
	if err != nil {
		return nil, mapPostgresError("search memory vectors", err)
	}
	defer rows.Close()

	hits := make([]domain.SearchHit, 0)
	for rows.Next() {
		memory, score, scanErr := scanMemoryWithScore(rows)
		if scanErr != nil {
			return nil, scanErr
		}
		hits = append(hits, domain.SearchHit{Memory: memory, Score: score})
	}
	if err := rows.Err(); err != nil {
		return nil, mapPostgresError("iterate memory vector search results", err)
	}
	return hits, nil
}

func (s *Store) VectorMetadata(ctx context.Context) (VectorMetadata, error) {
	if err := s.ready(); err != nil {
		return VectorMetadata{}, err
	}
	metadata := VectorMetadata{
		Dimension:      VectorDimension,
		DistanceMetric: VectorDistanceMetric,
		SearchStrategy: VectorSearchStrategy,
	}
	var embeddingColumnType string
	err := s.pool.QueryRow(ctx, `
		SELECT current_setting('server_version_num'),
		       COALESCE((SELECT extversion FROM pg_extension WHERE extname = 'vector'), ''),
		       COALESCE((SELECT max(version) FROM public.agent_memory_schema_version), 0),
		       COALESCE((
		           SELECT format_type(attribute.atttypid, attribute.atttypmod)
		           FROM pg_attribute AS attribute
		           JOIN pg_class AS relation ON relation.oid = attribute.attrelid
		           JOIN pg_namespace AS namespace ON namespace.oid = relation.relnamespace
		           WHERE namespace.nspname = 'agent_memory'
		             AND relation.relname = 'memory_embeddings'
		             AND attribute.attname = 'embedding'
		             AND NOT attribute.attisdropped
		       ), ''),
		       COALESCE((
		           SELECT count(*)
		           FROM pg_index AS index_row
		           JOIN pg_class AS index_relation ON index_relation.oid = index_row.indexrelid
		           JOIN pg_am AS access_method ON access_method.oid = index_relation.relam
		           JOIN pg_class AS table_relation ON table_relation.oid = index_row.indrelid
		           JOIN pg_namespace AS namespace ON namespace.oid = table_relation.relnamespace
		           WHERE namespace.nspname = 'agent_memory'
		             AND table_relation.relname = 'memory_embeddings'
		             AND access_method.amname IN ('hnsw', 'ivfflat')
		       ), 0)`).Scan(
		&metadata.ServerVersionNum,
		&metadata.ExtensionVersion,
		&metadata.SchemaMigrationVersion,
		&embeddingColumnType,
		&metadata.ApproximateIndexCount,
	)
	if err != nil {
		return VectorMetadata{}, mapPostgresError("load PostgreSQL vector metadata", err)
	}
	if metadata.ExtensionVersion == "" {
		return VectorMetadata{}, fmt.Errorf("pgvector extension is unavailable: %w", domain.ErrInvariant)
	}
	if metadata.SchemaMigrationVersion < 4 || embeddingColumnType != "vector(1024)" {
		return VectorMetadata{}, fmt.Errorf("PostgreSQL vector schema is incompatible: %w", domain.ErrInvariant)
	}
	if metadata.ApproximateIndexCount != 0 {
		return VectorMetadata{}, fmt.Errorf("approximate vector index is incompatible with exact-search evidence: %w", domain.ErrInvariant)
	}
	return metadata, nil
}

func ensureEmbeddingSpace(ctx context.Context, tx pgx.Tx, value MemoryEmbedding) error {
	_, err := tx.Exec(ctx, `
		INSERT INTO agent_memory.embedding_spaces (
			id, provider, model, dimension, document_version, query_version,
			model_fingerprint, created_at
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
		ON CONFLICT (id) DO NOTHING`,
		value.EmbeddingSpace,
		value.Provider,
		value.Model,
		VectorDimension,
		value.DocumentVersion,
		value.QueryVersion,
		value.ModelFingerprint,
		value.CreatedAt,
	)
	if err != nil {
		return mapPostgresError("register embedding space", err)
	}

	var provider, model, documentVersion, queryVersion, modelFingerprint string
	var dimension int
	err = tx.QueryRow(ctx, `
		SELECT provider, model, dimension, document_version, query_version, model_fingerprint
		FROM agent_memory.embedding_spaces
		WHERE id = $1
		FOR KEY SHARE`, value.EmbeddingSpace).Scan(
		&provider,
		&model,
		&dimension,
		&documentVersion,
		&queryVersion,
		&modelFingerprint,
	)
	if err != nil {
		return mapExpectedRowError("load embedding space", err)
	}
	if provider != value.Provider ||
		model != value.Model ||
		dimension != VectorDimension ||
		documentVersion != value.DocumentVersion ||
		queryVersion != value.QueryVersion ||
		modelFingerprint != value.ModelFingerprint {
		return fmt.Errorf("embedding space %q configuration differs from its registry: %w", value.EmbeddingSpace, domain.ErrConflict)
	}
	return nil
}

func lockExistingScope(ctx context.Context, tx pgx.Tx, tenantID, userID string) error {
	var revision int64
	err := tx.QueryRow(ctx, `
		SELECT context_revision
		FROM agent_memory.user_scope_state
		WHERE tenant_id = $1 AND user_id = $2
		FOR UPDATE`, tenantID, userID).Scan(&revision)
	if errors.Is(err, pgx.ErrNoRows) {
		return fmt.Errorf("user scope: %w", domain.ErrNotFound)
	}
	if err != nil {
		return mapPostgresError("lock existing user scope", err)
	}
	if revision < 0 {
		return fmt.Errorf("negative context revision: %w", domain.ErrInvariant)
	}
	return nil
}

func validateMemoryEmbedding(value MemoryEmbedding) (MemoryEmbedding, string, error) {
	for _, field := range []struct {
		name  string
		value string
	}{
		{name: "tenant id", value: value.TenantID},
		{name: "user id", value: value.UserID},
		{name: "memory id", value: value.MemoryID},
		{name: "embedding space", value: value.EmbeddingSpace},
		{name: "provider", value: value.Provider},
		{name: "model", value: value.Model},
		{name: "document version", value: value.DocumentVersion},
		{name: "query version", value: value.QueryVersion},
	} {
		if err := validateRequired(field.name, field.value); err != nil {
			return MemoryEmbedding{}, "", err
		}
	}
	contentHash, err := normalizeSHA256("content sha256", value.ContentSHA256)
	if err != nil {
		return MemoryEmbedding{}, "", err
	}
	modelFingerprint, err := normalizeSHA256("model fingerprint", value.ModelFingerprint)
	if err != nil {
		return MemoryEmbedding{}, "", err
	}
	vectorText, err := encodeVector(value.Vector)
	if err != nil {
		return MemoryEmbedding{}, "", fmt.Errorf("embedding vector: %w", err)
	}
	if value.CreatedAt.IsZero() {
		return MemoryEmbedding{}, "", fmt.Errorf("embedding created_at is required: %w", domain.ErrInvalid)
	}
	value.ContentSHA256 = contentHash
	value.ModelFingerprint = modelFingerprint
	value.CreatedAt = value.CreatedAt.UTC().Truncate(time.Microsecond)
	return value, vectorText, nil
}

const sha256Size = 32

func normalizeSHA256(name, value string) (string, error) {
	normalized := strings.ToLower(strings.TrimSpace(value))
	decoded, err := hex.DecodeString(normalized)
	if err != nil || len(decoded) != sha256Size {
		return "", fmt.Errorf("%s must be 64 hexadecimal characters: %w", name, domain.ErrInvalid)
	}
	return normalized, nil
}

func validateRequired(name, value string) error {
	if strings.TrimSpace(value) == "" {
		return fmt.Errorf("%s is required: %w", name, domain.ErrInvalid)
	}
	return nil
}

func encodeVector(vector []float32) (string, error) {
	if len(vector) != VectorDimension {
		return "", fmt.Errorf("dimension is %d, want %d: %w", len(vector), VectorDimension, domain.ErrInvalid)
	}
	result := make([]byte, 0, len(vector)*10)
	result = append(result, '[')
	normSquared := float64(0)
	for index, component := range vector {
		asFloat64 := float64(component)
		if math.IsNaN(asFloat64) || math.IsInf(asFloat64, 0) {
			return "", fmt.Errorf("component %d is not finite: %w", index, domain.ErrInvalid)
		}
		if index > 0 {
			result = append(result, ',')
		}
		result = strconv.AppendFloat(result, asFloat64, 'g', -1, 32)
		normSquared += asFloat64 * asFloat64
	}
	if normSquared == 0 {
		return "", fmt.Errorf("vector norm is zero: %w", domain.ErrInvalid)
	}
	result = append(result, ']')
	return string(result), nil
}
