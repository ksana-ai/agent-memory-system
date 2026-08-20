package postgres

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/kai443/go-agent-memory-system/internal/domain"
)

// ServingVectorExpectation pins a retrieval request to one observed
// deployment. Both fields are compare-and-swap preconditions: callers must
// refresh them from CurrentServingProjection after a promotion.
type ServingVectorExpectation struct {
	EmbeddingSpace string
	Generation     int64
}

const servingVectorSearchSQL = `
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
	FROM agent_memory.embedding_projection_targets AS target
	JOIN agent_memory.embedding_projection_jobs AS job
	  ON job.embedding_space = target.embedding_space
	JOIN agent_memory.memory_cards AS card
	  ON card.tenant_id = job.tenant_id
	 AND card.user_id = job.user_id
	 AND card.id = job.memory_id
	JOIN agent_memory.memory_embeddings AS embedding
	  ON embedding.tenant_id = card.tenant_id
	 AND embedding.user_id = card.user_id
	 AND embedding.memory_id = card.id
	 AND embedding.embedding_space = target.embedding_space
	CROSS JOIN query_embedding
	WHERE target.embedding_space = $3
	  AND target.state = 'serving'
	  AND target.enqueue_new
	  AND job.tenant_id = $1
	  AND job.user_id = $2
	  AND job.state = 'succeeded'
	  AND job.expected_memory_version = card.version
	  AND card.tenant_id = $1
	  AND card.user_id = $2
	  AND card.status = 'active'
	  AND (card.expires_at IS NULL OR card.expires_at > $6)
	  AND EXISTS (
	      SELECT 1
	      FROM agent_memory.candidate_source_events AS required_source
	      WHERE required_source.tenant_id = card.tenant_id
	        AND required_source.user_id = card.user_id
	        AND required_source.candidate_id = card.candidate_id
	  )
	ORDER BY embedding.embedding <=> query_embedding.value ASC,
	         card.created_at DESC,
	         card.id ASC
	LIMIT $5`

// SearchServingVector performs an exact cosine search against only the
// deployment that is currently serving. The deployment singleton and serving
// target are held with shared row locks for the complete short transaction,
// so a promotion cannot expose a mixed generation or target to this query.
//
// Projection readiness is enforced in the ranking SQL itself. A card is
// eligible only when its serving-space job succeeded for the current card
// version and the matching embedding and at least one provenance source still
// exist. Shadow, blocked, retired, stale, expired, and cross-scope rows never
// enter the result set.
func (s *Store) SearchServingVector(
	ctx context.Context,
	tenantID, userID string,
	expected ServingVectorExpectation,
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
	if err := validateServingVectorExpectation(expected); err != nil {
		return nil, err
	}

	var queryText string
	if limit > 0 {
		var err error
		queryText, err = encodeVector(query)
		if err != nil {
			return nil, fmt.Errorf("serving query vector: %w", err)
		}
	}
	if err := s.ready(); err != nil {
		return nil, err
	}

	tx, err := s.pool.BeginTx(ctx, servingVectorTxOptions())
	if err != nil {
		return nil, mapProjectionPostgresError("begin serving vector search", err)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()

	generation, err := lockProjectionDeploymentShared(ctx, tx)
	if err != nil {
		return nil, err
	}
	if generation != expected.Generation {
		return nil, fmt.Errorf("serving projection generation differs from expectation: %w", domain.ErrConflict)
	}

	target, found, err := readCurrentServingProjectionTarget(ctx, tx, true)
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, fmt.Errorf("serving projection target: %w", domain.ErrNotFound)
	}
	if target.Space.ID != expected.EmbeddingSpace || !target.EnqueueNew {
		return nil, fmt.Errorf("serving projection target differs from expectation: %w", domain.ErrConflict)
	}

	if limit <= 0 {
		if err := tx.Commit(ctx); err != nil {
			return nil, mapProjectionPostgresError("commit empty serving vector search", err)
		}
		return []domain.SearchHit{}, nil
	}

	rows, err := tx.Query(ctx, servingVectorSearchSQL,
		tenantID, userID, expected.EmbeddingSpace, queryText, limit, asOf,
	)
	if err != nil {
		return nil, mapProjectionPostgresError("search serving memory vectors", err)
	}

	hits := make([]domain.SearchHit, 0)
	for rows.Next() {
		memory, score, scanErr := scanMemoryWithScore(rows)
		if scanErr != nil {
			rows.Close()
			return nil, mapProjectionPostgresError("scan serving memory vector", scanErr)
		}
		hits = append(hits, domain.SearchHit{Memory: memory, Score: score})
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return nil, mapProjectionPostgresError("iterate serving memory vector results", err)
	}
	rows.Close()

	if err := tx.Commit(ctx); err != nil {
		return nil, mapProjectionPostgresError("commit serving vector search", err)
	}
	return hits, nil
}

func servingVectorTxOptions() pgx.TxOptions {
	// READ COMMITTED is explicit so the state observed after waiting for a
	// concurrent promotion is the newly committed deployment, never a stale
	// session-default snapshot. Row locks require a read-write transaction.
	return pgx.TxOptions{IsoLevel: pgx.ReadCommitted}
}

func validateServingVectorExpectation(expected ServingVectorExpectation) error {
	if err := validateProjectionIdentifier("serving embedding space", expected.EmbeddingSpace); err != nil {
		return err
	}
	if expected.Generation < 0 {
		return fmt.Errorf("serving projection generation must be nonnegative: %w", domain.ErrInvalid)
	}
	return nil
}
