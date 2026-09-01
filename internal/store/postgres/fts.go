package postgres

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/ksana-ai/agent-memory-system/internal/domain"
)

const (
	FTSTextSearchConfig = "simple"
	FTSQueryStrategy    = "to_tsvector_lexemes_or_v1"
	FTSRankFunction     = "ts_rank_cd_v1"
)

// FTSMetadata describes the non-sensitive PostgreSQL component facts needed
// to reproduce and compare retrieval runs. It intentionally contains no
// connection string, host, database name, or credentials.
type FTSMetadata struct {
	ServerVersionNum       string `json:"server_version_num"`
	SchemaMigrationVersion int    `json:"schema_migration_version"`
	TextSearchConfig       string `json:"text_search_config"`
	QueryStrategy          string `json:"query_strategy"`
	RankFunction           string `json:"rank_function"`
}

// Search performs tenant-scoped PostgreSQL full-text retrieval over reviewed
// cards. Query syntax is never accepted from the caller: PostgreSQL first
// tokenizes the parameter as plain text, then the resulting lexemes are safely
// quoted and joined into an OR tsquery.
func (s *Store) Search(ctx context.Context, tenantID, userID, query string, limit int, asOf time.Time) ([]domain.SearchHit, error) {
	if strings.TrimSpace(query) == "" || limit <= 0 {
		return []domain.SearchHit{}, nil
	}
	if err := s.ready(); err != nil {
		return nil, err
	}

	rows, err := s.pool.Query(ctx, `
		WITH query_lexemes AS (
			SELECT lexeme
			FROM unnest(
				tsvector_to_array(
					to_tsvector('pg_catalog.simple'::regconfig, $3)
				)
			) AS token(lexeme)
		),
		search_query AS (
			SELECT string_agg(quote_literal(lexeme), ' | ' ORDER BY lexeme)::tsquery AS value
			FROM query_lexemes
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
		       ts_rank_cd(card.search_document, search_query.value)::double precision
		FROM agent_memory.memory_cards AS card
		CROSS JOIN search_query
		WHERE search_query.value IS NOT NULL
		  AND card.tenant_id = $1
		  AND card.user_id = $2
		  AND card.status = 'active'
		  AND (card.expires_at IS NULL OR card.expires_at > $5)
		  AND card.search_document @@ search_query.value
		ORDER BY ts_rank_cd(card.search_document, search_query.value) DESC,
		         card.created_at DESC,
		         card.id ASC
		LIMIT $4`, tenantID, userID, query, limit, asOf)
	if err != nil {
		return nil, mapPostgresError("search memory cards", err)
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
		return nil, mapPostgresError("iterate memory search results", err)
	}
	return hits, nil
}

func (s *Store) FTSMetadata(ctx context.Context) (FTSMetadata, error) {
	if err := s.ready(); err != nil {
		return FTSMetadata{}, err
	}
	metadata := FTSMetadata{
		TextSearchConfig: FTSTextSearchConfig,
		QueryStrategy:    FTSQueryStrategy,
		RankFunction:     FTSRankFunction,
	}
	err := s.pool.QueryRow(ctx, `
		SELECT current_setting('server_version_num'),
		       COALESCE((
		           SELECT max(version)
		           FROM public.agent_memory_schema_version
		       ), 0)`).Scan(&metadata.ServerVersionNum, &metadata.SchemaMigrationVersion)
	if err != nil {
		return FTSMetadata{}, mapPostgresError("load PostgreSQL FTS metadata", err)
	}
	if metadata.SchemaMigrationVersion < 0 {
		return FTSMetadata{}, fmt.Errorf("negative schema migration version: %w", domain.ErrInvariant)
	}
	return metadata, nil
}
