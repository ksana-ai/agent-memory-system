package postgres

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/kai443/go-agent-memory-system/internal/domain"
)

const evaluationScopePrefix = "eval_"

// DeleteEvaluationScopeState removes the revision tombstone for an ephemeral
// evaluation scope after its lifecycle data has already been erased. It is
// intentionally unavailable through the domain Store interface: production
// scopes retain their revision across ForgetUser to prevent ABA.
func (s *Store) DeleteEvaluationScopeState(ctx context.Context, tenantID, userID string) error {
	if !strings.HasPrefix(tenantID, evaluationScopePrefix) || !strings.HasPrefix(userID, evaluationScopePrefix) {
		return fmt.Errorf("evaluation scope must use %q tenant and user prefixes: %w", evaluationScopePrefix, domain.ErrInvalid)
	}
	if err := s.ready(); err != nil {
		return err
	}

	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return fmt.Errorf("begin evaluation scope cleanup: %w", err)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()

	var state int
	err = tx.QueryRow(ctx, `
		SELECT 1
		FROM agent_memory.user_scope_state
		WHERE tenant_id = $1 AND user_id = $2
		FOR UPDATE`, tenantID, userID).Scan(&state)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil
	}
	if err != nil {
		return mapPostgresError("lock evaluation scope state", err)
	}

	var evidence, candidates, sources, cards, embeddings, projectionJobs, identities int64
	err = tx.QueryRow(ctx, `
		SELECT
			(SELECT count(*) FROM agent_memory.evidence_events
			 WHERE tenant_id = $1 AND user_id = $2),
			(SELECT count(*) FROM agent_memory.memory_candidates
			 WHERE tenant_id = $1 AND user_id = $2),
			(SELECT count(*) FROM agent_memory.candidate_source_events
			 WHERE tenant_id = $1 AND user_id = $2),
			(SELECT count(*) FROM agent_memory.memory_cards
			 WHERE tenant_id = $1 AND user_id = $2),
			(SELECT count(*) FROM agent_memory.memory_embeddings
			 WHERE tenant_id = $1 AND user_id = $2),
			(SELECT count(*) FROM agent_memory.embedding_projection_jobs
			 WHERE tenant_id = $1 AND user_id = $2),
			(SELECT count(*) FROM agent_memory.memory_identity_chains
			 WHERE tenant_id = $1 AND user_id = $2)`, tenantID, userID).Scan(
		&evidence,
		&candidates,
		&sources,
		&cards,
		&embeddings,
		&projectionJobs,
		&identities,
	)
	if err != nil {
		return mapPostgresError("inspect evaluation scope contents", err)
	}
	if evidence != 0 || candidates != 0 || sources != 0 || cards != 0 || embeddings != 0 || projectionJobs != 0 || identities != 0 {
		return fmt.Errorf(
			"evaluation scope still contains lifecycle data (evidence=%d candidates=%d sources=%d cards=%d embeddings=%d projection_jobs=%d identities=%d): %w",
			evidence,
			candidates,
			sources,
			cards,
			embeddings,
			projectionJobs,
			identities,
			domain.ErrConflict,
		)
	}

	commandTag, err := tx.Exec(ctx, `
		DELETE FROM agent_memory.user_scope_state
		WHERE tenant_id = $1 AND user_id = $2`, tenantID, userID)
	if err != nil {
		return mapPostgresError("delete evaluation scope state", err)
	}
	if commandTag.RowsAffected() != 1 {
		return fmt.Errorf("evaluation scope state disappeared during cleanup: %w", domain.ErrInvariant)
	}
	if err := tx.Commit(ctx); err != nil {
		return mapPostgresError("commit evaluation scope cleanup", err)
	}
	return nil
}
