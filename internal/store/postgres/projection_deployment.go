package postgres

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/ksana-ai/agent-memory-system/internal/domain"
)

// lockProjectionDeploymentShared freezes the projection deployment generation
// and target membership for a transaction. Callers must take this lock before
// any scope, candidate, target, or projection-job lock.
func lockProjectionDeploymentShared(ctx context.Context, tx pgx.Tx) (int64, error) {
	return lockProjectionDeployment(ctx, tx, "FOR SHARE")
}

// lockProjectionDeploymentExclusive serializes material target changes with
// approvals and reconciliation. Callers must take it before locking a target.
func lockProjectionDeploymentExclusive(ctx context.Context, tx pgx.Tx) (int64, error) {
	return lockProjectionDeployment(ctx, tx, "FOR UPDATE")
}

func lockProjectionDeployment(ctx context.Context, tx pgx.Tx, lockClause string) (int64, error) {
	var generation int64
	err := tx.QueryRow(ctx, `
		SELECT generation
		FROM agent_memory.embedding_projection_deployment
		WHERE singleton
		`+lockClause).Scan(&generation)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, fmt.Errorf("projection deployment singleton is missing: %w", domain.ErrInvariant)
	}
	if err != nil {
		return 0, mapProjectionPostgresError("lock projection deployment", err)
	}
	if generation < 0 {
		return 0, fmt.Errorf("projection deployment generation is negative: %w", domain.ErrInvariant)
	}
	return generation, nil
}

// advanceProjectionDeploymentGeneration records one material target change.
// The transaction must already hold the singleton row FOR UPDATE.
func advanceProjectionDeploymentGeneration(ctx context.Context, tx pgx.Tx) (int64, error) {
	var generation int64
	err := tx.QueryRow(ctx, `
		UPDATE agent_memory.embedding_projection_deployment
		SET generation = generation + 1,
		    updated_at = clock_timestamp()
		WHERE singleton
		RETURNING generation`).Scan(&generation)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, fmt.Errorf("projection deployment singleton disappeared: %w", domain.ErrInvariant)
	}
	if err != nil {
		return 0, mapProjectionPostgresError("advance projection deployment generation", err)
	}
	if generation <= 0 {
		return 0, fmt.Errorf("projection deployment generation did not advance: %w", domain.ErrInvariant)
	}
	return generation, nil
}
