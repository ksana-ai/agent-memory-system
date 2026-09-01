package postgres

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/ksana-ai/agent-memory-system/internal/domain"
	"github.com/ksana-ai/agent-memory-system/internal/embedding"
)

const (
	defaultProjectionMaxAttempts = 5
	maxProjectionAttempts        = 100
	maxProjectionClaimLimit      = 100
	maxProjectionLease           = 24 * time.Hour
)

// ErrProjectionLeaseLost means that a worker no longer owns the durable
// right to mutate a projection job. It intentionally covers an expired or
// reclaimed lease as well as lifecycle deletion, supersession, and target
// deactivation: in every case the old worker must discard its result.
var ErrProjectionLeaseLost = errors.New("projection lease lost")

// ClaimProjectionJobsCommand claims runnable work for one immutable embedding
// space. Provider endpoints and credentials deliberately remain outside the
// repository boundary.
type ClaimProjectionJobsCommand struct {
	EmbeddingSpace string
	LeaseOwner     string
	LeaseDuration  time.Duration
	MaxAttempts    int
	Limit          int
}

// ProjectionWorkItem is the immutable input snapshot associated with a lease.
// Callers may perform provider I/O only after ClaimProjectionJobs commits.
type ProjectionWorkItem struct {
	Job            ProjectionJob
	Target         ProjectionTarget
	Memory         domain.MemoryCard
	Document       string
	DocumentSHA256 string
}

// FinalizeProjectionJobCommand carries only a fenced lease token and the
// provider-produced vector. DocumentSHA256 binds that vector to the exact
// document returned by ClaimProjectionJobs.
type FinalizeProjectionJobCommand struct {
	JobID          int64
	TenantID       string
	UserID         string
	EmbeddingSpace string
	LeaseOwner     string
	LeaseVersion   int64
	DocumentSHA256 string
	Vector         []float32
}

// FinalizeProjectionJobResult distinguishes durable vector changes from the
// narrower serving-visible changes that advance context_revision.
type FinalizeProjectionJobResult struct {
	Job              ProjectionJob
	EmbeddingChanged bool
	RevisionAdvanced bool
	Cancelled        bool
	Requeued         bool
}

type RetryProjectionJobCommand struct {
	JobID        int64
	TenantID     string
	UserID       string
	LeaseOwner   string
	LeaseVersion int64
	ErrorCode    ProjectionErrorCode
	RetryAfter   time.Duration
}

type DeadLetterProjectionJobCommand struct {
	JobID        int64
	TenantID     string
	UserID       string
	LeaseOwner   string
	LeaseVersion int64
	ErrorCode    ProjectionErrorCode
}

type CancelProjectionJobCommand struct {
	JobID        int64
	TenantID     string
	UserID       string
	LeaseOwner   string
	LeaseVersion int64
}

// ClaimProjectionJobs atomically leases available pending/retry work and
// reclaims expired leases. FOR UPDATE SKIP LOCKED lets workers share a queue
// without duplicate live ownership. Lifecycle validity is evaluated in the
// same SQL statement that establishes the lease.
func (s *Store) ClaimProjectionJobs(ctx context.Context, command ClaimProjectionJobsCommand) ([]ProjectionWorkItem, error) {
	normalized, err := validateClaimProjectionJobs(command)
	if err != nil {
		return nil, err
	}
	if err := s.ready(); err != nil {
		return nil, err
	}

	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return nil, mapProjectionPostgresError("begin projection job claim", err)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()

	// Keep deployment state stable until the claim commits. FOR SHARE is
	// compatible across workers and conflicts with target state updates; KEY
	// SHARE would not block a non-key state/enqueue update.
	target, err := scanProjectionTarget(tx.QueryRow(ctx, projectionTargetSelect+`
		WHERE target.embedding_space = $1
		FOR SHARE OF target`, normalized.EmbeddingSpace))
	if errors.Is(err, domain.ErrNotFound) {
		return nil, fmt.Errorf("projection target: %w", domain.ErrNotFound)
	}
	if err != nil {
		return nil, err
	}
	operationAt, err := projectionDatabaseTime(ctx, tx)
	if err != nil {
		return nil, err
	}
	leaseUntil := canonicalProjectionTime(operationAt.Add(normalized.LeaseDuration))
	// Permanently invalid runnable work must not accumulate forever. A live
	// lease remains fenced to its owner; if that owner disappears, the same row
	// is cancelled here after its lease expires. Blocked targets intentionally
	// keep their jobs because blocking is a reversible operational pause.
	if _, err := tx.Exec(ctx, `
		UPDATE agent_memory.embedding_projection_jobs AS job
		SET state = 'cancelled', lease_owner = NULL, lease_until = NULL,
		    updated_at = $2, completed_at = $2
		WHERE job.embedding_space = $1
		  AND job.created_at <= $2
		  AND (
			job.state IN ('pending', 'retry')
			OR (job.state = 'leased' AND job.lease_until <= $2)
		  )
		  AND (
			$3::boolean
			OR EXISTS (
				SELECT 1
				FROM agent_memory.memory_cards AS card
				WHERE card.tenant_id = job.tenant_id
				  AND card.user_id = job.user_id
				  AND card.id = job.memory_id
				  AND (
					card.status <> 'active'
					OR card.version <> job.expected_memory_version
					OR (card.expires_at IS NOT NULL AND card.expires_at <= $2)
				  )
			)
		  )`,
		normalized.EmbeddingSpace,
		operationAt,
		target.State == ProjectionTargetRetired,
	); err != nil {
		return nil, mapProjectionPostgresError("cancel invalid projection jobs", err)
	}
	if target.State == ProjectionTargetBlocked || target.State == ProjectionTargetRetired {
		if err := tx.Commit(ctx); err != nil {
			return nil, mapProjectionPostgresError("commit disabled projection claim", err)
		}
		return []ProjectionWorkItem{}, nil
	}
	if target.Space.DocumentVersion != embedding.MemoryCardDocumentVersion {
		return nil, fmt.Errorf("projection document version is unsupported: %w", domain.ErrInvariant)
	}
	// A provider call that crashes the process never reaches Retry/DeadLetter.
	// Bound those expired-lease deliveries here so one poison job cannot be
	// reclaimed forever and starve the v1 single-item queue.
	if _, err := tx.Exec(ctx, `
		UPDATE agent_memory.embedding_projection_jobs AS job
		SET state = 'dead', lease_owner = NULL, lease_until = NULL,
		    last_error_code = 'attempts_exhausted', last_error_at = $2,
		    updated_at = $2, completed_at = $2
		WHERE job.embedding_space = $1
		  AND job.attempt_count >= $3
		  AND (
		    job.state = 'retry'
		    OR (job.state = 'leased' AND job.lease_until <= $2)
		  )`, normalized.EmbeddingSpace, operationAt, normalized.MaxAttempts); err != nil {
		return nil, mapProjectionPostgresError("dead-letter exhausted projection jobs", err)
	}

	rows, err := tx.Query(ctx, `
		WITH eligible AS MATERIALIZED (
			SELECT job.id
			FROM agent_memory.embedding_projection_jobs AS job
			JOIN agent_memory.memory_cards AS card
			  ON card.tenant_id = job.tenant_id
			 AND card.user_id = job.user_id
			 AND card.id = job.memory_id
			WHERE job.embedding_space = $1
			  AND (
				(job.state IN ('pending', 'retry') AND job.available_at <= $2)
				OR (job.state = 'leased' AND job.lease_until <= $2)
			  )
			  AND card.status = 'active'
			  AND card.version = job.expected_memory_version
			  AND (card.expires_at IS NULL OR card.expires_at > $2)
			ORDER BY job.available_at, job.created_at, job.id
			FOR UPDATE OF job SKIP LOCKED
			LIMIT $3
		), claimed AS (
			UPDATE agent_memory.embedding_projection_jobs AS job
			SET state = 'leased',
			    attempt_count = job.attempt_count + 1,
			    lease_owner = $4,
			    lease_version = job.lease_version + 1,
			    lease_until = $5,
			    updated_at = $2,
			    completed_at = NULL
			FROM eligible
			WHERE job.id = eligible.id
			RETURNING job.id, job.tenant_id, job.user_id, job.memory_id,
			          job.embedding_space, job.expected_memory_version, job.state,
			          job.attempt_count, job.available_at, job.lease_owner,
			          job.lease_version, job.lease_until, job.last_error_code,
			          job.last_error_at, job.created_at, job.updated_at,
			          job.completed_at
		)
		SELECT claimed.id, claimed.tenant_id, claimed.user_id, claimed.memory_id,
		       claimed.embedding_space, claimed.expected_memory_version,
		       claimed.state, claimed.attempt_count, claimed.available_at,
		       COALESCE(claimed.lease_owner, ''), claimed.lease_version,
		       claimed.lease_until, COALESCE(claimed.last_error_code, ''),
		       claimed.last_error_at, claimed.created_at, claimed.updated_at,
		       claimed.completed_at,
		       card.id, card.candidate_id, card.tenant_id, card.user_id, card.kind,
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
		       card.superseded_at
		FROM claimed
		JOIN agent_memory.memory_cards AS card
		  ON card.tenant_id = claimed.tenant_id
		 AND card.user_id = claimed.user_id
		 AND card.id = claimed.memory_id
		ORDER BY claimed.available_at, claimed.created_at, claimed.id`,
		normalized.EmbeddingSpace,
		operationAt,
		normalized.Limit,
		normalized.LeaseOwner,
		leaseUntil,
	)
	if err != nil {
		return nil, mapProjectionPostgresError("claim projection jobs", err)
	}
	defer rows.Close()

	items := make([]ProjectionWorkItem, 0, normalized.Limit)
	for rows.Next() {
		job, memory, scanErr := scanProjectionWorkItem(rows)
		if scanErr != nil {
			return nil, scanErr
		}
		document := embedding.MemoryCardDocumentV1(memory)
		items = append(items, ProjectionWorkItem{
			Job:            job,
			Target:         target,
			Memory:         memory,
			Document:       document,
			DocumentSHA256: embedding.DocumentSHA256(document),
		})
	}
	if err := rows.Err(); err != nil {
		return nil, mapProjectionPostgresError("iterate claimed projection jobs", err)
	}
	rows.Close()
	// SQL ordering is explicit, but preserve it defensively if a future query
	// plan changes the data-modifying CTE output order.
	sort.SliceStable(items, func(left, right int) bool {
		if !items[left].Job.AvailableAt.Equal(items[right].Job.AvailableAt) {
			return items[left].Job.AvailableAt.Before(items[right].Job.AvailableAt)
		}
		if !items[left].Job.CreatedAt.Equal(items[right].Job.CreatedAt) {
			return items[left].Job.CreatedAt.Before(items[right].Job.CreatedAt)
		}
		return items[left].Job.ID < items[right].Job.ID
	})
	if err := tx.Commit(ctx); err != nil {
		return nil, mapProjectionPostgresError("commit projection job claim", err)
	}
	return items, nil
}

// FinalizeProjectionJob validates a lease and the current lifecycle snapshot,
// then writes the vector and succeeded acknowledgement atomically. A shadow
// vector is durable but does not alter serving context_revision.
func (s *Store) FinalizeProjectionJob(ctx context.Context, command FinalizeProjectionJobCommand) (FinalizeProjectionJobResult, error) {
	normalized, vectorText, err := validateFinalizeProjectionJob(command)
	if err != nil {
		return FinalizeProjectionJobResult{}, err
	}
	if err := s.ready(); err != nil {
		return FinalizeProjectionJobResult{}, err
	}

	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return FinalizeProjectionJobResult{}, mapProjectionPostgresError("begin projection finalization", err)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()

	if err := lockProjectionScope(ctx, tx, normalized.TenantID, normalized.UserID); err != nil {
		return FinalizeProjectionJobResult{}, err
	}
	target, err := scanProjectionTarget(tx.QueryRow(ctx, projectionTargetSelect+`
		WHERE target.embedding_space = $1
		FOR SHARE OF target`, normalized.EmbeddingSpace))
	if err != nil {
		if errors.Is(err, domain.ErrNotFound) {
			return FinalizeProjectionJobResult{}, projectionLeaseLost("projection target disappeared")
		}
		return FinalizeProjectionJobResult{}, err
	}
	job, err := readProjectionJobForUpdate(ctx, tx, normalized.JobID)
	if err != nil {
		return FinalizeProjectionJobResult{}, err
	}
	if err := verifyProjectionLeaseIdentity(
		job,
		normalized.TenantID,
		normalized.UserID,
		normalized.EmbeddingSpace,
		normalized.LeaseOwner,
		normalized.LeaseVersion,
	); err != nil {
		return FinalizeProjectionJobResult{}, err
	}
	if target.State == ProjectionTargetRetired {
		operationAt, timeErr := projectionDatabaseTime(ctx, tx)
		if timeErr != nil {
			return FinalizeProjectionJobResult{}, timeErr
		}
		if err := verifyProjectionLeaseDeadline(job, operationAt); err != nil {
			return FinalizeProjectionJobResult{}, err
		}
		cancelledJob, transitionErr := transitionLockedProjectionLease(ctx, tx, projectionLeaseTransition{
			JobID: job.ID, TenantID: job.TenantID, UserID: job.UserID,
			LeaseOwner: normalized.LeaseOwner, LeaseVersion: normalized.LeaseVersion,
			At: operationAt, State: ProjectionJobCancelled,
			AvailableAt: operationAt,
		})
		if transitionErr != nil {
			return FinalizeProjectionJobResult{}, transitionErr
		}
		if err := tx.Commit(ctx); err != nil {
			return FinalizeProjectionJobResult{}, mapProjectionPostgresError("commit retired projection cancellation", err)
		}
		return FinalizeProjectionJobResult{Job: cancelledJob, Cancelled: true}, nil
	}
	if target.State == ProjectionTargetBlocked {
		operationAt, timeErr := projectionDatabaseTime(ctx, tx)
		if timeErr != nil {
			return FinalizeProjectionJobResult{}, timeErr
		}
		if err := verifyProjectionLeaseDeadline(job, operationAt); err != nil {
			return FinalizeProjectionJobResult{}, err
		}
		retryJob, transitionErr := transitionLockedProjectionLease(ctx, tx, projectionLeaseTransition{
			JobID: job.ID, TenantID: job.TenantID, UserID: job.UserID,
			LeaseOwner: normalized.LeaseOwner, LeaseVersion: normalized.LeaseVersion,
			At: operationAt, State: ProjectionJobRetry,
			AvailableAt: operationAt,
		})
		if transitionErr != nil {
			return FinalizeProjectionJobResult{}, transitionErr
		}
		if err := tx.Commit(ctx); err != nil {
			return FinalizeProjectionJobResult{}, mapProjectionPostgresError("commit blocked projection requeue", err)
		}
		return FinalizeProjectionJobResult{Job: retryJob, Requeued: true}, nil
	}
	if target.State != ProjectionTargetShadow && target.State != ProjectionTargetServing {
		return FinalizeProjectionJobResult{}, fmt.Errorf("stored projection target state is invalid: %w", domain.ErrInvariant)
	}
	if target.Space.DocumentVersion != embedding.MemoryCardDocumentVersion {
		return FinalizeProjectionJobResult{}, fmt.Errorf("projection document version is unsupported: %w", domain.ErrInvariant)
	}

	memory, err := readProjectionMemoryForShare(ctx, tx, job.TenantID, job.UserID, job.MemoryID)
	if err != nil {
		if errors.Is(err, domain.ErrNotFound) {
			return FinalizeProjectionJobResult{}, projectionLeaseLost("projection memory disappeared")
		}
		return FinalizeProjectionJobResult{}, err
	}
	operationAt, err := projectionDatabaseTime(ctx, tx)
	if err != nil {
		return FinalizeProjectionJobResult{}, err
	}
	if err := verifyProjectionLeaseDeadline(job, operationAt); err != nil {
		return FinalizeProjectionJobResult{}, err
	}
	if memory.Status != domain.MemoryActive ||
		memory.Version != job.ExpectedMemoryVersion ||
		(memory.ExpiresAt != nil && !memory.ExpiresAt.After(operationAt)) {
		cancelledJob, transitionErr := transitionLockedProjectionLease(ctx, tx, projectionLeaseTransition{
			JobID: job.ID, TenantID: job.TenantID, UserID: job.UserID,
			LeaseOwner: normalized.LeaseOwner, LeaseVersion: normalized.LeaseVersion,
			At: operationAt, State: ProjectionJobCancelled,
			AvailableAt: operationAt,
		})
		if transitionErr != nil {
			return FinalizeProjectionJobResult{}, transitionErr
		}
		if err := tx.Commit(ctx); err != nil {
			return FinalizeProjectionJobResult{}, mapProjectionPostgresError("commit invalid projection cancellation", err)
		}
		return FinalizeProjectionJobResult{Job: cancelledJob, Cancelled: true}, nil
	}
	documentSHA256 := embedding.MemoryCardDocumentV1SHA256(memory)
	if documentSHA256 != normalized.DocumentSHA256 {
		return FinalizeProjectionJobResult{}, fmt.Errorf("projection document changed: %w", domain.ErrConflict)
	}

	var existingContentSHA256 string
	err = tx.QueryRow(ctx, `
		SELECT content_sha256
		FROM agent_memory.memory_embeddings
		WHERE tenant_id = $1 AND user_id = $2 AND memory_id = $3
		  AND embedding_space = $4
		FOR UPDATE`, job.TenantID, job.UserID, job.MemoryID, job.EmbeddingSpace).Scan(&existingContentSHA256)
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return FinalizeProjectionJobResult{}, mapProjectionPostgresError("load projection embedding", err)
	}
	if err == nil && existingContentSHA256 != documentSHA256 {
		return FinalizeProjectionJobResult{}, fmt.Errorf("projection embedding content conflicts with current document: %w", domain.ErrConflict)
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
		job.TenantID,
		job.UserID,
		job.MemoryID,
		job.EmbeddingSpace,
		target.Space.Provider,
		target.Space.Model,
		target.Space.DocumentVersion,
		target.Space.QueryVersion,
		target.Space.ModelFingerprint,
		documentSHA256,
		vectorText,
		operationAt,
	)
	if err != nil {
		return FinalizeProjectionJobResult{}, mapProjectionPostgresError("upsert finalized projection embedding", err)
	}
	if commandTag.RowsAffected() > 1 {
		return FinalizeProjectionJobResult{}, fmt.Errorf("projection embedding affected an invalid row count: %w", domain.ErrInvariant)
	}
	embeddingChanged := commandTag.RowsAffected() == 1

	completedJob, err := scanProjectionJob(tx.QueryRow(ctx, `
		UPDATE agent_memory.embedding_projection_jobs
		SET state = 'succeeded', lease_owner = NULL, lease_until = NULL,
		    updated_at = $6, completed_at = $6
		WHERE id = $1 AND tenant_id = $2 AND user_id = $3
		  AND state = 'leased' AND lease_owner = $4 AND lease_version = $5
		  AND lease_until > $6
		RETURNING id, tenant_id, user_id, memory_id, embedding_space,
		          expected_memory_version, state, attempt_count, available_at,
		          COALESCE(lease_owner, ''), lease_version, lease_until,
		          COALESCE(last_error_code, ''), last_error_at,
		          created_at, updated_at, completed_at`,
		normalized.JobID,
		normalized.TenantID,
		normalized.UserID,
		normalized.LeaseOwner,
		normalized.LeaseVersion,
		operationAt,
	))
	if err != nil {
		if errors.Is(err, domain.ErrNotFound) {
			return FinalizeProjectionJobResult{}, projectionLeaseLost("projection lease changed during finalization")
		}
		return FinalizeProjectionJobResult{}, err
	}

	revisionAdvanced := false
	if embeddingChanged && target.State == ProjectionTargetServing {
		commandTag, err = tx.Exec(ctx, `
			UPDATE agent_memory.user_scope_state
			SET context_revision = context_revision + 1,
			    updated_at = $3
			WHERE tenant_id = $1 AND user_id = $2`,
			normalized.TenantID,
			normalized.UserID,
			operationAt,
		)
		if err != nil {
			return FinalizeProjectionJobResult{}, mapProjectionPostgresError("advance finalized projection revision", err)
		}
		if commandTag.RowsAffected() != 1 {
			return FinalizeProjectionJobResult{}, fmt.Errorf("projection scope disappeared during finalization: %w", domain.ErrInvariant)
		}
		revisionAdvanced = true
	}
	if err := tx.Commit(ctx); err != nil {
		return FinalizeProjectionJobResult{}, mapProjectionPostgresError("commit projection finalization", err)
	}
	return FinalizeProjectionJobResult{
		Job:              completedJob,
		EmbeddingChanged: embeddingChanged,
		RevisionAdvanced: revisionAdvanced,
	}, nil
}

// RetryProjectionJob releases a fenced lease to a future runnable time while
// persisting only a stable redacted error code.
func (s *Store) RetryProjectionJob(ctx context.Context, command RetryProjectionJobCommand) (ProjectionJob, error) {
	normalized, err := validateRetryProjectionJob(command)
	if err != nil {
		return ProjectionJob{}, err
	}
	return s.transitionProjectionLease(ctx, projectionLeaseTransition{
		JobID: normalized.JobID, TenantID: normalized.TenantID, UserID: normalized.UserID,
		LeaseOwner: normalized.LeaseOwner, LeaseVersion: normalized.LeaseVersion,
		State: ProjectionJobRetry, Backoff: normalized.RetryAfter,
		ErrorCode: normalized.ErrorCode,
	})
}

// DeadLetterProjectionJob terminally records a fenced failure using only the
// closed ProjectionErrorCode vocabulary.
func (s *Store) DeadLetterProjectionJob(ctx context.Context, command DeadLetterProjectionJobCommand) (ProjectionJob, error) {
	normalized, err := validateDeadLetterProjectionJob(command)
	if err != nil {
		return ProjectionJob{}, err
	}
	return s.transitionProjectionLease(ctx, projectionLeaseTransition{
		JobID: normalized.JobID, TenantID: normalized.TenantID, UserID: normalized.UserID,
		LeaseOwner: normalized.LeaseOwner, LeaseVersion: normalized.LeaseVersion,
		State: ProjectionJobDead, ErrorCode: normalized.ErrorCode,
	})
}

// CancelProjectionJob terminally cancels a fenced lease without inventing a
// provider error. Any earlier redacted failure remains as attempt history.
func (s *Store) CancelProjectionJob(ctx context.Context, command CancelProjectionJobCommand) (ProjectionJob, error) {
	normalized, err := validateCancelProjectionJob(command)
	if err != nil {
		return ProjectionJob{}, err
	}
	return s.transitionProjectionLease(ctx, projectionLeaseTransition{
		JobID: normalized.JobID, TenantID: normalized.TenantID, UserID: normalized.UserID,
		LeaseOwner: normalized.LeaseOwner, LeaseVersion: normalized.LeaseVersion,
		State: ProjectionJobCancelled,
	})
}

type projectionLeaseTransition struct {
	JobID        int64
	TenantID     string
	UserID       string
	LeaseOwner   string
	LeaseVersion int64
	At           time.Time
	State        ProjectionJobState
	AvailableAt  time.Time
	Backoff      time.Duration
	ErrorCode    ProjectionErrorCode
}

func (s *Store) transitionProjectionLease(ctx context.Context, transition projectionLeaseTransition) (ProjectionJob, error) {
	if err := s.ready(); err != nil {
		return ProjectionJob{}, err
	}
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return ProjectionJob{}, mapProjectionPostgresError("begin projection lease transition", err)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	if err := lockProjectionScope(ctx, tx, transition.TenantID, transition.UserID); err != nil {
		return ProjectionJob{}, err
	}
	leasedJob, err := readProjectionJobForUpdate(ctx, tx, transition.JobID)
	if err != nil {
		return ProjectionJob{}, err
	}
	if err := verifyProjectionLeaseIdentity(
		leasedJob,
		transition.TenantID,
		transition.UserID,
		"",
		transition.LeaseOwner,
		transition.LeaseVersion,
	); err != nil {
		return ProjectionJob{}, err
	}
	operationAt, err := projectionDatabaseTime(ctx, tx)
	if err != nil {
		return ProjectionJob{}, err
	}
	if err := verifyProjectionLeaseDeadline(leasedJob, operationAt); err != nil {
		return ProjectionJob{}, err
	}
	transition.At = operationAt
	transition.AvailableAt = operationAt
	if transition.State == ProjectionJobRetry {
		transition.AvailableAt = canonicalProjectionTime(operationAt.Add(transition.Backoff))
	}
	job, err := transitionLockedProjectionLease(ctx, tx, transition)
	if err != nil {
		return ProjectionJob{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return ProjectionJob{}, mapProjectionPostgresError("commit projection lease transition", err)
	}
	return job, nil
}

func transitionLockedProjectionLease(ctx context.Context, tx pgx.Tx, transition projectionLeaseTransition) (ProjectionJob, error) {
	var completedAt any
	if transition.State == ProjectionJobDead || transition.State == ProjectionJobCancelled {
		completedAt = transition.At
	}
	var lastErrorCode any
	var lastErrorAt any
	if transition.ErrorCode != "" {
		lastErrorCode = string(transition.ErrorCode)
		lastErrorAt = transition.At
	}
	job, err := scanProjectionJob(tx.QueryRow(ctx, `
		UPDATE agent_memory.embedding_projection_jobs
		SET state = $7, available_at = $8, lease_owner = NULL,
		    lease_until = NULL,
		    last_error_code = COALESCE($9::text, last_error_code),
		    last_error_at = COALESCE($10::timestamptz, last_error_at),
		    updated_at = $6, completed_at = $11
		WHERE id = $1 AND tenant_id = $2 AND user_id = $3
		  AND state = 'leased' AND lease_owner = $4 AND lease_version = $5
		  AND lease_until > $6
		RETURNING id, tenant_id, user_id, memory_id, embedding_space,
		          expected_memory_version, state, attempt_count, available_at,
		          COALESCE(lease_owner, ''), lease_version, lease_until,
		          COALESCE(last_error_code, ''), last_error_at,
		          created_at, updated_at, completed_at`,
		transition.JobID,
		transition.TenantID,
		transition.UserID,
		transition.LeaseOwner,
		transition.LeaseVersion,
		transition.At,
		string(transition.State),
		transition.AvailableAt,
		lastErrorCode,
		lastErrorAt,
		completedAt,
	))
	if err != nil {
		if errors.Is(err, domain.ErrNotFound) {
			return ProjectionJob{}, projectionLeaseLost("projection lease cannot transition")
		}
		return ProjectionJob{}, err
	}
	return job, nil
}

func scanProjectionWorkItem(row rowScanner) (ProjectionJob, domain.MemoryCard, error) {
	var job ProjectionJob
	var jobState, lastErrorCode string
	var jobLeaseUntil, jobLastErrorAt, jobCompletedAt pgOptionalTime
	var memory domain.MemoryCard
	var memoryKind, memoryStatus string
	var memoryExpiresAt, memorySupersededAt pgOptionalTime
	if err := row.Scan(
		&job.ID, &job.TenantID, &job.UserID, &job.MemoryID, &job.EmbeddingSpace,
		&job.ExpectedMemoryVersion, &jobState, &job.AttemptCount, &job.AvailableAt,
		&job.LeaseOwner, &job.LeaseVersion, &jobLeaseUntil, &lastErrorCode,
		&jobLastErrorAt, &job.CreatedAt, &job.UpdatedAt, &jobCompletedAt,
		&memory.ID, &memory.CandidateID, &memory.TenantID, &memory.UserID,
		&memoryKind, &memory.Category, &memory.Key, &memory.Value, &memory.Person,
		&memory.Relationship, &memory.Backstory, &memory.SourceEventIDs,
		&memory.Version, &memoryStatus, &memory.CreatedAt, &memoryExpiresAt,
		&memorySupersededAt,
	); err != nil {
		return ProjectionJob{}, domain.MemoryCard{}, mapProjectionPostgresError("scan projection work item", err)
	}
	job.State = ProjectionJobState(jobState)
	job.LastErrorCode = ProjectionErrorCode(lastErrorCode)
	job.LeaseUntil = jobLeaseUntil.pointer()
	job.LastErrorAt = jobLastErrorAt.pointer()
	job.CompletedAt = jobCompletedAt.pointer()
	job.AvailableAt = canonicalProjectionTime(job.AvailableAt)
	job.CreatedAt = canonicalProjectionTime(job.CreatedAt)
	job.UpdatedAt = canonicalProjectionTime(job.UpdatedAt)
	if err := validateStoredProjectionJob(job); err != nil {
		return ProjectionJob{}, domain.MemoryCard{}, err
	}
	memory.Kind = domain.MemoryKind(memoryKind)
	memory.Status = domain.MemoryStatus(memoryStatus)
	memory.CreatedAt = canonicalProjectionTime(memory.CreatedAt)
	memory.ExpiresAt = memoryExpiresAt.pointer()
	memory.SupersededAt = memorySupersededAt.pointer()
	return job, memory, nil
}

// pgOptionalTime is a small scanner-compatible alias that avoids making the
// work-item scanner depend on pgtype details in its public shape.
type pgOptionalTime struct {
	Time  time.Time
	Valid bool
}

func (value *pgOptionalTime) Scan(src any) error {
	switch typed := src.(type) {
	case nil:
		value.Time = time.Time{}
		value.Valid = false
		return nil
	case time.Time:
		value.Time = typed
		value.Valid = true
		return nil
	default:
		return fmt.Errorf("optional projection timestamp has an invalid database type")
	}
}

func (value pgOptionalTime) pointer() *time.Time {
	if !value.Valid {
		return nil
	}
	result := canonicalProjectionTime(value.Time)
	return &result
}

func readProjectionJobForUpdate(ctx context.Context, tx pgx.Tx, jobID int64) (ProjectionJob, error) {
	job, err := scanProjectionJob(tx.QueryRow(ctx, `
		SELECT id, tenant_id, user_id, memory_id, embedding_space,
		       expected_memory_version, state, attempt_count, available_at,
		       COALESCE(lease_owner, ''), lease_version, lease_until,
		       COALESCE(last_error_code, ''), last_error_at,
		       created_at, updated_at, completed_at
		FROM agent_memory.embedding_projection_jobs
		WHERE id = $1
		FOR UPDATE`, jobID))
	if errors.Is(err, domain.ErrNotFound) {
		return ProjectionJob{}, projectionLeaseLost("projection job disappeared")
	}
	return job, err
}

func readProjectionMemoryForShare(ctx context.Context, tx pgx.Tx, tenantID, userID, memoryID string) (domain.MemoryCard, error) {
	memory, err := scanMemory(tx.QueryRow(ctx, `
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
		       card.superseded_at
		FROM agent_memory.memory_cards AS card
		WHERE card.tenant_id = $1 AND card.user_id = $2 AND card.id = $3
		FOR KEY SHARE OF card`, tenantID, userID, memoryID))
	if err == nil || errors.Is(err, domain.ErrNotFound) {
		return memory, err
	}
	// scanMemory predates the projection boundary and may wrap arbitrary
	// driver text. Re-map every non-not-found failure through the redacting
	// projection mapper before it can escape this public worker API.
	return domain.MemoryCard{}, redactProjectionMemoryReadError(err)
}

func redactProjectionMemoryReadError(err error) error {
	return mapProjectionPostgresError("load projection memory", err)
}

func lockProjectionScope(ctx context.Context, tx pgx.Tx, tenantID, userID string) error {
	var revision int64
	err := tx.QueryRow(ctx, `
		SELECT context_revision
		FROM agent_memory.user_scope_state
		WHERE tenant_id = $1 AND user_id = $2
		FOR UPDATE`, tenantID, userID).Scan(&revision)
	if errors.Is(err, pgx.ErrNoRows) {
		return projectionLeaseLost("projection scope disappeared")
	}
	if err != nil {
		return mapProjectionPostgresError("lock projection scope", err)
	}
	if revision < 0 {
		return fmt.Errorf("projection scope revision is invalid: %w", domain.ErrInvariant)
	}
	return nil
}

func verifyProjectionLeaseIdentity(
	job ProjectionJob,
	tenantID, userID, embeddingSpace, leaseOwner string,
	leaseVersion int64,
) error {
	if job.TenantID != tenantID || job.UserID != userID ||
		(embeddingSpace != "" && job.EmbeddingSpace != embeddingSpace) ||
		job.State != ProjectionJobLeased || job.LeaseOwner != leaseOwner ||
		job.LeaseVersion != leaseVersion || job.LeaseUntil == nil {
		return projectionLeaseLost("projection lease token is stale")
	}
	return nil
}

func verifyProjectionLeaseDeadline(job ProjectionJob, operationAt time.Time) error {
	if job.LeaseUntil == nil || !job.LeaseUntil.After(operationAt) {
		return projectionLeaseLost("projection lease has expired")
	}
	return nil
}

func projectionDatabaseTime(ctx context.Context, tx pgx.Tx) (time.Time, error) {
	var value time.Time
	if err := tx.QueryRow(ctx, `SELECT clock_timestamp()`).Scan(&value); err != nil {
		return time.Time{}, mapProjectionPostgresError("load projection database time", err)
	}
	return canonicalProjectionTime(value), nil
}

func projectionLeaseLost(reason string) error {
	return fmt.Errorf("%s: %w", reason, ErrProjectionLeaseLost)
}

func validateClaimProjectionJobs(command ClaimProjectionJobsCommand) (ClaimProjectionJobsCommand, error) {
	if err := validateProjectionIdentifier("embedding space", command.EmbeddingSpace); err != nil {
		return ClaimProjectionJobsCommand{}, err
	}
	if err := validateProjectionIdentifier("lease owner", command.LeaseOwner); err != nil {
		return ClaimProjectionJobsCommand{}, err
	}
	if command.LeaseDuration < time.Microsecond || command.LeaseDuration > maxProjectionLease {
		return ClaimProjectionJobsCommand{}, fmt.Errorf("projection lease duration must be between 1us and 24h: %w", domain.ErrInvalid)
	}
	if command.Limit < 1 || command.Limit > maxProjectionClaimLimit {
		return ClaimProjectionJobsCommand{}, fmt.Errorf("projection claim limit must be between 1 and %d: %w", maxProjectionClaimLimit, domain.ErrInvalid)
	}
	if command.MaxAttempts == 0 {
		command.MaxAttempts = defaultProjectionMaxAttempts
	}
	if command.MaxAttempts < 1 || command.MaxAttempts > maxProjectionAttempts {
		return ClaimProjectionJobsCommand{}, fmt.Errorf("projection max attempts must be between 1 and %d: %w", maxProjectionAttempts, domain.ErrInvalid)
	}
	return command, nil
}

func validateFinalizeProjectionJob(command FinalizeProjectionJobCommand) (FinalizeProjectionJobCommand, string, error) {
	if err := validateProjectionLeaseToken(command.JobID, command.TenantID, command.UserID, command.LeaseOwner, command.LeaseVersion); err != nil {
		return FinalizeProjectionJobCommand{}, "", err
	}
	if err := validateProjectionIdentifier("embedding space", command.EmbeddingSpace); err != nil {
		return FinalizeProjectionJobCommand{}, "", err
	}
	documentSHA256, err := normalizeSHA256("projection document sha256", command.DocumentSHA256)
	if err != nil {
		return FinalizeProjectionJobCommand{}, "", err
	}
	vectorText, err := encodeVector(command.Vector)
	if err != nil {
		return FinalizeProjectionJobCommand{}, "", fmt.Errorf("projection vector: %w", err)
	}
	command.DocumentSHA256 = documentSHA256
	return command, vectorText, nil
}

func validateRetryProjectionJob(command RetryProjectionJobCommand) (RetryProjectionJobCommand, error) {
	if err := validateProjectionLeaseToken(command.JobID, command.TenantID, command.UserID, command.LeaseOwner, command.LeaseVersion); err != nil {
		return RetryProjectionJobCommand{}, err
	}
	if err := validateProjectionErrorCode(command.ErrorCode); err != nil {
		return RetryProjectionJobCommand{}, err
	}
	if command.RetryAfter < 0 || command.RetryAfter > maxProjectionLease {
		return RetryProjectionJobCommand{}, fmt.Errorf("projection retry backoff must be between zero and 24h: %w", domain.ErrInvalid)
	}
	return command, nil
}

func validateDeadLetterProjectionJob(command DeadLetterProjectionJobCommand) (DeadLetterProjectionJobCommand, error) {
	if err := validateProjectionLeaseToken(command.JobID, command.TenantID, command.UserID, command.LeaseOwner, command.LeaseVersion); err != nil {
		return DeadLetterProjectionJobCommand{}, err
	}
	if err := validateProjectionErrorCode(command.ErrorCode); err != nil {
		return DeadLetterProjectionJobCommand{}, err
	}
	return command, nil
}

func validateCancelProjectionJob(command CancelProjectionJobCommand) (CancelProjectionJobCommand, error) {
	if err := validateProjectionLeaseToken(command.JobID, command.TenantID, command.UserID, command.LeaseOwner, command.LeaseVersion); err != nil {
		return CancelProjectionJobCommand{}, err
	}
	return command, nil
}

func validateProjectionLeaseToken(jobID int64, tenantID, userID, leaseOwner string, leaseVersion int64) error {
	if jobID < 1 {
		return fmt.Errorf("projection job id must be positive: %w", domain.ErrInvalid)
	}
	for _, field := range []struct {
		name  string
		value string
	}{
		{name: "tenant id", value: tenantID},
		{name: "user id", value: userID},
		{name: "lease owner", value: leaseOwner},
	} {
		if err := validateProjectionIdentifier(field.name, field.value); err != nil {
			return err
		}
	}
	if leaseVersion < 1 {
		return fmt.Errorf("projection lease version must be positive: %w", domain.ErrInvalid)
	}
	return nil
}
