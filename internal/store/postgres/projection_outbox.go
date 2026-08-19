package postgres

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/kai443/go-agent-memory-system/internal/domain"
)

// ProjectionTargetState controls whether a registered embedding space accepts
// new projection jobs and whether it is eligible for serving. This repository
// only persists the deployment intent; workers and serving code enforce it in
// later phases.
type ProjectionTargetState string

const (
	ProjectionTargetShadow  ProjectionTargetState = "shadow"
	ProjectionTargetServing ProjectionTargetState = "serving"
	ProjectionTargetBlocked ProjectionTargetState = "blocked"
	ProjectionTargetRetired ProjectionTargetState = "retired"
)

// EmbeddingSpaceDefinition is the immutable, non-secret identity of a vector
// space. It deliberately contains neither provider URLs nor credentials.
type EmbeddingSpaceDefinition struct {
	ID               string
	Provider         string
	Model            string
	Dimension        int
	DocumentVersion  string
	QueryVersion     string
	ModelFingerprint string
	CreatedAt        time.Time
}

// ProjectionTarget is the persisted deployment state for one immutable
// embedding space.
type ProjectionTarget struct {
	Space      EmbeddingSpaceDefinition
	State      ProjectionTargetState
	EnqueueNew bool
	CreatedAt  time.Time
	UpdatedAt  time.Time
}

// RegisterProjectionTargetCommand atomically registers an immutable embedding
// space and its initial projection target. Reusing an ID with different space
// metadata or different initial target settings is a conflict.
type RegisterProjectionTargetCommand struct {
	Space      EmbeddingSpaceDefinition
	State      ProjectionTargetState
	EnqueueNew bool
	CreatedAt  time.Time
}

// SetProjectionTargetCommand changes only the mutable target state. Embedding
// space metadata remains immutable.
type SetProjectionTargetCommand struct {
	EmbeddingSpace string
	State          ProjectionTargetState
	EnqueueNew     bool
	UpdatedAt      time.Time
}

// ProjectionJobState is the durable projection state exposed for operational
// acceptance. This phase intentionally provides no claim or finalize methods.
type ProjectionJobState string

const (
	ProjectionJobPending   ProjectionJobState = "pending"
	ProjectionJobLeased    ProjectionJobState = "leased"
	ProjectionJobRetry     ProjectionJobState = "retry"
	ProjectionJobSucceeded ProjectionJobState = "succeeded"
	ProjectionJobDead      ProjectionJobState = "dead"
	ProjectionJobCancelled ProjectionJobState = "cancelled"
)

// ProjectionErrorCode is a closed set of stable, non-secret failure classes.
// Provider messages must be mapped to one of these values and must never be
// normalized or copied into this field.
type ProjectionErrorCode string

const (
	ProjectionErrorTransport           ProjectionErrorCode = "transport"
	ProjectionErrorProviderTimeout     ProjectionErrorCode = "provider_timeout"
	ProjectionErrorProviderRateLimit   ProjectionErrorCode = "provider_rate_limited"
	ProjectionErrorProviderUnavailable ProjectionErrorCode = "provider_unavailable"
	ProjectionErrorProviderRejected    ProjectionErrorCode = "provider_rejected"
	ProjectionErrorInvalidResponse     ProjectionErrorCode = "invalid_response"
	ProjectionErrorModelMismatch       ProjectionErrorCode = "model_mismatch"
	ProjectionErrorDimensionMismatch   ProjectionErrorCode = "dimension_mismatch"
	ProjectionErrorNonFiniteVector     ProjectionErrorCode = "non_finite_vector"
	ProjectionErrorSpaceConflict       ProjectionErrorCode = "space_conflict"
	ProjectionErrorAttemptsExhausted   ProjectionErrorCode = "attempts_exhausted"
)

// ProjectionJob contains durable queue bookkeeping only. The schema and this
// type intentionally exclude documents, vectors, provider responses, endpoint
// URLs, credentials, and raw error text. LastErrorCode is a stable redacted
// code, not a provider error message.
type ProjectionJob struct {
	ID                    int64
	TenantID              string
	UserID                string
	MemoryID              string
	EmbeddingSpace        string
	ExpectedMemoryVersion int
	State                 ProjectionJobState
	AttemptCount          int
	AvailableAt           time.Time
	LeaseOwner            string
	LeaseVersion          int64
	LeaseUntil            *time.Time
	LastErrorCode         ProjectionErrorCode
	LastErrorAt           *time.Time
	CreatedAt             time.Time
	UpdatedAt             time.Time
	CompletedAt           *time.Time
}

// ProjectionJobFilter bounds an administrative job read. Scope fields must be
// provided together. An empty States slice means all states.
type ProjectionJobFilter struct {
	EmbeddingSpace string
	TenantID       string
	UserID         string
	States         []ProjectionJobState
	Limit          int
}

// ProjectionJobStatistics is a secret-free aggregate used by acceptance and
// restart/deletion propagation tests.
type ProjectionJobStatistics struct {
	EmbeddingSpace string
	Total          int64
	Pending        int64
	Leased         int64
	Retry          int64
	Succeeded      int64
	Dead           int64
	Cancelled      int64
	OldestRunnable *time.Time
	LastUpdatedAt  *time.Time
}

// RegisterProjectionTarget atomically registers immutable space metadata and
// an initial target. Exact retries are idempotent; any configuration drift
// fails closed with domain.ErrConflict.
func (s *Store) RegisterProjectionTarget(ctx context.Context, command RegisterProjectionTargetCommand) (ProjectionTarget, error) {
	normalized, err := validateRegisterProjectionTarget(command)
	if err != nil {
		return ProjectionTarget{}, err
	}
	if err := s.ready(); err != nil {
		return ProjectionTarget{}, err
	}

	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return ProjectionTarget{}, mapProjectionPostgresError("begin projection target registration", err)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()

	_, err = tx.Exec(ctx, `
		INSERT INTO agent_memory.embedding_spaces (
			id, provider, model, dimension, document_version, query_version,
			model_fingerprint, created_at
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
		ON CONFLICT (id) DO NOTHING`,
		normalized.Space.ID,
		normalized.Space.Provider,
		normalized.Space.Model,
		normalized.Space.Dimension,
		normalized.Space.DocumentVersion,
		normalized.Space.QueryVersion,
		normalized.Space.ModelFingerprint,
		normalized.Space.CreatedAt,
	)
	if err != nil {
		return ProjectionTarget{}, mapProjectionPostgresError("register projection embedding space", err)
	}

	registeredSpace, err := readEmbeddingSpaceDefinition(ctx, tx, normalized.Space.ID, true)
	if err != nil {
		return ProjectionTarget{}, err
	}
	if !sameEmbeddingSpaceConfiguration(registeredSpace, normalized.Space) {
		return ProjectionTarget{}, fmt.Errorf("embedding space configuration differs from its registry: %w", domain.ErrConflict)
	}

	_, err = tx.Exec(ctx, `
		INSERT INTO agent_memory.embedding_projection_targets (
			embedding_space, state, enqueue_new, created_at, updated_at
		) VALUES ($1, $2, $3, $4, $4)
		ON CONFLICT (embedding_space) DO NOTHING`,
		normalized.Space.ID,
		string(normalized.State),
		normalized.EnqueueNew,
		normalized.CreatedAt,
	)
	if err != nil {
		return ProjectionTarget{}, mapProjectionPostgresError("register projection target", err)
	}

	target, err := readProjectionTarget(ctx, tx, normalized.Space.ID, true)
	if err != nil {
		return ProjectionTarget{}, err
	}
	if target.State != normalized.State || target.EnqueueNew != normalized.EnqueueNew {
		return ProjectionTarget{}, fmt.Errorf("projection target initial configuration differs from its registry: %w", domain.ErrConflict)
	}
	if err := tx.Commit(ctx); err != nil {
		return ProjectionTarget{}, mapProjectionPostgresError("commit projection target registration", err)
	}
	return target, nil
}

// SetProjectionTarget updates target state under a row lock. Exact retries at
// the same timestamp are idempotent, while stale or ambiguous writes fail.
func (s *Store) SetProjectionTarget(ctx context.Context, command SetProjectionTargetCommand) (ProjectionTarget, error) {
	normalized, err := validateSetProjectionTarget(command)
	if err != nil {
		return ProjectionTarget{}, err
	}
	if err := s.ready(); err != nil {
		return ProjectionTarget{}, err
	}

	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return ProjectionTarget{}, mapProjectionPostgresError("begin projection target update", err)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()

	current, err := readProjectionTarget(ctx, tx, normalized.EmbeddingSpace, true)
	if err != nil {
		return ProjectionTarget{}, err
	}
	// Retired is terminal. Re-enabling the same vector space after operators
	// have begun cleanup would make coverage ambiguous; a new deployment must
	// register a new immutable space instead.
	if current.State == ProjectionTargetRetired && normalized.State != ProjectionTargetRetired {
		return ProjectionTarget{}, fmt.Errorf("retired projection target cannot be reactivated: %w", domain.ErrConflict)
	}
	if normalized.UpdatedAt.Before(current.UpdatedAt) {
		return ProjectionTarget{}, fmt.Errorf("projection target update is stale: %w", domain.ErrConflict)
	}
	if normalized.UpdatedAt.Equal(current.UpdatedAt) {
		if current.State != normalized.State || current.EnqueueNew != normalized.EnqueueNew {
			return ProjectionTarget{}, fmt.Errorf("projection target timestamp has conflicting values: %w", domain.ErrConflict)
		}
		if err := tx.Commit(ctx); err != nil {
			return ProjectionTarget{}, mapProjectionPostgresError("commit projection target no-op", err)
		}
		return current, nil
	}

	_, err = tx.Exec(ctx, `
		UPDATE agent_memory.embedding_projection_targets
		SET state = $2, enqueue_new = $3, updated_at = $4
		WHERE embedding_space = $1`,
		normalized.EmbeddingSpace,
		string(normalized.State),
		normalized.EnqueueNew,
		normalized.UpdatedAt,
	)
	if err != nil {
		return ProjectionTarget{}, mapProjectionPostgresError("update projection target", err)
	}
	target, err := readProjectionTarget(ctx, tx, normalized.EmbeddingSpace, false)
	if err != nil {
		return ProjectionTarget{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return ProjectionTarget{}, mapProjectionPostgresError("commit projection target update", err)
	}
	return target, nil
}

// ProjectionTargetBySpace loads one target and its immutable space metadata.
func (s *Store) ProjectionTargetBySpace(ctx context.Context, embeddingSpace string) (ProjectionTarget, error) {
	if err := validateProjectionIdentifier("embedding space", embeddingSpace); err != nil {
		return ProjectionTarget{}, err
	}
	if err := s.ready(); err != nil {
		return ProjectionTarget{}, err
	}
	return readProjectionTarget(ctx, s.pool, embeddingSpace, false)
}

// ProjectionTargets lists all targets in stable embedding-space order.
func (s *Store) ProjectionTargets(ctx context.Context) ([]ProjectionTarget, error) {
	if err := s.ready(); err != nil {
		return nil, err
	}
	rows, err := s.pool.Query(ctx, projectionTargetSelect+`
		ORDER BY target.embedding_space`)
	if err != nil {
		return nil, mapProjectionPostgresError("list projection targets", err)
	}
	defer rows.Close()

	targets := make([]ProjectionTarget, 0)
	for rows.Next() {
		target, scanErr := scanProjectionTarget(rows)
		if scanErr != nil {
			return nil, scanErr
		}
		targets = append(targets, target)
	}
	if err := rows.Err(); err != nil {
		return nil, mapProjectionPostgresError("iterate projection targets", err)
	}
	return targets, nil
}

// ProjectionJobs returns queue bookkeeping in stable operational order. It is
// read-only and intentionally cannot claim, mutate, or finalize jobs.
func (s *Store) ProjectionJobs(ctx context.Context, filter ProjectionJobFilter) ([]ProjectionJob, error) {
	normalized, states, err := validateProjectionJobFilter(filter)
	if err != nil {
		return nil, err
	}
	if err := s.ready(); err != nil {
		return nil, err
	}
	if _, err := readProjectionTarget(ctx, s.pool, normalized.EmbeddingSpace, false); err != nil {
		return nil, err
	}

	rows, err := s.pool.Query(ctx, `
		SELECT id, tenant_id, user_id, memory_id, embedding_space,
		       expected_memory_version, state, attempt_count, available_at,
		       COALESCE(lease_owner, ''), lease_version, lease_until,
		       COALESCE(last_error_code, ''), last_error_at,
		       created_at, updated_at, completed_at
		FROM agent_memory.embedding_projection_jobs
		WHERE embedding_space = $1
		  AND ($2::text = '' OR (tenant_id = $2 AND user_id = $3))
		  AND ($4::boolean OR state = ANY($5::text[]))
		ORDER BY available_at, created_at, id
		LIMIT $6`,
		normalized.EmbeddingSpace,
		normalized.TenantID,
		normalized.UserID,
		len(states) == 0,
		states,
		normalized.Limit,
	)
	if err != nil {
		return nil, mapProjectionPostgresError("list projection jobs", err)
	}
	defer rows.Close()

	jobs := make([]ProjectionJob, 0)
	for rows.Next() {
		job, scanErr := scanProjectionJob(rows)
		if scanErr != nil {
			return nil, scanErr
		}
		jobs = append(jobs, job)
	}
	if err := rows.Err(); err != nil {
		return nil, mapProjectionPostgresError("iterate projection jobs", err)
	}
	return jobs, nil
}

// ProjectionJobStats reports counts for one embedding space without exposing
// any per-job identifiers or errors.
func (s *Store) ProjectionJobStats(ctx context.Context, embeddingSpace string) (ProjectionJobStatistics, error) {
	if err := validateProjectionIdentifier("embedding space", embeddingSpace); err != nil {
		return ProjectionJobStatistics{}, err
	}
	if err := s.ready(); err != nil {
		return ProjectionJobStatistics{}, err
	}
	if _, err := readProjectionTarget(ctx, s.pool, embeddingSpace, false); err != nil {
		return ProjectionJobStatistics{}, err
	}

	statistics := ProjectionJobStatistics{EmbeddingSpace: embeddingSpace}
	var oldestRunnable, lastUpdated pgtype.Timestamptz
	err := s.pool.QueryRow(ctx, `
		SELECT count(*),
		       count(*) FILTER (WHERE state = 'pending'),
		       count(*) FILTER (WHERE state = 'leased'),
		       count(*) FILTER (WHERE state = 'retry'),
		       count(*) FILTER (WHERE state = 'succeeded'),
		       count(*) FILTER (WHERE state = 'dead'),
		       count(*) FILTER (WHERE state = 'cancelled'),
		       min(available_at) FILTER (WHERE state IN ('pending', 'retry')),
		       max(updated_at)
		FROM agent_memory.embedding_projection_jobs
		WHERE embedding_space = $1`, embeddingSpace).Scan(
		&statistics.Total,
		&statistics.Pending,
		&statistics.Leased,
		&statistics.Retry,
		&statistics.Succeeded,
		&statistics.Dead,
		&statistics.Cancelled,
		&oldestRunnable,
		&lastUpdated,
	)
	if err != nil {
		return ProjectionJobStatistics{}, mapProjectionPostgresError("load projection job statistics", err)
	}
	if statistics.Total != statistics.Pending+statistics.Leased+statistics.Retry+
		statistics.Succeeded+statistics.Dead+statistics.Cancelled {
		return ProjectionJobStatistics{}, fmt.Errorf("projection job state counts are inconsistent: %w", domain.ErrInvariant)
	}
	if oldestRunnable.Valid {
		value := canonicalProjectionTime(oldestRunnable.Time)
		statistics.OldestRunnable = &value
	}
	if lastUpdated.Valid {
		value := canonicalProjectionTime(lastUpdated.Time)
		statistics.LastUpdatedAt = &value
	}
	return statistics, nil
}

const projectionTargetSelect = `
	SELECT space.id, space.provider, space.model, space.dimension,
	       space.document_version, space.query_version, space.model_fingerprint,
	       space.created_at, target.state, target.enqueue_new,
	       target.created_at, target.updated_at
	FROM agent_memory.embedding_projection_targets AS target
	JOIN agent_memory.embedding_spaces AS space ON space.id = target.embedding_space`

func readProjectionTarget(ctx context.Context, queryer rowQueryer, embeddingSpace string, forUpdate bool) (ProjectionTarget, error) {
	query := projectionTargetSelect + ` WHERE target.embedding_space = $1`
	if forUpdate {
		query += ` FOR UPDATE OF target`
	}
	target, err := scanProjectionTarget(queryer.QueryRow(ctx, query, embeddingSpace))
	if errors.Is(err, domain.ErrNotFound) {
		return ProjectionTarget{}, fmt.Errorf("projection target: %w", domain.ErrNotFound)
	}
	return target, err
}

func scanProjectionTarget(row rowScanner) (ProjectionTarget, error) {
	var target ProjectionTarget
	var state string
	err := row.Scan(
		&target.Space.ID,
		&target.Space.Provider,
		&target.Space.Model,
		&target.Space.Dimension,
		&target.Space.DocumentVersion,
		&target.Space.QueryVersion,
		&target.Space.ModelFingerprint,
		&target.Space.CreatedAt,
		&state,
		&target.EnqueueNew,
		&target.CreatedAt,
		&target.UpdatedAt,
	)
	if err != nil {
		return ProjectionTarget{}, mapProjectionPostgresError("scan projection target", err)
	}
	target.State = ProjectionTargetState(state)
	if err := validateStoredProjectionTarget(target); err != nil {
		return ProjectionTarget{}, err
	}
	target.Space.CreatedAt = canonicalProjectionTime(target.Space.CreatedAt)
	target.CreatedAt = canonicalProjectionTime(target.CreatedAt)
	target.UpdatedAt = canonicalProjectionTime(target.UpdatedAt)
	return target, nil
}

func readEmbeddingSpaceDefinition(ctx context.Context, queryer rowQueryer, embeddingSpace string, forUpdate bool) (EmbeddingSpaceDefinition, error) {
	query := `
		SELECT id, provider, model, dimension, document_version, query_version,
		       model_fingerprint, created_at
		FROM agent_memory.embedding_spaces
		WHERE id = $1`
	if forUpdate {
		query += ` FOR UPDATE`
	}
	var definition EmbeddingSpaceDefinition
	err := queryer.QueryRow(ctx, query, embeddingSpace).Scan(
		&definition.ID,
		&definition.Provider,
		&definition.Model,
		&definition.Dimension,
		&definition.DocumentVersion,
		&definition.QueryVersion,
		&definition.ModelFingerprint,
		&definition.CreatedAt,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return EmbeddingSpaceDefinition{}, fmt.Errorf("embedding space registry row is missing: %w", domain.ErrInvariant)
		}
		return EmbeddingSpaceDefinition{}, mapProjectionPostgresError("load projection embedding space", err)
	}
	definition.CreatedAt = canonicalProjectionTime(definition.CreatedAt)
	return definition, nil
}

func scanProjectionJob(row rowScanner) (ProjectionJob, error) {
	var job ProjectionJob
	var state, lastErrorCode string
	var leaseUntil, lastErrorAt, completedAt pgtype.Timestamptz
	err := row.Scan(
		&job.ID,
		&job.TenantID,
		&job.UserID,
		&job.MemoryID,
		&job.EmbeddingSpace,
		&job.ExpectedMemoryVersion,
		&state,
		&job.AttemptCount,
		&job.AvailableAt,
		&job.LeaseOwner,
		&job.LeaseVersion,
		&leaseUntil,
		&lastErrorCode,
		&lastErrorAt,
		&job.CreatedAt,
		&job.UpdatedAt,
		&completedAt,
	)
	if err != nil {
		return ProjectionJob{}, mapProjectionPostgresError("scan projection job", err)
	}
	job.State = ProjectionJobState(state)
	job.LastErrorCode = ProjectionErrorCode(lastErrorCode)
	if leaseUntil.Valid {
		value := canonicalProjectionTime(leaseUntil.Time)
		job.LeaseUntil = &value
	}
	if lastErrorAt.Valid {
		value := canonicalProjectionTime(lastErrorAt.Time)
		job.LastErrorAt = &value
	}
	if completedAt.Valid {
		value := canonicalProjectionTime(completedAt.Time)
		job.CompletedAt = &value
	}
	job.AvailableAt = canonicalProjectionTime(job.AvailableAt)
	job.CreatedAt = canonicalProjectionTime(job.CreatedAt)
	job.UpdatedAt = canonicalProjectionTime(job.UpdatedAt)
	if err := validateStoredProjectionJob(job); err != nil {
		return ProjectionJob{}, err
	}
	return job, nil
}

func validateRegisterProjectionTarget(command RegisterProjectionTargetCommand) (RegisterProjectionTargetCommand, error) {
	space, err := validateEmbeddingSpaceDefinition(command.Space)
	if err != nil {
		return RegisterProjectionTargetCommand{}, err
	}
	if err := validateProjectionTargetSettings(command.State, command.EnqueueNew); err != nil {
		return RegisterProjectionTargetCommand{}, err
	}
	if command.CreatedAt.IsZero() {
		return RegisterProjectionTargetCommand{}, fmt.Errorf("projection target created_at is required: %w", domain.ErrInvalid)
	}
	command.Space = space
	command.CreatedAt = canonicalProjectionTime(command.CreatedAt)
	return command, nil
}

func validateSetProjectionTarget(command SetProjectionTargetCommand) (SetProjectionTargetCommand, error) {
	if err := validateProjectionIdentifier("embedding space", command.EmbeddingSpace); err != nil {
		return SetProjectionTargetCommand{}, err
	}
	if err := validateProjectionTargetSettings(command.State, command.EnqueueNew); err != nil {
		return SetProjectionTargetCommand{}, err
	}
	if command.UpdatedAt.IsZero() {
		return SetProjectionTargetCommand{}, fmt.Errorf("projection target updated_at is required: %w", domain.ErrInvalid)
	}
	command.UpdatedAt = canonicalProjectionTime(command.UpdatedAt)
	return command, nil
}

func validateEmbeddingSpaceDefinition(definition EmbeddingSpaceDefinition) (EmbeddingSpaceDefinition, error) {
	for _, field := range []struct {
		name  string
		value string
	}{
		{name: "embedding space", value: definition.ID},
		{name: "provider", value: definition.Provider},
		{name: "model", value: definition.Model},
		{name: "document version", value: definition.DocumentVersion},
		{name: "query version", value: definition.QueryVersion},
	} {
		if err := validateProjectionIdentifier(field.name, field.value); err != nil {
			return EmbeddingSpaceDefinition{}, err
		}
	}
	if definition.Dimension != VectorDimension {
		return EmbeddingSpaceDefinition{}, fmt.Errorf("embedding dimension must be %d: %w", VectorDimension, domain.ErrInvalid)
	}
	fingerprint, err := normalizeSHA256("model fingerprint", definition.ModelFingerprint)
	if err != nil {
		return EmbeddingSpaceDefinition{}, err
	}
	if definition.CreatedAt.IsZero() {
		return EmbeddingSpaceDefinition{}, fmt.Errorf("embedding space created_at is required: %w", domain.ErrInvalid)
	}
	definition.ModelFingerprint = fingerprint
	definition.CreatedAt = canonicalProjectionTime(definition.CreatedAt)
	return definition, nil
}

func validateProjectionTargetSettings(state ProjectionTargetState, enqueueNew bool) error {
	switch state {
	case ProjectionTargetShadow, ProjectionTargetServing:
		return nil
	case ProjectionTargetBlocked, ProjectionTargetRetired:
		if enqueueNew {
			return fmt.Errorf("blocked or retired projection target cannot enqueue new jobs: %w", domain.ErrInvalid)
		}
		return nil
	default:
		return fmt.Errorf("projection target state is invalid: %w", domain.ErrInvalid)
	}
}

func validateStoredProjectionTarget(target ProjectionTarget) error {
	if _, err := validateEmbeddingSpaceDefinition(target.Space); err != nil {
		return fmt.Errorf("stored embedding space is invalid: %w", domain.ErrInvariant)
	}
	if err := validateProjectionTargetSettings(target.State, target.EnqueueNew); err != nil {
		return fmt.Errorf("stored projection target is invalid: %w", domain.ErrInvariant)
	}
	if target.CreatedAt.IsZero() || target.UpdatedAt.IsZero() || target.UpdatedAt.Before(target.CreatedAt) {
		return fmt.Errorf("stored projection target timestamps are invalid: %w", domain.ErrInvariant)
	}
	return nil
}

func validateProjectionJobFilter(filter ProjectionJobFilter) (ProjectionJobFilter, []string, error) {
	if err := validateProjectionIdentifier("embedding space", filter.EmbeddingSpace); err != nil {
		return ProjectionJobFilter{}, nil, err
	}
	// Empty means unscoped. Whitespace is not silently normalized to empty,
	// because doing so would turn an operator typo into a misleading empty read.
	hasTenant := filter.TenantID != ""
	hasUser := filter.UserID != ""
	if hasTenant != hasUser {
		return ProjectionJobFilter{}, nil, fmt.Errorf("tenant id and user id must be provided together: %w", domain.ErrInvalid)
	}
	if hasTenant {
		if err := validateProjectionIdentifier("tenant id", filter.TenantID); err != nil {
			return ProjectionJobFilter{}, nil, err
		}
		if err := validateProjectionIdentifier("user id", filter.UserID); err != nil {
			return ProjectionJobFilter{}, nil, err
		}
	}
	if filter.Limit < 1 || filter.Limit > 1000 {
		return ProjectionJobFilter{}, nil, fmt.Errorf("projection job limit must be between 1 and 1000: %w", domain.ErrInvalid)
	}
	states := make([]string, 0, len(filter.States))
	seen := make(map[ProjectionJobState]struct{}, len(filter.States))
	for _, state := range filter.States {
		if !validProjectionJobState(state) {
			return ProjectionJobFilter{}, nil, fmt.Errorf("projection job state is invalid: %w", domain.ErrInvalid)
		}
		if _, exists := seen[state]; exists {
			return ProjectionJobFilter{}, nil, fmt.Errorf("projection job state is duplicated: %w", domain.ErrInvalid)
		}
		seen[state] = struct{}{}
		states = append(states, string(state))
	}
	return filter, states, nil
}

func validateStoredProjectionJob(job ProjectionJob) error {
	if job.ID < 1 ||
		job.ExpectedMemoryVersion < 1 ||
		job.AttemptCount < 0 ||
		job.LeaseVersion < 0 ||
		job.AvailableAt.IsZero() ||
		job.CreatedAt.IsZero() ||
		job.UpdatedAt.IsZero() ||
		job.UpdatedAt.Before(job.CreatedAt) ||
		!validProjectionJobState(job.State) {
		return fmt.Errorf("stored projection job fields are invalid: %w", domain.ErrInvariant)
	}
	for _, field := range []struct {
		name  string
		value string
	}{
		{name: "tenant id", value: job.TenantID},
		{name: "user id", value: job.UserID},
		{name: "memory id", value: job.MemoryID},
		{name: "embedding space", value: job.EmbeddingSpace},
	} {
		if err := validateProjectionIdentifier(field.name, field.value); err != nil {
			return fmt.Errorf("stored projection job identifier is invalid: %w", domain.ErrInvariant)
		}
	}
	if job.LastErrorCode != "" {
		if err := validateProjectionErrorCode(job.LastErrorCode); err != nil {
			return fmt.Errorf("stored projection job error code is invalid: %w", domain.ErrInvariant)
		}
	}
	if (job.LastErrorCode == "") != (job.LastErrorAt == nil) {
		return fmt.Errorf("stored projection job error fields are incomplete: %w", domain.ErrInvariant)
	}
	if job.State == ProjectionJobLeased {
		if strings.TrimSpace(job.LeaseOwner) == "" || job.LeaseVersion < 1 || job.LeaseUntil == nil || job.AttemptCount < 1 {
			return fmt.Errorf("stored leased projection job fields are invalid: %w", domain.ErrInvariant)
		}
	} else if job.LeaseOwner != "" || job.LeaseUntil != nil {
		return fmt.Errorf("stored non-leased projection job contains a lease: %w", domain.ErrInvariant)
	}
	terminal := job.State == ProjectionJobSucceeded || job.State == ProjectionJobDead || job.State == ProjectionJobCancelled
	if terminal != (job.CompletedAt != nil) {
		return fmt.Errorf("stored projection job completion fields are invalid: %w", domain.ErrInvariant)
	}
	if job.State == ProjectionJobDead && job.LastErrorCode == "" {
		return fmt.Errorf("stored dead projection job has no error code: %w", domain.ErrInvariant)
	}
	if job.State != ProjectionJobPending && job.State != ProjectionJobCancelled && job.AttemptCount < 1 {
		return fmt.Errorf("stored projection job attempt count is invalid for its state: %w", domain.ErrInvariant)
	}
	if job.LastErrorAt != nil && job.LastErrorAt.Before(job.CreatedAt) {
		return fmt.Errorf("stored projection job error timestamp is invalid: %w", domain.ErrInvariant)
	}
	if job.CompletedAt != nil && job.CompletedAt.Before(job.CreatedAt) {
		return fmt.Errorf("stored projection job completion timestamp is invalid: %w", domain.ErrInvariant)
	}
	return nil
}

func validProjectionJobState(state ProjectionJobState) bool {
	switch state {
	case ProjectionJobPending,
		ProjectionJobLeased,
		ProjectionJobRetry,
		ProjectionJobSucceeded,
		ProjectionJobDead,
		ProjectionJobCancelled:
		return true
	default:
		return false
	}
}

func validateProjectionIdentifier(name, value string) error {
	if strings.TrimSpace(value) == "" {
		return fmt.Errorf("%s is required: %w", name, domain.ErrInvalid)
	}
	if value != strings.TrimSpace(value) || len(value) > 512 || strings.IndexFunc(value, func(r rune) bool {
		return r < 0x20 || r == 0x7f
	}) >= 0 {
		return fmt.Errorf("%s has an invalid format: %w", name, domain.ErrInvalid)
	}
	return nil
}

func validateProjectionErrorCode(value ProjectionErrorCode) error {
	switch value {
	case ProjectionErrorTransport,
		ProjectionErrorProviderTimeout,
		ProjectionErrorProviderRateLimit,
		ProjectionErrorProviderUnavailable,
		ProjectionErrorProviderRejected,
		ProjectionErrorInvalidResponse,
		ProjectionErrorModelMismatch,
		ProjectionErrorDimensionMismatch,
		ProjectionErrorNonFiniteVector,
		ProjectionErrorSpaceConflict,
		ProjectionErrorAttemptsExhausted:
		return nil
	default:
		return fmt.Errorf("projection error code is not recognized: %w", domain.ErrInvalid)
	}
}

func sameEmbeddingSpaceConfiguration(left, right EmbeddingSpaceDefinition) bool {
	// CreatedAt records when the registry first observed the space. It is
	// provenance, not part of vector compatibility; the fields below are the
	// immutable configuration also protected by migration 004.
	return left.ID == right.ID &&
		left.Provider == right.Provider &&
		left.Model == right.Model &&
		left.Dimension == right.Dimension &&
		left.DocumentVersion == right.DocumentVersion &&
		left.QueryVersion == right.QueryVersion &&
		left.ModelFingerprint == right.ModelFingerprint
}

func canonicalProjectionTime(value time.Time) time.Time {
	return value.UTC().Truncate(time.Microsecond)
}

// mapProjectionPostgresError deliberately excludes the underlying database
// error from its text. PostgreSQL diagnostics and network errors can contain
// connection details or row values; callers receive only a stable domain kind.
func mapProjectionPostgresError(action string, err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return fmt.Errorf("%s: %w", action, err)
	}
	if errors.Is(err, pgx.ErrNoRows) {
		return fmt.Errorf("%s: %w", action, domain.ErrNotFound)
	}
	var postgresError *pgconn.PgError
	if errors.As(err, &postgresError) {
		switch postgresError.Code {
		case "23505", "40001", "40P01":
			return fmt.Errorf("%s: %w", action, domain.ErrConflict)
		case "23502", "23514", "22P02":
			return fmt.Errorf("%s: %w", action, domain.ErrInvalid)
		case "23503":
			return fmt.Errorf("%s: %w", action, domain.ErrInvariant)
		default:
			return fmt.Errorf("%s: database operation failed: %w", action, domain.ErrInvariant)
		}
	}
	return fmt.Errorf("%s: database operation failed: %w", action, domain.ErrInvariant)
}
