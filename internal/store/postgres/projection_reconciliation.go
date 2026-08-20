package postgres

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/kai443/go-agent-memory-system/internal/domain"
	"github.com/kai443/go-agent-memory-system/internal/embedding"
)

const maxProjectionReconciliationPageSize = 500

// ProjectionReconciliationSnapshot is a secret-free, restart-safe token for
// one reconciliation pass. Generation fences target registration and state
// changes; StartedAt is the database-clock audit boundary, not a caller clock.
type ProjectionReconciliationSnapshot struct {
	EmbeddingSpace string
	Generation     int64
	StartedAt      time.Time
	Repair         bool
}

// ProjectionReconciliationCursor is the exclusive keyset lower bound for the
// next page. All fields are either populated together or all empty.
type ProjectionReconciliationCursor struct {
	TenantID string
	UserID   string
	MemoryID string
}

// ProjectionReconciliationCounts classifies each scanned serviceable card
// into exactly one coverage state. It intentionally contains no card content,
// scope identifiers, provider response, endpoint, or database information.
type ProjectionReconciliationCounts struct {
	Scanned                   int64
	Converged                 int64
	MissingJob                int64
	InFlight                  int64
	Dead                      int64
	Cancelled                 int64
	SucceededMissingEmbedding int64
	ContentHashMismatch       int64
	VersionInvariant          int64
}

// ProjectionReconciliationRepairs reports committed repair actions for one
// page. RevisionsAdvanced counts scopes, not cards.
type ProjectionReconciliationRepairs struct {
	JobsEnqueued      int64
	JobsReset         int64
	EmbeddingsDeleted int64
	RevisionsAdvanced int64
}

// ProjectionReconciliationPage reports the state observed before any repairs
// in the same transaction. Complete means keyset traversal is complete; the
// final coverage gate is ProjectionReconciliationReport.Complete.
type ProjectionReconciliationPage struct {
	Counts     ProjectionReconciliationCounts
	Repairs    ProjectionReconciliationRepairs
	NextCursor *ProjectionReconciliationCursor
	Complete   bool
}

// ProjectionReconciliationReport is a point-in-time aggregate coverage gate.
// Concurrent workers and deletion can make an incomplete report conservative;
// callers may rerun. Complete is true only when every serviceable card is
// converged in the coverage statement's snapshot.
type ProjectionReconciliationReport struct {
	EmbeddingSpace string
	Generation     int64
	CheckedAt      time.Time
	Counts         ProjectionReconciliationCounts
	Complete       bool
}

// BeginProjectionReconciliation freezes and records the current deployment
// generation. Retired targets and unsupported document versions fail closed.
func (s *Store) BeginProjectionReconciliation(
	ctx context.Context,
	embeddingSpace string,
	repair bool,
) (ProjectionReconciliationSnapshot, error) {
	if err := validateProjectionIdentifier("embedding space", embeddingSpace); err != nil {
		return ProjectionReconciliationSnapshot{}, err
	}
	if err := s.ready(); err != nil {
		return ProjectionReconciliationSnapshot{}, err
	}

	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return ProjectionReconciliationSnapshot{}, mapProjectionPostgresError("begin projection reconciliation", err)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()

	generation, err := lockProjectionDeploymentShared(ctx, tx)
	if err != nil {
		return ProjectionReconciliationSnapshot{}, err
	}
	target, err := lockReconciliationTarget(ctx, tx, embeddingSpace)
	if err != nil {
		return ProjectionReconciliationSnapshot{}, err
	}
	if err := validateReconciliationTarget(target, repair); err != nil {
		return ProjectionReconciliationSnapshot{}, err
	}
	startedAt, err := projectionDatabaseTime(ctx, tx)
	if err != nil {
		return ProjectionReconciliationSnapshot{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return ProjectionReconciliationSnapshot{}, mapProjectionPostgresError("commit projection reconciliation start", err)
	}
	return ProjectionReconciliationSnapshot{
		EmbeddingSpace: embeddingSpace,
		Generation:     generation,
		StartedAt:      startedAt,
		Repair:         repair,
	}, nil
}

// ReconcileProjectionPage classifies and optionally repairs at most limit
// serviceable cards. It never holds a transaction across provider I/O.
func (s *Store) ReconcileProjectionPage(
	ctx context.Context,
	snapshot ProjectionReconciliationSnapshot,
	cursor ProjectionReconciliationCursor,
	limit int,
) (ProjectionReconciliationPage, error) {
	normalizedSnapshot, err := validateProjectionReconciliationSnapshot(snapshot)
	if err != nil {
		return ProjectionReconciliationPage{}, err
	}
	if err := validateProjectionReconciliationCursor(cursor); err != nil {
		return ProjectionReconciliationPage{}, err
	}
	if limit < 1 || limit > maxProjectionReconciliationPageSize {
		return ProjectionReconciliationPage{}, fmt.Errorf(
			"projection reconciliation page limit must be between 1 and %d: %w",
			maxProjectionReconciliationPageSize,
			domain.ErrInvalid,
		)
	}
	if err := s.ready(); err != nil {
		return ProjectionReconciliationPage{}, err
	}

	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return ProjectionReconciliationPage{}, mapProjectionPostgresError("begin projection reconciliation page", err)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()

	generation, err := lockProjectionDeploymentShared(ctx, tx)
	if err != nil {
		return ProjectionReconciliationPage{}, err
	}
	if generation != normalizedSnapshot.Generation {
		return ProjectionReconciliationPage{}, projectionReconciliationChanged()
	}
	operationAt, err := projectionDatabaseTime(ctx, tx)
	if err != nil {
		return ProjectionReconciliationPage{}, err
	}
	if operationAt.Before(normalizedSnapshot.StartedAt) {
		return ProjectionReconciliationPage{}, fmt.Errorf("projection reconciliation start is after database time: %w", domain.ErrInvalid)
	}

	keys, hasMore, err := selectProjectionReconciliationKeys(ctx, tx, cursor, limit, operationAt)
	if err != nil {
		return ProjectionReconciliationPage{}, err
	}
	if err := lockProjectionReconciliationScopes(ctx, tx, keys); err != nil {
		return ProjectionReconciliationPage{}, err
	}
	target, err := lockReconciliationTarget(ctx, tx, normalizedSnapshot.EmbeddingSpace)
	if err != nil {
		return ProjectionReconciliationPage{}, err
	}
	if err := validateReconciliationTarget(target, normalizedSnapshot.Repair); err != nil {
		return ProjectionReconciliationPage{}, err
	}

	cards, err := lockProjectionReconciliationCards(ctx, tx, keys, operationAt)
	if err != nil {
		return ProjectionReconciliationPage{}, err
	}
	page := ProjectionReconciliationPage{Complete: !hasMore}
	revisionScopes := make(map[projectionReconciliationScope]struct{})
	for _, card := range cards {
		job, jobExists, err := lockProjectionReconciliationJob(
			ctx,
			tx,
			card.TenantID,
			card.UserID,
			card.ID,
			normalizedSnapshot.EmbeddingSpace,
		)
		if err != nil {
			return ProjectionReconciliationPage{}, err
		}
		embeddingHash, embeddingExists, err := lockProjectionReconciliationEmbedding(
			ctx,
			tx,
			card.TenantID,
			card.UserID,
			card.ID,
			normalizedSnapshot.EmbeddingSpace,
		)
		if err != nil {
			return ProjectionReconciliationPage{}, err
		}

		expectedHash := embedding.MemoryCardDocumentV1SHA256(card)
		classification, staleEmbedding, err := classifyProjectionCoverage(
			card.Version,
			job,
			jobExists,
			embeddingHash,
			embeddingExists,
			expectedHash,
		)
		if err != nil {
			return ProjectionReconciliationPage{}, err
		}
		page.Counts.add(classification)

		if !normalizedSnapshot.Repair {
			continue
		}
		if staleEmbedding {
			deleted, err := deleteProjectionReconciliationEmbedding(
				ctx,
				tx,
				card.TenantID,
				card.UserID,
				card.ID,
				normalizedSnapshot.EmbeddingSpace,
			)
			if err != nil {
				return ProjectionReconciliationPage{}, err
			}
			if deleted {
				page.Repairs.EmbeddingsDeleted++
				if target.State == ProjectionTargetServing {
					revisionScopes[projectionReconciliationScope{card.TenantID, card.UserID}] = struct{}{}
				}
			}
		}

		switch classification {
		case projectionCoverageMissingJob:
			if err := enqueueProjectionReconciliationJob(
				ctx,
				tx,
				card,
				normalizedSnapshot.EmbeddingSpace,
				operationAt,
			); err != nil {
				return ProjectionReconciliationPage{}, err
			}
			page.Repairs.JobsEnqueued++
		case projectionCoverageSucceededMissingEmbedding,
			projectionCoverageContentHashMismatch:
			if err := resetProjectionReconciliationJob(ctx, tx, job.ID, operationAt); err != nil {
				return ProjectionReconciliationPage{}, err
			}
			page.Repairs.JobsReset++
			if target.State == ProjectionTargetServing && classification == projectionCoverageSucceededMissingEmbedding {
				revisionScopes[projectionReconciliationScope{card.TenantID, card.UserID}] = struct{}{}
			}
		}
	}

	if err := advanceProjectionReconciliationRevisions(ctx, tx, revisionScopes, operationAt); err != nil {
		return ProjectionReconciliationPage{}, err
	}
	page.Repairs.RevisionsAdvanced = int64(len(revisionScopes))
	if err := page.Counts.validate(); err != nil {
		return ProjectionReconciliationPage{}, err
	}
	if hasMore && len(keys) > 0 {
		last := keys[len(keys)-1]
		page.NextCursor = &ProjectionReconciliationCursor{
			TenantID: last.TenantID,
			UserID:   last.UserID,
			MemoryID: last.MemoryID,
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return ProjectionReconciliationPage{}, mapProjectionPostgresError("commit projection reconciliation page", err)
	}
	return page, nil
}

// FinalizeProjectionReconciliation revalidates deployment generation and
// target state under an exclusive deployment lock, then computes one
// aggregate in a subsequent READ COMMITTED statement. The later statement is
// essential: if the deployment lock waited for an approval, its new card must
// be visible after that approval commits. This is intentionally a point-in-time
// report, not a persisted promotion decision. The lock is retained for the
// O(N) coverage scan, so callers must treat this as an offline gate. Concurrent
// worker completion or deletion can produce a conservative false negative; neither can turn observed
// convergence into an invalid vector for a still-serviceable card.
func (s *Store) FinalizeProjectionReconciliation(
	ctx context.Context,
	snapshot ProjectionReconciliationSnapshot,
) (ProjectionReconciliationReport, error) {
	normalizedSnapshot, err := validateProjectionReconciliationSnapshot(snapshot)
	if err != nil {
		return ProjectionReconciliationReport{}, err
	}
	if err := s.ready(); err != nil {
		return ProjectionReconciliationReport{}, err
	}

	tx, err := s.pool.BeginTx(ctx, projectionReconciliationFinalizationTxOptions())
	if err != nil {
		return ProjectionReconciliationReport{}, mapProjectionPostgresError("begin projection reconciliation finalization", err)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()

	generation, err := lockProjectionDeploymentExclusive(ctx, tx)
	if err != nil {
		return ProjectionReconciliationReport{}, err
	}
	if generation != normalizedSnapshot.Generation {
		return ProjectionReconciliationReport{}, projectionReconciliationChanged()
	}
	target, err := lockReconciliationTarget(ctx, tx, normalizedSnapshot.EmbeddingSpace)
	if err != nil {
		return ProjectionReconciliationReport{}, err
	}
	if err := validateReconciliationTarget(target, normalizedSnapshot.Repair); err != nil {
		return ProjectionReconciliationReport{}, err
	}
	checkedAt, err := projectionDatabaseTime(ctx, tx)
	if err != nil {
		return ProjectionReconciliationReport{}, err
	}
	if checkedAt.Before(normalizedSnapshot.StartedAt) {
		return ProjectionReconciliationReport{}, fmt.Errorf("projection reconciliation start is after database time: %w", domain.ErrInvalid)
	}

	counts, err := projectionReconciliationCoverage(ctx, tx, normalizedSnapshot.EmbeddingSpace, checkedAt)
	if err != nil {
		return ProjectionReconciliationReport{}, err
	}
	report := ProjectionReconciliationReport{
		EmbeddingSpace: normalizedSnapshot.EmbeddingSpace,
		Generation:     generation,
		CheckedAt:      checkedAt,
		Counts:         counts,
		Complete:       counts.Scanned == counts.Converged,
	}
	if err := tx.Commit(ctx); err != nil {
		return ProjectionReconciliationReport{}, mapProjectionPostgresError("commit projection reconciliation finalization", err)
	}
	return report, nil
}

func projectionReconciliationFinalizationTxOptions() pgx.TxOptions {
	return pgx.TxOptions{IsoLevel: pgx.ReadCommitted}
}

type projectionReconciliationKey struct {
	TenantID string
	UserID   string
	MemoryID string
}

type projectionReconciliationScope struct {
	TenantID string
	UserID   string
}

type projectionReconciliationJob struct {
	ID                    int64
	ExpectedMemoryVersion int
	State                 ProjectionJobState
}

type projectionCoverageClass uint8

const (
	projectionCoverageConverged projectionCoverageClass = iota + 1
	projectionCoverageMissingJob
	projectionCoverageInFlight
	projectionCoverageDead
	projectionCoverageCancelled
	projectionCoverageSucceededMissingEmbedding
	projectionCoverageContentHashMismatch
	projectionCoverageVersionInvariant
)

func validateProjectionReconciliationSnapshot(snapshot ProjectionReconciliationSnapshot) (ProjectionReconciliationSnapshot, error) {
	if err := validateProjectionIdentifier("embedding space", snapshot.EmbeddingSpace); err != nil {
		return ProjectionReconciliationSnapshot{}, err
	}
	if snapshot.Generation < 0 {
		return ProjectionReconciliationSnapshot{}, fmt.Errorf("projection reconciliation generation is negative: %w", domain.ErrInvalid)
	}
	if snapshot.StartedAt.IsZero() {
		return ProjectionReconciliationSnapshot{}, fmt.Errorf("projection reconciliation started_at is required: %w", domain.ErrInvalid)
	}
	snapshot.StartedAt = canonicalProjectionTime(snapshot.StartedAt)
	return snapshot, nil
}

func validateProjectionReconciliationCursor(cursor ProjectionReconciliationCursor) error {
	values := []string{cursor.TenantID, cursor.UserID, cursor.MemoryID}
	populated := 0
	for _, value := range values {
		if value != "" {
			populated++
		}
	}
	if populated == 0 {
		return nil
	}
	if populated != len(values) {
		return fmt.Errorf("projection reconciliation cursor fields must be provided together: %w", domain.ErrInvalid)
	}
	for index, field := range []string{"tenant id", "user id", "memory id"} {
		if err := validateProjectionIdentifier("projection reconciliation cursor "+field, values[index]); err != nil {
			return err
		}
	}
	return nil
}

func projectionReconciliationChanged() error {
	return fmt.Errorf("projection reconciliation deployment changed: %w", domain.ErrConflict)
}

func lockReconciliationTarget(ctx context.Context, tx pgx.Tx, embeddingSpace string) (ProjectionTarget, error) {
	target, err := scanProjectionTarget(tx.QueryRow(ctx, projectionTargetSelect+`
		WHERE target.embedding_space = $1
		FOR SHARE OF target`, embeddingSpace))
	if errors.Is(err, domain.ErrNotFound) {
		return ProjectionTarget{}, fmt.Errorf("projection reconciliation target: %w", domain.ErrNotFound)
	}
	return target, err
}

func validateReconciliationTarget(target ProjectionTarget, repair bool) error {
	if target.State == ProjectionTargetRetired {
		return fmt.Errorf("retired projection target cannot be reconciled: %w", domain.ErrConflict)
	}
	if target.Space.DocumentVersion != embedding.MemoryCardDocumentVersion {
		return fmt.Errorf("projection reconciliation document version is unsupported: %w", domain.ErrInvariant)
	}
	if repair && (!target.EnqueueNew ||
		(target.State != ProjectionTargetShadow && target.State != ProjectionTargetServing)) {
		return fmt.Errorf("projection target does not accept reconciliation repairs: %w", domain.ErrConflict)
	}
	return nil
}

func selectProjectionReconciliationKeys(
	ctx context.Context,
	tx pgx.Tx,
	cursor ProjectionReconciliationCursor,
	limit int,
	operationAt time.Time,
) ([]projectionReconciliationKey, bool, error) {
	rows, err := tx.Query(ctx, `
		SELECT card.tenant_id, card.user_id, card.id
		FROM agent_memory.memory_cards AS card
		WHERE card.status = 'active'
		  AND (card.expires_at IS NULL OR card.expires_at > $1)
		  AND (
			$2::text = ''
			OR (card.tenant_id COLLATE "C", card.user_id COLLATE "C", card.id COLLATE "C")
			   > ($2::text COLLATE "C", $3::text COLLATE "C", $4::text COLLATE "C")
		  )
		ORDER BY card.tenant_id COLLATE "C", card.user_id COLLATE "C", card.id COLLATE "C"
		LIMIT $5`,
		operationAt,
		cursor.TenantID,
		cursor.UserID,
		cursor.MemoryID,
		limit+1,
	)
	if err != nil {
		return nil, false, mapProjectionPostgresError("select projection reconciliation page", err)
	}
	defer rows.Close()

	keys := make([]projectionReconciliationKey, 0, limit+1)
	for rows.Next() {
		var key projectionReconciliationKey
		if err := rows.Scan(&key.TenantID, &key.UserID, &key.MemoryID); err != nil {
			return nil, false, mapProjectionPostgresError("scan projection reconciliation key", err)
		}
		keys = append(keys, key)
	}
	if err := rows.Err(); err != nil {
		return nil, false, mapProjectionPostgresError("iterate projection reconciliation keys", err)
	}
	hasMore := len(keys) > limit
	if hasMore {
		keys = keys[:limit]
	}
	return keys, hasMore, nil
}

func lockProjectionReconciliationScopes(ctx context.Context, tx pgx.Tx, keys []projectionReconciliationKey) error {
	if len(keys) == 0 {
		return nil
	}
	scopeSet := make(map[projectionReconciliationScope]struct{}, len(keys))
	for _, key := range keys {
		scopeSet[projectionReconciliationScope{key.TenantID, key.UserID}] = struct{}{}
	}
	scopes := make([]projectionReconciliationScope, 0, len(scopeSet))
	for scope := range scopeSet {
		scopes = append(scopes, scope)
	}
	sort.Slice(scopes, func(left, right int) bool {
		if scopes[left].TenantID == scopes[right].TenantID {
			return scopes[left].UserID < scopes[right].UserID
		}
		return scopes[left].TenantID < scopes[right].TenantID
	})
	tenants := make([]string, len(scopes))
	users := make([]string, len(scopes))
	for index, scope := range scopes {
		tenants[index], users[index] = scope.TenantID, scope.UserID
	}

	rows, err := tx.Query(ctx, `
		SELECT scope.tenant_id, scope.user_id, scope.context_revision
		FROM unnest($1::text[], $2::text[]) AS requested(tenant_id, user_id)
		JOIN agent_memory.user_scope_state AS scope
		  ON scope.tenant_id = requested.tenant_id
		 AND scope.user_id = requested.user_id
		ORDER BY scope.tenant_id COLLATE "C", scope.user_id COLLATE "C"
		FOR UPDATE OF scope`, tenants, users)
	if err != nil {
		return mapProjectionPostgresError("lock projection reconciliation scopes", err)
	}
	defer rows.Close()

	locked := 0
	for rows.Next() {
		var tenantID, userID string
		var revision int64
		if err := rows.Scan(&tenantID, &userID, &revision); err != nil {
			return mapProjectionPostgresError("scan projection reconciliation scope", err)
		}
		if revision < 0 {
			return fmt.Errorf("projection reconciliation scope revision is negative: %w", domain.ErrInvariant)
		}
		locked++
	}
	if err := rows.Err(); err != nil {
		return mapProjectionPostgresError("iterate projection reconciliation scopes", err)
	}
	if locked != len(scopes) {
		return fmt.Errorf("projection reconciliation scope disappeared: %w", domain.ErrInvariant)
	}
	return nil
}

func lockProjectionReconciliationCards(
	ctx context.Context,
	tx pgx.Tx,
	keys []projectionReconciliationKey,
	operationAt time.Time,
) ([]domain.MemoryCard, error) {
	if len(keys) == 0 {
		return []domain.MemoryCard{}, nil
	}
	tenants := make([]string, len(keys))
	users := make([]string, len(keys))
	memories := make([]string, len(keys))
	for index, key := range keys {
		tenants[index], users[index], memories[index] = key.TenantID, key.UserID, key.MemoryID
	}
	rows, err := tx.Query(ctx, `
		SELECT card.id, card.tenant_id, card.user_id, card.kind, card.category,
		       card.memory_key, card.value, card.person, card.relationship,
		       card.backstory, card.version, card.status, card.created_at,
		       card.expires_at
		FROM unnest($1::text[], $2::text[], $3::text[])
		     AS requested(tenant_id, user_id, memory_id)
		JOIN agent_memory.memory_cards AS card
		  ON card.tenant_id = requested.tenant_id
		 AND card.user_id = requested.user_id
		 AND card.id = requested.memory_id
		WHERE card.status = 'active'
		  AND (card.expires_at IS NULL OR card.expires_at > $4)
		ORDER BY card.tenant_id COLLATE "C", card.user_id COLLATE "C", card.id COLLATE "C"
		FOR UPDATE OF card`, tenants, users, memories, operationAt)
	if err != nil {
		return nil, mapProjectionPostgresError("lock projection reconciliation cards", err)
	}
	defer rows.Close()

	cards := make([]domain.MemoryCard, 0, len(keys))
	for rows.Next() {
		card, err := scanProjectionReconciliationCard(rows)
		if err != nil {
			return nil, err
		}
		cards = append(cards, card)
	}
	if err := rows.Err(); err != nil {
		return nil, mapProjectionPostgresError("iterate projection reconciliation cards", err)
	}
	return cards, nil
}

func scanProjectionReconciliationCard(row rowScanner) (domain.MemoryCard, error) {
	var card domain.MemoryCard
	var kind, status string
	var expiresAt pgtype.Timestamptz
	if err := row.Scan(
		&card.ID,
		&card.TenantID,
		&card.UserID,
		&kind,
		&card.Category,
		&card.Key,
		&card.Value,
		&card.Person,
		&card.Relationship,
		&card.Backstory,
		&card.Version,
		&status,
		&card.CreatedAt,
		&expiresAt,
	); err != nil {
		return domain.MemoryCard{}, mapProjectionPostgresError("scan projection reconciliation card", err)
	}
	card.Kind = domain.MemoryKind(kind)
	card.Status = domain.MemoryStatus(status)
	if expiresAt.Valid {
		value := canonicalProjectionTime(expiresAt.Time)
		card.ExpiresAt = &value
	}
	if card.ID == "" || card.TenantID == "" || card.UserID == "" || card.Version < 1 || card.Status != domain.MemoryActive {
		return domain.MemoryCard{}, fmt.Errorf("stored projection reconciliation card is invalid: %w", domain.ErrInvariant)
	}
	return card, nil
}

func lockProjectionReconciliationJob(
	ctx context.Context,
	tx pgx.Tx,
	tenantID, userID, memoryID, embeddingSpace string,
) (projectionReconciliationJob, bool, error) {
	var job projectionReconciliationJob
	var state string
	err := tx.QueryRow(ctx, `
		SELECT id, expected_memory_version, state
		FROM agent_memory.embedding_projection_jobs
		WHERE tenant_id = $1 AND user_id = $2 AND memory_id = $3
		  AND embedding_space = $4
		FOR UPDATE`, tenantID, userID, memoryID, embeddingSpace).Scan(
		&job.ID,
		&job.ExpectedMemoryVersion,
		&state,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return projectionReconciliationJob{}, false, nil
	}
	if err != nil {
		return projectionReconciliationJob{}, false, mapProjectionPostgresError("lock projection reconciliation job", err)
	}
	job.State = ProjectionJobState(state)
	if job.ID < 1 || job.ExpectedMemoryVersion < 1 || !validProjectionJobState(job.State) {
		return projectionReconciliationJob{}, false, fmt.Errorf("stored projection reconciliation job is invalid: %w", domain.ErrInvariant)
	}
	return job, true, nil
}

func lockProjectionReconciliationEmbedding(
	ctx context.Context,
	tx pgx.Tx,
	tenantID, userID, memoryID, embeddingSpace string,
) (string, bool, error) {
	var contentHash string
	err := tx.QueryRow(ctx, `
		SELECT content_sha256
		FROM agent_memory.memory_embeddings
		WHERE tenant_id = $1 AND user_id = $2 AND memory_id = $3
		  AND embedding_space = $4
		FOR UPDATE`, tenantID, userID, memoryID, embeddingSpace).Scan(&contentHash)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", false, nil
	}
	if err != nil {
		return "", false, mapProjectionPostgresError("lock projection reconciliation embedding", err)
	}
	normalized, err := normalizeSHA256("stored projection content hash", contentHash)
	if err != nil || normalized != contentHash {
		return "", false, fmt.Errorf("stored projection content hash is invalid: %w", domain.ErrInvariant)
	}
	return contentHash, true, nil
}

func classifyProjectionCoverage(
	memoryVersion int,
	job projectionReconciliationJob,
	jobExists bool,
	embeddingHash string,
	embeddingExists bool,
	expectedHash string,
) (projectionCoverageClass, bool, error) {
	if memoryVersion < 1 {
		return 0, false, fmt.Errorf("projection reconciliation memory version is invalid: %w", domain.ErrInvariant)
	}
	if _, err := normalizeSHA256("expected projection content hash", expectedHash); err != nil {
		return 0, false, fmt.Errorf("projection reconciliation expected hash is invalid: %w", domain.ErrInvariant)
	}
	if embeddingExists {
		if normalized, err := normalizeSHA256("projection content hash", embeddingHash); err != nil || normalized != embeddingHash {
			return 0, false, fmt.Errorf("projection reconciliation embedding hash is invalid: %w", domain.ErrInvariant)
		}
	}
	staleEmbedding := embeddingExists && embeddingHash != expectedHash
	if !jobExists {
		return projectionCoverageMissingJob, staleEmbedding, nil
	}
	if job.ExpectedMemoryVersion != memoryVersion {
		return projectionCoverageVersionInvariant, staleEmbedding, fmt.Errorf(
			"projection job expected memory version does not match its card: %w",
			domain.ErrInvariant,
		)
	}
	switch job.State {
	case ProjectionJobPending, ProjectionJobLeased, ProjectionJobRetry:
		return projectionCoverageInFlight, staleEmbedding, nil
	case ProjectionJobDead:
		return projectionCoverageDead, staleEmbedding, nil
	case ProjectionJobCancelled:
		return projectionCoverageCancelled, staleEmbedding, nil
	case ProjectionJobSucceeded:
		if !embeddingExists {
			return projectionCoverageSucceededMissingEmbedding, false, nil
		}
		if staleEmbedding {
			return projectionCoverageContentHashMismatch, true, nil
		}
		return projectionCoverageConverged, false, nil
	default:
		return 0, false, fmt.Errorf("projection reconciliation job state is invalid: %w", domain.ErrInvariant)
	}
}

func (counts *ProjectionReconciliationCounts) add(class projectionCoverageClass) {
	counts.Scanned++
	switch class {
	case projectionCoverageConverged:
		counts.Converged++
	case projectionCoverageMissingJob:
		counts.MissingJob++
	case projectionCoverageInFlight:
		counts.InFlight++
	case projectionCoverageDead:
		counts.Dead++
	case projectionCoverageCancelled:
		counts.Cancelled++
	case projectionCoverageSucceededMissingEmbedding:
		counts.SucceededMissingEmbedding++
	case projectionCoverageContentHashMismatch:
		counts.ContentHashMismatch++
	case projectionCoverageVersionInvariant:
		counts.VersionInvariant++
	}
}

func (counts ProjectionReconciliationCounts) validate() error {
	classified := counts.Converged + counts.MissingJob + counts.InFlight + counts.Dead +
		counts.Cancelled + counts.SucceededMissingEmbedding + counts.ContentHashMismatch +
		counts.VersionInvariant
	if counts.Scanned < 0 || counts.Converged < 0 || counts.MissingJob < 0 ||
		counts.InFlight < 0 || counts.Dead < 0 || counts.Cancelled < 0 ||
		counts.SucceededMissingEmbedding < 0 || counts.ContentHashMismatch < 0 ||
		counts.VersionInvariant < 0 || classified != counts.Scanned {
		return fmt.Errorf("projection reconciliation counts are inconsistent: %w", domain.ErrInvariant)
	}
	return nil
}

func enqueueProjectionReconciliationJob(
	ctx context.Context,
	tx pgx.Tx,
	card domain.MemoryCard,
	embeddingSpace string,
	operationAt time.Time,
) error {
	commandTag, err := tx.Exec(ctx, `
		INSERT INTO agent_memory.embedding_projection_jobs (
			tenant_id, user_id, memory_id, embedding_space,
			expected_memory_version, state, available_at, created_at, updated_at
		) VALUES ($1, $2, $3, $4, $5, 'pending', $6, $6, $6)
		ON CONFLICT (tenant_id, user_id, memory_id, embedding_space) DO NOTHING`,
		card.TenantID,
		card.UserID,
		card.ID,
		embeddingSpace,
		card.Version,
		operationAt,
	)
	if err != nil {
		return mapProjectionPostgresError("enqueue reconciled projection job", err)
	}
	if commandTag.RowsAffected() != 1 {
		return fmt.Errorf("projection reconciliation job appeared after its lock point: %w", domain.ErrConflict)
	}
	return nil
}

func resetProjectionReconciliationJob(ctx context.Context, tx pgx.Tx, jobID int64, operationAt time.Time) error {
	commandTag, err := tx.Exec(ctx, `
		UPDATE agent_memory.embedding_projection_jobs
		SET state = 'pending', attempt_count = 0, available_at = $2,
		    lease_owner = NULL, lease_until = NULL,
		    last_error_code = NULL, last_error_at = NULL,
		    updated_at = $2, completed_at = NULL
		WHERE id = $1 AND state = 'succeeded'`, jobID, operationAt)
	if err != nil {
		return mapProjectionPostgresError("reset reconciled projection job", err)
	}
	if commandTag.RowsAffected() != 1 {
		return fmt.Errorf("projection reconciliation job changed after its lock point: %w", domain.ErrConflict)
	}
	return nil
}

func deleteProjectionReconciliationEmbedding(
	ctx context.Context,
	tx pgx.Tx,
	tenantID, userID, memoryID, embeddingSpace string,
) (bool, error) {
	commandTag, err := tx.Exec(ctx, `
		DELETE FROM agent_memory.memory_embeddings
		WHERE tenant_id = $1 AND user_id = $2 AND memory_id = $3
		  AND embedding_space = $4`, tenantID, userID, memoryID, embeddingSpace)
	if err != nil {
		return false, mapProjectionPostgresError("delete stale reconciled projection embedding", err)
	}
	if commandTag.RowsAffected() > 1 {
		return false, fmt.Errorf("projection reconciliation deleted multiple embeddings: %w", domain.ErrInvariant)
	}
	return commandTag.RowsAffected() == 1, nil
}

func advanceProjectionReconciliationRevisions(
	ctx context.Context,
	tx pgx.Tx,
	scopeSet map[projectionReconciliationScope]struct{},
	operationAt time.Time,
) error {
	scopes := make([]projectionReconciliationScope, 0, len(scopeSet))
	for scope := range scopeSet {
		scopes = append(scopes, scope)
	}
	sort.Slice(scopes, func(left, right int) bool {
		if scopes[left].TenantID == scopes[right].TenantID {
			return scopes[left].UserID < scopes[right].UserID
		}
		return scopes[left].TenantID < scopes[right].TenantID
	})
	for _, scope := range scopes {
		commandTag, err := tx.Exec(ctx, `
			UPDATE agent_memory.user_scope_state
			SET context_revision = context_revision + 1, updated_at = $3
			WHERE tenant_id = $1 AND user_id = $2`, scope.TenantID, scope.UserID, operationAt)
		if err != nil {
			return mapProjectionPostgresError("advance reconciled projection revision", err)
		}
		if commandTag.RowsAffected() != 1 {
			return fmt.Errorf("projection reconciliation scope disappeared during revision update: %w", domain.ErrInvariant)
		}
	}
	return nil
}

func projectionReconciliationCoverage(
	ctx context.Context,
	tx pgx.Tx,
	embeddingSpace string,
	checkedAt time.Time,
) (ProjectionReconciliationCounts, error) {
	rows, err := tx.Query(ctx, `
		SELECT card.id, card.tenant_id, card.user_id, card.kind, card.category,
		       card.memory_key, card.value, card.person, card.relationship,
		       card.backstory, card.version, card.status, card.created_at,
		       card.expires_at,
		       job.id, job.expected_memory_version, job.state,
		       embedding.content_sha256
		FROM agent_memory.memory_cards AS card
		LEFT JOIN agent_memory.embedding_projection_jobs AS job
		  ON job.tenant_id = card.tenant_id
		 AND job.user_id = card.user_id
		 AND job.memory_id = card.id
		 AND job.embedding_space = $1
		LEFT JOIN agent_memory.memory_embeddings AS embedding
		  ON embedding.tenant_id = card.tenant_id
		 AND embedding.user_id = card.user_id
		 AND embedding.memory_id = card.id
		 AND embedding.embedding_space = $1
		WHERE card.status = 'active'
		  AND (card.expires_at IS NULL OR card.expires_at > $2)
		ORDER BY card.tenant_id COLLATE "C", card.user_id COLLATE "C", card.id COLLATE "C"`,
		embeddingSpace,
		checkedAt,
	)
	if err != nil {
		return ProjectionReconciliationCounts{}, mapProjectionPostgresError("scan projection reconciliation coverage", err)
	}
	defer rows.Close()

	var counts ProjectionReconciliationCounts
	for rows.Next() {
		var card domain.MemoryCard
		var kind, status string
		var expiresAt pgtype.Timestamptz
		var jobID pgtype.Int8
		var expectedVersion pgtype.Int4
		var jobState, embeddingHash pgtype.Text
		if err := rows.Scan(
			&card.ID,
			&card.TenantID,
			&card.UserID,
			&kind,
			&card.Category,
			&card.Key,
			&card.Value,
			&card.Person,
			&card.Relationship,
			&card.Backstory,
			&card.Version,
			&status,
			&card.CreatedAt,
			&expiresAt,
			&jobID,
			&expectedVersion,
			&jobState,
			&embeddingHash,
		); err != nil {
			return ProjectionReconciliationCounts{}, mapProjectionPostgresError("read projection reconciliation coverage", err)
		}
		card.Kind = domain.MemoryKind(kind)
		card.Status = domain.MemoryStatus(status)
		if expiresAt.Valid {
			value := canonicalProjectionTime(expiresAt.Time)
			card.ExpiresAt = &value
		}
		if card.Version < 1 || card.Status != domain.MemoryActive {
			return ProjectionReconciliationCounts{}, fmt.Errorf("stored coverage card is invalid: %w", domain.ErrInvariant)
		}

		job := projectionReconciliationJob{}
		jobExists := jobID.Valid
		if jobExists {
			if !expectedVersion.Valid || !jobState.Valid {
				return ProjectionReconciliationCounts{}, fmt.Errorf("stored coverage job is partial: %w", domain.ErrInvariant)
			}
			job.ID = jobID.Int64
			job.ExpectedMemoryVersion = int(expectedVersion.Int32)
			job.State = ProjectionJobState(jobState.String)
			if job.ID < 1 || job.ExpectedMemoryVersion < 1 || !validProjectionJobState(job.State) {
				return ProjectionReconciliationCounts{}, fmt.Errorf("stored coverage job is invalid: %w", domain.ErrInvariant)
			}
		} else if expectedVersion.Valid || jobState.Valid {
			return ProjectionReconciliationCounts{}, fmt.Errorf("stored coverage job is partial: %w", domain.ErrInvariant)
		}
		if embeddingHash.Valid {
			normalized, err := normalizeSHA256("stored coverage embedding hash", embeddingHash.String)
			if err != nil || normalized != embeddingHash.String {
				return ProjectionReconciliationCounts{}, fmt.Errorf("stored coverage embedding hash is invalid: %w", domain.ErrInvariant)
			}
		}

		classification, _, classificationErr := classifyProjectionCoverage(
			card.Version,
			job,
			jobExists,
			embeddingHash.String,
			embeddingHash.Valid,
			embedding.MemoryCardDocumentV1SHA256(card),
		)
		if classificationErr != nil {
			if errors.Is(classificationErr, domain.ErrInvariant) &&
				jobExists && job.ExpectedMemoryVersion != card.Version {
				counts.add(projectionCoverageVersionInvariant)
				continue
			}
			return ProjectionReconciliationCounts{}, classificationErr
		}
		counts.add(classification)
	}
	if err := rows.Err(); err != nil {
		return ProjectionReconciliationCounts{}, mapProjectionPostgresError("iterate projection reconciliation coverage", err)
	}
	if err := counts.validate(); err != nil {
		return ProjectionReconciliationCounts{}, err
	}
	return counts, nil
}
