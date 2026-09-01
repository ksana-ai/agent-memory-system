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

// PromoteProjectionCommand atomically changes the one serving embedding
// space. ExpectedFrom is an explicit compare-and-swap precondition: the empty
// string means that the caller expects no serving space, rather than "any".
// A rollback is a new promotion whose ExpectedFrom and ToSpace are reversed;
// it is subject to the same live coverage proof as a forward promotion.
type PromoteProjectionCommand struct {
	OperationID  string
	ExpectedFrom string
	ToSpace      string
	AllowEmpty   bool
}

// ProjectionPromotionReceipt contains only aggregate deployment facts. It is
// durable and returned for exact OperationID retries. CutoffAt is the database
// time used for serviceability and coverage; PromotedAt is the later database
// time at which the target swap was recorded.
type ProjectionPromotionReceipt struct {
	OperationID        string
	FromSpace          string
	ToSpace            string
	AllowEmpty         bool
	LiveScopeCount     int64
	LiveCardCount      int64
	CoveredCardCount   int64
	PreviousGeneration int64
	Generation         int64
	CutoffAt           time.Time
	PromotedAt         time.Time
}

// ServingProjectionState is a deployment-fenced read of the current serving
// target and generation. Target is nil when no serving space is configured.
type ServingProjectionState struct {
	Target     *ProjectionTarget
	Generation int64
}

type projectionPromotionKey struct {
	tenantID string
	userID   string
	memoryID string
}

type projectionPromotionJob struct {
	expectedMemoryVersion int
	state                 ProjectionJobState
}

type projectionPromotionCard struct {
	key     projectionPromotionKey
	version int
	memory  domain.MemoryCard
}

// CurrentServingProjection reads the serving target while holding the
// deployment gate, so a public promotion or target mutation cannot interleave
// with the result.
func (s *Store) CurrentServingProjection(ctx context.Context) (ServingProjectionState, error) {
	if err := s.ready(); err != nil {
		return ServingProjectionState{}, err
	}
	tx, err := s.pool.BeginTx(ctx, projectionPromotionTxOptions())
	if err != nil {
		return ServingProjectionState{}, mapProjectionPostgresError("begin serving projection read", err)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()

	generation, err := lockProjectionDeploymentShared(ctx, tx)
	if err != nil {
		return ServingProjectionState{}, err
	}
	target, found, err := readCurrentServingProjectionTarget(ctx, tx, true)
	if err != nil {
		return ServingProjectionState{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return ServingProjectionState{}, mapProjectionPostgresError("commit serving projection read", err)
	}
	result := ServingProjectionState{Generation: generation}
	if found {
		result.Target = &target
	}
	return result, nil
}

func projectionPromotionTxOptions() pgx.TxOptions {
	// Promotion intentionally uses a fresh statement snapshot after waiting for
	// every active-card scope lock. Pin READ COMMITTED rather than inheriting a
	// database/session default that could make that post-lock read stale.
	return pgx.TxOptions{IsoLevel: pgx.ReadCommitted}
}

// ProjectionPromotionByOperationID loads one immutable promotion receipt.
// A missing operation returns domain.ErrNotFound; callers may use that result
// to decide whether a live provider gate is required before first execution.
func (s *Store) ProjectionPromotionByOperationID(ctx context.Context, operationID string) (ProjectionPromotionReceipt, error) {
	if err := validatePromotionOperationID(operationID); err != nil {
		return ProjectionPromotionReceipt{}, err
	}
	if err := s.ready(); err != nil {
		return ProjectionPromotionReceipt{}, err
	}
	receipt, found, err := readProjectionPromotionReceipt(ctx, s.pool, operationID)
	if err != nil {
		return ProjectionPromotionReceipt{}, err
	}
	if !found {
		return ProjectionPromotionReceipt{}, fmt.Errorf("projection promotion receipt: %w", domain.ErrNotFound)
	}
	return receipt, nil
}

// PromoteProjection proves complete projection coverage at one database-clock
// cutoff and atomically swaps the serving target. The global lock order is
// deployment -> active-card scopes (C order) -> targets (C order) -> jobs ->
// cards -> embeddings. This matches worker scope-before-target ordering and
// prevents approvals, deletion, finalization, or another promotion from
// invalidating a successful receipt. Scope locking conservatively includes
// active-but-expired cards; only serviceable scopes receive revision bumps.
func (s *Store) PromoteProjection(ctx context.Context, command PromoteProjectionCommand) (ProjectionPromotionReceipt, error) {
	normalized, err := validatePromoteProjectionCommand(command)
	if err != nil {
		return ProjectionPromotionReceipt{}, err
	}
	if err := s.ready(); err != nil {
		return ProjectionPromotionReceipt{}, err
	}

	tx, err := s.pool.BeginTx(ctx, projectionPromotionTxOptions())
	if err != nil {
		return ProjectionPromotionReceipt{}, mapProjectionPostgresError("begin projection promotion", err)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()

	previousGeneration, err := lockProjectionDeploymentExclusive(ctx, tx)
	if err != nil {
		return ProjectionPromotionReceipt{}, err
	}
	if receipt, found, readErr := readProjectionPromotionReceipt(ctx, tx, normalized.OperationID); readErr != nil {
		return ProjectionPromotionReceipt{}, readErr
	} else if found {
		if !promotionReceiptMatchesCommand(receipt, normalized) {
			return ProjectionPromotionReceipt{}, fmt.Errorf("projection promotion operation parameters differ: %w", domain.ErrConflict)
		}
		if err := tx.Commit(ctx); err != nil {
			return ProjectionPromotionReceipt{}, mapProjectionPostgresError("commit projection promotion retry", err)
		}
		return receipt, nil
	}

	// Lock every scope that currently has an active card, including a card that
	// is already expired. Only after every competing scope writer has finished
	// do we take the serviceability cutoff. This avoids freezing an old cutoff
	// while waiting for a worker or deletion transaction.
	lockedScopes, err := lockProjectionPromotionScopes(ctx, tx)
	if err != nil {
		return ProjectionPromotionReceipt{}, err
	}
	cutoffAt, err := projectionDatabaseTime(ctx, tx)
	if err != nil {
		return ProjectionPromotionReceipt{}, err
	}
	current, currentFound, err := readCurrentServingProjectionTarget(ctx, tx, false)
	if err != nil {
		return ProjectionPromotionReceipt{}, err
	}
	currentSpace := ""
	if currentFound {
		currentSpace = current.Space.ID
	}
	if currentSpace != normalized.ExpectedFrom {
		return ProjectionPromotionReceipt{}, fmt.Errorf("current serving projection does not match expected source: %w", domain.ErrConflict)
	}

	spaces := []string{normalized.ToSpace}
	if currentSpace != "" {
		spaces = append(spaces, currentSpace)
	}
	targets, err := lockProjectionPromotionTargets(ctx, tx, spaces)
	if err != nil {
		return ProjectionPromotionReceipt{}, err
	}
	toTarget, found := targets[normalized.ToSpace]
	if !found {
		return ProjectionPromotionReceipt{}, fmt.Errorf("promotion destination projection target: %w", domain.ErrNotFound)
	}
	if toTarget.State != ProjectionTargetShadow || !toTarget.EnqueueNew {
		return ProjectionPromotionReceipt{}, fmt.Errorf("promotion destination must be an enqueue-enabled shadow target: %w", domain.ErrConflict)
	}
	if toTarget.Space.DocumentVersion != embedding.MemoryCardDocumentVersion {
		return ProjectionPromotionReceipt{}, fmt.Errorf("promotion destination document version is unsupported: %w", domain.ErrInvariant)
	}
	if toTarget.Space.QueryVersion != embedding.RawQueryVersion {
		return ProjectionPromotionReceipt{}, fmt.Errorf("promotion destination query version is unsupported: %w", domain.ErrInvariant)
	}
	if currentSpace != "" {
		lockedCurrent, found := targets[currentSpace]
		if !found || lockedCurrent.State != ProjectionTargetServing {
			return ProjectionPromotionReceipt{}, fmt.Errorf("serving projection changed while locked: %w", domain.ErrInvariant)
		}
	}

	jobs, err := lockProjectionPromotionJobs(ctx, tx, normalized.ToSpace, cutoffAt)
	if err != nil {
		return ProjectionPromotionReceipt{}, err
	}
	cards, err := lockProjectionPromotionCards(ctx, tx, cutoffAt)
	if err != nil {
		return ProjectionPromotionReceipt{}, err
	}
	embeddings, err := lockProjectionPromotionEmbeddings(ctx, tx, normalized.ToSpace, cutoffAt)
	if err != nil {
		return ProjectionPromotionReceipt{}, err
	}
	liveScopes, err := projectionPromotionLiveScopes(lockedScopes, cards)
	if err != nil {
		return ProjectionPromotionReceipt{}, err
	}

	liveCardCount := int64(len(cards))
	if liveCardCount == 0 && !normalized.AllowEmpty {
		return ProjectionPromotionReceipt{}, fmt.Errorf("projection promotion has no live cards: %w", domain.ErrConflict)
	}
	coveredCardCount := countCoveredProjectionPromotionCards(cards, jobs, embeddings)
	if coveredCardCount != liveCardCount {
		return ProjectionPromotionReceipt{}, fmt.Errorf("projection promotion coverage is incomplete: %w", domain.ErrConflict)
	}

	promotedAt, err := projectionDatabaseTime(ctx, tx)
	if err != nil {
		return ProjectionPromotionReceipt{}, err
	}
	targetTransitionAt := promotedAt
	for _, target := range targets {
		if !targetTransitionAt.After(target.UpdatedAt) {
			targetTransitionAt = canonicalProjectionTime(target.UpdatedAt.Add(time.Microsecond))
		}
	}
	if currentSpace != "" {
		if err := updatePromotedProjectionTarget(ctx, tx, currentSpace, ProjectionTargetShadow, targetTransitionAt); err != nil {
			return ProjectionPromotionReceipt{}, err
		}
	}
	if err := updatePromotedProjectionTarget(ctx, tx, normalized.ToSpace, ProjectionTargetServing, targetTransitionAt); err != nil {
		return ProjectionPromotionReceipt{}, err
	}
	if err := advanceProjectionPromotionRevisions(ctx, tx, liveScopes, promotedAt); err != nil {
		return ProjectionPromotionReceipt{}, err
	}
	generation, err := advanceProjectionDeploymentGeneration(ctx, tx)
	if err != nil {
		return ProjectionPromotionReceipt{}, err
	}
	if generation != previousGeneration+1 {
		return ProjectionPromotionReceipt{}, fmt.Errorf("projection promotion generation advanced unexpectedly: %w", domain.ErrInvariant)
	}
	receipt := ProjectionPromotionReceipt{
		OperationID:        normalized.OperationID,
		FromSpace:          currentSpace,
		ToSpace:            normalized.ToSpace,
		AllowEmpty:         normalized.AllowEmpty,
		LiveScopeCount:     int64(len(liveScopes)),
		LiveCardCount:      liveCardCount,
		CoveredCardCount:   coveredCardCount,
		PreviousGeneration: previousGeneration,
		Generation:         generation,
		CutoffAt:           cutoffAt,
		PromotedAt:         promotedAt,
	}
	if err := insertProjectionPromotionReceipt(ctx, tx, receipt); err != nil {
		return ProjectionPromotionReceipt{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return ProjectionPromotionReceipt{}, mapProjectionPostgresError("commit projection promotion", err)
	}
	return receipt, nil
}

func validatePromoteProjectionCommand(command PromoteProjectionCommand) (PromoteProjectionCommand, error) {
	if err := validatePromotionOperationID(command.OperationID); err != nil {
		return PromoteProjectionCommand{}, err
	}
	if command.ExpectedFrom != "" {
		if err := validateProjectionIdentifier("expected serving projection", command.ExpectedFrom); err != nil {
			return PromoteProjectionCommand{}, err
		}
	}
	if err := validateProjectionIdentifier("promotion destination projection", command.ToSpace); err != nil {
		return PromoteProjectionCommand{}, err
	}
	if command.ExpectedFrom == command.ToSpace {
		return PromoteProjectionCommand{}, fmt.Errorf("promotion source and destination must differ: %w", domain.ErrInvalid)
	}
	return command, nil
}

func validatePromotionOperationID(value string) error {
	if len(value) != 36 {
		return fmt.Errorf("promotion operation id must be a canonical lowercase UUID: %w", domain.ErrInvalid)
	}
	for index, character := range value {
		switch index {
		case 8, 13, 18, 23:
			if character != '-' {
				return fmt.Errorf("promotion operation id must be a canonical lowercase UUID: %w", domain.ErrInvalid)
			}
		default:
			if !((character >= '0' && character <= '9') || (character >= 'a' && character <= 'f')) {
				return fmt.Errorf("promotion operation id must be a canonical lowercase UUID: %w", domain.ErrInvalid)
			}
		}
	}
	return nil
}

func readCurrentServingProjectionTarget(ctx context.Context, queryer rowQueryer, forShare bool) (ProjectionTarget, bool, error) {
	query := projectionTargetSelect + ` WHERE target.state = 'serving'`
	if forShare {
		query += ` FOR SHARE OF target`
	}
	target, err := scanProjectionTarget(queryer.QueryRow(ctx, query))
	if errors.Is(err, domain.ErrNotFound) {
		return ProjectionTarget{}, false, nil
	}
	if err != nil {
		return ProjectionTarget{}, false, err
	}
	return target, true, nil
}

func readProjectionPromotionReceipt(ctx context.Context, queryer rowQueryer, operationID string) (ProjectionPromotionReceipt, bool, error) {
	var receipt ProjectionPromotionReceipt
	err := queryer.QueryRow(ctx, `
		SELECT operation_id, COALESCE(from_embedding_space, ''), to_embedding_space,
		       allow_empty, live_scope_count, live_card_count, covered_card_count,
		       previous_generation, generation, cutoff_at, promoted_at
		FROM agent_memory.embedding_projection_promotions
		WHERE operation_id = $1`, operationID).Scan(
		&receipt.OperationID,
		&receipt.FromSpace,
		&receipt.ToSpace,
		&receipt.AllowEmpty,
		&receipt.LiveScopeCount,
		&receipt.LiveCardCount,
		&receipt.CoveredCardCount,
		&receipt.PreviousGeneration,
		&receipt.Generation,
		&receipt.CutoffAt,
		&receipt.PromotedAt,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return ProjectionPromotionReceipt{}, false, nil
	}
	if err != nil {
		return ProjectionPromotionReceipt{}, false, mapProjectionPostgresError("load projection promotion receipt", err)
	}
	receipt.CutoffAt = canonicalProjectionTime(receipt.CutoffAt)
	receipt.PromotedAt = canonicalProjectionTime(receipt.PromotedAt)
	if err := validateStoredProjectionPromotionReceipt(receipt); err != nil {
		return ProjectionPromotionReceipt{}, false, err
	}
	return receipt, true, nil
}

func validateStoredProjectionPromotionReceipt(receipt ProjectionPromotionReceipt) error {
	if _, err := validatePromoteProjectionCommand(PromoteProjectionCommand{
		OperationID:  receipt.OperationID,
		ExpectedFrom: receipt.FromSpace,
		ToSpace:      receipt.ToSpace,
		AllowEmpty:   receipt.AllowEmpty,
	}); err != nil {
		return fmt.Errorf("stored projection promotion receipt is invalid: %w", domain.ErrInvariant)
	}
	if receipt.LiveScopeCount < 0 || receipt.LiveCardCount < 0 || receipt.LiveScopeCount > receipt.LiveCardCount ||
		receipt.CoveredCardCount != receipt.LiveCardCount ||
		(!receipt.AllowEmpty && receipt.LiveCardCount == 0) ||
		receipt.PreviousGeneration < 0 || receipt.Generation != receipt.PreviousGeneration+1 ||
		receipt.CutoffAt.IsZero() || receipt.PromotedAt.IsZero() || receipt.PromotedAt.Before(receipt.CutoffAt) {
		return fmt.Errorf("stored projection promotion receipt fields are invalid: %w", domain.ErrInvariant)
	}
	return nil
}

func promotionReceiptMatchesCommand(receipt ProjectionPromotionReceipt, command PromoteProjectionCommand) bool {
	return receipt.OperationID == command.OperationID &&
		receipt.FromSpace == command.ExpectedFrom &&
		receipt.ToSpace == command.ToSpace &&
		receipt.AllowEmpty == command.AllowEmpty
}

func lockProjectionPromotionScopes(ctx context.Context, tx pgx.Tx) ([]projectionPromotionKey, error) {
	rows, err := tx.Query(ctx, `
		SELECT scope.tenant_id, scope.user_id
		FROM agent_memory.user_scope_state AS scope
		WHERE EXISTS (
			SELECT 1
			FROM agent_memory.memory_cards AS card
			WHERE card.tenant_id = scope.tenant_id
			  AND card.user_id = scope.user_id
			  AND card.status = 'active'
		)
		ORDER BY scope.tenant_id COLLATE "C", scope.user_id COLLATE "C"
		FOR UPDATE OF scope`)
	if err != nil {
		return nil, mapProjectionPostgresError("lock projection promotion scopes", err)
	}
	defer rows.Close()
	scopes := make([]projectionPromotionKey, 0)
	for rows.Next() {
		var scope projectionPromotionKey
		if err := rows.Scan(&scope.tenantID, &scope.userID); err != nil {
			return nil, mapProjectionPostgresError("scan projection promotion scope", err)
		}
		scopes = append(scopes, scope)
	}
	if err := rows.Err(); err != nil {
		return nil, mapProjectionPostgresError("iterate projection promotion scopes", err)
	}
	return scopes, nil
}

func lockProjectionPromotionTargets(ctx context.Context, tx pgx.Tx, spaces []string) (map[string]ProjectionTarget, error) {
	spaces = append([]string(nil), spaces...)
	sort.Strings(spaces)
	if len(spaces) == 2 && spaces[0] == spaces[1] {
		return nil, fmt.Errorf("projection promotion target set is duplicated: %w", domain.ErrInvalid)
	}
	rows, err := tx.Query(ctx, projectionTargetSelect+`
		WHERE target.embedding_space = ANY($1::text[])
		ORDER BY target.embedding_space COLLATE "C"
		FOR UPDATE OF target`, spaces)
	if err != nil {
		return nil, mapProjectionPostgresError("lock projection promotion targets", err)
	}
	defer rows.Close()
	targets := make(map[string]ProjectionTarget, len(spaces))
	for rows.Next() {
		target, scanErr := scanProjectionTarget(rows)
		if scanErr != nil {
			return nil, scanErr
		}
		targets[target.Space.ID] = target
	}
	if err := rows.Err(); err != nil {
		return nil, mapProjectionPostgresError("iterate projection promotion targets", err)
	}
	return targets, nil
}

func lockProjectionPromotionJobs(ctx context.Context, tx pgx.Tx, embeddingSpace string, cutoffAt time.Time) (map[projectionPromotionKey]projectionPromotionJob, error) {
	rows, err := tx.Query(ctx, `
		SELECT job.tenant_id, job.user_id, job.memory_id,
		       job.expected_memory_version, job.state
		FROM agent_memory.embedding_projection_jobs AS job
		JOIN agent_memory.memory_cards AS card
		  ON card.tenant_id = job.tenant_id
		 AND card.user_id = job.user_id
		 AND card.id = job.memory_id
		WHERE job.embedding_space = $1
		  AND card.status = 'active'
		  AND (card.expires_at IS NULL OR card.expires_at > $2)
		ORDER BY job.tenant_id COLLATE "C", job.user_id COLLATE "C", job.memory_id COLLATE "C"
		FOR UPDATE OF job`, embeddingSpace, cutoffAt)
	if err != nil {
		return nil, mapProjectionPostgresError("lock projection promotion jobs", err)
	}
	defer rows.Close()
	jobs := make(map[projectionPromotionKey]projectionPromotionJob)
	for rows.Next() {
		var key projectionPromotionKey
		var job projectionPromotionJob
		var state string
		if err := rows.Scan(&key.tenantID, &key.userID, &key.memoryID, &job.expectedMemoryVersion, &state); err != nil {
			return nil, mapProjectionPostgresError("scan projection promotion job", err)
		}
		job.state = ProjectionJobState(state)
		if !validProjectionJobState(job.state) || job.expectedMemoryVersion < 1 {
			return nil, fmt.Errorf("stored projection promotion job is invalid: %w", domain.ErrInvariant)
		}
		jobs[key] = job
	}
	if err := rows.Err(); err != nil {
		return nil, mapProjectionPostgresError("iterate projection promotion jobs", err)
	}
	return jobs, nil
}

func lockProjectionPromotionCards(ctx context.Context, tx pgx.Tx, cutoffAt time.Time) ([]projectionPromotionCard, error) {
	rows, err := tx.Query(ctx, `
		SELECT card.tenant_id, card.user_id, card.id, card.version, card.kind,
		       card.category, card.memory_key, card.value, card.person,
		       card.relationship, card.backstory
		FROM agent_memory.memory_cards AS card
		WHERE card.status = 'active'
		  AND (card.expires_at IS NULL OR card.expires_at > $1)
		ORDER BY card.tenant_id COLLATE "C", card.user_id COLLATE "C", card.id COLLATE "C"
		FOR SHARE OF card`, cutoffAt)
	if err != nil {
		return nil, mapProjectionPostgresError("lock projection promotion cards", err)
	}
	defer rows.Close()
	cards := make([]projectionPromotionCard, 0)
	for rows.Next() {
		var card projectionPromotionCard
		var kind string
		if err := rows.Scan(
			&card.key.tenantID,
			&card.key.userID,
			&card.key.memoryID,
			&card.version,
			&kind,
			&card.memory.Category,
			&card.memory.Key,
			&card.memory.Value,
			&card.memory.Person,
			&card.memory.Relationship,
			&card.memory.Backstory,
		); err != nil {
			return nil, mapProjectionPostgresError("scan projection promotion card", err)
		}
		card.memory.Kind = domain.MemoryKind(kind)
		if card.version < 1 {
			return nil, fmt.Errorf("stored projection promotion card version is invalid: %w", domain.ErrInvariant)
		}
		cards = append(cards, card)
	}
	if err := rows.Err(); err != nil {
		return nil, mapProjectionPostgresError("iterate projection promotion cards", err)
	}
	return cards, nil
}

func lockProjectionPromotionEmbeddings(ctx context.Context, tx pgx.Tx, embeddingSpace string, cutoffAt time.Time) (map[projectionPromotionKey]string, error) {
	rows, err := tx.Query(ctx, `
		SELECT value.tenant_id, value.user_id, value.memory_id, value.content_sha256
		FROM agent_memory.memory_embeddings AS value
		JOIN agent_memory.memory_cards AS card
		  ON card.tenant_id = value.tenant_id
		 AND card.user_id = value.user_id
		 AND card.id = value.memory_id
		WHERE value.embedding_space = $1
		  AND card.status = 'active'
		  AND (card.expires_at IS NULL OR card.expires_at > $2)
		ORDER BY value.tenant_id COLLATE "C", value.user_id COLLATE "C", value.memory_id COLLATE "C"
		FOR SHARE OF value`, embeddingSpace, cutoffAt)
	if err != nil {
		return nil, mapProjectionPostgresError("lock projection promotion embeddings", err)
	}
	defer rows.Close()
	embeddings := make(map[projectionPromotionKey]string)
	for rows.Next() {
		var key projectionPromotionKey
		var contentSHA256 string
		if err := rows.Scan(&key.tenantID, &key.userID, &key.memoryID, &contentSHA256); err != nil {
			return nil, mapProjectionPostgresError("scan projection promotion embedding", err)
		}
		embeddings[key] = contentSHA256
	}
	if err := rows.Err(); err != nil {
		return nil, mapProjectionPostgresError("iterate projection promotion embeddings", err)
	}
	return embeddings, nil
}

func projectionPromotionLiveScopes(scopes []projectionPromotionKey, cards []projectionPromotionCard) ([]projectionPromotionKey, error) {
	locked := make(map[projectionPromotionKey]struct{}, len(scopes))
	for _, scope := range scopes {
		locked[projectionPromotionKey{tenantID: scope.tenantID, userID: scope.userID}] = struct{}{}
	}
	seen := make(map[projectionPromotionKey]struct{})
	for _, card := range cards {
		scope := projectionPromotionKey{tenantID: card.key.tenantID, userID: card.key.userID}
		if _, ok := locked[scope]; !ok {
			return nil, fmt.Errorf("projection promotion card scope was not locked: %w", domain.ErrInvariant)
		}
		seen[scope] = struct{}{}
	}
	live := make([]projectionPromotionKey, 0, len(seen))
	for _, scope := range scopes {
		key := projectionPromotionKey{tenantID: scope.tenantID, userID: scope.userID}
		if _, ok := seen[key]; ok {
			live = append(live, key)
		}
	}
	return live, nil
}

func countCoveredProjectionPromotionCards(
	cards []projectionPromotionCard,
	jobs map[projectionPromotionKey]projectionPromotionJob,
	embeddings map[projectionPromotionKey]string,
) int64 {
	var covered int64
	for _, card := range cards {
		job, hasJob := jobs[card.key]
		contentSHA256, hasEmbedding := embeddings[card.key]
		if !hasJob || job.expectedMemoryVersion != card.version || job.state != ProjectionJobSucceeded || !hasEmbedding {
			continue
		}
		if contentSHA256 != embedding.MemoryCardDocumentV1SHA256(card.memory) {
			continue
		}
		covered++
	}
	return covered
}

func updatePromotedProjectionTarget(ctx context.Context, tx pgx.Tx, embeddingSpace string, state ProjectionTargetState, promotedAt time.Time) error {
	commandTag, err := tx.Exec(ctx, `
		UPDATE agent_memory.embedding_projection_targets
		SET state = $2, enqueue_new = true,
		    updated_at = $3
		WHERE embedding_space = $1`, embeddingSpace, string(state), promotedAt)
	if err != nil {
		return mapProjectionPostgresError("update promoted projection target", err)
	}
	if commandTag.RowsAffected() != 1 {
		return fmt.Errorf("promoted projection target disappeared: %w", domain.ErrInvariant)
	}
	return nil
}

func advanceProjectionPromotionRevisions(ctx context.Context, tx pgx.Tx, scopes []projectionPromotionKey, promotedAt time.Time) error {
	for _, scope := range scopes {
		commandTag, err := tx.Exec(ctx, `
			UPDATE agent_memory.user_scope_state
			SET context_revision = context_revision + 1,
			    updated_at = GREATEST(updated_at, $3)
			WHERE tenant_id = $1 AND user_id = $2`, scope.tenantID, scope.userID, promotedAt)
		if err != nil {
			return mapProjectionPostgresError("advance projection promotion revision", err)
		}
		if commandTag.RowsAffected() != 1 {
			return fmt.Errorf("projection promotion scope disappeared: %w", domain.ErrInvariant)
		}
	}
	return nil
}

func insertProjectionPromotionReceipt(ctx context.Context, tx pgx.Tx, receipt ProjectionPromotionReceipt) error {
	var fromSpace any
	if receipt.FromSpace != "" {
		fromSpace = receipt.FromSpace
	}
	commandTag, err := tx.Exec(ctx, `
		INSERT INTO agent_memory.embedding_projection_promotions (
			operation_id, from_embedding_space, to_embedding_space, allow_empty,
			live_scope_count, live_card_count, covered_card_count,
			previous_generation, generation, cutoff_at, promoted_at
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)`,
		receipt.OperationID,
		fromSpace,
		receipt.ToSpace,
		receipt.AllowEmpty,
		receipt.LiveScopeCount,
		receipt.LiveCardCount,
		receipt.CoveredCardCount,
		receipt.PreviousGeneration,
		receipt.Generation,
		receipt.CutoffAt,
		receipt.PromotedAt,
	)
	if err != nil {
		return mapProjectionPostgresError("insert projection promotion receipt", err)
	}
	if commandTag.RowsAffected() != 1 {
		return fmt.Errorf("projection promotion receipt was not inserted: %w", domain.ErrInvariant)
	}
	return nil
}
