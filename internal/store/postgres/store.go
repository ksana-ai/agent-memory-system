// Package postgres implements the durable Store adapter with PostgreSQL.
package postgres

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/kai443/go-agent-memory-system/internal/domain"
	domainstore "github.com/kai443/go-agent-memory-system/internal/store"
)

// Store persists the complete memory lifecycle in PostgreSQL.
type Store struct {
	pool *pgxpool.Pool
}

var _ domainstore.Store = (*Store)(nil)

// Open creates and verifies a PostgreSQL connection pool. Schema migration is
// intentionally owned by application bootstrap, before Open is called.
func Open(ctx context.Context, databaseURL string) (*Store, error) {
	if strings.TrimSpace(databaseURL) == "" {
		return nil, fmt.Errorf("database URL is required: %w", domain.ErrInvalid)
	}

	config, err := pgxpool.ParseConfig(databaseURL)
	if err != nil {
		return nil, fmt.Errorf("parse database URL: %w", err)
	}
	if config.ConnConfig.RuntimeParams == nil {
		config.ConnConfig.RuntimeParams = make(map[string]string)
	}
	config.ConnConfig.RuntimeParams["application_name"] = "go-agent-memory-system"

	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		return nil, fmt.Errorf("open PostgreSQL pool: %w", err)
	}
	storage := New(pool)
	if err := storage.Ping(ctx); err != nil {
		pool.Close()
		return nil, err
	}
	return storage, nil
}

// New wraps an existing pool. The caller retains responsibility for ensuring
// migrations have been applied.
func New(pool *pgxpool.Pool) *Store {
	return &Store{pool: pool}
}

// Ping verifies that PostgreSQL is reachable.
func (s *Store) Ping(ctx context.Context) error {
	if s == nil || s.pool == nil {
		return errors.New("PostgreSQL pool is nil")
	}
	if err := s.pool.Ping(ctx); err != nil {
		return fmt.Errorf("ping PostgreSQL: %w", err)
	}
	return nil
}

// Close releases all pooled connections.
func (s *Store) Close() {
	if s != nil && s.pool != nil {
		s.pool.Close()
	}
}

func (s *Store) AppendEvidence(ctx context.Context, event domain.EvidenceEvent) error {
	return s.withScopeWrite(ctx, event.TenantID, event.UserID, func(tx pgx.Tx) error {
		metadata, err := marshalMetadata(event.Metadata)
		if err != nil {
			return fmt.Errorf("encode evidence metadata: %w", err)
		}
		commandTag, err := tx.Exec(ctx, `
			INSERT INTO agent_memory.evidence_events (
				tenant_id, user_id, id, session_id, actor, content, metadata,
				occurred_at, recorded_at
			) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
			ON CONFLICT (tenant_id, user_id, id) DO NOTHING`,
			event.TenantID, event.UserID, event.ID, event.SessionID, string(event.Actor),
			event.Content, metadata, event.OccurredAt, event.RecordedAt,
		)
		if err != nil {
			return mapPostgresError("append evidence", err)
		}
		if commandTag.RowsAffected() != 1 {
			return fmt.Errorf("evidence %q: %w", event.ID, domain.ErrConflict)
		}
		return nil
	})
}

func (s *Store) EvidenceByID(ctx context.Context, tenantID, userID, eventID string) (domain.EvidenceEvent, error) {
	if err := s.ready(); err != nil {
		return domain.EvidenceEvent{}, err
	}
	return scanEvidence(s.pool.QueryRow(ctx, `
		SELECT id, tenant_id, user_id, session_id, actor, content, metadata,
		       occurred_at, recorded_at
		FROM agent_memory.evidence_events
		WHERE tenant_id = $1 AND user_id = $2 AND id = $3`, tenantID, userID, eventID), eventID)
}

func (s *Store) EvidenceByIDs(ctx context.Context, tenantID, userID string, eventIDs []string) ([]domain.EvidenceEvent, error) {
	if err := s.ready(); err != nil {
		return nil, err
	}
	if len(eventIDs) == 0 {
		return []domain.EvidenceEvent{}, nil
	}

	rows, err := s.pool.Query(ctx, `
		SELECT evidence.id, evidence.tenant_id, evidence.user_id, evidence.session_id,
		       evidence.actor, evidence.content, evidence.metadata,
		       evidence.occurred_at, evidence.recorded_at
		FROM unnest($3::text[]) WITH ORDINALITY AS requested(id, position)
		JOIN agent_memory.evidence_events AS evidence
		  ON evidence.tenant_id = $1
		 AND evidence.user_id = $2
		 AND evidence.id = requested.id
		ORDER BY requested.position`, tenantID, userID, eventIDs)
	if err != nil {
		return nil, mapPostgresError("load evidence", err)
	}
	defer rows.Close()

	events := make([]domain.EvidenceEvent, 0, len(eventIDs))
	for rows.Next() {
		event, scanErr := scanEvidence(rows, "")
		if scanErr != nil {
			return nil, scanErr
		}
		events = append(events, event)
	}
	if err := rows.Err(); err != nil {
		return nil, mapPostgresError("iterate evidence", err)
	}
	if len(events) != len(eventIDs) {
		found := make(map[string]int, len(events))
		for _, event := range events {
			found[event.ID]++
		}
		for _, eventID := range eventIDs {
			if found[eventID] == 0 {
				return nil, fmt.Errorf("evidence %q: %w", eventID, domain.ErrNotFound)
			}
			found[eventID]--
		}
		return nil, fmt.Errorf("requested evidence set is incomplete: %w", domain.ErrNotFound)
	}
	return events, nil
}

func (s *Store) CreateCandidate(ctx context.Context, candidate domain.MemoryCandidate) error {
	if len(candidate.SourceEventIDs) == 0 {
		return fmt.Errorf("candidate %q has no source evidence: %w", candidate.ID, domain.ErrInvalid)
	}
	if duplicate := firstDuplicate(candidate.SourceEventIDs); duplicate != "" {
		return fmt.Errorf("candidate %q repeats source evidence %q: %w", candidate.ID, duplicate, domain.ErrInvalid)
	}

	return s.withScopeWrite(ctx, candidate.TenantID, candidate.UserID, func(tx pgx.Tx) error {
		if err := validateEvidenceSources(ctx, tx, candidate.TenantID, candidate.UserID, candidate.SourceEventIDs); err != nil {
			return fmt.Errorf("candidate %q: %w", candidate.ID, err)
		}
		metadata, err := marshalMetadata(candidate.Metadata)
		if err != nil {
			return fmt.Errorf("encode candidate metadata: %w", err)
		}
		commandTag, err := tx.Exec(ctx, `
			INSERT INTO agent_memory.memory_candidates (
				tenant_id, user_id, id, kind, category, memory_key, value,
				person, relationship, backstory, extractor, extractor_version,
				status, metadata, created_at, expires_at
			) VALUES (
				$1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12,
				$13, $14, $15, $16
			)
			ON CONFLICT (tenant_id, user_id, id) DO NOTHING`,
			candidate.TenantID, candidate.UserID, candidate.ID, string(candidate.Kind),
			candidate.Category, candidate.Key, candidate.Value, candidate.Person,
			candidate.Relationship, candidate.Backstory, candidate.Extractor,
			candidate.ExtractorVersion, string(candidate.Status), metadata, candidate.CreatedAt,
			candidate.ExpiresAt,
		)
		if err != nil {
			return mapPostgresError("create candidate", err)
		}
		if commandTag.RowsAffected() != 1 {
			return fmt.Errorf("candidate %q: %w", candidate.ID, domain.ErrConflict)
		}
		for sourceOrder, eventID := range candidate.SourceEventIDs {
			if _, err := tx.Exec(ctx, `
				INSERT INTO agent_memory.candidate_source_events (
					tenant_id, user_id, candidate_id, evidence_event_id, source_order
				) VALUES ($1, $2, $3, $4, $5)`,
				candidate.TenantID, candidate.UserID, candidate.ID, eventID, sourceOrder,
			); err != nil {
				return mapPostgresError("attach candidate source evidence", err)
			}
		}
		return nil
	})
}

func (s *Store) CandidateByID(ctx context.Context, tenantID, userID, candidateID string) (domain.MemoryCandidate, error) {
	if err := s.ready(); err != nil {
		return domain.MemoryCandidate{}, err
	}
	return readCandidate(ctx, s.pool, tenantID, userID, candidateID, false)
}

func (s *Store) ReviewCandidate(ctx context.Context, command domainstore.CandidateReviewCommand) (domain.MemoryCandidate, *domain.MemoryCard, error) {
	if err := s.ready(); err != nil {
		return domain.MemoryCandidate{}, nil, err
	}
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return domain.MemoryCandidate{}, nil, fmt.Errorf("begin review transaction: %w", err)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()

	if err := lockScope(ctx, tx, command.TenantID, command.UserID); err != nil {
		return domain.MemoryCandidate{}, nil, err
	}
	candidate, err := readCandidate(ctx, tx, command.TenantID, command.UserID, command.CandidateID, true)
	if err != nil {
		return domain.MemoryCandidate{}, nil, err
	}
	if candidate.Status != domain.CandidatePending {
		return domain.MemoryCandidate{}, nil, fmt.Errorf("candidate %q already reviewed: %w", command.CandidateID, domain.ErrConflict)
	}
	if err := validateEvidenceSources(ctx, tx, command.TenantID, command.UserID, candidate.SourceEventIDs); err != nil {
		return domain.MemoryCandidate{}, nil, fmt.Errorf("candidate %q source evidence: %w", command.CandidateID, err)
	}

	switch command.Review.Decision {
	case domain.DecisionReject:
		candidate.Status = domain.CandidateRejected
		candidate.Review = cloneReview(&command.Review)
		if err := updateCandidateReview(ctx, tx, candidate); err != nil {
			return domain.MemoryCandidate{}, nil, err
		}
		if err := tx.Commit(ctx); err != nil {
			return domain.MemoryCandidate{}, nil, mapPostgresError("commit candidate rejection", err)
		}
		return candidate, nil, nil
	case domain.DecisionApprove:
		if strings.TrimSpace(command.MemoryID) == "" {
			return domain.MemoryCandidate{}, nil, fmt.Errorf("memory id is required: %w", domain.ErrInvalid)
		}
	default:
		return domain.MemoryCandidate{}, nil, fmt.Errorf("review decision %q: %w", command.Review.Decision, domain.ErrInvalid)
	}

	card := domain.MemoryCard{
		ID:             command.MemoryID,
		CandidateID:    candidate.ID,
		TenantID:       candidate.TenantID,
		UserID:         candidate.UserID,
		Kind:           candidate.Kind,
		Category:       candidate.Category,
		Key:            candidate.Key,
		Value:          candidate.Value,
		Person:         candidate.Person,
		Relationship:   candidate.Relationship,
		Backstory:      candidate.Backstory,
		SourceEventIDs: append([]string(nil), candidate.SourceEventIDs...),
		Status:         domain.MemoryActive,
		ExpiresAt:      cloneTime(candidate.ExpiresAt),
	}
	identity := card.Identity()
	identityKey := encodeIdentity(identity)
	baseCreatedAt := normalizedProjectionTime(command.Review.ReviewedAt, candidate.CreatedAt)

	if _, err := tx.Exec(ctx, `
		INSERT INTO agent_memory.memory_identity_chains (
			tenant_id, user_id, identity_key, kind, category, memory_key,
			person, relationship, latest_version, created_at, updated_at
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, 0, $9, $9)
		ON CONFLICT (tenant_id, user_id, kind, category, memory_key, person, relationship)
		DO NOTHING`,
		command.TenantID, command.UserID, identityKey, string(identity.Kind),
		identity.Category, identity.Key, identity.Person, identity.Relationship, baseCreatedAt,
	); err != nil {
		return domain.MemoryCandidate{}, nil, mapPostgresError("create memory identity chain", err)
	}

	var storedIdentityKey string
	var latestVersion int
	err = tx.QueryRow(ctx, `
		SELECT identity_key, latest_version
		FROM agent_memory.memory_identity_chains
		WHERE tenant_id = $1 AND user_id = $2
		  AND kind = $3 AND category = $4 AND memory_key = $5
		  AND person = $6 AND relationship = $7
		FOR UPDATE`,
		command.TenantID, command.UserID, string(identity.Kind), identity.Category,
		identity.Key, identity.Person, identity.Relationship,
	).Scan(&storedIdentityKey, &latestVersion)
	if err != nil {
		return domain.MemoryCandidate{}, nil, mapExpectedRowError("lock memory identity chain", err)
	}
	if storedIdentityKey != identityKey {
		return domain.MemoryCandidate{}, nil, fmt.Errorf("memory identity key collision: %w", domain.ErrInvariant)
	}

	card.Version = latestVersion + 1
	card.CreatedAt = baseCreatedAt
	if latestVersion > 0 {
		var latestCreatedAt time.Time
		err := tx.QueryRow(ctx, `
			SELECT created_at
			FROM agent_memory.memory_cards
			WHERE tenant_id = $1 AND user_id = $2 AND identity_key = $3
			  AND version = $4
			FOR UPDATE`, command.TenantID, command.UserID, identityKey, latestVersion).Scan(&latestCreatedAt)
		if err != nil {
			return domain.MemoryCandidate{}, nil, mapExpectedRowError("load latest memory version", err)
		}
		latestCreatedAt = latestCreatedAt.UTC().Truncate(time.Microsecond)
		if !card.CreatedAt.After(latestCreatedAt) {
			card.CreatedAt = latestCreatedAt.Add(time.Microsecond)
		}
	}

	commandTag, err := tx.Exec(ctx, `
		UPDATE agent_memory.memory_cards
		SET status = 'superseded', superseded_at = $4
		WHERE tenant_id = $1 AND user_id = $2 AND identity_key = $3
		  AND status = 'active'`, command.TenantID, command.UserID, identityKey, card.CreatedAt)
	if err != nil {
		return domain.MemoryCandidate{}, nil, mapPostgresError("supersede active memory", err)
	}
	expectedSuperseded := int64(0)
	if latestVersion > 0 {
		expectedSuperseded = 1
	}
	if commandTag.RowsAffected() != expectedSuperseded {
		return domain.MemoryCandidate{}, nil, fmt.Errorf(
			"memory identity %q has %d active rows, expected %d: %w",
			identityKey, commandTag.RowsAffected(), expectedSuperseded, domain.ErrInvariant,
		)
	}
	if expectedSuperseded == 1 {
		if _, err := tx.Exec(ctx, `
			DELETE FROM agent_memory.memory_embeddings AS embedding
			USING agent_memory.memory_cards AS card
			WHERE card.tenant_id = $1
			  AND card.user_id = $2
			  AND card.identity_key = $3
			  AND card.status = 'superseded'
			  AND embedding.tenant_id = card.tenant_id
			  AND embedding.user_id = card.user_id
			  AND embedding.memory_id = card.id`, command.TenantID, command.UserID, identityKey); err != nil {
			return domain.MemoryCandidate{}, nil, mapPostgresError("delete superseded memory embeddings", err)
		}
		if _, err := tx.Exec(ctx, `
			DELETE FROM agent_memory.embedding_projection_jobs AS job
			USING agent_memory.memory_cards AS card
			WHERE card.tenant_id = $1
			  AND card.user_id = $2
			  AND card.identity_key = $3
			  AND card.status = 'superseded'
			  AND job.tenant_id = card.tenant_id
			  AND job.user_id = card.user_id
			  AND job.memory_id = card.id`, command.TenantID, command.UserID, identityKey); err != nil {
			return domain.MemoryCandidate{}, nil, mapProjectionPostgresError("delete superseded memory projection jobs", err)
		}
	}

	if _, err := tx.Exec(ctx, `
		INSERT INTO agent_memory.memory_cards (
			tenant_id, user_id, id, candidate_id, identity_key, kind, category,
			memory_key, value, person, relationship, backstory, version, status,
			created_at, expires_at
		) VALUES (
			$1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13,
			'active', $14, $15
		)`,
		card.TenantID, card.UserID, card.ID, card.CandidateID, identityKey,
		string(card.Kind), card.Category, card.Key, card.Value, card.Person,
		card.Relationship, card.Backstory, card.Version, card.CreatedAt, card.ExpiresAt,
	); err != nil {
		return domain.MemoryCandidate{}, nil, mapPostgresError("create memory card", err)
	}

	// Projection targets are registered independently by the embedding worker.
	// Selecting them inside the approval transaction keeps the lifecycle card
	// and its durable projection handoff atomic without coupling the Store
	// interface to one live embedding endpoint. A target registered after this
	// snapshot is covered by the separate backfill/reconciliation path.
	if _, err := tx.Exec(ctx, `
		INSERT INTO agent_memory.embedding_projection_jobs (
			tenant_id, user_id, memory_id, embedding_space, expected_memory_version
		)
		SELECT $1, $2, $3, target.embedding_space, $4
		FROM agent_memory.embedding_projection_targets AS target
		WHERE target.enqueue_new
		  AND target.state IN ('shadow', 'serving')`,
		card.TenantID, card.UserID, card.ID, card.Version,
	); err != nil {
		return domain.MemoryCandidate{}, nil, mapProjectionPostgresError("enqueue memory embedding projection", err)
	}

	commandTag, err = tx.Exec(ctx, `
		UPDATE agent_memory.memory_identity_chains
		SET latest_version = $4, updated_at = $5
		WHERE tenant_id = $1 AND user_id = $2 AND identity_key = $3`,
		command.TenantID, command.UserID, identityKey, card.Version, card.CreatedAt)
	if err != nil {
		return domain.MemoryCandidate{}, nil, mapPostgresError("advance memory identity chain", err)
	}
	if commandTag.RowsAffected() != 1 {
		return domain.MemoryCandidate{}, nil, fmt.Errorf("memory identity chain disappeared: %w", domain.ErrInvariant)
	}

	candidate.Status = domain.CandidateApproved
	candidate.Review = cloneReview(&command.Review)
	if err := updateCandidateReview(ctx, tx, candidate); err != nil {
		return domain.MemoryCandidate{}, nil, err
	}
	commandTag, err = tx.Exec(ctx, `
		UPDATE agent_memory.user_scope_state
		SET context_revision = context_revision + 1, updated_at = CURRENT_TIMESTAMP
		WHERE tenant_id = $1 AND user_id = $2`, command.TenantID, command.UserID)
	if err != nil {
		return domain.MemoryCandidate{}, nil, mapPostgresError("advance context revision", err)
	}
	if commandTag.RowsAffected() != 1 {
		return domain.MemoryCandidate{}, nil, fmt.Errorf("user scope state disappeared: %w", domain.ErrInvariant)
	}
	if err := tx.Commit(ctx); err != nil {
		return domain.MemoryCandidate{}, nil, mapPostgresError("commit candidate approval", err)
	}
	return candidate, &card, nil
}

func (s *Store) ListServiceableMemories(ctx context.Context, tenantID, userID string, asOf time.Time) ([]domain.MemoryCard, error) {
	if err := s.ready(); err != nil {
		return nil, err
	}
	rows, err := s.pool.Query(ctx, `
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
		WHERE card.tenant_id = $1 AND card.user_id = $2 AND card.status = 'active'
		  AND (card.expires_at IS NULL OR card.expires_at > $3)
		ORDER BY card.created_at, card.id`, tenantID, userID, asOf)
	if err != nil {
		return nil, mapPostgresError("list serviceable memories", err)
	}
	defer rows.Close()

	memories := make([]domain.MemoryCard, 0)
	for rows.Next() {
		memory, scanErr := scanMemory(rows)
		if scanErr != nil {
			return nil, scanErr
		}
		memories = append(memories, memory)
	}
	if err := rows.Err(); err != nil {
		return nil, mapPostgresError("iterate serviceable memories", err)
	}
	return memories, nil
}

func (s *Store) ContextRevision(ctx context.Context, tenantID, userID string) (uint64, error) {
	if err := s.ready(); err != nil {
		return 0, err
	}
	var revision int64
	err := s.pool.QueryRow(ctx, `
		SELECT context_revision
		FROM agent_memory.user_scope_state
		WHERE tenant_id = $1 AND user_id = $2`, tenantID, userID).Scan(&revision)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, nil
	}
	if err != nil {
		return 0, mapPostgresError("load context revision", err)
	}
	if revision < 0 {
		return 0, fmt.Errorf("negative context revision: %w", domain.ErrInvariant)
	}
	return uint64(revision), nil
}

func (s *Store) ForgetUser(ctx context.Context, tenantID, userID string, deletedAt time.Time) (domain.DeletionReceipt, error) {
	receipt := domain.DeletionReceipt{TenantID: tenantID, UserID: userID, DeletedAt: deletedAt}
	err := s.withScopeWrite(ctx, tenantID, userID, func(tx pgx.Tx) error {
		commandTag, err := tx.Exec(ctx, `
			DELETE FROM agent_memory.memory_cards
			WHERE tenant_id = $1 AND user_id = $2`, tenantID, userID)
		if err != nil {
			return mapPostgresError("delete memory cards", err)
		}
		receipt.MemoriesDeleted = int(commandTag.RowsAffected())

		if _, err := tx.Exec(ctx, `
			DELETE FROM agent_memory.memory_identity_chains
			WHERE tenant_id = $1 AND user_id = $2`, tenantID, userID); err != nil {
			return mapPostgresError("delete memory identity chains", err)
		}

		commandTag, err = tx.Exec(ctx, `
			DELETE FROM agent_memory.memory_candidates
			WHERE tenant_id = $1 AND user_id = $2`, tenantID, userID)
		if err != nil {
			return mapPostgresError("delete memory candidates", err)
		}
		receipt.CandidatesDeleted = int(commandTag.RowsAffected())

		commandTag, err = tx.Exec(ctx, `
			DELETE FROM agent_memory.evidence_events
			WHERE tenant_id = $1 AND user_id = $2`, tenantID, userID)
		if err != nil {
			return mapPostgresError("delete evidence events", err)
		}
		receipt.EvidenceDeleted = int(commandTag.RowsAffected())

		commandTag, err = tx.Exec(ctx, `
			UPDATE agent_memory.user_scope_state
			SET context_revision = context_revision + 1,
			    last_deleted_at = $3,
			    updated_at = CURRENT_TIMESTAMP
			WHERE tenant_id = $1 AND user_id = $2`, tenantID, userID, deletedAt)
		if err != nil {
			return mapPostgresError("record user deletion", err)
		}
		if commandTag.RowsAffected() != 1 {
			return fmt.Errorf("user scope state disappeared: %w", domain.ErrInvariant)
		}
		return nil
	})
	if err != nil {
		return domain.DeletionReceipt{}, err
	}
	return receipt, nil
}

func (s *Store) withScopeWrite(ctx context.Context, tenantID, userID string, action func(pgx.Tx) error) error {
	if err := s.ready(); err != nil {
		return err
	}
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return fmt.Errorf("begin PostgreSQL transaction: %w", err)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()

	if err := lockScope(ctx, tx, tenantID, userID); err != nil {
		return err
	}
	if err := action(tx); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return mapPostgresError("commit PostgreSQL transaction", err)
	}
	return nil
}

func (s *Store) ready() error {
	if s == nil || s.pool == nil {
		return errors.New("PostgreSQL pool is nil")
	}
	return nil
}

func lockScope(ctx context.Context, tx pgx.Tx, tenantID, userID string) error {
	if _, err := tx.Exec(ctx, `
		INSERT INTO agent_memory.user_scope_state (tenant_id, user_id)
		VALUES ($1, $2)
		ON CONFLICT (tenant_id, user_id) DO NOTHING`, tenantID, userID); err != nil {
		return mapPostgresError("ensure user scope state", err)
	}
	var revision int64
	if err := tx.QueryRow(ctx, `
		SELECT context_revision
		FROM agent_memory.user_scope_state
		WHERE tenant_id = $1 AND user_id = $2
		FOR UPDATE`, tenantID, userID).Scan(&revision); err != nil {
		return mapExpectedRowError("lock user scope state", err)
	}
	if revision < 0 {
		return fmt.Errorf("negative context revision: %w", domain.ErrInvariant)
	}
	return nil
}

func validateEvidenceSources(ctx context.Context, tx pgx.Tx, tenantID, userID string, eventIDs []string) error {
	if len(eventIDs) == 0 {
		return fmt.Errorf("source evidence is required: %w", domain.ErrInvalid)
	}
	rows, err := tx.Query(ctx, `
		SELECT evidence.id
		FROM unnest($3::text[]) WITH ORDINALITY AS requested(id, position)
		JOIN agent_memory.evidence_events AS evidence
		  ON evidence.tenant_id = $1
		 AND evidence.user_id = $2
		 AND evidence.id = requested.id
		ORDER BY requested.position
		FOR KEY SHARE OF evidence`, tenantID, userID, eventIDs)
	if err != nil {
		return mapPostgresError("validate source evidence", err)
	}
	defer rows.Close()
	found := make([]string, 0, len(eventIDs))
	for rows.Next() {
		var eventID string
		if err := rows.Scan(&eventID); err != nil {
			return mapPostgresError("scan source evidence", err)
		}
		found = append(found, eventID)
	}
	if err := rows.Err(); err != nil {
		return mapPostgresError("iterate source evidence", err)
	}
	if len(found) != len(eventIDs) {
		present := make(map[string]int, len(found))
		for _, eventID := range found {
			present[eventID]++
		}
		for _, eventID := range eventIDs {
			if present[eventID] == 0 {
				return fmt.Errorf("evidence %q: %w", eventID, domain.ErrNotFound)
			}
			present[eventID]--
		}
		return fmt.Errorf("source evidence set is incomplete: %w", domain.ErrNotFound)
	}
	return nil
}

func updateCandidateReview(ctx context.Context, tx pgx.Tx, candidate domain.MemoryCandidate) error {
	if candidate.Review == nil {
		return fmt.Errorf("candidate review is missing: %w", domain.ErrInvariant)
	}
	commandTag, err := tx.Exec(ctx, `
		UPDATE agent_memory.memory_candidates
		SET status = $4,
		    review_decision = $5,
		    reviewer_id = $6,
		    review_reason = $7,
		    reviewed_at = $8
		WHERE tenant_id = $1 AND user_id = $2 AND id = $3 AND status = 'pending'`,
		candidate.TenantID, candidate.UserID, candidate.ID, string(candidate.Status),
		string(candidate.Review.Decision), candidate.Review.ReviewerID,
		candidate.Review.Reason, candidate.Review.ReviewedAt,
	)
	if err != nil {
		return mapPostgresError("update candidate review", err)
	}
	if commandTag.RowsAffected() != 1 {
		return fmt.Errorf("candidate %q review lost its lock: %w", candidate.ID, domain.ErrConflict)
	}
	return nil
}

func readCandidate(ctx context.Context, queryer rowQueryer, tenantID, userID, candidateID string, forUpdate bool) (domain.MemoryCandidate, error) {
	query := `
		SELECT candidate.id, candidate.tenant_id, candidate.user_id, candidate.kind,
		       candidate.category, candidate.memory_key, candidate.value,
		       candidate.person, candidate.relationship, candidate.backstory,
		       COALESCE(ARRAY(
		           SELECT source.evidence_event_id
		           FROM agent_memory.candidate_source_events AS source
		           WHERE source.tenant_id = candidate.tenant_id
		             AND source.user_id = candidate.user_id
		             AND source.candidate_id = candidate.id
		           ORDER BY source.source_order
		       ), ARRAY[]::text[]),
		       candidate.extractor, candidate.extractor_version, candidate.status,
		       candidate.review_decision, candidate.reviewer_id,
		       candidate.review_reason, candidate.reviewed_at,
		       candidate.created_at, candidate.expires_at, candidate.metadata
		FROM agent_memory.memory_candidates AS candidate
		WHERE candidate.tenant_id = $1 AND candidate.user_id = $2 AND candidate.id = $3`
	if forUpdate {
		query += " FOR UPDATE OF candidate"
	}
	return scanCandidate(queryer.QueryRow(ctx, query, tenantID, userID, candidateID), candidateID)
}

func scanEvidence(row rowScanner, requestedID string) (domain.EvidenceEvent, error) {
	var event domain.EvidenceEvent
	var actor string
	var metadata []byte
	if err := row.Scan(
		&event.ID, &event.TenantID, &event.UserID, &event.SessionID, &actor,
		&event.Content, &metadata, &event.OccurredAt, &event.RecordedAt,
	); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return domain.EvidenceEvent{}, fmt.Errorf("evidence %q: %w", requestedID, domain.ErrNotFound)
		}
		return domain.EvidenceEvent{}, mapPostgresError("scan evidence", err)
	}
	event.Actor = domain.Actor(actor)
	if err := unmarshalMetadata(metadata, &event.Metadata); err != nil {
		return domain.EvidenceEvent{}, fmt.Errorf("decode evidence %q metadata: %w", event.ID, domain.ErrInvariant)
	}
	return event, nil
}

func scanCandidate(row rowScanner, requestedID string) (domain.MemoryCandidate, error) {
	var candidate domain.MemoryCandidate
	var kind, status string
	var reviewDecision, reviewerID, reviewReason pgtype.Text
	var reviewedAt, expiresAt pgtype.Timestamptz
	var metadata []byte
	if err := row.Scan(
		&candidate.ID, &candidate.TenantID, &candidate.UserID, &kind,
		&candidate.Category, &candidate.Key, &candidate.Value, &candidate.Person,
		&candidate.Relationship, &candidate.Backstory, &candidate.SourceEventIDs,
		&candidate.Extractor, &candidate.ExtractorVersion, &status,
		&reviewDecision, &reviewerID, &reviewReason, &reviewedAt,
		&candidate.CreatedAt, &expiresAt, &metadata,
	); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return domain.MemoryCandidate{}, fmt.Errorf("candidate %q: %w", requestedID, domain.ErrNotFound)
		}
		return domain.MemoryCandidate{}, mapPostgresError("scan candidate", err)
	}
	candidate.Kind = domain.MemoryKind(kind)
	candidate.Status = domain.CandidateStatus(status)
	if expiresAt.Valid {
		candidate.ExpiresAt = cloneTime(&expiresAt.Time)
	}
	if reviewDecision.Valid || reviewerID.Valid || reviewReason.Valid || reviewedAt.Valid {
		if !(reviewDecision.Valid && reviewerID.Valid && reviewReason.Valid && reviewedAt.Valid) {
			return domain.MemoryCandidate{}, fmt.Errorf("candidate %q has a partial review: %w", candidate.ID, domain.ErrInvariant)
		}
		candidate.Review = &domain.CandidateReview{
			Decision:   domain.ReviewDecision(reviewDecision.String),
			ReviewerID: reviewerID.String,
			Reason:     reviewReason.String,
			ReviewedAt: reviewedAt.Time,
		}
	}
	if err := unmarshalMetadata(metadata, &candidate.Metadata); err != nil {
		return domain.MemoryCandidate{}, fmt.Errorf("decode candidate %q metadata: %w", candidate.ID, domain.ErrInvariant)
	}
	return candidate, nil
}

func scanMemory(row rowScanner) (domain.MemoryCard, error) {
	memory, _, err := scanMemoryRow(row, false)
	return memory, err
}

func scanMemoryWithScore(row rowScanner) (domain.MemoryCard, float64, error) {
	return scanMemoryRow(row, true)
}

func scanMemoryRow(row rowScanner, withScore bool) (domain.MemoryCard, float64, error) {
	var memory domain.MemoryCard
	var score float64
	var kind, status string
	var expiresAt, supersededAt pgtype.Timestamptz
	destinations := []any{
		&memory.ID, &memory.CandidateID, &memory.TenantID, &memory.UserID, &kind,
		&memory.Category, &memory.Key, &memory.Value, &memory.Person,
		&memory.Relationship, &memory.Backstory, &memory.SourceEventIDs,
		&memory.Version, &status, &memory.CreatedAt, &expiresAt, &supersededAt,
	}
	if withScore {
		destinations = append(destinations, &score)
	}
	if err := row.Scan(destinations...); err != nil {
		return domain.MemoryCard{}, 0, mapPostgresError("scan memory", err)
	}
	memory.Kind = domain.MemoryKind(kind)
	memory.Status = domain.MemoryStatus(status)
	if expiresAt.Valid {
		memory.ExpiresAt = cloneTime(&expiresAt.Time)
	}
	if supersededAt.Valid {
		value := supersededAt.Time
		memory.SupersededAt = &value
	}
	return memory, score, nil
}

type rowScanner interface {
	Scan(...any) error
}

type rowQueryer interface {
	QueryRow(context.Context, string, ...any) pgx.Row
}

func encodeIdentity(identity domain.MemoryIdentity) string {
	hash := sha256.New()
	for _, part := range []string{
		string(identity.Kind), identity.Category, identity.Key, identity.Person, identity.Relationship,
	} {
		_, _ = fmt.Fprintf(hash, "%d:", len(part))
		_, _ = hash.Write([]byte(part))
		_, _ = hash.Write([]byte{0})
	}
	return hex.EncodeToString(hash.Sum(nil))
}

func normalizedProjectionTime(reviewedAt, candidateCreatedAt time.Time) time.Time {
	createdAt := reviewedAt.UTC().Truncate(time.Microsecond)
	candidateTime := candidateCreatedAt.UTC().Truncate(time.Microsecond)
	if createdAt.IsZero() || createdAt.Before(candidateTime) {
		createdAt = candidateTime
	}
	return createdAt
}

func marshalMetadata(metadata map[string]string) ([]byte, error) {
	if metadata == nil {
		return []byte("{}"), nil
	}
	return json.Marshal(metadata)
}

func unmarshalMetadata(data []byte, destination *map[string]string) error {
	if len(data) == 0 {
		*destination = nil
		return nil
	}
	return json.Unmarshal(data, destination)
}

func firstDuplicate(values []string) string {
	seen := make(map[string]struct{}, len(values))
	for _, value := range values {
		if _, exists := seen[value]; exists {
			return value
		}
		seen[value] = struct{}{}
	}
	return ""
}

func cloneReview(review *domain.CandidateReview) *domain.CandidateReview {
	if review == nil {
		return nil
	}
	cloned := *review
	return &cloned
}

func cloneTime(value *time.Time) *time.Time {
	if value == nil {
		return nil
	}
	cloned := *value
	return &cloned
}

func mapExpectedRowError(action string, err error) error {
	if errors.Is(err, pgx.ErrNoRows) {
		return fmt.Errorf("%s: %w", action, domain.ErrInvariant)
	}
	return mapPostgresError(action, err)
}

func mapPostgresError(action string, err error) error {
	if err == nil {
		return nil
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
		}
	}
	return fmt.Errorf("%s: %w", action, err)
}
