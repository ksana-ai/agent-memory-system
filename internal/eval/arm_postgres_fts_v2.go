package eval

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"maps"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/kai443/go-agent-memory-system/internal/domain"
	"github.com/kai443/go-agent-memory-system/internal/store"
	"github.com/kai443/go-agent-memory-system/internal/store/postgres"
)

const ArmReviewedCardsPostgresFTSV1 = "reviewed-cards-postgres-fts-v1"

type postgresFTSBackend interface {
	store.Store
	Search(context.Context, string, string, string, int, time.Time) ([]domain.SearchHit, error)
	FTSMetadata(context.Context) (postgres.FTSMetadata, error)
	DeleteEvaluationScopeState(context.Context, string, string) error
	Close()
}

type postgresFTSOpener func(context.Context, string) (postgresFTSBackend, error)

// NewPostgresFTSArmFactory probes non-sensitive component metadata once and
// returns a factory that opens a fresh, isolated PostgreSQL runtime per case.
// Connection details are retained only inside the runtime closure and are
// deliberately omitted from descriptors and errors.
func NewPostgresFTSArmFactory(ctx context.Context, databaseURL string) (ArmFactory, error) {
	return newPostgresFTSArmFactory(ctx, databaseURL, func(ctx context.Context, databaseURL string) (postgresFTSBackend, error) {
		return postgres.Open(ctx, databaseURL)
	}, randomPostgresNamespaceToken)
}

func newPostgresFTSArmFactory(
	ctx context.Context,
	databaseURL string,
	opener postgresFTSOpener,
	namespaceToken func() (string, error),
) (ArmFactory, error) {
	databaseURL = strings.TrimSpace(databaseURL)
	if databaseURL == "" {
		return nil, errors.New("PostgreSQL URL is required for the PostgreSQL FTS evaluation arm")
	}
	if opener == nil || namespaceToken == nil {
		return nil, errors.New("PostgreSQL FTS evaluation dependencies are incomplete")
	}

	probe, err := opener(ctx, databaseURL)
	if err != nil {
		return nil, errors.New("connect to PostgreSQL for FTS component probe failed")
	}
	metadata, metadataErr := probe.FTSMetadata(ctx)
	probe.Close()
	if metadataErr != nil {
		return nil, errors.New("read PostgreSQL FTS component metadata failed")
	}
	descriptorMetadata := postgresFTSDescriptorMetadata(metadata)
	config := struct {
		Storage    string            `json:"storage"`
		Retriever  string            `json:"retriever"`
		Components map[string]string `json:"components"`
	}{Storage: "postgresql", Retriever: "postgres-fts-v1", Components: descriptorMetadata}

	return armFactory{
		descriptor: ArmDescriptor{
			ID:              ArmReviewedCardsPostgresFTSV1,
			Version:         "1",
			JudgmentProfile: "reviewed-memory-alias-v1",
			ResultKind:      "memory-card",
			ConfigHash:      hashArmConfig(config),
			Metadata:        descriptorMetadata,
		},
		newRuntime: func(ctx context.Context) (ArmRuntime, error) {
			backend, openErr := opener(ctx, databaseURL)
			if openErr != nil {
				return ArmRuntime{}, errors.New("open PostgreSQL FTS case runtime failed")
			}
			runtimeMetadata, runtimeMetadataErr := backend.FTSMetadata(ctx)
			if runtimeMetadataErr != nil {
				backend.Close()
				return ArmRuntime{}, errors.New("read PostgreSQL FTS case component metadata failed")
			}
			if !maps.Equal(postgresFTSDescriptorMetadata(runtimeMetadata), descriptorMetadata) {
				backend.Close()
				return ArmRuntime{}, errors.New("PostgreSQL FTS components changed after factory construction")
			}
			token, tokenErr := namespaceToken()
			if tokenErr != nil {
				backend.Close()
				return ArmRuntime{}, errors.New("create PostgreSQL FTS case namespace failed")
			}
			namespace := newPostgresFTSNamespace(backend, token)
			return ArmRuntime{Store: namespace, Retriever: namespace, Cleanup: namespace.Cleanup}, nil
		},
	}, nil
}

func postgresFTSDescriptorMetadata(metadata postgres.FTSMetadata) map[string]string {
	return map[string]string{
		"postgres_server_version_num": metadata.ServerVersionNum,
		"schema_migration_version":    strconv.Itoa(metadata.SchemaMigrationVersion),
		"text_search_config":          metadata.TextSearchConfig,
		"query_strategy":              metadata.QueryStrategy,
		"rank_function":               metadata.RankFunction,
	}
}

func randomPostgresNamespaceToken() (string, error) {
	buffer := make([]byte, 16)
	if _, err := rand.Read(buffer); err != nil {
		return "", err
	}
	return hex.EncodeToString(buffer), nil
}

type postgresLogicalScope struct {
	tenantID string
	userID   string
}

type postgresPhysicalScope struct {
	tenantID string
	userID   string
}

// postgresFTSNamespace is both the Store and Retriever boundary used by a
// single case. It keeps stable artifact IDs intact while replacing every
// logical tenant/user pair with a case-random physical scope.
type postgresFTSNamespace struct {
	backend postgresFTSBackend
	token   string

	mu     sync.Mutex
	scopes map[postgresLogicalScope]postgresPhysicalScope
	closed bool
}

func newPostgresFTSNamespace(backend postgresFTSBackend, token string) *postgresFTSNamespace {
	return &postgresFTSNamespace{
		backend: backend,
		token:   token,
		scopes:  make(map[postgresLogicalScope]postgresPhysicalScope),
	}
}

func (namespace *postgresFTSNamespace) physicalScope(tenantID, userID string) postgresPhysicalScope {
	logical := postgresLogicalScope{tenantID: tenantID, userID: userID}
	namespace.mu.Lock()
	defer namespace.mu.Unlock()
	if physical, exists := namespace.scopes[logical]; exists {
		return physical
	}
	index := len(namespace.scopes) + 1
	physical := postgresPhysicalScope{
		tenantID: fmt.Sprintf("eval_%s_t_%d", namespace.token, index),
		userID:   fmt.Sprintf("eval_%s_u_%d", namespace.token, index),
	}
	namespace.scopes[logical] = physical
	return physical
}

func (namespace *postgresFTSNamespace) AppendEvidence(ctx context.Context, event domain.EvidenceEvent) error {
	physical := namespace.physicalScope(event.TenantID, event.UserID)
	event.TenantID, event.UserID = physical.tenantID, physical.userID
	return namespace.backend.AppendEvidence(ctx, event)
}

func (namespace *postgresFTSNamespace) EvidenceByID(ctx context.Context, tenantID, userID, eventID string) (domain.EvidenceEvent, error) {
	physical := namespace.physicalScope(tenantID, userID)
	event, err := namespace.backend.EvidenceByID(ctx, physical.tenantID, physical.userID, eventID)
	if err != nil {
		return domain.EvidenceEvent{}, err
	}
	if err := restorePostgresScope(&event.TenantID, &event.UserID, tenantID, userID, physical); err != nil {
		return domain.EvidenceEvent{}, err
	}
	return event, nil
}

func (namespace *postgresFTSNamespace) EvidenceByIDs(ctx context.Context, tenantID, userID string, eventIDs []string) ([]domain.EvidenceEvent, error) {
	physical := namespace.physicalScope(tenantID, userID)
	events, err := namespace.backend.EvidenceByIDs(ctx, physical.tenantID, physical.userID, eventIDs)
	if err != nil {
		return nil, err
	}
	for index := range events {
		if err := restorePostgresScope(&events[index].TenantID, &events[index].UserID, tenantID, userID, physical); err != nil {
			return nil, err
		}
	}
	return events, nil
}

func (namespace *postgresFTSNamespace) CreateCandidate(ctx context.Context, candidate domain.MemoryCandidate) error {
	physical := namespace.physicalScope(candidate.TenantID, candidate.UserID)
	candidate.TenantID, candidate.UserID = physical.tenantID, physical.userID
	return namespace.backend.CreateCandidate(ctx, candidate)
}

func (namespace *postgresFTSNamespace) CreateCandidateBatch(ctx context.Context, command store.CandidateBatchCommand) error {
	logicalTenantID, logicalUserID := command.TenantID, command.UserID
	physical := namespace.physicalScope(logicalTenantID, logicalUserID)
	command.TenantID, command.UserID = physical.tenantID, physical.userID
	command.Candidates = append([]domain.MemoryCandidate(nil), command.Candidates...)
	for index := range command.Candidates {
		candidate := &command.Candidates[index]
		if candidate.TenantID == logicalTenantID && candidate.UserID == logicalUserID {
			candidate.TenantID, candidate.UserID = physical.tenantID, physical.userID
		}
	}
	return namespace.backend.CreateCandidateBatch(ctx, command)
}

func (namespace *postgresFTSNamespace) CandidateByID(ctx context.Context, tenantID, userID, candidateID string) (domain.MemoryCandidate, error) {
	physical := namespace.physicalScope(tenantID, userID)
	candidate, err := namespace.backend.CandidateByID(ctx, physical.tenantID, physical.userID, candidateID)
	if err != nil {
		return domain.MemoryCandidate{}, err
	}
	if err := restorePostgresScope(&candidate.TenantID, &candidate.UserID, tenantID, userID, physical); err != nil {
		return domain.MemoryCandidate{}, err
	}
	return candidate, nil
}

func (namespace *postgresFTSNamespace) ReviewCandidate(ctx context.Context, command store.CandidateReviewCommand) (domain.MemoryCandidate, *domain.MemoryCard, error) {
	physical := namespace.physicalScope(command.TenantID, command.UserID)
	logicalTenantID, logicalUserID := command.TenantID, command.UserID
	command.TenantID, command.UserID = physical.tenantID, physical.userID
	candidate, memory, err := namespace.backend.ReviewCandidate(ctx, command)
	if err != nil {
		return domain.MemoryCandidate{}, nil, err
	}
	if err := restorePostgresScope(&candidate.TenantID, &candidate.UserID, logicalTenantID, logicalUserID, physical); err != nil {
		return domain.MemoryCandidate{}, nil, err
	}
	if memory != nil {
		if err := restorePostgresScope(&memory.TenantID, &memory.UserID, logicalTenantID, logicalUserID, physical); err != nil {
			return domain.MemoryCandidate{}, nil, err
		}
	}
	return candidate, memory, nil
}

func (namespace *postgresFTSNamespace) ListServiceableMemories(ctx context.Context, tenantID, userID string, asOf time.Time) ([]domain.MemoryCard, error) {
	physical := namespace.physicalScope(tenantID, userID)
	memories, err := namespace.backend.ListServiceableMemories(ctx, physical.tenantID, physical.userID, asOf)
	if err != nil {
		return nil, err
	}
	for index := range memories {
		if err := restorePostgresScope(&memories[index].TenantID, &memories[index].UserID, tenantID, userID, physical); err != nil {
			return nil, err
		}
	}
	return memories, nil
}

func (namespace *postgresFTSNamespace) Search(ctx context.Context, tenantID, userID, query string, limit int, asOf time.Time) ([]domain.SearchHit, error) {
	physical := namespace.physicalScope(tenantID, userID)
	hits, err := namespace.backend.Search(ctx, physical.tenantID, physical.userID, query, limit, asOf)
	if err != nil {
		return nil, err
	}
	for index := range hits {
		if err := restorePostgresScope(&hits[index].Memory.TenantID, &hits[index].Memory.UserID, tenantID, userID, physical); err != nil {
			return nil, err
		}
	}
	return hits, nil
}

func (namespace *postgresFTSNamespace) ContextRevision(ctx context.Context, tenantID, userID string) (uint64, error) {
	physical := namespace.physicalScope(tenantID, userID)
	return namespace.backend.ContextRevision(ctx, physical.tenantID, physical.userID)
}

func (namespace *postgresFTSNamespace) ForgetUser(ctx context.Context, tenantID, userID string, deletedAt time.Time) (domain.DeletionReceipt, error) {
	physical := namespace.physicalScope(tenantID, userID)
	receipt, err := namespace.backend.ForgetUser(ctx, physical.tenantID, physical.userID, deletedAt)
	if err != nil {
		return domain.DeletionReceipt{}, err
	}
	if err := restorePostgresScope(&receipt.TenantID, &receipt.UserID, tenantID, userID, physical); err != nil {
		return domain.DeletionReceipt{}, err
	}
	return receipt, nil
}

func restorePostgresScope(actualTenantID, actualUserID *string, logicalTenantID, logicalUserID string, physical postgresPhysicalScope) error {
	if *actualTenantID != physical.tenantID || *actualUserID != physical.userID {
		return fmt.Errorf("PostgreSQL FTS namespace returned a cross-scope value: %w", domain.ErrInvariant)
	}
	*actualTenantID, *actualUserID = logicalTenantID, logicalUserID
	return nil
}

func (namespace *postgresFTSNamespace) Cleanup(ctx context.Context) error {
	namespace.mu.Lock()
	if namespace.closed {
		namespace.mu.Unlock()
		return nil
	}
	namespace.closed = true
	type cleanupScope struct {
		physical postgresPhysicalScope
	}
	scopes := make([]cleanupScope, 0, len(namespace.scopes))
	for _, physical := range namespace.scopes {
		scopes = append(scopes, cleanupScope{physical: physical})
	}
	namespace.mu.Unlock()
	sort.Slice(scopes, func(i, j int) bool {
		if scopes[i].physical.tenantID == scopes[j].physical.tenantID {
			return scopes[i].physical.userID < scopes[j].physical.userID
		}
		return scopes[i].physical.tenantID < scopes[j].physical.tenantID
	})

	cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
	defer cancel()
	var cleanupErr error
	for _, scope := range scopes {
		_, err := namespace.backend.ForgetUser(cleanupCtx, scope.physical.tenantID, scope.physical.userID, time.Now().UTC())
		if err != nil {
			cleanupErr = errors.Join(cleanupErr, fmt.Errorf("clean PostgreSQL FTS case scope: %w", err))
			continue
		}
		if err := namespace.backend.DeleteEvaluationScopeState(cleanupCtx, scope.physical.tenantID, scope.physical.userID); err != nil {
			cleanupErr = errors.Join(cleanupErr, fmt.Errorf("remove PostgreSQL FTS case scope state: %w", err))
		}
	}
	namespace.backend.Close()
	return cleanupErr
}
