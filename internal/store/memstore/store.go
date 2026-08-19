package memstore

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/kai443/go-agent-memory-system/internal/domain"
	"github.com/kai443/go-agent-memory-system/internal/store"
)

type Store struct {
	mu         sync.RWMutex
	evidence   map[evidenceKey]domain.EvidenceEvent
	candidates map[string]domain.MemoryCandidate
	memories   map[string]domain.MemoryCard
	revisions  map[userScope]uint64
}

type evidenceKey struct {
	tenantID string
	userID   string
	eventID  string
}

type userScope struct {
	tenantID string
	userID   string
}

func New() *Store {
	return &Store{
		evidence:   make(map[evidenceKey]domain.EvidenceEvent),
		candidates: make(map[string]domain.MemoryCandidate),
		memories:   make(map[string]domain.MemoryCard),
		revisions:  make(map[userScope]uint64),
	}
}

func (s *Store) AppendEvidence(ctx context.Context, event domain.EvidenceEvent) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	key := evidenceKey{tenantID: event.TenantID, userID: event.UserID, eventID: event.ID}
	if _, exists := s.evidence[key]; exists {
		return fmt.Errorf("evidence %q: %w", event.ID, domain.ErrConflict)
	}
	s.evidence[key] = cloneEvidence(event)
	return nil
}

func (s *Store) EvidenceByID(ctx context.Context, tenantID, userID, eventID string) (domain.EvidenceEvent, error) {
	if err := ctx.Err(); err != nil {
		return domain.EvidenceEvent{}, err
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	event, ok := s.evidence[evidenceKey{tenantID: tenantID, userID: userID, eventID: eventID}]
	if !ok {
		return domain.EvidenceEvent{}, fmt.Errorf("evidence %q: %w", eventID, domain.ErrNotFound)
	}
	return cloneEvidence(event), nil
}

func (s *Store) EvidenceByIDs(ctx context.Context, tenantID, userID string, eventIDs []string) ([]domain.EvidenceEvent, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	events := make([]domain.EvidenceEvent, 0, len(eventIDs))
	for _, eventID := range eventIDs {
		event, ok := s.evidence[evidenceKey{tenantID: tenantID, userID: userID, eventID: eventID}]
		if !ok {
			return nil, fmt.Errorf("evidence %q: %w", eventID, domain.ErrNotFound)
		}
		events = append(events, cloneEvidence(event))
	}
	return events, nil
}

func (s *Store) CreateCandidate(ctx context.Context, candidate domain.MemoryCandidate) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if _, exists := s.candidates[candidate.ID]; exists {
		return fmt.Errorf("candidate %q: %w", candidate.ID, domain.ErrConflict)
	}
	if len(candidate.SourceEventIDs) == 0 {
		return fmt.Errorf("candidate %q has no source evidence: %w", candidate.ID, domain.ErrInvalid)
	}
	for _, eventID := range candidate.SourceEventIDs {
		_, exists := s.evidence[evidenceKey{tenantID: candidate.TenantID, userID: candidate.UserID, eventID: eventID}]
		if !exists {
			return fmt.Errorf("candidate %q source evidence %q: %w", candidate.ID, eventID, domain.ErrNotFound)
		}
	}
	s.candidates[candidate.ID] = cloneCandidate(candidate)
	return nil
}

func (s *Store) CandidateByID(ctx context.Context, tenantID, userID, candidateID string) (domain.MemoryCandidate, error) {
	if err := ctx.Err(); err != nil {
		return domain.MemoryCandidate{}, err
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	candidate, ok := s.candidates[candidateID]
	if !ok || candidate.TenantID != tenantID || candidate.UserID != userID {
		return domain.MemoryCandidate{}, fmt.Errorf("candidate %q: %w", candidateID, domain.ErrNotFound)
	}
	return cloneCandidate(candidate), nil
}

func (s *Store) ReviewCandidate(ctx context.Context, command store.CandidateReviewCommand) (domain.MemoryCandidate, *domain.MemoryCard, error) {
	if err := ctx.Err(); err != nil {
		return domain.MemoryCandidate{}, nil, err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	candidate, ok := s.candidates[command.CandidateID]
	if !ok || candidate.TenantID != command.TenantID || candidate.UserID != command.UserID {
		return domain.MemoryCandidate{}, nil, fmt.Errorf("candidate %q: %w", command.CandidateID, domain.ErrNotFound)
	}
	if candidate.Status != domain.CandidatePending {
		return domain.MemoryCandidate{}, nil, fmt.Errorf("candidate %q already reviewed: %w", command.CandidateID, domain.ErrConflict)
	}

	candidate.Review = cloneReview(&command.Review)
	switch command.Review.Decision {
	case domain.DecisionReject:
		candidate.Status = domain.CandidateRejected
		s.candidates[candidate.ID] = cloneCandidate(candidate)
		return cloneCandidate(candidate), nil, nil
	case domain.DecisionApprove:
		candidate.Status = domain.CandidateApproved
	default:
		return domain.MemoryCandidate{}, nil, fmt.Errorf("review decision %q: %w", command.Review.Decision, domain.ErrInvalid)
	}

	if command.MemoryID == "" {
		return domain.MemoryCandidate{}, nil, fmt.Errorf("memory id is required: %w", domain.ErrInvalid)
	}
	if _, exists := s.memories[command.MemoryID]; exists {
		return domain.MemoryCandidate{}, nil, fmt.Errorf("memory %q: %w", command.MemoryID, domain.ErrConflict)
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
		Version:        1,
		Status:         domain.MemoryActive,
		CreatedAt:      command.Review.ReviewedAt,
		ExpiresAt:      cloneTime(candidate.ExpiresAt),
	}

	latestCreatedAt := time.Time{}
	for _, existing := range s.memories {
		if existing.TenantID != card.TenantID || existing.UserID != card.UserID || existing.Identity() != card.Identity() {
			continue
		}
		if existing.Version >= card.Version {
			card.Version = existing.Version + 1
		}
		if existing.CreatedAt.After(latestCreatedAt) {
			latestCreatedAt = existing.CreatedAt
		}
	}
	if !latestCreatedAt.IsZero() && !card.CreatedAt.After(latestCreatedAt) {
		card.CreatedAt = latestCreatedAt.Add(time.Nanosecond)
	}
	for id, existing := range s.memories {
		if existing.TenantID != card.TenantID || existing.UserID != card.UserID || existing.Identity() != card.Identity() {
			continue
		}
		if existing.Status == domain.MemoryActive {
			supersededAt := card.CreatedAt
			existing.Status = domain.MemorySuperseded
			existing.SupersededAt = &supersededAt
			s.memories[id] = cloneMemory(existing)
		}
	}

	s.candidates[candidate.ID] = cloneCandidate(candidate)
	s.memories[card.ID] = cloneMemory(card)
	s.revisions[userScope{tenantID: card.TenantID, userID: card.UserID}]++
	cloned := cloneMemory(card)
	return cloneCandidate(candidate), &cloned, nil
}

func (s *Store) ContextRevision(ctx context.Context, tenantID, userID string) (uint64, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}

	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.revisions[userScope{tenantID: tenantID, userID: userID}], nil
}

func (s *Store) ListServiceableMemories(ctx context.Context, tenantID, userID string, asOf time.Time) ([]domain.MemoryCard, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	memories := make([]domain.MemoryCard, 0)
	for _, memory := range s.memories {
		if memory.TenantID == tenantID && memory.UserID == userID && memory.ServiceableAt(asOf) {
			memories = append(memories, cloneMemory(memory))
		}
	}
	sort.Slice(memories, func(i, j int) bool {
		if memories[i].CreatedAt.Equal(memories[j].CreatedAt) {
			return memories[i].ID < memories[j].ID
		}
		return memories[i].CreatedAt.Before(memories[j].CreatedAt)
	})
	return memories, nil
}

func (s *Store) ForgetUser(ctx context.Context, tenantID, userID string, deletedAt time.Time) (domain.DeletionReceipt, error) {
	if err := ctx.Err(); err != nil {
		return domain.DeletionReceipt{}, err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	receipt := domain.DeletionReceipt{TenantID: tenantID, UserID: userID, DeletedAt: deletedAt}
	for key, event := range s.evidence {
		if event.TenantID == tenantID && event.UserID == userID {
			delete(s.evidence, key)
			receipt.EvidenceDeleted++
		}
	}
	for id, candidate := range s.candidates {
		if candidate.TenantID == tenantID && candidate.UserID == userID {
			delete(s.candidates, id)
			receipt.CandidatesDeleted++
		}
	}
	for id, memory := range s.memories {
		if memory.TenantID == tenantID && memory.UserID == userID {
			delete(s.memories, id)
			receipt.MemoriesDeleted++
		}
	}
	s.revisions[userScope{tenantID: tenantID, userID: userID}]++
	return receipt, nil
}

func cloneEvidence(event domain.EvidenceEvent) domain.EvidenceEvent {
	event.Metadata = cloneMap(event.Metadata)
	return event
}

func cloneCandidate(candidate domain.MemoryCandidate) domain.MemoryCandidate {
	candidate.SourceEventIDs = append([]string(nil), candidate.SourceEventIDs...)
	candidate.Metadata = cloneMap(candidate.Metadata)
	candidate.Review = cloneReview(candidate.Review)
	candidate.ExpiresAt = cloneTime(candidate.ExpiresAt)
	return candidate
}

func cloneMemory(memory domain.MemoryCard) domain.MemoryCard {
	memory.SourceEventIDs = append([]string(nil), memory.SourceEventIDs...)
	if memory.SupersededAt != nil {
		value := *memory.SupersededAt
		memory.SupersededAt = &value
	}
	memory.ExpiresAt = cloneTime(memory.ExpiresAt)
	return memory
}

func cloneTime(input *time.Time) *time.Time {
	if input == nil {
		return nil
	}
	value := *input
	return &value
}

func cloneReview(review *domain.CandidateReview) *domain.CandidateReview {
	if review == nil {
		return nil
	}
	copy := *review
	return &copy
}

func cloneMap(input map[string]string) map[string]string {
	if input == nil {
		return nil
	}
	output := make(map[string]string, len(input))
	for key, value := range input {
		output[key] = value
	}
	return output
}

var _ store.Store = (*Store)(nil)
