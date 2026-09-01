package app

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/ksana-ai/agent-memory-system/internal/domain"
	"github.com/ksana-ai/agent-memory-system/internal/extraction"
	"github.com/ksana-ai/agent-memory-system/internal/id"
	"github.com/ksana-ai/agent-memory-system/internal/store"
)

const (
	defaultSearchLimit = 5
	maxSearchLimit     = 20
	maxContentBytes    = 32 << 10
	maxValueBytes      = 4 << 10
	maxSourceEvents    = 20
)

type Retriever interface {
	Search(context.Context, string, string, string, int, time.Time) ([]domain.SearchHit, error)
}

type IDGenerator func(string) (string, error)
type Clock func() time.Time

type Option func(*Service)

func WithClock(clock Clock) Option {
	return func(service *Service) {
		service.now = clock
	}
}

func WithIDGenerator(generator IDGenerator) Option {
	return func(service *Service) {
		service.newID = generator
	}
}

func WithCandidateExtractor(extractor extraction.Extractor) Option {
	return func(service *Service) {
		service.extractor = extractor
	}
}

type Service struct {
	store               store.Store
	retriever           Retriever
	extractor           extraction.Extractor
	extractorDescriptor extraction.Descriptor
	now                 Clock
	newID               IDGenerator
}

func New(storage store.Store, retriever Retriever, options ...Option) (*Service, error) {
	if storage == nil {
		return nil, errors.New("store is required")
	}
	if retriever == nil {
		return nil, errors.New("retriever is required")
	}

	service := &Service{
		store:     storage,
		retriever: retriever,
		now:       func() time.Time { return time.Now().UTC() },
		newID:     id.New,
	}
	for _, option := range options {
		option(service)
	}
	if service.now == nil || service.newID == nil {
		return nil, errors.New("clock and id generator are required")
	}
	if service.extractor != nil {
		descriptor := service.extractor.Descriptor()
		if err := validateExtractionDescriptor(descriptor); err != nil {
			return nil, fmt.Errorf("configure candidate extractor: %w", err)
		}
		service.extractorDescriptor = descriptor
	}
	return service, nil
}

type IngestEvidenceInput struct {
	EventID    string
	TenantID   string
	UserID     string
	SessionID  string
	Actor      domain.Actor
	Content    string
	Metadata   map[string]string
	OccurredAt time.Time
}

func (s *Service) IngestEvidence(ctx context.Context, input IngestEvidenceInput) (domain.EvidenceEvent, error) {
	if err := validateScope(input.TenantID, input.UserID); err != nil {
		return domain.EvidenceEvent{}, err
	}
	if err := validateIdentifier("session_id", input.SessionID); err != nil {
		return domain.EvidenceEvent{}, err
	}
	if input.Actor != domain.ActorUser && input.Actor != domain.ActorAgent && input.Actor != domain.ActorTool {
		return domain.EvidenceEvent{}, invalid("actor must be user, agent, or tool")
	}
	if strings.TrimSpace(input.Content) == "" {
		return domain.EvidenceEvent{}, invalid("content is required")
	}
	if len(input.Content) > maxContentBytes {
		return domain.EvidenceEvent{}, invalid("content exceeds 32 KiB")
	}
	if err := validateMetadata(input.Metadata); err != nil {
		return domain.EvidenceEvent{}, err
	}

	now := s.now().UTC()
	eventID := strings.TrimSpace(input.EventID)
	if eventID == "" {
		var err error
		eventID, err = s.newID("evt")
		if err != nil {
			return domain.EvidenceEvent{}, err
		}
	} else if err := validateIdentifier("event_id", eventID); err != nil {
		return domain.EvidenceEvent{}, err
	}
	occurredAt := input.OccurredAt.UTC()
	if input.OccurredAt.IsZero() {
		occurredAt = now
	}

	event := domain.EvidenceEvent{
		ID:         eventID,
		TenantID:   strings.TrimSpace(input.TenantID),
		UserID:     strings.TrimSpace(input.UserID),
		SessionID:  strings.TrimSpace(input.SessionID),
		Actor:      input.Actor,
		Content:    input.Content,
		Metadata:   cloneMap(input.Metadata),
		OccurredAt: occurredAt,
		RecordedAt: now,
	}
	if err := s.store.AppendEvidence(ctx, event); err != nil {
		return domain.EvidenceEvent{}, err
	}
	return event, nil
}

type ProposeCandidateInput struct {
	TenantID         string
	UserID           string
	Kind             domain.MemoryKind
	Category         string
	Key              string
	Value            string
	Person           string
	Relationship     string
	Backstory        string
	SourceEventIDs   []string
	Extractor        string
	ExtractorVersion string
	Metadata         map[string]string
	ExpiresAt        *time.Time
}

func (s *Service) ProposeCandidate(ctx context.Context, input ProposeCandidateInput) (domain.MemoryCandidate, error) {
	sourceEventIDs, err := validateCandidateInput(input)
	if err != nil {
		return domain.MemoryCandidate{}, err
	}
	if _, err := s.store.EvidenceByIDs(ctx, input.TenantID, input.UserID, sourceEventIDs); err != nil {
		return domain.MemoryCandidate{}, fmt.Errorf("validate source evidence: %w", err)
	}

	candidate, err := s.newPendingCandidate(input, sourceEventIDs, s.now().UTC())
	if err != nil {
		return domain.MemoryCandidate{}, err
	}
	if err := s.store.CreateCandidate(ctx, candidate); err != nil {
		return domain.MemoryCandidate{}, err
	}
	return candidate, nil
}

func validateCandidateInput(input ProposeCandidateInput) ([]string, error) {
	if err := validateScope(input.TenantID, input.UserID); err != nil {
		return nil, err
	}
	if input.Kind != domain.MemoryKindEpisodic && input.Kind != domain.MemoryKindSemantic && input.Kind != domain.MemoryKindProcedural {
		return nil, invalid("kind must be episodic, semantic, or procedural")
	}
	if err := validateRequiredLabel("category", input.Category, 128); err != nil {
		return nil, err
	}
	if err := validateRequiredLabel("key", input.Key, 128); err != nil {
		return nil, err
	}
	if err := validateRequiredText("value", input.Value, maxValueBytes); err != nil {
		return nil, err
	}
	if err := validateOptionalLabel("person", input.Person, 256); err != nil {
		return nil, err
	}
	if err := validateOptionalLabel("relationship", input.Relationship, 256); err != nil {
		return nil, err
	}
	if err := validateOptionalText("backstory", input.Backstory, 2<<10); err != nil {
		return nil, err
	}
	if err := validateRequiredText("extractor", input.Extractor, 128); err != nil {
		return nil, err
	}
	if err := validateRequiredText("extractor_version", input.ExtractorVersion, 128); err != nil {
		return nil, err
	}
	if err := validateMetadata(input.Metadata); err != nil {
		return nil, err
	}
	if input.ExpiresAt != nil && input.ExpiresAt.IsZero() {
		return nil, invalid("expires_at must be a non-zero timestamp")
	}

	sourceEventIDs, err := uniqueIdentifiers("source_event_ids", input.SourceEventIDs, maxSourceEvents)
	if err != nil {
		return nil, err
	}
	return sourceEventIDs, nil
}

func (s *Service) newPendingCandidate(input ProposeCandidateInput, sourceEventIDs []string, createdAt time.Time) (domain.MemoryCandidate, error) {
	candidateID, err := s.newID("cand")
	if err != nil {
		return domain.MemoryCandidate{}, err
	}
	candidate := domain.MemoryCandidate{
		ID:               candidateID,
		TenantID:         strings.TrimSpace(input.TenantID),
		UserID:           strings.TrimSpace(input.UserID),
		Kind:             input.Kind,
		Category:         strings.TrimSpace(input.Category),
		Key:              strings.TrimSpace(input.Key),
		Value:            strings.TrimSpace(input.Value),
		Person:           strings.TrimSpace(input.Person),
		Relationship:     strings.TrimSpace(input.Relationship),
		Backstory:        strings.TrimSpace(input.Backstory),
		SourceEventIDs:   sourceEventIDs,
		Extractor:        strings.TrimSpace(input.Extractor),
		ExtractorVersion: strings.TrimSpace(input.ExtractorVersion),
		Status:           domain.CandidatePending,
		CreatedAt:        createdAt.UTC(),
		ExpiresAt:        cloneTime(input.ExpiresAt),
		Metadata:         cloneMap(input.Metadata),
	}
	return candidate, nil
}

type ReviewCandidateInput struct {
	TenantID    string
	UserID      string
	CandidateID string
	Decision    domain.ReviewDecision
	ReviewerID  string
	Reason      string
}

func (s *Service) ReviewCandidate(ctx context.Context, input ReviewCandidateInput) (domain.MemoryCandidate, *domain.MemoryCard, error) {
	if err := validateScope(input.TenantID, input.UserID); err != nil {
		return domain.MemoryCandidate{}, nil, err
	}
	if err := validateIdentifier("candidate_id", input.CandidateID); err != nil {
		return domain.MemoryCandidate{}, nil, err
	}
	if input.Decision != domain.DecisionApprove && input.Decision != domain.DecisionReject {
		return domain.MemoryCandidate{}, nil, invalid("decision must be approve or reject")
	}
	if err := validateIdentifier("reviewer_id", input.ReviewerID); err != nil {
		return domain.MemoryCandidate{}, nil, err
	}
	if err := validateRequiredText("reason", input.Reason, 2<<10); err != nil {
		return domain.MemoryCandidate{}, nil, err
	}

	memoryID := ""
	if input.Decision == domain.DecisionApprove {
		var err error
		memoryID, err = s.newID("mem")
		if err != nil {
			return domain.MemoryCandidate{}, nil, err
		}
	}
	return s.store.ReviewCandidate(ctx, store.CandidateReviewCommand{
		TenantID:    strings.TrimSpace(input.TenantID),
		UserID:      strings.TrimSpace(input.UserID),
		CandidateID: strings.TrimSpace(input.CandidateID),
		MemoryID:    memoryID,
		Review: domain.CandidateReview{
			Decision:   input.Decision,
			ReviewerID: strings.TrimSpace(input.ReviewerID),
			Reason:     strings.TrimSpace(input.Reason),
			ReviewedAt: s.now().UTC(),
		},
	})
}

type BuildContextInput struct {
	TenantID string
	UserID   string
	Query    string
	Limit    int
}

func (s *Service) BuildContext(ctx context.Context, input BuildContextInput) (domain.ContextPack, error) {
	if err := validateScope(input.TenantID, input.UserID); err != nil {
		return domain.ContextPack{}, err
	}
	if err := validateRequiredText("query", input.Query, 4<<10); err != nil {
		return domain.ContextPack{}, err
	}
	limit := input.Limit
	if limit == 0 {
		limit = defaultSearchLimit
	}
	if limit < 1 || limit > maxSearchLimit {
		return domain.ContextPack{}, invalid("limit must be between 1 and 20")
	}

	tenantID := strings.TrimSpace(input.TenantID)
	userID := strings.TrimSpace(input.UserID)
	query := strings.TrimSpace(input.Query)
	asOf := s.now().UTC()
	for attempt := 0; attempt < 3; attempt++ {
		startRevision, err := s.store.ContextRevision(ctx, tenantID, userID)
		if err != nil {
			return domain.ContextPack{}, err
		}
		hits, err := s.retriever.Search(ctx, tenantID, userID, query, limit, asOf)
		if err != nil {
			return domain.ContextPack{}, err
		}
		items := make([]domain.ContextItem, 0, len(hits))
		var sourceErr error
		for _, hit := range hits {
			if hit.Memory.TenantID != tenantID || hit.Memory.UserID != userID {
				return domain.ContextPack{}, fmt.Errorf("retriever returned a memory outside the requested scope: %w", domain.ErrInvariant)
			}
			if !hit.Memory.ServiceableAt(asOf) || len(hit.Memory.SourceEventIDs) == 0 {
				return domain.ContextPack{}, fmt.Errorf("retriever returned a non-serviceable memory: %w", domain.ErrInvariant)
			}
			sources, err := s.store.EvidenceByIDs(ctx, tenantID, userID, hit.Memory.SourceEventIDs)
			if err != nil {
				sourceErr = fmt.Errorf("load source evidence for memory %q: %w", hit.Memory.ID, err)
				break
			}
			items = append(items, domain.ContextItem{Memory: hit.Memory, Score: hit.Score, Sources: sources})
		}
		endRevision, err := s.store.ContextRevision(ctx, tenantID, userID)
		if err != nil {
			return domain.ContextPack{}, err
		}
		if startRevision != endRevision {
			continue
		}
		if sourceErr != nil {
			return domain.ContextPack{}, sourceErr
		}
		return domain.ContextPack{
			TenantID:    tenantID,
			UserID:      userID,
			Query:       query,
			Items:       items,
			GeneratedAt: asOf,
		}, nil
	}
	return domain.ContextPack{}, fmt.Errorf("context changed during retrieval; retry request: %w", domain.ErrConflict)
}

func (s *Service) ForgetUser(ctx context.Context, tenantID, userID string) (domain.DeletionReceipt, error) {
	if err := validateScope(tenantID, userID); err != nil {
		return domain.DeletionReceipt{}, err
	}
	return s.store.ForgetUser(ctx, strings.TrimSpace(tenantID), strings.TrimSpace(userID), s.now().UTC())
}

func validateScope(tenantID, userID string) error {
	if err := validateIdentifier("tenant_id", tenantID); err != nil {
		return err
	}
	return validateIdentifier("user_id", userID)
}

func validateIdentifier(name, value string) error {
	value = strings.TrimSpace(value)
	if value == "" {
		return invalid(name + " is required")
	}
	if len(value) > 128 || !utf8.ValidString(value) {
		return invalid(name + " is invalid")
	}
	for _, char := range value {
		if char <= 0x20 || char == 0x7f {
			return invalid(name + " cannot contain whitespace or control characters")
		}
	}
	return nil
}

func validateRequiredText(name, value string, maxBytes int) error {
	if strings.TrimSpace(value) == "" {
		return invalid(name + " is required")
	}
	if len(value) > maxBytes || !utf8.ValidString(value) {
		return invalid(fmt.Sprintf("%s exceeds %d bytes or is not UTF-8", name, maxBytes))
	}
	return nil
}

func validateOptionalText(name, value string, maxBytes int) error {
	if value == "" {
		return nil
	}
	if len(value) > maxBytes || !utf8.ValidString(value) {
		return invalid(fmt.Sprintf("%s exceeds %d bytes or is not UTF-8", name, maxBytes))
	}
	return nil
}

func validateRequiredLabel(name, value string, maxBytes int) error {
	if err := validateRequiredText(name, value, maxBytes); err != nil {
		return err
	}
	return rejectControlCharacters(name, value)
}

func validateOptionalLabel(name, value string, maxBytes int) error {
	if err := validateOptionalText(name, value, maxBytes); err != nil {
		return err
	}
	return rejectControlCharacters(name, value)
}

func rejectControlCharacters(name, value string) error {
	for _, char := range value {
		if char < 0x20 || char == 0x7f || (char >= 0x80 && char <= 0x9f) {
			return invalid(name + " cannot contain control characters")
		}
	}
	return nil
}

func uniqueIdentifiers(name string, values []string, max int) ([]string, error) {
	if len(values) == 0 {
		return nil, invalid(name + " must contain at least one item")
	}
	if len(values) > max {
		return nil, invalid(fmt.Sprintf("%s cannot contain more than %d items", name, max))
	}
	seen := make(map[string]struct{}, len(values))
	result := make([]string, 0, len(values))
	for _, value := range values {
		if err := validateIdentifier(name, value); err != nil {
			return nil, err
		}
		value = strings.TrimSpace(value)
		if _, exists := seen[value]; exists {
			continue
		}
		seen[value] = struct{}{}
		result = append(result, value)
	}
	return result, nil
}

func validateMetadata(metadata map[string]string) error {
	if len(metadata) > 32 {
		return invalid("metadata cannot contain more than 32 entries")
	}
	for key, value := range metadata {
		if err := validateIdentifier("metadata key", key); err != nil {
			return err
		}
		if len(value) > 1024 || !utf8.ValidString(value) {
			return invalid("metadata value exceeds 1024 bytes or is not UTF-8")
		}
	}
	return nil
}

func invalid(message string) error {
	return fmt.Errorf("%s: %w", message, domain.ErrInvalid)
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

func cloneTime(input *time.Time) *time.Time {
	if input == nil {
		return nil
	}
	// PostgreSQL timestamptz and pgx use microsecond precision. Canonicalizing
	// at the application boundary keeps the create response, durable reloads,
	// the in-memory adapter, and the exclusive expiration comparison identical.
	value := input.UTC().Truncate(time.Microsecond)
	return &value
}
