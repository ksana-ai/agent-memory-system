package app

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"unicode/utf8"

	"github.com/kai443/go-agent-memory-system/internal/domain"
	"github.com/kai443/go-agent-memory-system/internal/extraction"
	"github.com/kai443/go-agent-memory-system/internal/store"
)

const (
	maxExtractionCandidates    = 10
	maxExtractionEvidenceBytes = 64 << 10
	maxSupportingQuoteBytes    = 1024

	extractionGroundingVersion = "verbatim-quote-v1"
)

type ExtractCandidatesInput struct {
	TenantID       string
	UserID         string
	SourceEventIDs []string
}

type ExtractCandidatesResult struct {
	ExtractionID     string
	SourceEventIDs   []string
	ExtractorName    string
	ExtractorVersion string
	Candidates       []domain.MemoryCandidate
}

// ExtractCandidates invokes the configured model outside every database
// transaction, validates its complete result as untrusted input, and then
// creates the pending candidates in one revision-fenced store transaction.
// Only ReviewCandidate can promote one of these proposals into retrieval.
func (s *Service) ExtractCandidates(ctx context.Context, input ExtractCandidatesInput) (ExtractCandidatesResult, error) {
	if err := validateScope(input.TenantID, input.UserID); err != nil {
		return ExtractCandidatesResult{}, err
	}
	if s.extractor == nil {
		return ExtractCandidatesResult{}, fmt.Errorf("candidate extractor is not enabled: %w", domain.ErrExtractionDisabled)
	}

	sourceEventIDs, err := uniqueIdentifiers("source_event_ids", input.SourceEventIDs, maxSourceEvents)
	if err != nil {
		return ExtractCandidatesResult{}, err
	}
	if len(sourceEventIDs) != len(input.SourceEventIDs) {
		return ExtractCandidatesResult{}, invalid("source_event_ids cannot contain duplicates")
	}
	tenantID := strings.TrimSpace(input.TenantID)
	userID := strings.TrimSpace(input.UserID)

	expectedRevision, err := s.store.ContextRevision(ctx, tenantID, userID)
	if err != nil {
		return ExtractCandidatesResult{}, err
	}
	evidenceEvents, err := s.store.EvidenceByIDs(ctx, tenantID, userID, sourceEventIDs)
	if err != nil {
		return ExtractCandidatesResult{}, err
	}
	afterLoadRevision, err := s.store.ContextRevision(ctx, tenantID, userID)
	if err != nil {
		return ExtractCandidatesResult{}, err
	}
	if afterLoadRevision != expectedRevision {
		return ExtractCandidatesResult{}, fmt.Errorf("evidence scope changed while loading extraction input: %w", domain.ErrConflict)
	}

	modelEvidence := make([]extraction.Evidence, len(evidenceEvents))
	totalEvidenceBytes := 0
	evidenceByID := make(map[string]domain.EvidenceEvent, len(evidenceEvents))
	for index, event := range evidenceEvents {
		totalEvidenceBytes += len(event.Content)
		if totalEvidenceBytes > maxExtractionEvidenceBytes {
			return ExtractCandidatesResult{}, invalid("source evidence content exceeds 64 KiB in total")
		}
		evidenceByID[event.ID] = event
		modelEvidence[index] = extraction.Evidence{
			ID:         event.ID,
			SessionID:  event.SessionID,
			Actor:      event.Actor,
			Content:    event.Content,
			OccurredAt: event.OccurredAt,
		}
	}

	modelResult, err := s.extractor.Extract(ctx, extraction.Request{Evidence: modelEvidence})
	if err != nil {
		return ExtractCandidatesResult{}, mapCandidateExtractionError(ctx, err)
	}
	if len(modelResult.Candidates) > maxExtractionCandidates {
		return ExtractCandidatesResult{}, invalidExtractorOutput("candidate count exceeds 10")
	}

	extractionID, err := s.newID("extract")
	if err != nil {
		return ExtractCandidatesResult{}, err
	}
	createdAt := s.now().UTC()
	candidates := make([]domain.MemoryCandidate, 0, len(modelResult.Candidates))
	seenIdentities := make(map[domain.MemoryIdentity]struct{}, len(modelResult.Candidates))
	for index, proposal := range modelResult.Candidates {
		sourceIDs, validateErr := validateExtractedSupports(index, proposal.Supports, evidenceByID)
		if validateErr != nil {
			return ExtractCandidatesResult{}, validateErr
		}
		candidateInput := ProposeCandidateInput{
			TenantID:         tenantID,
			UserID:           userID,
			Kind:             proposal.Kind,
			Category:         proposal.Category,
			Key:              proposal.Key,
			Value:            proposal.Value,
			Person:           proposal.Person,
			Relationship:     proposal.Relationship,
			Backstory:        proposal.Backstory,
			SourceEventIDs:   sourceIDs,
			Extractor:        s.extractorDescriptor.Name,
			ExtractorVersion: s.extractorDescriptor.Version,
			Metadata: map[string]string{
				"extraction_run_id":       extractionID,
				"extraction_protocol":     s.extractorDescriptor.Protocol,
				"extraction_grounding":    extractionGroundingVersion,
				"extraction_source_count": strconv.Itoa(len(sourceIDs)),
			},
		}
		normalizedSourceIDs, validateErr := validateCandidateInput(candidateInput)
		if validateErr != nil {
			return ExtractCandidatesResult{}, invalidExtractorOutput(fmt.Sprintf("candidate %d violates the candidate contract", index))
		}
		candidate, createErr := s.newPendingCandidate(candidateInput, normalizedSourceIDs, createdAt)
		if createErr != nil {
			return ExtractCandidatesResult{}, createErr
		}
		identity := (domain.MemoryCard{
			Kind: candidate.Kind, Category: candidate.Category, Key: candidate.Key,
			Person: candidate.Person, Relationship: candidate.Relationship,
		}).Identity()
		if _, duplicate := seenIdentities[identity]; duplicate {
			return ExtractCandidatesResult{}, invalidExtractorOutput("candidate identities are duplicated")
		}
		seenIdentities[identity] = struct{}{}
		candidates = append(candidates, candidate)
	}

	if err := s.store.CreateCandidateBatch(ctx, store.CandidateBatchCommand{
		TenantID:         tenantID,
		UserID:           userID,
		ExpectedRevision: expectedRevision,
		Candidates:       candidates,
	}); err != nil {
		return ExtractCandidatesResult{}, err
	}
	return ExtractCandidatesResult{
		ExtractionID:     extractionID,
		SourceEventIDs:   append([]string(nil), sourceEventIDs...),
		ExtractorName:    s.extractorDescriptor.Name,
		ExtractorVersion: s.extractorDescriptor.Version,
		Candidates:       candidates,
	}, nil
}

func validateExtractionDescriptor(descriptor extraction.Descriptor) error {
	if err := validateRequiredLabel("extractor name", descriptor.Name, 128); err != nil {
		return err
	}
	if err := validateRequiredLabel("extractor version", descriptor.Version, 128); err != nil {
		return err
	}
	return validateRequiredLabel("extractor protocol", descriptor.Protocol, 128)
}

func validateExtractedSupports(index int, supports []extraction.Support, evidenceByID map[string]domain.EvidenceEvent) ([]string, error) {
	if len(supports) == 0 || len(supports) > maxSourceEvents {
		return nil, invalidExtractorOutput(fmt.Sprintf("candidate %d has an invalid support count", index))
	}
	seen := make(map[string]struct{}, len(supports))
	sourceIDs := make([]string, 0, len(supports))
	for _, support := range supports {
		evidenceID := strings.TrimSpace(support.EvidenceID)
		if err := validateIdentifier("support evidence_id", evidenceID); err != nil {
			return nil, invalidExtractorOutput(fmt.Sprintf("candidate %d has an invalid support identifier", index))
		}
		if _, duplicate := seen[evidenceID]; duplicate {
			return nil, invalidExtractorOutput(fmt.Sprintf("candidate %d repeats support evidence", index))
		}
		seen[evidenceID] = struct{}{}
		evidence, exists := evidenceByID[evidenceID]
		if !exists {
			return nil, invalidExtractorOutput(fmt.Sprintf("candidate %d cites evidence outside the request", index))
		}
		quote := support.Quote
		if strings.TrimSpace(quote) == "" || len(quote) > maxSupportingQuoteBytes || !utf8.ValidString(quote) {
			return nil, invalidExtractorOutput(fmt.Sprintf("candidate %d has an invalid supporting quote", index))
		}
		if !strings.Contains(evidence.Content, quote) {
			return nil, invalidExtractorOutput(fmt.Sprintf("candidate %d supporting quote is not present in evidence", index))
		}
		sourceIDs = append(sourceIDs, evidenceID)
	}
	return sourceIDs, nil
}

func mapCandidateExtractionError(ctx context.Context, err error) error {
	if contextErr := ctx.Err(); contextErr != nil {
		return contextErr
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) || errors.Is(err, extraction.ErrTimeout) {
		if errors.Is(err, context.Canceled) {
			return context.Canceled
		}
		return context.DeadlineExceeded
	}
	if errors.Is(err, extraction.ErrInvalidRequest) || errors.Is(err, extraction.ErrInvalidConfig) {
		return fmt.Errorf("candidate extractor request contract failed: %w", domain.ErrInvariant)
	}
	if errors.Is(err, extraction.ErrRefused) {
		return fmt.Errorf("candidate extractor rejected the request: %w", domain.ErrExtractionRejected)
	}
	var statusError *extraction.HTTPStatusError
	if errors.As(err, &statusError) {
		if statusError.StatusCode == http.StatusRequestTimeout ||
			statusError.StatusCode == http.StatusTooManyRequests ||
			statusError.StatusCode >= http.StatusInternalServerError {
			return fmt.Errorf("candidate extractor service is unavailable: %w", domain.ErrExtractionUnavailable)
		}
		return fmt.Errorf("candidate extractor rejected the request: %w", domain.ErrExtractionRejected)
	}
	if errors.Is(err, extraction.ErrInvalidResponse) || errors.Is(err, extraction.ErrResponseTooLarge) {
		return invalidExtractorOutput("model response failed the structured-output contract")
	}
	return fmt.Errorf("candidate extractor service is unavailable: %w", domain.ErrExtractionUnavailable)
}

func invalidExtractorOutput(reason string) error {
	return fmt.Errorf("%s: %w", reason, domain.ErrExtractionInvalidResponse)
}
