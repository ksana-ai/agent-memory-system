package app_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/ksana-ai/agent-memory-system/internal/app"
	"github.com/ksana-ai/agent-memory-system/internal/domain"
	"github.com/ksana-ai/agent-memory-system/internal/extraction"
	"github.com/ksana-ai/agent-memory-system/internal/retrieval"
	"github.com/ksana-ai/agent-memory-system/internal/store/memstore"
)

func TestExtractCandidatesCreatesGroundedPendingCandidatesOnly(t *testing.T) {
	extractor := &candidateExtractorStub{result: extraction.Result{Candidates: []extraction.Proposal{{
		Kind:         domain.MemoryKindSemantic,
		Category:     "travel",
		Key:          "seat_preference",
		Value:        "window seat",
		Person:       "self",
		Relationship: "self",
		Backstory:    "The user directly stated this preference.",
		Supports: []extraction.Support{{
			EvidenceID: "evt_extract_success",
			Quote:      "prefer a window seat",
		}},
	}}}}
	service, storage := newExtractionTestService(t, extractor, nil)
	ingestExtractionEvidence(t, service, "tenant-a", "user-a", "evt_extract_success", "I prefer a window seat when I fly.")

	result, err := service.ExtractCandidates(context.Background(), app.ExtractCandidatesInput{
		TenantID:       "tenant-a",
		UserID:         "user-a",
		SourceEventIDs: []string{"evt_extract_success"},
	})
	if err != nil {
		t.Fatalf("extract candidates: %v", err)
	}
	if extractor.calls != 1 || len(extractor.lastRequest.Evidence) != 1 {
		t.Fatalf("extractor calls=%d request=%#v", extractor.calls, extractor.lastRequest)
	}
	if result.ExtractionID == "" || result.ExtractorName != "stub-extractor" || result.ExtractorVersion != "fixture-v1" {
		t.Fatalf("unexpected extraction audit result: %#v", result)
	}
	if len(result.Candidates) != 1 {
		t.Fatalf("candidate count=%d, want 1", len(result.Candidates))
	}
	candidate := result.Candidates[0]
	if candidate.Status != domain.CandidatePending || candidate.Review != nil {
		t.Fatalf("extracted candidate bypassed review: %#v", candidate)
	}
	if candidate.Extractor != "stub-extractor" || candidate.ExtractorVersion != "fixture-v1" ||
		len(candidate.SourceEventIDs) != 1 || candidate.SourceEventIDs[0] != "evt_extract_success" {
		t.Fatalf("candidate provenance=%#v", candidate)
	}
	if candidate.Metadata["extraction_run_id"] != result.ExtractionID ||
		candidate.Metadata["extraction_grounding"] != "verbatim-quote-v1" {
		t.Fatalf("candidate audit metadata=%#v", candidate.Metadata)
	}
	stored, err := storage.CandidateByID(context.Background(), "tenant-a", "user-a", candidate.ID)
	if err != nil || stored.Status != domain.CandidatePending {
		t.Fatalf("stored candidate=%#v error=%v", stored, err)
	}

	before := buildContext(t, service, "tenant-a", "user-a", "window seat")
	if len(before.Items) != 0 {
		t.Fatalf("pending extraction leaked into retrieval: %#v", before.Items)
	}
	approve(t, service, "tenant-a", "user-a", candidate.ID)
	after := buildContext(t, service, "tenant-a", "user-a", "window seat")
	if len(after.Items) != 1 || after.Items[0].Memory.CandidateID != candidate.ID {
		t.Fatalf("approved extraction was not retrievable: %#v", after.Items)
	}
}

func TestExtractCandidatesAllowsAtomicEmptyResult(t *testing.T) {
	extractor := &candidateExtractorStub{result: extraction.Result{Candidates: []extraction.Proposal{}}}
	service, _ := newExtractionTestService(t, extractor, nil)
	ingestExtractionEvidence(t, service, "tenant-a", "user-a", "evt_extract_empty", "There is no durable preference here.")

	result, err := service.ExtractCandidates(context.Background(), app.ExtractCandidatesInput{
		TenantID: "tenant-a", UserID: "user-a", SourceEventIDs: []string{"evt_extract_empty"},
	})
	if err != nil {
		t.Fatalf("extract empty result: %v", err)
	}
	if result.Candidates == nil || len(result.Candidates) != 0 || result.ExtractionID == "" {
		t.Fatalf("empty result=%#v, want non-nil empty candidates and audit id", result)
	}
}

func TestExtractCandidatesMapsModelFailuresWithoutPersistence(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want error
	}{
		{name: "network", err: extraction.ErrRequestFailed, want: domain.ErrExtractionUnavailable},
		{name: "refusal", err: &extraction.RefusalError{}, want: domain.ErrExtractionRejected},
		{name: "invalid structured response", err: &extraction.InvalidResponseError{Reason: "fixture"}, want: domain.ErrExtractionInvalidResponse},
		{name: "timeout", err: &extraction.TimeoutError{}, want: context.DeadlineExceeded},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			extractor := &candidateExtractorStub{err: test.err}
			service, storage := newExtractionTestService(t, extractor, nil)
			ingestExtractionEvidence(t, service, "tenant-a", "user-a", "evt_model_failure", "I prefer tea.")

			_, err := service.ExtractCandidates(context.Background(), app.ExtractCandidatesInput{
				TenantID: "tenant-a", UserID: "user-a", SourceEventIDs: []string{"evt_model_failure"},
			})
			if !errors.Is(err, test.want) {
				t.Fatalf("error=%v, want %v", err, test.want)
			}
			if _, err = storage.CandidateByID(context.Background(), "tenant-a", "user-a", "cand_extract_002"); !errors.Is(err, domain.ErrNotFound) {
				t.Fatalf("model failure left a candidate: %v", err)
			}
		})
	}
}

func TestExtractCandidatesRejectsInvalidBatchWithoutPartialCandidates(t *testing.T) {
	extractor := &candidateExtractorStub{result: extraction.Result{Candidates: []extraction.Proposal{
		{
			Kind: domain.MemoryKindSemantic, Category: "travel", Key: "seat", Value: "window",
			Supports: []extraction.Support{{EvidenceID: "evt_atomic", Quote: "window seat"}},
		},
		{
			Kind: domain.MemoryKindSemantic, Category: "travel", Key: "meal", Value: "vegetarian",
			Supports: []extraction.Support{{EvidenceID: "evt_atomic", Quote: "text that does not occur"}},
		},
	}}}
	generatedCandidateIDs := make([]string, 0)
	idCounter := 0
	generator := func(prefix string) (string, error) {
		idCounter++
		value := fmt.Sprintf("%s_atomic_%03d", prefix, idCounter)
		if prefix == "cand" {
			generatedCandidateIDs = append(generatedCandidateIDs, value)
		}
		return value, nil
	}
	service, storage := newExtractionTestService(t, extractor, generator)
	ingestExtractionEvidence(t, service, "tenant-a", "user-a", "evt_atomic", "I prefer a window seat.")

	_, err := service.ExtractCandidates(context.Background(), app.ExtractCandidatesInput{
		TenantID: "tenant-a", UserID: "user-a", SourceEventIDs: []string{"evt_atomic"},
	})
	if !errors.Is(err, domain.ErrExtractionInvalidResponse) {
		t.Fatalf("invalid model output error=%v", err)
	}
	if len(generatedCandidateIDs) != 1 {
		t.Fatalf("generated candidate ids=%v, want the validated prefix only", generatedCandidateIDs)
	}
	if _, err = storage.CandidateByID(context.Background(), "tenant-a", "user-a", generatedCandidateIDs[0]); !errors.Is(err, domain.ErrNotFound) {
		t.Fatalf("partial candidate remained after invalid batch: %v", err)
	}
}

func TestExtractCandidatesRejectsDuplicateAndOutOfRequestSources(t *testing.T) {
	tests := []struct {
		name      string
		proposals []extraction.Proposal
		sourceIDs []string
	}{
		{
			name: "source outside request",
			proposals: []extraction.Proposal{{
				Kind: domain.MemoryKindSemantic, Category: "travel", Key: "seat", Value: "window",
				Supports: []extraction.Support{{EvidenceID: "evt_not_requested", Quote: "aisle seat"}},
			}},
			sourceIDs: []string{"evt_requested"},
		},
		{
			name: "duplicate identity",
			proposals: []extraction.Proposal{
				{Kind: domain.MemoryKindSemantic, Category: "Travel", Key: "Seat", Value: "window", Supports: []extraction.Support{{EvidenceID: "evt_requested", Quote: "window seat"}}},
				{Kind: domain.MemoryKindSemantic, Category: " travel ", Key: " seat ", Value: "window", Supports: []extraction.Support{{EvidenceID: "evt_requested", Quote: "window seat"}}},
			},
			sourceIDs: []string{"evt_requested"},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			extractor := &candidateExtractorStub{result: extraction.Result{Candidates: test.proposals}}
			service, _ := newExtractionTestService(t, extractor, nil)
			ingestExtractionEvidence(t, service, "tenant-a", "user-a", "evt_requested", "I prefer a window seat.")
			ingestExtractionEvidence(t, service, "tenant-a", "user-a", "evt_not_requested", "I avoid the aisle seat.")

			_, err := service.ExtractCandidates(context.Background(), app.ExtractCandidatesInput{
				TenantID: "tenant-a", UserID: "user-a", SourceEventIDs: test.sourceIDs,
			})
			if !errors.Is(err, domain.ErrExtractionInvalidResponse) {
				t.Fatalf("error=%v, want invalid extractor output", err)
			}
		})
	}
}

func TestExtractCandidatesEnforcesServerSideCountAndFieldLimits(t *testing.T) {
	valid := extraction.Proposal{
		Kind: domain.MemoryKindSemantic, Category: "travel", Key: "seat", Value: "window",
		Supports: []extraction.Support{{EvidenceID: "evt_limits", Quote: "window seat"}},
	}
	tooMany := make([]extraction.Proposal, 11)
	for index := range tooMany {
		tooMany[index] = valid
		tooMany[index].Key = fmt.Sprintf("seat_%d", index)
	}
	tests := []struct {
		name      string
		proposals []extraction.Proposal
	}{
		{name: "candidate count", proposals: tooMany},
		{name: "value length", proposals: []extraction.Proposal{{
			Kind: domain.MemoryKindSemantic, Category: "travel", Key: "seat", Value: strings.Repeat("x", (4<<10)+1),
			Supports: valid.Supports,
		}}},
		{name: "duplicate support", proposals: []extraction.Proposal{{
			Kind: domain.MemoryKindSemantic, Category: "travel", Key: "seat", Value: "window",
			Supports: []extraction.Support{
				{EvidenceID: "evt_limits", Quote: "window seat"},
				{EvidenceID: "evt_limits", Quote: "window seat"},
			},
		}}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			extractor := &candidateExtractorStub{result: extraction.Result{Candidates: test.proposals}}
			service, _ := newExtractionTestService(t, extractor, nil)
			ingestExtractionEvidence(t, service, "tenant-a", "user-a", "evt_limits", "I prefer a window seat.")
			_, err := service.ExtractCandidates(context.Background(), app.ExtractCandidatesInput{
				TenantID: "tenant-a", UserID: "user-a", SourceEventIDs: []string{"evt_limits"},
			})
			if !errors.Is(err, domain.ErrExtractionInvalidResponse) {
				t.Fatalf("limit error=%v, want invalid extractor output", err)
			}
		})
	}
}

func TestExtractCandidatesRejectsDuplicateInputSourcesBeforeModelCall(t *testing.T) {
	extractor := &candidateExtractorStub{}
	service, _ := newExtractionTestService(t, extractor, nil)
	ingestExtractionEvidence(t, service, "tenant-a", "user-a", "evt_duplicate_input", "I prefer tea.")
	_, err := service.ExtractCandidates(context.Background(), app.ExtractCandidatesInput{
		TenantID: "tenant-a", UserID: "user-a",
		SourceEventIDs: []string{"evt_duplicate_input", "evt_duplicate_input"},
	})
	if !errors.Is(err, domain.ErrInvalid) || extractor.calls != 0 {
		t.Fatalf("duplicate input error=%v extractor calls=%d", err, extractor.calls)
	}
}

func TestExtractCandidatesRejectsCrossScopeEvidenceBeforeModelCall(t *testing.T) {
	extractor := &candidateExtractorStub{}
	service, _ := newExtractionTestService(t, extractor, nil)
	ingestExtractionEvidence(t, service, "tenant-a", "user-a", "evt_private", "tenant-a private fact")

	_, err := service.ExtractCandidates(context.Background(), app.ExtractCandidatesInput{
		TenantID: "tenant-b", UserID: "user-a", SourceEventIDs: []string{"evt_private"},
	})
	if !errors.Is(err, domain.ErrNotFound) || extractor.calls != 0 {
		t.Fatalf("cross-scope error=%v extractor calls=%d", err, extractor.calls)
	}
}

func TestExtractCandidatesRevisionFenceRejectsForgetAndRecreateABA(t *testing.T) {
	var storage *memstore.Store
	extractor := &candidateExtractorStub{result: extraction.Result{Candidates: []extraction.Proposal{{
		Kind: domain.MemoryKindSemantic, Category: "preference", Key: "drink", Value: "tea",
		Supports: []extraction.Support{{EvidenceID: "evt_aba", Quote: "prefer tea"}},
	}}}}
	extractor.onExtract = func(ctx context.Context, _ extraction.Request) {
		if _, err := storage.ForgetUser(ctx, "tenant-a", "user-a", time.Now().UTC()); err != nil {
			t.Fatalf("forget during extraction: %v", err)
		}
		if err := storage.AppendEvidence(ctx, domain.EvidenceEvent{
			ID: "evt_aba", TenantID: "tenant-a", UserID: "user-a", SessionID: "replacement",
			Actor: domain.ActorUser, Content: "I prefer tea.", OccurredAt: time.Now().UTC(), RecordedAt: time.Now().UTC(),
		}); err != nil {
			t.Fatalf("recreate evidence during extraction: %v", err)
		}
	}
	service, createdStorage := newExtractionTestService(t, extractor, nil)
	storage = createdStorage
	ingestExtractionEvidence(t, service, "tenant-a", "user-a", "evt_aba", "I prefer tea.")

	_, err := service.ExtractCandidates(context.Background(), app.ExtractCandidatesInput{
		TenantID: "tenant-a", UserID: "user-a", SourceEventIDs: []string{"evt_aba"},
	})
	if !errors.Is(err, domain.ErrConflict) {
		t.Fatalf("ABA extraction error=%v, want conflict", err)
	}
}

type candidateExtractorStub struct {
	result      extraction.Result
	err         error
	calls       int
	lastRequest extraction.Request
	onExtract   func(context.Context, extraction.Request)
}

func (stub *candidateExtractorStub) Extract(ctx context.Context, request extraction.Request) (extraction.Result, error) {
	stub.calls++
	stub.lastRequest = request
	if stub.onExtract != nil {
		stub.onExtract(ctx, request)
	}
	return stub.result, stub.err
}

func (*candidateExtractorStub) Descriptor() extraction.Descriptor {
	return extraction.Descriptor{
		Name: "stub-extractor", Version: "fixture-v1", Protocol: "test-stub-v1",
	}
}

func newExtractionTestService(t *testing.T, extractor extraction.Extractor, generator app.IDGenerator) (*app.Service, *memstore.Store) {
	t.Helper()
	storage := memstore.New()
	retriever, err := retrieval.NewBM25(storage)
	if err != nil {
		t.Fatalf("new extraction retriever: %v", err)
	}
	if generator == nil {
		counter := 0
		generator = func(prefix string) (string, error) {
			counter++
			return fmt.Sprintf("%s_extract_%03d", prefix, counter), nil
		}
	}
	service, err := app.New(
		storage,
		retriever,
		app.WithClock(func() time.Time { return time.Date(2026, 8, 31, 9, 0, 0, 0, time.UTC) }),
		app.WithIDGenerator(generator),
		app.WithCandidateExtractor(extractor),
	)
	if err != nil {
		t.Fatalf("new extraction service: %v", err)
	}
	return service, storage
}

func ingestExtractionEvidence(t *testing.T, service *app.Service, tenantID, userID, eventID, content string) {
	t.Helper()
	if _, err := service.IngestEvidence(context.Background(), app.IngestEvidenceInput{
		EventID: eventID, TenantID: tenantID, UserID: userID, SessionID: "session-extraction",
		Actor: domain.ActorUser, Content: content,
	}); err != nil {
		t.Fatalf("ingest extraction evidence: %v", err)
	}
}
