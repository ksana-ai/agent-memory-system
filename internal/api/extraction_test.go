package api_test

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"testing"

	"github.com/kai443/go-agent-memory-system/internal/api"
	"github.com/kai443/go-agent-memory-system/internal/app"
	"github.com/kai443/go-agent-memory-system/internal/domain"
	"github.com/kai443/go-agent-memory-system/internal/extraction"
	"github.com/kai443/go-agent-memory-system/internal/retrieval"
	"github.com/kai443/go-agent-memory-system/internal/store/memstore"
)

func TestHTTPAutomaticExtractionRequiresReviewBeforeRetrieval(t *testing.T) {
	extractor := &httpExtractorStub{result: extraction.Result{Candidates: []extraction.Proposal{{
		Kind: domain.MemoryKindSemantic, Category: "travel", Key: "seat_preference", Value: "window seat",
		Supports: []extraction.Support{{EvidenceID: "evt_http_extract", Quote: "prefer window seats"}},
	}}}}
	routes := newExtractionRoutes(t, extractor)

	evidenceResponse := perform(t, routes, http.MethodPost, "/v1/evidence", `{
		"event_id":"evt_http_extract",
		"session_id":"session-extract",
		"actor":"user",
		"content":"I prefer window seats on flights."
	}`, true)
	if evidenceResponse.Code != http.StatusCreated {
		t.Fatalf("ingest status=%d body=%s", evidenceResponse.Code, evidenceResponse.Body.String())
	}

	extractionResponse := perform(t, routes, http.MethodPost, "/v1/memory-candidate-extractions", `{
		"source_event_ids":["evt_http_extract"]
	}`, true)
	if extractionResponse.Code != http.StatusOK {
		t.Fatalf("extract status=%d body=%s", extractionResponse.Code, extractionResponse.Body.String())
	}
	var result struct {
		ExtractionID string `json:"extraction_id"`
		Extractor    struct {
			Name    string `json:"name"`
			Version string `json:"version"`
		} `json:"extractor"`
		Candidates []domain.MemoryCandidate `json:"candidates"`
	}
	decodeResponse(t, extractionResponse, &result)
	if result.ExtractionID == "" || result.Extractor.Name != "http-stub" || result.Extractor.Version != "v1" || len(result.Candidates) != 1 {
		t.Fatalf("unexpected extraction response: %#v", result)
	}
	if result.Candidates[0].Status != domain.CandidatePending {
		t.Fatalf("automatic extraction status=%q, want pending", result.Candidates[0].Status)
	}

	beforeReview := perform(t, routes, http.MethodPost, "/v1/context-packs", `{"query":"window seat"}`, true)
	var emptyPack domain.ContextPack
	decodeResponse(t, beforeReview, &emptyPack)
	if len(emptyPack.Items) != 0 {
		t.Fatalf("pending automatic candidate leaked into context: %#v", emptyPack.Items)
	}

	reviewResponse := perform(t, routes, http.MethodPost, "/v1/memory-candidates/"+result.Candidates[0].ID+"/reviews", `{
		"decision":"approve",
		"reviewer_id":"reviewer-http",
		"reason":"The exact quote supports this candidate."
	}`, true)
	if reviewResponse.Code != http.StatusOK {
		t.Fatalf("review status=%d body=%s", reviewResponse.Code, reviewResponse.Body.String())
	}
	afterReview := perform(t, routes, http.MethodPost, "/v1/context-packs", `{"query":"window seat"}`, true)
	var servedPack domain.ContextPack
	decodeResponse(t, afterReview, &servedPack)
	if len(servedPack.Items) != 1 {
		t.Fatalf("approved automatic candidate not retrievable: %#v", servedPack.Items)
	}
}

func TestHTTPAutomaticExtractionAllowsEmptyCandidateList(t *testing.T) {
	routes := newExtractionRoutes(t, &httpExtractorStub{result: extraction.Result{Candidates: []extraction.Proposal{}}})
	response := perform(t, routes, http.MethodPost, "/v1/evidence", `{
		"event_id":"evt_http_empty","session_id":"session-extract","actor":"user","content":"hello"
	}`, true)
	if response.Code != http.StatusCreated {
		t.Fatalf("ingest status=%d body=%s", response.Code, response.Body.String())
	}
	response = perform(t, routes, http.MethodPost, "/v1/memory-candidate-extractions", `{"source_event_ids":["evt_http_empty"]}`, true)
	if response.Code != http.StatusOK {
		t.Fatalf("extract status=%d body=%s", response.Code, response.Body.String())
	}
	var body map[string]json.RawMessage
	decodeResponse(t, response, &body)
	if string(body["candidates"]) != "[]" {
		t.Fatalf("empty candidates must be encoded as [], body=%s", response.Body.String())
	}
}

func TestHTTPAutomaticExtractionErrorsAreStableAndRedacted(t *testing.T) {
	tests := []struct {
		name       string
		extractor  extraction.Extractor
		wantStatus int
		wantCode   string
	}{
		{name: "disabled", wantStatus: http.StatusServiceUnavailable, wantCode: "extraction_disabled"},
		{name: "unavailable", extractor: &httpExtractorStub{err: errors.New("provider-secret: network failure")}, wantStatus: http.StatusServiceUnavailable, wantCode: "extraction_unavailable"},
		{name: "refused", extractor: &httpExtractorStub{err: &extraction.RefusalError{}}, wantStatus: http.StatusBadGateway, wantCode: "extraction_rejected"},
		{name: "invalid output", extractor: &httpExtractorStub{err: &extraction.InvalidResponseError{Reason: "provider-secret"}}, wantStatus: http.StatusBadGateway, wantCode: "invalid_extractor_output"},
		{name: "timeout", extractor: &httpExtractorStub{err: &extraction.TimeoutError{}}, wantStatus: http.StatusGatewayTimeout, wantCode: "request_timeout"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			routes := newExtractionRoutes(t, test.extractor)
			response := perform(t, routes, http.MethodPost, "/v1/evidence", `{
				"event_id":"evt_http_error","session_id":"session-extract","actor":"user","content":"I prefer tea."
			}`, true)
			if response.Code != http.StatusCreated {
				t.Fatalf("ingest status=%d body=%s", response.Code, response.Body.String())
			}
			response = perform(t, routes, http.MethodPost, "/v1/memory-candidate-extractions", `{"source_event_ids":["evt_http_error"]}`, true)
			if response.Code != test.wantStatus {
				t.Fatalf("status=%d want=%d body=%s", response.Code, test.wantStatus, response.Body.String())
			}
			var body struct {
				Error struct {
					Code string `json:"code"`
				} `json:"error"`
			}
			decodeResponse(t, response, &body)
			if body.Error.Code != test.wantCode || strings.Contains(response.Body.String(), "provider-secret") {
				t.Fatalf("error response was unstable or leaked detail: %s", response.Body.String())
			}
		})
	}
}

func TestHTTPAutomaticExtractionRejectsCallerSuppliedCandidateFields(t *testing.T) {
	routes := newExtractionRoutes(t, &httpExtractorStub{})
	response := perform(t, routes, http.MethodPost, "/v1/memory-candidate-extractions", `{
		"source_event_ids":["evt"],
		"value":"caller must not provide this"
	}`, true)
	if response.Code != http.StatusBadRequest {
		t.Fatalf("status=%d body=%s", response.Code, response.Body.String())
	}
}

type httpExtractorStub struct {
	result extraction.Result
	err    error
}

func (stub *httpExtractorStub) Extract(context.Context, extraction.Request) (extraction.Result, error) {
	return stub.result, stub.err
}

func (*httpExtractorStub) Descriptor() extraction.Descriptor {
	return extraction.Descriptor{Name: "http-stub", Version: "v1", Protocol: "test-stub-v1"}
}

func newExtractionRoutes(t *testing.T, extractor extraction.Extractor) http.Handler {
	t.Helper()
	storage := memstore.New()
	retriever, err := retrieval.NewBM25(storage)
	if err != nil {
		t.Fatalf("new extraction HTTP retriever: %v", err)
	}
	options := make([]app.Option, 0, 1)
	if extractor != nil {
		options = append(options, app.WithCandidateExtractor(extractor))
	}
	service, err := app.New(storage, retriever, options...)
	if err != nil {
		t.Fatalf("new extraction HTTP service: %v", err)
	}
	handler, err := api.NewHandler(service)
	if err != nil {
		t.Fatalf("new extraction HTTP handler: %v", err)
	}
	return handler.Routes()
}
