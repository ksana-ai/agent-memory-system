//go:build integration

package main_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/kai443/go-agent-memory-system/internal/domain"
	"github.com/kai443/go-agent-memory-system/internal/extraction"
)

const (
	processExtractionModel   = "process-fake-extraction-model"
	processExtractorName     = "process-fake-extractor"
	processExtractorVersion  = "process-v1"
	processExtractionToken   = "process-fake-bearer-token"
	processExtractionQuote   = "I prefer window seats"
	processExtractionContent = "I prefer window seats on flights."
)

func TestServerProcessAutomaticExtractionRequiresExplicitReview(t *testing.T) {
	databaseURL := requiredServerTestDatabaseURL(t)
	serverBinary := requiredServerBinary(t)
	applyServerTestMigrations(t, databaseURL)

	suffix := fmt.Sprintf("%d", time.Now().UnixNano())
	tenantID, userID := "tenant_extraction_"+suffix, "user_extraction_"+suffix
	invalidTenantID, invalidUserID := "tenant_extraction_invalid_"+suffix, "user_extraction_invalid_"+suffix
	cleanupServerTestScopes(t, databaseURL, [][2]string{{tenantID, userID}, {invalidTenantID, invalidUserID}})

	var modelCalls atomic.Int32
	modelServer := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		modelCalls.Add(1)
		eventID, err := validateProcessExtractionModelRequest(request)
		if err != nil {
			t.Errorf("validate fake model request: %v", err)
			http.Error(writer, "invalid test request", http.StatusBadRequest)
			return
		}

		content := `{"candidates":[{"kind":"semantic"}]}`
		if eventID == "event-extraction" {
			encoded, encodeErr := json.Marshal(map[string]any{
				"candidates": []any{map[string]any{
					"kind":         "semantic",
					"category":     "travel",
					"key":          "seat_preference",
					"value":        "window seat",
					"person":       "self",
					"relationship": "self",
					"backstory":    "The user explicitly stated this flight preference.",
					"supports": []any{map[string]any{
						"evidence_id": eventID,
						"quote":       processExtractionQuote,
					}},
				}},
			})
			if encodeErr != nil {
				t.Errorf("encode fake model content: %v", encodeErr)
				http.Error(writer, "encode test response", http.StatusInternalServerError)
				return
			}
			content = string(encoded)
		}

		writer.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(writer).Encode(map[string]any{
			"id":    "process-fake-completion",
			"model": processExtractionModel,
			"choices": []any{map[string]any{
				"index": 0,
				"message": map[string]any{
					"role":    "assistant",
					"content": content,
					"refusal": nil,
				},
			}},
		}); err != nil {
			t.Errorf("write fake model completion: %v", err)
		}
	}))
	defer modelServer.Close()

	running := startServerWithEnvironment(t, serverBinary, databaseURL, map[string]string{
		"SERVER_RETRIEVAL_MODE":               "fts",
		"MEMORY_EXTRACTION_ENABLED":           "true",
		"MEMORY_EXTRACTION_ENDPOINT":          modelServer.URL + "/v1/chat/completions",
		"MEMORY_EXTRACTION_MODEL":             processExtractionModel,
		"MEMORY_EXTRACTION_AUTH_MODE":         "bearer",
		"MEMORY_EXTRACTION_BEARER_TOKEN":      processExtractionToken,
		"MEMORY_EXTRACTION_TIMEOUT":           "2s",
		"MEMORY_EXTRACTION_EXTRACTOR_NAME":    processExtractorName,
		"MEMORY_EXTRACTION_EXTRACTOR_VERSION": processExtractorVersion,
	})
	client := &http.Client{Timeout: 5 * time.Second}

	var event domain.EvidenceEvent
	doServerJSON(t, client, http.MethodPost, running.baseURL+"/v1/evidence", tenantID, userID, map[string]any{
		"event_id": "event-extraction", "session_id": "session-extraction",
		"actor": "user", "content": processExtractionContent,
	}, http.StatusCreated, &event)
	if event.ID != "event-extraction" || event.TenantID != tenantID || event.UserID != userID {
		t.Fatalf("created extraction evidence=%#v", event)
	}

	var extracted struct {
		ExtractionID   string   `json:"extraction_id"`
		SourceEventIDs []string `json:"source_event_ids"`
		Extractor      struct {
			Name    string `json:"name"`
			Version string `json:"version"`
		} `json:"extractor"`
		Candidates []domain.MemoryCandidate `json:"candidates"`
	}
	doServerJSON(t, client, http.MethodPost, running.baseURL+"/v1/memory-candidate-extractions", tenantID, userID, map[string]any{
		"source_event_ids": []string{event.ID},
	}, http.StatusOK, &extracted)
	if extracted.ExtractionID == "" || len(extracted.SourceEventIDs) != 1 || extracted.SourceEventIDs[0] != event.ID ||
		extracted.Extractor.Name != processExtractorName || extracted.Extractor.Version != processExtractorVersion || len(extracted.Candidates) != 1 {
		t.Fatalf("extraction response=%#v", extracted)
	}
	candidate := extracted.Candidates[0]
	if candidate.ID == "" || candidate.TenantID != tenantID || candidate.UserID != userID ||
		candidate.Status != domain.CandidatePending || candidate.Review != nil ||
		candidate.Kind != domain.MemoryKindSemantic || candidate.Category != "travel" ||
		candidate.Key != "seat_preference" || candidate.Value != "window seat" ||
		candidate.Extractor != processExtractorName || candidate.ExtractorVersion != processExtractorVersion ||
		len(candidate.SourceEventIDs) != 1 || candidate.SourceEventIDs[0] != event.ID {
		t.Fatalf("pending extracted candidate=%#v", candidate)
	}
	if candidate.Metadata["extraction_run_id"] != extracted.ExtractionID ||
		candidate.Metadata["extraction_protocol"] != extraction.ProtocolOpenAICompatibleChatCompletionsJSONSchema ||
		candidate.Metadata["extraction_grounding"] != "verbatim-quote-v1" ||
		candidate.Metadata["extraction_source_count"] != "1" {
		t.Fatalf("candidate provenance metadata=%#v", candidate.Metadata)
	}

	assertContextPack(t, client, running.baseURL, tenantID, userID, "window seat", "", 0)

	var reviewed struct {
		Candidate domain.MemoryCandidate `json:"candidate"`
		Memory    *domain.MemoryCard     `json:"memory"`
	}
	doServerJSON(
		t,
		client,
		http.MethodPost,
		running.baseURL+"/v1/memory-candidates/"+url.PathEscape(candidate.ID)+"/reviews",
		tenantID,
		userID,
		map[string]any{
			"decision": "approve", "reviewer_id": "process-extraction-reviewer",
			"reason": "The exact source quote supports this candidate.",
		},
		http.StatusOK,
		&reviewed,
	)
	if reviewed.Candidate.Status != domain.CandidateApproved || reviewed.Memory == nil ||
		reviewed.Memory.Status != domain.MemoryActive || reviewed.Memory.CandidateID != candidate.ID ||
		len(reviewed.Memory.SourceEventIDs) != 1 || reviewed.Memory.SourceEventIDs[0] != event.ID {
		t.Fatalf("reviewed extraction response=%#v", reviewed)
	}
	assertContextPack(t, client, running.baseURL, tenantID, userID, "window seat", "window seat", 1)

	var invalidEvent domain.EvidenceEvent
	doServerJSON(t, client, http.MethodPost, running.baseURL+"/v1/evidence", invalidTenantID, invalidUserID, map[string]any{
		"event_id": "event-invalid-output", "session_id": "session-invalid-output",
		"actor": "user", "content": "I prefer tea.",
	}, http.StatusCreated, &invalidEvent)
	var invalidResponse struct {
		Error struct {
			Code    string `json:"code"`
			Message string `json:"message"`
		} `json:"error"`
	}
	doServerJSON(t, client, http.MethodPost, running.baseURL+"/v1/memory-candidate-extractions", invalidTenantID, invalidUserID, map[string]any{
		"source_event_ids": []string{invalidEvent.ID},
	}, http.StatusBadGateway, &invalidResponse)
	if invalidResponse.Error.Code != "invalid_extractor_output" || invalidResponse.Error.Message != "candidate extractor returned invalid output" {
		t.Fatalf("invalid extraction error=%#v", invalidResponse)
	}
	assertProcessExtractionCandidateCount(t, databaseURL, invalidTenantID, invalidUserID, 0)

	if modelCalls.Load() != 2 {
		t.Fatalf("fake model calls=%d, want 2", modelCalls.Load())
	}
	running.stop(t)
	if logs := running.logs.String(); strings.Contains(logs, processExtractionToken) || strings.Contains(logs, processExtractionContent) {
		t.Fatalf("server logs leaked extraction credential or evidence: %s", logs)
	}
}

type processExtractionModelRequest struct {
	Model    string `json:"model"`
	Messages []struct {
		Role    string `json:"role"`
		Content string `json:"content"`
	} `json:"messages"`
	ResponseFormat struct {
		Type       string `json:"type"`
		JSONSchema struct {
			Name   string         `json:"name"`
			Strict bool           `json:"strict"`
			Schema map[string]any `json:"schema"`
		} `json:"json_schema"`
	} `json:"response_format"`
	Temperature float64 `json:"temperature"`
}

func validateProcessExtractionModelRequest(request *http.Request) (string, error) {
	if request.Method != http.MethodPost || request.URL.Path != "/v1/chat/completions" {
		return "", fmt.Errorf("method/path=%s/%s", request.Method, request.URL.Path)
	}
	if request.Header.Get("Content-Type") != "application/json" || request.Header.Get("Accept") != "application/json" {
		return "", errors.New("content negotiation headers are missing")
	}
	if request.Header.Get("Authorization") != "Bearer "+processExtractionToken {
		return "", errors.New("bearer authentication is missing")
	}
	decoder := json.NewDecoder(request.Body)
	decoder.DisallowUnknownFields()
	var body processExtractionModelRequest
	if err := decoder.Decode(&body); err != nil {
		return "", fmt.Errorf("decode request: %w", err)
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return "", errors.New("request contains trailing JSON")
	}
	if body.Model != processExtractionModel || body.Temperature != 0 || len(body.Messages) != 2 ||
		body.Messages[0].Role != "system" || body.Messages[1].Role != "user" {
		return "", errors.New("model, temperature, or message contract is invalid")
	}
	if body.ResponseFormat.Type != "json_schema" || body.ResponseFormat.JSONSchema.Name != "memory_candidate_extraction" ||
		!body.ResponseFormat.JSONSchema.Strict || body.ResponseFormat.JSONSchema.Schema["type"] != "object" ||
		body.ResponseFormat.JSONSchema.Schema["additionalProperties"] != false {
		return "", errors.New("strict response schema contract is invalid")
	}
	var input struct {
		Evidence []struct {
			ID         string       `json:"id"`
			SessionID  string       `json:"session_id"`
			Actor      domain.Actor `json:"actor"`
			Content    string       `json:"content"`
			OccurredAt time.Time    `json:"occurred_at"`
		} `json:"evidence"`
	}
	inputDecoder := json.NewDecoder(strings.NewReader(body.Messages[1].Content))
	inputDecoder.DisallowUnknownFields()
	if err := inputDecoder.Decode(&input); err != nil {
		return "", fmt.Errorf("decode evidence input: %w", err)
	}
	if len(input.Evidence) != 1 || input.Evidence[0].ID == "" || input.Evidence[0].SessionID == "" ||
		input.Evidence[0].Actor != domain.ActorUser || strings.TrimSpace(input.Evidence[0].Content) == "" || input.Evidence[0].OccurredAt.IsZero() {
		return "", errors.New("evidence input contract is invalid")
	}
	return input.Evidence[0].ID, nil
}

func assertProcessExtractionCandidateCount(t *testing.T, databaseURL, tenantID, userID string, want int) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	connection, err := pgx.Connect(ctx, databaseURL)
	if err != nil {
		t.Fatalf("connect to inspect extracted candidates: %v", err)
	}
	defer connection.Close(context.Background())
	var count int
	if err := connection.QueryRow(ctx, `
		SELECT count(*)
		FROM agent_memory.memory_candidates
		WHERE tenant_id=$1 AND user_id=$2`, tenantID, userID).Scan(&count); err != nil {
		t.Fatalf("count extracted candidates: %v", err)
	}
	if count != want {
		t.Fatalf("extracted candidate count=%d, want %d", count, want)
	}
}
