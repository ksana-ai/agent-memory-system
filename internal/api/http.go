package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/ksana-ai/agent-memory-system/internal/app"
	"github.com/ksana-ai/agent-memory-system/internal/domain"
)

const maxRequestBytes = 64 << 10

type Handler struct {
	service   *app.Service
	phase     string
	storage   string
	readiness ReadinessChecker
}

// ReadinessChecker is the smallest database capability required by /readyz.
// Keeping it separate from the application's Store interface avoids coupling
// domain operations to process lifecycle concerns.
type ReadinessChecker interface {
	Ping(context.Context) error
}

type HandlerOption func(*Handler) error

func WithPhase(phase string) HandlerOption {
	return func(handler *Handler) error {
		phase = strings.TrimSpace(phase)
		if phase == "" {
			return errors.New("health phase is required")
		}
		handler.phase = phase
		return nil
	}
}

func WithStorage(storage string) HandlerOption {
	return func(handler *Handler) error {
		storage = strings.TrimSpace(storage)
		if storage == "" {
			return errors.New("health storage is required")
		}
		handler.storage = storage
		return nil
	}
}

func WithReadiness(readiness ReadinessChecker) HandlerOption {
	return func(handler *Handler) error {
		if readiness == nil {
			return errors.New("readiness checker is required")
		}
		handler.readiness = readiness
		return nil
	}
}

func NewHandler(service *app.Service, options ...HandlerOption) (*Handler, error) {
	if service == nil {
		return nil, errors.New("service is required")
	}
	handler := &Handler{
		service: service,
		phase:   "walking-skeleton",
		storage: "test-double",
	}
	for _, option := range options {
		if option == nil {
			return nil, errors.New("handler option is required")
		}
		if err := option(handler); err != nil {
			return nil, fmt.Errorf("configure HTTP handler: %w", err)
		}
	}
	return handler, nil
}

func (handler *Handler) Routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", handler.health)
	mux.HandleFunc("GET /readyz", handler.ready)
	mux.HandleFunc("POST /v1/evidence", handler.ingestEvidence)
	mux.HandleFunc("POST /v1/memory-candidate-extractions", handler.extractCandidates)
	mux.HandleFunc("POST /v1/memory-candidates", handler.proposeCandidate)
	mux.HandleFunc("POST /v1/memory-candidates/{candidate_id}/reviews", handler.reviewCandidate)
	mux.HandleFunc("POST /v1/context-packs", handler.buildContext)
	mux.HandleFunc("DELETE /v1/users/{user_id}", handler.forgetUser)
	return mux
}

type extractCandidatesRequest struct {
	SourceEventIDs []string `json:"source_event_ids"`
}

type extractorResponse struct {
	Name    string `json:"name"`
	Version string `json:"version"`
}

type extractCandidatesResponse struct {
	ExtractionID   string                   `json:"extraction_id"`
	SourceEventIDs []string                 `json:"source_event_ids"`
	Extractor      extractorResponse        `json:"extractor"`
	Candidates     []domain.MemoryCandidate `json:"candidates"`
}

func (handler *Handler) extractCandidates(writer http.ResponseWriter, request *http.Request) {
	tenantID, userID, ok := readScope(writer, request)
	if !ok {
		return
	}
	var payload extractCandidatesRequest
	if err := decodeJSON(writer, request, &payload); err != nil {
		writeError(writer, err)
		return
	}
	result, err := handler.service.ExtractCandidates(request.Context(), app.ExtractCandidatesInput{
		TenantID:       tenantID,
		UserID:         userID,
		SourceEventIDs: payload.SourceEventIDs,
	})
	if err != nil {
		writeError(writer, err)
		return
	}
	writeJSON(writer, http.StatusOK, extractCandidatesResponse{
		ExtractionID:   result.ExtractionID,
		SourceEventIDs: result.SourceEventIDs,
		Extractor: extractorResponse{
			Name:    result.ExtractorName,
			Version: result.ExtractorVersion,
		},
		Candidates: result.Candidates,
	})
}

func (handler *Handler) health(writer http.ResponseWriter, _ *http.Request) {
	writeJSON(writer, http.StatusOK, map[string]string{
		"status":  "ok",
		"phase":   handler.phase,
		"storage": handler.storage,
	})
}

func (handler *Handler) ready(writer http.ResponseWriter, request *http.Request) {
	if handler.readiness != nil {
		ctx, cancel := context.WithTimeout(request.Context(), 2*time.Second)
		defer cancel()
		if err := handler.readiness.Ping(ctx); err != nil {
			writeJSON(writer, http.StatusServiceUnavailable, map[string]string{
				"status":  "not_ready",
				"storage": handler.storage,
			})
			return
		}
	}
	writeJSON(writer, http.StatusOK, map[string]string{
		"status":  "ready",
		"storage": handler.storage,
	})
}

type ingestEvidenceRequest struct {
	EventID    string            `json:"event_id"`
	SessionID  string            `json:"session_id"`
	Actor      domain.Actor      `json:"actor"`
	Content    string            `json:"content"`
	Metadata   map[string]string `json:"metadata"`
	OccurredAt *time.Time        `json:"occurred_at"`
}

func (handler *Handler) ingestEvidence(writer http.ResponseWriter, request *http.Request) {
	tenantID, userID, ok := readScope(writer, request)
	if !ok {
		return
	}
	var payload ingestEvidenceRequest
	if err := decodeJSON(writer, request, &payload); err != nil {
		writeError(writer, err)
		return
	}
	var occurredAt time.Time
	if payload.OccurredAt != nil {
		occurredAt = *payload.OccurredAt
	}
	event, err := handler.service.IngestEvidence(request.Context(), app.IngestEvidenceInput{
		EventID:    payload.EventID,
		TenantID:   tenantID,
		UserID:     userID,
		SessionID:  payload.SessionID,
		Actor:      payload.Actor,
		Content:    payload.Content,
		Metadata:   payload.Metadata,
		OccurredAt: occurredAt,
	})
	if err != nil {
		writeError(writer, err)
		return
	}
	writeJSON(writer, http.StatusCreated, event)
}

type proposeCandidateRequest struct {
	Kind             domain.MemoryKind `json:"kind"`
	Category         string            `json:"category"`
	Key              string            `json:"key"`
	Value            string            `json:"value"`
	Person           string            `json:"person"`
	Relationship     string            `json:"relationship"`
	Backstory        string            `json:"backstory"`
	SourceEventIDs   []string          `json:"source_event_ids"`
	Extractor        string            `json:"extractor"`
	ExtractorVersion string            `json:"extractor_version"`
	Metadata         map[string]string `json:"metadata"`
	ExpiresAt        *time.Time        `json:"expires_at"`
}

func (handler *Handler) proposeCandidate(writer http.ResponseWriter, request *http.Request) {
	tenantID, userID, ok := readScope(writer, request)
	if !ok {
		return
	}
	var payload proposeCandidateRequest
	if err := decodeJSON(writer, request, &payload); err != nil {
		writeError(writer, err)
		return
	}
	candidate, err := handler.service.ProposeCandidate(request.Context(), app.ProposeCandidateInput{
		TenantID:         tenantID,
		UserID:           userID,
		Kind:             payload.Kind,
		Category:         payload.Category,
		Key:              payload.Key,
		Value:            payload.Value,
		Person:           payload.Person,
		Relationship:     payload.Relationship,
		Backstory:        payload.Backstory,
		SourceEventIDs:   payload.SourceEventIDs,
		Extractor:        payload.Extractor,
		ExtractorVersion: payload.ExtractorVersion,
		Metadata:         payload.Metadata,
		ExpiresAt:        payload.ExpiresAt,
	})
	if err != nil {
		writeError(writer, err)
		return
	}
	writeJSON(writer, http.StatusCreated, candidate)
}

type reviewCandidateRequest struct {
	Decision   domain.ReviewDecision `json:"decision"`
	ReviewerID string                `json:"reviewer_id"`
	Reason     string                `json:"reason"`
}

type reviewCandidateResponse struct {
	Candidate domain.MemoryCandidate `json:"candidate"`
	Memory    *domain.MemoryCard     `json:"memory,omitempty"`
}

func (handler *Handler) reviewCandidate(writer http.ResponseWriter, request *http.Request) {
	tenantID, userID, ok := readScope(writer, request)
	if !ok {
		return
	}
	var payload reviewCandidateRequest
	if err := decodeJSON(writer, request, &payload); err != nil {
		writeError(writer, err)
		return
	}
	candidate, memory, err := handler.service.ReviewCandidate(request.Context(), app.ReviewCandidateInput{
		TenantID:    tenantID,
		UserID:      userID,
		CandidateID: request.PathValue("candidate_id"),
		Decision:    payload.Decision,
		ReviewerID:  payload.ReviewerID,
		Reason:      payload.Reason,
	})
	if err != nil {
		writeError(writer, err)
		return
	}
	writeJSON(writer, http.StatusOK, reviewCandidateResponse{Candidate: candidate, Memory: memory})
}

type buildContextRequest struct {
	Query string `json:"query"`
	Limit *int   `json:"limit"`
}

func (handler *Handler) buildContext(writer http.ResponseWriter, request *http.Request) {
	tenantID, userID, ok := readScope(writer, request)
	if !ok {
		return
	}
	var payload buildContextRequest
	if err := decodeJSON(writer, request, &payload); err != nil {
		writeError(writer, err)
		return
	}
	limit := 0
	if payload.Limit != nil {
		if *payload.Limit < 1 || *payload.Limit > 20 {
			writeError(writer, fmt.Errorf("limit must be between 1 and 20: %w", domain.ErrInvalid))
			return
		}
		limit = *payload.Limit
	}
	contextPack, err := handler.service.BuildContext(request.Context(), app.BuildContextInput{
		TenantID: tenantID,
		UserID:   userID,
		Query:    payload.Query,
		Limit:    limit,
	})
	if err != nil {
		writeError(writer, err)
		return
	}
	writeJSON(writer, http.StatusOK, contextPack)
}

func (handler *Handler) forgetUser(writer http.ResponseWriter, request *http.Request) {
	tenantID := strings.TrimSpace(request.Header.Get("X-Tenant-ID"))
	if tenantID == "" {
		writeError(writer, fmt.Errorf("X-Tenant-ID header is required: %w", domain.ErrInvalid))
		return
	}
	receipt, err := handler.service.ForgetUser(request.Context(), tenantID, request.PathValue("user_id"))
	if err != nil {
		writeError(writer, err)
		return
	}
	writeJSON(writer, http.StatusOK, receipt)
}

func readScope(writer http.ResponseWriter, request *http.Request) (string, string, bool) {
	tenantID := strings.TrimSpace(request.Header.Get("X-Tenant-ID"))
	userID := strings.TrimSpace(request.Header.Get("X-User-ID"))
	if tenantID == "" || userID == "" {
		writeError(writer, fmt.Errorf("X-Tenant-ID and X-User-ID headers are required: %w", domain.ErrInvalid))
		return "", "", false
	}
	return tenantID, userID, true
}

func decodeJSON(writer http.ResponseWriter, request *http.Request, destination any) error {
	request.Body = http.MaxBytesReader(writer, request.Body, maxRequestBytes)
	decoder := json.NewDecoder(request.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(destination); err != nil {
		return fmt.Errorf("decode request body: %w: %w", err, domain.ErrInvalid)
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return fmt.Errorf("request body must contain one JSON object: %w", domain.ErrInvalid)
	}
	return nil
}

type errorResponse struct {
	Error struct {
		Code    string `json:"code"`
		Message string `json:"message"`
	} `json:"error"`
}

func writeError(writer http.ResponseWriter, err error) {
	status := http.StatusInternalServerError
	code := "internal_error"
	message := "internal server error"
	switch {
	case errors.Is(err, domain.ErrInvalid):
		status, code, message = http.StatusBadRequest, "invalid_request", err.Error()
	case errors.Is(err, domain.ErrNotFound):
		status, code, message = http.StatusNotFound, "not_found", "resource not found"
	case errors.Is(err, domain.ErrConflict):
		status, code, message = http.StatusConflict, "conflict", err.Error()
	case errors.Is(err, domain.ErrExtractionDisabled):
		status, code, message = http.StatusServiceUnavailable, "extraction_disabled", "candidate extraction is disabled"
	case errors.Is(err, domain.ErrExtractionUnavailable):
		status, code, message = http.StatusServiceUnavailable, "extraction_unavailable", "candidate extraction is temporarily unavailable"
	case errors.Is(err, domain.ErrExtractionRejected):
		status, code, message = http.StatusBadGateway, "extraction_rejected", "candidate extractor rejected the request"
	case errors.Is(err, domain.ErrExtractionInvalidResponse):
		status, code, message = http.StatusBadGateway, "invalid_extractor_output", "candidate extractor returned invalid output"
	case errors.Is(err, domain.ErrUnavailable):
		status, code, message = http.StatusServiceUnavailable, "retrieval_unavailable", "retrieval is temporarily unavailable"
	case errors.Is(err, context.DeadlineExceeded):
		status, code, message = http.StatusGatewayTimeout, "request_timeout", "request timed out"
	case errors.Is(err, context.Canceled):
		status, code, message = http.StatusRequestTimeout, "request_canceled", "request canceled"
	}
	response := errorResponse{}
	response.Error.Code = code
	response.Error.Message = message
	writeJSON(writer, status, response)
}

func writeJSON(writer http.ResponseWriter, status int, value any) {
	writer.Header().Set("Content-Type", "application/json; charset=utf-8")
	writer.WriteHeader(status)
	_ = json.NewEncoder(writer).Encode(value)
}
