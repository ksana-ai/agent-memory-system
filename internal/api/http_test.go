package api_test

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/kai443/go-agent-memory-system/internal/api"
	"github.com/kai443/go-agent-memory-system/internal/app"
	"github.com/kai443/go-agent-memory-system/internal/domain"
	"github.com/kai443/go-agent-memory-system/internal/retrieval"
	"github.com/kai443/go-agent-memory-system/internal/store/memstore"
)

func TestHTTPLifecycle(t *testing.T) {
	routes := newRoutes(t)

	evidenceResponse := perform(t, routes, http.MethodPost, "/v1/evidence", `{
		"event_id":"evt_http_1",
		"session_id":"session-http",
		"actor":"user",
		"content":"I prefer window seats on flights."
	}`, true)
	if evidenceResponse.Code != http.StatusCreated {
		t.Fatalf("ingest status=%d body=%s", evidenceResponse.Code, evidenceResponse.Body.String())
	}

	candidateResponse := perform(t, routes, http.MethodPost, "/v1/memory-candidates", `{
		"kind":"semantic",
		"category":"travel",
		"key":"seat_preference",
		"value":"window seat",
		"person":"self",
		"relationship":"self",
		"backstory":"Direct statement from the user.",
		"source_event_ids":["evt_http_1"],
		"extractor":"manual-test",
		"extractor_version":"v1"
	}`, true)
	if candidateResponse.Code != http.StatusCreated {
		t.Fatalf("candidate status=%d body=%s", candidateResponse.Code, candidateResponse.Body.String())
	}
	var candidate domain.MemoryCandidate
	decodeResponse(t, candidateResponse, &candidate)

	reviewResponse := perform(t, routes, http.MethodPost, "/v1/memory-candidates/"+candidate.ID+"/reviews", `{
		"decision":"approve",
		"reviewer_id":"reviewer-http",
		"reason":"The evidence directly supports this fact."
	}`, true)
	if reviewResponse.Code != http.StatusOK {
		t.Fatalf("review status=%d body=%s", reviewResponse.Code, reviewResponse.Body.String())
	}

	contextResponse := perform(t, routes, http.MethodPost, "/v1/context-packs", `{
		"query":"window seat preference",
		"limit":5
	}`, true)
	if contextResponse.Code != http.StatusOK {
		t.Fatalf("context status=%d body=%s", contextResponse.Code, contextResponse.Body.String())
	}
	var contextPack domain.ContextPack
	decodeResponse(t, contextResponse, &contextPack)
	if len(contextPack.Items) != 1 || contextPack.Items[0].Memory.Key != "seat_preference" || len(contextPack.Items[0].Sources) != 1 {
		t.Fatalf("unexpected context pack: %#v", contextPack)
	}

	deleteResponse := perform(t, routes, http.MethodDelete, "/v1/users/user-http", "", false)
	if deleteResponse.Code != http.StatusOK {
		t.Fatalf("delete status=%d body=%s", deleteResponse.Code, deleteResponse.Body.String())
	}
	var receipt domain.DeletionReceipt
	decodeResponse(t, deleteResponse, &receipt)
	if receipt.EvidenceDeleted != 1 || receipt.MemoriesDeleted != 1 {
		t.Fatalf("unexpected deletion receipt: %#v", receipt)
	}

	afterDelete := perform(t, routes, http.MethodPost, "/v1/context-packs", `{"query":"window seat"}`, true)
	var emptyPack domain.ContextPack
	decodeResponse(t, afterDelete, &emptyPack)
	if len(emptyPack.Items) != 0 {
		t.Fatalf("deleted memory returned from API: %#v", emptyPack.Items)
	}
}

func TestHTTPRejectsUnknownJSONFields(t *testing.T) {
	routes := newRoutes(t)
	response := perform(t, routes, http.MethodPost, "/v1/evidence", `{
		"session_id":"session-http",
		"actor":"user",
		"content":"hello",
		"unexpected":true
	}`, true)
	if response.Code != http.StatusBadRequest {
		t.Fatalf("status=%d body=%s", response.Code, response.Body.String())
	}
}

func TestHTTPRejectsControlCharactersInMemoryIdentity(t *testing.T) {
	routes := newRoutes(t)
	response := perform(t, routes, http.MethodPost, "/v1/memory-candidates", `{
		"kind":"semantic",
		"category":"travel\u0000private",
		"key":"seat_preference",
		"value":"window seat",
		"source_event_ids":["missing"],
		"extractor":"manual-test",
		"extractor_version":"v1"
	}`, true)
	if response.Code != http.StatusBadRequest {
		t.Fatalf("status=%d body=%s", response.Code, response.Body.String())
	}
}

func TestHTTPRejectsExplicitZeroContextLimit(t *testing.T) {
	routes := newRoutes(t)
	response := perform(t, routes, http.MethodPost, "/v1/context-packs", `{"query":"seat","limit":0}`, true)
	if response.Code != http.StatusBadRequest {
		t.Fatalf("status=%d body=%s", response.Code, response.Body.String())
	}
}

func newRoutes(t *testing.T) http.Handler {
	t.Helper()
	storage := memstore.New()
	retriever, err := retrieval.NewBM25(storage)
	if err != nil {
		t.Fatalf("new retriever: %v", err)
	}
	service, err := app.New(storage, retriever)
	if err != nil {
		t.Fatalf("new service: %v", err)
	}
	handler, err := api.NewHandler(service)
	if err != nil {
		t.Fatalf("new handler: %v", err)
	}
	return handler.Routes()
}

func perform(t *testing.T, handler http.Handler, method, path, body string, includeUser bool) *httptest.ResponseRecorder {
	t.Helper()
	request := httptest.NewRequest(method, path, bytes.NewBufferString(body))
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("X-Tenant-ID", "tenant-http")
	if includeUser {
		request.Header.Set("X-User-ID", "user-http")
	}
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	return response
}

func decodeResponse(t *testing.T, response *httptest.ResponseRecorder, destination any) {
	t.Helper()
	if err := json.Unmarshal(response.Body.Bytes(), destination); err != nil {
		t.Fatalf("decode response %q: %v", response.Body.String(), err)
	}
}
