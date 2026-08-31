package extraction

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/kai443/go-agent-memory-system/internal/domain"
)

const (
	testModel            = "memory-extractor-model"
	testBearerToken      = "secret-bearer-token"
	testExtractorName    = "memory-extractor"
	testExtractorVersion = "2026-08-31"
)

func TestClientExtractSendsStrictSchemaAndReturnsStructuredProposals(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		if request.Method != http.MethodPost {
			t.Errorf("method = %s, want POST", request.Method)
		}
		if request.Header.Get("Content-Type") != "application/json" || request.Header.Get("Accept") != "application/json" {
			t.Errorf("unexpected content negotiation headers")
		}
		if request.Header.Get("Authorization") != "Bearer "+testBearerToken {
			t.Errorf("authorization header was not set")
		}

		var body chatCompletionRequest
		decoder := json.NewDecoder(request.Body)
		decoder.DisallowUnknownFields()
		if err := decoder.Decode(&body); err != nil {
			t.Errorf("decode request: %v", err)
		}
		if body.Model != testModel || body.Temperature != 0 {
			t.Errorf("model/temperature = %q/%v", body.Model, body.Temperature)
		}
		if len(body.Messages) != 2 || body.Messages[0].Role != "system" || body.Messages[1].Role != "user" {
			t.Fatalf("messages = %#v", body.Messages)
		}
		var evidenceRequest Request
		if err := json.Unmarshal([]byte(body.Messages[1].Content), &evidenceRequest); err != nil {
			t.Errorf("decode evidence message: %v", err)
		}
		if !slices.EqualFunc(evidenceRequest.Evidence, testRequest().Evidence, func(left, right Evidence) bool {
			return left.ID == right.ID && left.SessionID == right.SessionID && left.Actor == right.Actor && left.Content == right.Content && left.OccurredAt.Equal(right.OccurredAt)
		}) {
			t.Errorf("evidence message = %#v", evidenceRequest)
		}
		if body.ResponseFormat.Type != "json_schema" || !body.ResponseFormat.JSONSchema.Strict {
			t.Errorf("response format = %#v", body.ResponseFormat)
		}
		if body.ResponseFormat.JSONSchema.Name != "memory_candidate_extraction" {
			t.Errorf("schema name = %q", body.ResponseFormat.JSONSchema.Name)
		}
		candidateProperty := body.ResponseFormat.JSONSchema.Schema["properties"].(map[string]any)["candidates"].(map[string]any)
		if candidateProperty["maxItems"] != float64(MaxCandidates) && candidateProperty["maxItems"] != MaxCandidates {
			t.Errorf("candidate maxItems = %#v", candidateProperty["maxItems"])
		}

		writeCompletion(t, response, testModel, validResultJSON(), nil)
	}))
	defer server.Close()

	client := newTestClient(t, server, Config{BearerToken: testBearerToken})
	result, err := client.Extract(context.Background(), testRequest())
	if err != nil {
		t.Fatalf("extract: %v", err)
	}
	if len(result.Candidates) != 1 {
		t.Fatalf("candidates = %#v", result.Candidates)
	}
	want := Proposal{
		Kind:         domain.MemoryKindSemantic,
		Category:     "preference",
		Key:          "editor_theme",
		Value:        "dark",
		Person:       "",
		Relationship: "",
		Backstory:    "The user explicitly stated the preference.",
		Supports:     []Support{{EvidenceID: "evt-1", Quote: "I prefer a dark editor theme."}},
	}
	if !proposalsEqual(result.Candidates[0], want) {
		t.Fatalf("candidate = %#v, want %#v", result.Candidates[0], want)
	}

	descriptor := client.Descriptor()
	if descriptor != (Descriptor{Name: testExtractorName, Version: testExtractorVersion, Protocol: ProtocolOpenAICompatibleChatCompletionsJSONSchema}) {
		t.Fatalf("descriptor = %#v", descriptor)
	}
	assertNoSecrets(t, fmt.Errorf("%#v", descriptor), server.URL, testBearerToken, "I prefer")
}

func TestClientExtractAcceptsEmptyCandidateResult(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, _ *http.Request) {
		writeCompletion(t, response, testModel, `{"candidates":[]}`, nil)
	}))
	defer server.Close()

	result, err := newTestClient(t, server, Config{}).Extract(context.Background(), testRequest())
	if err != nil {
		t.Fatalf("extract empty result: %v", err)
	}
	if result.Candidates == nil || len(result.Candidates) != 0 {
		t.Fatalf("candidates = %#v, want non-nil empty slice", result.Candidates)
	}
}

func TestNewClientRejectsUnsafeOrIncompleteConfigurationWithoutLeaks(t *testing.T) {
	tests := []struct {
		name    string
		mutate  func(*Config)
		secrets []string
	}{
		{name: "missing endpoint", mutate: func(config *Config) { config.Endpoint = "" }},
		{name: "relative endpoint", mutate: func(config *Config) { config.Endpoint = "/v1/chat/completions" }},
		{name: "unsupported scheme", mutate: func(config *Config) { config.Endpoint = "file:///tmp/completions" }},
		{name: "endpoint userinfo", mutate: func(config *Config) {
			config.Endpoint = "http://secret-user:secret-password@localhost/v1/chat/completions"
		}, secrets: []string{"secret-user", "secret-password"}},
		{name: "endpoint query", mutate: func(config *Config) {
			config.Endpoint = "http://localhost/v1/chat/completions?key=secret-query"
		}, secrets: []string{"secret-query"}},
		{name: "endpoint empty query", mutate: func(config *Config) { config.Endpoint += "?" }},
		{name: "endpoint fragment", mutate: func(config *Config) {
			config.Endpoint = "http://localhost/v1/chat/completions#secret-fragment"
		}, secrets: []string{"secret-fragment"}},
		{name: "missing model", mutate: func(config *Config) { config.Model = " \t " }},
		{name: "missing extractor name", mutate: func(config *Config) { config.ExtractorName = "" }},
		{name: "missing extractor version", mutate: func(config *Config) { config.ExtractorVersion = "" }},
		{name: "negative timeout", mutate: func(config *Config) { config.Timeout = -time.Second }},
		{name: "negative response limit", mutate: func(config *Config) { config.MaxResponseBytes = -1 }},
		{name: "invalid bearer token", mutate: func(config *Config) {
			config.BearerToken = "secret-token\nInjected: yes"
		}, secrets: []string{"secret-token", "Injected"}},
		{name: "bearer token with surrounding whitespace", mutate: func(config *Config) {
			config.BearerToken = " secret-token "
		}, secrets: []string{"secret-token"}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			config := baseConfig("http://localhost/v1/chat/completions")
			test.mutate(&config)
			_, err := NewClient(config)
			if !errors.Is(err, ErrInvalidConfig) {
				t.Fatalf("error = %v, want ErrInvalidConfig", err)
			}
			assertNoSecrets(t, err, test.secrets...)
		})
	}
}

func TestClientRejectsInvalidRequestsBeforeSendingEvidence(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		calls.Add(1)
	}))
	defer server.Close()
	client := newTestClient(t, server, Config{})

	tooMany := make([]Evidence, MaxEvidenceItems+1)
	for index := range tooMany {
		tooMany[index] = validEvidence(fmt.Sprintf("evt-%d", index))
	}
	tests := []Request{
		{},
		{Evidence: tooMany},
		{Evidence: []Evidence{{ID: "secret-event"}}},
		{Evidence: []Evidence{{ID: "evt-1", SessionID: "session", Actor: "invalid", Content: "secret input", OccurredAt: time.Now()}}},
		{Evidence: []Evidence{validEvidence("evt-1"), validEvidence("evt-1")}},
	}
	for _, input := range tests {
		_, err := client.Extract(context.Background(), input)
		if !errors.Is(err, ErrInvalidRequest) {
			t.Fatalf("error = %v, want ErrInvalidRequest", err)
		}
		assertNoSecrets(t, err, "secret-event", "secret input")
	}
	if calls.Load() != 0 {
		t.Fatalf("requests sent = %d, want 0", calls.Load())
	}
}

func TestClientClassifiesHTTPStatusWithoutReadingOrLeakingBody(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, _ *http.Request) {
		response.WriteHeader(http.StatusTooManyRequests)
		_, _ = response.Write([]byte(`{"error":"secret-provider-body"}`))
	}))
	defer server.Close()

	_, err := newTestClient(t, server, Config{}).Extract(context.Background(), testRequest())
	var statusError *HTTPStatusError
	if !errors.As(err, &statusError) || statusError.StatusCode != http.StatusTooManyRequests || !errors.Is(err, ErrRequestFailed) {
		t.Fatalf("error = %#v, want typed HTTP 429", err)
	}
	assertNoSecrets(t, err, "secret-provider-body", "I prefer", server.URL, testBearerToken)
}

func TestClientClassifiesRefusalWithoutLeakingRefusalText(t *testing.T) {
	refusal := "secret refusal policy details"
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, _ *http.Request) {
		writeCompletion(t, response, testModel, "", &refusal)
	}))
	defer server.Close()

	_, err := newTestClient(t, server, Config{}).Extract(context.Background(), testRequest())
	var refusalError *RefusalError
	if !errors.As(err, &refusalError) || !errors.Is(err, ErrRefused) {
		t.Fatalf("error = %#v, want typed refusal", err)
	}
	assertNoSecrets(t, err, refusal, "I prefer", server.URL)
}

func TestClientClassifiesTimeoutAndCallerCancellation(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, _ *http.Request) {
		time.Sleep(100 * time.Millisecond)
		writeCompletion(t, response, testModel, validResultJSON(), nil)
	}))
	defer server.Close()

	t.Run("configured timeout", func(t *testing.T) {
		client := newTestClient(t, server, Config{Timeout: 10 * time.Millisecond})
		_, err := client.Extract(context.Background(), testRequest())
		var timeoutError *TimeoutError
		if !errors.As(err, &timeoutError) || !errors.Is(err, ErrTimeout) || !errors.Is(err, ErrRequestFailed) || !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("error = %#v, want typed deadline", err)
		}
		assertNoSecrets(t, err, "I prefer", server.URL)
	})

	t.Run("caller cancellation", func(t *testing.T) {
		client := newTestClient(t, server, Config{Timeout: time.Second})
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		_, err := client.Extract(ctx, testRequest())
		if !errors.Is(err, context.Canceled) || !errors.Is(err, ErrRequestFailed) {
			t.Fatalf("error = %#v, want caller cancellation", err)
		}
		var timeoutError *TimeoutError
		if errors.As(err, &timeoutError) {
			t.Fatalf("caller cancellation was classified as timeout: %v", err)
		}
		assertNoSecrets(t, err, "I prefer", server.URL)
	})

	t.Run("configured timeout while reading body", func(t *testing.T) {
		bodyStarted := make(chan struct{})
		bodyRelease := make(chan struct{})
		bodyServer := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, _ *http.Request) {
			response.Header().Set("Content-Type", "application/json")
			_, _ = response.Write([]byte(`{"model":"memory-extractor-model","choices":[`))
			if flusher, ok := response.(http.Flusher); ok {
				flusher.Flush()
			}
			close(bodyStarted)
			<-bodyRelease
		}))
		defer bodyServer.Close()
		defer close(bodyRelease)

		client := newTestClient(t, bodyServer, Config{Timeout: 10 * time.Millisecond})
		result := make(chan error, 1)
		go func() {
			_, err := client.Extract(context.Background(), testRequest())
			result <- err
		}()
		<-bodyStarted
		err := <-result
		var timeoutError *TimeoutError
		if !errors.As(err, &timeoutError) || !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("error = %#v, want typed body-read timeout", err)
		}
		assertNoSecrets(t, err, "I prefer", bodyServer.URL)
	})
}

func TestClientRejectsOversizedResponseAsTypedInvalidResponse(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, _ *http.Request) {
		_, _ = response.Write([]byte(strings.Repeat("secret-provider-response", 20)))
	}))
	defer server.Close()

	client := newTestClient(t, server, Config{MaxResponseBytes: 64})
	_, err := client.Extract(context.Background(), testRequest())
	var invalidError *InvalidResponseError
	if !errors.As(err, &invalidError) || !errors.Is(err, ErrInvalidResponse) || !errors.Is(err, ErrResponseTooLarge) {
		t.Fatalf("error = %#v, want typed oversized invalid response", err)
	}
	assertNoSecrets(t, err, "secret-provider-response", "I prefer", server.URL)
}

func TestClientRejectsNonUTF8ResponseBeforeJSONRepair(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, _ *http.Request) {
		body := append([]byte(`{"id":"`), byte(0xff))
		body = append(body, []byte(`","model":"memory-extractor-model","choices":[{"message":{"content":"{\"candidates\":[]}"}}]}`)...)
		_, _ = response.Write(body)
	}))
	defer server.Close()

	result, err := newTestClient(t, server, Config{}).Extract(context.Background(), testRequest())
	var invalidError *InvalidResponseError
	if !errors.As(err, &invalidError) || !errors.Is(err, ErrInvalidResponse) {
		t.Fatalf("error = %#v, want typed invalid UTF-8 response", err)
	}
	if len(result.Candidates) != 0 {
		t.Fatalf("invalid UTF-8 response returned candidates: %#v", result.Candidates)
	}
	assertNoSecrets(t, err, "I prefer", server.URL)
}

func TestClientStrictlyRejectsInvalidStructuredContentWithoutLeaks(t *testing.T) {
	validCandidate := validProposalMap()

	missingPerson := cloneProposalMap(validCandidate)
	delete(missingPerson, "person")
	invalidKind := cloneProposalMap(validCandidate)
	invalidKind["kind"] = "private"
	blankValue := cloneProposalMap(validCandidate)
	blankValue["value"] = " \t "
	emptySupports := cloneProposalMap(validCandidate)
	emptySupports["supports"] = []any{}
	missingQuote := cloneProposalMap(validCandidate)
	missingQuote["supports"] = []any{map[string]any{"evidence_id": "evt-1"}}
	unknownSupport := cloneProposalMap(validCandidate)
	unknownSupport["supports"] = []any{map[string]any{"evidence_id": "evt-1", "quote": "supported", "secret-provider-field": true}}
	tooManyCandidates := make([]any, MaxCandidates+1)
	for index := range tooManyCandidates {
		tooManyCandidates[index] = validCandidate
	}
	tooManySupports := make([]any, MaxSupportsPerCandidate+1)
	for index := range tooManySupports {
		tooManySupports[index] = map[string]any{"evidence_id": "evt-1", "quote": "supported"}
	}
	tooManySupportCandidate := cloneProposalMap(validCandidate)
	tooManySupportCandidate["supports"] = tooManySupports

	tests := []struct {
		name    string
		content string
	}{
		{name: "empty", content: ""},
		{name: "markdown fenced", content: "```json\n{\"candidates\":[]}\n```"},
		{name: "trailing JSON", content: `{"candidates":[]} {}`},
		{name: "duplicate key", content: `{"candidates":[],"candidates":[]}`},
		{name: "unknown top level", content: `{"candidates":[],"secret-provider-field":true}`},
		{name: "missing candidates", content: `{}`},
		{name: "null candidates", content: `{"candidates":null}`},
		{name: "too many candidates", content: marshalTestJSON(t, map[string]any{"candidates": tooManyCandidates})},
		{name: "missing candidate field", content: marshalTestJSON(t, map[string]any{"candidates": []any{missingPerson}})},
		{name: "unknown candidate field", content: marshalTestJSON(t, map[string]any{"candidates": []any{mergeProposalField(validCandidate, "secret-provider-field", true)}})},
		{name: "invalid kind", content: marshalTestJSON(t, map[string]any{"candidates": []any{invalidKind}})},
		{name: "blank required text", content: marshalTestJSON(t, map[string]any{"candidates": []any{blankValue}})},
		{name: "empty supports", content: marshalTestJSON(t, map[string]any{"candidates": []any{emptySupports}})},
		{name: "too many supports", content: marshalTestJSON(t, map[string]any{"candidates": []any{tooManySupportCandidate}})},
		{name: "missing support field", content: marshalTestJSON(t, map[string]any{"candidates": []any{missingQuote}})},
		{name: "unknown support field", content: marshalTestJSON(t, map[string]any{"candidates": []any{unknownSupport}})},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, _ *http.Request) {
				writeCompletion(t, response, testModel, test.content, nil)
			}))
			defer server.Close()

			_, err := newTestClient(t, server, Config{}).Extract(context.Background(), testRequest())
			var invalidError *InvalidResponseError
			if !errors.As(err, &invalidError) || !errors.Is(err, ErrInvalidResponse) {
				t.Fatalf("error = %#v, want typed invalid response", err)
			}
			assertNoSecrets(t, err, "secret-provider-field", "I prefer", server.URL)
		})
	}
}

func TestClientRejectsInvalidEnvelopeAndModelMismatch(t *testing.T) {
	tests := []struct {
		name         string
		body         string
		wantMismatch bool
	}{
		{name: "invalid envelope JSON", body: `secret-provider-body`},
		{name: "duplicate envelope key", body: `{"model":"memory-extractor-model","model":"secret-provider-model","choices":[]}`},
		{name: "missing choices", body: `{"model":"memory-extractor-model"}`},
		{name: "multiple choices", body: `{"model":"memory-extractor-model","choices":[{"message":{"content":"{}"}},{"message":{"content":"{}"}}]}`},
		{name: "missing content", body: `{"model":"memory-extractor-model","choices":[{"message":{}}]}`},
		{name: "model mismatch", body: `{"model":"secret-provider-model","choices":[{"message":{"content":"{\"candidates\":[]}"}}]}`, wantMismatch: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, _ *http.Request) {
				_, _ = response.Write([]byte(test.body))
			}))
			defer server.Close()

			_, err := newTestClient(t, server, Config{}).Extract(context.Background(), testRequest())
			var invalidError *InvalidResponseError
			if !errors.As(err, &invalidError) || !errors.Is(err, ErrInvalidResponse) {
				t.Fatalf("error = %#v, want typed invalid response", err)
			}
			if test.wantMismatch != errors.Is(err, ErrModelMismatch) {
				t.Fatalf("model mismatch classification = %v, want %v", errors.Is(err, ErrModelMismatch), test.wantMismatch)
			}
			assertNoSecrets(t, err, "secret-provider-body", "secret-provider-model", "I prefer", server.URL)
		})
	}
}

func TestClientDoesNotLeakTransportDetailsOrFollowRedirects(t *testing.T) {
	t.Run("transport", func(t *testing.T) {
		httpClient := &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
			return nil, errors.New("dial secret-transport-host with secret-provider-body")
		})}
		config := baseConfig("http://secret-endpoint.invalid/secret-path")
		config.BearerToken = testBearerToken
		config.HTTPClient = httpClient
		client, err := NewClient(config)
		if err != nil {
			t.Fatalf("new client: %v", err)
		}
		_, err = client.Extract(context.Background(), testRequest())
		if !errors.Is(err, ErrRequestFailed) {
			t.Fatalf("error = %v, want ErrRequestFailed", err)
		}
		assertNoSecrets(t, err, "secret-transport-host", "secret-provider-body", "secret-endpoint", "secret-path", testBearerToken, "I prefer")
	})

	t.Run("redirect", func(t *testing.T) {
		var redirectedCalls atomic.Int32
		redirected := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
			redirectedCalls.Add(1)
		}))
		defer redirected.Close()
		redirector := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
			http.Redirect(response, request, redirected.URL+"/secret-target", http.StatusTemporaryRedirect)
		}))
		defer redirector.Close()

		callerClient := redirector.Client()
		callerClient.CheckRedirect = func(*http.Request, []*http.Request) error { return nil }
		client := newTestClient(t, redirector, Config{HTTPClient: callerClient})
		_, err := client.Extract(context.Background(), testRequest())
		var statusError *HTTPStatusError
		if !errors.As(err, &statusError) || statusError.StatusCode != http.StatusTemporaryRedirect {
			t.Fatalf("error = %#v, want typed 307", err)
		}
		if redirectedCalls.Load() != 0 {
			t.Fatalf("redirected calls = %d, want 0", redirectedCalls.Load())
		}
		if callerClient.CheckRedirect == nil || callerClient.CheckRedirect(nil, nil) != nil {
			t.Fatal("caller's redirect policy was mutated")
		}
		assertNoSecrets(t, err, "secret-target", redirected.URL, "I prefer")
	})
}

func newTestClient(t *testing.T, server *httptest.Server, overrides Config) *Client {
	t.Helper()
	config := baseConfig(server.URL + "/v1/chat/completions")
	config.HTTPClient = server.Client()
	if overrides.BearerToken != "" {
		config.BearerToken = overrides.BearerToken
	}
	if overrides.Timeout != 0 {
		config.Timeout = overrides.Timeout
	}
	if overrides.MaxResponseBytes != 0 {
		config.MaxResponseBytes = overrides.MaxResponseBytes
	}
	if overrides.HTTPClient != nil {
		config.HTTPClient = overrides.HTTPClient
	}
	client, err := NewClient(config)
	if err != nil {
		t.Fatalf("new client: %v", err)
	}
	return client
}

func baseConfig(endpoint string) Config {
	return Config{
		Endpoint:         endpoint,
		Model:            testModel,
		Timeout:          time.Second,
		ExtractorName:    testExtractorName,
		ExtractorVersion: testExtractorVersion,
	}
}

func testRequest() Request {
	return Request{Evidence: []Evidence{{
		ID:         "evt-1",
		SessionID:  "session-1",
		Actor:      domain.ActorUser,
		Content:    "I prefer a dark editor theme.",
		OccurredAt: time.Date(2026, 8, 31, 8, 0, 0, 0, time.UTC),
	}}}
}

func validEvidence(id string) Evidence {
	evidence := testRequest().Evidence[0]
	evidence.ID = id
	return evidence
}

func validResultJSON() string {
	contents, err := json.Marshal(Result{Candidates: []Proposal{{
		Kind:         domain.MemoryKindSemantic,
		Category:     "preference",
		Key:          "editor_theme",
		Value:        "dark",
		Person:       "",
		Relationship: "",
		Backstory:    "The user explicitly stated the preference.",
		Supports:     []Support{{EvidenceID: "evt-1", Quote: "I prefer a dark editor theme."}},
	}}})
	if err != nil {
		panic(err)
	}
	return string(contents)
}

func validProposalMap() map[string]any {
	return map[string]any{
		"kind":         "semantic",
		"category":     "preference",
		"key":          "editor_theme",
		"value":        "dark",
		"person":       "",
		"relationship": "",
		"backstory":    "supported",
		"supports":     []any{map[string]any{"evidence_id": "evt-1", "quote": "supported"}},
	}
}

func cloneProposalMap(source map[string]any) map[string]any {
	clone := make(map[string]any, len(source))
	for key, value := range source {
		clone[key] = value
	}
	return clone
}

func mergeProposalField(source map[string]any, key string, value any) map[string]any {
	clone := cloneProposalMap(source)
	clone[key] = value
	return clone
}

func marshalTestJSON(t *testing.T, value any) string {
	t.Helper()
	contents, err := json.Marshal(value)
	if err != nil {
		t.Fatalf("marshal test JSON: %v", err)
	}
	return string(contents)
}

func writeCompletion(t *testing.T, response http.ResponseWriter, model, content string, refusal *string) {
	t.Helper()
	response.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(response).Encode(map[string]any{
		"id":    "completion-secret-id",
		"model": model,
		"choices": []any{map[string]any{
			"index": 0,
			"message": map[string]any{
				"role":    "assistant",
				"content": content,
				"refusal": refusal,
			},
		}},
	}); err != nil {
		t.Errorf("write completion: %v", err)
	}
}

func proposalsEqual(left, right Proposal) bool {
	return left.Kind == right.Kind && left.Category == right.Category && left.Key == right.Key && left.Value == right.Value &&
		left.Person == right.Person && left.Relationship == right.Relationship && left.Backstory == right.Backstory &&
		slices.Equal(left.Supports, right.Supports)
}

func assertNoSecrets(t *testing.T, err error, secrets ...string) {
	t.Helper()
	if err == nil {
		return
	}
	for _, secret := range secrets {
		if secret != "" && strings.Contains(err.Error(), secret) {
			t.Fatalf("error leaked %q: %v", secret, err)
		}
	}
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (function roundTripFunc) RoundTrip(request *http.Request) (*http.Response, error) {
	return function(request)
}
