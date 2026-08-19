package embedding

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
)

const testModel = "text-embedding-bge-m3"

func TestClientEmbedBatchValidatesRequestAndRestoresIndexOrder(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		if request.Method != http.MethodPost {
			t.Errorf("method = %s, want POST", request.Method)
		}
		if request.Header.Get("Content-Type") != "application/json" {
			t.Errorf("content type = %q", request.Header.Get("Content-Type"))
		}
		if request.Header.Get("Accept") != "application/json" {
			t.Errorf("accept = %q", request.Header.Get("Accept"))
		}

		var body embeddingsRequest
		if err := json.NewDecoder(request.Body).Decode(&body); err != nil {
			t.Errorf("decode request: %v", err)
		}
		if body.Model != testModel {
			t.Errorf("model = %q", body.Model)
		}
		if !slices.Equal(body.Input, []string{"first secret text", "第二条记忆"}) {
			t.Errorf("input = %#v", body.Input)
		}

		response.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(response).Encode(map[string]any{
			"model": testModel,
			"data": []any{
				map[string]any{"index": 1, "embedding": testVector(DefaultDimension, 0.2)},
				map[string]any{"index": 0, "embedding": testVector(DefaultDimension, 0.1)},
			},
		})
	}))
	defer server.Close()

	client := newTestClient(t, server, Config{})
	vectors, err := client.Embed(context.Background(), []string{"first secret text", "第二条记忆"})
	if err != nil {
		t.Fatalf("embed: %v", err)
	}
	if len(vectors) != 2 || len(vectors[0]) != DefaultDimension || len(vectors[1]) != DefaultDimension {
		t.Fatalf("vector dimensions = %d/%d", len(vectors[0]), len(vectors[1]))
	}
	if vectors[0][0] != float32(0.1) || vectors[1][0] != float32(0.2) {
		t.Fatalf("vectors were not restored by index: first=%v second=%v", vectors[0][0], vectors[1][0])
	}

	descriptor := client.Descriptor()
	wantDescriptor := Descriptor{
		Provider:        ProviderLMStudio,
		API:             APIEmbeddingsV1,
		Model:           testModel,
		Dimension:       DefaultDimension,
		DocumentVersion: MemoryCardDocumentVersion,
	}
	if descriptor != wantDescriptor {
		t.Fatalf("descriptor = %#v, want %#v", descriptor, wantDescriptor)
	}
	if strings.Contains(fmt.Sprintf("%#v", descriptor), server.URL) {
		t.Fatal("descriptor leaked endpoint")
	}
}

func TestNewClientRejectsUnsafeOrInvalidEndpointsWithoutLeakingThem(t *testing.T) {
	tests := []struct {
		name     string
		endpoint string
		secrets  []string
	}{
		{name: "missing", endpoint: ""},
		{name: "relative", endpoint: "/v1/embeddings"},
		{name: "unsupported scheme", endpoint: "file:///tmp/embeddings"},
		{name: "userinfo", endpoint: "http://api-user:super-secret@localhost/v1/embeddings", secrets: []string{"api-user", "super-secret"}},
		{name: "query", endpoint: "http://localhost/v1/embeddings?api_key=super-secret", secrets: []string{"api_key", "super-secret"}},
		{name: "empty query", endpoint: "http://localhost/v1/embeddings?"},
		{name: "fragment", endpoint: "http://localhost/v1/embeddings#super-secret", secrets: []string{"super-secret"}},
		{name: "invalid escape", endpoint: "http://localhost/%super-secret", secrets: []string{"super-secret"}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := NewClient(Config{Endpoint: test.endpoint, Model: testModel})
			if !errors.Is(err, ErrInvalidConfig) {
				t.Fatalf("error = %v, want ErrInvalidConfig", err)
			}
			for _, secret := range test.secrets {
				if strings.Contains(err.Error(), secret) {
					t.Fatalf("error leaked %q: %v", secret, err)
				}
			}
		})
	}
}

func TestNewClientValidatesNonURLConfiguration(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*Config)
	}{
		{name: "blank model", mutate: func(config *Config) { config.Model = " \n\t " }},
		{name: "negative dimension", mutate: func(config *Config) { config.ExpectedDimension = -1 }},
		{name: "negative timeout", mutate: func(config *Config) { config.Timeout = -time.Second }},
		{name: "negative response limit", mutate: func(config *Config) { config.MaxResponseBytes = -1 }},
		{name: "negative batch limit", mutate: func(config *Config) { config.MaxBatchSize = -1 }},
		{name: "negative input limit", mutate: func(config *Config) { config.MaxInputBytes = -1 }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			config := Config{Endpoint: "http://localhost/v1/embeddings", Model: testModel}
			test.mutate(&config)
			_, err := NewClient(config)
			if !errors.Is(err, ErrInvalidConfig) {
				t.Fatalf("error = %v, want ErrInvalidConfig", err)
			}
		})
	}
}

func TestClientRejectsEmptyInputsWithoutSendingRequestOrLeakingInput(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		calls.Add(1)
	}))
	defer server.Close()
	client := newTestClient(t, server, Config{})

	for _, inputs := range [][]string{nil, {}, {"valid", " \t\n"}} {
		_, err := client.Embed(context.Background(), inputs)
		if !errors.Is(err, ErrInvalidInput) {
			t.Fatalf("inputs %#v: error = %v", inputs, err)
		}
	}
	if calls.Load() != 0 {
		t.Fatalf("server calls = %d, want 0", calls.Load())
	}
}

func TestClientRejectsBatchAndInputByteLimitsBeforeSending(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		calls.Add(1)
	}))
	defer server.Close()

	t.Run("batch", func(t *testing.T) {
		client := newTestClient(t, server, Config{MaxBatchSize: 1})
		_, err := client.Embed(context.Background(), []string{"one", "two-sensitive"})
		if !errors.Is(err, ErrInvalidInput) {
			t.Fatalf("error = %v, want ErrInvalidInput", err)
		}
		assertNoSecrets(t, err, "two-sensitive")
	})
	t.Run("UTF-8 bytes", func(t *testing.T) {
		client := newTestClient(t, server, Config{MaxInputBytes: 5})
		_, err := client.Embed(context.Background(), []string{"中文"})
		if !errors.Is(err, ErrInvalidInput) {
			t.Fatalf("error = %v, want ErrInvalidInput", err)
		}
		assertNoSecrets(t, err, "中文")
	})
	if calls.Load() != 0 {
		t.Fatalf("server calls = %d, want 0", calls.Load())
	}
}

func TestClientRejectsResponseContractViolations(t *testing.T) {
	tests := []struct {
		name   string
		inputs []string
		body   func() any
	}{
		{
			name: "model mismatch", inputs: []string{"one"},
			body: func() any { return responseBody("different-model", result(0, testVector(DefaultDimension, 1))) },
		},
		{
			name: "missing model", inputs: []string{"one"},
			body: func() any { return responseBody("", result(0, testVector(DefaultDimension, 1))) },
		},
		{
			name: "count mismatch", inputs: []string{"one", "two"},
			body: func() any { return responseBody(testModel, result(0, testVector(DefaultDimension, 1))) },
		},
		{
			name: "missing index", inputs: []string{"one"},
			body: func() any {
				return responseBody(testModel, map[string]any{"embedding": testVector(DefaultDimension, 1)})
			},
		},
		{
			name: "duplicate index", inputs: []string{"one", "two"},
			body: func() any {
				return responseBody(testModel, result(0, testVector(DefaultDimension, 1)), result(0, testVector(DefaultDimension, 2)))
			},
		},
		{
			name: "index out of range", inputs: []string{"one"},
			body: func() any { return responseBody(testModel, result(1, testVector(DefaultDimension, 1))) },
		},
		{
			name: "dimension mismatch", inputs: []string{"one"},
			body: func() any { return responseBody(testModel, result(0, testVector(DefaultDimension-1, 1))) },
		},
		{
			name: "all zero", inputs: []string{"one"},
			body: func() any { return responseBody(testModel, result(0, make([]float64, DefaultDimension))) },
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, _ *http.Request) {
				_ = json.NewEncoder(response).Encode(test.body())
			}))
			defer server.Close()
			client := newTestClient(t, server, Config{})

			_, err := client.Embed(context.Background(), test.inputs)
			if !errors.Is(err, ErrInvalidResponse) {
				t.Fatalf("error = %v, want ErrInvalidResponse", err)
			}
		})
	}
}

func TestClientRejectsNaNInfinityAndFloat32OverflowFromHTTP(t *testing.T) {
	tests := []struct {
		name  string
		value string
	}{
		{name: "NaN", value: "NaN"},
		{name: "positive infinity", value: "Infinity"},
		{name: "negative infinity", value: "-Infinity"},
		{name: "float64 overflow", value: "1e400"},
		{name: "float32 overflow", value: "1e100"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, _ *http.Request) {
				_, _ = fmt.Fprintf(response, `{"model":%q,"data":[{"index":0,"embedding":[%s`, testModel, test.value)
				for range DefaultDimension - 1 {
					_, _ = response.Write([]byte(",0"))
				}
				_, _ = response.Write([]byte("]}]}"))
			}))
			defer server.Close()
			client := newTestClient(t, server, Config{})

			_, err := client.Embed(context.Background(), []string{"sensitive input"})
			if !errors.Is(err, ErrInvalidResponse) {
				t.Fatalf("error = %v, want ErrInvalidResponse", err)
			}
			if strings.Contains(err.Error(), "sensitive input") || strings.Contains(err.Error(), test.value) {
				t.Fatalf("error leaked input or response: %v", err)
			}
		})
	}
}

func TestClientRejectsOversizedAndErrorResponsesWithoutBodyLeak(t *testing.T) {
	t.Run("oversized", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, _ *http.Request) {
			_, _ = response.Write([]byte(strings.Repeat("sensitive-response-body", 20)))
		}))
		defer server.Close()
		client := newTestClient(t, server, Config{MaxResponseBytes: 64})
		_, err := client.Embed(context.Background(), []string{"sensitive-input-body"})
		if !errors.Is(err, ErrResponseTooLarge) {
			t.Fatalf("error = %v, want ErrResponseTooLarge", err)
		}
		assertNoSecrets(t, err, "sensitive-response-body", "sensitive-input-body")
	})

	t.Run("non-200", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, _ *http.Request) {
			response.WriteHeader(http.StatusUnauthorized)
			_, _ = response.Write([]byte(`{"error":"sensitive-response-body"}`))
		}))
		defer server.Close()
		client := newTestClient(t, server, Config{})
		_, err := client.Embed(context.Background(), []string{"sensitive-input-body"})
		if !errors.Is(err, ErrInvalidResponse) || !strings.Contains(err.Error(), "401") {
			t.Fatalf("error = %v, want sanitized HTTP status", err)
		}
		assertNoSecrets(t, err, "sensitive-response-body", "sensitive-input-body")
	})

	t.Run("invalid JSON", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, _ *http.Request) {
			_, _ = response.Write([]byte(`sensitive-response-body`))
		}))
		defer server.Close()
		client := newTestClient(t, server, Config{})
		_, err := client.Embed(context.Background(), []string{"sensitive-input-body"})
		if !errors.Is(err, ErrInvalidResponse) {
			t.Fatalf("error = %v, want ErrInvalidResponse", err)
		}
		assertNoSecrets(t, err, "sensitive-response-body", "sensitive-input-body")
	})
}

func TestClientHonorsCallerCancellationAndConfiguredTimeout(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, _ *http.Request) {
		time.Sleep(100 * time.Millisecond)
		_, _ = response.Write([]byte(`{"unused":true}`))
	}))
	defer server.Close()

	t.Run("caller cancellation", func(t *testing.T) {
		client := newTestClient(t, server, Config{Timeout: time.Second})
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		_, err := client.Embed(ctx, []string{"sensitive input"})
		if !errors.Is(err, context.Canceled) || !errors.Is(err, ErrRequestFailed) {
			t.Fatalf("error = %v, want cancellation and ErrRequestFailed", err)
		}
		assertNoSecrets(t, err, "sensitive input", server.URL)
	})

	t.Run("configured timeout", func(t *testing.T) {
		client := newTestClient(t, server, Config{Timeout: 20 * time.Millisecond})
		_, err := client.Embed(context.Background(), []string{"sensitive input"})
		if !errors.Is(err, context.DeadlineExceeded) || !errors.Is(err, ErrRequestFailed) {
			t.Fatalf("error = %v, want deadline and ErrRequestFailed", err)
		}
		assertNoSecrets(t, err, "sensitive input", server.URL)
	})
}

func TestClientDoesNotLeakEndpointOrInputOnTransportFailure(t *testing.T) {
	const secretPath = "secret-endpoint-token"
	client, err := NewClient(Config{
		Endpoint: "http://127.0.0.1:1/" + secretPath,
		Model:    testModel,
		Timeout:  100 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("new client: %v", err)
	}
	_, err = client.Embed(context.Background(), []string{"secret-input-token"})
	if !errors.Is(err, ErrRequestFailed) {
		t.Fatalf("error = %v, want ErrRequestFailed", err)
	}
	assertNoSecrets(t, err, secretPath, "secret-input-token", "127.0.0.1")
}

func TestClientNeverForwardsEmbeddingInputAcrossRedirect(t *testing.T) {
	var redirectedCalls atomic.Int32
	redirected := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		redirectedCalls.Add(1)
	}))
	defer redirected.Close()

	redirector := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		http.Redirect(response, request, redirected.URL+"/secret-target", http.StatusTemporaryRedirect)
	}))
	defer redirector.Close()

	// Supply a client whose own policy explicitly allows redirects. NewClient
	// must clone and override it without mutating the caller's instance.
	callerClient := redirector.Client()
	callerRedirectPolicy := func(*http.Request, []*http.Request) error { return nil }
	callerClient.CheckRedirect = callerRedirectPolicy
	client, err := NewClient(Config{
		Endpoint:   redirector.URL + "/v1/embeddings",
		Model:      testModel,
		Timeout:    time.Second,
		HTTPClient: callerClient,
	})
	if err != nil {
		t.Fatalf("new client: %v", err)
	}

	_, err = client.Embed(context.Background(), []string{"sensitive-input-body"})
	if !errors.Is(err, ErrInvalidResponse) || !strings.Contains(err.Error(), "307") {
		t.Fatalf("error = %v, want sanitized redirect status", err)
	}
	assertNoSecrets(t, err, "sensitive-input-body", "secret-target", redirected.URL)
	if redirectedCalls.Load() != 0 {
		t.Fatalf("redirect target calls = %d, want 0", redirectedCalls.Load())
	}
	if callerClient.CheckRedirect == nil || callerClient.CheckRedirect(nil, nil) != nil {
		t.Fatal("caller client redirect policy was mutated")
	}
}

func newTestClient(t *testing.T, server *httptest.Server, overrides Config) *Client {
	t.Helper()
	config := Config{
		Endpoint:          server.URL + "/v1/embeddings",
		Model:             testModel,
		ExpectedDimension: DefaultDimension,
		Timeout:           time.Second,
		HTTPClient:        server.Client(),
	}
	if overrides.MaxResponseBytes != 0 {
		config.MaxResponseBytes = overrides.MaxResponseBytes
	}
	if overrides.Timeout != 0 {
		config.Timeout = overrides.Timeout
	}
	if overrides.MaxBatchSize != 0 {
		config.MaxBatchSize = overrides.MaxBatchSize
	}
	if overrides.MaxInputBytes != 0 {
		config.MaxInputBytes = overrides.MaxInputBytes
	}
	client, err := NewClient(config)
	if err != nil {
		t.Fatalf("new client: %v", err)
	}
	return client
}

func testVector(dimension int, first float64) []float64 {
	vector := make([]float64, dimension)
	vector[0] = first
	return vector
}

func result(index int, vector []float64) map[string]any {
	return map[string]any{"index": index, "embedding": vector}
}

func responseBody(model string, results ...map[string]any) map[string]any {
	return map[string]any{"model": model, "data": results}
}

func assertNoSecrets(t *testing.T, err error, secrets ...string) {
	t.Helper()
	if err == nil {
		t.Fatal("expected an error")
	}
	for _, secret := range secrets {
		if strings.Contains(err.Error(), secret) {
			t.Fatalf("error leaked %q: %v", secret, err)
		}
	}
}
