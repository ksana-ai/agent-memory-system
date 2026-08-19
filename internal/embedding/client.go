// Package embedding provides the narrow OpenAI-compatible embeddings client
// used by the dense retrieval projection. It deliberately keeps endpoint
// details out of its public descriptor and errors.
package embedding

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

const (
	DefaultDimension        = 1024
	DefaultTimeout          = 15 * time.Second
	DefaultMaxResponseBytes = 16 << 20
	DefaultMaxBatchSize     = 128
	DefaultMaxInputBytes    = 32 << 10

	ProviderLMStudio = "lmstudio"
	APIEmbeddingsV1  = "openai-compatible-v1-embeddings"
)

var (
	ErrInvalidConfig    = errors.New("invalid embedding client configuration")
	ErrInvalidInput     = errors.New("invalid embedding input")
	ErrRequestFailed    = errors.New("embedding request failed")
	ErrResponseTooLarge = errors.New("embedding response exceeds size limit")
	ErrInvalidResponse  = errors.New("invalid embedding response")
)

// Config is the complete network and output contract for Client. A zero
// ExpectedDimension selects DefaultDimension; a zero Timeout selects
// DefaultTimeout; and zero limits select their corresponding DefaultMax*
// constants.
type Config struct {
	Endpoint          string
	Model             string
	ExpectedDimension int
	Timeout           time.Duration
	HTTPClient        *http.Client
	MaxResponseBytes  int64
	MaxBatchSize      int
	MaxInputBytes     int
}

// Descriptor is safe to persist in evaluation manifests. In particular, it
// never contains the endpoint, headers, or any input text.
type Descriptor struct {
	Provider        string `json:"provider"`
	API             string `json:"api"`
	Model           string `json:"model"`
	Dimension       int    `json:"dimension"`
	DocumentVersion string `json:"document_version"`
}

type Client struct {
	endpoint         string
	model            string
	dimension        int
	timeout          time.Duration
	httpClient       *http.Client
	maxResponseBytes int64
	maxBatchSize     int
	maxInputBytes    int
}

// NewClient validates all configuration before any input can be sent. Only
// absolute HTTP(S) endpoints without userinfo, query strings, or fragments are
// accepted, preventing credentials from being hidden in endpoint URLs.
func NewClient(config Config) (*Client, error) {
	endpoint, err := validateEndpoint(config.Endpoint)
	if err != nil {
		return nil, err
	}

	model := strings.TrimSpace(config.Model)
	if model == "" {
		return nil, fmt.Errorf("%w: model is required", ErrInvalidConfig)
	}

	dimension := config.ExpectedDimension
	if dimension == 0 {
		dimension = DefaultDimension
	}
	if dimension < 1 {
		return nil, fmt.Errorf("%w: expected dimension must be positive", ErrInvalidConfig)
	}

	timeout := config.Timeout
	if timeout == 0 {
		timeout = DefaultTimeout
	}
	if timeout < 0 {
		return nil, fmt.Errorf("%w: timeout must be positive", ErrInvalidConfig)
	}

	maxResponseBytes := config.MaxResponseBytes
	if maxResponseBytes == 0 {
		maxResponseBytes = DefaultMaxResponseBytes
	}
	if maxResponseBytes < 1 {
		return nil, fmt.Errorf("%w: response size limit must be positive", ErrInvalidConfig)
	}
	maxBatchSize := config.MaxBatchSize
	if maxBatchSize == 0 {
		maxBatchSize = DefaultMaxBatchSize
	}
	if maxBatchSize < 1 {
		return nil, fmt.Errorf("%w: batch size limit must be positive", ErrInvalidConfig)
	}
	maxInputBytes := config.MaxInputBytes
	if maxInputBytes == 0 {
		maxInputBytes = DefaultMaxInputBytes
	}
	if maxInputBytes < 1 {
		return nil, fmt.Errorf("%w: input byte limit must be positive", ErrInvalidConfig)
	}

	httpClient := config.HTTPClient
	if httpClient == nil {
		httpClient = http.DefaultClient
	}
	// Clone the caller's client value before enforcing a no-redirect policy.
	// Embedding inputs are sensitive and must never be replayed to a Location
	// selected by the remote endpoint, including a different host.
	redirectSafeHTTPClient := *httpClient
	redirectSafeHTTPClient.CheckRedirect = func(*http.Request, []*http.Request) error {
		return http.ErrUseLastResponse
	}

	return &Client{
		endpoint:         endpoint,
		model:            model,
		dimension:        dimension,
		timeout:          timeout,
		httpClient:       &redirectSafeHTTPClient,
		maxResponseBytes: maxResponseBytes,
		maxBatchSize:     maxBatchSize,
		maxInputBytes:    maxInputBytes,
	}, nil
}

// Descriptor returns non-sensitive component provenance.
func (client *Client) Descriptor() Descriptor {
	return Descriptor{
		Provider:        ProviderLMStudio,
		API:             APIEmbeddingsV1,
		Model:           client.model,
		Dimension:       client.dimension,
		DocumentVersion: MemoryCardDocumentVersion,
	}
}

type embeddingsRequest struct {
	Model string   `json:"model"`
	Input []string `json:"input"`
}

type embeddingsResponse struct {
	Data  []embeddingData `json:"data"`
	Model string          `json:"model"`
}

type embeddingData struct {
	Embedding []json.Number `json:"embedding"`
	Index     *int          `json:"index"`
}

// Embed sends a batch in one request and returns vectors in the same order as
// inputs. The service may return data entries out of order; their unique index
// fields are validated and used to restore input order.
func (client *Client) Embed(ctx context.Context, inputs []string) ([][]float32, error) {
	if len(inputs) == 0 {
		return nil, fmt.Errorf("%w: batch must not be empty", ErrInvalidInput)
	}
	if len(inputs) > client.maxBatchSize {
		return nil, fmt.Errorf("%w: batch exceeds item limit", ErrInvalidInput)
	}
	for _, input := range inputs {
		if strings.TrimSpace(input) == "" {
			return nil, fmt.Errorf("%w: batch contains an empty item", ErrInvalidInput)
		}
		if len(input) > client.maxInputBytes {
			return nil, fmt.Errorf("%w: batch item exceeds byte limit", ErrInvalidInput)
		}
	}

	body, err := json.Marshal(embeddingsRequest{Model: client.model, Input: inputs})
	if err != nil {
		return nil, ErrRequestFailed
	}

	requestContext, cancel := context.WithTimeout(ctx, client.timeout)
	defer cancel()

	request, err := http.NewRequestWithContext(requestContext, http.MethodPost, client.endpoint, bytes.NewReader(body))
	if err != nil {
		return nil, ErrRequestFailed
	}
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Accept", "application/json")

	response, err := client.httpClient.Do(request)
	if err != nil {
		if contextError := ctx.Err(); contextError != nil {
			return nil, fmt.Errorf("%w: %w", ErrRequestFailed, contextError)
		}
		if requestError := requestContext.Err(); requestError != nil {
			return nil, fmt.Errorf("%w: %w", ErrRequestFailed, requestError)
		}
		// Do not wrap the transport error: net/http errors include the
		// request URL and may include remote response details.
		return nil, ErrRequestFailed
	}
	defer response.Body.Close()

	responseBody, tooLarge, err := readLimited(response.Body, client.maxResponseBytes)
	if err != nil {
		return nil, ErrRequestFailed
	}
	if tooLarge {
		return nil, ErrResponseTooLarge
	}
	if response.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("%w: HTTP status %d", ErrInvalidResponse, response.StatusCode)
	}

	var decoded embeddingsResponse
	if err := json.Unmarshal(responseBody, &decoded); err != nil {
		// The decoder error is intentionally discarded because it can quote
		// bytes from the untrusted response body.
		return nil, fmt.Errorf("%w: response is not valid JSON", ErrInvalidResponse)
	}
	return client.validateResponse(decoded, len(inputs))
}

func (client *Client) validateResponse(response embeddingsResponse, inputCount int) ([][]float32, error) {
	if response.Model != client.model {
		return nil, fmt.Errorf("%w: model mismatch", ErrInvalidResponse)
	}
	if len(response.Data) != inputCount {
		return nil, fmt.Errorf("%w: result count mismatch", ErrInvalidResponse)
	}

	vectors := make([][]float32, inputCount)
	seen := make([]bool, inputCount)
	for _, item := range response.Data {
		if item.Index == nil {
			return nil, fmt.Errorf("%w: result index is missing", ErrInvalidResponse)
		}
		index := *item.Index
		if index < 0 || index >= inputCount {
			return nil, fmt.Errorf("%w: result index is out of range", ErrInvalidResponse)
		}
		if seen[index] {
			return nil, fmt.Errorf("%w: result index is duplicated", ErrInvalidResponse)
		}
		seen[index] = true

		if len(item.Embedding) != client.dimension {
			return nil, fmt.Errorf("%w: vector dimension mismatch", ErrInvalidResponse)
		}
		vector := make([]float32, client.dimension)
		nonzero := false
		for position, number := range item.Embedding {
			value, err := strconv.ParseFloat(string(number), 64)
			if err != nil || math.IsNaN(value) || math.IsInf(value, 0) {
				return nil, fmt.Errorf("%w: vector contains a non-finite value", ErrInvalidResponse)
			}
			value32 := float32(value)
			if math.IsNaN(float64(value32)) || math.IsInf(float64(value32), 0) {
				return nil, fmt.Errorf("%w: vector contains a non-finite value", ErrInvalidResponse)
			}
			vector[position] = value32
			nonzero = nonzero || value32 != 0
		}
		if !nonzero {
			return nil, fmt.Errorf("%w: vector must not be all zero", ErrInvalidResponse)
		}
		vectors[index] = vector
	}

	for _, present := range seen {
		if !present {
			return nil, fmt.Errorf("%w: result index is missing", ErrInvalidResponse)
		}
	}
	return vectors, nil
}

func validateEndpoint(raw string) (string, error) {
	parsed, err := url.Parse(strings.TrimSpace(raw))
	if err != nil {
		return "", fmt.Errorf("%w: endpoint URL is invalid", ErrInvalidConfig)
	}
	if parsed.Scheme != "http" && parsed.Scheme != "https" {
		return "", fmt.Errorf("%w: endpoint must use HTTP or HTTPS", ErrInvalidConfig)
	}
	if parsed.Host == "" || parsed.Opaque != "" {
		return "", fmt.Errorf("%w: endpoint must be an absolute URL", ErrInvalidConfig)
	}
	if parsed.User != nil || parsed.RawQuery != "" || parsed.ForceQuery || parsed.Fragment != "" {
		return "", fmt.Errorf("%w: endpoint must not contain credentials, a query, or a fragment", ErrInvalidConfig)
	}
	return parsed.String(), nil
}

func readLimited(reader io.Reader, limit int64) ([]byte, bool, error) {
	limited := io.LimitReader(reader, limit+1)
	contents, err := io.ReadAll(limited)
	if err != nil {
		return nil, false, err
	}
	if int64(len(contents)) > limit {
		return nil, true, nil
	}
	return contents, false, nil
}
