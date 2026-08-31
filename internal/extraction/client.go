// Package extraction provides a narrow, provider-neutral contract for turning
// persisted evidence into untrusted memory candidate proposals. Callers remain
// responsible for scope, source-grounding, lifecycle, and persistence checks.
package extraction

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/kai443/go-agent-memory-system/internal/domain"
)

const (
	DefaultTimeout          = 30 * time.Second
	DefaultMaxResponseBytes = 1 << 20
	MaxCandidates           = 10
	MaxSupportsPerCandidate = 20
	MaxEvidenceItems        = 20

	ProtocolOpenAICompatibleChatCompletionsJSONSchema = "openai-compatible-v1-chat-completions-json-schema"
)

var (
	ErrInvalidConfig    = errors.New("invalid extraction client configuration")
	ErrInvalidRequest   = errors.New("invalid extraction request")
	ErrRequestFailed    = errors.New("extraction request failed")
	ErrResponseTooLarge = errors.New("extraction response exceeds size limit")
	ErrInvalidResponse  = errors.New("invalid extraction response")
	ErrModelMismatch    = errors.New("extraction model mismatch")
	ErrRefused          = errors.New("extraction refused")
	ErrTimeout          = errors.New("extraction request timed out")
)

// Extractor is the provider-neutral boundary used by the application service.
// Implementations return proposals only; they must not persist or approve them.
type Extractor interface {
	Extract(context.Context, Request) (Result, error)
	Descriptor() Descriptor
}

type Request struct {
	Evidence []Evidence `json:"evidence"`
}

type Evidence struct {
	ID         string       `json:"id"`
	SessionID  string       `json:"session_id"`
	Actor      domain.Actor `json:"actor"`
	Content    string       `json:"content"`
	OccurredAt time.Time    `json:"occurred_at"`
}

type Proposal struct {
	Kind         domain.MemoryKind `json:"kind"`
	Category     string            `json:"category"`
	Key          string            `json:"key"`
	Value        string            `json:"value"`
	Person       string            `json:"person"`
	Relationship string            `json:"relationship"`
	Backstory    string            `json:"backstory"`
	Supports     []Support         `json:"supports"`
}

type Support struct {
	EvidenceID string `json:"evidence_id"`
	Quote      string `json:"quote"`
}

type Result struct {
	Candidates []Proposal `json:"candidates"`
}

// Descriptor is safe to persist with candidate audit metadata. It contains no
// endpoint, credential, request evidence, or provider response.
type Descriptor struct {
	Name     string `json:"name"`
	Version  string `json:"version"`
	Protocol string `json:"protocol"`
}

// Config is the complete network and provenance contract for Client.
// BearerToken is optional so local or otherwise unauthenticated compatible
// endpoints remain supported. A zero Timeout or MaxResponseBytes selects the
// corresponding default.
type Config struct {
	Endpoint         string
	Model            string
	BearerToken      string
	Timeout          time.Duration
	HTTPClient       *http.Client
	ExtractorName    string
	ExtractorVersion string
	MaxResponseBytes int64
}

// HTTPStatusError exposes only a numeric status. Provider error response bodies
// are never read or retained.
type HTTPStatusError struct {
	StatusCode int
}

func (err *HTTPStatusError) Error() string {
	return fmt.Sprintf("%s: HTTP status %d", ErrRequestFailed, err.StatusCode)
}

func (err *HTTPStatusError) Unwrap() error {
	return ErrRequestFailed
}

// RefusalError classifies an explicit model refusal without retaining the
// provider's refusal text.
type RefusalError struct{}

func (*RefusalError) Error() string {
	return ErrRefused.Error()
}

func (*RefusalError) Unwrap() error {
	return ErrRefused
}

// InvalidResponseError reports a fixed, non-sensitive contract reason. Reason
// is always selected locally and never contains provider response bytes.
type InvalidResponseError struct {
	Reason string
}

func (err *InvalidResponseError) Error() string {
	if err.Reason == "" {
		return ErrInvalidResponse.Error()
	}
	return fmt.Sprintf("%s: %s", ErrInvalidResponse, err.Reason)
}

func (*InvalidResponseError) Unwrap() error {
	return ErrInvalidResponse
}

// TimeoutError is both a domain-specific timeout and a standard context
// deadline so HTTP layers can map it without inspecting error strings.
type TimeoutError struct{}

func (*TimeoutError) Error() string {
	return ErrTimeout.Error()
}

func (*TimeoutError) Unwrap() []error {
	return []error{ErrTimeout, ErrRequestFailed, context.DeadlineExceeded}
}

type Client struct {
	endpoint         string
	model            string
	bearerToken      string
	timeout          time.Duration
	httpClient       *http.Client
	descriptor       Descriptor
	maxResponseBytes int64
}

// NewClient validates configuration before any evidence can be sent. Endpoint
// URLs must be absolute HTTP(S) URLs without embedded credentials, query
// strings, or fragments.
func NewClient(config Config) (*Client, error) {
	endpoint, err := validateEndpoint(config.Endpoint)
	if err != nil {
		return nil, err
	}
	model := strings.TrimSpace(config.Model)
	if model == "" {
		return nil, fmt.Errorf("%w: model is required", ErrInvalidConfig)
	}
	name := strings.TrimSpace(config.ExtractorName)
	if name == "" {
		return nil, fmt.Errorf("%w: extractor name is required", ErrInvalidConfig)
	}
	version := strings.TrimSpace(config.ExtractorVersion)
	if version == "" {
		return nil, fmt.Errorf("%w: extractor version is required", ErrInvalidConfig)
	}

	token := config.BearerToken
	if token != "" && (token != strings.TrimSpace(token) || !validBearerToken(token)) {
		return nil, fmt.Errorf("%w: bearer token contains invalid characters", ErrInvalidConfig)
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

	httpClient := config.HTTPClient
	if httpClient == nil {
		httpClient = http.DefaultClient
	}
	redirectSafeHTTPClient := *httpClient
	redirectSafeHTTPClient.CheckRedirect = func(*http.Request, []*http.Request) error {
		return http.ErrUseLastResponse
	}

	return &Client{
		endpoint:    endpoint,
		model:       model,
		bearerToken: token,
		timeout:     timeout,
		httpClient:  &redirectSafeHTTPClient,
		descriptor: Descriptor{
			Name:     name,
			Version:  version,
			Protocol: ProtocolOpenAICompatibleChatCompletionsJSONSchema,
		},
		maxResponseBytes: maxResponseBytes,
	}, nil
}

func (client *Client) Descriptor() Descriptor {
	return client.descriptor
}

type chatCompletionRequest struct {
	Model          string         `json:"model"`
	Messages       []chatMessage  `json:"messages"`
	ResponseFormat responseFormat `json:"response_format"`
	Temperature    float64        `json:"temperature"`
}

type chatMessage struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

type responseFormat struct {
	Type       string             `json:"type"`
	JSONSchema responseJSONSchema `json:"json_schema"`
}

type responseJSONSchema struct {
	Name   string         `json:"name"`
	Strict bool           `json:"strict"`
	Schema map[string]any `json:"schema"`
}

type chatCompletionResponse struct {
	Model   string             `json:"model"`
	Choices []completionChoice `json:"choices"`
}

type completionChoice struct {
	Message completionMessage `json:"message"`
}

type completionMessage struct {
	Content *string `json:"content"`
	Refusal *string `json:"refusal"`
}

const extractionSystemPrompt = `Extract only durable memory candidates directly supported by the supplied evidence. Return only JSON matching the required schema. Every candidate must include at least one support whose evidence_id names supplied evidence and whose quote is an exact excerpt. Do not infer unsupported facts, approve candidates, or add prose.`

// Extract sends evidence to an OpenAI-compatible chat-completions endpoint
// using strict JSON Schema response formatting. Returned proposals remain
// untrusted and require application-level validation before persistence.
func (client *Client) Extract(ctx context.Context, input Request) (Result, error) {
	if err := validateRequest(input); err != nil {
		return Result{}, err
	}
	evidenceJSON, err := json.Marshal(input)
	if err != nil {
		return Result{}, ErrInvalidRequest
	}
	body, err := json.Marshal(chatCompletionRequest{
		Model: client.model,
		Messages: []chatMessage{
			{Role: "system", Content: extractionSystemPrompt},
			{Role: "user", Content: string(evidenceJSON)},
		},
		ResponseFormat: responseFormat{
			Type: "json_schema",
			JSONSchema: responseJSONSchema{
				Name:   "memory_candidate_extraction",
				Strict: true,
				Schema: candidateResultSchema(),
			},
		},
		Temperature: 0,
	})
	if err != nil {
		return Result{}, ErrRequestFailed
	}

	requestContext, cancel := context.WithTimeout(ctx, client.timeout)
	defer cancel()
	request, err := http.NewRequestWithContext(requestContext, http.MethodPost, client.endpoint, bytes.NewReader(body))
	if err != nil {
		return Result{}, ErrRequestFailed
	}
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Accept", "application/json")
	if client.bearerToken != "" {
		request.Header.Set("Authorization", "Bearer "+client.bearerToken)
	}

	response, err := client.httpClient.Do(request)
	if err != nil {
		if contextError := ctx.Err(); contextError != nil {
			if errors.Is(contextError, context.DeadlineExceeded) {
				return Result{}, &TimeoutError{}
			}
			return Result{}, fmt.Errorf("%w: %w", ErrRequestFailed, contextError)
		}
		if errors.Is(requestContext.Err(), context.DeadlineExceeded) {
			return Result{}, &TimeoutError{}
		}
		return Result{}, ErrRequestFailed
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return Result{}, &HTTPStatusError{StatusCode: response.StatusCode}
	}

	responseBody, tooLarge, err := readLimited(response.Body, client.maxResponseBytes)
	if err != nil {
		if contextError := ctx.Err(); contextError != nil {
			if errors.Is(contextError, context.DeadlineExceeded) {
				return Result{}, &TimeoutError{}
			}
			return Result{}, fmt.Errorf("%w: %w", ErrRequestFailed, contextError)
		}
		if errors.Is(requestContext.Err(), context.DeadlineExceeded) {
			return Result{}, &TimeoutError{}
		}
		return Result{}, ErrRequestFailed
	}
	if tooLarge {
		return Result{}, fmt.Errorf("%w: %w", &InvalidResponseError{Reason: "response exceeds size limit"}, ErrResponseTooLarge)
	}
	if !utf8.Valid(responseBody) {
		return Result{}, invalidResponse("response envelope is not valid UTF-8")
	}
	if err := rejectDuplicateJSONKeys(responseBody); err != nil {
		return Result{}, invalidResponse("response envelope contains invalid or duplicate JSON fields")
	}

	var decoded chatCompletionResponse
	if err := json.Unmarshal(responseBody, &decoded); err != nil {
		return Result{}, invalidResponse("response envelope is not valid JSON")
	}
	if decoded.Model != client.model {
		return Result{}, fmt.Errorf("%w: %w", invalidResponse("response model does not match request"), ErrModelMismatch)
	}
	if len(decoded.Choices) != 1 {
		return Result{}, invalidResponse("response must contain exactly one choice")
	}
	message := decoded.Choices[0].Message
	if message.Refusal != nil && strings.TrimSpace(*message.Refusal) != "" {
		return Result{}, &RefusalError{}
	}
	if message.Content == nil {
		return Result{}, invalidResponse("choice content is missing")
	}
	return decodeResult(*message.Content)
}

type resultPayload struct {
	Candidates *[]proposalPayload `json:"candidates"`
}

type proposalPayload struct {
	Kind         *domain.MemoryKind `json:"kind"`
	Category     *string            `json:"category"`
	Key          *string            `json:"key"`
	Value        *string            `json:"value"`
	Person       *string            `json:"person"`
	Relationship *string            `json:"relationship"`
	Backstory    *string            `json:"backstory"`
	Supports     *[]supportPayload  `json:"supports"`
}

type supportPayload struct {
	EvidenceID *string `json:"evidence_id"`
	Quote      *string `json:"quote"`
}

func decodeResult(content string) (Result, error) {
	if strings.TrimSpace(content) == "" {
		return Result{}, invalidResponse("choice content is empty")
	}
	if err := rejectDuplicateJSONKeys([]byte(content)); err != nil {
		return Result{}, invalidResponse("choice content contains invalid or duplicate JSON fields")
	}

	decoder := json.NewDecoder(strings.NewReader(content))
	decoder.DisallowUnknownFields()
	var payload resultPayload
	if err := decoder.Decode(&payload); err != nil {
		return Result{}, invalidResponse("choice content does not match the required schema")
	}
	if err := ensureJSONEOF(decoder); err != nil {
		return Result{}, invalidResponse("choice content must contain one JSON value")
	}
	if payload.Candidates == nil {
		return Result{}, invalidResponse("candidates is required")
	}
	if len(*payload.Candidates) > MaxCandidates {
		return Result{}, invalidResponse("candidate count exceeds limit")
	}

	result := Result{Candidates: make([]Proposal, 0, len(*payload.Candidates))}
	for _, candidate := range *payload.Candidates {
		if candidate.Kind == nil || candidate.Category == nil || candidate.Key == nil || candidate.Value == nil ||
			candidate.Person == nil || candidate.Relationship == nil || candidate.Backstory == nil || candidate.Supports == nil {
			return Result{}, invalidResponse("candidate is missing a required field")
		}
		if *candidate.Kind != domain.MemoryKindEpisodic && *candidate.Kind != domain.MemoryKindSemantic && *candidate.Kind != domain.MemoryKindProcedural {
			return Result{}, invalidResponse("candidate kind is invalid")
		}
		if strings.TrimSpace(*candidate.Category) == "" || strings.TrimSpace(*candidate.Key) == "" || strings.TrimSpace(*candidate.Value) == "" {
			return Result{}, invalidResponse("candidate required text is empty")
		}
		if len(*candidate.Supports) == 0 || len(*candidate.Supports) > MaxSupportsPerCandidate {
			return Result{}, invalidResponse("candidate support count is invalid")
		}
		supports := make([]Support, 0, len(*candidate.Supports))
		for _, support := range *candidate.Supports {
			if support.EvidenceID == nil || support.Quote == nil {
				return Result{}, invalidResponse("support is missing a required field")
			}
			if strings.TrimSpace(*support.EvidenceID) == "" || strings.TrimSpace(*support.Quote) == "" {
				return Result{}, invalidResponse("support required text is empty")
			}
			supports = append(supports, Support{EvidenceID: *support.EvidenceID, Quote: *support.Quote})
		}
		result.Candidates = append(result.Candidates, Proposal{
			Kind:         *candidate.Kind,
			Category:     *candidate.Category,
			Key:          *candidate.Key,
			Value:        *candidate.Value,
			Person:       *candidate.Person,
			Relationship: *candidate.Relationship,
			Backstory:    *candidate.Backstory,
			Supports:     supports,
		})
	}
	return result, nil
}

func validateRequest(input Request) error {
	if len(input.Evidence) == 0 {
		return fmt.Errorf("%w: evidence is required", ErrInvalidRequest)
	}
	if len(input.Evidence) > MaxEvidenceItems {
		return fmt.Errorf("%w: evidence item count exceeds limit", ErrInvalidRequest)
	}
	seen := make(map[string]struct{}, len(input.Evidence))
	for _, evidence := range input.Evidence {
		id := strings.TrimSpace(evidence.ID)
		if id == "" || strings.TrimSpace(evidence.SessionID) == "" || strings.TrimSpace(evidence.Content) == "" || evidence.OccurredAt.IsZero() {
			return fmt.Errorf("%w: evidence contains a missing required field", ErrInvalidRequest)
		}
		if evidence.Actor != domain.ActorUser && evidence.Actor != domain.ActorAgent && evidence.Actor != domain.ActorTool {
			return fmt.Errorf("%w: evidence actor is invalid", ErrInvalidRequest)
		}
		if _, exists := seen[id]; exists {
			return fmt.Errorf("%w: evidence ID is duplicated", ErrInvalidRequest)
		}
		seen[id] = struct{}{}
	}
	return nil
}

func candidateResultSchema() map[string]any {
	stringSchema := map[string]any{"type": "string"}
	return map[string]any{
		"type":                 "object",
		"additionalProperties": false,
		"required":             []string{"candidates"},
		"properties": map[string]any{
			"candidates": map[string]any{
				"type":     "array",
				"maxItems": MaxCandidates,
				"items": map[string]any{
					"type":                 "object",
					"additionalProperties": false,
					"required": []string{
						"kind", "category", "key", "value", "person", "relationship", "backstory", "supports",
					},
					"properties": map[string]any{
						"kind": map[string]any{
							"type": "string",
							"enum": []string{string(domain.MemoryKindEpisodic), string(domain.MemoryKindSemantic), string(domain.MemoryKindProcedural)},
						},
						"category":     stringSchema,
						"key":          stringSchema,
						"value":        stringSchema,
						"person":       stringSchema,
						"relationship": stringSchema,
						"backstory":    stringSchema,
						"supports": map[string]any{
							"type":     "array",
							"minItems": 1,
							"maxItems": MaxSupportsPerCandidate,
							"items": map[string]any{
								"type":                 "object",
								"additionalProperties": false,
								"required":             []string{"evidence_id", "quote"},
								"properties": map[string]any{
									"evidence_id": stringSchema,
									"quote":       stringSchema,
								},
							},
						},
					},
				},
			},
		},
	}
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

func validBearerToken(token string) bool {
	for _, character := range token {
		if character < 0x21 || character > 0x7e {
			return false
		}
	}
	return true
}

func readLimited(reader io.Reader, limit int64) ([]byte, bool, error) {
	contents, err := io.ReadAll(io.LimitReader(reader, limit+1))
	if err != nil {
		return nil, false, err
	}
	if int64(len(contents)) > limit {
		return nil, true, nil
	}
	return contents, false, nil
}

func invalidResponse(reason string) error {
	return &InvalidResponseError{Reason: reason}
}

func ensureJSONEOF(decoder *json.Decoder) error {
	var extra any
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		if err == nil {
			return errors.New("trailing JSON value")
		}
		return err
	}
	return nil
}

// rejectDuplicateJSONKeys removes encoding/json's last-key-wins ambiguity
// before the typed strict decoder runs.
func rejectDuplicateJSONKeys(contents []byte) error {
	decoder := json.NewDecoder(bytes.NewReader(contents))
	decoder.UseNumber()
	if err := consumeJSONValue(decoder); err != nil {
		return err
	}
	if token, err := decoder.Token(); !errors.Is(err, io.EOF) {
		if err == nil {
			return fmt.Errorf("unexpected token %v", token)
		}
		return err
	}
	return nil
}

func consumeJSONValue(decoder *json.Decoder) error {
	token, err := decoder.Token()
	if err != nil {
		return err
	}
	delimiter, isDelimiter := token.(json.Delim)
	if !isDelimiter {
		return nil
	}
	switch delimiter {
	case '{':
		seen := make(map[string]struct{})
		for decoder.More() {
			keyToken, err := decoder.Token()
			if err != nil {
				return err
			}
			key, ok := keyToken.(string)
			if !ok {
				return errors.New("object key is not a string")
			}
			if _, duplicate := seen[key]; duplicate {
				return errors.New("duplicate object key")
			}
			seen[key] = struct{}{}
			if err := consumeJSONValue(decoder); err != nil {
				return err
			}
		}
		closing, err := decoder.Token()
		if err != nil || closing != json.Delim('}') {
			return errors.New("object is not closed")
		}
	case '[':
		for decoder.More() {
			if err := consumeJSONValue(decoder); err != nil {
				return err
			}
		}
		closing, err := decoder.Token()
		if err != nil || closing != json.Delim(']') {
			return errors.New("array is not closed")
		}
	default:
		return errors.New("unexpected JSON delimiter")
	}
	return nil
}

var _ Extractor = (*Client)(nil)
