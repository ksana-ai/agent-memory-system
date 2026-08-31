package main

import (
	"context"
	"errors"
	"os"
	"strings"
	"testing"
)

func TestOpenAPIHealthPhasesMatchServerModes(t *testing.T) {
	contract, err := os.ReadFile("../../api/openapi.yaml")
	if err != nil {
		t.Fatalf("read OpenAPI contract: %v", err)
	}
	want := "phase: {type: string, enum: [" + strings.Join([]string{
		serverPhaseFTS, serverPhaseDense, serverPhaseHybridRRF,
	}, ", ") + "]}"
	if !strings.Contains(string(contract), want) {
		t.Fatalf("OpenAPI health phases do not match %q", want)
	}
}

func TestOpenAPIIncludesAutomaticExtractionContract(t *testing.T) {
	contract, err := os.ReadFile("../../api/openapi.yaml")
	if err != nil {
		t.Fatalf("read OpenAPI contract: %v", err)
	}
	contents := string(contract)
	for _, required := range []string{
		"/v1/memory-candidate-extractions:",
		"operationId: extractMemoryCandidates",
		"$ref: \"#/components/schemas/ExtractCandidatesRequest\"",
		"$ref: \"#/components/schemas/ExtractCandidatesResponse\"",
		"ExtractionBadGateway:",
		"ExtractionUnavailable:",
		"RequestTimeout:",
		"status: {type: string, const: pending}",
	} {
		if !strings.Contains(contents, required) {
			t.Fatalf("OpenAPI automatic extraction contract is missing %q", required)
		}
	}
}

func TestLoadServerConfigDefaultsToFTSWithoutReadingEmbeddingSettings(t *testing.T) {
	read := make([]string, 0)
	getenv := func(key string) string {
		read = append(read, key)
		switch key {
		case "DATABASE_URL":
			return "postgres://configured"
		case "SERVER_RETRIEVAL_MODE":
			return ""
		case "MEMORY_EXTRACTION_ENABLED":
			return ""
		default:
			return "provider-secret-must-not-be-read"
		}
	}
	config, err := loadServerConfig(nil, getenv)
	if err != nil {
		t.Fatalf("load default FTS config: %v", err)
	}
	if config.retrievalMode != retrievalModeFTS || config.retrievalPhase != serverPhaseFTS || config.address != "127.0.0.1:8080" {
		t.Fatalf("default FTS config=%#v", config)
	}
	if strings.Join(read, ",") != "DATABASE_URL,SERVER_RETRIEVAL_MODE,MEMORY_EXTRACTION_ENABLED" {
		t.Fatalf("FTS config read unexpected environment keys: %v", read)
	}
}

func TestLoadServerConfigEnablesExtractionWithExplicitModelAndAuditSettings(t *testing.T) {
	values := map[string]string{
		"DATABASE_URL":                        "postgres://configured",
		"MEMORY_EXTRACTION_ENABLED":           "true",
		"MEMORY_EXTRACTION_ENDPOINT":          "http://127.0.0.1:1234/v1/chat/completions",
		"MEMORY_EXTRACTION_MODEL":             "local-structured-model",
		"MEMORY_EXTRACTION_AUTH_MODE":         "none",
		"MEMORY_EXTRACTION_TIMEOUT":           "7s",
		"MEMORY_EXTRACTION_EXTRACTOR_NAME":    "local-extractor",
		"MEMORY_EXTRACTION_EXTRACTOR_VERSION": "prompt-v3",
		"MEMORY_EXTRACTION_BEARER_TOKEN":      "provider-secret-must-not-be-read",
	}
	read := make([]string, 0)
	config, err := loadServerConfig(nil, func(key string) string {
		read = append(read, key)
		return values[key]
	})
	if err != nil {
		t.Fatalf("load extraction config: %v", err)
	}
	if !config.extractionEnabled || config.extractionEndpoint != values["MEMORY_EXTRACTION_ENDPOINT"] ||
		config.extractionModel != "local-structured-model" || config.extractionAuthMode != extractionAuthNone ||
		config.extractionTimeout.String() != "7s" || config.extractorName != "local-extractor" ||
		config.extractorVersion != "prompt-v3" || config.extractionToken != "" {
		t.Fatalf("extraction config=%#v", config)
	}
	if strings.Contains(strings.Join(read, ","), "MEMORY_EXTRACTION_BEARER_TOKEN") {
		t.Fatalf("none auth read bearer secret: %v", read)
	}
}

func TestLoadServerConfigRequiresValidExtractionConfiguration(t *testing.T) {
	base := map[string]string{
		"DATABASE_URL":                        "postgres://configured",
		"MEMORY_EXTRACTION_ENABLED":           "true",
		"MEMORY_EXTRACTION_ENDPOINT":          "http://127.0.0.1:1234/v1/chat/completions",
		"MEMORY_EXTRACTION_MODEL":             "local-structured-model",
		"MEMORY_EXTRACTION_AUTH_MODE":         "none",
		"MEMORY_EXTRACTION_EXTRACTOR_NAME":    "local-extractor",
		"MEMORY_EXTRACTION_EXTRACTOR_VERSION": "prompt-v3",
	}
	tests := []struct {
		name   string
		mutate func(map[string]string)
	}{
		{name: "missing endpoint", mutate: func(values map[string]string) { delete(values, "MEMORY_EXTRACTION_ENDPOINT") }},
		{name: "missing model", mutate: func(values map[string]string) { delete(values, "MEMORY_EXTRACTION_MODEL") }},
		{name: "missing extractor name", mutate: func(values map[string]string) { delete(values, "MEMORY_EXTRACTION_EXTRACTOR_NAME") }},
		{name: "missing extractor version", mutate: func(values map[string]string) { delete(values, "MEMORY_EXTRACTION_EXTRACTOR_VERSION") }},
		{name: "invalid auth mode", mutate: func(values map[string]string) { values["MEMORY_EXTRACTION_AUTH_MODE"] = "query-string-secret" }},
		{name: "missing bearer token", mutate: func(values map[string]string) { values["MEMORY_EXTRACTION_AUTH_MODE"] = "bearer" }},
		{name: "invalid timeout", mutate: func(values map[string]string) { values["MEMORY_EXTRACTION_TIMEOUT"] = "forever" }},
		{name: "timeout too large", mutate: func(values map[string]string) { values["MEMORY_EXTRACTION_TIMEOUT"] = "121s" }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			values := cloneServerTestEnvironment(base)
			test.mutate(values)
			if _, err := loadServerConfig(nil, func(key string) string { return values[key] }); err == nil {
				t.Fatal("invalid extraction configuration was accepted")
			}
		})
	}

	invalidEnabled := cloneServerTestEnvironment(base)
	invalidEnabled["MEMORY_EXTRACTION_ENABLED"] = "yes"
	if _, err := loadServerConfig(nil, func(key string) string { return invalidEnabled[key] }); err == nil {
		t.Fatal("non-boolean extraction enabled value was accepted")
	}
}

func TestLoadServerConfigReadsBearerTokenOnlyForBearerAuthentication(t *testing.T) {
	values := map[string]string{
		"DATABASE_URL":                        "postgres://configured",
		"MEMORY_EXTRACTION_ENABLED":           "true",
		"MEMORY_EXTRACTION_ENDPOINT":          "https://models.example.test/v1/chat/completions",
		"MEMORY_EXTRACTION_MODEL":             "structured-model",
		"MEMORY_EXTRACTION_AUTH_MODE":         "bearer",
		"MEMORY_EXTRACTION_BEARER_TOKEN":      "operator-secret",
		"MEMORY_EXTRACTION_EXTRACTOR_NAME":    "remote-extractor",
		"MEMORY_EXTRACTION_EXTRACTOR_VERSION": "v1",
	}
	config, err := loadServerConfig(nil, func(key string) string { return values[key] })
	if err != nil {
		t.Fatalf("load bearer config: %v", err)
	}
	if config.extractionToken != "operator-secret" || config.extractionAuthMode != extractionAuthBearer {
		t.Fatalf("bearer config=%#v", config)
	}
}

func TestRunRejectsInvalidExtractionEndpointBeforeDatabaseStartupWithoutLeakingIt(t *testing.T) {
	values := map[string]string{
		"DATABASE_URL":                        "postgres://database-must-not-be-opened",
		"MEMORY_EXTRACTION_ENABLED":           "true",
		"MEMORY_EXTRACTION_ENDPOINT":          "http://user:provider-secret@example.test/v1/chat/completions",
		"MEMORY_EXTRACTION_MODEL":             "structured-model",
		"MEMORY_EXTRACTION_AUTH_MODE":         "none",
		"MEMORY_EXTRACTION_EXTRACTOR_NAME":    "remote-extractor",
		"MEMORY_EXTRACTION_EXTRACTOR_VERSION": "v1",
	}
	err := run(nil, func(key string) string { return values[key] })
	if err == nil || !strings.Contains(err.Error(), "invalid extraction client configuration") || !strings.Contains(err.Error(), "endpoint") || strings.Contains(err.Error(), "provider-secret") {
		t.Fatalf("startup error=%v", err)
	}
}

func TestLoadServerConfigRequiresExplicitDenseAndHybridDependencies(t *testing.T) {
	base := map[string]string{
		"DATABASE_URL":                  "postgres://configured",
		"LMSTUDIO_EMBEDDINGS_URL":       "http://127.0.0.1:1234/v1/embeddings",
		"LMSTUDIO_EMBEDDING_MODEL":      "text-embedding-bge-m3",
		"SERVER_EXPECTED_SERVING_SPACE": "space_v1_expected",
	}
	for _, mode := range []struct {
		value string
		phase string
	}{{retrievalModeDense, serverPhaseDense}, {retrievalModeHybrid, serverPhaseHybridRRF}} {
		t.Run(mode.value, func(t *testing.T) {
			values := cloneServerTestEnvironment(base)
			values["SERVER_RETRIEVAL_MODE"] = mode.value
			config, err := loadServerConfig([]string{"-addr", "127.0.0.1:9090"}, func(key string) string { return values[key] })
			if err != nil {
				t.Fatalf("load %s config: %v", mode.value, err)
			}
			if config.retrievalMode != mode.value || config.retrievalPhase != mode.phase || config.expectedSpace != values["SERVER_EXPECTED_SERVING_SPACE"] {
				t.Fatalf("%s config=%#v", mode.value, config)
			}
		})
	}

	for _, missing := range []string{"LMSTUDIO_EMBEDDINGS_URL", "LMSTUDIO_EMBEDDING_MODEL", "SERVER_EXPECTED_SERVING_SPACE"} {
		t.Run("missing_"+missing, func(t *testing.T) {
			values := cloneServerTestEnvironment(base)
			values["SERVER_RETRIEVAL_MODE"] = retrievalModeDense
			delete(values, missing)
			if _, err := loadServerConfig(nil, func(key string) string { return values[key] }); err == nil {
				t.Fatalf("missing %s was accepted", missing)
			}
		})
	}
}

func TestLoadServerConfigRejectsUnknownModeAndArgumentsWithoutLeakingValues(t *testing.T) {
	values := map[string]string{
		"DATABASE_URL":             "postgres://configured",
		"SERVER_RETRIEVAL_MODE":    "silent-fallback",
		"LMSTUDIO_EMBEDDINGS_URL":  "http://provider-secret.invalid/v1/embeddings",
		"LMSTUDIO_EMBEDDING_MODEL": "provider-secret-model",
	}
	_, err := loadServerConfig(nil, func(key string) string { return values[key] })
	if err == nil || strings.Contains(err.Error(), "provider-secret") {
		t.Fatalf("unknown mode error=%v", err)
	}
	_, err = loadServerConfig([]string{"-database-url=TOP_SECRET"}, func(key string) string { return values[key] })
	if err == nil || strings.Contains(err.Error(), "TOP_SECRET") {
		t.Fatalf("argument error=%v", err)
	}
}

func TestCombinedReadinessRequiresBothDatabaseAndDenseDependencies(t *testing.T) {
	storage := &serverReadinessStub{}
	dense := &serverDenseReadinessStub{}
	readiness := combinedReadiness{storage: storage, dense: dense}
	if err := readiness.Ping(context.Background()); err != nil {
		t.Fatalf("healthy combined readiness: %v", err)
	}
	if storage.calls != 1 || dense.calls != 1 {
		t.Fatalf("healthy calls storage=%d dense=%d", storage.calls, dense.calls)
	}

	storage.err = errors.New("database-secret")
	if err := readiness.Ping(context.Background()); err == nil || dense.calls != 1 {
		t.Fatalf("database failure error=%v dense_calls=%d", err, dense.calls)
	}
	storage.err = nil
	dense.err = errors.New("provider-secret")
	if err := readiness.Ping(context.Background()); err == nil {
		t.Fatal("dense readiness failure was accepted")
	}
}

type serverReadinessStub struct {
	err   error
	calls int
}

func (stub *serverReadinessStub) Ping(context.Context) error {
	stub.calls++
	return stub.err
}

type serverDenseReadinessStub struct {
	err   error
	calls int
}

func (stub *serverDenseReadinessStub) Ready(context.Context) error {
	stub.calls++
	return stub.err
}

func cloneServerTestEnvironment(source map[string]string) map[string]string {
	result := make(map[string]string, len(source))
	for key, value := range source {
		result[key] = value
	}
	return result
}
