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

func TestLoadServerConfigDefaultsToFTSWithoutReadingEmbeddingSettings(t *testing.T) {
	read := make([]string, 0)
	getenv := func(key string) string {
		read = append(read, key)
		switch key {
		case "DATABASE_URL":
			return "postgres://configured"
		case "SERVER_RETRIEVAL_MODE":
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
	if strings.Join(read, ",") != "DATABASE_URL,SERVER_RETRIEVAL_MODE" {
		t.Fatalf("FTS config read unexpected environment keys: %v", read)
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
