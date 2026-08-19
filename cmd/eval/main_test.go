package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func TestSelectArmFactoriesSortsAndRejectsInvalidIDs(t *testing.T) {
	factories, err := selectArmFactories(context.Background(), "reviewed-cards-bm25-v1,no-memory-v1", "", "", "")
	if err != nil {
		t.Fatalf("select arms: %v", err)
	}
	if got := factories[0].Descriptor().ID; got != "no-memory-v1" {
		t.Fatalf("first arm = %q, want no-memory-v1", got)
	}
	if _, err := selectArmFactories(context.Background(), "no-memory-v1,no-memory-v1", "", "", ""); err == nil {
		t.Fatal("duplicate arm was accepted")
	}
	if _, err := selectArmFactories(context.Background(), "unknown", "", "", ""); err == nil {
		t.Fatal("unknown arm was accepted")
	}
	if _, err := selectArmFactories(context.Background(), "reviewed-cards-postgres-fts-v1", "", "", ""); err == nil {
		t.Fatal("PostgreSQL arm without URL was accepted")
	}
	if _, err := selectArmFactories(context.Background(), "reviewed-cards-postgres-vector-v1", "postgres://database", "", "text-embedding-bge-m3"); err == nil {
		t.Fatal("PostgreSQL vector arm without embedding URL was accepted")
	}
}

func TestPostgresURLDefaultDoesNotLeakThroughFlagHelp(t *testing.T) {
	t.Setenv("TEST_DATABASE_URL", "postgres://user:flag-help-secret@example.invalid/database")
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	err := run(context.Background(), []string{"-help"}, &stdout, &stderr)
	if !errors.Is(err, flag.ErrHelp) {
		t.Fatalf("run help error = %v, want flag.ErrHelp", err)
	}
	if strings.Contains(stderr.String(), "flag-help-secret") {
		t.Fatalf("flag help leaked TEST_DATABASE_URL: %q", stderr.String())
	}
}

func TestPostgresURLFlagIsNotAccepted(t *testing.T) {
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	err := run(context.Background(), []string{"-postgres-url=postgres://user:secret@example.invalid/database"}, &stdout, &stderr)
	if err == nil || !strings.Contains(err.Error(), "flag provided but not defined") {
		t.Fatalf("removed PostgreSQL URL flag error = %v", err)
	}
	if strings.Contains(err.Error(), "secret") || strings.Contains(stderr.String(), "secret") {
		t.Fatalf("removed PostgreSQL URL flag leaked its value: error=%q stderr=%q", err, stderr.String())
	}
}

func TestEmbeddingEndpointFlagIsNotAccepted(t *testing.T) {
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	err := run(context.Background(), []string{"-embeddings-url=http://example.invalid/raw-flag-secret"}, &stdout, &stderr)
	if err == nil || !strings.Contains(err.Error(), "flag provided but not defined") {
		t.Fatalf("removed embedding endpoint flag error = %v", err)
	}
	if strings.Contains(err.Error(), "raw-flag-secret") || strings.Contains(stderr.String(), "raw-flag-secret") {
		t.Fatalf("removed embedding endpoint flag leaked its value: error=%q stderr=%q", err, stderr.String())
	}
}

func TestEvalPostgresDryRunDoesNotPutDatabaseURLInCommand(t *testing.T) {
	const secretURL = "postgres://eval-user:make-dry-run-secret@db.example.invalid/agent_memory"
	for _, target := range []string{"eval-postgres", "eval-postgres-recorded"} {
		command := exec.Command("make", "-n", target, "TEST_DATABASE_URL="+secretURL)
		command.Dir = filepath.Join("..", "..")
		output, err := command.CombinedOutput()
		if err != nil {
			t.Fatalf("make -n %s: %v\n%s", target, err, output)
		}
		if strings.Contains(string(output), secretURL) || strings.Contains(string(output), "make-dry-run-secret") {
			t.Fatalf("%s command leaked TEST_DATABASE_URL: %s", target, output)
		}
		if strings.Contains(string(output), "-postgres-url") {
			t.Fatalf("%s passed the database URL through argv: %s", target, output)
		}
	}
}

func TestEvalVectorDryRunDoesNotPutConnectionsInCommand(t *testing.T) {
	const databaseURL = "postgres://eval-user:vector-database-secret@db.example.invalid/agent_memory"
	const embeddingsURL = "http://127.0.0.1:1234/v1/embeddings/vector-endpoint-secret"
	for _, target := range []string{
		"eval-vector", "eval-vector-recorded", "verify-vector",
		"eval-semantic", "eval-semantic-recorded", "verify-semantic",
	} {
		command := exec.Command(
			"make", "-n", target,
			"TEST_DATABASE_URL="+databaseURL,
			"LMSTUDIO_EMBEDDINGS_URL="+embeddingsURL,
		)
		command.Dir = filepath.Join("..", "..")
		output, err := command.CombinedOutput()
		if err != nil {
			t.Fatalf("make -n %s: %v\n%s", target, err, output)
		}
		for _, secret := range []string{databaseURL, "vector-database-secret", embeddingsURL, "vector-endpoint-secret"} {
			if strings.Contains(string(output), secret) {
				t.Fatalf("%s command leaked connection configuration: %s", target, output)
			}
		}
		if strings.Contains(string(output), "-postgres-url") || strings.Contains(string(output), "-embeddings-url") {
			t.Fatalf("%s passed a connection through argv: %s", target, output)
		}
	}
}

func TestWriteAtomicReplacesCompleteManifest(t *testing.T) {
	path := filepath.Join(t.TempDir(), "nested", "manifest.json")
	if err := writeAtomic(path, []byte("first\n")); err != nil {
		t.Fatalf("write first manifest: %v", err)
	}
	if err := writeAtomic(path, []byte("second\n")); err != nil {
		t.Fatalf("replace manifest: %v", err)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read manifest: %v", err)
	}
	if string(data) != "second\n" {
		t.Fatalf("manifest = %q, want second", data)
	}
}

func TestRunDispatchesV2Dataset(t *testing.T) {
	datasetPath := filepath.Join(t.TempDir(), "dataset.json")
	data := []byte(`{
  "schema_version":"2","id":"cli-v2","version":"2.0.0","description":"CLI dispatch fixture",
  "cases":[{"id":"case-one","scopes":[{"id":"subject","tenant_id":"tenant-cli","user_id":"user-cli"}],"timeline":[
    {"op":"memory.remember","memory_ref":"drink","scope":"subject","at":"2026-08-19T10:01:00Z","review_state":"approved","memory":{"kind":"semantic","category":"food","key":"drink","value":"tea"},"evidence":[{"alias":"evt-drink","session_id":"session-cli","actor":"user","content":"I prefer tea.","occurred_at":"2026-08-19T10:00:00Z"}]},
    {"op":"query","id":"drink-query","scope":"subject","at":"2026-08-19T10:02:00Z","text":"preferred drink","judgments":{"memory_cards":{"relevance":{"drink":3}}}}
  ]}]
}`)
	if err := os.WriteFile(datasetPath, data, 0o644); err != nil {
		t.Fatalf("write dataset: %v", err)
	}
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	err := run(context.Background(), []string{
		"-dataset", datasetPath,
		"-arms", "no-memory-v1",
		"-k", "5",
		"-ndcg-k", "10",
		"-query-timeout", "1s",
	}, &stdout, &stderr)
	if err != nil {
		t.Fatalf("run v2 CLI: %v (stderr %q)", err, stderr.String())
	}
	var manifest struct {
		SchemaVersion string `json:"schema_version"`
		Arms          []any  `json:"arms"`
	}
	if err := json.Unmarshal(stdout.Bytes(), &manifest); err != nil {
		t.Fatalf("decode manifest: %v", err)
	}
	if manifest.SchemaVersion != "2" || len(manifest.Arms) != 1 {
		t.Fatalf("unexpected manifest: %#v", manifest)
	}
}
