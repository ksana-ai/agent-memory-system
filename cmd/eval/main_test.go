package main

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

func TestSelectArmFactoriesSortsAndRejectsInvalidIDs(t *testing.T) {
	factories, err := selectArmFactories("reviewed-cards-bm25-v1,no-memory-v1")
	if err != nil {
		t.Fatalf("select arms: %v", err)
	}
	if got := factories[0].Descriptor().ID; got != "no-memory-v1" {
		t.Fatalf("first arm = %q, want no-memory-v1", got)
	}
	if _, err := selectArmFactories("no-memory-v1,no-memory-v1"); err == nil {
		t.Fatal("duplicate arm was accepted")
	}
	if _, err := selectArmFactories("unknown"); err == nil {
		t.Fatal("unknown arm was accepted")
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
