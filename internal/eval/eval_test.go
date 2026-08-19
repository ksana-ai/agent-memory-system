package eval

import (
	"context"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"
)

func TestRunProducesReproducibleMetrics(t *testing.T) {
	dataset, err := Load([]byte(`{
		"schema_version":"1",
		"id":"tiny",
		"version":"1.0.0",
		"description":"unit fixture",
		"cases":[{
			"id":"seat",
			"query":"preferred window seat",
			"memories":[
				{"kind":"semantic","category":"travel","key":"seat_preference","value":"window seat","person":"self","relationship":"self","backstory":"flight preference","evidence":"I prefer window seats."},
				{"kind":"semantic","category":"travel","key":"meal_preference","value":"vegetarian meal","person":"self","relationship":"self","backstory":"flight meal","evidence":"I need vegetarian meals."}
			],
			"gold_keys":["seat_preference"]
		}]
	}`))
	if err != nil {
		t.Fatalf("load dataset: %v", err)
	}
	fixed := time.Date(2026, 8, 19, 12, 0, 0, 0, time.UTC)
	manifest, err := Run(context.Background(), dataset, Config{K: 1, Now: func() time.Time { return fixed }})
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if manifest.Metrics.RecallAtK != 1 || manifest.Metrics.MRR != 1 || manifest.Metrics.PassRate != 1 {
		t.Fatalf("unexpected metrics: %#v", manifest.Metrics)
	}
	if manifest.Dataset.SHA256 == "" || len(manifest.Cases) != 1 || manifest.Run.GeneratedAt != fixed {
		t.Fatalf("manifest is incomplete: %#v", manifest)
	}
}

func TestLoadRejectsUnknownFields(t *testing.T) {
	_, err := Load([]byte(`{"schema_version":"1","id":"x","version":"1","description":"x","cases":[],"unknown":true}`))
	if err == nil {
		t.Fatal("Load accepted an unknown field")
	}
}

func TestRepositorySmokeDatasetRuns(t *testing.T) {
	_, filename, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("runtime.Caller failed")
	}
	data, err := os.ReadFile(filepath.Join(filepath.Dir(filename), "..", "..", "datasets", "retrieval-smoke-v1.json"))
	if err != nil {
		t.Fatalf("read repository dataset: %v", err)
	}
	dataset, err := Load(data)
	if err != nil {
		t.Fatalf("load repository dataset: %v", err)
	}
	if dataset.ID != "memory-card-retrieval-smoke" || dataset.Version != "1.0.0" || len(dataset.Cases) != 8 || dataset.SHA256 == "" {
		t.Fatalf("unexpected dataset metadata: %#v", dataset)
	}
	manifest, err := Run(context.Background(), dataset, Config{K: 5, Now: func() time.Time {
		return time.Date(2026, 8, 19, 12, 0, 0, 0, time.UTC)
	}})
	if err != nil {
		t.Fatalf("run repository dataset: %v", err)
	}
	if manifest.Metrics.RecallAtK != 1 || manifest.Metrics.MRR != 1 || manifest.Metrics.PassRate != 1 {
		t.Fatalf("unexpected smoke metrics: %#v", manifest.Metrics)
	}
}

func TestRunRejectsDatasetMutationAfterLoad(t *testing.T) {
	dataset, err := Load([]byte(`{
		"schema_version":"1","id":"tiny","version":"1","description":"x",
		"cases":[{"id":"one","query":"fact","memories":[{"kind":"semantic","category":"c","key":"k","value":"v","person":"self","relationship":"self","backstory":"b","evidence":"fact"}],"gold_keys":["k"]}]
	}`))
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	dataset.Cases[0].Query = "mutated"
	if _, err := Run(context.Background(), dataset, Config{K: 1}); err == nil {
		t.Fatal("Run accepted a dataset mutated after loading")
	}
}

func TestRunRejectsDatasetHashMutationAfterLoad(t *testing.T) {
	dataset, err := Load([]byte(`{
		"schema_version":"1","id":"tiny","version":"1","description":"x",
		"cases":[{"id":"one","query":"fact","memories":[{"kind":"semantic","category":"c","key":"k","value":"v","person":"self","relationship":"self","backstory":"b","evidence":"fact"}],"gold_keys":["k"]}]
	}`))
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	dataset.SHA256 = "fake"
	if _, err := Run(context.Background(), dataset, Config{K: 1}); err == nil {
		t.Fatal("Run accepted a mutated dataset SHA-256")
	}
}
