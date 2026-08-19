package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"

	memoryeval "github.com/kai443/go-agent-memory-system/internal/eval"
)

func main() {
	datasetPath := flag.String("dataset", "./datasets/retrieval-smoke-v1.json", "path to a versioned evaluation dataset")
	outputPath := flag.String("output", "", "optional path for the JSON manifest")
	topK := flag.Int("k", 5, "retrieval cutoff")
	flag.Parse()

	data, err := os.ReadFile(*datasetPath)
	if err != nil {
		fatal("read dataset", err)
	}
	dataset, err := memoryeval.Load(data)
	if err != nil {
		fatal("load dataset", err)
	}
	manifest, err := memoryeval.Run(context.Background(), dataset, memoryeval.Config{K: *topK})
	if err != nil {
		fatal("run evaluation", err)
	}
	encoded, err := json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		fatal("encode manifest", err)
	}
	encoded = append(encoded, '\n')
	if *outputPath != "" {
		if err := os.MkdirAll(filepath.Dir(*outputPath), 0o755); err != nil {
			fatal("create output directory", err)
		}
		if err := os.WriteFile(*outputPath, encoded, 0o644); err != nil {
			fatal("write manifest", err)
		}
	}
	_, _ = os.Stdout.Write(encoded)
}

func fatal(action string, err error) {
	fmt.Fprintf(os.Stderr, "%s: %v\n", action, err)
	os.Exit(1)
}
