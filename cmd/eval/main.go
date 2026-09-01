package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	memoryeval "github.com/ksana-ai/agent-memory-system/internal/eval"
)

func main() {
	if err := run(context.Background(), os.Args[1:], os.Stdout, os.Stderr); err != nil {
		fmt.Fprintf(os.Stderr, "evaluation failed: %v\n", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, arguments []string, stdout, stderr io.Writer) error {
	flags := flag.NewFlagSet("agent-memory-eval", flag.ContinueOnError)
	flags.SetOutput(stderr)
	datasetPath := flags.String("dataset", "./datasets/retrieval-smoke-v1.json", "path to a versioned evaluation dataset")
	outputPath := flags.String("output", "", "optional path for the JSON manifest")
	quiet := flags.Bool("quiet", false, "suppress the manifest on stdout")
	topK := flags.Int("k", 5, "recall cutoff")
	armIDs := flags.String("arms", "", "comma-separated v2 retrieval arm IDs")
	ndcgK := flags.Int("ndcg-k", 10, "v2 nDCG cutoff and minimum retrieval depth")
	warmupRuns := flags.Int("warmup-runs", 0, "v2 unmeasured search runs per query")
	measuredRuns := flags.Int("measured-runs", 1, "v2 measured search runs per query")
	queryTimeout := flags.Duration("query-timeout", 5*time.Second, "v2 timeout for each search run")
	sourceDirectory := flags.String("source-dir", ".", "Git checkout used for v2 runtime source verification")
	requireClean := flags.Bool("require-clean", false, "require matching clean build-time and runtime Git revisions")
	requirePolicyPass := flags.Bool("require-policy-pass", false, "fail after writing a v2 manifest if any policy invariant fails")
	if err := flags.Parse(arguments); err != nil {
		return err
	}
	if flags.NArg() != 0 {
		return fmt.Errorf("unexpected positional arguments: %s", strings.Join(flags.Args(), " "))
	}
	postgresURL := os.Getenv("TEST_DATABASE_URL")
	embeddingsURL := os.Getenv("LMSTUDIO_EMBEDDINGS_URL")
	embeddingModel := os.Getenv("LMSTUDIO_EMBEDDING_MODEL")

	data, err := os.ReadFile(*datasetPath)
	if err != nil {
		return fmt.Errorf("read dataset: %w", err)
	}
	schemaVersion, err := detectDatasetSchema(data)
	if err != nil {
		return err
	}

	var manifest any
	switch schemaVersion {
	case memoryeval.DatasetSchemaVersion:
		if strings.TrimSpace(*armIDs) != "" || *requireClean || *requirePolicyPass {
			return errors.New("-arms, -require-clean, and -require-policy-pass require a schema v2 dataset")
		}
		dataset, loadErr := memoryeval.Load(data)
		if loadErr != nil {
			return fmt.Errorf("load dataset: %w", loadErr)
		}
		manifest, err = memoryeval.Run(ctx, dataset, memoryeval.Config{K: *topK})
	case memoryeval.DatasetSchemaVersionV2:
		dataset, loadErr := memoryeval.LoadV2(data)
		if loadErr != nil {
			return fmt.Errorf("load v2 dataset: %w", loadErr)
		}
		arms, armErr := selectArmFactories(ctx, *armIDs, postgresURL, embeddingsURL, embeddingModel)
		if armErr != nil {
			return armErr
		}
		sourceBefore := memoryeval.InspectSourceStateV2(ctx, *sourceDirectory)
		if *requireClean {
			if cleanErr := memoryeval.RequireCleanSourceV2(sourceBefore); cleanErr != nil {
				return cleanErr
			}
		}
		sourceProof := memoryeval.SourceProofV2{
			Before: sourceBefore, After: sourceBefore, Stable: true,
			CleanRequired: *requireClean,
		}
		v2Manifest, runErr := memoryeval.RunV2(ctx, dataset, memoryeval.ConfigV2{
			RecallK:           *topK,
			NDCGK:             *ndcgK,
			WarmupRuns:        *warmupRuns,
			MeasuredRuns:      *measuredRuns,
			QueryTimeout:      *queryTimeout,
			Arms:              arms,
			Source:            sourceProof,
			RequirePolicyPass: *requirePolicyPass,
		})
		if runErr != nil {
			err = runErr
			break
		}
		sourceAfter := memoryeval.InspectSourceStateV2(ctx, *sourceDirectory)
		if *requireClean {
			if cleanErr := memoryeval.RequireCleanSourceV2(sourceAfter); cleanErr != nil {
				return fmt.Errorf("source changed while evaluation was running: %w", cleanErr)
			}
			if sourceBefore != sourceAfter {
				return errors.New("source state changed while evaluation was running")
			}
		}
		sourceProof.After = sourceAfter
		sourceProof.Stable = sourceBefore == sourceAfter
		sourceProof.CleanVerified = sourceProof.Stable &&
			memoryeval.RequireCleanSourceV2(sourceBefore) == nil &&
			memoryeval.RequireCleanSourceV2(sourceAfter) == nil
		v2Manifest.Source = sourceProof
		manifest = v2Manifest
	default:
		return fmt.Errorf("unsupported dataset schema_version %q", schemaVersion)
	}
	if err != nil {
		return fmt.Errorf("run evaluation: %w", err)
	}

	encoded, err := json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		return fmt.Errorf("encode manifest: %w", err)
	}
	encoded = append(encoded, '\n')
	if *outputPath != "" {
		if err := writeAtomic(*outputPath, encoded); err != nil {
			return fmt.Errorf("write manifest: %w", err)
		}
	}
	if !*quiet {
		if _, err := stdout.Write(encoded); err != nil {
			return fmt.Errorf("write manifest to stdout: %w", err)
		}
	}

	if *requirePolicyPass && schemaVersion == memoryeval.DatasetSchemaVersionV2 {
		v2Manifest := manifest.(memoryeval.ManifestV2)
		for _, arm := range v2Manifest.Arms {
			if !arm.Aggregate.PolicyPassed {
				return fmt.Errorf("retrieval arm %q failed policy invariants", arm.Descriptor.ID)
			}
		}
	}
	return nil
}

func detectDatasetSchema(data []byte) (string, error) {
	var envelope struct {
		SchemaVersion string `json:"schema_version"`
	}
	if err := json.Unmarshal(data, &envelope); err != nil {
		return "", fmt.Errorf("decode dataset schema_version: %w", err)
	}
	if strings.TrimSpace(envelope.SchemaVersion) == "" {
		return "", errors.New("dataset schema_version is required")
	}
	return envelope.SchemaVersion, nil
}

func selectArmFactories(ctx context.Context, value, postgresURL, embeddingsURL, embeddingModel string) ([]memoryeval.ArmFactory, error) {
	if strings.TrimSpace(value) == "" {
		return memoryeval.BuiltinArmFactories(), nil
	}
	seen := make(map[string]struct{})
	factories := make([]memoryeval.ArmFactory, 0)
	for _, rawID := range strings.Split(value, ",") {
		id := strings.TrimSpace(rawID)
		if id == "" {
			return nil, errors.New("evaluation arm ID must not be empty")
		}
		if _, exists := seen[id]; exists {
			return nil, fmt.Errorf("duplicate evaluation arm %q", id)
		}
		var factory memoryeval.ArmFactory
		var err error
		if id == memoryeval.ArmReviewedCardsPostgresFTSV1 {
			factory, err = memoryeval.NewPostgresFTSArmFactory(ctx, postgresURL)
		} else if id == memoryeval.ArmReviewedCardsPostgresVectorV1 {
			factory, err = memoryeval.NewPostgresVectorArmFactory(ctx, postgresURL, embeddingsURL, embeddingModel)
		} else {
			factory, err = memoryeval.BuiltinArmFactory(id)
		}
		if err != nil {
			return nil, err
		}
		seen[id] = struct{}{}
		factories = append(factories, factory)
	}
	sort.Slice(factories, func(i, j int) bool {
		return factories[i].Descriptor().ID < factories[j].Descriptor().ID
	})
	return factories, nil
}

func writeAtomic(path string, data []byte) error {
	directory := filepath.Dir(path)
	if err := os.MkdirAll(directory, 0o755); err != nil {
		return err
	}
	temporary, err := os.CreateTemp(directory, ".agent-memory-eval-*.tmp")
	if err != nil {
		return err
	}
	temporaryPath := temporary.Name()
	defer os.Remove(temporaryPath)

	if err := temporary.Chmod(0o644); err != nil {
		_ = temporary.Close()
		return err
	}
	if _, err := temporary.Write(data); err != nil {
		_ = temporary.Close()
		return err
	}
	if err := temporary.Sync(); err != nil {
		_ = temporary.Close()
		return err
	}
	if err := temporary.Close(); err != nil {
		return err
	}
	return os.Rename(temporaryPath, path)
}
