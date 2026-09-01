package eval

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"runtime"
	"runtime/debug"
	"strings"
	"time"

	"github.com/ksana-ai/agent-memory-system/internal/app"
	"github.com/ksana-ai/agent-memory-system/internal/domain"
	"github.com/ksana-ai/agent-memory-system/internal/retrieval"
	"github.com/ksana-ai/agent-memory-system/internal/store/memstore"
)

const DatasetSchemaVersion = "1"

type Dataset struct {
	SchemaVersion string `json:"schema_version"`
	ID            string `json:"id"`
	Version       string `json:"version"`
	Description   string `json:"description"`
	Cases         []Case `json:"cases"`
	SHA256        string `json:"-"`
	fingerprint   string
}

type Case struct {
	ID       string          `json:"id"`
	Query    string          `json:"query"`
	Memories []MemoryFixture `json:"memories"`
	GoldKeys []string        `json:"gold_keys"`
}

type MemoryFixture struct {
	Kind         domain.MemoryKind `json:"kind"`
	Category     string            `json:"category"`
	Key          string            `json:"key"`
	Value        string            `json:"value"`
	Person       string            `json:"person"`
	Relationship string            `json:"relationship"`
	Backstory    string            `json:"backstory"`
	Evidence     string            `json:"evidence"`
}

func Load(data []byte) (Dataset, error) {
	var dataset Dataset
	decoder := json.NewDecoder(strings.NewReader(string(data)))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&dataset); err != nil {
		return Dataset{}, fmt.Errorf("decode dataset: %w", err)
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return Dataset{}, errors.New("dataset contains multiple JSON values")
	}
	if err := validateDataset(dataset); err != nil {
		return Dataset{}, err
	}
	digest := sha256.Sum256(data)
	dataset.SHA256 = hex.EncodeToString(digest[:])
	fingerprint, err := datasetFingerprint(dataset)
	if err != nil {
		return Dataset{}, err
	}
	dataset.fingerprint = fingerprint
	return dataset, nil
}

type Config struct {
	K   int
	Now func() time.Time
}

type Manifest struct {
	SchemaVersion string          `json:"schema_version"`
	Run           RunMetadata     `json:"run"`
	Dataset       DatasetMetadata `json:"dataset"`
	System        SystemMetadata  `json:"system"`
	Metrics       Metrics         `json:"metrics"`
	Cases         []CaseResult    `json:"cases"`
}

type RunMetadata struct {
	GeneratedAt    time.Time `json:"generated_at"`
	GoVersion      string    `json:"go_version"`
	SourceRevision string    `json:"source_revision"`
	SourceModified bool      `json:"source_modified"`
}

type DatasetMetadata struct {
	ID      string `json:"id"`
	Version string `json:"version"`
	SHA256  string `json:"sha256"`
	Cases   int    `json:"cases"`
}

type SystemMetadata struct {
	Retriever string `json:"retriever"`
	TopK      int    `json:"top_k"`
	Storage   string `json:"storage"`
	Extractor string `json:"extractor"`
}

type Metrics struct {
	RecallAtK float64 `json:"recall_at_k"`
	MRR       float64 `json:"mrr"`
	PassRate  float64 `json:"pass_rate"`
}

type CaseResult struct {
	ID             string   `json:"id"`
	GoldKeys       []string `json:"gold_keys"`
	RetrievedKeys  []string `json:"retrieved_keys"`
	RecallAtK      float64  `json:"recall_at_k"`
	ReciprocalRank float64  `json:"reciprocal_rank"`
	Passed         bool     `json:"passed"`
}

func Run(ctx context.Context, dataset Dataset, config Config) (Manifest, error) {
	if err := validateDataset(dataset); err != nil {
		return Manifest{}, err
	}
	if dataset.fingerprint == "" {
		return Manifest{}, errors.New("dataset must be loaded with eval.Load")
	}
	currentFingerprint, err := datasetFingerprint(dataset)
	if err != nil {
		return Manifest{}, err
	}
	if currentFingerprint != dataset.fingerprint {
		return Manifest{}, errors.New("dataset changed after loading; reload it before evaluation")
	}
	if config.K <= 0 || config.K > 20 {
		return Manifest{}, fmt.Errorf("top-k must be between 1 and 20")
	}
	if config.Now == nil {
		config.Now = func() time.Time { return time.Now().UTC() }
	}

	manifest := Manifest{
		SchemaVersion: "1",
		Run: RunMetadata{
			GeneratedAt: config.Now().UTC(),
			GoVersion:   runtime.Version(),
		},
		Dataset: DatasetMetadata{
			ID:      dataset.ID,
			Version: dataset.Version,
			SHA256:  dataset.SHA256,
			Cases:   len(dataset.Cases),
		},
		System: SystemMetadata{
			Retriever: "bm25-memory-card-v1",
			TopK:      config.K,
			Storage:   "in-memory",
			Extractor: "dataset-fixture-v1",
		},
	}
	manifest.Run.SourceRevision, manifest.Run.SourceModified = sourceRevision()

	for caseIndex, testCase := range dataset.Cases {
		result, err := runCase(ctx, config, caseIndex, testCase)
		if err != nil {
			return Manifest{}, fmt.Errorf("case %q: %w", testCase.ID, err)
		}
		manifest.Cases = append(manifest.Cases, result)
		manifest.Metrics.RecallAtK += result.RecallAtK
		manifest.Metrics.MRR += result.ReciprocalRank
		if result.Passed {
			manifest.Metrics.PassRate++
		}
	}
	count := float64(len(manifest.Cases))
	manifest.Metrics.RecallAtK /= count
	manifest.Metrics.MRR /= count
	manifest.Metrics.PassRate /= count
	return manifest, nil
}

func runCase(ctx context.Context, config Config, caseIndex int, testCase Case) (CaseResult, error) {
	storage := memstore.New()
	retriever, err := retrieval.NewBM25(storage)
	if err != nil {
		return CaseResult{}, err
	}
	idCounter := 0
	service, err := app.New(storage, retriever,
		app.WithClock(func() time.Time { return config.Now().UTC() }),
		app.WithIDGenerator(func(prefix string) (string, error) {
			idCounter++
			return fmt.Sprintf("%s_eval_%03d_%03d", prefix, caseIndex, idCounter), nil
		}),
	)
	if err != nil {
		return CaseResult{}, err
	}
	tenantID := fmt.Sprintf("eval-tenant-%03d", caseIndex)
	userID := fmt.Sprintf("eval-user-%03d", caseIndex)
	for memoryIndex, fixture := range testCase.Memories {
		event, err := service.IngestEvidence(ctx, app.IngestEvidenceInput{
			EventID:   fmt.Sprintf("evt_eval_%03d_%03d", caseIndex, memoryIndex),
			TenantID:  tenantID,
			UserID:    userID,
			SessionID: fmt.Sprintf("eval-session-%03d", caseIndex),
			Actor:     domain.ActorUser,
			Content:   fixture.Evidence,
		})
		if err != nil {
			return CaseResult{}, err
		}
		candidate, err := service.ProposeCandidate(ctx, app.ProposeCandidateInput{
			TenantID:         tenantID,
			UserID:           userID,
			Kind:             fixture.Kind,
			Category:         fixture.Category,
			Key:              fixture.Key,
			Value:            fixture.Value,
			Person:           fixture.Person,
			Relationship:     fixture.Relationship,
			Backstory:        fixture.Backstory,
			SourceEventIDs:   []string{event.ID},
			Extractor:        "dataset-fixture",
			ExtractorVersion: datasetFixtureVersion,
		})
		if err != nil {
			return CaseResult{}, err
		}
		if _, _, err := service.ReviewCandidate(ctx, app.ReviewCandidateInput{
			TenantID:    tenantID,
			UserID:      userID,
			CandidateID: candidate.ID,
			Decision:    domain.DecisionApprove,
			ReviewerID:  "deterministic-verifier-v1",
			Reason:      "Fixture candidate is defined as supported by fixture evidence.",
		}); err != nil {
			return CaseResult{}, err
		}
	}

	pack, err := service.BuildContext(ctx, app.BuildContextInput{
		TenantID: tenantID,
		UserID:   userID,
		Query:    testCase.Query,
		Limit:    config.K,
	})
	if err != nil {
		return CaseResult{}, err
	}
	retrievedKeys := make([]string, 0, len(pack.Items))
	for _, item := range pack.Items {
		retrievedKeys = append(retrievedKeys, item.Memory.Key)
	}

	gold := make(map[string]struct{}, len(testCase.GoldKeys))
	for _, key := range testCase.GoldKeys {
		gold[key] = struct{}{}
	}
	found := make(map[string]struct{}, len(gold))
	firstGoldRank := 0
	for index, key := range retrievedKeys {
		if _, ok := gold[key]; !ok {
			continue
		}
		found[key] = struct{}{}
		if firstGoldRank == 0 {
			firstGoldRank = index + 1
		}
	}
	recall := float64(len(found)) / float64(len(gold))
	reciprocalRank := 0.0
	if firstGoldRank > 0 {
		reciprocalRank = 1 / float64(firstGoldRank)
	}
	return CaseResult{
		ID:             testCase.ID,
		GoldKeys:       append([]string(nil), testCase.GoldKeys...),
		RetrievedKeys:  retrievedKeys,
		RecallAtK:      recall,
		ReciprocalRank: reciprocalRank,
		Passed:         len(found) == len(gold),
	}, nil
}

const datasetFixtureVersion = "v1"

func validateDataset(dataset Dataset) error {
	if dataset.SchemaVersion != DatasetSchemaVersion {
		return fmt.Errorf("unsupported dataset schema_version %q", dataset.SchemaVersion)
	}
	if strings.TrimSpace(dataset.ID) == "" || strings.TrimSpace(dataset.Version) == "" {
		return errors.New("dataset id and version are required")
	}
	if len(dataset.Cases) == 0 {
		return errors.New("dataset must contain at least one case")
	}
	caseIDs := make(map[string]struct{}, len(dataset.Cases))
	for _, testCase := range dataset.Cases {
		if strings.TrimSpace(testCase.ID) == "" || strings.TrimSpace(testCase.Query) == "" {
			return errors.New("every case needs an id and query")
		}
		if _, exists := caseIDs[testCase.ID]; exists {
			return fmt.Errorf("duplicate case id %q", testCase.ID)
		}
		caseIDs[testCase.ID] = struct{}{}
		if len(testCase.Memories) == 0 || len(testCase.GoldKeys) == 0 {
			return fmt.Errorf("case %q needs memories and gold_keys", testCase.ID)
		}
		memoryKeys := make(map[string]struct{}, len(testCase.Memories))
		for _, fixture := range testCase.Memories {
			if fixture.Kind != domain.MemoryKindEpisodic && fixture.Kind != domain.MemoryKindSemantic && fixture.Kind != domain.MemoryKindProcedural {
				return fmt.Errorf("case %q has invalid memory kind %q", testCase.ID, fixture.Kind)
			}
			if strings.TrimSpace(fixture.Category) == "" || strings.TrimSpace(fixture.Key) == "" || strings.TrimSpace(fixture.Value) == "" || strings.TrimSpace(fixture.Evidence) == "" {
				return fmt.Errorf("case %q has an incomplete memory fixture", testCase.ID)
			}
			if _, exists := memoryKeys[fixture.Key]; exists {
				return fmt.Errorf("case %q has duplicate memory key %q", testCase.ID, fixture.Key)
			}
			memoryKeys[fixture.Key] = struct{}{}
		}
		goldKeys := make(map[string]struct{}, len(testCase.GoldKeys))
		for _, goldKey := range testCase.GoldKeys {
			if _, exists := memoryKeys[goldKey]; !exists {
				return fmt.Errorf("case %q gold key %q is absent from memories", testCase.ID, goldKey)
			}
			if _, exists := goldKeys[goldKey]; exists {
				return fmt.Errorf("case %q has duplicate gold key %q", testCase.ID, goldKey)
			}
			goldKeys[goldKey] = struct{}{}
		}
	}
	return nil
}

func datasetFingerprint(dataset Dataset) (string, error) {
	encoded, err := json.Marshal(dataset)
	if err != nil {
		return "", fmt.Errorf("fingerprint dataset: %w", err)
	}
	encoded = append(encoded, 0)
	encoded = append(encoded, dataset.SHA256...)
	digest := sha256.Sum256(encoded)
	return hex.EncodeToString(digest[:]), nil
}

func sourceRevision() (string, bool) {
	revision := "unknown"
	modified := true
	info, ok := debug.ReadBuildInfo()
	if !ok {
		return revision, modified
	}
	for _, setting := range info.Settings {
		switch setting.Key {
		case "vcs.revision":
			revision = setting.Value
		case "vcs.modified":
			modified = setting.Value == "true"
		}
	}
	return revision, modified
}
