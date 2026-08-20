package main

import (
	"context"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/kai443/go-agent-memory-system/internal/domain"
	"github.com/kai443/go-agent-memory-system/internal/embedding"
	"github.com/kai443/go-agent-memory-system/internal/store/postgres"
)

func TestLoadWorkerProcessConfigUsesSafeDefaults(t *testing.T) {
	environment := map[string]string{
		"DATABASE_URL":                      "postgres://worker:secret@database.invalid/memory",
		"LMSTUDIO_EMBEDDINGS_URL":           "http://127.0.0.1:1234/v1/embeddings",
		"LMSTUDIO_EMBEDDING_MODEL":          "text-embedding-bge-m3",
		"PROJECTION_WORKER_EMBEDDING_SPACE": "space_v1_expected",
	}
	config, err := loadWorkerProcessConfig(nil, mapEnvironment(environment))
	if err != nil {
		t.Fatalf("load worker config: %v", err)
	}
	if config.databaseURL != environment["DATABASE_URL"] ||
		config.embeddingsURL != environment["LMSTUDIO_EMBEDDINGS_URL"] ||
		config.embeddingModel != environment["LMSTUDIO_EMBEDDING_MODEL"] ||
		config.requestTimeout != embedding.DefaultTimeout ||
		config.leaseDuration <= config.requestTimeout ||
		config.once {
		t.Fatalf("config=%#v", config)
	}
}

func TestLoadWorkerProcessConfigRejectsArgumentsAndInvalidBoundsWithoutLeakingValues(t *testing.T) {
	const secret = "TOP_SECRET_WORKER_CONFIG"
	base := map[string]string{
		"DATABASE_URL":                      "postgres://worker:" + secret + "@database.invalid/memory",
		"LMSTUDIO_EMBEDDINGS_URL":           "http://127.0.0.1:1234/" + secret,
		"LMSTUDIO_EMBEDDING_MODEL":          "model-" + secret,
		"PROJECTION_WORKER_EMBEDDING_SPACE": "space_v1_expected",
	}
	tests := []struct {
		name        string
		args        []string
		environment map[string]string
	}{
		{name: "argument", args: []string{"-database-url=" + base["DATABASE_URL"]}, environment: base},
		{name: "request timeout", environment: withWorkerEnvironment(base, "PROJECTION_WORKER_REQUEST_TIMEOUT", secret)},
		{name: "lease budget", environment: withWorkerEnvironment(
			withWorkerEnvironment(base, "PROJECTION_WORKER_REQUEST_TIMEOUT", "2s"),
			"PROJECTION_WORKER_LEASE_DURATION", "1s",
		)},
		{name: "attempts", environment: withWorkerEnvironment(base, "PROJECTION_WORKER_MAX_ATTEMPTS", secret)},
		{name: "once", environment: withWorkerEnvironment(base, "PROJECTION_WORKER_ONCE", secret)},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := loadWorkerProcessConfig(test.args, mapEnvironment(test.environment))
			if err == nil {
				t.Fatal("invalid worker config was accepted")
			}
			if strings.Contains(err.Error(), secret) || strings.Contains(err.Error(), "worker:TOP_SECRET") {
				t.Fatalf("configuration error leaked a secret: %v", err)
			}
		})
	}
}

func TestProbeModeDoesNotRequireDatabaseOrExpectedSpace(t *testing.T) {
	config, err := loadWorkerProcessConfig(nil, mapEnvironment(map[string]string{
		"LMSTUDIO_EMBEDDINGS_URL":  "http://127.0.0.1:1234/v1/embeddings",
		"LMSTUDIO_EMBEDDING_MODEL": "text-embedding-bge-m3",
		"PROJECTION_WORKER_MODE":   workerModeProbe,
	}))
	if err != nil {
		t.Fatalf("load probe config: %v", err)
	}
	if config.mode != workerModeProbe || config.databaseURL != "" || config.expectedSpace != "" {
		t.Fatalf("probe config=%#v", config)
	}
}

func TestWorkerMakeTargetsKeepConnectionsOutOfArguments(t *testing.T) {
	const (
		databaseURL   = "postgres://worker:TOP_SECRET_DATABASE@db.invalid/memory"
		embeddingsURL = "http://127.0.0.1:1234/TOP_SECRET_ENDPOINT"
	)
	for _, target := range []string{
		"projection-worker-probe",
		"projection-target-register",
		"projection-worker",
		"verify-worker",
	} {
		command := exec.Command(
			"make", "-n", target,
			"DATABASE_URL="+databaseURL,
			"TEST_DATABASE_URL="+databaseURL,
			"LMSTUDIO_EMBEDDINGS_URL="+embeddingsURL,
			"PROJECTION_WORKER_EMBEDDING_SPACE=space_v1_expected",
		)
		command.Dir = filepath.Join("..", "..")
		output, err := command.CombinedOutput()
		if err != nil {
			t.Fatalf("make -n %s: %v\n%s", target, err, output)
		}
		for _, secret := range []string{databaseURL, embeddingsURL, "TOP_SECRET_DATABASE", "TOP_SECRET_ENDPOINT"} {
			if strings.Contains(string(output), secret) {
				t.Fatalf("%s leaked connection configuration: %s", target, output)
			}
		}
		if strings.Contains(string(output), "-database-url") || strings.Contains(string(output), "-embeddings-url") {
			t.Fatalf("%s put connection configuration in process arguments: %s", target, output)
		}
	}
}

func TestEnsureProjectionTargetRegistersShadowAndAcceptsExistingDeploymentStates(t *testing.T) {
	descriptor := workerTestDescriptor()
	fingerprint := strings.Repeat("a", 64)
	space, err := embedding.SpaceID(
		descriptor.Provider,
		descriptor.Model,
		descriptor.Dimension,
		descriptor.DocumentVersion,
		projectionWorkerQueryV1,
		fingerprint,
	)
	if err != nil {
		t.Fatalf("derive space: %v", err)
	}
	observedAt := time.Date(2026, time.August, 19, 12, 0, 0, 0, time.UTC)

	t.Run("register new shadow", func(t *testing.T) {
		repository := &fakeTargetRepository{loadErrors: []error{domain.ErrNotFound}}
		target, err := ensureProjectionTarget(context.Background(), repository, descriptor, fingerprint, space, observedAt, true)
		if err != nil {
			t.Fatalf("ensure target: %v", err)
		}
		if target.State != postgres.ProjectionTargetShadow || !target.EnqueueNew || repository.registerCalls != 1 {
			t.Fatalf("target=%#v register calls=%d", target, repository.registerCalls)
		}
	})

	t.Run("run mode does not auto register", func(t *testing.T) {
		repository := &fakeTargetRepository{loadErrors: []error{domain.ErrNotFound}}
		if _, err := ensureProjectionTarget(
			context.Background(), repository, descriptor, fingerprint, space, observedAt, false,
		); err == nil {
			t.Fatal("missing target was accepted in run mode")
		}
		if repository.registerCalls != 0 {
			t.Fatalf("run mode registered a target %d times", repository.registerCalls)
		}
	})

	for _, state := range []postgres.ProjectionTargetState{
		postgres.ProjectionTargetShadow,
		postgres.ProjectionTargetServing,
		postgres.ProjectionTargetBlocked,
	} {
		t.Run(string(state), func(t *testing.T) {
			repository := &fakeTargetRepository{target: workerTestTarget(descriptor, fingerprint, space, state, observedAt)}
			target, err := ensureProjectionTarget(context.Background(), repository, descriptor, fingerprint, space, observedAt.Add(time.Hour), false)
			if err != nil || target.State != state || repository.registerCalls != 0 {
				t.Fatalf("target=%#v register calls=%d error=%v", target, repository.registerCalls, err)
			}
		})
	}
}

func TestEnsureProjectionTargetFailsClosedOnRetiredOrDriftedSpace(t *testing.T) {
	descriptor := workerTestDescriptor()
	fingerprint := strings.Repeat("b", 64)
	space, err := embedding.SpaceID(
		descriptor.Provider,
		descriptor.Model,
		descriptor.Dimension,
		descriptor.DocumentVersion,
		projectionWorkerQueryV1,
		fingerprint,
	)
	if err != nil {
		t.Fatalf("derive space: %v", err)
	}
	observedAt := time.Now().UTC()

	retired := &fakeTargetRepository{target: workerTestTarget(
		descriptor,
		fingerprint,
		space,
		postgres.ProjectionTargetRetired,
		observedAt,
	)}
	if _, err := ensureProjectionTarget(context.Background(), retired, descriptor, fingerprint, space, observedAt, false); err == nil {
		t.Fatal("retired target was accepted")
	}

	driftedTarget := workerTestTarget(descriptor, fingerprint, space, postgres.ProjectionTargetShadow, observedAt)
	driftedTarget.Space.Model = "different-model"
	drifted := &fakeTargetRepository{target: driftedTarget}
	if _, err := ensureProjectionTarget(context.Background(), drifted, descriptor, fingerprint, space, observedAt, false); err == nil {
		t.Fatal("drifted target was accepted")
	}
}

func mapEnvironment(values map[string]string) func(string) string {
	return func(key string) string { return values[key] }
}

func withWorkerEnvironment(source map[string]string, key, value string) map[string]string {
	result := make(map[string]string, len(source)+1)
	for currentKey, currentValue := range source {
		result[currentKey] = currentValue
	}
	result[key] = value
	return result
}

func workerTestDescriptor() embedding.Descriptor {
	return embedding.Descriptor{
		Provider:        embedding.ProviderLMStudio,
		API:             embedding.APIEmbeddingsV1,
		Model:           "test-worker-model",
		Dimension:       postgres.VectorDimension,
		DocumentVersion: embedding.MemoryCardDocumentVersion,
	}
}

func workerTestTarget(
	descriptor embedding.Descriptor,
	fingerprint, space string,
	state postgres.ProjectionTargetState,
	createdAt time.Time,
) postgres.ProjectionTarget {
	return postgres.ProjectionTarget{
		Space: postgres.EmbeddingSpaceDefinition{
			ID:               space,
			Provider:         descriptor.Provider,
			Model:            descriptor.Model,
			Dimension:        descriptor.Dimension,
			DocumentVersion:  descriptor.DocumentVersion,
			QueryVersion:     projectionWorkerQueryV1,
			ModelFingerprint: fingerprint,
			CreatedAt:        createdAt,
		},
		State:      state,
		EnqueueNew: state == postgres.ProjectionTargetShadow || state == postgres.ProjectionTargetServing,
		CreatedAt:  createdAt,
		UpdatedAt:  createdAt,
	}
}

type fakeTargetRepository struct {
	target        postgres.ProjectionTarget
	loadErrors    []error
	registerError error
	registerCalls int
}

func (repository *fakeTargetRepository) ProjectionTargetBySpace(context.Context, string) (postgres.ProjectionTarget, error) {
	if len(repository.loadErrors) != 0 {
		err := repository.loadErrors[0]
		repository.loadErrors = repository.loadErrors[1:]
		return postgres.ProjectionTarget{}, err
	}
	return repository.target, nil
}

func (repository *fakeTargetRepository) RegisterProjectionTarget(
	_ context.Context,
	command postgres.RegisterProjectionTargetCommand,
) (postgres.ProjectionTarget, error) {
	repository.registerCalls++
	if repository.registerError != nil {
		return postgres.ProjectionTarget{}, repository.registerError
	}
	repository.target = postgres.ProjectionTarget{
		Space:      command.Space,
		State:      command.State,
		EnqueueNew: command.EnqueueNew,
		CreatedAt:  command.CreatedAt,
		UpdatedAt:  command.CreatedAt,
	}
	return repository.target, nil
}

var _ projectionTargetRepository = (*fakeTargetRepository)(nil)
