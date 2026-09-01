package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/ksana-ai/agent-memory-system/internal/domain"
	"github.com/ksana-ai/agent-memory-system/internal/embedding"
	"github.com/ksana-ai/agent-memory-system/internal/id"
	"github.com/ksana-ai/agent-memory-system/internal/migrations"
	"github.com/ksana-ai/agent-memory-system/internal/projectionworker"
	"github.com/ksana-ai/agent-memory-system/internal/store/postgres"
)

const (
	workerStartupTimeout      = 60 * time.Second
	minimumRequestTimeout     = time.Millisecond
	maximumRequestTimeout     = time.Minute
	minimumIdleInterval       = 10 * time.Millisecond
	maximumIdleInterval       = time.Minute
	maximumConfiguredAttempts = projectionworker.MaximumAttempts
	projectionWorkerIDPrefix  = "projection_worker"
	projectionWorkerQueryV1   = embedding.RawQueryVersion

	workerModeRun      = "run"
	workerModeProbe    = "probe"
	workerModeRegister = "register-shadow"
)

type workerProcessConfig struct {
	databaseURL    string
	embeddingsURL  string
	embeddingModel string
	requestTimeout time.Duration
	leaseDuration  time.Duration
	idleInterval   time.Duration
	maxAttempts    int
	once           bool
	mode           string
	expectedSpace  string
}

type projectionTargetRepository interface {
	ProjectionTargetBySpace(context.Context, string) (postgres.ProjectionTarget, error)
	RegisterProjectionTarget(context.Context, postgres.RegisterProjectionTargetCommand) (postgres.ProjectionTarget, error)
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := run(ctx, os.Args[1:], os.Getenv); err != nil {
		slog.Error("projection worker stopped", "error", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, args []string, getenv func(string) string) error {
	config, err := loadWorkerProcessConfig(args, getenv)
	if err != nil {
		return err
	}

	startupContext, cancelStartup := context.WithTimeout(ctx, workerStartupTimeout)
	defer cancelStartup()
	embedder, err := embedding.NewClient(embedding.Config{
		Endpoint:          config.embeddingsURL,
		Model:             config.embeddingModel,
		ExpectedDimension: postgres.VectorDimension,
		Timeout:           config.requestTimeout,
		MaxBatchSize:      projectionworker.EmbeddingBatchSizeV1,
	})
	if err != nil {
		return errors.New("create projection worker embedding client failed")
	}
	descriptor := embedder.Descriptor()
	probeVectors, err := embedder.Embed(startupContext, []string{embedding.ProbeTextV1})
	if err != nil || len(probeVectors) != 1 || len(probeVectors[0]) != descriptor.Dimension {
		return errors.New("probe projection worker embedding component failed")
	}
	modelFingerprint := embedding.VectorSHA256(probeVectors[0])
	embeddingSpace, err := embedding.SpaceID(
		descriptor.Provider,
		descriptor.Model,
		descriptor.Dimension,
		descriptor.DocumentVersion,
		projectionWorkerQueryV1,
		modelFingerprint,
	)
	if err != nil {
		return errors.New("derive projection worker embedding space failed")
	}
	if config.expectedSpace != "" && config.expectedSpace != embeddingSpace {
		return errors.New("live embedding probe does not match PROJECTION_WORKER_EMBEDDING_SPACE")
	}
	if config.mode == workerModeProbe {
		slog.Info(
			"projection worker embedding probe complete",
			"embedding_space", embeddingSpace,
			"model_fingerprint", modelFingerprint,
			"probe_text_sha256", embedding.ProbeTextV1SHA256,
		)
		return nil
	}

	if err := migrations.Apply(startupContext, config.databaseURL); err != nil {
		return errors.New("apply projection worker database migrations failed")
	}
	storage, err := postgres.Open(startupContext, config.databaseURL)
	if err != nil {
		return errors.New("open projection worker PostgreSQL store failed")
	}
	defer storage.Close()
	target, err := ensureProjectionTarget(
		startupContext,
		storage,
		descriptor,
		modelFingerprint,
		embeddingSpace,
		time.Now().UTC(),
		config.mode == workerModeRegister,
	)
	if err != nil {
		return err
	}
	if config.mode == workerModeRegister {
		slog.Info(
			"projection shadow target registered",
			"embedding_space", embeddingSpace,
			"target_state", target.State,
			"enqueue_new", target.EnqueueNew,
		)
		return nil
	}

	leaseOwner, err := id.New(projectionWorkerIDPrefix)
	if err != nil {
		return errors.New("create projection worker identity failed")
	}
	worker, err := projectionworker.New(storage, embedder, projectionworker.Config{
		EmbeddingSpace:   embeddingSpace,
		ModelFingerprint: modelFingerprint,
		QueryVersion:     projectionWorkerQueryV1,
		LeaseOwner:       leaseOwner,
		BatchSize:        1,
		LeaseDuration:    config.leaseDuration,
		IdleInterval:     config.idleInterval,
		MaxAttempts:      config.maxAttempts,
		Concurrency:      projectionworker.ConcurrencyV1,
	})
	if err != nil {
		return errors.New("create projection worker runtime failed")
	}
	cancelStartup()

	slog.Info(
		"starting projection worker",
		"embedding_space", embeddingSpace,
		"target_state", target.State,
		"once", config.once,
	)
	if config.once {
		result, runErr := worker.RunOnce(ctx)
		if runErr != nil {
			if ctx.Err() != nil {
				return nil
			}
			return errors.New("run projection worker batch failed")
		}
		slog.Info(
			"projection worker batch complete",
			"claimed", result.Claimed,
			"succeeded", result.Succeeded,
			"retried", result.Retried,
			"dead_lettered", result.DeadLettered,
			"cancelled", result.Cancelled,
			"lease_lost", result.LeaseLost,
		)
		return nil
	}
	if err := worker.Run(ctx); err != nil {
		if ctx.Err() != nil {
			return nil
		}
		return errors.New("run projection worker failed")
	}
	return nil
}

func loadWorkerProcessConfig(args []string, getenv func(string) string) (workerProcessConfig, error) {
	if len(args) != 0 {
		return workerProcessConfig{}, errors.New("projection worker accepts no command-line arguments; configure it with environment variables")
	}
	if getenv == nil {
		return workerProcessConfig{}, errors.New("projection worker environment reader is required")
	}
	config := workerProcessConfig{
		databaseURL:    strings.TrimSpace(getenv("DATABASE_URL")),
		embeddingsURL:  strings.TrimSpace(getenv("LMSTUDIO_EMBEDDINGS_URL")),
		embeddingModel: strings.TrimSpace(getenv("LMSTUDIO_EMBEDDING_MODEL")),
		requestTimeout: embedding.DefaultTimeout,
		leaseDuration:  projectionworker.DefaultLeaseDuration,
		idleInterval:   projectionworker.DefaultIdleInterval,
		maxAttempts:    projectionworker.DefaultMaxAttempts,
		mode:           workerModeRun,
		expectedSpace:  strings.TrimSpace(getenv("PROJECTION_WORKER_EMBEDDING_SPACE")),
	}
	if config.embeddingsURL == "" {
		return workerProcessConfig{}, errors.New("LMSTUDIO_EMBEDDINGS_URL is required")
	}
	if config.embeddingModel == "" {
		return workerProcessConfig{}, errors.New("LMSTUDIO_EMBEDDING_MODEL is required")
	}
	if rawMode := strings.TrimSpace(getenv("PROJECTION_WORKER_MODE")); rawMode != "" {
		config.mode = rawMode
	}
	switch config.mode {
	case workerModeRun, workerModeProbe, workerModeRegister:
	default:
		return workerProcessConfig{}, errors.New("PROJECTION_WORKER_MODE must be run, probe, or register-shadow")
	}
	if config.mode != workerModeProbe && config.databaseURL == "" {
		return workerProcessConfig{}, errors.New("DATABASE_URL is required outside probe mode")
	}
	if config.mode != workerModeProbe && config.expectedSpace == "" {
		return workerProcessConfig{}, errors.New("PROJECTION_WORKER_EMBEDDING_SPACE is required outside probe mode")
	}

	var err error
	if config.requestTimeout, err = durationEnvironment(
		getenv,
		"PROJECTION_WORKER_REQUEST_TIMEOUT",
		config.requestTimeout,
		minimumRequestTimeout,
		maximumRequestTimeout,
	); err != nil {
		return workerProcessConfig{}, err
	}
	if config.leaseDuration, err = durationEnvironment(
		getenv,
		"PROJECTION_WORKER_LEASE_DURATION",
		config.leaseDuration,
		time.Microsecond,
		projectionworker.MaximumLeaseDuration,
	); err != nil {
		return workerProcessConfig{}, err
	}
	if config.leaseDuration <= config.requestTimeout {
		return workerProcessConfig{}, errors.New("PROJECTION_WORKER_LEASE_DURATION must exceed PROJECTION_WORKER_REQUEST_TIMEOUT")
	}
	if config.idleInterval, err = durationEnvironment(
		getenv,
		"PROJECTION_WORKER_IDLE_INTERVAL",
		config.idleInterval,
		minimumIdleInterval,
		maximumIdleInterval,
	); err != nil {
		return workerProcessConfig{}, err
	}
	if raw := strings.TrimSpace(getenv("PROJECTION_WORKER_MAX_ATTEMPTS")); raw != "" {
		config.maxAttempts, err = strconv.Atoi(raw)
		if err != nil || config.maxAttempts < 1 || config.maxAttempts > maximumConfiguredAttempts {
			return workerProcessConfig{}, errors.New("PROJECTION_WORKER_MAX_ATTEMPTS must be an integer between 1 and 100")
		}
	}
	if raw := strings.TrimSpace(getenv("PROJECTION_WORKER_ONCE")); raw != "" {
		config.once, err = strconv.ParseBool(raw)
		if err != nil {
			return workerProcessConfig{}, errors.New("PROJECTION_WORKER_ONCE must be a boolean")
		}
	}
	return config, nil
}

func durationEnvironment(
	getenv func(string) string,
	name string,
	fallback, minimum, maximum time.Duration,
) (time.Duration, error) {
	raw := strings.TrimSpace(getenv(name))
	if raw == "" {
		return fallback, nil
	}
	value, err := time.ParseDuration(raw)
	if err != nil || value < minimum || value > maximum {
		return 0, fmt.Errorf("%s is outside its allowed duration range", name)
	}
	return value, nil
}

func ensureProjectionTarget(
	ctx context.Context,
	repository projectionTargetRepository,
	descriptor embedding.Descriptor,
	modelFingerprint, embeddingSpace string,
	observedAt time.Time,
	registerMissing bool,
) (postgres.ProjectionTarget, error) {
	definition := postgres.EmbeddingSpaceDefinition{
		ID:               embeddingSpace,
		Provider:         descriptor.Provider,
		Model:            descriptor.Model,
		Dimension:        descriptor.Dimension,
		DocumentVersion:  descriptor.DocumentVersion,
		QueryVersion:     projectionWorkerQueryV1,
		ModelFingerprint: modelFingerprint,
		CreatedAt:        observedAt,
	}
	target, err := repository.ProjectionTargetBySpace(ctx, embeddingSpace)
	if errors.Is(err, domain.ErrNotFound) && registerMissing {
		target, err = repository.RegisterProjectionTarget(ctx, postgres.RegisterProjectionTargetCommand{
			Space:      definition,
			State:      postgres.ProjectionTargetShadow,
			EnqueueNew: true,
			CreatedAt:  observedAt,
		})
		if errors.Is(err, domain.ErrConflict) {
			// Another process or an operator may have registered/promoted the
			// same immutable space between the read and insert.
			target, err = repository.ProjectionTargetBySpace(ctx, embeddingSpace)
		}
	}
	if errors.Is(err, domain.ErrNotFound) {
		return postgres.ProjectionTarget{}, errors.New("projection target is not registered; run explicit register-shadow mode first")
	}
	if err != nil {
		return postgres.ProjectionTarget{}, errors.New("load or register projection target failed")
	}
	if !sameEmbeddingSpace(definition, target.Space) {
		return postgres.ProjectionTarget{}, errors.New("projection target embedding space conflicts with the probed model")
	}
	switch target.State {
	case postgres.ProjectionTargetShadow, postgres.ProjectionTargetServing, postgres.ProjectionTargetBlocked:
		return target, nil
	case postgres.ProjectionTargetRetired:
		return postgres.ProjectionTarget{}, errors.New("projection target is retired; register a new immutable embedding space")
	default:
		return postgres.ProjectionTarget{}, errors.New("projection target has an invalid state")
	}
}

func sameEmbeddingSpace(left, right postgres.EmbeddingSpaceDefinition) bool {
	return left.ID == right.ID &&
		left.Provider == right.Provider &&
		left.Model == right.Model &&
		left.Dimension == right.Dimension &&
		left.DocumentVersion == right.DocumentVersion &&
		left.QueryVersion == right.QueryVersion &&
		left.ModelFingerprint == right.ModelFingerprint
}
