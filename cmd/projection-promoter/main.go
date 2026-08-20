package main

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/kai443/go-agent-memory-system/internal/domain"
	"github.com/kai443/go-agent-memory-system/internal/embedding"
	"github.com/kai443/go-agent-memory-system/internal/migrations"
	"github.com/kai443/go-agent-memory-system/internal/store/postgres"
)

const (
	promoterStartupTimeout = 60 * time.Second
	promoterExpectedNone   = "none"
	promoterQueryVersion   = embedding.RawQueryVersion
)

type promoterProcessConfig struct {
	databaseURL    string
	embeddingsURL  string
	embeddingModel string
	toSpace        string
	operationID    string
	expectedFrom   string
	allowEmpty     bool
}

type promotionRepository interface {
	ProjectionPromotionByOperationID(context.Context, string) (postgres.ProjectionPromotionReceipt, error)
	PromoteProjection(context.Context, postgres.PromoteProjectionCommand) (postgres.ProjectionPromotionReceipt, error)
}

type promotionRepositoryFactory func(context.Context, string) (promotionRepository, func(), error)

type promotionSpaceProbe func(context.Context, promoterProcessConfig) (string, error)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	receipt, err := run(ctx, os.Args[1:], os.Getenv, openPromotionRepository, probeLivePromotionSpace)
	if err != nil {
		slog.Error("projection promoter stopped", "error", err)
		os.Exit(1)
	}
	logPromotionReceipt(receipt)
}

func run(
	ctx context.Context,
	args []string,
	getenv func(string) string,
	openRepository promotionRepositoryFactory,
	probeSpace promotionSpaceProbe,
) (postgres.ProjectionPromotionReceipt, error) {
	config, err := loadPromoterProcessConfig(args, getenv)
	if err != nil {
		return postgres.ProjectionPromotionReceipt{}, err
	}
	if openRepository == nil {
		return postgres.ProjectionPromotionReceipt{}, errors.New("projection promoter repository factory is required")
	}
	if probeSpace == nil {
		return postgres.ProjectionPromotionReceipt{}, errors.New("projection promoter embedding probe is required")
	}

	startupContext, cancelStartup := context.WithTimeout(ctx, promoterStartupTimeout)
	defer cancelStartup()
	repository, closeRepository, err := openRepository(startupContext, config.databaseURL)
	if err != nil {
		return postgres.ProjectionPromotionReceipt{}, errors.New("open projection promotion repository failed")
	}
	if repository == nil || closeRepository == nil {
		if closeRepository != nil {
			closeRepository()
		}
		return postgres.ProjectionPromotionReceipt{}, errors.New("open projection promotion repository failed")
	}
	defer closeRepository()

	command := postgres.PromoteProjectionCommand{
		OperationID:  config.operationID,
		ExpectedFrom: config.expectedFrom,
		ToSpace:      config.toSpace,
		AllowEmpty:   config.allowEmpty,
	}
	existing, err := repository.ProjectionPromotionByOperationID(startupContext, config.operationID)
	if err == nil {
		if !promotionReceiptMatchesCommand(existing, command) {
			return postgres.ProjectionPromotionReceipt{}, errors.New("projection promotion operation id is already bound to a different command")
		}
		return existing, nil
	}
	if !errors.Is(err, domain.ErrNotFound) {
		return postgres.ProjectionPromotionReceipt{}, errors.New("load projection promotion receipt failed")
	}

	liveSpace, err := probeSpace(startupContext, config)
	if err != nil {
		return postgres.ProjectionPromotionReceipt{}, errors.New("probe projection promoter embedding component failed")
	}
	if liveSpace != config.toSpace {
		return postgres.ProjectionPromotionReceipt{}, errors.New("live embedding probe does not match PROJECTION_PROMOTER_EMBEDDING_SPACE")
	}

	receipt, err := repository.PromoteProjection(startupContext, command)
	if err != nil {
		return postgres.ProjectionPromotionReceipt{}, errors.New("promote projection target failed")
	}
	return receipt, nil
}

func promotionReceiptMatchesCommand(
	receipt postgres.ProjectionPromotionReceipt,
	command postgres.PromoteProjectionCommand,
) bool {
	return receipt.OperationID == command.OperationID &&
		receipt.FromSpace == command.ExpectedFrom &&
		receipt.ToSpace == command.ToSpace &&
		receipt.AllowEmpty == command.AllowEmpty
}

func openPromotionRepository(
	ctx context.Context,
	databaseURL string,
) (promotionRepository, func(), error) {
	if err := migrations.Apply(ctx, databaseURL); err != nil {
		return nil, nil, errors.New("apply projection promoter database migrations failed")
	}
	storage, err := postgres.Open(ctx, databaseURL)
	if err != nil {
		return nil, nil, errors.New("open projection promoter PostgreSQL store failed")
	}
	return storage, storage.Close, nil
}

func probeLivePromotionSpace(ctx context.Context, config promoterProcessConfig) (string, error) {
	client, err := embedding.NewClient(embedding.Config{
		Endpoint:          config.embeddingsURL,
		Model:             config.embeddingModel,
		ExpectedDimension: postgres.VectorDimension,
		Timeout:           embedding.DefaultTimeout,
		MaxBatchSize:      1,
	})
	if err != nil {
		return "", errors.New("create projection promoter embedding client failed")
	}
	descriptor := client.Descriptor()
	vectors, err := client.Embed(ctx, []string{embedding.ProbeTextV1})
	if err != nil || len(vectors) != 1 || len(vectors[0]) != descriptor.Dimension {
		return "", errors.New("probe projection promoter embedding client failed")
	}
	space, err := embedding.SpaceID(
		descriptor.Provider,
		descriptor.Model,
		descriptor.Dimension,
		descriptor.DocumentVersion,
		promoterQueryVersion,
		embedding.VectorSHA256(vectors[0]),
	)
	if err != nil {
		return "", errors.New("derive projection promoter embedding space failed")
	}
	return space, nil
}

func loadPromoterProcessConfig(args []string, getenv func(string) string) (promoterProcessConfig, error) {
	if len(args) != 0 {
		return promoterProcessConfig{}, errors.New("projection promoter accepts no command-line arguments; configure it with environment variables")
	}
	if getenv == nil {
		return promoterProcessConfig{}, errors.New("projection promoter environment reader is required")
	}
	config := promoterProcessConfig{
		databaseURL:    strings.TrimSpace(getenv("DATABASE_URL")),
		embeddingsURL:  strings.TrimSpace(getenv("LMSTUDIO_EMBEDDINGS_URL")),
		embeddingModel: strings.TrimSpace(getenv("LMSTUDIO_EMBEDDING_MODEL")),
		toSpace:        strings.TrimSpace(getenv("PROJECTION_PROMOTER_EMBEDDING_SPACE")),
		operationID:    strings.TrimSpace(getenv("PROJECTION_PROMOTER_OPERATION_ID")),
	}
	if config.databaseURL == "" {
		return promoterProcessConfig{}, errors.New("DATABASE_URL is required")
	}
	if config.embeddingsURL == "" {
		return promoterProcessConfig{}, errors.New("LMSTUDIO_EMBEDDINGS_URL is required")
	}
	if config.embeddingModel == "" {
		return promoterProcessConfig{}, errors.New("LMSTUDIO_EMBEDDING_MODEL is required")
	}
	if config.toSpace == "" {
		return promoterProcessConfig{}, errors.New("PROJECTION_PROMOTER_EMBEDDING_SPACE is required")
	}
	if config.operationID == "" {
		return promoterProcessConfig{}, errors.New("PROJECTION_PROMOTER_OPERATION_ID is required")
	}
	if !isCanonicalLowercaseUUID(config.operationID) {
		return promoterProcessConfig{}, errors.New("PROJECTION_PROMOTER_OPERATION_ID must be a canonical lowercase UUID")
	}
	rawExpectedFrom := strings.TrimSpace(getenv("PROJECTION_PROMOTER_EXPECTED_FROM"))
	if rawExpectedFrom == "" {
		return promoterProcessConfig{}, errors.New("PROJECTION_PROMOTER_EXPECTED_FROM is required; use none when no serving space is expected")
	}
	if rawExpectedFrom == promoterExpectedNone {
		config.expectedFrom = ""
	} else {
		config.expectedFrom = rawExpectedFrom
	}
	if rawAllowEmpty := strings.TrimSpace(getenv("PROJECTION_PROMOTER_ALLOW_EMPTY")); rawAllowEmpty != "" {
		allowEmpty, err := strconv.ParseBool(rawAllowEmpty)
		if err != nil {
			return promoterProcessConfig{}, errors.New("PROJECTION_PROMOTER_ALLOW_EMPTY must be a boolean")
		}
		config.allowEmpty = allowEmpty
	}
	return config, nil
}

func isCanonicalLowercaseUUID(value string) bool {
	if len(value) != 36 {
		return false
	}
	for index := 0; index < len(value); index++ {
		switch index {
		case 8, 13, 18, 23:
			if value[index] != '-' {
				return false
			}
		default:
			character := value[index]
			if !((character >= '0' && character <= '9') || (character >= 'a' && character <= 'f')) {
				return false
			}
		}
	}
	return true
}

func logPromotionReceipt(receipt postgres.ProjectionPromotionReceipt) {
	slog.Info(
		"projection promotion complete",
		"operation_id", receipt.OperationID,
		"from_embedding_space", receipt.FromSpace,
		"to_embedding_space", receipt.ToSpace,
		"allow_empty", receipt.AllowEmpty,
		"live_scope_count", receipt.LiveScopeCount,
		"live_card_count", receipt.LiveCardCount,
		"covered_card_count", receipt.CoveredCardCount,
		"previous_generation", receipt.PreviousGeneration,
		"generation", receipt.Generation,
		"cutoff_at", receipt.CutoffAt,
		"promoted_at", receipt.PromotedAt,
	)
}
