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

	"github.com/kai443/go-agent-memory-system/internal/domain"
	"github.com/kai443/go-agent-memory-system/internal/migrations"
	"github.com/kai443/go-agent-memory-system/internal/store/postgres"
)

const (
	reconcilerStartupTimeout       = 60 * time.Second
	defaultReconciliationBatchSize = 100
	minimumReconciliationBatchSize = 1
	maximumReconciliationBatchSize = 500
	maximumReconciliationPages     = 1_000_000
	maximumGenerationRestarts      = 3

	reconcilerModeAudit    = "audit"
	reconcilerModeBackfill = "backfill"
)

var errProjectionCoverageIncomplete = errors.New("projection reconciliation incomplete")

type reconcilerProcessConfig struct {
	databaseURL    string
	embeddingSpace string
	batchSize      int
	mode           string
}

type reconciliationRepository interface {
	BeginProjectionReconciliation(context.Context, string, bool) (postgres.ProjectionReconciliationSnapshot, error)
	ReconcileProjectionPage(
		context.Context,
		postgres.ProjectionReconciliationSnapshot,
		postgres.ProjectionReconciliationCursor,
		int,
	) (postgres.ProjectionReconciliationPage, error)
	FinalizeProjectionReconciliation(
		context.Context,
		postgres.ProjectionReconciliationSnapshot,
	) (postgres.ProjectionReconciliationReport, error)
}

type reconciliationRepositoryFactory func(context.Context, string) (reconciliationRepository, func(), error)

type reconciliationRunResult struct {
	Mode     string
	Report   postgres.ProjectionReconciliationReport
	Repairs  postgres.ProjectionReconciliationRepairs
	Pages    int
	Restarts int
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	result, err := run(ctx, os.Args[1:], os.Getenv, openReconciliationRepository)
	if result.Report.EmbeddingSpace != "" {
		logReconciliationResult(result)
	}
	if err != nil {
		slog.Error("projection reconciler stopped", "error", err)
		os.Exit(1)
	}
}

func run(
	ctx context.Context,
	args []string,
	getenv func(string) string,
	openRepository reconciliationRepositoryFactory,
) (reconciliationRunResult, error) {
	config, err := loadReconcilerProcessConfig(args, getenv)
	if err != nil {
		return reconciliationRunResult{}, err
	}
	if openRepository == nil {
		return reconciliationRunResult{}, errors.New("projection reconciler repository factory is required")
	}

	startupContext, cancelStartup := context.WithTimeout(ctx, reconcilerStartupTimeout)
	repository, closeRepository, err := openRepository(startupContext, config.databaseURL)
	cancelStartup()
	if err != nil {
		return reconciliationRunResult{}, errors.New("open projection reconciliation repository failed")
	}
	if repository == nil || closeRepository == nil {
		if closeRepository != nil {
			closeRepository()
		}
		return reconciliationRunResult{}, errors.New("open projection reconciliation repository failed")
	}
	defer closeRepository()

	result, err := reconcileProjection(ctx, repository, config)
	if err != nil {
		return result, err
	}
	if config.mode == reconcilerModeAudit && !result.Report.Complete {
		return result, errProjectionCoverageIncomplete
	}
	return result, nil
}

func openReconciliationRepository(
	ctx context.Context,
	databaseURL string,
) (reconciliationRepository, func(), error) {
	if err := migrations.Apply(ctx, databaseURL); err != nil {
		return nil, nil, errors.New("apply projection reconciler database migrations failed")
	}
	storage, err := postgres.Open(ctx, databaseURL)
	if err != nil {
		return nil, nil, errors.New("open projection reconciler PostgreSQL store failed")
	}
	return storage, storage.Close, nil
}

func reconcileProjection(
	ctx context.Context,
	repository reconciliationRepository,
	config reconcilerProcessConfig,
) (reconciliationRunResult, error) {
	result := reconciliationRunResult{Mode: config.mode}
	repair := config.mode == reconcilerModeBackfill
	for restart := 0; restart <= maximumGenerationRestarts; restart++ {
		snapshot, err := repository.BeginProjectionReconciliation(ctx, config.embeddingSpace, repair)
		if err != nil {
			return result, errors.New("begin projection reconciliation failed")
		}

		cursor := postgres.ProjectionReconciliationCursor{}
		generationChanged := false
		for pageNumber := 0; pageNumber < maximumReconciliationPages; pageNumber++ {
			page, pageErr := repository.ReconcileProjectionPage(ctx, snapshot, cursor, config.batchSize)
			if errors.Is(pageErr, domain.ErrConflict) {
				generationChanged = true
				break
			}
			if pageErr != nil {
				return result, errors.New("reconcile projection page failed")
			}
			result.Pages++
			addProjectionRepairs(&result.Repairs, page.Repairs)
			if page.Complete {
				if page.NextCursor != nil {
					return result, errors.New("projection reconciliation returned an invalid terminal cursor")
				}
				report, finalizeErr := repository.FinalizeProjectionReconciliation(ctx, snapshot)
				if errors.Is(finalizeErr, domain.ErrConflict) {
					generationChanged = true
					break
				}
				if finalizeErr != nil {
					return result, errors.New("finalize projection reconciliation failed")
				}
				result.Report = report
				return result, nil
			}
			if page.NextCursor == nil || !projectionCursorAdvances(cursor, *page.NextCursor) {
				return result, errors.New("projection reconciliation cursor did not advance")
			}
			cursor = *page.NextCursor
		}
		if !generationChanged {
			return result, errors.New("projection reconciliation exceeded its page limit")
		}
		if restart == maximumGenerationRestarts {
			return result, errors.New("projection deployment changed too often during reconciliation")
		}
		result.Restarts++
	}
	return result, errors.New("projection reconciliation restart limit is invalid")
}

func loadReconcilerProcessConfig(args []string, getenv func(string) string) (reconcilerProcessConfig, error) {
	if len(args) != 0 {
		return reconcilerProcessConfig{}, errors.New("projection reconciler accepts no command-line arguments; configure it with environment variables")
	}
	if getenv == nil {
		return reconcilerProcessConfig{}, errors.New("projection reconciler environment reader is required")
	}
	config := reconcilerProcessConfig{
		databaseURL:    strings.TrimSpace(getenv("DATABASE_URL")),
		embeddingSpace: strings.TrimSpace(getenv("PROJECTION_RECONCILER_EMBEDDING_SPACE")),
		batchSize:      defaultReconciliationBatchSize,
		mode:           reconcilerModeAudit,
	}
	if config.databaseURL == "" {
		return reconcilerProcessConfig{}, errors.New("DATABASE_URL is required")
	}
	if config.embeddingSpace == "" {
		return reconcilerProcessConfig{}, errors.New("PROJECTION_RECONCILER_EMBEDDING_SPACE is required")
	}
	if mode := strings.TrimSpace(getenv("PROJECTION_RECONCILER_MODE")); mode != "" {
		config.mode = mode
	}
	if config.mode != reconcilerModeAudit && config.mode != reconcilerModeBackfill {
		return reconcilerProcessConfig{}, errors.New("PROJECTION_RECONCILER_MODE must be audit or backfill")
	}
	if rawBatchSize := strings.TrimSpace(getenv("PROJECTION_RECONCILER_BATCH_SIZE")); rawBatchSize != "" {
		batchSize, err := strconv.Atoi(rawBatchSize)
		if err != nil || batchSize < minimumReconciliationBatchSize || batchSize > maximumReconciliationBatchSize {
			return reconcilerProcessConfig{}, fmt.Errorf(
				"PROJECTION_RECONCILER_BATCH_SIZE must be an integer between %d and %d",
				minimumReconciliationBatchSize,
				maximumReconciliationBatchSize,
			)
		}
		config.batchSize = batchSize
	}
	return config, nil
}

func projectionCursorAdvances(
	previous postgres.ProjectionReconciliationCursor,
	next postgres.ProjectionReconciliationCursor,
) bool {
	if next.TenantID == "" || next.UserID == "" || next.MemoryID == "" {
		return false
	}
	if previous.TenantID == "" && previous.UserID == "" && previous.MemoryID == "" {
		return true
	}
	if next.TenantID != previous.TenantID {
		return next.TenantID > previous.TenantID
	}
	if next.UserID != previous.UserID {
		return next.UserID > previous.UserID
	}
	return next.MemoryID > previous.MemoryID
}

func addProjectionRepairs(
	total *postgres.ProjectionReconciliationRepairs,
	page postgres.ProjectionReconciliationRepairs,
) {
	total.JobsEnqueued += page.JobsEnqueued
	total.JobsReset += page.JobsReset
	total.EmbeddingsDeleted += page.EmbeddingsDeleted
	total.RevisionsAdvanced += page.RevisionsAdvanced
}

func logReconciliationResult(result reconciliationRunResult) {
	counts := result.Report.Counts
	slog.Info(
		"projection reconciliation complete",
		"mode", result.Mode,
		"embedding_space", result.Report.EmbeddingSpace,
		"generation", result.Report.Generation,
		"checked_at", result.Report.CheckedAt,
		"complete", result.Report.Complete,
		"scanned", counts.Scanned,
		"converged", counts.Converged,
		"missing_job", counts.MissingJob,
		"in_flight", counts.InFlight,
		"dead", counts.Dead,
		"cancelled", counts.Cancelled,
		"succeeded_missing_embedding", counts.SucceededMissingEmbedding,
		"content_hash_mismatch", counts.ContentHashMismatch,
		"version_invariant", counts.VersionInvariant,
		"jobs_enqueued", result.Repairs.JobsEnqueued,
		"jobs_reset", result.Repairs.JobsReset,
		"embeddings_deleted", result.Repairs.EmbeddingsDeleted,
		"revisions_advanced", result.Repairs.RevisionsAdvanced,
		"pages", result.Pages,
		"generation_restarts", result.Restarts,
	)
}
