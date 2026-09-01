package main

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/ksana-ai/agent-memory-system/internal/domain"
	"github.com/ksana-ai/agent-memory-system/internal/store/postgres"
)

func TestLoadReconcilerProcessConfigUsesAuditDefaults(t *testing.T) {
	environment := map[string]string{
		"DATABASE_URL":                          "postgres://reconciler:secret@database.invalid/memory",
		"PROJECTION_RECONCILER_EMBEDDING_SPACE": "space_v1_expected",
	}
	config, err := loadReconcilerProcessConfig(nil, reconciliationEnvironment(environment))
	if err != nil {
		t.Fatalf("load reconciler config: %v", err)
	}
	if config.databaseURL != environment["DATABASE_URL"] ||
		config.embeddingSpace != environment["PROJECTION_RECONCILER_EMBEDDING_SPACE"] ||
		config.batchSize != defaultReconciliationBatchSize ||
		config.mode != reconcilerModeAudit {
		t.Fatalf("config=%#v", config)
	}
}

func TestLoadReconcilerProcessConfigRejectsUnsafeOrInvalidInputWithoutLeakingValues(t *testing.T) {
	const secret = "TOP_SECRET_RECONCILER_CONFIG"
	base := map[string]string{
		"DATABASE_URL":                          "postgres://reconciler:" + secret + "@database.invalid/memory",
		"PROJECTION_RECONCILER_EMBEDDING_SPACE": "space_" + secret,
	}
	tests := []struct {
		name        string
		args        []string
		environment map[string]string
	}{
		{name: "arguments", args: []string{"-database-url=" + base["DATABASE_URL"]}, environment: base},
		{name: "mode", environment: withReconciliationEnvironment(base, "PROJECTION_RECONCILER_MODE", secret)},
		{name: "batch text", environment: withReconciliationEnvironment(base, "PROJECTION_RECONCILER_BATCH_SIZE", secret)},
		{name: "batch too small", environment: withReconciliationEnvironment(base, "PROJECTION_RECONCILER_BATCH_SIZE", "0")},
		{name: "batch too large", environment: withReconciliationEnvironment(base, "PROJECTION_RECONCILER_BATCH_SIZE", "501")},
		{name: "missing database", environment: withReconciliationEnvironment(base, "DATABASE_URL", "")},
		{name: "missing space", environment: withReconciliationEnvironment(base, "PROJECTION_RECONCILER_EMBEDDING_SPACE", "")},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := loadReconcilerProcessConfig(test.args, reconciliationEnvironment(test.environment))
			if err == nil {
				t.Fatal("invalid reconciliation config was accepted")
			}
			if strings.Contains(err.Error(), secret) || strings.Contains(err.Error(), "reconciler:TOP_SECRET") {
				t.Fatalf("configuration error leaked a secret: %v", err)
			}
		})
	}
}

func TestAuditModeIsReadOnlyAndFailsWithFixedIncompleteError(t *testing.T) {
	var repairObserved bool
	repository := &fakeReconciliationRepository{}
	repository.begin = func(_ context.Context, space string, repair bool) (postgres.ProjectionReconciliationSnapshot, error) {
		if space != "space_v1_audit" {
			t.Fatalf("space=%q", space)
		}
		repairObserved = repair
		return reconciliationSnapshot(space, 7, repair), nil
	}
	repository.page = func(
		_ context.Context,
		_ postgres.ProjectionReconciliationSnapshot,
		cursor postgres.ProjectionReconciliationCursor,
		limit int,
	) (postgres.ProjectionReconciliationPage, error) {
		if cursor != (postgres.ProjectionReconciliationCursor{}) || limit != 23 {
			t.Fatalf("cursor=%#v limit=%d", cursor, limit)
		}
		return postgres.ProjectionReconciliationPage{Complete: true}, nil
	}
	repository.finalize = func(
		_ context.Context,
		snapshot postgres.ProjectionReconciliationSnapshot,
	) (postgres.ProjectionReconciliationReport, error) {
		return postgres.ProjectionReconciliationReport{
			EmbeddingSpace: snapshot.EmbeddingSpace,
			Generation:     snapshot.Generation,
			CheckedAt:      time.Now().UTC(),
			Counts: postgres.ProjectionReconciliationCounts{
				Scanned:    1,
				MissingJob: 1,
			},
			Complete: false,
		}, nil
	}
	closed := false
	result, err := run(
		context.Background(),
		nil,
		reconciliationEnvironment(map[string]string{
			"DATABASE_URL":                          "postgres://unused.invalid/memory",
			"PROJECTION_RECONCILER_EMBEDDING_SPACE": "space_v1_audit",
			"PROJECTION_RECONCILER_BATCH_SIZE":      "23",
			"PROJECTION_RECONCILER_MODE":            reconcilerModeAudit,
		}),
		func(context.Context, string) (reconciliationRepository, func(), error) {
			return repository, func() { closed = true }, nil
		},
	)
	if !errors.Is(err, errProjectionCoverageIncomplete) {
		t.Fatalf("error=%v", err)
	}
	if repairObserved {
		t.Fatal("audit mode requested a repairing snapshot")
	}
	if !closed || repository.pageCalls != 1 || repository.finalizeCalls != 1 || result.Report.Complete {
		t.Fatalf("closed=%t page calls=%d finalize calls=%d result=%#v", closed, repository.pageCalls, repository.finalizeCalls, result)
	}
	if result.Repairs != (postgres.ProjectionReconciliationRepairs{}) {
		t.Fatalf("audit reported repairs: %#v", result.Repairs)
	}
}

func TestBackfillRestartsFromTheBeginningAfterGenerationChange(t *testing.T) {
	beginCalls := 0
	var cursors []postgres.ProjectionReconciliationCursor
	repository := &fakeReconciliationRepository{}
	repository.begin = func(_ context.Context, space string, repair bool) (postgres.ProjectionReconciliationSnapshot, error) {
		if !repair {
			t.Fatal("backfill did not request repair mode")
		}
		beginCalls++
		return reconciliationSnapshot(space, int64(beginCalls), repair), nil
	}
	repository.page = func(
		_ context.Context,
		snapshot postgres.ProjectionReconciliationSnapshot,
		cursor postgres.ProjectionReconciliationCursor,
		_ int,
	) (postgres.ProjectionReconciliationPage, error) {
		cursors = append(cursors, cursor)
		if snapshot.Generation == 1 && len(cursors) == 1 {
			next := postgres.ProjectionReconciliationCursor{TenantID: "tenant-a", UserID: "user-a", MemoryID: "memory-a"}
			return postgres.ProjectionReconciliationPage{
				Repairs:    postgres.ProjectionReconciliationRepairs{JobsEnqueued: 1},
				NextCursor: &next,
			}, nil
		}
		if snapshot.Generation == 1 {
			return postgres.ProjectionReconciliationPage{}, domain.ErrConflict
		}
		if cursor != (postgres.ProjectionReconciliationCursor{}) {
			t.Fatalf("restart resumed from a content cursor: %#v", cursor)
		}
		return postgres.ProjectionReconciliationPage{
			Repairs:  postgres.ProjectionReconciliationRepairs{JobsEnqueued: 2},
			Complete: true,
		}, nil
	}
	repository.finalize = func(
		_ context.Context,
		snapshot postgres.ProjectionReconciliationSnapshot,
	) (postgres.ProjectionReconciliationReport, error) {
		return postgres.ProjectionReconciliationReport{
			EmbeddingSpace: snapshot.EmbeddingSpace,
			Generation:     snapshot.Generation,
			Complete:       false,
		}, nil
	}
	result, err := reconcileProjection(context.Background(), repository, reconcilerProcessConfig{
		embeddingSpace: "space_v1_backfill",
		batchSize:      1,
		mode:           reconcilerModeBackfill,
	})
	if err != nil {
		t.Fatalf("backfill: %v", err)
	}
	if beginCalls != 2 || result.Restarts != 1 || result.Pages != 2 || result.Repairs.JobsEnqueued != 3 {
		t.Fatalf("begin=%d result=%#v", beginCalls, result)
	}
	if len(cursors) != 3 || cursors[0] != (postgres.ProjectionReconciliationCursor{}) ||
		cursors[2] != (postgres.ProjectionReconciliationCursor{}) {
		t.Fatalf("cursors=%#v", cursors)
	}
}

func TestAuditRestartsWhenGenerationChangesDuringFinalReport(t *testing.T) {
	beginCalls := 0
	var cursors []postgres.ProjectionReconciliationCursor
	repository := &fakeReconciliationRepository{}
	repository.begin = func(_ context.Context, space string, repair bool) (postgres.ProjectionReconciliationSnapshot, error) {
		beginCalls++
		return reconciliationSnapshot(space, int64(beginCalls), repair), nil
	}
	repository.page = func(
		_ context.Context,
		_ postgres.ProjectionReconciliationSnapshot,
		cursor postgres.ProjectionReconciliationCursor,
		_ int,
	) (postgres.ProjectionReconciliationPage, error) {
		cursors = append(cursors, cursor)
		return postgres.ProjectionReconciliationPage{Complete: true}, nil
	}
	repository.finalize = func(
		_ context.Context,
		snapshot postgres.ProjectionReconciliationSnapshot,
	) (postgres.ProjectionReconciliationReport, error) {
		if snapshot.Generation == 1 {
			return postgres.ProjectionReconciliationReport{}, domain.ErrConflict
		}
		return postgres.ProjectionReconciliationReport{
			EmbeddingSpace: snapshot.EmbeddingSpace,
			Generation:     snapshot.Generation,
			Complete:       true,
		}, nil
	}
	result, err := reconcileProjection(context.Background(), repository, reconcilerProcessConfig{
		embeddingSpace: "space_v1_finalize_restart",
		batchSize:      10,
		mode:           reconcilerModeAudit,
	})
	if err != nil {
		t.Fatalf("audit: %v", err)
	}
	if beginCalls != 2 || result.Restarts != 1 || result.Report.Generation != 2 || len(cursors) != 2 {
		t.Fatalf("begin=%d cursors=%#v result=%#v", beginCalls, cursors, result)
	}
	for _, cursor := range cursors {
		if cursor != (postgres.ProjectionReconciliationCursor{}) {
			t.Fatalf("finalize restart retained cursor %#v", cursor)
		}
	}
}

func TestReconciliationGenerationRestartLimitFailsClosedWithoutLeakingSpace(t *testing.T) {
	const secret = "TOP_SECRET_SPACE"
	repository := &fakeReconciliationRepository{
		begin: func(_ context.Context, space string, repair bool) (postgres.ProjectionReconciliationSnapshot, error) {
			return reconciliationSnapshot(space, 1, repair), nil
		},
		page: func(
			context.Context,
			postgres.ProjectionReconciliationSnapshot,
			postgres.ProjectionReconciliationCursor,
			int,
		) (postgres.ProjectionReconciliationPage, error) {
			return postgres.ProjectionReconciliationPage{}, domain.ErrConflict
		},
	}
	result, err := reconcileProjection(context.Background(), repository, reconcilerProcessConfig{
		embeddingSpace: "space_" + secret,
		batchSize:      10,
		mode:           reconcilerModeBackfill,
	})
	if err == nil {
		t.Fatal("generation livelock was accepted")
	}
	if strings.Contains(err.Error(), secret) || repository.beginCalls != maximumGenerationRestarts+1 ||
		result.Restarts != maximumGenerationRestarts {
		t.Fatalf("error=%v begin calls=%d result=%#v", err, repository.beginCalls, result)
	}
}

func TestProjectionReconciliationRejectsNonAdvancingCursor(t *testing.T) {
	repository := &fakeReconciliationRepository{
		begin: func(_ context.Context, space string, repair bool) (postgres.ProjectionReconciliationSnapshot, error) {
			return reconciliationSnapshot(space, 1, repair), nil
		},
		page: func(
			context.Context,
			postgres.ProjectionReconciliationSnapshot,
			postgres.ProjectionReconciliationCursor,
			int,
		) (postgres.ProjectionReconciliationPage, error) {
			next := postgres.ProjectionReconciliationCursor{TenantID: "tenant-only"}
			return postgres.ProjectionReconciliationPage{NextCursor: &next}, nil
		},
	}
	_, err := reconcileProjection(context.Background(), repository, reconcilerProcessConfig{
		embeddingSpace: "space_v1_cursor",
		batchSize:      10,
		mode:           reconcilerModeAudit,
	})
	if err == nil || !strings.Contains(err.Error(), "cursor did not advance") {
		t.Fatalf("error=%v", err)
	}
}

func TestReconcilerSuppressesRepositoryAndConnectionDetails(t *testing.T) {
	const secret = "TOP_SECRET_CARD_OR_DATABASE_DETAIL"
	baseEnvironment := reconciliationEnvironment(map[string]string{
		"DATABASE_URL":                          "postgres://reconciler:" + secret + "@database.invalid/memory",
		"PROJECTION_RECONCILER_EMBEDDING_SPACE": "space_v1_redaction",
		"PROJECTION_RECONCILER_MODE":            reconcilerModeAudit,
	})
	_, err := run(
		context.Background(),
		nil,
		baseEnvironment,
		func(context.Context, string) (reconciliationRepository, func(), error) {
			return nil, nil, errors.New("open failed with " + secret)
		},
	)
	if err == nil || strings.Contains(err.Error(), secret) {
		t.Fatalf("connection error=%v", err)
	}

	repository := &fakeReconciliationRepository{
		begin: func(_ context.Context, space string, repair bool) (postgres.ProjectionReconciliationSnapshot, error) {
			return reconciliationSnapshot(space, 1, repair), nil
		},
		page: func(
			context.Context,
			postgres.ProjectionReconciliationSnapshot,
			postgres.ProjectionReconciliationCursor,
			int,
		) (postgres.ProjectionReconciliationPage, error) {
			return postgres.ProjectionReconciliationPage{}, errors.New("card content " + secret)
		},
	}
	_, err = run(
		context.Background(),
		nil,
		baseEnvironment,
		func(context.Context, string) (reconciliationRepository, func(), error) {
			return repository, func() {}, nil
		},
	)
	if err == nil || strings.Contains(err.Error(), secret) {
		t.Fatalf("repository error=%v", err)
	}
}

func TestReconciliationSummaryContainsOnlyAggregateFields(t *testing.T) {
	const (
		secretScope   = "tenant-sensitive user-sensitive"
		secretCard    = "memory-sensitive"
		secretContent = "sensitive memory content"
	)
	var output bytes.Buffer
	previous := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&output, nil)))
	t.Cleanup(func() { slog.SetDefault(previous) })
	logReconciliationResult(reconciliationRunResult{
		Mode: reconcilerModeAudit,
		Report: postgres.ProjectionReconciliationReport{
			EmbeddingSpace: "space_v1_safe",
			Generation:     9,
			Counts:         postgres.ProjectionReconciliationCounts{Scanned: 3, Converged: 3},
			Complete:       true,
		},
	})
	for _, forbidden := range []string{secretScope, secretCard, secretContent, "DATABASE_URL"} {
		if strings.Contains(output.String(), forbidden) {
			t.Fatalf("aggregate summary leaked %q: %s", forbidden, output.String())
		}
	}
}

func TestReconcilerMakeTargetsKeepDatabaseURLOutOfArguments(t *testing.T) {
	const databaseURL = "postgres://reconciler:TOP_SECRET_DATABASE@db.invalid/memory"
	for _, target := range []string{
		"build-projection-reconciler",
		"projection-backfill",
		"projection-reconcile",
		"test-reconciliation-integration",
		"verify-reconciliation",
	} {
		command := exec.Command(
			"make", "-n", target,
			"DATABASE_URL="+databaseURL,
			"TEST_DATABASE_URL="+databaseURL,
			"PROJECTION_RECONCILER_EMBEDDING_SPACE=space_v1_expected",
		)
		command.Dir = filepath.Join("..", "..")
		output, err := command.CombinedOutput()
		if err != nil {
			t.Fatalf("make -n %s: %v\n%s", target, err, output)
		}
		if strings.Contains(string(output), databaseURL) || strings.Contains(string(output), "TOP_SECRET_DATABASE") {
			t.Fatalf("%s leaked database configuration: %s", target, output)
		}
		if strings.Contains(string(output), "-database-url") {
			t.Fatalf("%s put database configuration in process arguments: %s", target, output)
		}
	}
}

type fakeReconciliationRepository struct {
	begin    func(context.Context, string, bool) (postgres.ProjectionReconciliationSnapshot, error)
	page     func(context.Context, postgres.ProjectionReconciliationSnapshot, postgres.ProjectionReconciliationCursor, int) (postgres.ProjectionReconciliationPage, error)
	finalize func(context.Context, postgres.ProjectionReconciliationSnapshot) (postgres.ProjectionReconciliationReport, error)

	beginCalls    int
	pageCalls     int
	finalizeCalls int
}

func (repository *fakeReconciliationRepository) BeginProjectionReconciliation(
	ctx context.Context,
	embeddingSpace string,
	repair bool,
) (postgres.ProjectionReconciliationSnapshot, error) {
	repository.beginCalls++
	if repository.begin == nil {
		return postgres.ProjectionReconciliationSnapshot{}, errors.New("unexpected begin")
	}
	return repository.begin(ctx, embeddingSpace, repair)
}

func (repository *fakeReconciliationRepository) ReconcileProjectionPage(
	ctx context.Context,
	snapshot postgres.ProjectionReconciliationSnapshot,
	cursor postgres.ProjectionReconciliationCursor,
	limit int,
) (postgres.ProjectionReconciliationPage, error) {
	repository.pageCalls++
	if repository.page == nil {
		return postgres.ProjectionReconciliationPage{}, errors.New("unexpected page")
	}
	return repository.page(ctx, snapshot, cursor, limit)
}

func (repository *fakeReconciliationRepository) FinalizeProjectionReconciliation(
	ctx context.Context,
	snapshot postgres.ProjectionReconciliationSnapshot,
) (postgres.ProjectionReconciliationReport, error) {
	repository.finalizeCalls++
	if repository.finalize == nil {
		return postgres.ProjectionReconciliationReport{}, errors.New("unexpected finalize")
	}
	return repository.finalize(ctx, snapshot)
}

func reconciliationSnapshot(
	space string,
	generation int64,
	repair bool,
) postgres.ProjectionReconciliationSnapshot {
	return postgres.ProjectionReconciliationSnapshot{
		EmbeddingSpace: space,
		Generation:     generation,
		StartedAt:      time.Now().UTC(),
		Repair:         repair,
	}
}

func reconciliationEnvironment(values map[string]string) func(string) string {
	return func(key string) string { return values[key] }
}

func withReconciliationEnvironment(source map[string]string, key, value string) map[string]string {
	result := make(map[string]string, len(source)+1)
	for currentKey, currentValue := range source {
		result[currentKey] = currentValue
	}
	result[key] = value
	return result
}

var _ reconciliationRepository = (*fakeReconciliationRepository)(nil)
