//go:build integration

package postgres_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/kai443/go-agent-memory-system/internal/domain"
	"github.com/kai443/go-agent-memory-system/internal/store/postgres"
)

func TestProjectionRepositoryRegistersReadsUpdatesAndRetiresTarget(t *testing.T) {
	ctx := context.Background()
	databaseURL := requiredDatabaseURL(t)
	applyMigrations(t, databaseURL)
	storage := openStore(t, databaseURL)
	defer storage.Close()

	space := uniqueProjectionRepositorySpace("target")
	cleanupProjectionRepositorySpaces(t, databaseURL, space)
	command := projectionRepositoryRegistration(space, postgres.ProjectionTargetShadow, false, fixtureTime(1))
	target, err := storage.RegisterProjectionTarget(ctx, command)
	if err != nil {
		t.Fatalf("register target: %v", err)
	}
	if target.Space.ID != space || target.State != postgres.ProjectionTargetShadow || target.EnqueueNew {
		t.Fatalf("registered target=%#v", target)
	}

	// created_at is first-observation provenance, not vector compatibility.
	// Re-registering the same immutable configuration at another observation
	// time is an exact idempotent retry and preserves the original timestamps.
	retry := command
	retry.Space.CreatedAt = command.Space.CreatedAt.Add(time.Hour)
	retry.CreatedAt = command.CreatedAt.Add(time.Hour)
	retried, err := storage.RegisterProjectionTarget(ctx, retry)
	if err != nil {
		t.Fatalf("idempotent registration with new observation time: %v", err)
	}
	if !retried.Space.CreatedAt.Equal(target.Space.CreatedAt) || !retried.CreatedAt.Equal(target.CreatedAt) {
		t.Fatalf("retry changed registry timestamps: before=%#v after=%#v", target, retried)
	}

	conflictingSpace := command
	conflictingSpace.Space.QueryVersion = "raw-query-v2"
	if _, err := storage.RegisterProjectionTarget(ctx, conflictingSpace); !errors.Is(err, domain.ErrConflict) {
		t.Fatalf("configuration drift error=%v, want conflict", err)
	}
	loaded, err := storage.ProjectionTargetBySpace(ctx, space)
	if err != nil || loaded.Space.QueryVersion != command.Space.QueryVersion {
		t.Fatalf("target after drift attempt=%#v error=%v", loaded, err)
	}

	blockedAt := target.UpdatedAt.Add(time.Second)
	blockedCommand := postgres.SetProjectionTargetCommand{
		EmbeddingSpace: space,
		State:          postgres.ProjectionTargetBlocked,
		EnqueueNew:     false,
		UpdatedAt:      blockedAt,
	}
	blocked, err := storage.SetProjectionTarget(ctx, blockedCommand)
	if err != nil || blocked.State != postgres.ProjectionTargetBlocked || blocked.EnqueueNew {
		t.Fatalf("blocked target=%#v error=%v", blocked, err)
	}
	if repeated, err := storage.SetProjectionTarget(ctx, blockedCommand); err != nil || repeated.State != blocked.State {
		t.Fatalf("equal-timestamp idempotent update=%#v error=%v", repeated, err)
	}
	equalTimestampConflict := blockedCommand
	equalTimestampConflict.State = postgres.ProjectionTargetShadow
	equalTimestampConflict.EnqueueNew = true
	if _, err := storage.SetProjectionTarget(ctx, equalTimestampConflict); !errors.Is(err, domain.ErrConflict) {
		t.Fatalf("equal-timestamp conflicting update error=%v, want conflict", err)
	}
	stale := blockedCommand
	stale.UpdatedAt = target.UpdatedAt
	if _, err := storage.SetProjectionTarget(ctx, stale); !errors.Is(err, domain.ErrConflict) {
		t.Fatalf("stale target update error=%v, want conflict", err)
	}

	retiredAt := blockedAt.Add(time.Second)
	retired, err := storage.SetProjectionTarget(ctx, postgres.SetProjectionTargetCommand{
		EmbeddingSpace: space,
		State:          postgres.ProjectionTargetRetired,
		EnqueueNew:     false,
		UpdatedAt:      retiredAt,
	})
	if err != nil || retired.State != postgres.ProjectionTargetRetired || retired.EnqueueNew {
		t.Fatalf("retired target=%#v error=%v", retired, err)
	}
	if _, err := storage.SetProjectionTarget(ctx, postgres.SetProjectionTargetCommand{
		EmbeddingSpace: space,
		State:          postgres.ProjectionTargetShadow,
		EnqueueNew:     true,
		UpdatedAt:      retiredAt.Add(time.Second),
	}); !errors.Is(err, domain.ErrConflict) {
		t.Fatalf("retired reactivation error=%v, want terminal-state conflict", err)
	}

	if _, err := storage.SetProjectionTarget(ctx, postgres.SetProjectionTargetCommand{
		EmbeddingSpace: space,
		State:          postgres.ProjectionTargetBlocked,
		EnqueueNew:     true,
		UpdatedAt:      retiredAt.Add(time.Second),
	}); !errors.Is(err, domain.ErrInvalid) {
		t.Fatalf("blocked enqueue error=%v, want invalid", err)
	}
	if _, err := storage.ProjectionTargetBySpace(ctx, "space_v1_missing_for_repository_test"); !errors.Is(err, domain.ErrNotFound) {
		t.Fatalf("missing target error=%v, want not found", err)
	}
	if _, err := storage.ProjectionJobs(ctx, postgres.ProjectionJobFilter{
		EmbeddingSpace: "space_v1_missing_for_repository_test",
		Limit:          10,
	}); !errors.Is(err, domain.ErrNotFound) {
		t.Fatalf("jobs for missing target error=%v, want not found", err)
	}
	if _, err := storage.ProjectionJobStats(ctx, "space_v1_missing_for_repository_test"); !errors.Is(err, domain.ErrNotFound) {
		t.Fatalf("stats for missing target error=%v, want not found", err)
	}
}

func TestProjectionRepositoryConcurrentRegistrationRejectsSpaceDrift(t *testing.T) {
	ctx := context.Background()
	databaseURL := requiredDatabaseURL(t)
	applyMigrations(t, databaseURL)
	storage := openStore(t, databaseURL)
	defer storage.Close()

	space := uniqueProjectionRepositorySpace("concurrent_drift")
	cleanupProjectionRepositorySpaces(t, databaseURL, space)
	left := projectionRepositoryRegistration(space, postgres.ProjectionTargetShadow, false, fixtureTime(10))
	right := left
	right.Space.DocumentVersion = "memory-card-document-v2"

	type result struct {
		target postgres.ProjectionTarget
		err    error
	}
	start := make(chan struct{})
	results := make(chan result, 2)
	var wait sync.WaitGroup
	for _, command := range []postgres.RegisterProjectionTargetCommand{left, right} {
		command := command
		wait.Add(1)
		go func() {
			defer wait.Done()
			<-start
			target, err := storage.RegisterProjectionTarget(context.Background(), command)
			results <- result{target: target, err: err}
		}()
	}
	close(start)
	wait.Wait()
	close(results)

	var successes, conflicts int
	for result := range results {
		switch {
		case result.err == nil:
			successes++
		case errors.Is(result.err, domain.ErrConflict):
			conflicts++
		default:
			t.Fatalf("unexpected concurrent registration result: target=%#v error=%v", result.target, result.err)
		}
	}
	if successes != 1 || conflicts != 1 {
		t.Fatalf("successes/conflicts=%d/%d, want 1/1", successes, conflicts)
	}
	target, err := storage.ProjectionTargetBySpace(ctx, space)
	if err != nil {
		t.Fatalf("load concurrent winner: %v", err)
	}
	if target.Space.DocumentVersion != left.Space.DocumentVersion && target.Space.DocumentVersion != right.Space.DocumentVersion {
		t.Fatalf("winner has unexpected configuration: %#v", target.Space)
	}
}

func TestProjectionRepositoryAllowsOnlyOneServingTarget(t *testing.T) {
	ctx := context.Background()
	databaseURL := requiredDatabaseURL(t)
	applyMigrations(t, databaseURL)
	storage := openStore(t, databaseURL)
	defer storage.Close()

	secondSpace := uniqueProjectionRepositorySpace("serving_b")
	cleanupProjectionRepositorySpaces(t, databaseURL, secondSpace)
	targets, err := storage.ProjectionTargets(ctx)
	if err != nil {
		t.Fatalf("list existing targets: %v", err)
	}
	hasServing := false
	for _, target := range targets {
		if target.State == postgres.ProjectionTargetServing {
			hasServing = true
			break
		}
	}
	if !hasServing {
		firstSpace := uniqueProjectionRepositorySpace("serving_a")
		cleanupProjectionRepositorySpaces(t, databaseURL, firstSpace)
		firstCommand := projectionRepositoryRegistration(firstSpace, postgres.ProjectionTargetServing, false, fixtureTime(20))
		if _, err := storage.RegisterProjectionTarget(ctx, firstCommand); err != nil {
			t.Fatalf("register first serving target: %v", err)
		}
	}
	secondCommand := projectionRepositoryRegistration(secondSpace, postgres.ProjectionTargetServing, false, fixtureTime(21))
	if _, err := storage.RegisterProjectionTarget(ctx, secondCommand); !errors.Is(err, domain.ErrConflict) {
		t.Fatalf("second serving target error=%v, want conflict", err)
	}
	if _, err := storage.ProjectionTargetBySpace(ctx, secondSpace); !errors.Is(err, domain.ErrNotFound) {
		t.Fatalf("rolled-back second target error=%v, want not found", err)
	}

	conn, err := pgx.Connect(ctx, databaseURL)
	if err != nil {
		t.Fatalf("connect to inspect serving rollback: %v", err)
	}
	defer conn.Close(context.Background())
	var secondSpaceRows int
	if err := conn.QueryRow(ctx, `SELECT count(*) FROM agent_memory.embedding_spaces WHERE id=$1`, secondSpace).Scan(&secondSpaceRows); err != nil {
		t.Fatalf("count rolled-back embedding space: %v", err)
	}
	if secondSpaceRows != 0 {
		t.Fatalf("second serving transaction left %d embedding-space rows, want 0", secondSpaceRows)
	}
}

func TestProjectionRepositoryListsTargetsInStableOrder(t *testing.T) {
	ctx := context.Background()
	databaseURL := requiredDatabaseURL(t)
	applyMigrations(t, databaseURL)
	storage := openStore(t, databaseURL)
	defer storage.Close()

	prefix := fmt.Sprintf("space_repository_list_%d_", time.Now().UnixNano())
	firstSpace, secondSpace := prefix+"a", prefix+"z"
	cleanupProjectionRepositorySpaces(t, databaseURL, firstSpace, secondSpace)
	// Register in reverse lexical order to prove the repository ordering.
	for index, space := range []string{secondSpace, firstSpace} {
		command := projectionRepositoryRegistration(space, postgres.ProjectionTargetShadow, false, fixtureTime(30+index))
		if _, err := storage.RegisterProjectionTarget(ctx, command); err != nil {
			t.Fatalf("register list target %q: %v", space, err)
		}
	}
	targets, err := storage.ProjectionTargets(ctx)
	if err != nil {
		t.Fatalf("list targets: %v", err)
	}
	positions := map[string]int{firstSpace: -1, secondSpace: -1}
	for index, target := range targets {
		if _, tracked := positions[target.Space.ID]; tracked {
			positions[target.Space.ID] = index
		}
	}
	if positions[firstSpace] < 0 || positions[secondSpace] < 0 || positions[firstSpace] >= positions[secondSpace] {
		t.Fatalf("tracked target positions=%v, want first before second", positions)
	}
}

func TestProjectionRepositoryListsFiltersAndCountsJobs(t *testing.T) {
	ctx := context.Background()
	databaseURL := requiredDatabaseURL(t)
	applyMigrations(t, databaseURL)
	storage := openStore(t, databaseURL)
	defer storage.Close()

	space := uniqueProjectionRepositorySpace("jobs")
	cleanupProjectionRepositorySpaces(t, databaseURL, space)
	if _, err := storage.RegisterProjectionTarget(ctx, projectionRepositoryRegistration(
		space,
		postgres.ProjectionTargetShadow,
		false,
		fixtureTime(40),
	)); err != nil {
		t.Fatalf("register job target: %v", err)
	}

	tenantID, userID := uniqueScope("projection_repository_jobs")
	cleanupScopes(t, databaseURL, [][2]string{{tenantID, userID}})
	cards := make([]domain.MemoryCard, 0, 3)
	for index := 0; index < 3; index++ {
		candidate := seedCandidate(
			t,
			storage,
			tenantID,
			userID,
			fmt.Sprintf("projection-repository-job-%d", index),
			fmt.Sprintf("projection_repository_key_%d", index),
			fmt.Sprintf("value-%d", index),
			50+index*3,
		)
		_, card, err := storage.ReviewCandidate(ctx, approval(candidate, fmt.Sprintf("memory-projection-repository-%d", index), 52+index*3))
		if err != nil || card == nil {
			t.Fatalf("approve job fixture %d: card=%#v error=%v", index, card, err)
		}
		cards = append(cards, *card)
	}

	base := time.Now().UTC().Truncate(time.Microsecond).Add(time.Minute)
	conn, err := pgx.Connect(ctx, databaseURL)
	if err != nil {
		t.Fatalf("connect to insert job fixtures: %v", err)
	}
	defer conn.Close(context.Background())
	for index, card := range cards {
		createdAt := base.Add(time.Duration(index) * time.Second)
		state := postgres.ProjectionJobPending
		attemptCount := 0
		var lastErrorCode any
		var lastErrorAt any
		var completedAt any
		switch index {
		case 1:
			state = postgres.ProjectionJobRetry
			attemptCount = 1
			lastErrorCode = "provider_timeout"
			lastErrorAt = createdAt
		case 2:
			state = postgres.ProjectionJobSucceeded
			attemptCount = 1
			completedAt = createdAt
		}
		_, err := conn.Exec(ctx, `
			INSERT INTO agent_memory.embedding_projection_jobs (
				tenant_id, user_id, memory_id, embedding_space,
				expected_memory_version, state, attempt_count, available_at,
				last_error_code, last_error_at, created_at, updated_at, completed_at
			) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $8, $8, $11)`,
			card.TenantID,
			card.UserID,
			card.ID,
			space,
			card.Version,
			string(state),
			attemptCount,
			createdAt,
			lastErrorCode,
			lastErrorAt,
			completedAt,
		)
		if err != nil {
			t.Fatalf("insert %s job fixture: %v", state, err)
		}
	}

	all, err := storage.ProjectionJobs(ctx, postgres.ProjectionJobFilter{EmbeddingSpace: space, Limit: 10})
	if err != nil {
		t.Fatalf("list all jobs: %v", err)
	}
	if len(all) != 3 {
		t.Fatalf("all job count=%d, want 3", len(all))
	}
	for index, job := range all {
		if job.MemoryID != cards[index].ID || job.ExpectedMemoryVersion != cards[index].Version {
			t.Fatalf("job %d=%#v, want memory=%s version=%d", index, job, cards[index].ID, cards[index].Version)
		}
	}
	if all[1].LastErrorCode != "provider_timeout" || all[1].LastErrorAt == nil {
		t.Fatalf("retry job has unexpected stable error fields: %#v", all[1])
	}
	if all[2].CompletedAt == nil {
		t.Fatalf("succeeded job has no completion timestamp: %#v", all[2])
	}

	runnable, err := storage.ProjectionJobs(ctx, postgres.ProjectionJobFilter{
		EmbeddingSpace: space,
		TenantID:       tenantID,
		UserID:         userID,
		States:         []postgres.ProjectionJobState{postgres.ProjectionJobPending, postgres.ProjectionJobRetry},
		Limit:          2,
	})
	if err != nil || len(runnable) != 2 || runnable[0].State != postgres.ProjectionJobPending || runnable[1].State != postgres.ProjectionJobRetry {
		t.Fatalf("runnable jobs=%#v error=%v", runnable, err)
	}
	limited, err := storage.ProjectionJobs(ctx, postgres.ProjectionJobFilter{EmbeddingSpace: space, Limit: 1})
	if err != nil || len(limited) != 1 || limited[0].MemoryID != cards[0].ID {
		t.Fatalf("limited jobs=%#v error=%v", limited, err)
	}
	otherScope, err := storage.ProjectionJobs(ctx, postgres.ProjectionJobFilter{
		EmbeddingSpace: space,
		TenantID:       tenantID + "-other",
		UserID:         userID + "-other",
		Limit:          10,
	})
	if err != nil || len(otherScope) != 0 {
		t.Fatalf("other-scope jobs=%#v error=%v, want empty", otherScope, err)
	}

	statistics, err := storage.ProjectionJobStats(ctx, space)
	if err != nil {
		t.Fatalf("load job statistics: %v", err)
	}
	if statistics.Total != 3 ||
		statistics.Pending != 1 ||
		statistics.Retry != 1 ||
		statistics.Succeeded != 1 ||
		statistics.Leased != 0 ||
		statistics.Dead != 0 ||
		statistics.Cancelled != 0 ||
		statistics.OldestRunnable == nil ||
		statistics.LastUpdatedAt == nil {
		t.Fatalf("job statistics=%#v", statistics)
	}
}

func projectionRepositoryRegistration(
	embeddingSpace string,
	state postgres.ProjectionTargetState,
	enqueueNew bool,
	createdAt time.Time,
) postgres.RegisterProjectionTargetCommand {
	return postgres.RegisterProjectionTargetCommand{
		Space: postgres.EmbeddingSpaceDefinition{
			ID:               embeddingSpace,
			Provider:         "lmstudio",
			Model:            "text-embedding-bge-m3",
			Dimension:        postgres.VectorDimension,
			DocumentVersion:  "memory-card-document-v1",
			QueryVersion:     "raw-query-v1",
			ModelFingerprint: strings.Repeat("d", 64),
			CreatedAt:        createdAt,
		},
		State:      state,
		EnqueueNew: enqueueNew,
		CreatedAt:  createdAt,
	}
}

func uniqueProjectionRepositorySpace(label string) string {
	return fmt.Sprintf("space_repository_%s_%d", label, time.Now().UnixNano())
}

func cleanupProjectionRepositorySpaces(t *testing.T, databaseURL string, embeddingSpaces ...string) {
	t.Helper()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		conn, err := pgx.Connect(ctx, databaseURL)
		if err != nil {
			t.Errorf("connect to clean projection repository spaces: %v", err)
			return
		}
		defer conn.Close(context.Background())
		tx, err := conn.Begin(ctx)
		if err != nil {
			t.Errorf("begin projection repository cleanup: %v", err)
			return
		}
		for _, embeddingSpace := range embeddingSpaces {
			for _, statement := range []string{
				"DELETE FROM agent_memory.embedding_projection_targets WHERE embedding_space=$1",
				"DELETE FROM agent_memory.embedding_projection_jobs WHERE embedding_space=$1",
				"DELETE FROM agent_memory.memory_embeddings WHERE embedding_space=$1",
				"DELETE FROM agent_memory.embedding_spaces WHERE id=$1",
			} {
				if _, err := tx.Exec(ctx, statement, embeddingSpace); err != nil {
					_ = tx.Rollback(ctx)
					t.Errorf("clean projection repository space: %v", err)
					return
				}
			}
		}
		if err := tx.Commit(ctx); err != nil {
			t.Errorf("commit projection repository cleanup: %v", err)
		}
	})
}
