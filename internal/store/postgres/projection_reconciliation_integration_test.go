//go:build integration

package postgres_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/ksana-ai/agent-memory-system/internal/domain"
	"github.com/ksana-ai/agent-memory-system/internal/embedding"
	"github.com/ksana-ai/agent-memory-system/internal/store/postgres"
)

func TestProjectionReconciliationBackfillsInBoundedIdempotentPages(t *testing.T) {
	ctx := context.Background()
	databaseURL := requiredDatabaseURL(t)
	applyMigrations(t, databaseURL)
	storage := openStore(t, databaseURL)
	defer storage.Close()

	baseline := countServiceableProjectionCards(t, databaseURL)
	space := registerReconciliationTarget(t, storage, databaseURL, "backfill", postgres.ProjectionTargetShadow, true)

	scopes := make([][2]string, 0, 4)
	for index := 0; index < 3; index++ {
		tenantID, userID := uniqueScope(fmt.Sprintf("reconcile_page_%d", index))
		scopes = append(scopes, [2]string{tenantID, userID})
		_ = approveVectorCard(
			t,
			storage,
			tenantID,
			userID,
			fmt.Sprintf("reconcile-page-%d", index),
			fmt.Sprintf("key-%d", index),
			fmt.Sprintf("value-%d", index),
			10+index*3,
			12+index*3,
			nil,
		)
	}
	expiredTenant, expiredUser := uniqueScope("reconcile_expired")
	scopes = append(scopes, [2]string{expiredTenant, expiredUser})
	expiredAt := time.Now().UTC().Add(-time.Hour).Truncate(time.Microsecond)
	expiredCard := approveVectorCard(
		t,
		storage,
		expiredTenant,
		expiredUser,
		"reconcile-expired",
		"expired-key",
		"expired-value",
		30,
		32,
		&expiredAt,
	)
	cleanupScopes(t, databaseURL, scopes)

	execReconciliationSQL(t, databaseURL, `
		DELETE FROM agent_memory.embedding_projection_jobs
		WHERE embedding_space = $1`, space)

	snapshot, err := storage.BeginProjectionReconciliation(ctx, space, true)
	if err != nil {
		t.Fatalf("begin repair reconciliation: %v", err)
	}
	wantServiceable := baseline + 3
	first := runProjectionReconciliationPass(t, storage, snapshot, 2)
	if first.Counts.Scanned != wantServiceable || first.Counts.MissingJob != wantServiceable ||
		first.Counts.Converged != 0 || first.Counts.InFlight != 0 {
		t.Fatalf("first reconciliation counts=%#v, want %d missing jobs", first.Counts, wantServiceable)
	}
	if first.Repairs.JobsEnqueued != wantServiceable || first.Repairs.JobsReset != 0 ||
		first.Repairs.EmbeddingsDeleted != 0 {
		t.Fatalf("first reconciliation repairs=%#v, want %d enqueued", first.Repairs, wantServiceable)
	}
	if countProjectionJobForCardAndSpace(t, databaseURL, expiredTenant, expiredUser, expiredCard.ID, space) != 0 {
		t.Fatal("expired card received a reconciliation job")
	}

	second := runProjectionReconciliationPass(t, storage, snapshot, 2)
	if second.Counts.Scanned != wantServiceable || second.Counts.InFlight != wantServiceable {
		t.Fatalf("second reconciliation counts=%#v, want %d in flight", second.Counts, wantServiceable)
	}
	if second.Repairs != (postgres.ProjectionReconciliationRepairs{}) {
		t.Fatalf("idempotent reconciliation performed repairs: %#v", second.Repairs)
	}
	report, err := storage.FinalizeProjectionReconciliation(ctx, snapshot)
	if err != nil {
		t.Fatalf("finalize pending reconciliation: %v", err)
	}
	if report.Complete || report.Counts.Scanned != wantServiceable || report.Counts.InFlight != wantServiceable {
		t.Fatalf("pending coverage report=%#v", report)
	}
}

func TestProjectionReconciliationRepairsStaleServingRowsAndPreservesDeadBlocker(t *testing.T) {
	ctx := context.Background()
	databaseURL := requiredDatabaseURL(t)
	applyMigrations(t, databaseURL)
	assertNoServingProjectionTarget(t, databaseURL)
	storage := openStore(t, databaseURL)
	defer storage.Close()
	space := registerReconciliationTarget(t, storage, databaseURL, "serving_repair", postgres.ProjectionTargetServing, true)
	target, err := storage.ProjectionTargetBySpace(ctx, space)
	if err != nil {
		t.Fatalf("load serving reconciliation target: %v", err)
	}

	tenantID, userID := uniqueScope("reconcile_serving")
	cleanupScopes(t, databaseURL, [][2]string{{tenantID, userID}})
	staleCard := approveVectorCard(t, storage, tenantID, userID, "reconcile-stale", "key-stale", "stale-current-value", 40, 42, nil)
	missingCard := approveVectorCard(t, storage, tenantID, userID, "reconcile-missing", "key-missing", "missing-value", 44, 46, nil)
	deadCard := approveVectorCard(t, storage, tenantID, userID, "reconcile-dead", "key-dead", "dead-value", 48, 50, nil)

	markProjectionJobState(t, databaseURL, tenantID, userID, staleCard.ID, space, "succeeded")
	markProjectionJobState(t, databaseURL, tenantID, userID, missingCard.ID, space, "succeeded")
	markProjectionJobState(t, databaseURL, tenantID, userID, deadCard.ID, space, "dead")
	if err := storage.UpsertMemoryEmbedding(ctx, reconciliationEmbedding(
		staleCard,
		target.Space,
		strings.Repeat("f", 64),
	)); err != nil {
		t.Fatalf("insert stale serving embedding: %v", err)
	}
	revisionBefore, err := storage.ContextRevision(ctx, tenantID, userID)
	if err != nil {
		t.Fatalf("load revision before reconciliation: %v", err)
	}

	snapshot, err := storage.BeginProjectionReconciliation(ctx, space, true)
	if err != nil {
		t.Fatalf("begin serving repair: %v", err)
	}
	page, err := storage.ReconcileProjectionPage(ctx, snapshot, postgres.ProjectionReconciliationCursor{
		TenantID: tenantID,
		UserID:   userID,
		MemoryID: "!",
	}, 3)
	if err != nil {
		t.Fatalf("repair serving page: %v", err)
	}
	if page.Counts.Scanned != 3 || page.Counts.Dead != 1 ||
		page.Counts.SucceededMissingEmbedding != 1 || page.Counts.ContentHashMismatch != 1 {
		t.Fatalf("serving repair counts=%#v", page.Counts)
	}
	if page.Repairs.JobsReset != 2 || page.Repairs.EmbeddingsDeleted != 1 ||
		page.Repairs.RevisionsAdvanced != 1 {
		t.Fatalf("serving repair actions=%#v", page.Repairs)
	}
	revisionAfter, err := storage.ContextRevision(ctx, tenantID, userID)
	if err != nil || revisionAfter != revisionBefore+1 {
		t.Fatalf("revision after repair=%d error=%v, want %d", revisionAfter, err, revisionBefore+1)
	}
	assertProjectionJobState(t, databaseURL, tenantID, userID, staleCard.ID, space, "pending", 0)
	assertProjectionJobState(t, databaseURL, tenantID, userID, missingCard.ID, space, "pending", 0)
	assertProjectionJobState(t, databaseURL, tenantID, userID, deadCard.ID, space, "dead", 1)
	if countProjectionEmbeddingForCardAndSpace(t, databaseURL, tenantID, userID, staleCard.ID, space) != 0 {
		t.Fatal("stale serving embedding survived reconciliation")
	}

	report, err := storage.FinalizeProjectionReconciliation(ctx, snapshot)
	if err != nil {
		t.Fatalf("finalize serving repair: %v", err)
	}
	if report.Complete || report.Counts.Dead < 1 || report.Counts.InFlight < 2 {
		t.Fatalf("serving blocker report=%#v", report)
	}
}

func TestProjectionReconciliationFencesGenerationAndRepairEligibility(t *testing.T) {
	ctx := context.Background()
	databaseURL := requiredDatabaseURL(t)
	applyMigrations(t, databaseURL)
	storage := openStore(t, databaseURL)
	defer storage.Close()
	space := registerReconciliationTarget(t, storage, databaseURL, "generation", postgres.ProjectionTargetShadow, true)

	snapshot, err := storage.BeginProjectionReconciliation(ctx, space, false)
	if err != nil {
		t.Fatalf("begin generation snapshot: %v", err)
	}
	target, err := storage.ProjectionTargetBySpace(ctx, space)
	if err != nil {
		t.Fatalf("load generation target: %v", err)
	}
	if _, err := storage.SetProjectionTarget(ctx, postgres.SetProjectionTargetCommand{
		EmbeddingSpace: space,
		State:          postgres.ProjectionTargetBlocked,
		EnqueueNew:     false,
		UpdatedAt:      target.UpdatedAt.Add(time.Microsecond),
	}); err != nil {
		t.Fatalf("block reconciliation target: %v", err)
	}
	if _, err := storage.ReconcileProjectionPage(ctx, snapshot, postgres.ProjectionReconciliationCursor{}, 1); !errors.Is(err, domain.ErrConflict) {
		t.Fatalf("stale generation page error=%v, want conflict", err)
	}
	if _, err := storage.FinalizeProjectionReconciliation(ctx, snapshot); !errors.Is(err, domain.ErrConflict) {
		t.Fatalf("stale generation finalization error=%v, want conflict", err)
	}
	if _, err := storage.BeginProjectionReconciliation(ctx, space, true); !errors.Is(err, domain.ErrConflict) {
		t.Fatalf("blocked repair begin error=%v, want conflict", err)
	}
	audit, err := storage.BeginProjectionReconciliation(ctx, space, false)
	if err != nil {
		t.Fatalf("blocked audit begin: %v", err)
	}
	if _, err := storage.ReconcileProjectionPage(ctx, audit, postgres.ProjectionReconciliationCursor{}, 1); err != nil {
		t.Fatalf("blocked audit page: %v", err)
	}
}

func TestProjectionReconciliationVersionMismatchFailsPageAndFinalReportCountsIt(t *testing.T) {
	ctx := context.Background()
	databaseURL := requiredDatabaseURL(t)
	applyMigrations(t, databaseURL)
	storage := openStore(t, databaseURL)
	defer storage.Close()
	space := registerReconciliationTarget(t, storage, databaseURL, "version_invariant", postgres.ProjectionTargetShadow, true)
	tenantID, userID := uniqueScope("reconcile_version")
	cleanupScopes(t, databaseURL, [][2]string{{tenantID, userID}})
	card := approveVectorCard(t, storage, tenantID, userID, "reconcile-version", "key-version", "value-version", 60, 62, nil)
	execReconciliationSQL(t, databaseURL, `
		UPDATE agent_memory.embedding_projection_jobs
		SET expected_memory_version = expected_memory_version + 1
		WHERE tenant_id = $1 AND user_id = $2 AND memory_id = $3 AND embedding_space = $4`,
		tenantID, userID, card.ID, space)

	snapshot, err := storage.BeginProjectionReconciliation(ctx, space, true)
	if err != nil {
		t.Fatalf("begin version invariant reconciliation: %v", err)
	}
	_, err = storage.ReconcileProjectionPage(ctx, snapshot, postgres.ProjectionReconciliationCursor{
		TenantID: tenantID,
		UserID:   userID,
		MemoryID: "!",
	}, 1)
	if !errors.Is(err, domain.ErrInvariant) {
		t.Fatalf("version mismatch page error=%v, want invariant", err)
	}
	report, err := storage.FinalizeProjectionReconciliation(ctx, snapshot)
	if err != nil {
		t.Fatalf("finalize version invariant report: %v", err)
	}
	if report.Complete || report.Counts.VersionInvariant < 1 {
		t.Fatalf("version invariant report=%#v", report)
	}
	assertProjectionJobExpectedVersion(t, databaseURL, tenantID, userID, card.ID, space, card.Version+1)
}

func TestProjectionReconciliationSerializesWithForgetAndSupersedeWithoutResurrection(t *testing.T) {
	ctx := context.Background()
	databaseURL := requiredDatabaseURL(t)
	applyMigrations(t, databaseURL)
	storage := openStore(t, databaseURL)
	defer storage.Close()
	space := registerReconciliationTarget(t, storage, databaseURL, "lifecycle_race", postgres.ProjectionTargetShadow, true)
	snapshot, err := storage.BeginProjectionReconciliation(ctx, space, true)
	if err != nil {
		t.Fatalf("begin lifecycle race reconciliation: %v", err)
	}

	sequence := scopeSequence.Add(1)
	advisoryKey := int64(1_700_000_000 + sequence%100_000_000)
	installProjectionReconciliationInsertBarrier(t, databaseURL, space, advisoryKey, sequence)
	blocker, err := pgx.Connect(ctx, databaseURL)
	if err != nil {
		t.Fatalf("connect reconciliation barrier owner: %v", err)
	}
	defer blocker.Close(context.Background())

	t.Run("Forget", func(t *testing.T) {
		tenantID, userID := uniqueScope("reconcile_forget_race")
		cleanupScopes(t, databaseURL, [][2]string{{tenantID, userID}})
		card := approveVectorCard(t, storage, tenantID, userID, "reconcile-forget-race", "forget-race-key", "forget-race-value", 70, 72, nil)
		execReconciliationSQL(t, databaseURL, `
			DELETE FROM agent_memory.embedding_projection_jobs
			WHERE tenant_id=$1 AND user_id=$2 AND memory_id=$3 AND embedding_space=$4`,
			tenantID, userID, card.ID, space)

		pageApp := fmt.Sprintf("projection_reconcile_page_%d", sequence)
		forgetApp := fmt.Sprintf("projection_reconcile_forget_%d", sequence)
		pageStore := openNamedReconciliationStore(t, databaseURL, pageApp)
		defer pageStore.Close()
		forgetStore := openNamedReconciliationStore(t, databaseURL, forgetApp)
		defer forgetStore.Close()

		setProjectionReconciliationAdvisoryLock(t, blocker, advisoryKey, true)
		pageResults := make(chan struct {
			page postgres.ProjectionReconciliationPage
			err  error
		}, 1)
		go func() {
			page, pageErr := pageStore.ReconcileProjectionPage(ctx, snapshot, postgres.ProjectionReconciliationCursor{
				TenantID: tenantID,
				UserID:   userID,
				MemoryID: "!",
			}, 1)
			pageResults <- struct {
				page postgres.ProjectionReconciliationPage
				err  error
			}{page: page, err: pageErr}
		}()
		waitForProjectionReconciliationLock(t, databaseURL, pageApp, "advisory")

		forgetResults := make(chan error, 1)
		go func() {
			_, forgetErr := forgetStore.ForgetUser(ctx, tenantID, userID, time.Now().UTC().Truncate(time.Microsecond))
			forgetResults <- forgetErr
		}()
		waitForProjectionReconciliationLock(t, databaseURL, forgetApp, "")
		setProjectionReconciliationAdvisoryLock(t, blocker, advisoryKey, false)

		select {
		case result := <-pageResults:
			if result.err != nil || result.page.Counts.MissingJob != 1 || result.page.Repairs.JobsEnqueued != 1 {
				t.Fatalf("forget race reconciliation page=%#v error=%v", result.page, result.err)
			}
		case <-time.After(10 * time.Second):
			t.Fatal("timed out waiting for forget race reconciliation page")
		}
		select {
		case forgetErr := <-forgetResults:
			if forgetErr != nil {
				t.Fatalf("forget after reconciliation: %v", forgetErr)
			}
		case <-time.After(10 * time.Second):
			t.Fatal("timed out waiting for forget race")
		}
		if countProjectionJobForCardAndSpace(t, databaseURL, tenantID, userID, card.ID, space) != 0 {
			t.Fatal("Forget left a reconciled projection job")
		}
	})

	t.Run("Supersede", func(t *testing.T) {
		tenantID, userID := uniqueScope("reconcile_supersede_race")
		cleanupScopes(t, databaseURL, [][2]string{{tenantID, userID}})
		oldCard := approveVectorCard(t, storage, tenantID, userID, "reconcile-supersede-old", "supersede-race-key", "old-value", 80, 82, nil)
		newEvent := evidence(tenantID, userID, "event-reconcile-supersede-new", "new-value", 84)
		mustAppend(t, storage, newEvent)
		newCandidate := candidate(
			tenantID,
			userID,
			"candidate-reconcile-supersede-new",
			"supersede-race-key",
			"new-value",
			[]string{newEvent.ID},
			85,
		)
		mustCreateCandidate(t, storage, newCandidate)
		execReconciliationSQL(t, databaseURL, `
			DELETE FROM agent_memory.embedding_projection_jobs
			WHERE tenant_id=$1 AND user_id=$2 AND memory_id=$3 AND embedding_space=$4`,
			tenantID, userID, oldCard.ID, space)

		pageApp := fmt.Sprintf("projection_reconcile_page_supersede_%d", sequence)
		approveApp := fmt.Sprintf("projection_reconcile_approve_%d", sequence)
		pageStore := openNamedReconciliationStore(t, databaseURL, pageApp)
		defer pageStore.Close()
		approveStore := openNamedReconciliationStore(t, databaseURL, approveApp)
		defer approveStore.Close()

		setProjectionReconciliationAdvisoryLock(t, blocker, advisoryKey, true)
		pageResults := make(chan error, 1)
		go func() {
			_, pageErr := pageStore.ReconcileProjectionPage(ctx, snapshot, postgres.ProjectionReconciliationCursor{
				TenantID: tenantID,
				UserID:   userID,
				MemoryID: "!",
			}, 1)
			pageResults <- pageErr
		}()
		waitForProjectionReconciliationLock(t, databaseURL, pageApp, "advisory")

		type approvalResult struct {
			card *domain.MemoryCard
			err  error
		}
		approvalResults := make(chan approvalResult, 1)
		go func() {
			_, card, approveErr := approveStore.ReviewCandidate(ctx, approval(newCandidate, "memory-reconcile-supersede-new", 87))
			approvalResults <- approvalResult{card: card, err: approveErr}
		}()
		waitForProjectionReconciliationLock(t, databaseURL, approveApp, "")
		setProjectionReconciliationAdvisoryLock(t, blocker, advisoryKey, false)

		select {
		case pageErr := <-pageResults:
			if pageErr != nil {
				t.Fatalf("supersede race reconciliation page: %v", pageErr)
			}
		case <-time.After(10 * time.Second):
			t.Fatal("timed out waiting for supersede race reconciliation page")
		}
		var newCard *domain.MemoryCard
		select {
		case result := <-approvalResults:
			if result.err != nil || result.card == nil {
				t.Fatalf("supersede after reconciliation card=%#v error=%v", result.card, result.err)
			}
			newCard = result.card
		case <-time.After(10 * time.Second):
			t.Fatal("timed out waiting for supersede race approval")
		}
		if countProjectionJobForCardAndSpace(t, databaseURL, tenantID, userID, oldCard.ID, space) != 0 {
			t.Fatal("supersede left the old reconciled projection job")
		}
		if countProjectionJobForCardAndSpace(t, databaseURL, tenantID, userID, newCard.ID, space) != 1 {
			t.Fatal("supersede did not atomically enqueue the new projection job")
		}
	})
}

func TestProjectionReconciliationSerializesStaleDeletionWithWorkerFinalization(t *testing.T) {
	ctx := context.Background()
	databaseURL := requiredDatabaseURL(t)
	applyMigrations(t, databaseURL)
	storage := openStore(t, databaseURL)
	defer storage.Close()
	space := registerReconciliationTarget(t, storage, databaseURL, "worker_race", postgres.ProjectionTargetShadow, true)
	target, err := storage.ProjectionTargetBySpace(ctx, space)
	if err != nil {
		t.Fatalf("load worker race target: %v", err)
	}
	tenantID, userID := uniqueScope("reconcile_worker_race")
	cleanupScopes(t, databaseURL, [][2]string{{tenantID, userID}})
	card := approveVectorCard(t, storage, tenantID, userID, "reconcile-worker-race", "worker-race-key", "worker-race-value", 90, 92, nil)
	if err := storage.UpsertMemoryEmbedding(ctx, reconciliationEmbedding(card, target.Space, strings.Repeat("e", 64))); err != nil {
		t.Fatalf("insert worker race stale embedding: %v", err)
	}
	items, err := storage.ClaimProjectionJobs(ctx, postgres.ClaimProjectionJobsCommand{
		EmbeddingSpace: space,
		LeaseOwner:     "reconciliation-worker-race",
		LeaseDuration:  time.Minute,
		MaxAttempts:    5,
		Limit:          1,
	})
	if err != nil || len(items) != 1 || items[0].Memory.ID != card.ID {
		t.Fatalf("claim worker race item=%#v error=%v", items, err)
	}

	snapshot, err := storage.BeginProjectionReconciliation(ctx, space, true)
	if err != nil {
		t.Fatalf("begin worker race reconciliation: %v", err)
	}
	sequence := scopeSequence.Add(1)
	advisoryKey := int64(1_800_000_000 + sequence%100_000_000)
	installProjectionReconciliationDeleteBarrier(t, databaseURL, space, advisoryKey, sequence)
	blocker, err := pgx.Connect(ctx, databaseURL)
	if err != nil {
		t.Fatalf("connect worker race barrier owner: %v", err)
	}
	defer blocker.Close(context.Background())
	setProjectionReconciliationAdvisoryLock(t, blocker, advisoryKey, true)

	pageApp := fmt.Sprintf("projection_reconcile_worker_page_%d", sequence)
	workerApp := fmt.Sprintf("projection_reconcile_worker_finalize_%d", sequence)
	pageStore := openNamedReconciliationStore(t, databaseURL, pageApp)
	defer pageStore.Close()
	workerStore := openNamedReconciliationStore(t, databaseURL, workerApp)
	defer workerStore.Close()
	pageResults := make(chan struct {
		page postgres.ProjectionReconciliationPage
		err  error
	}, 1)
	go func() {
		page, pageErr := pageStore.ReconcileProjectionPage(ctx, snapshot, postgres.ProjectionReconciliationCursor{
			TenantID: tenantID,
			UserID:   userID,
			MemoryID: "!",
		}, 1)
		pageResults <- struct {
			page postgres.ProjectionReconciliationPage
			err  error
		}{page: page, err: pageErr}
	}()
	waitForProjectionReconciliationLock(t, databaseURL, pageApp, "advisory")

	workerVector := make([]float32, postgres.VectorDimension)
	workerVector[1] = 1
	workerResults := make(chan error, 1)
	go func() {
		_, finalizeErr := workerStore.FinalizeProjectionJob(ctx, postgres.FinalizeProjectionJobCommand{
			JobID:          items[0].Job.ID,
			TenantID:       tenantID,
			UserID:         userID,
			EmbeddingSpace: space,
			LeaseOwner:     items[0].Job.LeaseOwner,
			LeaseVersion:   items[0].Job.LeaseVersion,
			DocumentSHA256: items[0].DocumentSHA256,
			Vector:         workerVector,
		})
		workerResults <- finalizeErr
	}()
	waitForProjectionReconciliationLock(t, databaseURL, workerApp, "")
	setProjectionReconciliationAdvisoryLock(t, blocker, advisoryKey, false)

	select {
	case result := <-pageResults:
		if result.err != nil || result.page.Counts.InFlight != 1 || result.page.Repairs.EmbeddingsDeleted != 1 {
			t.Fatalf("worker race page=%#v error=%v", result.page, result.err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for worker race reconciliation")
	}
	select {
	case workerErr := <-workerResults:
		if workerErr != nil {
			t.Fatalf("worker finalize after reconciliation: %v", workerErr)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for worker race finalization")
	}
	assertProjectionJobState(t, databaseURL, tenantID, userID, card.ID, space, "succeeded", 1)
	assertProjectionEmbeddingHash(
		t,
		databaseURL,
		tenantID,
		userID,
		card.ID,
		space,
		embedding.MemoryCardDocumentV1SHA256(card),
	)
}

func TestProjectionReconciliationFinalizationSeesApprovalCommittedWhileWaitingForDeploymentLock(t *testing.T) {
	ctx := context.Background()
	databaseURL := requiredDatabaseURL(t)
	applyMigrations(t, databaseURL)
	storage := openStore(t, databaseURL)
	defer storage.Close()
	space := registerReconciliationTarget(t, storage, databaseURL, "finalize_approval_visibility", postgres.ProjectionTargetShadow, true)
	snapshot, err := storage.BeginProjectionReconciliation(ctx, space, false)
	if err != nil {
		t.Fatalf("begin finalization visibility reconciliation: %v", err)
	}
	baseline := countServiceableProjectionCards(t, databaseURL)

	tenantID, userID := uniqueScope("reconcile_finalize_approval")
	cleanupScopes(t, databaseURL, [][2]string{{tenantID, userID}})
	event := evidence(tenantID, userID, "event-reconcile-finalize-approval", "approval-after-finalize-start", 100)
	mustAppend(t, storage, event)
	value := candidate(
		tenantID,
		userID,
		"candidate-reconcile-finalize-approval",
		"finalize-approval-key",
		"approval-after-finalize-start",
		[]string{event.ID},
		101,
	)
	mustCreateCandidate(t, storage, value)

	sequence := scopeSequence.Add(1)
	advisoryKey := int64(1_900_000_000 + sequence%100_000_000)
	installProjectionReconciliationCardInsertBarrier(t, databaseURL, tenantID, userID, advisoryKey, sequence)
	blocker, err := pgx.Connect(ctx, databaseURL)
	if err != nil {
		t.Fatalf("connect finalization visibility barrier owner: %v", err)
	}
	defer blocker.Close(context.Background())
	setProjectionReconciliationAdvisoryLock(t, blocker, advisoryKey, true)

	approvalApp := fmt.Sprintf("projection_reconcile_visibility_approval_%d", sequence)
	finalizeApp := fmt.Sprintf("projection_reconcile_visibility_finalize_%d", sequence)
	approvalStore := openNamedReconciliationStore(t, databaseURL, approvalApp)
	defer approvalStore.Close()
	finalizeStore := openNamedReconciliationStore(t, databaseURL, finalizeApp)
	defer finalizeStore.Close()

	type approvalResult struct {
		card *domain.MemoryCard
		err  error
	}
	approvalResults := make(chan approvalResult, 1)
	go func() {
		_, card, approvalErr := approvalStore.ReviewCandidate(
			ctx,
			approval(value, "memory-reconcile-finalize-approval", 103),
		)
		approvalResults <- approvalResult{card: card, err: approvalErr}
	}()
	waitForProjectionReconciliationLock(t, databaseURL, approvalApp, "advisory")

	type finalizationResult struct {
		report postgres.ProjectionReconciliationReport
		err    error
	}
	finalizationResults := make(chan finalizationResult, 1)
	go func() {
		report, finalizeErr := finalizeStore.FinalizeProjectionReconciliation(ctx, snapshot)
		finalizationResults <- finalizationResult{report: report, err: finalizeErr}
	}()
	waitForProjectionReconciliationLock(t, databaseURL, finalizeApp, "")
	setProjectionReconciliationAdvisoryLock(t, blocker, advisoryKey, false)

	var card *domain.MemoryCard
	select {
	case result := <-approvalResults:
		if result.err != nil || result.card == nil {
			t.Fatalf("visibility approval card=%#v error=%v", result.card, result.err)
		}
		card = result.card
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for visibility approval")
	}
	select {
	case result := <-finalizationResults:
		if result.err != nil {
			t.Fatalf("finalize after waiting for approval: %v", result.err)
		}
		if result.report.Counts.Scanned != baseline+1 || result.report.Counts.InFlight < 1 || result.report.Complete {
			t.Fatalf("post-approval coverage report=%#v, baseline=%d", result.report, baseline)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for post-approval finalization")
	}
	assertProjectionJobState(t, databaseURL, tenantID, userID, card.ID, space, "pending", 0)
}

type projectionReconciliationPass struct {
	Counts  postgres.ProjectionReconciliationCounts
	Repairs postgres.ProjectionReconciliationRepairs
}

func runProjectionReconciliationPass(
	t *testing.T,
	storage *postgres.Store,
	snapshot postgres.ProjectionReconciliationSnapshot,
	limit int,
) projectionReconciliationPass {
	t.Helper()
	cursor := postgres.ProjectionReconciliationCursor{}
	result := projectionReconciliationPass{}
	pages := 0
	for {
		page, err := storage.ReconcileProjectionPage(context.Background(), snapshot, cursor, limit)
		if err != nil {
			t.Fatalf("reconcile page %d: %v", pages+1, err)
		}
		pages++
		addProjectionReconciliationCounts(&result.Counts, page.Counts)
		addProjectionReconciliationRepairs(&result.Repairs, page.Repairs)
		if page.Complete {
			if page.NextCursor != nil {
				t.Fatal("complete reconciliation page returned a cursor")
			}
			break
		}
		if page.NextCursor == nil {
			t.Fatal("incomplete reconciliation page omitted its cursor")
		}
		if *page.NextCursor == cursor {
			t.Fatal("reconciliation cursor did not advance")
		}
		cursor = *page.NextCursor
		if pages > 10000 {
			t.Fatal("reconciliation traversal exceeded safety bound")
		}
	}
	if result.Counts.Scanned > int64(limit) && pages < 2 {
		t.Fatalf("%d rows were not split across bounded pages", result.Counts.Scanned)
	}
	return result
}

func addProjectionReconciliationCounts(total *postgres.ProjectionReconciliationCounts, value postgres.ProjectionReconciliationCounts) {
	total.Scanned += value.Scanned
	total.Converged += value.Converged
	total.MissingJob += value.MissingJob
	total.InFlight += value.InFlight
	total.Dead += value.Dead
	total.Cancelled += value.Cancelled
	total.SucceededMissingEmbedding += value.SucceededMissingEmbedding
	total.ContentHashMismatch += value.ContentHashMismatch
	total.VersionInvariant += value.VersionInvariant
}

func addProjectionReconciliationRepairs(total *postgres.ProjectionReconciliationRepairs, value postgres.ProjectionReconciliationRepairs) {
	total.JobsEnqueued += value.JobsEnqueued
	total.JobsReset += value.JobsReset
	total.EmbeddingsDeleted += value.EmbeddingsDeleted
	total.RevisionsAdvanced += value.RevisionsAdvanced
}

func registerReconciliationTarget(
	t *testing.T,
	storage *postgres.Store,
	databaseURL, label string,
	state postgres.ProjectionTargetState,
	enqueueNew bool,
) string {
	t.Helper()
	space := uniqueProjectionRepositorySpace("reconciliation_" + label)
	cleanupProjectionRepositorySpaces(t, databaseURL, space)
	createdAt := time.Now().UTC().Truncate(time.Microsecond)
	registeredState := state
	if state == postgres.ProjectionTargetServing {
		// This fixture exercises serving-only reconciliation effects, not the
		// promotion protocol. Public registration deliberately rejects serving.
		registeredState = postgres.ProjectionTargetShadow
	}
	if _, err := storage.RegisterProjectionTarget(
		context.Background(),
		projectionRepositoryRegistration(space, registeredState, enqueueNew, createdAt),
	); err != nil {
		t.Fatalf("register reconciliation target: %v", err)
	}
	if state == postgres.ProjectionTargetServing {
		execReconciliationSQL(t, databaseURL, `
			UPDATE agent_memory.embedding_projection_targets
			SET state = 'serving', enqueue_new = true,
			    updated_at = GREATEST(updated_at, clock_timestamp())
			WHERE embedding_space = $1`, space)
	}
	return space
}

func assertNoServingProjectionTarget(t *testing.T, databaseURL string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	conn, err := pgx.Connect(ctx, databaseURL)
	if err != nil {
		t.Fatalf("connect to inspect serving target: %v", err)
	}
	defer conn.Close(context.Background())
	var count int
	if err := conn.QueryRow(ctx, `
		SELECT count(*)
		FROM agent_memory.embedding_projection_targets
		WHERE state = 'serving'`).Scan(&count); err != nil {
		t.Fatalf("count serving projection targets: %v", err)
	}
	if count != 0 {
		t.Skip("shared integration database already has a serving projection target")
	}
}

func countServiceableProjectionCards(t *testing.T, databaseURL string) int64 {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	conn, err := pgx.Connect(ctx, databaseURL)
	if err != nil {
		t.Fatalf("connect to count serviceable cards: %v", err)
	}
	defer conn.Close(context.Background())
	var count int64
	if err := conn.QueryRow(ctx, `
		SELECT count(*)
		FROM agent_memory.memory_cards
		WHERE status = 'active'
		  AND (expires_at IS NULL OR expires_at > clock_timestamp())`).Scan(&count); err != nil {
		t.Fatalf("count serviceable projection cards: %v", err)
	}
	return count
}

func execReconciliationSQL(t *testing.T, databaseURL, statement string, arguments ...any) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	conn, err := pgx.Connect(ctx, databaseURL)
	if err != nil {
		t.Fatalf("connect for reconciliation fixture: %v", err)
	}
	defer conn.Close(context.Background())
	if _, err := conn.Exec(ctx, statement, arguments...); err != nil {
		t.Fatalf("execute reconciliation fixture: %v", err)
	}
}

func markProjectionJobState(
	t *testing.T,
	databaseURL, tenantID, userID, memoryID, space, state string,
) {
	t.Helper()
	switch state {
	case "succeeded":
		execReconciliationSQL(t, databaseURL, `
			UPDATE agent_memory.embedding_projection_jobs
			SET state = 'succeeded', attempt_count = 1, lease_version = 1,
			    lease_owner = NULL, lease_until = NULL,
			    updated_at = clock_timestamp(), completed_at = clock_timestamp()
			WHERE tenant_id = $1 AND user_id = $2 AND memory_id = $3 AND embedding_space = $4`,
			tenantID, userID, memoryID, space)
	case "dead":
		execReconciliationSQL(t, databaseURL, `
			UPDATE agent_memory.embedding_projection_jobs
			SET state = 'dead', attempt_count = 1, lease_version = 1,
			    lease_owner = NULL, lease_until = NULL,
			    last_error_code = 'attempts_exhausted', last_error_at = clock_timestamp(),
			    updated_at = clock_timestamp(), completed_at = clock_timestamp()
			WHERE tenant_id = $1 AND user_id = $2 AND memory_id = $3 AND embedding_space = $4`,
			tenantID, userID, memoryID, space)
	default:
		t.Fatalf("unsupported fixture state %q", state)
	}
}

func reconciliationEmbedding(
	card domain.MemoryCard,
	space postgres.EmbeddingSpaceDefinition,
	contentHash string,
) postgres.MemoryEmbedding {
	vector := make([]float32, postgres.VectorDimension)
	vector[0] = 1
	return postgres.MemoryEmbedding{
		TenantID:         card.TenantID,
		UserID:           card.UserID,
		MemoryID:         card.ID,
		EmbeddingSpace:   space.ID,
		Provider:         space.Provider,
		Model:            space.Model,
		DocumentVersion:  space.DocumentVersion,
		QueryVersion:     space.QueryVersion,
		ModelFingerprint: space.ModelFingerprint,
		ContentSHA256:    contentHash,
		Vector:           vector,
		CreatedAt:        time.Now().UTC().Truncate(time.Microsecond),
	}
}

func countProjectionJobForCardAndSpace(t *testing.T, databaseURL, tenantID, userID, memoryID, space string) int {
	t.Helper()
	return countReconciliationRows(t, databaseURL, "embedding_projection_jobs", tenantID, userID, memoryID, space)
}

func countProjectionEmbeddingForCardAndSpace(t *testing.T, databaseURL, tenantID, userID, memoryID, space string) int {
	t.Helper()
	return countReconciliationRows(t, databaseURL, "memory_embeddings", tenantID, userID, memoryID, space)
}

func countReconciliationRows(t *testing.T, databaseURL, table, tenantID, userID, memoryID, space string) int {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	conn, err := pgx.Connect(ctx, databaseURL)
	if err != nil {
		t.Fatalf("connect to count reconciliation rows: %v", err)
	}
	defer conn.Close(context.Background())
	query := fmt.Sprintf(`
		SELECT count(*) FROM agent_memory.%s
		WHERE tenant_id=$1 AND user_id=$2 AND memory_id=$3 AND embedding_space=$4`, table)
	var count int
	if err := conn.QueryRow(ctx, query, tenantID, userID, memoryID, space).Scan(&count); err != nil {
		t.Fatalf("count reconciliation rows: %v", err)
	}
	return count
}

func assertProjectionJobState(
	t *testing.T,
	databaseURL, tenantID, userID, memoryID, space, wantState string,
	wantAttempts int,
) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	conn, err := pgx.Connect(ctx, databaseURL)
	if err != nil {
		t.Fatalf("connect to inspect reconciled job: %v", err)
	}
	defer conn.Close(context.Background())
	var state string
	var attempts int
	if err := conn.QueryRow(ctx, `
		SELECT state, attempt_count
		FROM agent_memory.embedding_projection_jobs
		WHERE tenant_id=$1 AND user_id=$2 AND memory_id=$3 AND embedding_space=$4`,
		tenantID, userID, memoryID, space).Scan(&state, &attempts); err != nil {
		t.Fatalf("read reconciled job: %v", err)
	}
	if state != wantState || attempts != wantAttempts {
		t.Fatalf("reconciled job state/attempts=%s/%d, want %s/%d", state, attempts, wantState, wantAttempts)
	}
}

func assertProjectionJobExpectedVersion(
	t *testing.T,
	databaseURL, tenantID, userID, memoryID, space string,
	want int,
) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	conn, err := pgx.Connect(ctx, databaseURL)
	if err != nil {
		t.Fatalf("connect to inspect expected version: %v", err)
	}
	defer conn.Close(context.Background())
	var got int
	if err := conn.QueryRow(ctx, `
		SELECT expected_memory_version
		FROM agent_memory.embedding_projection_jobs
		WHERE tenant_id=$1 AND user_id=$2 AND memory_id=$3 AND embedding_space=$4`,
		tenantID, userID, memoryID, space).Scan(&got); err != nil {
		t.Fatalf("read expected memory version: %v", err)
	}
	if got != want {
		t.Fatalf("expected memory version=%d, want %d", got, want)
	}
}

func openNamedReconciliationStore(t *testing.T, databaseURL, applicationName string) *postgres.Store {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	config, err := pgxpool.ParseConfig(databaseURL)
	if err != nil {
		t.Fatalf("parse named reconciliation pool: %v", err)
	}
	if config.ConnConfig.RuntimeParams == nil {
		config.ConnConfig.RuntimeParams = make(map[string]string)
	}
	config.ConnConfig.RuntimeParams["application_name"] = applicationName
	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		t.Fatalf("open named reconciliation pool: %v", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		t.Fatalf("ping named reconciliation pool: %v", err)
	}
	return postgres.New(pool)
}

func installProjectionReconciliationInsertBarrier(
	t *testing.T,
	databaseURL, embeddingSpace string,
	advisoryKey int64,
	sequence uint64,
) {
	t.Helper()
	functionName := fmt.Sprintf("test_reconcile_insert_barrier_%d", sequence)
	triggerName := fmt.Sprintf("test_reconcile_insert_barrier_trigger_%d", sequence)
	qualifiedFunction := "agent_memory." + functionName
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	conn, err := pgx.Connect(ctx, databaseURL)
	if err != nil {
		t.Fatalf("connect to install reconciliation barrier: %v", err)
	}
	defer conn.Close(context.Background())
	if _, err := conn.Exec(ctx, fmt.Sprintf(`
		CREATE FUNCTION %s() RETURNS trigger
		LANGUAGE plpgsql AS $body$
		BEGIN
			IF NEW.embedding_space = %s THEN
				PERFORM pg_advisory_xact_lock(%d);
			END IF;
			RETURN NEW;
		END
		$body$`, qualifiedFunction, postgresTestLiteral(embeddingSpace), advisoryKey)); err != nil {
		t.Fatalf("create reconciliation barrier function: %v", err)
	}
	if _, err := conn.Exec(ctx, fmt.Sprintf(`
		CREATE TRIGGER %s
		BEFORE INSERT ON agent_memory.embedding_projection_jobs
		FOR EACH ROW EXECUTE FUNCTION %s()`, triggerName, qualifiedFunction)); err != nil {
		_, _ = conn.Exec(ctx, "DROP FUNCTION IF EXISTS "+qualifiedFunction+"()")
		t.Fatalf("create reconciliation barrier trigger: %v", err)
	}
	t.Cleanup(func() {
		cleanupContext, cleanupCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cleanupCancel()
		cleanupConn, cleanupErr := pgx.Connect(cleanupContext, databaseURL)
		if cleanupErr != nil {
			t.Errorf("connect to remove reconciliation barrier: %v", cleanupErr)
			return
		}
		defer cleanupConn.Close(context.Background())
		if _, cleanupErr = cleanupConn.Exec(cleanupContext, fmt.Sprintf(
			"DROP TRIGGER IF EXISTS %s ON agent_memory.embedding_projection_jobs", triggerName,
		)); cleanupErr != nil {
			t.Errorf("drop reconciliation barrier trigger: %v", cleanupErr)
			return
		}
		if _, cleanupErr = cleanupConn.Exec(cleanupContext, "DROP FUNCTION IF EXISTS "+qualifiedFunction+"()"); cleanupErr != nil {
			t.Errorf("drop reconciliation barrier function: %v", cleanupErr)
		}
	})
}

func installProjectionReconciliationDeleteBarrier(
	t *testing.T,
	databaseURL, embeddingSpace string,
	advisoryKey int64,
	sequence uint64,
) {
	t.Helper()
	functionName := fmt.Sprintf("test_reconcile_delete_barrier_%d", sequence)
	triggerName := fmt.Sprintf("test_reconcile_delete_barrier_trigger_%d", sequence)
	qualifiedFunction := "agent_memory." + functionName
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	conn, err := pgx.Connect(ctx, databaseURL)
	if err != nil {
		t.Fatalf("connect to install reconciliation delete barrier: %v", err)
	}
	defer conn.Close(context.Background())
	if _, err := conn.Exec(ctx, fmt.Sprintf(`
		CREATE FUNCTION %s() RETURNS trigger
		LANGUAGE plpgsql AS $body$
		BEGIN
			IF OLD.embedding_space = %s THEN
				PERFORM pg_advisory_xact_lock(%d);
			END IF;
			RETURN OLD;
		END
		$body$`, qualifiedFunction, postgresTestLiteral(embeddingSpace), advisoryKey)); err != nil {
		t.Fatalf("create reconciliation delete barrier function: %v", err)
	}
	if _, err := conn.Exec(ctx, fmt.Sprintf(`
		CREATE TRIGGER %s
		BEFORE DELETE ON agent_memory.memory_embeddings
		FOR EACH ROW EXECUTE FUNCTION %s()`, triggerName, qualifiedFunction)); err != nil {
		_, _ = conn.Exec(ctx, "DROP FUNCTION IF EXISTS "+qualifiedFunction+"()")
		t.Fatalf("create reconciliation delete barrier trigger: %v", err)
	}
	t.Cleanup(func() {
		cleanupContext, cleanupCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cleanupCancel()
		cleanupConn, cleanupErr := pgx.Connect(cleanupContext, databaseURL)
		if cleanupErr != nil {
			t.Errorf("connect to remove reconciliation delete barrier: %v", cleanupErr)
			return
		}
		defer cleanupConn.Close(context.Background())
		if _, cleanupErr = cleanupConn.Exec(cleanupContext, fmt.Sprintf(
			"DROP TRIGGER IF EXISTS %s ON agent_memory.memory_embeddings", triggerName,
		)); cleanupErr != nil {
			t.Errorf("drop reconciliation delete barrier trigger: %v", cleanupErr)
			return
		}
		if _, cleanupErr = cleanupConn.Exec(cleanupContext, "DROP FUNCTION IF EXISTS "+qualifiedFunction+"()"); cleanupErr != nil {
			t.Errorf("drop reconciliation delete barrier function: %v", cleanupErr)
		}
	})
}

func installProjectionReconciliationCardInsertBarrier(
	t *testing.T,
	databaseURL, tenantID, userID string,
	advisoryKey int64,
	sequence uint64,
) {
	t.Helper()
	functionName := fmt.Sprintf("test_reconcile_card_insert_barrier_%d", sequence)
	triggerName := fmt.Sprintf("test_reconcile_card_insert_barrier_trigger_%d", sequence)
	qualifiedFunction := "agent_memory." + functionName
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	conn, err := pgx.Connect(ctx, databaseURL)
	if err != nil {
		t.Fatalf("connect to install reconciliation card barrier: %v", err)
	}
	defer conn.Close(context.Background())
	if _, err := conn.Exec(ctx, fmt.Sprintf(`
		CREATE FUNCTION %s() RETURNS trigger
		LANGUAGE plpgsql AS $body$
		BEGIN
			IF NEW.tenant_id = %s AND NEW.user_id = %s THEN
				PERFORM pg_advisory_xact_lock(%d);
			END IF;
			RETURN NEW;
		END
		$body$`, qualifiedFunction, postgresTestLiteral(tenantID), postgresTestLiteral(userID), advisoryKey)); err != nil {
		t.Fatalf("create reconciliation card barrier function: %v", err)
	}
	if _, err := conn.Exec(ctx, fmt.Sprintf(`
		CREATE TRIGGER %s
		BEFORE INSERT ON agent_memory.memory_cards
		FOR EACH ROW EXECUTE FUNCTION %s()`, triggerName, qualifiedFunction)); err != nil {
		_, _ = conn.Exec(ctx, "DROP FUNCTION IF EXISTS "+qualifiedFunction+"()")
		t.Fatalf("create reconciliation card barrier trigger: %v", err)
	}
	t.Cleanup(func() {
		cleanupContext, cleanupCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cleanupCancel()
		cleanupConn, cleanupErr := pgx.Connect(cleanupContext, databaseURL)
		if cleanupErr != nil {
			t.Errorf("connect to remove reconciliation card barrier: %v", cleanupErr)
			return
		}
		defer cleanupConn.Close(context.Background())
		if _, cleanupErr = cleanupConn.Exec(cleanupContext, fmt.Sprintf(
			"DROP TRIGGER IF EXISTS %s ON agent_memory.memory_cards", triggerName,
		)); cleanupErr != nil {
			t.Errorf("drop reconciliation card barrier trigger: %v", cleanupErr)
			return
		}
		if _, cleanupErr = cleanupConn.Exec(cleanupContext, "DROP FUNCTION IF EXISTS "+qualifiedFunction+"()"); cleanupErr != nil {
			t.Errorf("drop reconciliation card barrier function: %v", cleanupErr)
		}
	})
}

func setProjectionReconciliationAdvisoryLock(t *testing.T, conn *pgx.Conn, advisoryKey int64, lock bool) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	query := "SELECT pg_advisory_lock($1)"
	if lock {
		if _, err := conn.Exec(ctx, query, advisoryKey); err != nil {
			t.Fatalf("acquire reconciliation advisory lock: %v", err)
		}
		return
	}
	query = "SELECT pg_advisory_unlock($1)"
	var unlocked bool
	if err := conn.QueryRow(ctx, query, advisoryKey).Scan(&unlocked); err != nil {
		t.Fatalf("change reconciliation advisory lock: %v", err)
	}
	if !unlocked {
		t.Fatal("reconciliation advisory lock was not held")
	}
}

func waitForProjectionReconciliationLock(t *testing.T, databaseURL, applicationName, waitEvent string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	conn, err := pgx.Connect(ctx, databaseURL)
	if err != nil {
		t.Fatalf("connect to observe reconciliation lock: %v", err)
	}
	defer conn.Close(context.Background())
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		var waiting bool
		err := conn.QueryRow(ctx, `
			SELECT EXISTS (
				SELECT 1
				FROM pg_stat_activity
				WHERE application_name = $1
				  AND wait_event_type = 'Lock'
				  AND ($2::text = '' OR wait_event = $2)
			)`, applicationName, waitEvent).Scan(&waiting)
		if err != nil {
			t.Fatalf("observe reconciliation lock: %v", err)
		}
		if waiting {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatalf("timed out waiting for %s lock wait", applicationName)
		case <-ticker.C:
		}
	}
}

func assertProjectionEmbeddingHash(
	t *testing.T,
	databaseURL, tenantID, userID, memoryID, space, want string,
) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	conn, err := pgx.Connect(ctx, databaseURL)
	if err != nil {
		t.Fatalf("connect to inspect reconciled embedding hash: %v", err)
	}
	defer conn.Close(context.Background())
	var got string
	if err := conn.QueryRow(ctx, `
		SELECT content_sha256
		FROM agent_memory.memory_embeddings
		WHERE tenant_id=$1 AND user_id=$2 AND memory_id=$3 AND embedding_space=$4`,
		tenantID, userID, memoryID, space).Scan(&got); err != nil {
		t.Fatalf("read reconciled embedding hash: %v", err)
	}
	if got != want {
		t.Fatalf("reconciled embedding hash=%q, want %q", got, want)
	}
}
