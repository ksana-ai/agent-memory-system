package postgres

import (
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/ksana-ai/agent-memory-system/internal/domain"
)

func TestValidateProjectionReconciliationSnapshotCanonicalizesDatabaseTime(t *testing.T) {
	startedAt := time.Date(2026, time.August, 20, 10, 0, 0, 123456789, time.FixedZone("test", 8*60*60))
	snapshot, err := validateProjectionReconciliationSnapshot(ProjectionReconciliationSnapshot{
		EmbeddingSpace: "space_v1_reconciliation_unit",
		Generation:     7,
		StartedAt:      startedAt,
		Repair:         true,
	})
	if err != nil {
		t.Fatalf("validate reconciliation snapshot: %v", err)
	}
	if snapshot.StartedAt.Location() != time.UTC || snapshot.StartedAt.Nanosecond()%1000 != 0 {
		t.Fatalf("started_at=%s, want canonical UTC microseconds", snapshot.StartedAt.Format(time.RFC3339Nano))
	}
	if snapshot.Generation != 7 || !snapshot.Repair {
		t.Fatalf("snapshot=%#v, want generation and repair preserved", snapshot)
	}
}

func TestValidateProjectionReconciliationSnapshotRejectsInvalidValues(t *testing.T) {
	valid := ProjectionReconciliationSnapshot{
		EmbeddingSpace: "space_v1_reconciliation_unit",
		Generation:     1,
		StartedAt:      time.Date(2026, time.August, 20, 2, 0, 0, 0, time.UTC),
	}
	tests := []struct {
		name   string
		mutate func(*ProjectionReconciliationSnapshot)
	}{
		{name: "blank space", mutate: func(value *ProjectionReconciliationSnapshot) { value.EmbeddingSpace = " " }},
		{name: "negative generation", mutate: func(value *ProjectionReconciliationSnapshot) { value.Generation = -1 }},
		{name: "missing started at", mutate: func(value *ProjectionReconciliationSnapshot) { value.StartedAt = time.Time{} }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			value := valid
			test.mutate(&value)
			if _, err := validateProjectionReconciliationSnapshot(value); !errors.Is(err, domain.ErrInvalid) {
				t.Fatalf("validation error=%v, want invalid", err)
			}
		})
	}
}

func TestValidateProjectionReconciliationCursorRequiresCompleteKey(t *testing.T) {
	for _, cursor := range []ProjectionReconciliationCursor{
		{},
		{TenantID: "tenant", UserID: "user", MemoryID: "memory"},
	} {
		if err := validateProjectionReconciliationCursor(cursor); err != nil {
			t.Fatalf("valid cursor=%#v error=%v", cursor, err)
		}
	}
	for _, cursor := range []ProjectionReconciliationCursor{
		{TenantID: "tenant"},
		{TenantID: "tenant", UserID: "user"},
		{TenantID: " tenant", UserID: "user", MemoryID: "memory"},
		{TenantID: "tenant", UserID: "user", MemoryID: "memory\nsecret"},
	} {
		if err := validateProjectionReconciliationCursor(cursor); !errors.Is(err, domain.ErrInvalid) {
			t.Fatalf("invalid cursor=%#v error=%v, want invalid", cursor, err)
		}
	}
}

func TestValidateReconciliationTargetSeparatesAuditFromRepairEligibility(t *testing.T) {
	target := ProjectionTarget{
		Space: EmbeddingSpaceDefinition{DocumentVersion: "memory-card-document-v1"},
		State: ProjectionTargetShadow,
	}
	if err := validateReconciliationTarget(target, false); err != nil {
		t.Fatalf("audit disabled-enqueue shadow target: %v", err)
	}
	if err := validateReconciliationTarget(target, true); !errors.Is(err, domain.ErrConflict) {
		t.Fatalf("repair disabled-enqueue target error=%v, want conflict", err)
	}
	target.EnqueueNew = true
	if err := validateReconciliationTarget(target, true); err != nil {
		t.Fatalf("repair eligible shadow target: %v", err)
	}
	target.State, target.EnqueueNew = ProjectionTargetBlocked, false
	if err := validateReconciliationTarget(target, false); err != nil {
		t.Fatalf("audit blocked target: %v", err)
	}
	if err := validateReconciliationTarget(target, true); !errors.Is(err, domain.ErrConflict) {
		t.Fatalf("repair blocked target error=%v, want conflict", err)
	}
	target.State = ProjectionTargetRetired
	if err := validateReconciliationTarget(target, false); !errors.Is(err, domain.ErrConflict) {
		t.Fatalf("audit retired target error=%v, want conflict", err)
	}
	target.State = ProjectionTargetShadow
	target.Space.DocumentVersion = "future-document-v2"
	if err := validateReconciliationTarget(target, false); !errors.Is(err, domain.ErrInvariant) {
		t.Fatalf("unsupported document error=%v, want invariant", err)
	}
}

func TestClassifyProjectionCoverageUsesExclusivePrecedence(t *testing.T) {
	const expectedHash = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	const staleHash = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	baseJob := projectionReconciliationJob{ID: 1, ExpectedMemoryVersion: 2, State: ProjectionJobSucceeded}
	tests := []struct {
		name            string
		job             projectionReconciliationJob
		jobExists       bool
		embeddingHash   string
		embeddingExists bool
		want            projectionCoverageClass
		wantStale       bool
	}{
		{name: "missing job", jobExists: false, embeddingHash: staleHash, embeddingExists: true, want: projectionCoverageMissingJob, wantStale: true},
		{name: "pending", job: withReconciliationJobState(baseJob, ProjectionJobPending), jobExists: true, embeddingHash: expectedHash, embeddingExists: true, want: projectionCoverageInFlight},
		{name: "leased stale", job: withReconciliationJobState(baseJob, ProjectionJobLeased), jobExists: true, embeddingHash: staleHash, embeddingExists: true, want: projectionCoverageInFlight, wantStale: true},
		{name: "retry", job: withReconciliationJobState(baseJob, ProjectionJobRetry), jobExists: true, want: projectionCoverageInFlight},
		{name: "dead", job: withReconciliationJobState(baseJob, ProjectionJobDead), jobExists: true, embeddingHash: staleHash, embeddingExists: true, want: projectionCoverageDead, wantStale: true},
		{name: "cancelled", job: withReconciliationJobState(baseJob, ProjectionJobCancelled), jobExists: true, want: projectionCoverageCancelled},
		{name: "succeeded missing", job: baseJob, jobExists: true, want: projectionCoverageSucceededMissingEmbedding},
		{name: "succeeded stale", job: baseJob, jobExists: true, embeddingHash: staleHash, embeddingExists: true, want: projectionCoverageContentHashMismatch, wantStale: true},
		{name: "converged", job: baseJob, jobExists: true, embeddingHash: expectedHash, embeddingExists: true, want: projectionCoverageConverged},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, stale, err := classifyProjectionCoverage(
				2,
				test.job,
				test.jobExists,
				test.embeddingHash,
				test.embeddingExists,
				expectedHash,
			)
			if err != nil || got != test.want || stale != test.wantStale {
				t.Fatalf("classification=%d stale=%t error=%v, want %d/%t", got, stale, err, test.want, test.wantStale)
			}
		})
	}
}

func TestClassifyProjectionCoverageFailsVersionInvariant(t *testing.T) {
	class, stale, err := classifyProjectionCoverage(
		2,
		projectionReconciliationJob{ID: 1, ExpectedMemoryVersion: 1, State: ProjectionJobSucceeded},
		true,
		strings.Repeat("b", 64),
		true,
		strings.Repeat("a", 64),
	)
	if class != projectionCoverageVersionInvariant || !stale || !errors.Is(err, domain.ErrInvariant) {
		t.Fatalf("classification=%d stale=%t error=%v, want version invariant", class, stale, err)
	}
}

func TestProjectionReconciliationCountsAreMutuallyExclusive(t *testing.T) {
	var counts ProjectionReconciliationCounts
	classes := []projectionCoverageClass{
		projectionCoverageConverged,
		projectionCoverageMissingJob,
		projectionCoverageInFlight,
		projectionCoverageDead,
		projectionCoverageCancelled,
		projectionCoverageSucceededMissingEmbedding,
		projectionCoverageContentHashMismatch,
		projectionCoverageVersionInvariant,
	}
	for _, class := range classes {
		counts.add(class)
	}
	if err := counts.validate(); err != nil {
		t.Fatalf("validate exhaustive counts: %v", err)
	}
	counts.MissingJob++
	if err := counts.validate(); !errors.Is(err, domain.ErrInvariant) {
		t.Fatalf("inconsistent counts error=%v, want invariant", err)
	}
}

func TestProjectionReconciliationPublicResultsExposeOnlyAggregateData(t *testing.T) {
	for _, value := range []any{
		ProjectionReconciliationCounts{},
		ProjectionReconciliationRepairs{},
		ProjectionReconciliationPage{},
		ProjectionReconciliationReport{},
	} {
		typeOfValue := reflect.TypeOf(value)
		for index := 0; index < typeOfValue.NumField(); index++ {
			name := strings.ToLower(typeOfValue.Field(index).Name)
			for _, forbidden := range []string{
				"document", "value", "backstory", "dsn", "url", "credential",
				"secret", "response", "rawerror", "errortext", "errormessage", "vector",
			} {
				if strings.Contains(name, forbidden) {
					t.Fatalf("%s exposes forbidden field %q", typeOfValue.Name(), typeOfValue.Field(index).Name)
				}
			}
		}
	}
}

func TestProjectionReconciliationDatabaseErrorsRedactConnectionAndCardData(t *testing.T) {
	const secret = "postgres://operator:secret-password@db.internal/memory card-private-value"
	err := mapProjectionPostgresError(
		"scan projection reconciliation coverage",
		errors.New("driver failure "+secret),
	)
	if strings.Contains(err.Error(), "secret-password") ||
		strings.Contains(err.Error(), "db.internal") ||
		strings.Contains(err.Error(), "card-private-value") {
		t.Fatalf("reconciliation error exposed raw input: %q", err)
	}
	if !errors.Is(err, domain.ErrInvariant) {
		t.Fatalf("reconciliation database error=%v, want invariant", err)
	}
}

func TestProjectionReconciliationFinalizationUsesPostLockStatementSnapshot(t *testing.T) {
	options := projectionReconciliationFinalizationTxOptions()
	if options.IsoLevel != pgx.ReadCommitted {
		t.Fatalf("finalization isolation=%q, want READ COMMITTED so post-lock coverage sees committed approvals", options.IsoLevel)
	}
}

func withReconciliationJobState(value projectionReconciliationJob, state ProjectionJobState) projectionReconciliationJob {
	value.State = state
	return value
}
