package postgres

import (
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/kai443/go-agent-memory-system/internal/domain"
)

func TestValidateClaimProjectionJobsIsBoundedAndCanonical(t *testing.T) {
	command := ClaimProjectionJobsCommand{
		EmbeddingSpace: "space_v1_worker_unit",
		LeaseOwner:     "worker-unit-1",
		LeaseDuration:  30 * time.Second,
		MaxAttempts:    7,
		Limit:          10,
	}
	normalized, err := validateClaimProjectionJobs(command)
	if err != nil {
		t.Fatalf("validate claim: %v", err)
	}
	if normalized != command {
		t.Fatalf("normalized claim=%#v, want %#v", normalized, command)
	}

	tests := []struct {
		name   string
		mutate func(*ClaimProjectionJobsCommand)
	}{
		{name: "missing space", mutate: func(value *ClaimProjectionJobsCommand) { value.EmbeddingSpace = "" }},
		{name: "missing owner", mutate: func(value *ClaimProjectionJobsCommand) { value.LeaseOwner = " " }},
		{name: "zero duration", mutate: func(value *ClaimProjectionJobsCommand) { value.LeaseDuration = 0 }},
		{name: "sub-microsecond duration", mutate: func(value *ClaimProjectionJobsCommand) { value.LeaseDuration = time.Nanosecond }},
		{name: "excess duration", mutate: func(value *ClaimProjectionJobsCommand) { value.LeaseDuration = maxProjectionLease + time.Nanosecond }},
		{name: "negative attempts", mutate: func(value *ClaimProjectionJobsCommand) { value.MaxAttempts = -1 }},
		{name: "excess attempts", mutate: func(value *ClaimProjectionJobsCommand) { value.MaxAttempts = maxProjectionAttempts + 1 }},
		{name: "zero limit", mutate: func(value *ClaimProjectionJobsCommand) { value.Limit = 0 }},
		{name: "excess limit", mutate: func(value *ClaimProjectionJobsCommand) { value.Limit = maxProjectionClaimLimit + 1 }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			value := command
			test.mutate(&value)
			if _, err := validateClaimProjectionJobs(value); !errors.Is(err, domain.ErrInvalid) {
				t.Fatalf("error=%v, want invalid", err)
			}
		})
	}
	withDefaultAttempts := command
	withDefaultAttempts.MaxAttempts = 0
	normalized, err = validateClaimProjectionJobs(withDefaultAttempts)
	if err != nil || normalized.MaxAttempts != defaultProjectionMaxAttempts {
		t.Fatalf("default max attempts=%d error=%v", normalized.MaxAttempts, err)
	}
}

func TestValidateFinalizeProjectionJobRequiresFencedFiniteVector(t *testing.T) {
	command := FinalizeProjectionJobCommand{
		JobID:          1,
		TenantID:       "tenant-unit",
		UserID:         "user-unit",
		EmbeddingSpace: "space_v1_worker_unit",
		LeaseOwner:     "worker-unit",
		LeaseVersion:   2,
		DocumentSHA256: strings.Repeat("a", 64),
		Vector:         workerUnitVector(),
	}
	if _, _, err := validateFinalizeProjectionJob(command); err != nil {
		t.Fatalf("validate finalize: %v", err)
	}

	tests := []struct {
		name   string
		mutate func(*FinalizeProjectionJobCommand)
	}{
		{name: "invalid job", mutate: func(value *FinalizeProjectionJobCommand) { value.JobID = 0 }},
		{name: "invalid version", mutate: func(value *FinalizeProjectionJobCommand) { value.LeaseVersion = 0 }},
		{name: "invalid space", mutate: func(value *FinalizeProjectionJobCommand) { value.EmbeddingSpace = " " }},
		{name: "invalid sha", mutate: func(value *FinalizeProjectionJobCommand) { value.DocumentSHA256 = "provider raw response" }},
		{name: "wrong dimension", mutate: func(value *FinalizeProjectionJobCommand) { value.Vector = []float32{1} }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			value := command
			test.mutate(&value)
			if _, _, err := validateFinalizeProjectionJob(value); !errors.Is(err, domain.ErrInvalid) {
				t.Fatalf("error=%v, want invalid", err)
			}
		})
	}
}

func TestValidateProjectionFailureTransitionsAcceptOnlyClosedErrorCodes(t *testing.T) {
	retry := RetryProjectionJobCommand{
		JobID: 1, TenantID: "tenant-unit", UserID: "user-unit",
		LeaseOwner: "worker-unit", LeaseVersion: 1,
		ErrorCode: ProjectionErrorProviderTimeout, RetryAfter: time.Second,
	}
	if _, err := validateRetryProjectionJob(retry); err != nil {
		t.Fatalf("validate retry: %v", err)
	}
	retry.ErrorCode = ProjectionErrorCode("timeout: secret endpoint http://127.0.0.1")
	if _, err := validateRetryProjectionJob(retry); !errors.Is(err, domain.ErrInvalid) {
		t.Fatalf("raw error code error=%v, want invalid", err)
	}

	dead := DeadLetterProjectionJobCommand{
		JobID: 1, TenantID: "tenant-unit", UserID: "user-unit",
		LeaseOwner: "worker-unit", LeaseVersion: 1,
		ErrorCode: ProjectionErrorAttemptsExhausted,
	}
	if _, err := validateDeadLetterProjectionJob(dead); err != nil {
		t.Fatalf("validate dead letter: %v", err)
	}
	dead.ErrorCode = ""
	if _, err := validateDeadLetterProjectionJob(dead); !errors.Is(err, domain.ErrInvalid) {
		t.Fatalf("empty dead error=%v, want invalid", err)
	}

	cancel := CancelProjectionJobCommand{
		JobID: 1, TenantID: "tenant-unit", UserID: "user-unit",
		LeaseOwner: "worker-unit", LeaseVersion: 1,
	}
	if _, err := validateCancelProjectionJob(cancel); err != nil {
		t.Fatalf("validate cancel: %v", err)
	}
}

func TestProjectionLeaseLostSentinelIsStable(t *testing.T) {
	err := projectionLeaseLost("fixture disappeared")
	if !errors.Is(err, ErrProjectionLeaseLost) {
		t.Fatalf("error=%v, want lease-lost sentinel", err)
	}
	if errors.Is(err, domain.ErrNotFound) || errors.Is(err, domain.ErrConflict) {
		t.Fatalf("lease-lost error aliases an unrelated domain error: %v", err)
	}
}

func TestProjectionMemoryReadErrorRedactsLegacyScannerDiagnostics(t *testing.T) {
	const secret = "postgres://worker:secret-password@internal-db/projections"
	legacy := fmt.Errorf("scan memory: %w", errors.New("dial "+secret))
	mapped := redactProjectionMemoryReadError(legacy)
	if strings.Contains(mapped.Error(), secret) || strings.Contains(mapped.Error(), "secret-password") {
		t.Fatalf("mapped memory read error leaked legacy diagnostic: %v", mapped)
	}
	if !errors.Is(mapped, domain.ErrInvariant) {
		t.Fatalf("mapped memory read error=%v, want invariant", mapped)
	}
}

func TestProjectionWorkerCommandsCannotSupplyWallClockAuthority(t *testing.T) {
	wallClockType := reflect.TypeOf(time.Time{})
	commands := []any{
		ClaimProjectionJobsCommand{},
		FinalizeProjectionJobCommand{},
		RetryProjectionJobCommand{},
		DeadLetterProjectionJobCommand{},
		CancelProjectionJobCommand{},
	}
	for _, command := range commands {
		commandType := reflect.TypeOf(command)
		for index := 0; index < commandType.NumField(); index++ {
			field := commandType.Field(index)
			if field.Type == wallClockType {
				t.Fatalf("%s exposes caller wall-clock field %s", commandType.Name(), field.Name)
			}
		}
	}
}

func workerUnitVector() []float32 {
	vector := make([]float32, VectorDimension)
	vector[0] = 1
	return vector
}
