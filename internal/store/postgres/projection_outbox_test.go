package postgres

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/kai443/go-agent-memory-system/internal/domain"
)

func TestValidateRegisterProjectionTargetCanonicalizesPublicMetadata(t *testing.T) {
	command := validRegisterProjectionTargetCommandForUnitTest()
	command.Space.ModelFingerprint = strings.ToUpper(command.Space.ModelFingerprint)
	command.Space.CreatedAt = time.Date(2026, time.August, 19, 16, 0, 0, 123456789, time.FixedZone("test", 8*60*60))
	command.CreatedAt = command.Space.CreatedAt.Add(time.Second)

	normalized, err := validateRegisterProjectionTarget(command)
	if err != nil {
		t.Fatalf("validate projection target registration: %v", err)
	}
	if normalized.Space.ModelFingerprint != strings.ToLower(command.Space.ModelFingerprint) {
		t.Fatalf("model fingerprint=%q, want lowercase", normalized.Space.ModelFingerprint)
	}
	for label, value := range map[string]time.Time{
		"space created_at":  normalized.Space.CreatedAt,
		"target created_at": normalized.CreatedAt,
	} {
		if value.Location() != time.UTC || value.Nanosecond()%1000 != 0 {
			t.Fatalf("%s=%s, want canonical UTC microseconds", label, value.Format(time.RFC3339Nano))
		}
	}
}

func TestValidateRegisterProjectionTargetRejectsInvalidValues(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*RegisterProjectionTargetCommand)
	}{
		{name: "blank space", mutate: func(value *RegisterProjectionTargetCommand) { value.Space.ID = " " }},
		{name: "space surrounding whitespace", mutate: func(value *RegisterProjectionTargetCommand) { value.Space.ID = " space" }},
		{name: "space control character", mutate: func(value *RegisterProjectionTargetCommand) { value.Space.ID = "space\nsecret" }},
		{name: "blank provider", mutate: func(value *RegisterProjectionTargetCommand) { value.Space.Provider = "" }},
		{name: "blank model", mutate: func(value *RegisterProjectionTargetCommand) { value.Space.Model = "" }},
		{name: "wrong dimension", mutate: func(value *RegisterProjectionTargetCommand) { value.Space.Dimension = VectorDimension - 1 }},
		{name: "blank document version", mutate: func(value *RegisterProjectionTargetCommand) { value.Space.DocumentVersion = "" }},
		{name: "blank query version", mutate: func(value *RegisterProjectionTargetCommand) { value.Space.QueryVersion = "" }},
		{name: "bad fingerprint", mutate: func(value *RegisterProjectionTargetCommand) { value.Space.ModelFingerprint = "not-a-sha" }},
		{name: "missing space created at", mutate: func(value *RegisterProjectionTargetCommand) { value.Space.CreatedAt = time.Time{} }},
		{name: "invalid state", mutate: func(value *RegisterProjectionTargetCommand) { value.State = "warming" }},
		{name: "initial serving", mutate: func(value *RegisterProjectionTargetCommand) {
			value.State, value.EnqueueNew = ProjectionTargetServing, true
		}},
		{name: "blocked enqueues", mutate: func(value *RegisterProjectionTargetCommand) {
			value.State, value.EnqueueNew = ProjectionTargetBlocked, true
		}},
		{name: "retired enqueues", mutate: func(value *RegisterProjectionTargetCommand) {
			value.State, value.EnqueueNew = ProjectionTargetRetired, true
		}},
		{name: "missing target created at", mutate: func(value *RegisterProjectionTargetCommand) { value.CreatedAt = time.Time{} }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			value := validRegisterProjectionTargetCommandForUnitTest()
			test.mutate(&value)
			if _, err := validateRegisterProjectionTarget(value); !errors.Is(err, domain.ErrInvalid) {
				t.Fatalf("validation error=%v, want domain.ErrInvalid", err)
			}
		})
	}
}

func TestValidateProjectionTargetStates(t *testing.T) {
	valid := []struct {
		state      ProjectionTargetState
		enqueueNew bool
	}{
		{state: ProjectionTargetShadow, enqueueNew: false},
		{state: ProjectionTargetShadow, enqueueNew: true},
		{state: ProjectionTargetServing, enqueueNew: false},
		{state: ProjectionTargetServing, enqueueNew: true},
		{state: ProjectionTargetBlocked, enqueueNew: false},
		{state: ProjectionTargetRetired, enqueueNew: false},
	}
	for _, value := range valid {
		if err := validateProjectionTargetSettings(value.state, value.enqueueNew); err != nil {
			t.Fatalf("state=%q enqueue=%t: %v", value.state, value.enqueueNew, err)
		}
	}
}

func TestValidateSetProjectionTargetCanonicalizesAndRejectsStaleShape(t *testing.T) {
	value := SetProjectionTargetCommand{
		EmbeddingSpace: "space_v1_unit",
		State:          ProjectionTargetServing,
		EnqueueNew:     true,
		UpdatedAt:      time.Date(2026, time.August, 19, 16, 0, 0, 987654321, time.FixedZone("test", 8*60*60)),
	}
	normalized, err := validateSetProjectionTarget(value)
	if err != nil {
		t.Fatalf("validate target update: %v", err)
	}
	if normalized.UpdatedAt.Location() != time.UTC || normalized.UpdatedAt.Nanosecond()%1000 != 0 {
		t.Fatalf("updated_at=%s, want canonical UTC microseconds", normalized.UpdatedAt.Format(time.RFC3339Nano))
	}
	value.UpdatedAt = time.Time{}
	if _, err := validateSetProjectionTarget(value); !errors.Is(err, domain.ErrInvalid) {
		t.Fatalf("missing updated_at error=%v, want invalid", err)
	}
}

func TestValidateProjectionJobFilterIsScopeAndStateStrict(t *testing.T) {
	filter := ProjectionJobFilter{
		EmbeddingSpace: "space_v1_unit",
		TenantID:       "tenant-unit",
		UserID:         "user-unit",
		States:         []ProjectionJobState{ProjectionJobPending, ProjectionJobRetry},
		Limit:          100,
	}
	_, states, err := validateProjectionJobFilter(filter)
	if err != nil {
		t.Fatalf("validate job filter: %v", err)
	}
	if got := strings.Join(states, ","); got != "pending,retry" {
		t.Fatalf("states=%q, want pending,retry", got)
	}

	tests := []struct {
		name   string
		mutate func(*ProjectionJobFilter)
	}{
		{name: "partial scope", mutate: func(value *ProjectionJobFilter) { value.UserID = "" }},
		{name: "whitespace scope", mutate: func(value *ProjectionJobFilter) { value.TenantID, value.UserID = " ", "\t" }},
		{name: "unknown state", mutate: func(value *ProjectionJobFilter) { value.States = []ProjectionJobState{"failed"} }},
		{name: "duplicate state", mutate: func(value *ProjectionJobFilter) {
			value.States = []ProjectionJobState{ProjectionJobPending, ProjectionJobPending}
		}},
		{name: "zero limit", mutate: func(value *ProjectionJobFilter) { value.Limit = 0 }},
		{name: "excessive limit", mutate: func(value *ProjectionJobFilter) { value.Limit = 1001 }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			value := filter
			test.mutate(&value)
			if _, _, err := validateProjectionJobFilter(value); !errors.Is(err, domain.ErrInvalid) {
				t.Fatalf("validation error=%v, want invalid", err)
			}
		})
	}
}

func TestValidateProjectionErrorCodeRejectsRawMessages(t *testing.T) {
	for _, value := range []ProjectionErrorCode{
		ProjectionErrorTransport,
		ProjectionErrorProviderTimeout,
		ProjectionErrorProviderRateLimit,
		ProjectionErrorProviderUnavailable,
		ProjectionErrorProviderRejected,
		ProjectionErrorInvalidResponse,
		ProjectionErrorModelMismatch,
		ProjectionErrorDimensionMismatch,
		ProjectionErrorNonFiniteVector,
		ProjectionErrorSpaceConflict,
		ProjectionErrorAttemptsExhausted,
	} {
		if err := validateProjectionErrorCode(value); err != nil {
			t.Fatalf("valid code %q: %v", value, err)
		}
	}
	for _, value := range []ProjectionErrorCode{
		"",
		"1timeout",
		"ProviderTimeout",
		"provider timeout",
		"https://host/token",
		"timeout: raw body",
		"provider_api_key_super_secret",
	} {
		if err := validateProjectionErrorCode(value); !errors.Is(err, domain.ErrInvalid) {
			t.Fatalf("invalid code %q error=%v, want invalid", value, err)
		}
	}
}

func TestProjectionRepositoryErrorsDoNotExposeRawDatabaseDetails(t *testing.T) {
	const secret = "postgres://user:super-secret@db.example/agent_memory"
	tests := []error{
		errors.New("dial " + secret),
		&pgconn.PgError{Code: "XX000", Message: "internal failure " + secret, Detail: secret},
		&pgconn.PgError{Code: "23505", Message: "duplicate " + secret, Detail: secret},
	}
	for _, raw := range tests {
		mapped := mapProjectionPostgresError("test projection operation", raw)
		if strings.Contains(mapped.Error(), secret) || strings.Contains(mapped.Error(), "super-secret") {
			t.Fatalf("mapped error exposed raw database details: %q", mapped)
		}
	}
	if err := mapProjectionPostgresError("cancelled operation", context.Canceled); !errors.Is(err, context.Canceled) {
		t.Fatalf("context cancellation error=%v, want context.Canceled", err)
	}
}

func TestProjectionJobPublicShapeHasNoRawPayloadOrSecretFields(t *testing.T) {
	typeOfJob := reflect.TypeOf(ProjectionJob{})
	for index := 0; index < typeOfJob.NumField(); index++ {
		name := strings.ToLower(typeOfJob.Field(index).Name)
		for _, forbidden := range []string{"secret", "credential", "url", "document", "vector", "response", "errorraw", "errortext", "errormessage"} {
			if strings.Contains(name, forbidden) {
				t.Fatalf("ProjectionJob exposes forbidden field %q", typeOfJob.Field(index).Name)
			}
		}
	}
}

func TestValidateStoredProjectionJobChecksStateRelationships(t *testing.T) {
	valid := validProjectionJobForUnitTest()
	if err := validateStoredProjectionJob(valid); err != nil {
		t.Fatalf("valid stored job: %v", err)
	}

	tests := []struct {
		name   string
		mutate func(*ProjectionJob)
	}{
		{name: "leased without owner", mutate: func(value *ProjectionJob) {
			value.State, value.AttemptCount, value.LeaseVersion = ProjectionJobLeased, 1, 1
			leaseUntil := value.UpdatedAt.Add(time.Minute)
			value.LeaseUntil = &leaseUntil
		}},
		{name: "non leased with owner", mutate: func(value *ProjectionJob) { value.LeaseOwner = "worker-1" }},
		{name: "terminal without completion", mutate: func(value *ProjectionJob) {
			value.State, value.AttemptCount = ProjectionJobSucceeded, 1
		}},
		{name: "pending with completion", mutate: func(value *ProjectionJob) {
			completed := value.UpdatedAt
			value.CompletedAt = &completed
		}},
		{name: "dead without stable error", mutate: func(value *ProjectionJob) {
			value.State, value.AttemptCount = ProjectionJobDead, 1
			completed := value.UpdatedAt
			value.CompletedAt = &completed
		}},
		{name: "partial error pair", mutate: func(value *ProjectionJob) { value.LastErrorCode = "provider_timeout" }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			value := valid
			test.mutate(&value)
			if err := validateStoredProjectionJob(value); !errors.Is(err, domain.ErrInvariant) {
				t.Fatalf("stored validation error=%v, want invariant", err)
			}
		})
	}
}

func validRegisterProjectionTargetCommandForUnitTest() RegisterProjectionTargetCommand {
	createdAt := time.Date(2026, time.August, 19, 8, 0, 0, 123456000, time.UTC)
	return RegisterProjectionTargetCommand{
		Space: EmbeddingSpaceDefinition{
			ID:               "space_v1_" + strings.Repeat("a", 64),
			Provider:         "lmstudio",
			Model:            "text-embedding-bge-m3",
			Dimension:        VectorDimension,
			DocumentVersion:  "memory-card-document-v1",
			QueryVersion:     "raw-query-v1",
			ModelFingerprint: strings.Repeat("b", 64),
			CreatedAt:        createdAt,
		},
		State:      ProjectionTargetShadow,
		EnqueueNew: true,
		CreatedAt:  createdAt,
	}
}

func validProjectionJobForUnitTest() ProjectionJob {
	createdAt := time.Date(2026, time.August, 19, 8, 0, 0, 123456000, time.UTC)
	return ProjectionJob{
		ID:                    1,
		TenantID:              "tenant-unit",
		UserID:                "user-unit",
		MemoryID:              "memory-unit",
		EmbeddingSpace:        "space_v1_unit",
		ExpectedMemoryVersion: 1,
		State:                 ProjectionJobPending,
		AvailableAt:           createdAt,
		CreatedAt:             createdAt,
		UpdatedAt:             createdAt,
	}
}
