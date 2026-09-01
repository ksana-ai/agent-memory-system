package postgres

import (
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/ksana-ai/agent-memory-system/internal/domain"
	"github.com/ksana-ai/agent-memory-system/internal/embedding"
)

const promotionUnitOperationID = "018f47a2-6d9c-7f31-8b52-4e1a9c03d671"

func TestProjectionPromotionPinsReadCommittedForPostLockSnapshot(t *testing.T) {
	options := projectionPromotionTxOptions()
	if options.IsoLevel != pgx.ReadCommitted {
		t.Fatalf("promotion isolation=%q, want READ COMMITTED", options.IsoLevel)
	}
}

func TestValidatePromoteProjectionCommandUsesExplicitCASAndCanonicalUUID(t *testing.T) {
	valid := PromoteProjectionCommand{
		OperationID: promotionUnitOperationID,
		ToSpace:     "space_v1_destination",
		AllowEmpty:  true,
	}
	if got, err := validatePromoteProjectionCommand(valid); err != nil || got != valid {
		t.Fatalf("valid promotion command=%#v error=%v", got, err)
	}
	withSource := valid
	withSource.ExpectedFrom = "space_v1_source"
	if _, err := validatePromoteProjectionCommand(withSource); err != nil {
		t.Fatalf("valid sourced promotion: %v", err)
	}

	tests := []struct {
		name   string
		mutate func(*PromoteProjectionCommand)
	}{
		{name: "blank operation", mutate: func(value *PromoteProjectionCommand) { value.OperationID = "" }},
		{name: "uppercase uuid", mutate: func(value *PromoteProjectionCommand) { value.OperationID = strings.ToUpper(value.OperationID) }},
		{name: "uuid without separators", mutate: func(value *PromoteProjectionCommand) {
			value.OperationID = strings.ReplaceAll(value.OperationID, "-", "")
		}},
		{name: "semantic operation", mutate: func(value *PromoteProjectionCommand) { value.OperationID = "deploy-customer-secret" }},
		{name: "blank destination", mutate: func(value *PromoteProjectionCommand) { value.ToSpace = " " }},
		{name: "invalid source", mutate: func(value *PromoteProjectionCommand) { value.ExpectedFrom = " source" }},
		{name: "same source and destination", mutate: func(value *PromoteProjectionCommand) { value.ExpectedFrom = value.ToSpace }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			value := valid
			test.mutate(&value)
			if _, err := validatePromoteProjectionCommand(value); !errors.Is(err, domain.ErrInvalid) {
				t.Fatalf("validation error=%v, want invalid", err)
			}
		})
	}
}

func TestProjectionPromotionCoverageRequiresSucceededCurrentHash(t *testing.T) {
	card := projectionPromotionCard{
		key:     projectionPromotionKey{tenantID: "tenant", userID: "user", memoryID: "memory"},
		version: 2,
		memory: domain.MemoryCard{
			Kind: domain.MemoryKindSemantic, Category: "preference", Key: "editor", Value: "vim",
		},
	}
	expectedHash := embedding.MemoryCardDocumentV1SHA256(card.memory)
	succeeded := projectionPromotionJob{expectedMemoryVersion: 2, state: ProjectionJobSucceeded}
	tests := []struct {
		name       string
		jobs       map[projectionPromotionKey]projectionPromotionJob
		embeddings map[projectionPromotionKey]string
		want       int64
	}{
		{name: "missing job", jobs: map[projectionPromotionKey]projectionPromotionJob{}, embeddings: map[projectionPromotionKey]string{card.key: expectedHash}},
		{name: "pending", jobs: map[projectionPromotionKey]projectionPromotionJob{card.key: {expectedMemoryVersion: 2, state: ProjectionJobPending}}, embeddings: map[projectionPromotionKey]string{card.key: expectedHash}},
		{name: "dead", jobs: map[projectionPromotionKey]projectionPromotionJob{card.key: {expectedMemoryVersion: 2, state: ProjectionJobDead}}, embeddings: map[projectionPromotionKey]string{card.key: expectedHash}},
		{name: "cancelled", jobs: map[projectionPromotionKey]projectionPromotionJob{card.key: {expectedMemoryVersion: 2, state: ProjectionJobCancelled}}, embeddings: map[projectionPromotionKey]string{card.key: expectedHash}},
		{name: "succeeded missing vector", jobs: map[projectionPromotionKey]projectionPromotionJob{card.key: succeeded}, embeddings: map[projectionPromotionKey]string{}},
		{name: "hash mismatch", jobs: map[projectionPromotionKey]projectionPromotionJob{card.key: succeeded}, embeddings: map[projectionPromotionKey]string{card.key: strings.Repeat("f", 64)}},
		{name: "version mismatch", jobs: map[projectionPromotionKey]projectionPromotionJob{card.key: {expectedMemoryVersion: 1, state: ProjectionJobSucceeded}}, embeddings: map[projectionPromotionKey]string{card.key: expectedHash}},
		{name: "covered", jobs: map[projectionPromotionKey]projectionPromotionJob{card.key: succeeded}, embeddings: map[projectionPromotionKey]string{card.key: expectedHash}, want: 1},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := countCoveredProjectionPromotionCards([]projectionPromotionCard{card}, test.jobs, test.embeddings); got != test.want {
				t.Fatalf("covered=%d, want %d", got, test.want)
			}
		})
	}
}

func TestProjectionPromotionLiveScopesAreStableAndOrdered(t *testing.T) {
	locked := []projectionPromotionKey{
		{tenantID: "a", userID: "expired"},
		{tenantID: "b", userID: "live"},
		{tenantID: "c", userID: "live"},
	}
	cards := []projectionPromotionCard{
		{key: projectionPromotionKey{tenantID: "b", userID: "live", memoryID: "one"}},
		{key: projectionPromotionKey{tenantID: "b", userID: "live", memoryID: "two"}},
		{key: projectionPromotionKey{tenantID: "c", userID: "live", memoryID: "three"}},
	}
	live, err := projectionPromotionLiveScopes(locked, cards)
	if err != nil {
		t.Fatalf("derive live scopes: %v", err)
	}
	want := []projectionPromotionKey{{tenantID: "b", userID: "live"}, {tenantID: "c", userID: "live"}}
	if !reflect.DeepEqual(live, want) {
		t.Fatalf("live scopes=%#v, want %#v", live, want)
	}
	cards = append(cards, projectionPromotionCard{key: projectionPromotionKey{tenantID: "missing", userID: "scope", memoryID: "four"}})
	if _, err := projectionPromotionLiveScopes(locked, cards); !errors.Is(err, domain.ErrInvariant) {
		t.Fatalf("unlocked scope error=%v, want invariant", err)
	}
}

func TestValidateStoredProjectionPromotionReceiptChecksAuditFacts(t *testing.T) {
	cutoff := time.Date(2026, time.August, 20, 10, 0, 0, 0, time.UTC)
	valid := ProjectionPromotionReceipt{
		OperationID: promotionUnitOperationID, ToSpace: "space_v1_destination",
		AllowEmpty: true, PreviousGeneration: 3, Generation: 4,
		CutoffAt: cutoff, PromotedAt: cutoff.Add(time.Microsecond),
	}
	if err := validateStoredProjectionPromotionReceipt(valid); err != nil {
		t.Fatalf("valid stored receipt: %v", err)
	}
	tests := []struct {
		name   string
		mutate func(*ProjectionPromotionReceipt)
	}{
		{name: "incomplete coverage", mutate: func(value *ProjectionPromotionReceipt) { value.LiveCardCount, value.CoveredCardCount = 2, 1 }},
		{name: "scope exceeds cards", mutate: func(value *ProjectionPromotionReceipt) { value.LiveScopeCount = 1 }},
		{name: "empty unauthorized", mutate: func(value *ProjectionPromotionReceipt) { value.AllowEmpty = false }},
		{name: "generation gap", mutate: func(value *ProjectionPromotionReceipt) { value.Generation++ }},
		{name: "time reversal", mutate: func(value *ProjectionPromotionReceipt) { value.PromotedAt = value.CutoffAt.Add(-time.Microsecond) }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			value := valid
			test.mutate(&value)
			if err := validateStoredProjectionPromotionReceipt(value); !errors.Is(err, domain.ErrInvariant) {
				t.Fatalf("stored receipt error=%v, want invariant", err)
			}
		})
	}
}

func TestProjectionPromotionPublicShapeContainsOnlyAggregateAuditData(t *testing.T) {
	for _, value := range []any{PromoteProjectionCommand{}, ProjectionPromotionReceipt{}, ServingProjectionState{}} {
		typeOfValue := reflect.TypeOf(value)
		for index := 0; index < typeOfValue.NumField(); index++ {
			name := strings.ToLower(typeOfValue.Field(index).Name)
			for _, forbidden := range []string{"document", "value", "backstory", "dsn", "url", "credential", "secret", "response", "rawerror", "vector"} {
				if strings.Contains(name, forbidden) {
					t.Fatalf("%s exposes forbidden field %q", typeOfValue.Name(), typeOfValue.Field(index).Name)
				}
			}
		}
	}
}
