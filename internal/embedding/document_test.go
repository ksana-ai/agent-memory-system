package embedding

import (
	"errors"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/ksana-ai/agent-memory-system/internal/domain"
)

func TestMemoryCardDocumentV1ChineseGoldenAndSHA256(t *testing.T) {
	card := domain.MemoryCard{
		Kind:         domain.MemoryKindSemantic,
		Category:     "旅行偏好",
		Key:          "座位",
		Value:        "靠窗",
		Person:       "小明",
		Relationship: "本人",
		Backstory:    "容易晕车\n需要看窗外",
	}
	want := "" +
		"document_version=\"memory-card-document-v1\"\n" +
		"kind=\"semantic\"\n" +
		"category=\"旅行偏好\"\n" +
		"key=\"座位\"\n" +
		"value=\"靠窗\"\n" +
		"person=\"小明\"\n" +
		"relationship=\"本人\"\n" +
		"backstory=\"容易晕车\\n需要看窗外\"\n"
	document := MemoryCardDocumentV1(card)
	if document != want {
		t.Fatalf("document:\n%s\nwant:\n%s", document, want)
	}
	const wantSHA256 = "70ac7524c8e7f76d793e0193b4aa43e7bc2b926aa4c3edd1869436fdf18a23b5"
	if got := DocumentSHA256(document); got != wantSHA256 {
		t.Fatalf("SHA-256 = %s, want %s", got, wantSHA256)
	}
	if got := MemoryCardDocumentV1SHA256(card); got != wantSHA256 {
		t.Fatalf("card SHA-256 = %s, want %s", got, wantSHA256)
	}
}

func TestMemoryCardDocumentV1IsDeterministicAndExcludesOperationalFields(t *testing.T) {
	card := domain.MemoryCard{
		ID:             "card-one",
		CandidateID:    "candidate-one",
		TenantID:       "tenant-one",
		UserID:         "user-one",
		Kind:           domain.MemoryKindProcedural,
		Category:       "communication",
		Key:            "status_updates",
		Value:          "brief",
		Person:         "",
		Relationship:   "",
		Backstory:      "Prefer concise progress updates",
		SourceEventIDs: []string{"event-one"},
		Version:        1,
		Status:         domain.MemoryActive,
		CreatedAt:      time.Date(2026, 8, 19, 1, 2, 3, 0, time.UTC),
	}
	baseline := MemoryCardDocumentV1(card)
	for range 100 {
		if got := MemoryCardDocumentV1(card); got != baseline {
			t.Fatalf("document changed across calls: %q != %q", got, baseline)
		}
	}

	changedOperational := card
	changedOperational.ID = "card-two"
	changedOperational.CandidateID = "candidate-two"
	changedOperational.TenantID = "tenant-two"
	changedOperational.UserID = "user-two"
	changedOperational.SourceEventIDs = []string{"event-two", "event-three"}
	changedOperational.Version = 99
	changedOperational.Status = domain.MemorySuperseded
	changedOperational.CreatedAt = card.CreatedAt.Add(time.Hour)
	if got := MemoryCardDocumentV1(changedOperational); got != baseline {
		t.Fatalf("operational fields changed semantic document:\n%s\nwant:\n%s", got, baseline)
	}
}

func TestMemoryCardDocumentV1SemanticFieldDifferencesChangeSHA256(t *testing.T) {
	baseline := domain.MemoryCard{
		Kind:         domain.MemoryKindSemantic,
		Category:     "travel",
		Key:          "seat",
		Value:        "window",
		Person:       "self",
		Relationship: "traveler",
		Backstory:    "motion sickness",
	}
	baselineHash := MemoryCardDocumentV1SHA256(baseline)
	tests := []struct {
		name   string
		mutate func(*domain.MemoryCard)
	}{
		{name: "kind", mutate: func(card *domain.MemoryCard) { card.Kind = domain.MemoryKindProcedural }},
		{name: "category", mutate: func(card *domain.MemoryCard) { card.Category += "-changed" }},
		{name: "key", mutate: func(card *domain.MemoryCard) { card.Key += "-changed" }},
		{name: "value", mutate: func(card *domain.MemoryCard) { card.Value += "-changed" }},
		{name: "person", mutate: func(card *domain.MemoryCard) { card.Person += "-changed" }},
		{name: "relationship", mutate: func(card *domain.MemoryCard) { card.Relationship += "-changed" }},
		{name: "backstory", mutate: func(card *domain.MemoryCard) { card.Backstory += "-changed" }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			changed := baseline
			test.mutate(&changed)
			if got := MemoryCardDocumentV1SHA256(changed); got == baselineHash {
				t.Fatalf("%s difference did not change hash %s", test.name, got)
			}
		})
	}
}

func TestProbeTextV1SHA256Golden(t *testing.T) {
	if got := DocumentSHA256(ProbeTextV1); got != ProbeTextV1SHA256 {
		t.Fatalf("probe text SHA-256 = %s, want %s", got, ProbeTextV1SHA256)
	}
}

func TestVectorSHA256UsesIEEE754BigEndianGolden(t *testing.T) {
	vector := []float32{1, -2.5, 0, float32(math.Copysign(0, -1))}
	const want = "2ae6147499e414cb29b626701532c534f03728e8c6e58500d2d943f94649a1f6"
	if got := VectorSHA256(vector); got != want {
		t.Fatalf("vector SHA-256 = %s, want %s", got, want)
	}
	if VectorSHA256([]float32{0}) == VectorSHA256([]float32{float32(math.Copysign(0, -1))}) {
		t.Fatal("vector hash did not preserve negative-zero IEEE 754 bits")
	}
}

func TestSpaceIDIsStableAndEveryFieldContributes(t *testing.T) {
	type parameters struct {
		provider         string
		model            string
		dimension        int
		documentVersion  string
		queryVersion     string
		modelFingerprint string
	}
	baseline := parameters{
		provider:         ProviderLMStudio,
		model:            testModel,
		dimension:        DefaultDimension,
		documentVersion:  MemoryCardDocumentVersion,
		queryVersion:     RawQueryVersion,
		modelFingerprint: strings.Repeat("a", 64),
	}
	spaceID := func(value parameters) (string, error) {
		return SpaceID(value.provider, value.model, value.dimension, value.documentVersion, value.queryVersion, value.modelFingerprint)
	}
	want, err := spaceID(baseline)
	if err != nil {
		t.Fatalf("space ID: %v", err)
	}
	if !strings.HasPrefix(want, "space_v1_") || len(want) != len("space_v1_")+64 {
		t.Fatalf("space ID shape = %q", want)
	}
	for range 100 {
		got, err := spaceID(baseline)
		if err != nil || got != want {
			t.Fatalf("space ID instability: got=%q err=%v want=%q", got, err, want)
		}
	}

	mutations := []struct {
		name   string
		mutate func(*parameters)
	}{
		{name: "provider", mutate: func(value *parameters) { value.provider += "x" }},
		{name: "model", mutate: func(value *parameters) { value.model += "x" }},
		{name: "dimension", mutate: func(value *parameters) { value.dimension++ }},
		{name: "document version", mutate: func(value *parameters) { value.documentVersion += "x" }},
		{name: "query version", mutate: func(value *parameters) { value.queryVersion += "x" }},
		{name: "model fingerprint", mutate: func(value *parameters) { value.modelFingerprint = strings.Repeat("b", 64) }},
	}
	for _, mutation := range mutations {
		t.Run(mutation.name, func(t *testing.T) {
			changed := baseline
			mutation.mutate(&changed)
			got, err := spaceID(changed)
			if err != nil {
				t.Fatalf("space ID: %v", err)
			}
			if got == want {
				t.Fatalf("mutation did not change space ID %q", got)
			}
		})
	}
}

func TestSpaceIDLengthPrefixPreventsConcatenationCollision(t *testing.T) {
	left, err := SpaceID("ab", "c", 1, "d", "e", "f")
	if err != nil {
		t.Fatalf("left space ID: %v", err)
	}
	right, err := SpaceID("a", "bc", 1, "d", "e", "f")
	if err != nil {
		t.Fatalf("right space ID: %v", err)
	}
	if left == right {
		t.Fatalf("length-prefix collision: %q", left)
	}
}

func TestSpaceIDRejectsMissingComponents(t *testing.T) {
	tests := []struct {
		name             string
		provider         string
		model            string
		dimension        int
		documentVersion  string
		queryVersion     string
		modelFingerprint string
	}{
		{name: "provider", model: "m", dimension: 1, documentVersion: "d", queryVersion: "q", modelFingerprint: "f"},
		{name: "model", provider: "p", dimension: 1, documentVersion: "d", queryVersion: "q", modelFingerprint: "f"},
		{name: "dimension", provider: "p", model: "m", dimension: 0, documentVersion: "d", queryVersion: "q", modelFingerprint: "f"},
		{name: "document version", provider: "p", model: "m", dimension: 1, queryVersion: "q", modelFingerprint: "f"},
		{name: "query version", provider: "p", model: "m", dimension: 1, documentVersion: "d", modelFingerprint: "f"},
		{name: "model fingerprint", provider: "p", model: "m", dimension: 1, documentVersion: "d", queryVersion: "q"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := SpaceID(test.provider, test.model, test.dimension, test.documentVersion, test.queryVersion, test.modelFingerprint)
			if !errors.Is(err, ErrInvalidSpace) {
				t.Fatalf("error = %v, want ErrInvalidSpace", err)
			}
		})
	}
}
