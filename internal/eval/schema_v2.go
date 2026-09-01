package eval

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/ksana-ai/agent-memory-system/internal/domain"
)

const DatasetSchemaVersionV2 = "2"

const (
	OperationEvidenceAppendV2   = "evidence.append"
	OperationCandidateProposeV2 = "candidate.propose"
	OperationCandidateReviewV2  = "candidate.review"
	OperationMemoryRememberV2   = "memory.remember"
	OperationForgetUserV2       = "forget_user"
	OperationQueryV2            = "query"
)

// DatasetV2 describes ordered, multi-scope lifecycle scenarios. SHA256 is the
// digest of the exact bytes passed to LoadV2. The private fingerprint protects
// the decoded semantic value and the raw digest from mutation after loading.
type DatasetV2 struct {
	SchemaVersion string   `json:"schema_version"`
	ID            string   `json:"id"`
	Version       string   `json:"version"`
	Description   string   `json:"description"`
	Cases         []CaseV2 `json:"cases"`
	SHA256        string   `json:"-"`
	fingerprint   string
}

type CaseV2 struct {
	ID          string                `json:"id"`
	Description string                `json:"description,omitempty"`
	Tags        []string              `json:"tags,omitempty"`
	Scopes      []ScopeV2             `json:"scopes"`
	Timeline    []TimelineOperationV2 `json:"timeline"`
}

type ScopeV2 struct {
	ID       string `json:"id"`
	TenantID string `json:"tenant_id"`
	UserID   string `json:"user_id"`
}

// TimelineOperationV2 is sealed so every operation can be strictly decoded
// and exhaustively validated by this package.
type TimelineOperationV2 interface {
	Operation() string
	isTimelineOperationV2()
}

type EvidenceAppendV2 struct {
	Op        string            `json:"op"`
	As        string            `json:"as"`
	Scope     string            `json:"scope"`
	SessionID string            `json:"session_id"`
	At        time.Time         `json:"at"`
	Actor     domain.Actor      `json:"actor"`
	Content   string            `json:"content"`
	Metadata  map[string]string `json:"metadata,omitempty"`
}

func (*EvidenceAppendV2) Operation() string      { return OperationEvidenceAppendV2 }
func (*EvidenceAppendV2) isTimelineOperationV2() {}

type CandidateProposeV2 struct {
	Op               string            `json:"op"`
	As               string            `json:"as"`
	Scope            string            `json:"scope"`
	At               time.Time         `json:"at"`
	SourceEventIDs   []string          `json:"source_event_ids"`
	Memory           MemorySpecV2      `json:"memory"`
	Extractor        string            `json:"extractor"`
	ExtractorVersion string            `json:"extractor_version"`
	Metadata         map[string]string `json:"metadata,omitempty"`
}

func (*CandidateProposeV2) Operation() string      { return OperationCandidateProposeV2 }
func (*CandidateProposeV2) isTimelineOperationV2() {}

type MemorySpecV2 struct {
	Kind         domain.MemoryKind `json:"kind"`
	Category     string            `json:"category"`
	Key          string            `json:"key"`
	Value        string            `json:"value"`
	Person       string            `json:"person,omitempty"`
	Relationship string            `json:"relationship,omitempty"`
	Backstory    string            `json:"backstory,omitempty"`
	ExpiresAt    *time.Time        `json:"expires_at,omitempty"`
}

type CandidateReviewV2 struct {
	Op         string                `json:"op"`
	Candidate  string                `json:"candidate"`
	Scope      string                `json:"scope"`
	At         time.Time             `json:"at"`
	Decision   domain.ReviewDecision `json:"decision"`
	MemoryAs   string                `json:"memory_as,omitempty"`
	ReviewerID string                `json:"reviewer_id"`
	Reason     string                `json:"reason"`
}

func (*CandidateReviewV2) Operation() string      { return OperationCandidateReviewV2 }
func (*CandidateReviewV2) isTimelineOperationV2() {}

type RememberReviewStateV2 string

const (
	RememberApprovedV2 RememberReviewStateV2 = "approved"
	RememberRejectedV2 RememberReviewStateV2 = "rejected"
	RememberPendingV2  RememberReviewStateV2 = "pending"
)

// MemoryRememberV2 is the compact authoring form for a complete fixture. A
// runner expands it through the public application lifecycle: append every
// evidence fixture, propose one candidate, and approve or reject it when the
// review state requires that transition. It never authorizes direct inserts.
type MemoryRememberV2 struct {
	Op          string                `json:"op"`
	MemoryRef   string                `json:"memory_ref"`
	Scope       string                `json:"scope"`
	At          time.Time             `json:"at"`
	ReviewState RememberReviewStateV2 `json:"review_state"`
	Memory      MemorySpecV2          `json:"memory"`
	Evidence    []EvidenceFixtureV2   `json:"evidence"`
}

func (*MemoryRememberV2) Operation() string      { return OperationMemoryRememberV2 }
func (*MemoryRememberV2) isTimelineOperationV2() {}

type EvidenceFixtureV2 struct {
	Alias      string            `json:"alias"`
	SessionID  string            `json:"session_id"`
	Actor      domain.Actor      `json:"actor"`
	Content    string            `json:"content"`
	OccurredAt time.Time         `json:"occurred_at"`
	Metadata   map[string]string `json:"metadata,omitempty"`
}

type ForgetUserV2 struct {
	Op    string    `json:"op"`
	Scope string    `json:"scope"`
	At    time.Time `json:"at"`
}

func (*ForgetUserV2) Operation() string      { return OperationForgetUserV2 }
func (*ForgetUserV2) isTimelineOperationV2() {}

type QueryV2 struct {
	Op        string           `json:"op"`
	ID        string           `json:"id"`
	Scope     string           `json:"scope"`
	At        time.Time        `json:"at"`
	Text      string           `json:"text"`
	Judgments QueryJudgmentsV2 `json:"judgments"`
}

func (*QueryV2) Operation() string      { return OperationQueryV2 }
func (*QueryV2) isTimelineOperationV2() {}

// QueryJudgmentsV2 keeps heterogeneous corpora separate. An arm selects one
// of these profiles; memory and evidence aliases are never scored as if they
// were documents from the same corpus.
type QueryJudgmentsV2 struct {
	MemoryCards    *JudgmentProfileV2 `json:"memory_cards,omitempty"`
	EvidenceEvents *JudgmentProfileV2 `json:"evidence_events,omitempty"`
}

type JudgmentProfileV2 struct {
	Relevance    map[string]int `json:"relevance,omitempty"`
	Forbidden    []string       `json:"forbidden,omitempty"`
	RequireEmpty bool           `json:"require_empty,omitempty"`
}

type caseV2Wire struct {
	ID          string            `json:"id"`
	Description string            `json:"description,omitempty"`
	Tags        []string          `json:"tags,omitempty"`
	Scopes      []ScopeV2         `json:"scopes"`
	Timeline    []json.RawMessage `json:"timeline"`
}

func (testCase *CaseV2) UnmarshalJSON(data []byte) error {
	var wire caseV2Wire
	if err := decodeStrictV2(data, &wire); err != nil {
		return err
	}
	timeline := make([]TimelineOperationV2, 0, len(wire.Timeline))
	for index, raw := range wire.Timeline {
		operation, err := decodeTimelineOperationV2(raw)
		if err != nil {
			return fmt.Errorf("timeline[%d]: %w", index, err)
		}
		timeline = append(timeline, operation)
	}
	*testCase = CaseV2{
		ID:          wire.ID,
		Description: wire.Description,
		Tags:        wire.Tags,
		Scopes:      wire.Scopes,
		Timeline:    timeline,
	}
	return nil
}

func decodeTimelineOperationV2(data []byte) (TimelineOperationV2, error) {
	var envelope struct {
		Op string `json:"op"`
	}
	if err := json.Unmarshal(data, &envelope); err != nil {
		return nil, fmt.Errorf("decode operation discriminator: %w", err)
	}
	if strings.TrimSpace(envelope.Op) == "" {
		return nil, errors.New("operation needs an op discriminator")
	}

	var operation TimelineOperationV2
	switch envelope.Op {
	case OperationEvidenceAppendV2:
		operation = &EvidenceAppendV2{}
	case OperationCandidateProposeV2:
		operation = &CandidateProposeV2{}
	case OperationCandidateReviewV2:
		operation = &CandidateReviewV2{}
	case OperationMemoryRememberV2:
		operation = &MemoryRememberV2{}
	case OperationForgetUserV2:
		operation = &ForgetUserV2{}
	case OperationQueryV2:
		operation = &QueryV2{}
	default:
		return nil, fmt.Errorf("unsupported timeline operation %q", envelope.Op)
	}
	if err := decodeStrictV2(data, operation); err != nil {
		return nil, fmt.Errorf("decode %s: %w", envelope.Op, err)
	}
	return operation, nil
}

// LoadV2 strictly decodes and validates an evaluation dataset. Unknown fields
// are rejected at the dataset, case, operation, memory, and judgment levels.
func LoadV2(data []byte) (DatasetV2, error) {
	var dataset DatasetV2
	if err := decodeStrictV2(data, &dataset); err != nil {
		return DatasetV2{}, fmt.Errorf("decode v2 dataset: %w", err)
	}
	if err := validateDatasetV2(dataset); err != nil {
		return DatasetV2{}, err
	}
	digest := sha256.Sum256(data)
	dataset.SHA256 = hex.EncodeToString(digest[:])
	fingerprint, err := datasetFingerprintV2(dataset)
	if err != nil {
		return DatasetV2{}, err
	}
	dataset.fingerprint = fingerprint
	return dataset, nil
}

// VerifyIntegrity proves that the DatasetV2 is still the exact semantic value
// loaded by LoadV2 and still carries the raw-byte digest assigned at load time.
func (dataset DatasetV2) VerifyIntegrity() error {
	if err := validateDatasetV2(dataset); err != nil {
		return err
	}
	if dataset.fingerprint == "" {
		return errors.New("v2 dataset must be loaded with eval.LoadV2")
	}
	fingerprint, err := datasetFingerprintV2(dataset)
	if err != nil {
		return err
	}
	if fingerprint != dataset.fingerprint {
		return errors.New("v2 dataset changed after loading; reload it before evaluation")
	}
	return nil
}

func decodeStrictV2(data []byte, destination any) error {
	decoder := json.NewDecoder(strings.NewReader(string(data)))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(destination); err != nil {
		return err
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return errors.New("JSON contains multiple values")
	}
	return nil
}

type artifactKindV2 string

const (
	artifactEvidenceV2  artifactKindV2 = "evidence"
	artifactCandidateV2 artifactKindV2 = "candidate"
	artifactMemoryV2    artifactKindV2 = "memory"
)

type artifactStateV2 struct {
	kind        artifactKindV2
	scope       string
	deleted     bool
	serviceable bool
	identity    memoryIdentityV2
	expiresAt   *time.Time
	candidate   *candidateStateV2
}

type candidateStateV2 struct {
	reviewed bool
	memory   MemorySpecV2
}

type memoryIdentityV2 struct {
	kind         domain.MemoryKind
	category     string
	key          string
	person       string
	relationship string
}

func validateDatasetV2(dataset DatasetV2) error {
	if dataset.SchemaVersion != DatasetSchemaVersionV2 {
		return fmt.Errorf("unsupported v2 dataset schema_version %q", dataset.SchemaVersion)
	}
	if err := validateIdentifierV2("dataset id", dataset.ID); err != nil {
		return err
	}
	if err := validateRequiredTextV2("dataset version", dataset.Version, 128); err != nil {
		return err
	}
	if err := validateRequiredTextV2("dataset description", dataset.Description, 4096); err != nil {
		return err
	}
	if len(dataset.Cases) == 0 {
		return errors.New("v2 dataset must contain at least one case")
	}

	caseIDs := make(map[string]struct{}, len(dataset.Cases))
	for index, testCase := range dataset.Cases {
		if err := validateCaseV2(testCase); err != nil {
			return fmt.Errorf("case[%d] %q: %w", index, testCase.ID, err)
		}
		if _, exists := caseIDs[testCase.ID]; exists {
			return fmt.Errorf("duplicate v2 case id %q", testCase.ID)
		}
		caseIDs[testCase.ID] = struct{}{}
	}
	return nil
}

func validateCaseV2(testCase CaseV2) error {
	if err := validateIdentifierV2("case id", testCase.ID); err != nil {
		return err
	}
	if testCase.Description != "" {
		if err := validateRequiredTextV2("case description", testCase.Description, 4096); err != nil {
			return err
		}
	}
	tags := make(map[string]struct{}, len(testCase.Tags))
	for _, tag := range testCase.Tags {
		if err := validateIdentifierV2("case tag", tag); err != nil {
			return err
		}
		if _, exists := tags[tag]; exists {
			return fmt.Errorf("duplicate case tag %q", tag)
		}
		tags[tag] = struct{}{}
	}
	if len(testCase.Scopes) == 0 {
		return errors.New("case must define at least one scope")
	}
	if len(testCase.Timeline) == 0 {
		return errors.New("case must define at least one timeline operation")
	}

	scopes := make(map[string]ScopeV2, len(testCase.Scopes))
	scopeValues := make(map[string]string, len(testCase.Scopes))
	for _, scope := range testCase.Scopes {
		if err := validateIdentifierV2("scope id", scope.ID); err != nil {
			return err
		}
		if err := validateIdentifierV2("tenant id", scope.TenantID); err != nil {
			return err
		}
		if err := validateIdentifierV2("user id", scope.UserID); err != nil {
			return err
		}
		if _, exists := scopes[scope.ID]; exists {
			return fmt.Errorf("duplicate scope id %q", scope.ID)
		}
		valueKey := scope.TenantID + "\x00" + scope.UserID
		if previous, exists := scopeValues[valueKey]; exists {
			return fmt.Errorf("scopes %q and %q select the same tenant/user", previous, scope.ID)
		}
		scopes[scope.ID] = scope
		scopeValues[valueKey] = scope.ID
	}

	artifacts := make(map[string]*artifactStateV2)
	activeMemories := make(map[string]string)
	queryIDs := make(map[string]struct{})
	var previousTime time.Time
	queryCount := 0
	for index, operation := range testCase.Timeline {
		if operation == nil {
			return fmt.Errorf("timeline[%d] is nil", index)
		}
		at, err := operationTimeV2(operation)
		if err != nil {
			return fmt.Errorf("timeline[%d] %s: %w", index, operation.Operation(), err)
		}
		if at.IsZero() {
			return fmt.Errorf("timeline[%d] %s needs an explicit at timestamp", index, operation.Operation())
		}
		if !previousTime.IsZero() && at.Before(previousTime) {
			return fmt.Errorf("timeline[%d] %s timestamp %s precedes %s", index, operation.Operation(), at.Format(time.RFC3339Nano), previousTime.Format(time.RFC3339Nano))
		}
		previousTime = at

		if err := validateTimelineOperationV2(operation, scopes, artifacts, activeMemories, queryIDs); err != nil {
			return fmt.Errorf("timeline[%d] %s: %w", index, operation.Operation(), err)
		}
		if operation.Operation() == OperationQueryV2 {
			queryCount++
		}
	}
	if queryCount == 0 {
		return errors.New("case must contain at least one query")
	}
	return nil
}

func operationTimeV2(operation TimelineOperationV2) (time.Time, error) {
	switch value := operation.(type) {
	case *EvidenceAppendV2:
		if value == nil {
			return time.Time{}, errors.New("nil evidence.append operation")
		}
		return value.At, nil
	case *CandidateProposeV2:
		if value == nil {
			return time.Time{}, errors.New("nil candidate.propose operation")
		}
		return value.At, nil
	case *CandidateReviewV2:
		if value == nil {
			return time.Time{}, errors.New("nil candidate.review operation")
		}
		return value.At, nil
	case *MemoryRememberV2:
		if value == nil {
			return time.Time{}, errors.New("nil memory.remember operation")
		}
		return value.At, nil
	case *ForgetUserV2:
		if value == nil {
			return time.Time{}, errors.New("nil forget_user operation")
		}
		return value.At, nil
	case *QueryV2:
		if value == nil {
			return time.Time{}, errors.New("nil query operation")
		}
		return value.At, nil
	default:
		return time.Time{}, fmt.Errorf("unsupported in-memory operation type %T", operation)
	}
}

func validateTimelineOperationV2(
	operation TimelineOperationV2,
	scopes map[string]ScopeV2,
	artifacts map[string]*artifactStateV2,
	activeMemories map[string]string,
	queryIDs map[string]struct{},
) error {
	switch value := operation.(type) {
	case *EvidenceAppendV2:
		if value.Op != OperationEvidenceAppendV2 {
			return fmt.Errorf("op is %q, want %q", value.Op, OperationEvidenceAppendV2)
		}
		if err := validateScopeReferenceV2(value.Scope, scopes); err != nil {
			return err
		}
		if err := claimAliasV2(value.As, artifactEvidenceV2, value.Scope, artifacts); err != nil {
			return err
		}
		if err := validateIdentifierV2("session id", value.SessionID); err != nil {
			return err
		}
		if value.Actor != domain.ActorUser && value.Actor != domain.ActorAgent && value.Actor != domain.ActorTool {
			return fmt.Errorf("actor %q must be user, agent, or tool", value.Actor)
		}
		if err := validateRequiredTextV2("evidence content", value.Content, 32<<10); err != nil {
			return err
		}
		return validateMetadataV2(value.Metadata)

	case *CandidateProposeV2:
		if value.Op != OperationCandidateProposeV2 {
			return fmt.Errorf("op is %q, want %q", value.Op, OperationCandidateProposeV2)
		}
		if err := validateScopeReferenceV2(value.Scope, scopes); err != nil {
			return err
		}
		if err := validateMemorySpecV2(value.Memory); err != nil {
			return err
		}
		if err := validateRequiredTextV2("extractor", value.Extractor, 128); err != nil {
			return err
		}
		if err := validateRequiredTextV2("extractor version", value.ExtractorVersion, 128); err != nil {
			return err
		}
		if err := validateMetadataV2(value.Metadata); err != nil {
			return err
		}
		if len(value.SourceEventIDs) == 0 || len(value.SourceEventIDs) > 20 {
			return errors.New("source_event_ids must contain between 1 and 20 aliases")
		}
		seenSources := make(map[string]struct{}, len(value.SourceEventIDs))
		for _, sourceAlias := range value.SourceEventIDs {
			if _, exists := seenSources[sourceAlias]; exists {
				return fmt.Errorf("duplicate source evidence alias %q", sourceAlias)
			}
			seenSources[sourceAlias] = struct{}{}
			source, exists := artifacts[sourceAlias]
			if !exists {
				return fmt.Errorf("source evidence alias %q is referenced before use", sourceAlias)
			}
			if source.kind != artifactEvidenceV2 {
				return fmt.Errorf("source alias %q is %s, not evidence", sourceAlias, source.kind)
			}
			if source.scope != value.Scope {
				return fmt.Errorf("source evidence alias %q belongs to scope %q, not %q", sourceAlias, source.scope, value.Scope)
			}
			if source.deleted {
				return fmt.Errorf("source evidence alias %q was deleted", sourceAlias)
			}
		}
		if err := claimAliasV2(value.As, artifactCandidateV2, value.Scope, artifacts); err != nil {
			return err
		}
		artifacts[value.As].candidate = &candidateStateV2{memory: value.Memory}
		return nil

	case *CandidateReviewV2:
		if value.Op != OperationCandidateReviewV2 {
			return fmt.Errorf("op is %q, want %q", value.Op, OperationCandidateReviewV2)
		}
		if err := validateScopeReferenceV2(value.Scope, scopes); err != nil {
			return err
		}
		candidate, exists := artifacts[value.Candidate]
		if !exists {
			return fmt.Errorf("candidate alias %q is referenced before use", value.Candidate)
		}
		if candidate.kind != artifactCandidateV2 || candidate.candidate == nil {
			return fmt.Errorf("alias %q is not a candidate", value.Candidate)
		}
		if candidate.scope != value.Scope {
			return fmt.Errorf("candidate alias %q belongs to scope %q, not %q", value.Candidate, candidate.scope, value.Scope)
		}
		if candidate.deleted {
			return fmt.Errorf("candidate alias %q was deleted", value.Candidate)
		}
		if candidate.candidate.reviewed {
			return fmt.Errorf("candidate alias %q was already reviewed", value.Candidate)
		}
		if err := validateIdentifierV2("reviewer id", value.ReviewerID); err != nil {
			return err
		}
		if err := validateRequiredTextV2("review reason", value.Reason, 2<<10); err != nil {
			return err
		}
		switch value.Decision {
		case domain.DecisionApprove:
			if strings.TrimSpace(value.MemoryAs) == "" {
				return errors.New("approved candidate needs memory_as")
			}
			if err := claimAliasV2(value.MemoryAs, artifactMemoryV2, value.Scope, artifacts); err != nil {
				return err
			}
			identity := normalizedMemoryIdentityV2(candidate.candidate.memory)
			identityKey := memoryIdentityKeyV2(value.Scope, identity)
			if previousAlias, exists := activeMemories[identityKey]; exists {
				artifacts[previousAlias].serviceable = false
			}
			memory := artifacts[value.MemoryAs]
			memory.identity = identity
			memory.expiresAt = candidate.candidate.memory.ExpiresAt
			memory.serviceable = true
			activeMemories[identityKey] = value.MemoryAs
		case domain.DecisionReject:
			if strings.TrimSpace(value.MemoryAs) != "" {
				return errors.New("rejected candidate must not define memory_as")
			}
		default:
			return fmt.Errorf("review decision %q must be approve or reject", value.Decision)
		}
		candidate.candidate.reviewed = true
		return nil

	case *MemoryRememberV2:
		if value.Op != OperationMemoryRememberV2 {
			return fmt.Errorf("op is %q, want %q", value.Op, OperationMemoryRememberV2)
		}
		if err := validateScopeReferenceV2(value.Scope, scopes); err != nil {
			return err
		}
		if err := validateMemorySpecV2(value.Memory); err != nil {
			return err
		}
		if len(value.Evidence) == 0 {
			return errors.New("memory.remember needs at least one evidence fixture")
		}
		var previousEvidenceTime time.Time
		for index, fixture := range value.Evidence {
			if fixture.OccurredAt.IsZero() {
				return fmt.Errorf("evidence[%d] needs an explicit occurred_at timestamp", index)
			}
			if fixture.OccurredAt.After(value.At) {
				return fmt.Errorf("evidence[%d] occurred_at %s is after remember at %s", index, fixture.OccurredAt.Format(time.RFC3339Nano), value.At.Format(time.RFC3339Nano))
			}
			if !previousEvidenceTime.IsZero() && fixture.OccurredAt.Before(previousEvidenceTime) {
				return fmt.Errorf("evidence[%d] occurred_at precedes the prior evidence fixture", index)
			}
			previousEvidenceTime = fixture.OccurredAt
			if err := validateIdentifierV2("evidence alias", fixture.Alias); err != nil {
				return err
			}
			if err := validateIdentifierV2("session id", fixture.SessionID); err != nil {
				return err
			}
			if fixture.Actor != domain.ActorUser && fixture.Actor != domain.ActorAgent && fixture.Actor != domain.ActorTool {
				return fmt.Errorf("evidence[%d] actor %q must be user, agent, or tool", index, fixture.Actor)
			}
			if err := validateRequiredTextV2("evidence content", fixture.Content, 32<<10); err != nil {
				return err
			}
			if err := validateMetadataV2(fixture.Metadata); err != nil {
				return err
			}
			if err := claimAliasV2(fixture.Alias, artifactEvidenceV2, value.Scope, artifacts); err != nil {
				return err
			}
		}
		if err := claimAliasV2(value.MemoryRef, artifactMemoryV2, value.Scope, artifacts); err != nil {
			return err
		}
		memory := artifacts[value.MemoryRef]
		memory.identity = normalizedMemoryIdentityV2(value.Memory)
		memory.expiresAt = value.Memory.ExpiresAt
		switch value.ReviewState {
		case RememberApprovedV2:
			identityKey := memoryIdentityKeyV2(value.Scope, memory.identity)
			if previousAlias, exists := activeMemories[identityKey]; exists {
				artifacts[previousAlias].serviceable = false
			}
			memory.serviceable = true
			activeMemories[identityKey] = value.MemoryRef
		case RememberRejectedV2, RememberPendingV2:
			memory.serviceable = false
		default:
			return fmt.Errorf("review_state %q must be approved, rejected, or pending", value.ReviewState)
		}
		return nil

	case *ForgetUserV2:
		if value.Op != OperationForgetUserV2 {
			return fmt.Errorf("op is %q, want %q", value.Op, OperationForgetUserV2)
		}
		if err := validateScopeReferenceV2(value.Scope, scopes); err != nil {
			return err
		}
		for _, artifact := range artifacts {
			if artifact.scope == value.Scope {
				artifact.deleted = true
				artifact.serviceable = false
			}
		}
		for identityKey, alias := range activeMemories {
			if artifacts[alias].scope == value.Scope {
				delete(activeMemories, identityKey)
			}
		}
		return nil

	case *QueryV2:
		if value.Op != OperationQueryV2 {
			return fmt.Errorf("op is %q, want %q", value.Op, OperationQueryV2)
		}
		if err := validateIdentifierV2("query id", value.ID); err != nil {
			return err
		}
		if _, exists := queryIDs[value.ID]; exists {
			return fmt.Errorf("duplicate query id %q", value.ID)
		}
		queryIDs[value.ID] = struct{}{}
		if err := validateScopeReferenceV2(value.Scope, scopes); err != nil {
			return err
		}
		if err := validateRequiredTextV2("query text", value.Text, 4<<10); err != nil {
			return err
		}
		if value.Judgments.MemoryCards == nil {
			return errors.New("query needs a memory_cards judgment profile")
		}
		if err := validateJudgmentProfileV2("memory_cards", *value.Judgments.MemoryCards, artifactMemoryV2, value.Scope, value.At, artifacts); err != nil {
			return err
		}
		if value.Judgments.EvidenceEvents != nil {
			if err := validateJudgmentProfileV2("evidence_events", *value.Judgments.EvidenceEvents, artifactEvidenceV2, value.Scope, value.At, artifacts); err != nil {
				return err
			}
		}
		return nil

	default:
		return fmt.Errorf("unsupported in-memory operation type %T", operation)
	}
}

func validateJudgmentProfileV2(
	name string,
	profile JudgmentProfileV2,
	wantKind artifactKindV2,
	queryScope string,
	queryAt time.Time,
	artifacts map[string]*artifactStateV2,
) error {
	if len(profile.Relevance) == 0 && len(profile.Forbidden) == 0 && !profile.RequireEmpty {
		return fmt.Errorf("%s judgment profile makes no assertion", name)
	}
	if profile.RequireEmpty && len(profile.Relevance) != 0 {
		return fmt.Errorf("%s require_empty cannot be combined with positive relevance", name)
	}
	for alias, grade := range profile.Relevance {
		if grade < 1 || grade > 3 {
			return fmt.Errorf("%s relevance for %q is %d, want 1..3", name, alias, grade)
		}
		artifact, exists := artifacts[alias]
		if !exists {
			return fmt.Errorf("%s relevance alias %q is referenced before use", name, alias)
		}
		if artifact.kind != wantKind {
			return fmt.Errorf("%s relevance alias %q is %s, want %s", name, alias, artifact.kind, wantKind)
		}
		if artifact.scope != queryScope {
			return fmt.Errorf("%s relevance alias %q belongs to scope %q, not query scope %q", name, alias, artifact.scope, queryScope)
		}
		if artifact.deleted {
			return fmt.Errorf("%s relevance alias %q was deleted", name, alias)
		}
		if wantKind == artifactMemoryV2 && !artifact.serviceable {
			return fmt.Errorf("%s relevance alias %q is not an active memory", name, alias)
		}
		if wantKind == artifactMemoryV2 && artifact.expiresAt != nil && !queryAt.Before(*artifact.expiresAt) {
			return fmt.Errorf("%s relevance alias %q expired at %s before or at query %s", name, alias, artifact.expiresAt.Format(time.RFC3339Nano), queryAt.Format(time.RFC3339Nano))
		}
	}

	seenForbidden := make(map[string]struct{}, len(profile.Forbidden))
	for _, alias := range profile.Forbidden {
		if err := validateIdentifierV2(name+" forbidden alias", alias); err != nil {
			return err
		}
		if _, exists := seenForbidden[alias]; exists {
			return fmt.Errorf("%s forbidden alias %q is duplicated", name, alias)
		}
		seenForbidden[alias] = struct{}{}
		if _, overlap := profile.Relevance[alias]; overlap {
			return fmt.Errorf("%s alias %q cannot be both relevant and forbidden", name, alias)
		}
		artifact, exists := artifacts[alias]
		if !exists {
			return fmt.Errorf("%s forbidden alias %q is referenced before use", name, alias)
		}
		if artifact.kind != wantKind {
			return fmt.Errorf("%s forbidden alias %q is %s, want %s", name, alias, artifact.kind, wantKind)
		}
	}
	return nil
}

func claimAliasV2(alias string, kind artifactKindV2, scope string, artifacts map[string]*artifactStateV2) error {
	if err := validateIdentifierV2(string(kind)+" alias", alias); err != nil {
		return err
	}
	if previous, exists := artifacts[alias]; exists {
		return fmt.Errorf("alias %q is already used by %s", alias, previous.kind)
	}
	artifacts[alias] = &artifactStateV2{kind: kind, scope: scope}
	return nil
}

func validateScopeReferenceV2(scope string, scopes map[string]ScopeV2) error {
	if err := validateIdentifierV2("scope reference", scope); err != nil {
		return err
	}
	if _, exists := scopes[scope]; !exists {
		return fmt.Errorf("scope %q is not defined", scope)
	}
	return nil
}

func validateMemorySpecV2(memory MemorySpecV2) error {
	if memory.Kind != domain.MemoryKindEpisodic && memory.Kind != domain.MemoryKindSemantic && memory.Kind != domain.MemoryKindProcedural {
		return fmt.Errorf("memory kind %q must be episodic, semantic, or procedural", memory.Kind)
	}
	if err := validateRequiredLabelV2("memory category", memory.Category, 128); err != nil {
		return err
	}
	if err := validateRequiredLabelV2("memory key", memory.Key, 128); err != nil {
		return err
	}
	if err := validateRequiredTextV2("memory value", memory.Value, 4<<10); err != nil {
		return err
	}
	if err := validateOptionalLabelV2("memory person", memory.Person, 256); err != nil {
		return err
	}
	if err := validateOptionalLabelV2("memory relationship", memory.Relationship, 256); err != nil {
		return err
	}
	if err := validateOptionalTextV2("memory backstory", memory.Backstory, 2<<10); err != nil {
		return err
	}
	if memory.ExpiresAt != nil && memory.ExpiresAt.IsZero() {
		return errors.New("memory expires_at must not be zero")
	}
	return nil
}

func validateIdentifierV2(name, value string) error {
	if strings.TrimSpace(value) == "" {
		return fmt.Errorf("%s is required", name)
	}
	if len(value) > 128 || !utf8.ValidString(value) {
		return fmt.Errorf("%s exceeds 128 bytes or is not UTF-8", name)
	}
	if value != strings.TrimSpace(value) {
		return fmt.Errorf("%s must not have surrounding whitespace", name)
	}
	for _, character := range value {
		if character <= 0x20 || character == 0x7f {
			return fmt.Errorf("%s cannot contain whitespace or control characters", name)
		}
	}
	return nil
}

func validateRequiredTextV2(name, value string, maxBytes int) error {
	if strings.TrimSpace(value) == "" {
		return fmt.Errorf("%s is required", name)
	}
	if len(value) > maxBytes || !utf8.ValidString(value) {
		return fmt.Errorf("%s exceeds %d bytes or is not UTF-8", name, maxBytes)
	}
	return nil
}

func validateOptionalTextV2(name, value string, maxBytes int) error {
	if value == "" {
		return nil
	}
	if len(value) > maxBytes || !utf8.ValidString(value) {
		return fmt.Errorf("%s exceeds %d bytes or is not UTF-8", name, maxBytes)
	}
	return nil
}

func validateRequiredLabelV2(name, value string, maxBytes int) error {
	if err := validateRequiredTextV2(name, value, maxBytes); err != nil {
		return err
	}
	return rejectControlCharactersV2(name, value)
}

func validateOptionalLabelV2(name, value string, maxBytes int) error {
	if err := validateOptionalTextV2(name, value, maxBytes); err != nil {
		return err
	}
	return rejectControlCharactersV2(name, value)
}

func rejectControlCharactersV2(name, value string) error {
	for _, character := range value {
		if character < 0x20 || character == 0x7f || (character >= 0x80 && character <= 0x9f) {
			return fmt.Errorf("%s cannot contain control characters", name)
		}
	}
	return nil
}

func validateMetadataV2(metadata map[string]string) error {
	if len(metadata) > 32 {
		return errors.New("metadata cannot contain more than 32 entries")
	}
	for key, value := range metadata {
		if err := validateIdentifierV2("metadata key", key); err != nil {
			return err
		}
		if len(value) > 1024 || !utf8.ValidString(value) {
			return errors.New("metadata value exceeds 1024 bytes or is not UTF-8")
		}
	}
	return nil
}

func normalizedMemoryIdentityV2(memory MemorySpecV2) memoryIdentityV2 {
	return memoryIdentityV2{
		kind:         domain.MemoryKind(strings.ToLower(strings.TrimSpace(string(memory.Kind)))),
		category:     strings.ToLower(strings.TrimSpace(memory.Category)),
		key:          strings.ToLower(strings.TrimSpace(memory.Key)),
		person:       strings.ToLower(strings.TrimSpace(memory.Person)),
		relationship: strings.ToLower(strings.TrimSpace(memory.Relationship)),
	}
}

func memoryIdentityKeyV2(scope string, identity memoryIdentityV2) string {
	parts := []string{scope, string(identity.kind), identity.category, identity.key, identity.person, identity.relationship}
	var builder strings.Builder
	for _, part := range parts {
		_, _ = fmt.Fprintf(&builder, "%d:%s\x00", len(part), part)
	}
	return builder.String()
}

func datasetFingerprintV2(dataset DatasetV2) (string, error) {
	encoded, err := json.Marshal(dataset)
	if err != nil {
		return "", fmt.Errorf("fingerprint v2 dataset: %w", err)
	}
	encoded = append(encoded, 0)
	encoded = append(encoded, dataset.SHA256...)
	digest := sha256.Sum256(encoded)
	return hex.EncodeToString(digest[:]), nil
}
