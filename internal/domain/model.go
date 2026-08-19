package domain

import (
	"strings"
	"time"
)

type Actor string

const (
	ActorUser  Actor = "user"
	ActorAgent Actor = "agent"
	ActorTool  Actor = "tool"
)

type MemoryKind string

const (
	MemoryKindEpisodic   MemoryKind = "episodic"
	MemoryKindSemantic   MemoryKind = "semantic"
	MemoryKindProcedural MemoryKind = "procedural"
)

type CandidateStatus string

const (
	CandidatePending  CandidateStatus = "pending"
	CandidateApproved CandidateStatus = "approved"
	CandidateRejected CandidateStatus = "rejected"
)

type MemoryStatus string

const (
	MemoryActive     MemoryStatus = "active"
	MemorySuperseded MemoryStatus = "superseded"
)

type ReviewDecision string

const (
	DecisionApprove ReviewDecision = "approve"
	DecisionReject  ReviewDecision = "reject"
)

// EvidenceEvent is an immutable source record during normal operation. An
// explicit privacy-erasure request is the only operation allowed to remove it.
type EvidenceEvent struct {
	ID         string            `json:"id"`
	TenantID   string            `json:"tenant_id"`
	UserID     string            `json:"user_id"`
	SessionID  string            `json:"session_id"`
	Actor      Actor             `json:"actor"`
	Content    string            `json:"content"`
	Metadata   map[string]string `json:"metadata,omitempty"`
	OccurredAt time.Time         `json:"occurred_at"`
	RecordedAt time.Time         `json:"recorded_at"`
}

// MemoryCandidate is a proposal. It cannot be served as memory until an
// explicit review approves it against its source evidence.
type MemoryCandidate struct {
	ID               string            `json:"id"`
	TenantID         string            `json:"tenant_id"`
	UserID           string            `json:"user_id"`
	Kind             MemoryKind        `json:"kind"`
	Category         string            `json:"category"`
	Key              string            `json:"key"`
	Value            string            `json:"value"`
	Person           string            `json:"person,omitempty"`
	Relationship     string            `json:"relationship,omitempty"`
	Backstory        string            `json:"backstory,omitempty"`
	SourceEventIDs   []string          `json:"source_event_ids"`
	Extractor        string            `json:"extractor"`
	ExtractorVersion string            `json:"extractor_version"`
	Status           CandidateStatus   `json:"status"`
	Review           *CandidateReview  `json:"review,omitempty"`
	CreatedAt        time.Time         `json:"created_at"`
	ExpiresAt        *time.Time        `json:"expires_at,omitempty"`
	Metadata         map[string]string `json:"metadata,omitempty"`
}

type CandidateReview struct {
	Decision   ReviewDecision `json:"decision"`
	ReviewerID string         `json:"reviewer_id"`
	Reason     string         `json:"reason"`
	ReviewedAt time.Time      `json:"reviewed_at"`
}

// MemoryCard is a reviewed projection. Cards sharing the same identity form
// an ordered version chain; at most one version is active. Active cards may
// still be unavailable after ExpiresAt; callers use ServiceableAt.
type MemoryCard struct {
	ID             string       `json:"id"`
	CandidateID    string       `json:"candidate_id"`
	TenantID       string       `json:"tenant_id"`
	UserID         string       `json:"user_id"`
	Kind           MemoryKind   `json:"kind"`
	Category       string       `json:"category"`
	Key            string       `json:"key"`
	Value          string       `json:"value"`
	Person         string       `json:"person,omitempty"`
	Relationship   string       `json:"relationship,omitempty"`
	Backstory      string       `json:"backstory,omitempty"`
	SourceEventIDs []string     `json:"source_event_ids"`
	Version        int          `json:"version"`
	Status         MemoryStatus `json:"status"`
	CreatedAt      time.Time    `json:"created_at"`
	ExpiresAt      *time.Time   `json:"expires_at,omitempty"`
	SupersededAt   *time.Time   `json:"superseded_at,omitempty"`
}

// ServiceableAt applies the request-time availability boundary without
// changing lifecycle status. expires_at is exclusive: equality is expired.
func (m MemoryCard) ServiceableAt(asOf time.Time) bool {
	return m.Status == MemoryActive && (m.ExpiresAt == nil || m.ExpiresAt.After(asOf))
}

type MemoryIdentity struct {
	Kind         MemoryKind
	Category     string
	Key          string
	Person       string
	Relationship string
}

func (m MemoryCard) Identity() MemoryIdentity {
	return MemoryIdentity{
		Kind:         MemoryKind(strings.ToLower(strings.TrimSpace(string(m.Kind)))),
		Category:     strings.ToLower(strings.TrimSpace(m.Category)),
		Key:          strings.ToLower(strings.TrimSpace(m.Key)),
		Person:       strings.ToLower(strings.TrimSpace(m.Person)),
		Relationship: strings.ToLower(strings.TrimSpace(m.Relationship)),
	}
}

type SearchHit struct {
	Memory MemoryCard `json:"memory"`
	Score  float64    `json:"score"`
}

type ContextItem struct {
	Memory  MemoryCard      `json:"memory"`
	Score   float64         `json:"score"`
	Sources []EvidenceEvent `json:"sources"`
}

type ContextPack struct {
	TenantID    string        `json:"tenant_id"`
	UserID      string        `json:"user_id"`
	Query       string        `json:"query"`
	Items       []ContextItem `json:"items"`
	GeneratedAt time.Time     `json:"generated_at"`
}

type DeletionReceipt struct {
	TenantID          string    `json:"tenant_id"`
	UserID            string    `json:"user_id"`
	EvidenceDeleted   int       `json:"evidence_deleted"`
	CandidatesDeleted int       `json:"candidates_deleted"`
	MemoriesDeleted   int       `json:"memories_deleted"`
	DeletedAt         time.Time `json:"deleted_at"`
}
