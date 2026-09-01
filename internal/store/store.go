package store

import (
	"context"
	"time"

	"github.com/ksana-ai/agent-memory-system/internal/domain"
)

type CandidateReviewCommand struct {
	TenantID    string
	UserID      string
	CandidateID string
	MemoryID    string
	Review      domain.CandidateReview
}

// CandidateBatchCommand atomically creates pending candidates for one user
// scope, provided the scope has not changed since the caller loaded its source
// evidence. ExpectedRevision fences a concurrent ForgetUser followed by
// evidence recreation with the same IDs.
type CandidateBatchCommand struct {
	TenantID         string
	UserID           string
	ExpectedRevision uint64
	Candidates       []domain.MemoryCandidate
}

// Store owns atomic lifecycle transitions. Every method takes the tenant and
// user scope explicitly so an adapter cannot accidentally perform an unscoped
// lookup.
type Store interface {
	AppendEvidence(context.Context, domain.EvidenceEvent) error
	EvidenceByID(context.Context, string, string, string) (domain.EvidenceEvent, error)
	EvidenceByIDs(context.Context, string, string, []string) ([]domain.EvidenceEvent, error)

	CreateCandidate(context.Context, domain.MemoryCandidate) error
	CreateCandidateBatch(context.Context, CandidateBatchCommand) error
	CandidateByID(context.Context, string, string, string) (domain.MemoryCandidate, error)
	ReviewCandidate(context.Context, CandidateReviewCommand) (domain.MemoryCandidate, *domain.MemoryCard, error)

	ListServiceableMemories(context.Context, string, string, time.Time) ([]domain.MemoryCard, error)
	ContextRevision(context.Context, string, string) (uint64, error)
	ForgetUser(context.Context, string, string, time.Time) (domain.DeletionReceipt, error)
}
