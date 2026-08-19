package store

import (
	"context"
	"time"

	"github.com/kai443/go-agent-memory-system/internal/domain"
)

type CandidateReviewCommand struct {
	TenantID    string
	UserID      string
	CandidateID string
	MemoryID    string
	Review      domain.CandidateReview
}

// Store owns atomic lifecycle transitions. Every method takes the tenant and
// user scope explicitly so an adapter cannot accidentally perform an unscoped
// lookup.
type Store interface {
	AppendEvidence(context.Context, domain.EvidenceEvent) error
	EvidenceByID(context.Context, string, string, string) (domain.EvidenceEvent, error)
	EvidenceByIDs(context.Context, string, string, []string) ([]domain.EvidenceEvent, error)

	CreateCandidate(context.Context, domain.MemoryCandidate) error
	CandidateByID(context.Context, string, string, string) (domain.MemoryCandidate, error)
	ReviewCandidate(context.Context, CandidateReviewCommand) (domain.MemoryCandidate, *domain.MemoryCard, error)

	ListServiceableMemories(context.Context, string, string, time.Time) ([]domain.MemoryCard, error)
	ContextRevision(context.Context, string, string) (uint64, error)
	ForgetUser(context.Context, string, string, time.Time) (domain.DeletionReceipt, error)
}
