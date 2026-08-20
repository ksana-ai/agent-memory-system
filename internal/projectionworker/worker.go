// Package projectionworker orchestrates durable embedding projection jobs.
//
// It deliberately owns no database transaction. Repository claims and state
// transitions are short, fenced operations; Embed is the only external I/O
// and always runs after ClaimProjectionJobs has returned.
package projectionworker

import (
	"context"
	"encoding/hex"
	"errors"
	"math"
	"net/http"
	"strings"
	"time"

	"github.com/kai443/go-agent-memory-system/internal/domain"
	"github.com/kai443/go-agent-memory-system/internal/embedding"
	"github.com/kai443/go-agent-memory-system/internal/store/postgres"
)

const (
	// V1 claims exactly one item at a time. Without a separate per-request
	// timeout budget, claiming a serial batch would let earlier HTTP calls
	// consume later items' leases before they start.
	DefaultBatchSize     = 1
	MaximumBatchSize     = 1
	DefaultLeaseDuration = 30 * time.Second
	MaximumLeaseDuration = 24 * time.Hour
	DefaultIdleInterval  = 500 * time.Millisecond
	DefaultMaxAttempts   = 5
	MaximumAttempts      = 100
	// EmbeddingBatchSizeV1 is the exact provider request shape: one behavioral
	// probe followed by one versioned memory document.
	EmbeddingBatchSizeV1 = 2

	// ConcurrencyV1 is intentionally one. A bounded claim batch is processed
	// serially, which makes external-call and lease behavior explicit.
	ConcurrencyV1 = 1
)

var (
	ErrInvalidConfig       = errors.New("invalid projection worker configuration")
	ErrRepositoryOperation = errors.New("projection worker repository operation failed")
	ErrRepositoryInvariant = errors.New("projection worker repository invariant failed")
	ErrBackoff             = errors.New("projection worker backoff failed")
)

// Repository is the smallest durable API needed by the worker. Every mutation
// carries the claim's owner and lease version so a late worker cannot restore
// a superseded or erased projection.
type Repository interface {
	ClaimProjectionJobs(context.Context, postgres.ClaimProjectionJobsCommand) ([]postgres.ProjectionWorkItem, error)
	FinalizeProjectionJob(context.Context, postgres.FinalizeProjectionJobCommand) (postgres.FinalizeProjectionJobResult, error)
	RetryProjectionJob(context.Context, postgres.RetryProjectionJobCommand) (postgres.ProjectionJob, error)
	DeadLetterProjectionJob(context.Context, postgres.DeadLetterProjectionJobCommand) (postgres.ProjectionJob, error)
}

var _ Repository = (*postgres.Store)(nil)

// Embedder is implemented directly by embedding.Client. Descriptor is used to
// fail closed before any memory content is sent to a mismatched model.
type Embedder interface {
	Descriptor() embedding.Descriptor
	Embed(context.Context, []string) ([][]float32, error)
}

// MonotonicClock supplies process-local time values used only to bound how
// long a claimed document may wait before or remain in provider I/O. The
// default time.Now values carry Go's monotonic component. This clock never
// decides durable lease, expiry, or lifecycle state; PostgreSQL does.
type MonotonicClock func() time.Time

// Backoff returns the delay for an already-counted attempt. It is invoked once
// only after a retryable embedding failure; it never performs a retry itself.
type Backoff func(attemptCount int) time.Duration

type Config struct {
	EmbeddingSpace   string
	ModelFingerprint string
	QueryVersion     string
	LeaseOwner       string
	BatchSize        int
	LeaseDuration    time.Duration
	IdleInterval     time.Duration
	MaxAttempts      int
	Concurrency      int
	MonotonicNow     MonotonicClock
	Backoff          Backoff
}

// BatchResult contains only aggregate operational facts. It intentionally
// excludes job identifiers, scope identifiers, documents, and provider data.
type BatchResult struct {
	Claimed      int
	Succeeded    int
	Retried      int
	DeadLettered int
	Cancelled    int
	LeaseLost    int
}

type Worker struct {
	repository       Repository
	embedder         Embedder
	descriptor       embedding.Descriptor
	embeddingSpace   string
	modelFingerprint string
	queryVersion     string
	leaseOwner       string
	batchSize        int
	leaseDuration    time.Duration
	idleInterval     time.Duration
	maxAttempts      int
	monotonicNow     MonotonicClock
	backoff          Backoff
}

// New validates the worker/model/space binding without making a network call.
// The caller must obtain ModelFingerprint from a successful behavioral probe
// of the exact embedder instance before constructing the worker.
func New(repository Repository, embedder Embedder, config Config) (*Worker, error) {
	if repository == nil || embedder == nil {
		return nil, ErrInvalidConfig
	}

	descriptor := embedder.Descriptor()
	queryVersion := config.QueryVersion
	if queryVersion == "" {
		queryVersion = embedding.RawQueryVersion
	}
	if !validIdentifier(config.EmbeddingSpace) ||
		!validIdentifier(config.LeaseOwner) ||
		!validIdentifier(queryVersion) ||
		!validIdentifier(descriptor.Provider) ||
		!validIdentifier(descriptor.Model) ||
		descriptor.Dimension < 1 ||
		descriptor.DocumentVersion != embedding.MemoryCardDocumentVersion ||
		!validSHA256(config.ModelFingerprint) {
		return nil, ErrInvalidConfig
	}

	spaceID, err := embedding.SpaceID(
		descriptor.Provider,
		descriptor.Model,
		descriptor.Dimension,
		descriptor.DocumentVersion,
		queryVersion,
		config.ModelFingerprint,
	)
	if err != nil || spaceID != config.EmbeddingSpace {
		return nil, ErrInvalidConfig
	}

	batchSize := config.BatchSize
	if batchSize == 0 {
		batchSize = DefaultBatchSize
	}
	if batchSize < 1 || batchSize > MaximumBatchSize {
		return nil, ErrInvalidConfig
	}
	leaseDuration := config.LeaseDuration
	if leaseDuration == 0 {
		leaseDuration = DefaultLeaseDuration
	}
	if leaseDuration < time.Millisecond || leaseDuration > MaximumLeaseDuration {
		return nil, ErrInvalidConfig
	}
	idleInterval := config.IdleInterval
	if idleInterval == 0 {
		idleInterval = DefaultIdleInterval
	}
	if idleInterval < 0 {
		return nil, ErrInvalidConfig
	}
	maxAttempts := config.MaxAttempts
	if maxAttempts == 0 {
		maxAttempts = DefaultMaxAttempts
	}
	if maxAttempts < 1 || maxAttempts > MaximumAttempts {
		return nil, ErrInvalidConfig
	}
	concurrency := config.Concurrency
	if concurrency == 0 {
		concurrency = ConcurrencyV1
	}
	if concurrency != ConcurrencyV1 {
		return nil, ErrInvalidConfig
	}
	monotonicNow := config.MonotonicNow
	if monotonicNow == nil {
		monotonicNow = time.Now
	}
	backoff := config.Backoff
	if backoff == nil {
		backoff = defaultBackoff
	}

	return &Worker{
		repository:       repository,
		embedder:         embedder,
		descriptor:       descriptor,
		embeddingSpace:   config.EmbeddingSpace,
		modelFingerprint: config.ModelFingerprint,
		queryVersion:     queryVersion,
		leaseOwner:       config.LeaseOwner,
		batchSize:        batchSize,
		leaseDuration:    leaseDuration,
		idleInterval:     idleInterval,
		maxAttempts:      maxAttempts,
		monotonicNow:     monotonicNow,
		backoff:          backoff,
	}, nil
}

// Run processes bounded batches until ctx is cancelled. Cancellation is a
// graceful shutdown: an in-flight HTTP request is cancelled and its durable
// lease is left for expiry/recovery rather than being mutated with a stale
// token.
func (worker *Worker) Run(ctx context.Context) error {
	for {
		result, err := worker.RunOnce(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return err
		}
		if result.Claimed != 0 {
			continue
		}

		timer := time.NewTimer(worker.idleInterval)
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				<-timer.C
			}
			return nil
		case <-timer.C:
		}
	}
}

// RunOnce claims at most the configured batch and processes it serially. One
// claimed item causes at most one Embed call. Durable retry is represented by
// an explicit repository transition and a later, separately claimed attempt.
func (worker *Worker) RunOnce(ctx context.Context) (BatchResult, error) {
	if err := ctx.Err(); err != nil {
		return BatchResult{}, err
	}
	claimStarted := worker.monotonicNow()
	if claimStarted.IsZero() {
		return BatchResult{}, ErrInvalidConfig
	}
	items, err := worker.repository.ClaimProjectionJobs(ctx, postgres.ClaimProjectionJobsCommand{
		EmbeddingSpace: worker.embeddingSpace,
		LeaseOwner:     worker.leaseOwner,
		LeaseDuration:  worker.leaseDuration,
		MaxAttempts:    worker.maxAttempts,
		Limit:          worker.batchSize,
	})
	if err != nil {
		if ctx.Err() != nil {
			return BatchResult{}, ctx.Err()
		}
		return BatchResult{}, ErrRepositoryOperation
	}
	if len(items) > worker.batchSize {
		return BatchResult{}, ErrRepositoryInvariant
	}

	result := BatchResult{Claimed: len(items)}
	for _, item := range items {
		if err := ctx.Err(); err != nil {
			return result, err
		}
		outcome, err := worker.process(ctx, item, claimStarted)
		if err != nil {
			return result, err
		}
		switch outcome {
		case outcomeSucceeded:
			result.Succeeded++
		case outcomeRetried:
			result.Retried++
		case outcomeDeadLettered:
			result.DeadLettered++
		case outcomeCancelled:
			result.Cancelled++
		case outcomeLeaseLost:
			result.LeaseLost++
		default:
			return result, ErrRepositoryInvariant
		}
	}
	return result, nil
}

type itemOutcome uint8

const (
	outcomeSucceeded itemOutcome = iota + 1
	outcomeRetried
	outcomeDeadLettered
	outcomeCancelled
	outcomeLeaseLost
)

func (worker *Worker) process(ctx context.Context, item postgres.ProjectionWorkItem, claimStarted time.Time) (itemOutcome, error) {
	if !validLease(item.Job, worker.leaseOwner, worker.embeddingSpace) {
		return outcomeLeaseLost, nil
	}

	if item.Target.State != postgres.ProjectionTargetShadow && item.Target.State != postgres.ProjectionTargetServing {
		return 0, ErrRepositoryInvariant
	}
	if item.Memory.Status != domain.MemoryActive {
		return 0, ErrRepositoryInvariant
	}

	document := embedding.MemoryCardDocumentV1(item.Memory)
	documentSHA256 := embedding.DocumentSHA256(document)
	if !worker.validWorkItem(item, document, documentSHA256) {
		return 0, ErrRepositoryInvariant
	}

	remaining, err := worker.remainingLeaseBudget(claimStarted)
	if err != nil {
		return 0, err
	}
	if remaining <= 0 {
		return outcomeLeaseLost, nil
	}
	embedContext, cancelEmbed := context.WithTimeout(ctx, remaining)
	// Probe and document share one provider request. This continuously binds
	// every projected vector to the registered public-probe fingerprint and
	// detects probe-visible drift behind a stable model alias.
	vectors, embedErr := worker.embedder.Embed(embedContext, []string{embedding.ProbeTextV1, document})
	embedContextErr := embedContext.Err()
	cancelEmbed()
	if ctx.Err() != nil {
		return 0, ctx.Err()
	}
	if embedErr != nil {
		remaining, budgetErr := worker.remainingLeaseBudget(claimStarted)
		if budgetErr != nil {
			return 0, budgetErr
		}
		if remaining <= 0 || errors.Is(embedContextErr, context.DeadlineExceeded) {
			return outcomeLeaseLost, nil
		}
		code := classifyEmbeddingError(embedErr)
		return worker.recordFailure(ctx, item, code, retryable(code))
	}
	remaining, err = worker.remainingLeaseBudget(claimStarted)
	if err != nil {
		return 0, err
	}
	if remaining <= 0 || errors.Is(embedContextErr, context.DeadlineExceeded) {
		return outcomeLeaseLost, nil
	}

	vector, code := worker.validateVectors(vectors)
	if code != "" {
		return worker.recordFailure(ctx, item, code, false)
	}
	finalized, err := worker.repository.FinalizeProjectionJob(ctx, postgres.FinalizeProjectionJobCommand{
		JobID:          item.Job.ID,
		TenantID:       item.Job.TenantID,
		UserID:         item.Job.UserID,
		EmbeddingSpace: item.Job.EmbeddingSpace,
		LeaseOwner:     worker.leaseOwner,
		LeaseVersion:   item.Job.LeaseVersion,
		DocumentSHA256: documentSHA256,
		Vector:         vector,
	})
	if errors.Is(err, postgres.ErrProjectionLeaseLost) {
		return outcomeLeaseLost, nil
	}
	if err != nil {
		if ctx.Err() != nil {
			return 0, ctx.Err()
		}
		return 0, ErrRepositoryOperation
	}
	if finalized.Cancelled && finalized.Requeued {
		return 0, ErrRepositoryInvariant
	}
	if finalized.Cancelled {
		return outcomeCancelled, nil
	}
	if finalized.Requeued {
		return outcomeRetried, nil
	}
	return outcomeSucceeded, nil
}

func (worker *Worker) remainingLeaseBudget(claimStarted time.Time) (time.Duration, error) {
	now := worker.monotonicNow()
	if now.IsZero() {
		return 0, ErrInvalidConfig
	}
	elapsed := now.Sub(claimStarted)
	if elapsed < 0 {
		return 0, ErrInvalidConfig
	}
	return worker.leaseDuration - elapsed, nil
}

func (worker *Worker) validWorkItem(item postgres.ProjectionWorkItem, document, documentSHA256 string) bool {
	space := item.Target.Space
	return item.Job.TenantID == item.Memory.TenantID &&
		item.Job.UserID == item.Memory.UserID &&
		item.Job.MemoryID == item.Memory.ID &&
		item.Job.ExpectedMemoryVersion == item.Memory.Version &&
		item.Job.EmbeddingSpace == item.Target.Space.ID &&
		item.Job.EmbeddingSpace == worker.embeddingSpace &&
		space.Provider == worker.descriptor.Provider &&
		space.Model == worker.descriptor.Model &&
		space.Dimension == worker.descriptor.Dimension &&
		space.DocumentVersion == worker.descriptor.DocumentVersion &&
		space.QueryVersion == worker.queryVersion &&
		space.ModelFingerprint == worker.modelFingerprint &&
		item.Document == document &&
		item.DocumentSHA256 == documentSHA256
}

func (worker *Worker) validateVectors(vectors [][]float32) ([]float32, postgres.ProjectionErrorCode) {
	if len(vectors) != 2 {
		return nil, postgres.ProjectionErrorInvalidResponse
	}
	probe, code := worker.validateVector(vectors[0])
	if code != "" {
		return nil, code
	}
	if embedding.VectorSHA256(probe) != worker.modelFingerprint {
		return nil, postgres.ProjectionErrorModelMismatch
	}
	return worker.validateVector(vectors[1])
}

func (worker *Worker) validateVector(input []float32) ([]float32, postgres.ProjectionErrorCode) {
	if len(input) != worker.descriptor.Dimension {
		return nil, postgres.ProjectionErrorDimensionMismatch
	}
	vector := make([]float32, len(input))
	nonzero := false
	for index, value := range input {
		if math.IsNaN(float64(value)) || math.IsInf(float64(value), 0) {
			return nil, postgres.ProjectionErrorNonFiniteVector
		}
		vector[index] = value
		nonzero = nonzero || value != 0
	}
	if !nonzero {
		return nil, postgres.ProjectionErrorInvalidResponse
	}
	return vector, ""
}

func (worker *Worker) recordFailure(
	ctx context.Context,
	item postgres.ProjectionWorkItem,
	code postgres.ProjectionErrorCode,
	canRetry bool,
) (itemOutcome, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	if canRetry && item.Job.AttemptCount < worker.maxAttempts {
		delay := worker.backoff(item.Job.AttemptCount)
		if delay < 0 || delay > MaximumLeaseDuration {
			return 0, ErrBackoff
		}
		_, err := worker.repository.RetryProjectionJob(ctx, postgres.RetryProjectionJobCommand{
			JobID:        item.Job.ID,
			TenantID:     item.Job.TenantID,
			UserID:       item.Job.UserID,
			LeaseOwner:   worker.leaseOwner,
			LeaseVersion: item.Job.LeaseVersion,
			ErrorCode:    code,
			RetryAfter:   delay,
		})
		if errors.Is(err, postgres.ErrProjectionLeaseLost) {
			return outcomeLeaseLost, nil
		}
		if err != nil {
			if ctx.Err() != nil {
				return 0, ctx.Err()
			}
			return 0, ErrRepositoryOperation
		}
		return outcomeRetried, nil
	}
	if canRetry {
		code = postgres.ProjectionErrorAttemptsExhausted
	}
	_, err := worker.repository.DeadLetterProjectionJob(ctx, postgres.DeadLetterProjectionJobCommand{
		JobID:        item.Job.ID,
		TenantID:     item.Job.TenantID,
		UserID:       item.Job.UserID,
		LeaseOwner:   worker.leaseOwner,
		LeaseVersion: item.Job.LeaseVersion,
		ErrorCode:    code,
	})
	if errors.Is(err, postgres.ErrProjectionLeaseLost) {
		return outcomeLeaseLost, nil
	}
	if err != nil {
		if ctx.Err() != nil {
			return 0, ctx.Err()
		}
		return 0, ErrRepositoryOperation
	}
	return outcomeDeadLettered, nil
}

func classifyEmbeddingError(err error) postgres.ProjectionErrorCode {
	if errors.Is(err, context.DeadlineExceeded) {
		return postgres.ProjectionErrorProviderTimeout
	}
	var statusError *embedding.HTTPStatusError
	if errors.As(err, &statusError) {
		switch {
		case statusError.StatusCode == http.StatusRequestTimeout:
			return postgres.ProjectionErrorProviderTimeout
		case statusError.StatusCode == http.StatusTooManyRequests:
			return postgres.ProjectionErrorProviderRateLimit
		case statusError.StatusCode >= http.StatusInternalServerError && statusError.StatusCode <= 599:
			return postgres.ProjectionErrorProviderUnavailable
		case statusError.StatusCode >= http.StatusBadRequest && statusError.StatusCode < http.StatusInternalServerError:
			return postgres.ProjectionErrorProviderRejected
		default:
			return postgres.ProjectionErrorInvalidResponse
		}
	}
	if errors.Is(err, embedding.ErrModelMismatch) {
		return postgres.ProjectionErrorModelMismatch
	}
	if errors.Is(err, embedding.ErrDimensionMismatch) {
		return postgres.ProjectionErrorDimensionMismatch
	}
	if errors.Is(err, embedding.ErrNonFiniteVector) {
		return postgres.ProjectionErrorNonFiniteVector
	}
	if errors.Is(err, embedding.ErrZeroVector) {
		return postgres.ProjectionErrorInvalidResponse
	}
	if errors.Is(err, embedding.ErrResponseTooLarge) || errors.Is(err, embedding.ErrInvalidResponse) {
		return postgres.ProjectionErrorInvalidResponse
	}
	if errors.Is(err, embedding.ErrInvalidInput) || errors.Is(err, embedding.ErrInvalidConfig) {
		return postgres.ProjectionErrorSpaceConflict
	}
	return postgres.ProjectionErrorTransport
}

func retryable(code postgres.ProjectionErrorCode) bool {
	switch code {
	case postgres.ProjectionErrorTransport,
		postgres.ProjectionErrorProviderTimeout,
		postgres.ProjectionErrorProviderRateLimit,
		postgres.ProjectionErrorProviderUnavailable:
		return true
	default:
		return false
	}
}

func validLease(job postgres.ProjectionJob, leaseOwner, embeddingSpace string) bool {
	return job.ID > 0 &&
		job.State == postgres.ProjectionJobLeased &&
		job.LeaseOwner == leaseOwner &&
		job.LeaseVersion > 0 &&
		job.AttemptCount > 0 &&
		job.EmbeddingSpace == embeddingSpace &&
		job.LeaseUntil != nil
}

func validIdentifier(value string) bool {
	return value != "" && value == strings.TrimSpace(value) && len(value) <= 512 && strings.IndexFunc(value, func(r rune) bool {
		return r < 0x20 || r == 0x7f
	}) == -1
}

func validSHA256(value string) bool {
	if len(value) != 64 || strings.ToLower(value) != value {
		return false
	}
	decoded, err := hex.DecodeString(value)
	return err == nil && len(decoded) == 32
}

func defaultBackoff(attemptCount int) time.Duration {
	if attemptCount < 1 {
		attemptCount = 1
	}
	shift := attemptCount - 1
	if shift > 8 {
		shift = 8
	}
	return time.Second * time.Duration(1<<shift)
}
