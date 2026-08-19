package eval

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strings"
	"time"

	"github.com/kai443/go-agent-memory-system/internal/app"
	"github.com/kai443/go-agent-memory-system/internal/domain"
)

const runnerVersionV2 = "evaluation-runner-v2"

// Timer is used only around Retriever.Search. The logical fixture clock is
// controlled separately by timeline timestamps and is never used as latency.
type Timer interface {
	Now() time.Time
}

type TimerFunc func() time.Time

func (timer TimerFunc) Now() time.Time { return timer() }

type ConfigV2 struct {
	RecallK           int
	NDCGK             int
	WarmupRuns        int
	MeasuredRuns      int
	QueryTimeout      time.Duration
	Arms              []ArmFactory
	Timer             Timer
	GeneratedAt       func() time.Time
	Source            SourceProofV2
	RequirePolicyPass bool
}

func RunV2(ctx context.Context, dataset DatasetV2, config ConfigV2) (ManifestV2, error) {
	if err := dataset.VerifyIntegrity(); err != nil {
		return ManifestV2{}, err
	}
	if err := validateConfigV2(config); err != nil {
		return ManifestV2{}, err
	}
	if config.Timer == nil {
		config.Timer = TimerFunc(time.Now)
	}
	if config.GeneratedAt == nil {
		config.GeneratedAt = func() time.Time { return time.Now().UTC() }
	}

	depth := max(config.RecallK, config.NDCGK)
	manifest := ManifestV2{
		SchemaVersion: ManifestSchemaVersionV2,
		GeneratedAt:   config.GeneratedAt().UTC(),
		Source:        config.Source,
		Acceptance:    AcceptanceV2{PolicyPassRequired: config.RequirePolicyPass, PolicyPassVerified: true},
		Dataset: DatasetMetadataV2{
			ID:      dataset.ID,
			Version: dataset.Version,
			SHA256:  dataset.SHA256,
			Cases:   len(dataset.Cases),
			Queries: countQueriesV2(dataset),
		},
		System: SystemMetadataV2{
			RunnerVersion:           runnerVersionV2,
			RecallK:                 config.RecallK,
			NDCGK:                   config.NDCGK,
			RetrievalDepth:          depth,
			WarmupRuns:              config.WarmupRuns,
			MeasuredRuns:            config.MeasuredRuns,
			QueryTimeoutNanoseconds: config.QueryTimeout.Nanoseconds(),
			ConfigHash:              configHashV2(config),
		},
	}

	for _, factory := range config.Arms {
		armResult := ArmResultV2{Descriptor: factory.Descriptor()}
		for _, testCase := range dataset.Cases {
			caseQueries, err := runCaseV2(ctx, testCase, factory, config, depth)
			if err != nil {
				return ManifestV2{}, fmt.Errorf("arm %q case %q: %w", factory.Descriptor().ID, testCase.ID, err)
			}
			armResult.Queries = append(armResult.Queries, caseQueries...)
		}
		aggregate, err := summarizeArmV2(armResult.Queries)
		if err != nil {
			return ManifestV2{}, fmt.Errorf("summarize arm %q: %w", factory.Descriptor().ID, err)
		}
		armResult.Aggregate = aggregate
		manifest.Arms = append(manifest.Arms, armResult)
		manifest.Acceptance.PolicyPassVerified = manifest.Acceptance.PolicyPassVerified && armResult.Aggregate.PolicyPassed
	}
	return manifest, nil
}

func validateConfigV2(config ConfigV2) error {
	if config.RecallK < 1 || config.RecallK > 20 {
		return errors.New("v2 recall_k must be between 1 and 20")
	}
	if config.NDCGK < 1 || config.NDCGK > 20 {
		return errors.New("v2 ndcg_k must be between 1 and 20")
	}
	if config.WarmupRuns < 0 {
		return errors.New("v2 warmup_runs must be non-negative")
	}
	if config.MeasuredRuns < 1 || config.MeasuredRuns > 1000 {
		return errors.New("v2 measured_runs must be between 1 and 1000")
	}
	if config.QueryTimeout <= 0 {
		return errors.New("v2 query_timeout must be positive")
	}
	return validateArmFactories(config.Arms)
}

type caseRuntimeV2 struct {
	testCase          CaseV2
	service           *app.Service
	timed             *timedRetrieverV2
	logicalNow        time.Time
	scopes            map[string]ScopeV2
	evidenceIDs       map[string]string
	evidenceAliases   map[string]string
	evidenceExpected  map[string]domain.EvidenceEvent
	candidateIDs      map[string]string
	candidateSources  map[string][]string
	candidateExpected map[string]domain.MemoryCandidate
	memoryIDs         map[string]string
	memoryAliases     map[string]string
	memoryState       map[string]*logicalMemoryV2
	nextIDAlias       string
}

type logicalMemoryV2 struct {
	ScopeID       string
	MemoryID      string
	Identity      domain.MemoryIdentity
	Active        bool
	Deleted       bool
	SourceAliases []string
	Expected      domain.MemoryCard
}

func runCaseV2(ctx context.Context, testCase CaseV2, factory ArmFactory, config ConfigV2, depth int) (results []QueryResultV2, err error) {
	runtime, err := factory.NewRuntime(ctx)
	if err != nil {
		return nil, err
	}
	if runtime.Cleanup != nil {
		defer func() { err = errors.Join(err, runtime.Cleanup(context.WithoutCancel(ctx))) }()
	}
	timed := &timedRetrieverV2{inner: runtime.Retriever, timer: config.Timer}
	state := &caseRuntimeV2{
		testCase:          testCase,
		timed:             timed,
		scopes:            make(map[string]ScopeV2, len(testCase.Scopes)),
		evidenceIDs:       make(map[string]string),
		evidenceAliases:   make(map[string]string),
		evidenceExpected:  make(map[string]domain.EvidenceEvent),
		candidateIDs:      make(map[string]string),
		candidateSources:  make(map[string][]string),
		candidateExpected: make(map[string]domain.MemoryCandidate),
		memoryIDs:         make(map[string]string),
		memoryAliases:     make(map[string]string),
		memoryState:       make(map[string]*logicalMemoryV2),
	}
	for _, scope := range testCase.Scopes {
		state.scopes[scope.ID] = scope
	}
	state.service, err = app.New(runtime.Store, timed,
		app.WithClock(func() time.Time { return state.logicalNow }),
		app.WithIDGenerator(func(prefix string) (string, error) {
			if state.nextIDAlias == "" {
				return "", fmt.Errorf("unexpected %s id request without a logical alias", prefix)
			}
			return stableArtifactIDV2(prefix, testCase.ID, state.nextIDAlias), nil
		}),
	)
	if err != nil {
		return nil, err
	}

	for _, operation := range testCase.Timeline {
		state.logicalNow, err = operationTimeV2(operation)
		if err != nil {
			return nil, err
		}
		switch value := operation.(type) {
		case *EvidenceAppendV2:
			err = state.appendEvidence(ctx, value.As, value.Scope, value.SessionID, value.Actor, value.Content, value.Metadata, value.At)
		case *CandidateProposeV2:
			err = state.proposeCandidate(ctx, value.As, value.Scope, value.SourceEventIDs, value.Memory, value.Extractor, value.ExtractorVersion, value.Metadata)
		case *CandidateReviewV2:
			err = state.reviewCandidate(ctx, value.Candidate, value.Scope, value.Decision, value.MemoryAs, value.ReviewerID, value.Reason)
		case *MemoryRememberV2:
			err = state.remember(ctx, value)
		case *ForgetUserV2:
			err = state.forget(ctx, value.Scope)
		case *QueryV2:
			var result QueryResultV2
			result, err = state.query(ctx, value, factory.Descriptor(), config, depth)
			if err == nil {
				results = append(results, result)
			}
		default:
			err = fmt.Errorf("unsupported timeline operation %T", operation)
		}
		if err != nil {
			return nil, fmt.Errorf("%s at %s: %w", operation.Operation(), state.logicalNow.Format(time.RFC3339Nano), err)
		}
	}
	return results, nil
}

func (state *caseRuntimeV2) appendEvidence(ctx context.Context, alias, scopeID, sessionID string, actor domain.Actor, content string, metadata map[string]string, occurredAt time.Time) error {
	scope := state.scopes[scopeID]
	eventID := stableArtifactIDV2("evt", state.testCase.ID, alias)
	event, err := state.service.IngestEvidence(ctx, app.IngestEvidenceInput{
		EventID: eventID, TenantID: scope.TenantID, UserID: scope.UserID,
		SessionID: sessionID, Actor: actor, Content: content, Metadata: metadata, OccurredAt: occurredAt,
	})
	if err == nil {
		expected := domain.EvidenceEvent{
			ID: eventID, TenantID: scope.TenantID, UserID: scope.UserID, SessionID: sessionID,
			Actor: actor, Content: content, Metadata: maps.Clone(metadata), OccurredAt: occurredAt.UTC(), RecordedAt: state.logicalNow.UTC(),
		}
		if !evidenceEventsEqualV2(event, expected) {
			return fmt.Errorf("evidence %q differs from authored fixture", alias)
		}
		state.evidenceIDs[alias] = eventID
		state.evidenceAliases[eventID] = alias
		state.evidenceExpected[alias] = expected
	}
	return err
}

func (state *caseRuntimeV2) proposeCandidate(ctx context.Context, alias, scopeID string, sourceAliases []string, memory MemorySpecV2, extractor, extractorVersion string, metadata map[string]string) error {
	scope := state.scopes[scopeID]
	sourceIDs := make([]string, 0, len(sourceAliases))
	for _, sourceAlias := range sourceAliases {
		sourceIDs = append(sourceIDs, state.evidenceIDs[sourceAlias])
	}
	state.nextIDAlias = alias
	defer func() { state.nextIDAlias = "" }()
	candidate, err := state.service.ProposeCandidate(ctx, app.ProposeCandidateInput{
		TenantID: scope.TenantID, UserID: scope.UserID,
		Kind: memory.Kind, Category: memory.Category, Key: memory.Key, Value: memory.Value,
		Person: memory.Person, Relationship: memory.Relationship, Backstory: memory.Backstory,
		SourceEventIDs: sourceIDs, Extractor: extractor, ExtractorVersion: extractorVersion, Metadata: metadata,
	})
	if err == nil {
		expectedID := stableArtifactIDV2("cand", state.testCase.ID, alias)
		expected := domain.MemoryCandidate{
			ID: expectedID, TenantID: scope.TenantID, UserID: scope.UserID,
			Kind: memory.Kind, Category: strings.TrimSpace(memory.Category), Key: strings.TrimSpace(memory.Key), Value: strings.TrimSpace(memory.Value),
			Person: strings.TrimSpace(memory.Person), Relationship: strings.TrimSpace(memory.Relationship), Backstory: strings.TrimSpace(memory.Backstory),
			SourceEventIDs: append([]string(nil), sourceIDs...), Extractor: strings.TrimSpace(extractor), ExtractorVersion: strings.TrimSpace(extractorVersion),
			Status: domain.CandidatePending, CreatedAt: state.logicalNow.UTC(), Metadata: maps.Clone(metadata),
		}
		if !memoryCandidatesEqualV2(candidate, expected) {
			return fmt.Errorf("candidate %q differs from authored fixture", alias)
		}
		state.candidateIDs[alias] = candidate.ID
		state.candidateSources[alias] = append([]string(nil), sourceAliases...)
		state.candidateExpected[alias] = expected
	}
	return err
}

func (state *caseRuntimeV2) reviewCandidate(ctx context.Context, candidateAlias, scopeID string, decision domain.ReviewDecision, memoryAlias, reviewerID, reason string) error {
	scope := state.scopes[scopeID]
	state.nextIDAlias = memoryAlias
	defer func() { state.nextIDAlias = "" }()
	candidate, card, err := state.service.ReviewCandidate(ctx, app.ReviewCandidateInput{
		TenantID: scope.TenantID, UserID: scope.UserID, CandidateID: state.candidateIDs[candidateAlias],
		Decision: decision, ReviewerID: reviewerID, Reason: reason,
	})
	if err != nil {
		return err
	}
	expectedCandidate := state.candidateExpected[candidateAlias]
	expectedCandidate.Status = domain.CandidateRejected
	if decision == domain.DecisionApprove {
		expectedCandidate.Status = domain.CandidateApproved
	}
	expectedCandidate.Review = &domain.CandidateReview{Decision: decision, ReviewerID: strings.TrimSpace(reviewerID), Reason: strings.TrimSpace(reason), ReviewedAt: state.logicalNow.UTC()}
	if !memoryCandidatesEqualV2(candidate, expectedCandidate) {
		return fmt.Errorf("reviewed candidate %q differs from authored lifecycle", candidateAlias)
	}
	state.candidateExpected[candidateAlias] = expectedCandidate
	if decision == domain.DecisionReject {
		if card != nil {
			return fmt.Errorf("rejected candidate %q produced a memory", candidateAlias)
		}
		return nil
	}
	if card == nil {
		return fmt.Errorf("approved candidate %q produced no memory", candidateAlias)
	}
	expectedCard := state.expectedApprovedMemoryV2(memoryAlias, scopeID, expectedCandidate)
	if !memoryCardsEqualV2(*card, expectedCard) {
		return fmt.Errorf("approved memory %q differs from authored lifecycle", memoryAlias)
	}
	state.registerMemory(memoryAlias, scopeID, state.candidateSources[candidateAlias], expectedCard)
	return nil
}

func (state *caseRuntimeV2) remember(ctx context.Context, operation *MemoryRememberV2) error {
	sourceAliases := make([]string, 0, len(operation.Evidence))
	for _, fixture := range operation.Evidence {
		if err := state.appendEvidence(ctx, fixture.Alias, operation.Scope, fixture.SessionID, fixture.Actor, fixture.Content, fixture.Metadata, fixture.OccurredAt); err != nil {
			return err
		}
		sourceAliases = append(sourceAliases, fixture.Alias)
	}
	candidateAlias := "\x00compact-candidate:" + operation.MemoryRef
	if err := state.proposeCandidate(ctx, candidateAlias, operation.Scope, sourceAliases, operation.Memory, "dataset-fixture", "v2", nil); err != nil {
		return err
	}
	switch operation.ReviewState {
	case RememberPendingV2:
		return nil
	case RememberApprovedV2:
		return state.reviewCandidate(ctx, candidateAlias, operation.Scope, domain.DecisionApprove, operation.MemoryRef, "deterministic-verifier-v2", "approved fixture")
	case RememberRejectedV2:
		return state.reviewCandidate(ctx, candidateAlias, operation.Scope, domain.DecisionReject, "", "deterministic-verifier-v2", "rejected fixture")
	default:
		return fmt.Errorf("unsupported memory review state %q", operation.ReviewState)
	}
}

func (state *caseRuntimeV2) expectedApprovedMemoryV2(alias, scopeID string, candidate domain.MemoryCandidate) domain.MemoryCard {
	createdAt := state.logicalNow.UTC()
	version := 1
	identity := domain.MemoryCard{Kind: candidate.Kind, Category: candidate.Category, Key: candidate.Key, Person: candidate.Person, Relationship: candidate.Relationship}.Identity()
	for _, existing := range state.memoryState {
		if existing.ScopeID != scopeID || existing.Identity != identity {
			continue
		}
		if existing.Expected.Version >= version {
			version = existing.Expected.Version + 1
		}
		if !createdAt.After(existing.Expected.CreatedAt) {
			createdAt = existing.Expected.CreatedAt.Add(time.Nanosecond)
		}
		if existing.Active {
			existing.Active = false
			existing.Expected.Status = domain.MemorySuperseded
			supersededAt := createdAt
			existing.Expected.SupersededAt = &supersededAt
		}
	}
	return domain.MemoryCard{
		ID: stableArtifactIDV2("mem", state.testCase.ID, alias), CandidateID: candidate.ID,
		TenantID: candidate.TenantID, UserID: candidate.UserID, Kind: candidate.Kind, Category: candidate.Category,
		Key: candidate.Key, Value: candidate.Value, Person: candidate.Person, Relationship: candidate.Relationship,
		Backstory: candidate.Backstory, SourceEventIDs: append([]string(nil), candidate.SourceEventIDs...),
		Version: version, Status: domain.MemoryActive, CreatedAt: createdAt,
	}
}

func (state *caseRuntimeV2) registerMemory(alias, scopeID string, sourceAliases []string, card domain.MemoryCard) {
	for _, existing := range state.memoryState {
		if existing.ScopeID == scopeID && existing.Active && existing.Identity == card.Identity() {
			existing.Active = false
		}
	}
	state.memoryIDs[alias] = card.ID
	state.memoryAliases[card.ID] = alias
	state.memoryState[alias] = &logicalMemoryV2{ScopeID: scopeID, MemoryID: card.ID, Identity: card.Identity(), Active: true, SourceAliases: append([]string(nil), sourceAliases...), Expected: card}
}

func (state *caseRuntimeV2) forget(ctx context.Context, scopeID string) error {
	scope := state.scopes[scopeID]
	if _, err := state.service.ForgetUser(ctx, scope.TenantID, scope.UserID); err != nil {
		return err
	}
	for _, memory := range state.memoryState {
		if memory.ScopeID == scopeID {
			memory.Active = false
			memory.Deleted = true
		}
	}
	return nil
}

func (state *caseRuntimeV2) query(ctx context.Context, query *QueryV2, descriptor ArmDescriptor, config ConfigV2, depth int) (QueryResultV2, error) {
	profile, err := judgmentProfileV2(query.Judgments, descriptor.JudgmentProfile)
	if err != nil {
		return QueryResultV2{}, err
	}
	scope := state.scopes[query.Scope]
	input := app.BuildContextInput{TenantID: scope.TenantID, UserID: scope.UserID, Query: query.Text, Limit: depth}
	state.timed.reset()
	var warmupPacks []domain.ContextPack
	for range config.WarmupRuns {
		state.timed.measuring = false
		queryCtx, cancel := context.WithTimeout(ctx, config.QueryTimeout)
		pack, warmupErr := state.service.BuildContext(queryCtx, input)
		cancel()
		if warmupErr != nil {
			return QueryResultV2{}, fmt.Errorf("warmup query: %w", warmupErr)
		}
		warmupPacks = append(warmupPacks, pack)
	}

	state.timed.resetMeasured()
	var executionErr error
	var measuredPacks []domain.ContextPack
	for range config.MeasuredRuns {
		state.timed.measuring = true
		queryCtx, cancel := context.WithTimeout(ctx, config.QueryTimeout)
		pack, runErr := state.service.BuildContext(queryCtx, input)
		cancel()
		if runErr != nil {
			executionErr = errors.Join(executionErr, runErr)
		} else {
			measuredPacks = append(measuredPacks, pack)
		}
	}
	state.timed.measuring = false
	searches := state.timed.searchResults()
	var hits []domain.SearchHit
	if len(searches) > 0 {
		hits = searches[0][:min(depth, len(searches[0]))]
		for _, observed := range searches[1:] {
			if !sameRankingV2(searches[0], observed) {
				executionErr = errors.Join(executionErr, errors.New("measured search rankings changed within one query checkpoint"))
				break
			}
		}
	}
	var canonicalPack domain.ContextPack
	if len(measuredPacks) > 0 {
		canonicalPack = measuredPacks[0]
	}
	result := QueryResultV2{
		CaseID: state.testCase.ID, QueryID: query.ID, Scope: query.Scope, RetrievalDepth: depth,
		Hits: hitResultsV2(hits, state.memoryAliases, state.evidenceAliases, canonicalPack), DurationsNanoseconds: durationNanosecondsV2(state.timed.samples),
	}
	if executionErr != nil {
		result.ExecutionError = executionErr.Error()
	}
	policyPacks := append(append([]domain.ContextPack(nil), warmupPacks...), measuredPacks...)
	result.Policy = state.policyV2(scope, state.timed.allObservations(), profile, policyPacks, executionErr, depth)
	if len(profile.Relevance) > 0 {
		quality, qualityErr := qualityV2(hits, state.memoryAliases, profile.Relevance, config.RecallK, config.NDCGK)
		if qualityErr != nil {
			return QueryResultV2{}, qualityErr
		}
		result.Quality = &quality
	}
	return result, nil
}

func judgmentProfileV2(judgments QueryJudgmentsV2, profile string) (JudgmentProfileV2, error) {
	switch profile {
	case "reviewed-memory-alias-v1":
		if judgments.MemoryCards == nil {
			return JudgmentProfileV2{}, errors.New("arm requires memory_cards judgments")
		}
		return *judgments.MemoryCards, nil
	default:
		return JudgmentProfileV2{}, fmt.Errorf("unsupported judgment profile %q", profile)
	}
}

func qualityV2(hits []domain.SearchHit, aliases map[string]string, relevance map[string]int, recallK, ndcgK int) (QueryQualityV2, error) {
	found := make(map[string]struct{}, len(relevance))
	firstRank := 0
	depth := max(recallK, ndcgK)
	for index, hit := range hits {
		if index >= depth {
			break
		}
		alias := aliases[hit.Memory.ID]
		if relevance[alias] == 0 {
			continue
		}
		if firstRank == 0 {
			firstRank = index + 1
		}
		if index < recallK {
			found[alias] = struct{}{}
		}
	}
	rankedAliases := make([]string, 0, min(ndcgK, len(hits)))
	seen := make(map[string]struct{})
	for index, hit := range hits {
		if index >= ndcgK {
			break
		}
		alias := aliases[hit.Memory.ID]
		if alias == "" {
			alias = fmt.Sprintf("__unknown_%d_%s", index, hit.Memory.ID)
		}
		if _, duplicate := seen[alias]; duplicate {
			alias = fmt.Sprintf("__duplicate_%d_%s", index, alias)
		}
		seen[alias] = struct{}{}
		rankedAliases = append(rankedAliases, alias)
	}
	relevanceFloat := make(map[string]float64, len(relevance))
	for alias, grade := range relevance {
		relevanceFloat[alias] = float64(grade)
	}
	ndcg, err := NDCGAtK(rankedAliases, relevanceFloat, ndcgK)
	if err != nil {
		return QueryQualityV2{}, err
	}
	mrr := 0.0
	if firstRank > 0 {
		mrr = 1 / float64(firstRank)
	}
	return QueryQualityV2{
		RelevantCount: len(relevance), RecallAtK: float64(len(found)) / float64(len(relevance)),
		MRR: mrr, NDCGAtK: ndcg, Passed: len(found) == len(relevance),
	}, nil
}

func (state *caseRuntimeV2) policyV2(scope ScopeV2, searches [][]domain.SearchHit, profile JudgmentProfileV2, packs []domain.ContextPack, executionErr error, depth int) QueryPolicyV2 {
	forbidden := make(map[string]struct{}, len(profile.Forbidden))
	for _, alias := range profile.Forbidden {
		forbidden[alias] = struct{}{}
	}
	policy := QueryPolicyV2{}
	anyHits := false
	for _, hits := range searches {
		if len(hits) > depth {
			policy.OverLimitHits += len(hits) - depth
		}
		if len(hits) > 0 {
			anyHits = true
		}
		seen := make(map[string]struct{}, len(hits))
		for _, hit := range hits {
			alias, known := state.memoryAliases[hit.Memory.ID]
			if !known {
				policy.UnknownHits++
			}
			if _, exists := seen[hit.Memory.ID]; exists {
				policy.DuplicateHits++
			}
			seen[hit.Memory.ID] = struct{}{}
			if hit.Memory.TenantID != scope.TenantID || hit.Memory.UserID != scope.UserID {
				policy.ScopeViolations++
			}
			logical := state.memoryState[alias]
			if hit.Memory.Status != domain.MemoryActive || (logical != nil && (!logical.Active || logical.Deleted)) {
				policy.NonActiveHits++
			}
			if logical != nil && !memoryCardsEqualV2(hit.Memory, logical.Expected) {
				policy.MemoryPayloadViolations++
			}
			if _, exists := forbidden[alias]; exists {
				policy.ForbiddenHits++
			}
		}
	}
	policy.RequireEmptyFailure = profile.RequireEmpty && anyHits
	for _, pack := range packs {
		for _, item := range pack.Items {
			alias := state.memoryAliases[item.Memory.ID]
			logical := state.memoryState[alias]
			if logical == nil {
				continue
			}
			if !memoryCardsEqualV2(item.Memory, logical.Expected) {
				policy.MemoryPayloadViolations++
			}
			actualAliases := make([]string, 0, len(item.Sources))
			for _, source := range item.Sources {
				sourceAlias, known := state.evidenceAliases[source.ID]
				if !known {
					policy.UnknownSourceIDs++
				}
				actualAliases = append(actualAliases, sourceAlias)
				if source.TenantID != scope.TenantID || source.UserID != scope.UserID {
					policy.SourceScopeViolations++
				}
				if known && !evidenceEventsEqualV2(source, state.evidenceExpected[sourceAlias]) {
					policy.EvidencePayloadViolations++
				}
			}
			if len(actualAliases) != len(logical.SourceAliases) {
				policy.MissingSources++
			} else if !slices.Equal(actualAliases, logical.SourceAliases) {
				policy.ReorderedSources++
			}
		}
	}
	policy.Passed = executionErr == nil && policy.ForbiddenHits == 0 && !policy.RequireEmptyFailure && policy.ScopeViolations == 0 && policy.NonActiveHits == 0 && policy.UnknownHits == 0 && policy.DuplicateHits == 0 && policy.OverLimitHits == 0 && policy.UnknownSourceIDs == 0 && policy.MissingSources == 0 && policy.ReorderedSources == 0 && policy.SourceScopeViolations == 0 && policy.MemoryPayloadViolations == 0 && policy.EvidencePayloadViolations == 0
	return policy
}

func summarizeArmV2(queries []QueryResultV2) (ArmAggregateV2, error) {
	aggregate := ArmAggregateV2{QueryCount: len(queries), PolicyPassed: true}
	var durations []time.Duration
	policyPasses := 0
	nonRecallPasses := 0
	for _, query := range queries {
		if query.Quality != nil {
			aggregate.QualityQueryCount++
			aggregate.RecallAtK += query.Quality.RecallAtK
			aggregate.MRR += query.Quality.MRR
			aggregate.NDCGAtK += query.Quality.NDCGAtK
			if query.Quality.Passed {
				aggregate.PassRate++
			}
		} else {
			aggregate.NonRecallQueryCount++
			if query.Policy.Passed && query.ExecutionError == "" {
				nonRecallPasses++
			}
		}
		for _, value := range query.DurationsNanoseconds {
			durations = append(durations, time.Duration(value))
		}
		aggregate.ForbiddenHits += query.Policy.ForbiddenHits
		if query.Policy.RequireEmptyFailure {
			aggregate.RequireEmptyFailures++
		}
		aggregate.ScopeViolations += query.Policy.ScopeViolations
		aggregate.NonActiveHits += query.Policy.NonActiveHits
		aggregate.UnknownHits += query.Policy.UnknownHits
		aggregate.DuplicateHits += query.Policy.DuplicateHits
		aggregate.OverLimitHits += query.Policy.OverLimitHits
		aggregate.UnknownSourceIDs += query.Policy.UnknownSourceIDs
		aggregate.MissingSources += query.Policy.MissingSources
		aggregate.ReorderedSources += query.Policy.ReorderedSources
		aggregate.SourceScopeViolations += query.Policy.SourceScopeViolations
		aggregate.MemoryPayloadViolations += query.Policy.MemoryPayloadViolations
		aggregate.EvidencePayloadViolations += query.Policy.EvidencePayloadViolations
		if query.ExecutionError != "" {
			aggregate.QueryExecutionFailures++
		}
		aggregate.PolicyPassed = aggregate.PolicyPassed && query.Policy.Passed && query.ExecutionError == ""
		if query.Policy.Passed && query.ExecutionError == "" {
			policyPasses++
		}
	}
	if aggregate.QualityQueryCount > 0 {
		count := float64(aggregate.QualityQueryCount)
		aggregate.RecallAtK /= count
		aggregate.MRR /= count
		aggregate.NDCGAtK /= count
		aggregate.PassRate /= count
	}
	if aggregate.QueryCount > 0 {
		aggregate.PolicyPassRate = float64(policyPasses) / float64(aggregate.QueryCount)
	}
	if aggregate.NonRecallQueryCount > 0 {
		aggregate.NonRecallPassRate = float64(nonRecallPasses) / float64(aggregate.NonRecallQueryCount)
	}
	if len(durations) > 0 {
		latency, err := SummarizeLatency(durations)
		if err != nil {
			return ArmAggregateV2{}, err
		}
		aggregate.LatencyP50Nanoseconds = latency.P50.Nanoseconds()
		aggregate.LatencyP95Nanoseconds = latency.P95.Nanoseconds()
		aggregate.LatencySampleCount = len(durations)
		for _, duration := range durations {
			if duration.Nanoseconds() > aggregate.LatencyMaxNanoseconds {
				aggregate.LatencyMaxNanoseconds = duration.Nanoseconds()
			}
		}
	}
	aggregate.QualityResultSHA256 = qualityResultHashV2(queries)
	return aggregate, nil
}

type timedRetrieverV2 struct {
	inner        app.Retriever
	timer        Timer
	measuring    bool
	samples      []time.Duration
	searches     [][]domain.SearchHit
	observations [][]domain.SearchHit
}

func (retriever *timedRetrieverV2) Search(ctx context.Context, tenantID, userID, query string, limit int) ([]domain.SearchHit, error) {
	started := retriever.timer.Now()
	hits, err := retriever.inner.Search(ctx, tenantID, userID, query, limit)
	elapsed := retriever.timer.Now().Sub(started)
	if elapsed < 0 {
		return nil, errors.New("monotonic evaluation timer moved backwards")
	}
	if retriever.measuring {
		retriever.samples = append(retriever.samples, elapsed)
		retriever.searches = append(retriever.searches, append([]domain.SearchHit(nil), hits...))
	}
	retriever.observations = append(retriever.observations, append([]domain.SearchHit(nil), hits...))
	return hits, err
}

func (retriever *timedRetrieverV2) reset() {
	retriever.resetMeasured()
	retriever.observations = nil
}

func (retriever *timedRetrieverV2) resetMeasured() {
	retriever.samples = nil
	retriever.searches = nil
}

func (retriever *timedRetrieverV2) searchResults() [][]domain.SearchHit {
	results := make([][]domain.SearchHit, len(retriever.searches))
	for index, hits := range retriever.searches {
		results[index] = append([]domain.SearchHit(nil), hits...)
	}
	return results
}

func (retriever *timedRetrieverV2) allObservations() [][]domain.SearchHit {
	results := make([][]domain.SearchHit, len(retriever.observations))
	for index, hits := range retriever.observations {
		results[index] = append([]domain.SearchHit(nil), hits...)
	}
	return results
}

func sameRankingV2(left, right []domain.SearchHit) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index].Memory.ID != right[index].Memory.ID ||
			!memoryCardsEqualV2(left[index].Memory, right[index].Memory) ||
			left[index].Score != right[index].Score {
			return false
		}
	}
	return true
}

func hitResultsV2(hits []domain.SearchHit, aliases, evidenceAliases map[string]string, pack domain.ContextPack) []HitResultV2 {
	sourcesByMemory := make(map[string][]string, len(pack.Items))
	sourcePayloadsByMemory := make(map[string][]string, len(pack.Items))
	for _, item := range pack.Items {
		sourceAliases := make([]string, 0, len(item.Sources))
		sourcePayloads := make([]string, 0, len(item.Sources))
		for _, source := range item.Sources {
			sourceAliases = append(sourceAliases, evidenceAliases[source.ID])
			sourcePayloads = append(sourcePayloads, evidencePayloadHashV2(source))
		}
		sourcesByMemory[item.Memory.ID] = sourceAliases
		sourcePayloadsByMemory[item.Memory.ID] = sourcePayloads
	}
	results := make([]HitResultV2, 0, len(hits))
	for index, hit := range hits {
		results = append(results, HitResultV2{
			Rank: index + 1, Alias: aliases[hit.Memory.ID], MemoryID: hit.Memory.ID,
			TenantID: hit.Memory.TenantID, UserID: hit.Memory.UserID, Status: string(hit.Memory.Status), Score: hit.Score,
			SourceAliases:       append([]string(nil), sourcesByMemory[hit.Memory.ID]...),
			SourcePayloadSHA256: append([]string(nil), sourcePayloadsByMemory[hit.Memory.ID]...),
			PayloadSHA256:       memoryPayloadHashV2(hit.Memory),
		})
	}
	return results
}

func memoryCardsEqualV2(left, right domain.MemoryCard) bool {
	return left.ID == right.ID && left.CandidateID == right.CandidateID && left.TenantID == right.TenantID && left.UserID == right.UserID &&
		left.Kind == right.Kind && left.Category == right.Category && left.Key == right.Key && left.Value == right.Value &&
		left.Person == right.Person && left.Relationship == right.Relationship && left.Backstory == right.Backstory &&
		slices.Equal(left.SourceEventIDs, right.SourceEventIDs) && left.Version == right.Version && left.Status == right.Status &&
		left.CreatedAt.Equal(right.CreatedAt) && equalOptionalTimeV2(left.SupersededAt, right.SupersededAt)
}

func equalOptionalTimeV2(left, right *time.Time) bool {
	if left == nil || right == nil {
		return left == nil && right == nil
	}
	return left.Equal(*right)
}

func memoryPayloadHashV2(memory domain.MemoryCard) string {
	encoded, _ := json.Marshal(memory)
	digest := sha256.Sum256(encoded)
	return hex.EncodeToString(digest[:])
}

func memoryCandidatesEqualV2(left, right domain.MemoryCandidate) bool {
	return left.ID == right.ID && left.TenantID == right.TenantID && left.UserID == right.UserID &&
		left.Kind == right.Kind && left.Category == right.Category && left.Key == right.Key && left.Value == right.Value &&
		left.Person == right.Person && left.Relationship == right.Relationship && left.Backstory == right.Backstory &&
		slices.Equal(left.SourceEventIDs, right.SourceEventIDs) && left.Extractor == right.Extractor && left.ExtractorVersion == right.ExtractorVersion &&
		left.Status == right.Status && candidateReviewsEqualV2(left.Review, right.Review) && left.CreatedAt.Equal(right.CreatedAt) && maps.Equal(left.Metadata, right.Metadata)
}

func candidateReviewsEqualV2(left, right *domain.CandidateReview) bool {
	if left == nil || right == nil {
		return left == nil && right == nil
	}
	return left.Decision == right.Decision && left.ReviewerID == right.ReviewerID && left.Reason == right.Reason && left.ReviewedAt.Equal(right.ReviewedAt)
}

func evidenceEventsEqualV2(left, right domain.EvidenceEvent) bool {
	return left.ID == right.ID && left.TenantID == right.TenantID && left.UserID == right.UserID &&
		left.SessionID == right.SessionID && left.Actor == right.Actor && left.Content == right.Content &&
		maps.Equal(left.Metadata, right.Metadata) && left.OccurredAt.Equal(right.OccurredAt) && left.RecordedAt.Equal(right.RecordedAt)
}

func evidencePayloadHashV2(event domain.EvidenceEvent) string {
	encoded, _ := json.Marshal(event)
	digest := sha256.Sum256(encoded)
	return hex.EncodeToString(digest[:])
}

func durationNanosecondsV2(durations []time.Duration) []int64 {
	values := make([]int64, len(durations))
	for index, duration := range durations {
		values[index] = duration.Nanoseconds()
	}
	return values
}

func stableArtifactIDV2(prefix, caseID, alias string) string {
	digest := sha256.Sum256([]byte(caseID + "\x00" + alias))
	return prefix + "_evalv2_" + hex.EncodeToString(digest[:12])
}

func countQueriesV2(dataset DatasetV2) int {
	count := 0
	for _, testCase := range dataset.Cases {
		for _, operation := range testCase.Timeline {
			if operation.Operation() == OperationQueryV2 {
				count++
			}
		}
	}
	return count
}

func configHashV2(config ConfigV2) string {
	descriptors := make([]ArmDescriptor, 0, len(config.Arms))
	for _, factory := range config.Arms {
		descriptors = append(descriptors, factory.Descriptor())
	}
	value := struct {
		Runner             string          `json:"runner"`
		RecallK            int             `json:"recall_k"`
		NDCGK              int             `json:"ndcg_k"`
		WarmupRuns         int             `json:"warmup_runs"`
		MeasuredRuns       int             `json:"measured_runs"`
		QueryTimeout       int64           `json:"query_timeout_nanoseconds"`
		CleanRequired      bool            `json:"clean_required"`
		PolicyPassRequired bool            `json:"policy_pass_required"`
		Arms               []ArmDescriptor `json:"arms"`
	}{runnerVersionV2, config.RecallK, config.NDCGK, config.WarmupRuns, config.MeasuredRuns, config.QueryTimeout.Nanoseconds(), config.Source.CleanRequired, config.RequirePolicyPass, descriptors}
	encoded, _ := json.Marshal(value)
	digest := sha256.Sum256(encoded)
	return hex.EncodeToString(digest[:])
}

func qualityResultHashV2(queries []QueryResultV2) string {
	type stableQuery struct {
		CaseID         string          `json:"case_id"`
		QueryID        string          `json:"query_id"`
		Hits           []HitResultV2   `json:"hits"`
		Quality        *QueryQualityV2 `json:"quality,omitempty"`
		Policy         QueryPolicyV2   `json:"policy"`
		ExecutionError string          `json:"execution_error,omitempty"`
	}
	stable := make([]stableQuery, 0, len(queries))
	for _, query := range queries {
		stable = append(stable, stableQuery{query.CaseID, query.QueryID, query.Hits, query.Quality, query.Policy, query.ExecutionError})
	}
	encoded, _ := json.Marshal(stable)
	digest := sha256.Sum256(encoded)
	return hex.EncodeToString(digest[:])
}
