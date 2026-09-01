//go:build integration && !vector

package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"os/exec"
	"sort"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/ksana-ai/agent-memory-system/internal/domain"
	"github.com/ksana-ai/agent-memory-system/internal/embedding"
	"github.com/ksana-ai/agent-memory-system/internal/migrations"
	storecontract "github.com/ksana-ai/agent-memory-system/internal/store"
	"github.com/ksana-ai/agent-memory-system/internal/store/postgres"
)

const (
	promoterProcessSecret       = "TOP_SECRET_PROMOTER_PROCESS"
	promoterProcessEvidenceText = "sensitive promotion integration evidence"
	promoterProcessCardValue    = "sensitive promotion integration memory"
	promoterProcessModel        = "sensitive-promoter-test-model"
	promoterInitialOperationID  = "11111111-1111-4111-8111-111111111111"
	promoterProcessOperationID  = "22222222-2222-4222-8222-222222222222"
)

var promoterDatabaseSequence atomic.Uint64

func TestProjectionPromoterProcessRestartReplaysReceiptWithoutProviderOrSecondInvalidation(t *testing.T) {
	binary := requiredProjectionPromoterBinary(t)
	databaseURL := isolatedProjectionPromoterDatabase(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	if err := migrations.Apply(ctx, databaseURL); err != nil {
		t.Fatal("apply isolated promoter migrations")
	}
	storage, err := postgres.Open(ctx, databaseURL)
	if err != nil {
		t.Fatal("open isolated promoter store")
	}
	defer storage.Close()

	probeVector := make([]float32, postgres.VectorDimension)
	probeVector[23] = 1
	newDefinition := projectionPromoterSpaceDefinition(
		t,
		promoterProcessModel,
		embedding.VectorSHA256(probeVector),
		time.Now().UTC().Add(-10*time.Second),
	)
	oldDefinition := projectionPromoterSpaceDefinition(
		t,
		"previous-promoter-model",
		strings.Repeat("b", 64),
		time.Now().UTC().Add(-20*time.Second),
	)
	registerProjectionPromoterTarget(t, storage, oldDefinition)
	initialReceipt, err := storage.PromoteProjection(ctx, postgres.PromoteProjectionCommand{
		OperationID: promoterInitialOperationID,
		ToSpace:     oldDefinition.ID,
		AllowEmpty:  true,
	})
	if err != nil || initialReceipt.ToSpace != oldDefinition.ID || initialReceipt.LiveCardCount != 0 {
		t.Fatal("establish initial serving projection")
	}
	registerProjectionPromoterTarget(t, storage, newDefinition)

	tenantID := fmt.Sprintf("tenant_promoter_%d", time.Now().UnixNano())
	userID := fmt.Sprintf("user_promoter_%d", time.Now().UnixNano())
	card := seedProjectionPromoterCard(t, storage, tenantID, userID)
	projectPromotionCard(t, storage, card, oldDefinition, 1)
	projectPromotionCard(t, storage, card, newDefinition, 2)
	markProjectionPromoterJobsSucceeded(t, databaseURL, card, oldDefinition.ID, newDefinition.ID)

	beforeState, err := storage.CurrentServingProjection(ctx)
	if err != nil || beforeState.Target == nil || beforeState.Target.Space.ID != oldDefinition.ID {
		t.Fatal("load serving projection before process promotion")
	}
	revisionBefore, err := storage.ContextRevision(ctx, tenantID, userID)
	if err != nil {
		t.Fatal("load scope revision before process promotion")
	}

	var probeRequests atomic.Int64
	probeServer := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		probeRequests.Add(1)
		var payload struct {
			Model string   `json:"model"`
			Input []string `json:"input"`
		}
		if request.Method != http.MethodPost || request.URL.Path != "/v1/embeddings" ||
			json.NewDecoder(request.Body).Decode(&payload) != nil ||
			payload.Model != promoterProcessModel ||
			len(payload.Input) != 1 || payload.Input[0] != embedding.ProbeTextV1 {
			response.WriteHeader(http.StatusBadRequest)
			return
		}
		response.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(response).Encode(map[string]any{
			"model": promoterProcessModel,
			"data":  []any{map[string]any{"index": 0, "embedding": probeVector}},
		}); err != nil {
			t.Error("encode promoter probe response")
		}
	}))
	t.Cleanup(probeServer.Close)

	childConfig := projectionPromoterChildConfig{
		databaseURL:   databaseURL,
		embeddingsURL: probeServer.URL + "/v1/embeddings",
		model:         promoterProcessModel,
		toSpace:       newDefinition.ID,
		operationID:   promoterProcessOperationID,
		expectedFrom:  oldDefinition.ID,
	}
	firstOutput, firstErr := runProjectionPromoterChild(t, binary, childConfig)
	if firstErr != nil {
		t.Fatal("first projection promoter process failed")
	}
	assertProjectionPromoterOutputRedacted(t, firstOutput,
		databaseURL, childConfig.embeddingsURL, promoterProcessModel,
		tenantID, userID, card.ID, card.Key, card.Value, card.Backstory,
		promoterProcessEvidenceText, promoterProcessCardValue,
	)
	if !strings.Contains(firstOutput, promoterProcessOperationID) ||
		!strings.Contains(firstOutput, newDefinition.ID) {
		t.Fatal("promotion process omitted its non-sensitive receipt identity")
	}
	if probeRequests.Load() != 1 {
		t.Fatalf("first promotion issued %d public probes, want 1", probeRequests.Load())
	}

	receipt, err := storage.ProjectionPromotionByOperationID(ctx, promoterProcessOperationID)
	if err != nil {
		t.Fatal("load durable process promotion receipt")
	}
	if receipt.FromSpace != oldDefinition.ID || receipt.ToSpace != newDefinition.ID || receipt.AllowEmpty ||
		receipt.LiveScopeCount != 1 || receipt.LiveCardCount != 1 || receipt.CoveredCardCount != 1 ||
		receipt.PreviousGeneration != beforeState.Generation || receipt.Generation != beforeState.Generation+1 {
		t.Fatalf("durable process receipt=%#v", receipt)
	}
	afterState, err := storage.CurrentServingProjection(ctx)
	if err != nil || afterState.Target == nil || afterState.Target.Space.ID != newDefinition.ID ||
		afterState.Generation != receipt.Generation {
		t.Fatal("new projection was not durably serving")
	}
	oldTarget, err := storage.ProjectionTargetBySpace(ctx, oldDefinition.ID)
	if err != nil || oldTarget.State != postgres.ProjectionTargetShadow || !oldTarget.EnqueueNew {
		t.Fatal("old serving projection was not atomically moved to shadow")
	}
	revisionAfter, err := storage.ContextRevision(ctx, tenantID, userID)
	if err != nil || revisionAfter != revisionBefore+1 {
		t.Fatalf("scope revision after promotion=%d error=%v, want %d", revisionAfter, err, revisionBefore+1)
	}

	// The committed receipt is the restart checkpoint. Taking the provider
	// offline proves an exact retry does not require a second external call.
	probeServer.Close()
	restartOutput, restartErr := runProjectionPromoterChild(t, binary, childConfig)
	if restartErr != nil {
		t.Fatal("restarted projection promoter did not recover its committed receipt")
	}
	assertProjectionPromoterOutputRedacted(t, restartOutput,
		databaseURL, childConfig.embeddingsURL, promoterProcessModel,
		tenantID, userID, card.ID, card.Key, card.Value, card.Backstory,
		promoterProcessEvidenceText, promoterProcessCardValue,
	)
	if probeRequests.Load() != 1 {
		t.Fatalf("receipt replay issued a second public probe; requests=%d", probeRequests.Load())
	}
	replayed, err := storage.ProjectionPromotionByOperationID(ctx, promoterProcessOperationID)
	if err != nil || !sameProjectionPromotionReceipt(receipt, replayed) {
		t.Fatalf("replayed receipt=%#v error=%v, want %#v", replayed, err, receipt)
	}
	restartedState, err := storage.CurrentServingProjection(ctx)
	if err != nil || restartedState.Target == nil || restartedState.Target.Space.ID != newDefinition.ID ||
		restartedState.Generation != receipt.Generation {
		t.Fatal("receipt replay changed serving state or generation")
	}
	revisionRestarted, err := storage.ContextRevision(ctx, tenantID, userID)
	if err != nil || revisionRestarted != revisionAfter {
		t.Fatal("receipt replay advanced the scope revision twice")
	}
	if countProjectionPromoterOperations(t, databaseURL, promoterProcessOperationID) != 1 {
		t.Fatal("receipt replay duplicated the durable operation")
	}
}

type projectionPromoterChildConfig struct {
	databaseURL   string
	embeddingsURL string
	model         string
	toSpace       string
	operationID   string
	expectedFrom  string
}

func runProjectionPromoterChild(
	t *testing.T,
	binary string,
	config projectionPromoterChildConfig,
) (string, error) {
	t.Helper()
	command := exec.Command(binary)
	command.Env = projectionPromoterChildEnvironment(config)
	var output bytes.Buffer
	command.Stdout = &output
	command.Stderr = &output
	done := make(chan error, 1)
	if err := command.Start(); err != nil {
		t.Fatal("start projection promoter child")
	}
	go func() { done <- command.Wait() }()
	select {
	case err := <-done:
		return output.String(), err
	case <-time.After(30 * time.Second):
		_ = command.Process.Kill()
		<-done
		t.Fatal("projection promoter child timed out")
		return "", context.DeadlineExceeded
	}
}

func projectionPromoterChildEnvironment(config projectionPromoterChildConfig) []string {
	overrides := map[string]string{
		"DATABASE_URL":                         config.databaseURL,
		"LMSTUDIO_EMBEDDINGS_URL":              config.embeddingsURL,
		"LMSTUDIO_EMBEDDING_MODEL":             config.model,
		"PROJECTION_PROMOTER_EMBEDDING_SPACE":  config.toSpace,
		"PROJECTION_PROMOTER_OPERATION_ID":     config.operationID,
		"PROJECTION_PROMOTER_EXPECTED_FROM":    config.expectedFrom,
		"PROJECTION_PROMOTER_ALLOW_EMPTY":      "false",
		"PROJECTION_PROMOTER_PROCESS_SENTINEL": promoterProcessSecret,
	}
	result := make([]string, 0, len(os.Environ())+len(overrides))
	for _, entry := range os.Environ() {
		key, _, found := strings.Cut(entry, "=")
		if _, replaced := overrides[key]; found && replaced {
			continue
		}
		result = append(result, entry)
	}
	keys := make([]string, 0, len(overrides))
	for key := range overrides {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		result = append(result, key+"="+overrides[key])
	}
	return result
}

func requiredProjectionPromoterBinary(t *testing.T) string {
	t.Helper()
	binary := strings.TrimSpace(os.Getenv("TEST_PROJECTION_PROMOTER_BINARY"))
	if binary == "" {
		t.Fatal("TEST_PROJECTION_PROMOTER_BINARY is required")
	}
	if _, err := os.Stat(binary); err != nil {
		t.Fatal("TEST_PROJECTION_PROMOTER_BINARY is not readable")
	}
	return binary
}

func isolatedProjectionPromoterDatabase(t *testing.T) string {
	t.Helper()
	baseURL := strings.TrimSpace(os.Getenv("TEST_DATABASE_URL"))
	if baseURL == "" {
		t.Fatal("TEST_DATABASE_URL is required")
	}
	baseConfig, err := pgx.ParseConfig(baseURL)
	if err != nil {
		t.Fatal("parse TEST_DATABASE_URL")
	}
	parsedURL, err := url.Parse(baseURL)
	if err != nil || (parsedURL.Scheme != "postgres" && parsedURL.Scheme != "postgresql") {
		t.Fatal("TEST_DATABASE_URL must be a PostgreSQL URL")
	}
	databaseName := fmt.Sprintf(
		"agent_memory_promoter_%d_%d",
		time.Now().UnixNano(),
		promoterDatabaseSequence.Add(1),
	)
	adminConfig := baseConfig.Copy()
	adminConfig.Database = "postgres"
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	admin, err := pgx.ConnectConfig(ctx, adminConfig)
	if err != nil {
		t.Fatal("connect to PostgreSQL maintenance database")
	}
	defer admin.Close(context.Background())
	quotedDatabase := pgx.Identifier{databaseName}.Sanitize()
	if _, err := admin.Exec(ctx, "CREATE DATABASE "+quotedDatabase); err != nil {
		t.Fatal("create isolated projection promoter database")
	}
	t.Cleanup(func() {
		dropContext, dropCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer dropCancel()
		dropConnection, dropErr := pgx.ConnectConfig(dropContext, adminConfig)
		if dropErr != nil {
			t.Error("reconnect to drop isolated projection promoter database")
			return
		}
		defer dropConnection.Close(context.Background())
		if _, dropErr := dropConnection.Exec(
			dropContext,
			"DROP DATABASE IF EXISTS "+quotedDatabase+" WITH (FORCE)",
		); dropErr != nil {
			t.Error("drop isolated projection promoter database")
		}
	})
	parsedURL.Path = "/" + databaseName
	parsedURL.RawPath = ""
	parameters := parsedURL.Query()
	parameters.Set("application_name", promoterProcessSecret)
	parsedURL.RawQuery = parameters.Encode()
	return parsedURL.String()
}

func projectionPromoterSpaceDefinition(
	t *testing.T,
	model, fingerprint string,
	createdAt time.Time,
) postgres.EmbeddingSpaceDefinition {
	t.Helper()
	space, err := embedding.SpaceID(
		embedding.ProviderLMStudio,
		model,
		postgres.VectorDimension,
		embedding.MemoryCardDocumentVersion,
		embedding.RawQueryVersion,
		fingerprint,
	)
	if err != nil {
		t.Fatal("derive projection promoter space")
	}
	return postgres.EmbeddingSpaceDefinition{
		ID:               space,
		Provider:         embedding.ProviderLMStudio,
		Model:            model,
		Dimension:        postgres.VectorDimension,
		DocumentVersion:  embedding.MemoryCardDocumentVersion,
		QueryVersion:     embedding.RawQueryVersion,
		ModelFingerprint: fingerprint,
		CreatedAt:        createdAt.UTC().Truncate(time.Microsecond),
	}
}

func registerProjectionPromoterTarget(
	t *testing.T,
	storage *postgres.Store,
	definition postgres.EmbeddingSpaceDefinition,
) {
	t.Helper()
	if _, err := storage.RegisterProjectionTarget(context.Background(), postgres.RegisterProjectionTargetCommand{
		Space:      definition,
		State:      postgres.ProjectionTargetShadow,
		EnqueueNew: true,
		CreatedAt:  definition.CreatedAt,
	}); err != nil {
		t.Fatal("register projection promoter target")
	}
}

func seedProjectionPromoterCard(
	t *testing.T,
	storage *postgres.Store,
	tenantID, userID string,
) domain.MemoryCard {
	t.Helper()
	ctx := context.Background()
	baseTime := time.Now().UTC().Add(-time.Minute).Truncate(time.Microsecond)
	event := domain.EvidenceEvent{
		ID:         "event-promoter-sensitive",
		TenantID:   tenantID,
		UserID:     userID,
		SessionID:  "session-promoter-sensitive",
		Actor:      domain.ActorUser,
		Content:    promoterProcessEvidenceText,
		OccurredAt: baseTime,
		RecordedAt: baseTime.Add(time.Microsecond),
	}
	if err := storage.AppendEvidence(ctx, event); err != nil {
		t.Fatal("append promoter evidence")
	}
	candidate := domain.MemoryCandidate{
		ID:               "candidate-promoter-sensitive",
		TenantID:         tenantID,
		UserID:           userID,
		Kind:             domain.MemoryKindSemantic,
		Category:         "promotion",
		Key:              "promoter-sensitive-key",
		Value:            promoterProcessCardValue,
		Person:           "self",
		Relationship:     "self",
		Backstory:        "sensitive promoter backstory",
		SourceEventIDs:   []string{event.ID},
		Extractor:        "promoter-integration",
		ExtractorVersion: "v1",
		Status:           domain.CandidatePending,
		CreatedAt:        baseTime.Add(2 * time.Microsecond),
	}
	if err := storage.CreateCandidate(ctx, candidate); err != nil {
		t.Fatal("create promoter candidate")
	}
	_, card, err := storage.ReviewCandidate(ctx, storecontract.CandidateReviewCommand{
		TenantID:    tenantID,
		UserID:      userID,
		CandidateID: candidate.ID,
		MemoryID:    "memory-promoter-sensitive",
		Review: domain.CandidateReview{
			Decision:   domain.DecisionApprove,
			ReviewerID: "promoter-reviewer",
			Reason:     "integration fixture",
			ReviewedAt: baseTime.Add(3 * time.Microsecond),
		},
	})
	if err != nil || card == nil {
		t.Fatal("approve promoter candidate")
	}
	return *card
}

func projectPromotionCard(
	t *testing.T,
	storage *postgres.Store,
	card domain.MemoryCard,
	space postgres.EmbeddingSpaceDefinition,
	vectorIndex int,
) {
	t.Helper()
	vector := make([]float32, postgres.VectorDimension)
	vector[vectorIndex] = 1
	if err := storage.UpsertMemoryEmbedding(context.Background(), postgres.MemoryEmbedding{
		TenantID:         card.TenantID,
		UserID:           card.UserID,
		MemoryID:         card.ID,
		EmbeddingSpace:   space.ID,
		Provider:         space.Provider,
		Model:            space.Model,
		DocumentVersion:  space.DocumentVersion,
		QueryVersion:     space.QueryVersion,
		ModelFingerprint: space.ModelFingerprint,
		ContentSHA256:    embedding.MemoryCardDocumentV1SHA256(card),
		Vector:           vector,
		CreatedAt:        time.Now().UTC().Truncate(time.Microsecond),
	}); err != nil {
		t.Fatal("project promoter card")
	}
}

func markProjectionPromoterJobsSucceeded(
	t *testing.T,
	databaseURL string,
	card domain.MemoryCard,
	spaces ...string,
) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	connection, err := pgx.Connect(ctx, databaseURL)
	if err != nil {
		t.Fatal("connect to complete promoter jobs")
	}
	defer connection.Close(context.Background())
	commandTag, err := connection.Exec(ctx, `
		UPDATE agent_memory.embedding_projection_jobs
		SET state = 'succeeded', attempt_count = 1, lease_version = 1,
		    lease_owner = NULL, lease_until = NULL,
		    updated_at = clock_timestamp(), completed_at = clock_timestamp()
		WHERE tenant_id = $1 AND user_id = $2 AND memory_id = $3
		  AND embedding_space = ANY($4::text[])`,
		card.TenantID, card.UserID, card.ID, spaces,
	)
	if err != nil || commandTag.RowsAffected() != int64(len(spaces)) {
		t.Fatal("complete promoter projection jobs")
	}
}

func countProjectionPromoterOperations(t *testing.T, databaseURL, operationID string) int64 {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	connection, err := pgx.Connect(ctx, databaseURL)
	if err != nil {
		t.Fatal("connect to count promoter operations")
	}
	defer connection.Close(context.Background())
	var count int64
	if err := connection.QueryRow(ctx, `
		SELECT count(*)
		FROM agent_memory.embedding_projection_promotions
		WHERE operation_id = $1`, operationID).Scan(&count); err != nil {
		t.Fatal("count promoter operations")
	}
	return count
}

func assertProjectionPromoterOutputRedacted(t *testing.T, output string, forbidden ...string) {
	t.Helper()
	forbidden = append(forbidden, promoterProcessSecret)
	for _, value := range forbidden {
		if value != "" && strings.Contains(output, value) {
			t.Fatal("projection promoter output leaked sensitive configuration or content")
		}
	}
}

func sameProjectionPromotionReceipt(left, right postgres.ProjectionPromotionReceipt) bool {
	return left.OperationID == right.OperationID &&
		left.FromSpace == right.FromSpace &&
		left.ToSpace == right.ToSpace &&
		left.AllowEmpty == right.AllowEmpty &&
		left.LiveScopeCount == right.LiveScopeCount &&
		left.LiveCardCount == right.LiveCardCount &&
		left.CoveredCardCount == right.CoveredCardCount &&
		left.PreviousGeneration == right.PreviousGeneration &&
		left.Generation == right.Generation &&
		left.CutoffAt.Equal(right.CutoffAt) &&
		left.PromotedAt.Equal(right.PromotedAt)
}
