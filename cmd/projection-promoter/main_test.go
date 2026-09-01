package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/ksana-ai/agent-memory-system/internal/domain"
	"github.com/ksana-ai/agent-memory-system/internal/embedding"
	"github.com/ksana-ai/agent-memory-system/internal/store/postgres"
)

func TestLoadPromoterProcessConfigRequiresExplicitExpectedStateAndSafeDefaults(t *testing.T) {
	environment := promoterTestEnvironment()
	environment["PROJECTION_PROMOTER_EXPECTED_FROM"] = promoterExpectedNone
	config, err := loadPromoterProcessConfig(nil, promoterEnvironment(environment))
	if err != nil {
		t.Fatalf("load promoter config: %v", err)
	}
	if config.databaseURL != environment["DATABASE_URL"] ||
		config.embeddingsURL != environment["LMSTUDIO_EMBEDDINGS_URL"] ||
		config.embeddingModel != environment["LMSTUDIO_EMBEDDING_MODEL"] ||
		config.toSpace != environment["PROJECTION_PROMOTER_EMBEDDING_SPACE"] ||
		config.operationID != environment["PROJECTION_PROMOTER_OPERATION_ID"] ||
		config.expectedFrom != "" || config.allowEmpty {
		t.Fatalf("config=%#v", config)
	}

	environment["PROJECTION_PROMOTER_EXPECTED_FROM"] = "space_v1_current"
	environment["PROJECTION_PROMOTER_ALLOW_EMPTY"] = "true"
	config, err = loadPromoterProcessConfig(nil, promoterEnvironment(environment))
	if err != nil {
		t.Fatalf("load explicit promoter config: %v", err)
	}
	if config.expectedFrom != "space_v1_current" || !config.allowEmpty {
		t.Fatalf("explicit config=%#v", config)
	}
}

func TestLoadPromoterProcessConfigRejectsInvalidInputWithoutLeakingValues(t *testing.T) {
	const secret = "TOP_SECRET_PROMOTER_CONFIG"
	base := promoterTestEnvironment()
	base["DATABASE_URL"] = "postgres://promoter:" + secret + "@database.invalid/memory"
	base["LMSTUDIO_EMBEDDINGS_URL"] = "http://127.0.0.1:1234/" + secret
	base["LMSTUDIO_EMBEDDING_MODEL"] = "model-" + secret
	base["PROJECTION_PROMOTER_EMBEDDING_SPACE"] = "space_v1_" + secret
	base["PROJECTION_PROMOTER_EXPECTED_FROM"] = "space_v1_previous_" + secret
	tests := []struct {
		name        string
		args        []string
		environment map[string]string
	}{
		{name: "argument", args: []string{"-database-url=" + base["DATABASE_URL"]}, environment: base},
		{name: "missing database", environment: withPromoterEnvironment(base, "DATABASE_URL", "")},
		{name: "missing endpoint", environment: withPromoterEnvironment(base, "LMSTUDIO_EMBEDDINGS_URL", "")},
		{name: "missing model", environment: withPromoterEnvironment(base, "LMSTUDIO_EMBEDDING_MODEL", "")},
		{name: "missing target", environment: withPromoterEnvironment(base, "PROJECTION_PROMOTER_EMBEDDING_SPACE", "")},
		{name: "missing operation", environment: withPromoterEnvironment(base, "PROJECTION_PROMOTER_OPERATION_ID", "")},
		{name: "noncanonical operation", environment: withPromoterEnvironment(base, "PROJECTION_PROMOTER_OPERATION_ID", secret)},
		{name: "uppercase operation", environment: withPromoterEnvironment(base, "PROJECTION_PROMOTER_OPERATION_ID", "8D91B379-D6F7-42D4-9DF0-B6C0A136C8D1")},
		{name: "missing expected state", environment: withPromoterEnvironment(base, "PROJECTION_PROMOTER_EXPECTED_FROM", "")},
		{name: "invalid allow empty", environment: withPromoterEnvironment(base, "PROJECTION_PROMOTER_ALLOW_EMPTY", secret)},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := loadPromoterProcessConfig(test.args, promoterEnvironment(test.environment))
			if err == nil {
				t.Fatal("invalid promoter config was accepted")
			}
			if strings.Contains(err.Error(), secret) || strings.Contains(err.Error(), "promoter:TOP_SECRET") {
				t.Fatalf("configuration error leaked a secret: %v", err)
			}
		})
	}
}

func TestRunPromoterReplaysAnExactReceiptWithoutASecondProbeOrMutation(t *testing.T) {
	config := promoterTestEnvironment()
	config["PROJECTION_PROMOTER_EXPECTED_FROM"] = promoterExpectedNone
	config["PROJECTION_PROMOTER_ALLOW_EMPTY"] = "true"
	cutoff := time.Date(2026, time.August, 20, 4, 5, 6, 0, time.UTC)
	receipt := postgres.ProjectionPromotionReceipt{
		OperationID:        config["PROJECTION_PROMOTER_OPERATION_ID"],
		ToSpace:            config["PROJECTION_PROMOTER_EMBEDDING_SPACE"],
		AllowEmpty:         true,
		LiveScopeCount:     0,
		LiveCardCount:      0,
		CoveredCardCount:   0,
		PreviousGeneration: 8,
		Generation:         9,
		CutoffAt:           cutoff,
		PromotedAt:         cutoff.Add(time.Microsecond),
	}
	repository := &fakePromotionRepository{
		receipt:      receipt,
		lookupErrors: []error{domain.ErrNotFound},
	}
	var events []string
	repository.onLookup = func() { events = append(events, "lookup") }
	repository.onPromote = func() { events = append(events, "promote") }
	open := func(_ context.Context, databaseURL string) (promotionRepository, func(), error) {
		if databaseURL != config["DATABASE_URL"] {
			t.Fatalf("database URL passed incorrectly")
		}
		events = append(events, "open")
		return repository, func() { events = append(events, "close") }, nil
	}
	probe := func(_ context.Context, observed promoterProcessConfig) (string, error) {
		events = append(events, "probe")
		if observed.embeddingsURL != config["LMSTUDIO_EMBEDDINGS_URL"] ||
			observed.embeddingModel != config["LMSTUDIO_EMBEDDING_MODEL"] {
			t.Fatalf("probe config=%#v", observed)
		}
		return config["PROJECTION_PROMOTER_EMBEDDING_SPACE"], nil
	}

	for invocation := 0; invocation < 2; invocation++ {
		got, err := run(context.Background(), nil, promoterEnvironment(config), open, probe)
		if err != nil {
			t.Fatalf("promotion invocation %d: %v", invocation+1, err)
		}
		if !reflect.DeepEqual(got, receipt) {
			t.Fatalf("receipt=%#v, want %#v", got, receipt)
		}
	}
	if len(repository.commands) != 1 || repository.lookupCalls != 2 {
		t.Fatalf("commands=%#v", repository.commands)
	}
	wantCommand := postgres.PromoteProjectionCommand{
		OperationID:  config["PROJECTION_PROMOTER_OPERATION_ID"],
		ExpectedFrom: "",
		ToSpace:      config["PROJECTION_PROMOTER_EMBEDDING_SPACE"],
		AllowEmpty:   true,
	}
	if repository.commands[0] != wantCommand {
		t.Fatalf("command=%#v, want %#v", repository.commands[0], wantCommand)
	}
	if !reflect.DeepEqual(events, []string{
		"open", "lookup", "probe", "promote", "close",
		"open", "lookup", "close",
	}) {
		t.Fatalf("events=%#v", events)
	}
}

func TestRunPromoterRejectsOperationReuseWithDifferentShapeWithoutProbing(t *testing.T) {
	environment := promoterTestEnvironment()
	repository := &fakePromotionRepository{receipt: postgres.ProjectionPromotionReceipt{
		OperationID: environment["PROJECTION_PROMOTER_OPERATION_ID"],
		FromSpace:   environment["PROJECTION_PROMOTER_EXPECTED_FROM"],
		ToSpace:     "space_v1_different_target",
	}}
	probed := false
	_, err := run(
		context.Background(), nil, promoterEnvironment(environment),
		func(context.Context, string) (promotionRepository, func(), error) {
			return repository, func() {}, nil
		},
		func(context.Context, promoterProcessConfig) (string, error) {
			probed = true
			return "", nil
		},
	)
	if err == nil || !strings.Contains(err.Error(), "different command") {
		t.Fatalf("reuse error=%v", err)
	}
	if probed || len(repository.commands) != 0 {
		t.Fatalf("probed=%t commands=%#v", probed, repository.commands)
	}
}

func TestRunPromoterFailsBeforeMutationOnLiveSpaceMismatch(t *testing.T) {
	environment := promoterTestEnvironment()
	repository := &fakePromotionRepository{lookupErrors: []error{domain.ErrNotFound}}
	closed := false
	_, err := run(
		context.Background(),
		nil,
		promoterEnvironment(environment),
		func(context.Context, string) (promotionRepository, func(), error) {
			return repository, func() { closed = true }, nil
		},
		func(context.Context, promoterProcessConfig) (string, error) {
			return "space_v1_live_different", nil
		},
	)
	if err == nil || !strings.Contains(err.Error(), "does not match") {
		t.Fatalf("mismatch error=%v", err)
	}
	if len(repository.commands) != 0 || !closed {
		t.Fatalf("commands=%#v closed=%t", repository.commands, closed)
	}
}

func TestRunPromoterSuppressesConnectionProbeAndRepositoryDetails(t *testing.T) {
	const secret = "TOP_SECRET_PROMOTION_RUNTIME"
	environment := promoterTestEnvironment()
	environment["DATABASE_URL"] = "postgres://promoter:" + secret + "@db.invalid/memory"

	_, err := run(
		context.Background(), nil, promoterEnvironment(environment),
		func(context.Context, string) (promotionRepository, func(), error) {
			return nil, nil, errors.New("database said " + secret)
		},
		func(context.Context, promoterProcessConfig) (string, error) { return "", nil },
	)
	if err == nil || strings.Contains(err.Error(), secret) {
		t.Fatalf("open error=%v", err)
	}

	_, err = run(
		context.Background(), nil, promoterEnvironment(environment),
		func(context.Context, string) (promotionRepository, func(), error) {
			return &fakePromotionRepository{lookupErrors: []error{errors.New("receipt read " + secret)}}, func() {}, nil
		},
		func(context.Context, promoterProcessConfig) (string, error) { return "", nil },
	)
	if err == nil || strings.Contains(err.Error(), secret) {
		t.Fatalf("receipt lookup error=%v", err)
	}

	_, err = run(
		context.Background(), nil, promoterEnvironment(environment),
		func(context.Context, string) (promotionRepository, func(), error) {
			return &fakePromotionRepository{lookupErrors: []error{domain.ErrNotFound}}, func() {}, nil
		},
		func(context.Context, promoterProcessConfig) (string, error) {
			return "", errors.New("provider said " + secret)
		},
	)
	if err == nil || strings.Contains(err.Error(), secret) {
		t.Fatalf("probe error=%v", err)
	}

	repository := &fakePromotionRepository{
		err:          errors.New("card content " + secret),
		lookupErrors: []error{domain.ErrNotFound},
	}
	_, err = run(
		context.Background(), nil, promoterEnvironment(environment),
		func(context.Context, string) (promotionRepository, func(), error) {
			return repository, func() {}, nil
		},
		func(context.Context, promoterProcessConfig) (string, error) {
			return environment["PROJECTION_PROMOTER_EMBEDDING_SPACE"], nil
		},
	)
	if err == nil || strings.Contains(err.Error(), secret) {
		t.Fatalf("repository error=%v", err)
	}
}

func TestProbeLivePromotionSpaceUsesOnlyThePublicProbe(t *testing.T) {
	model := "promoter-test-model"
	vector := make([]float32, postgres.VectorDimension)
	vector[17] = 1
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		var payload struct {
			Model string   `json:"model"`
			Input []string `json:"input"`
		}
		if request.Method != http.MethodPost || json.NewDecoder(request.Body).Decode(&payload) != nil ||
			payload.Model != model || !reflect.DeepEqual(payload.Input, []string{embedding.ProbeTextV1}) {
			t.Error("unexpected promotion probe request")
			response.WriteHeader(http.StatusBadRequest)
			return
		}
		response.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(response).Encode(map[string]any{
			"model": model,
			"data":  []any{map[string]any{"index": 0, "embedding": vector}},
		}); err != nil {
			t.Error("encode probe response")
		}
	}))
	defer server.Close()

	space, err := probeLivePromotionSpace(context.Background(), promoterProcessConfig{
		embeddingsURL:  server.URL,
		embeddingModel: model,
	})
	if err != nil {
		t.Fatalf("probe live promotion space: %v", err)
	}
	want, err := embedding.SpaceID(
		embedding.ProviderLMStudio,
		model,
		postgres.VectorDimension,
		embedding.MemoryCardDocumentVersion,
		embedding.RawQueryVersion,
		embedding.VectorSHA256(vector),
	)
	if err != nil {
		t.Fatalf("derive expected space: %v", err)
	}
	if space != want {
		t.Fatalf("space=%q, want %q", space, want)
	}
}

func TestPromotionReceiptLogContainsOnlyAggregateOperationalFields(t *testing.T) {
	const secret = "TOP_SECRET_CARD_OR_CONNECTION"
	var output bytes.Buffer
	previous := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&output, nil)))
	t.Cleanup(func() { slog.SetDefault(previous) })
	logPromotionReceipt(postgres.ProjectionPromotionReceipt{
		OperationID:        "8d91b379-d6f7-42d4-9df0-b6c0a136c8d1",
		FromSpace:          "space_v1_from",
		ToSpace:            "space_v1_to",
		LiveScopeCount:     2,
		LiveCardCount:      3,
		CoveredCardCount:   3,
		PreviousGeneration: 4,
		Generation:         5,
		CutoffAt:           time.Date(2026, time.August, 20, 1, 2, 3, 0, time.UTC),
		PromotedAt:         time.Date(2026, time.August, 20, 1, 2, 4, 0, time.UTC),
	})
	for _, forbidden := range []string{secret, "DATABASE_URL", "LMSTUDIO_EMBEDDINGS_URL", "content="} {
		if strings.Contains(output.String(), forbidden) {
			t.Fatalf("promotion summary leaked %q: %s", forbidden, output.String())
		}
	}
}

func TestPromoterMakeTargetsKeepConnectionsOutOfArguments(t *testing.T) {
	const (
		databaseURL   = "postgres://promoter:TOP_SECRET_DATABASE@db.invalid/memory"
		embeddingsURL = "http://127.0.0.1:1234/TOP_SECRET_ENDPOINT"
	)
	for _, target := range []string{
		"build-projection-promoter",
		"projection-promote",
		"test-promotion-integration",
		"verify-promotion",
	} {
		command := exec.Command(
			"make", "-n", target,
			"DATABASE_URL="+databaseURL,
			"TEST_DATABASE_URL="+databaseURL,
			"LMSTUDIO_EMBEDDINGS_URL="+embeddingsURL,
			"PROJECTION_PROMOTER_EMBEDDING_SPACE=space_v1_expected",
			"PROJECTION_PROMOTER_OPERATION_ID=8d91b379-d6f7-42d4-9df0-b6c0a136c8d1",
			"PROJECTION_PROMOTER_EXPECTED_FROM=none",
		)
		command.Dir = filepath.Join("..", "..")
		output, err := command.CombinedOutput()
		if err != nil {
			t.Fatalf("make -n %s: %v\n%s", target, err, output)
		}
		for _, secret := range []string{databaseURL, embeddingsURL, "TOP_SECRET_DATABASE", "TOP_SECRET_ENDPOINT"} {
			if strings.Contains(string(output), secret) {
				t.Fatalf("%s leaked connection configuration: %s", target, output)
			}
		}
		if strings.Contains(string(output), "-database-url") || strings.Contains(string(output), "-embeddings-url") {
			t.Fatalf("%s put connection configuration in process arguments: %s", target, output)
		}
	}
}

type fakePromotionRepository struct {
	receipt      postgres.ProjectionPromotionReceipt
	err          error
	lookupErrors []error
	commands     []postgres.PromoteProjectionCommand
	lookupCalls  int
	onLookup     func()
	onPromote    func()
}

func (repository *fakePromotionRepository) ProjectionPromotionByOperationID(
	_ context.Context,
	_ string,
) (postgres.ProjectionPromotionReceipt, error) {
	repository.lookupCalls++
	if repository.onLookup != nil {
		repository.onLookup()
	}
	if len(repository.lookupErrors) != 0 {
		err := repository.lookupErrors[0]
		repository.lookupErrors = repository.lookupErrors[1:]
		return postgres.ProjectionPromotionReceipt{}, err
	}
	if repository.receipt.OperationID == "" {
		return postgres.ProjectionPromotionReceipt{}, domain.ErrNotFound
	}
	return repository.receipt, nil
}

func (repository *fakePromotionRepository) PromoteProjection(
	_ context.Context,
	command postgres.PromoteProjectionCommand,
) (postgres.ProjectionPromotionReceipt, error) {
	if repository.onPromote != nil {
		repository.onPromote()
	}
	repository.commands = append(repository.commands, command)
	if repository.err != nil {
		return postgres.ProjectionPromotionReceipt{}, repository.err
	}
	return repository.receipt, nil
}

func promoterTestEnvironment() map[string]string {
	return map[string]string{
		"DATABASE_URL":                        "postgres://promoter:secret@database.invalid/memory",
		"LMSTUDIO_EMBEDDINGS_URL":             "http://127.0.0.1:1234/v1/embeddings",
		"LMSTUDIO_EMBEDDING_MODEL":            "text-embedding-bge-m3",
		"PROJECTION_PROMOTER_EMBEDDING_SPACE": "space_v1_target",
		"PROJECTION_PROMOTER_OPERATION_ID":    "8d91b379-d6f7-42d4-9df0-b6c0a136c8d1",
		"PROJECTION_PROMOTER_EXPECTED_FROM":   "space_v1_previous",
	}
}

func promoterEnvironment(values map[string]string) func(string) string {
	return func(key string) string { return values[key] }
}

func withPromoterEnvironment(source map[string]string, key, value string) map[string]string {
	result := make(map[string]string, len(source)+1)
	for currentKey, currentValue := range source {
		result[currentKey] = currentValue
	}
	result[key] = value
	return result
}

var _ promotionRepository = (*fakePromotionRepository)(nil)
