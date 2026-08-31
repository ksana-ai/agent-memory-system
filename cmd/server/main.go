package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/kai443/go-agent-memory-system/internal/api"
	"github.com/kai443/go-agent-memory-system/internal/app"
	"github.com/kai443/go-agent-memory-system/internal/embedding"
	"github.com/kai443/go-agent-memory-system/internal/extraction"
	"github.com/kai443/go-agent-memory-system/internal/migrations"
	"github.com/kai443/go-agent-memory-system/internal/retrieval"
	"github.com/kai443/go-agent-memory-system/internal/store/postgres"
)

const (
	serverPhaseFTS       = "postgres-fts"
	serverPhaseDense     = "postgres-dense"
	serverPhaseHybridRRF = "postgres-hybrid-rrf"

	retrievalModeFTS    = "fts"
	retrievalModeDense  = "dense"
	retrievalModeHybrid = "hybrid"

	serverStartupTimeout     = 30 * time.Second
	serverEmbeddingTimeout   = 10 * time.Second
	serverDefaultTimeout     = 15 * time.Second
	serverExtractionGrace    = 5 * time.Second
	defaultExtractionTimeout = 10 * time.Second
	maxExtractionTimeout     = 120 * time.Second

	extractionAuthNone   = "none"
	extractionAuthBearer = "bearer"
)

type serverConfig struct {
	address        string
	databaseURL    string
	retrievalMode  string
	embeddingsURL  string
	embeddingModel string
	expectedSpace  string
	retrievalPhase string

	extractionEnabled  bool
	extractionEndpoint string
	extractionModel    string
	extractionAuthMode string
	extractionToken    string
	extractionTimeout  time.Duration
	extractorName      string
	extractorVersion   string
}

type denseReadiness interface {
	Ready(context.Context) error
}

type storageReadiness interface {
	Ping(context.Context) error
}

type combinedReadiness struct {
	storage storageReadiness
	dense   denseReadiness
}

func (readiness combinedReadiness) Ping(ctx context.Context) error {
	if readiness.storage == nil || readiness.dense == nil {
		return errors.New("retrieval readiness is not configured")
	}
	if err := readiness.storage.Ping(ctx); err != nil {
		return err
	}
	return readiness.dense.Ready(ctx)
}

func main() {
	if err := run(os.Args[1:], os.Getenv); err != nil {
		slog.Error("server stopped", "error", err)
		os.Exit(1)
	}
}

func run(args []string, getenv func(string) string) error {
	config, err := loadServerConfig(args, getenv)
	if err != nil {
		return err
	}
	var candidateExtractor extraction.Extractor
	if config.extractionEnabled {
		client, createErr := extraction.NewClient(extraction.Config{
			Endpoint:         config.extractionEndpoint,
			Model:            config.extractionModel,
			BearerToken:      config.extractionToken,
			Timeout:          config.extractionTimeout,
			ExtractorName:    config.extractorName,
			ExtractorVersion: config.extractorVersion,
		})
		if createErr != nil {
			return fmt.Errorf("create candidate extractor: %w", createErr)
		}
		candidateExtractor = client
	}

	startupContext, cancelStartup := context.WithTimeout(context.Background(), serverStartupTimeout)
	if err := migrations.Apply(startupContext, config.databaseURL); err != nil {
		cancelStartup()
		return fmt.Errorf("apply database migrations: %w", err)
	}
	storage, err := postgres.Open(startupContext, config.databaseURL)
	cancelStartup()
	if err != nil {
		return fmt.Errorf("open PostgreSQL store: %w", err)
	}
	defer storage.Close()

	var selectedRetriever app.Retriever = storage
	var readiness api.ReadinessChecker = storage
	if config.retrievalMode != retrievalModeFTS {
		embeddingClient, createErr := embedding.NewClient(embedding.Config{
			Endpoint:          config.embeddingsURL,
			Model:             config.embeddingModel,
			ExpectedDimension: postgres.VectorDimension,
			Timeout:           serverEmbeddingTimeout,
			MaxBatchSize:      2,
		})
		if createErr != nil {
			return errors.New("create dense retrieval embedding client failed")
		}
		dense, createErr := retrieval.NewDense(storage, embeddingClient, config.expectedSpace)
		if createErr != nil {
			return errors.New("create dense retriever failed")
		}
		selectedRetriever = dense
		if config.retrievalMode == retrievalModeHybrid {
			selectedRetriever, createErr = retrieval.NewHybrid(storage, dense)
			if createErr != nil {
				return errors.New("create hybrid retriever failed")
			}
		}
		readiness = combinedReadiness{storage: storage, dense: dense}
	}

	serviceOptions := make([]app.Option, 0, 1)
	if candidateExtractor != nil {
		serviceOptions = append(serviceOptions, app.WithCandidateExtractor(candidateExtractor))
	}
	service, err := app.New(storage, selectedRetriever, serviceOptions...)
	if err != nil {
		return fmt.Errorf("create service: %w", err)
	}
	handler, err := api.NewHandler(
		service,
		api.WithPhase(config.retrievalPhase),
		api.WithStorage("postgresql"),
		api.WithReadiness(readiness),
	)
	if err != nil {
		return fmt.Errorf("create HTTP handler: %w", err)
	}

	writeTimeout := serverDefaultTimeout
	if config.extractionEnabled && config.extractionTimeout+serverExtractionGrace > writeTimeout {
		writeTimeout = config.extractionTimeout + serverExtractionGrace
	}
	server := &http.Server{
		Addr:              config.address,
		Handler:           handler.Routes(),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       serverDefaultTimeout,
		WriteTimeout:      writeTimeout,
		IdleTimeout:       60 * time.Second,
	}
	shutdownContext, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	slog.Info("starting HTTP server", "address", config.address, "storage", "postgresql", "retrieval", config.retrievalPhase)
	serverError := make(chan error, 1)
	go func() {
		serverError <- server.ListenAndServe()
	}()

	select {
	case err := <-serverError:
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			return fmt.Errorf("serve HTTP: %w", err)
		}
		return nil
	case <-shutdownContext.Done():
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := server.Shutdown(ctx); err != nil {
			return fmt.Errorf("shutdown HTTP server: %w", err)
		}
		if err := <-serverError; err != nil && !errors.Is(err, http.ErrServerClosed) {
			return fmt.Errorf("serve HTTP during shutdown: %w", err)
		}
		return nil
	}
}

func loadServerConfig(args []string, getenv func(string) string) (serverConfig, error) {
	if getenv == nil {
		return serverConfig{}, errors.New("server environment reader is required")
	}
	flags := flag.NewFlagSet("agent-memory-server", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	address := flags.String("addr", "127.0.0.1:8080", "HTTP listen address")
	if err := flags.Parse(args); err != nil || flags.NArg() != 0 {
		return serverConfig{}, errors.New("parse server arguments failed")
	}

	config := serverConfig{
		address:       strings.TrimSpace(*address),
		databaseURL:   strings.TrimSpace(getenv("DATABASE_URL")),
		retrievalMode: strings.TrimSpace(getenv("SERVER_RETRIEVAL_MODE")),
	}
	if config.address == "" {
		return serverConfig{}, errors.New("server address is required")
	}
	if config.databaseURL == "" {
		return serverConfig{}, errors.New("PostgreSQL connection URL is required: set DATABASE_URL")
	}
	if config.retrievalMode == "" {
		config.retrievalMode = retrievalModeFTS
	}
	switch config.retrievalMode {
	case retrievalModeFTS:
		config.retrievalPhase = serverPhaseFTS
	case retrievalModeDense:
		config.retrievalPhase = serverPhaseDense
	case retrievalModeHybrid:
		config.retrievalPhase = serverPhaseHybridRRF
	default:
		return serverConfig{}, errors.New("SERVER_RETRIEVAL_MODE must be fts, dense, or hybrid")
	}

	if config.retrievalMode != retrievalModeFTS {
		config.embeddingsURL = strings.TrimSpace(getenv("LMSTUDIO_EMBEDDINGS_URL"))
		config.embeddingModel = strings.TrimSpace(getenv("LMSTUDIO_EMBEDDING_MODEL"))
		config.expectedSpace = strings.TrimSpace(getenv("SERVER_EXPECTED_SERVING_SPACE"))
		if config.embeddingsURL == "" {
			return serverConfig{}, errors.New("LMSTUDIO_EMBEDDINGS_URL is required for dense or hybrid retrieval")
		}
		if config.embeddingModel == "" {
			return serverConfig{}, errors.New("LMSTUDIO_EMBEDDING_MODEL is required for dense or hybrid retrieval")
		}
		if config.expectedSpace == "" {
			return serverConfig{}, errors.New("SERVER_EXPECTED_SERVING_SPACE is required for dense or hybrid retrieval")
		}
	}

	enabledValue := strings.TrimSpace(getenv("MEMORY_EXTRACTION_ENABLED"))
	if enabledValue == "" {
		enabledValue = "false"
	}
	var enabled bool
	switch enabledValue {
	case "true":
		enabled = true
	case "false":
		enabled = false
	default:
		return serverConfig{}, errors.New("MEMORY_EXTRACTION_ENABLED must be true or false")
	}
	config.extractionEnabled = enabled
	if !enabled {
		return config, nil
	}

	config.extractionEndpoint = strings.TrimSpace(getenv("MEMORY_EXTRACTION_ENDPOINT"))
	config.extractionModel = strings.TrimSpace(getenv("MEMORY_EXTRACTION_MODEL"))
	config.extractionAuthMode = strings.TrimSpace(getenv("MEMORY_EXTRACTION_AUTH_MODE"))
	if config.extractionAuthMode == "" {
		config.extractionAuthMode = extractionAuthNone
	}
	config.extractorName = strings.TrimSpace(getenv("MEMORY_EXTRACTION_EXTRACTOR_NAME"))
	config.extractorVersion = strings.TrimSpace(getenv("MEMORY_EXTRACTION_EXTRACTOR_VERSION"))
	timeoutValue := strings.TrimSpace(getenv("MEMORY_EXTRACTION_TIMEOUT"))
	if timeoutValue == "" {
		config.extractionTimeout = defaultExtractionTimeout
	} else {
		parsedTimeout, parseErr := time.ParseDuration(timeoutValue)
		if parseErr != nil || parsedTimeout <= 0 || parsedTimeout > maxExtractionTimeout {
			return serverConfig{}, errors.New("MEMORY_EXTRACTION_TIMEOUT must be a positive duration no greater than 120s")
		}
		config.extractionTimeout = parsedTimeout
	}
	if config.extractionEndpoint == "" {
		return serverConfig{}, errors.New("MEMORY_EXTRACTION_ENDPOINT is required when extraction is enabled")
	}
	if config.extractionModel == "" {
		return serverConfig{}, errors.New("MEMORY_EXTRACTION_MODEL is required when extraction is enabled")
	}
	if config.extractorName == "" {
		return serverConfig{}, errors.New("MEMORY_EXTRACTION_EXTRACTOR_NAME is required when extraction is enabled")
	}
	if config.extractorVersion == "" {
		return serverConfig{}, errors.New("MEMORY_EXTRACTION_EXTRACTOR_VERSION is required when extraction is enabled")
	}
	switch config.extractionAuthMode {
	case extractionAuthNone:
	case extractionAuthBearer:
		config.extractionToken = strings.TrimSpace(getenv("MEMORY_EXTRACTION_BEARER_TOKEN"))
		if config.extractionToken == "" {
			return serverConfig{}, errors.New("MEMORY_EXTRACTION_BEARER_TOKEN is required for bearer authentication")
		}
	default:
		return serverConfig{}, errors.New("MEMORY_EXTRACTION_AUTH_MODE must be none or bearer")
	}
	return config, nil
}
