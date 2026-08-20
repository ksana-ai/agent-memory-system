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

	serverStartupTimeout   = 30 * time.Second
	serverEmbeddingTimeout = 10 * time.Second
)

type serverConfig struct {
	address        string
	databaseURL    string
	retrievalMode  string
	embeddingsURL  string
	embeddingModel string
	expectedSpace  string
	retrievalPhase string
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

	service, err := app.New(storage, selectedRetriever)
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

	server := &http.Server{
		Addr:              config.address,
		Handler:           handler.Routes(),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       15 * time.Second,
		WriteTimeout:      15 * time.Second,
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
		return config, nil
	case retrievalModeDense:
		config.retrievalPhase = serverPhaseDense
	case retrievalModeHybrid:
		config.retrievalPhase = serverPhaseHybridRRF
	default:
		return serverConfig{}, errors.New("SERVER_RETRIEVAL_MODE must be fts, dense, or hybrid")
	}

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
	return config, nil
}
