package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/kai443/go-agent-memory-system/internal/api"
	"github.com/kai443/go-agent-memory-system/internal/app"
	"github.com/kai443/go-agent-memory-system/internal/migrations"
	"github.com/kai443/go-agent-memory-system/internal/retrieval"
	"github.com/kai443/go-agent-memory-system/internal/store/postgres"
)

func main() {
	if err := run(); err != nil {
		slog.Error("server stopped", "error", err)
		os.Exit(1)
	}
}

func run() error {
	address := flag.String("addr", "127.0.0.1:8080", "HTTP listen address")
	databaseURL := flag.String("database-url", os.Getenv("DATABASE_URL"), "PostgreSQL connection URL (defaults to DATABASE_URL)")
	flag.Parse()
	if strings.TrimSpace(*databaseURL) == "" {
		return errors.New("PostgreSQL connection URL is required: set DATABASE_URL or pass -database-url")
	}

	startupContext, cancelStartup := context.WithTimeout(context.Background(), 30*time.Second)
	if err := migrations.Apply(startupContext, *databaseURL); err != nil {
		cancelStartup()
		return fmt.Errorf("apply database migrations: %w", err)
	}
	storage, err := postgres.Open(startupContext, *databaseURL)
	cancelStartup()
	if err != nil {
		return fmt.Errorf("open PostgreSQL store: %w", err)
	}
	defer storage.Close()

	retriever, err := retrieval.NewBM25(storage)
	if err != nil {
		return fmt.Errorf("create retriever: %w", err)
	}
	service, err := app.New(storage, retriever)
	if err != nil {
		return fmt.Errorf("create service: %w", err)
	}
	handler, err := api.NewHandler(
		service,
		api.WithPhase("durable-storage"),
		api.WithStorage("postgresql"),
		api.WithReadiness(storage),
	)
	if err != nil {
		return fmt.Errorf("create HTTP handler: %w", err)
	}

	server := &http.Server{
		Addr:              *address,
		Handler:           handler.Routes(),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       15 * time.Second,
		WriteTimeout:      15 * time.Second,
		IdleTimeout:       60 * time.Second,
	}
	shutdownContext, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	slog.Info("starting HTTP server", "address", *address, "storage", "postgresql")
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
