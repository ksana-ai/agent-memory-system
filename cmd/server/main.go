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
	"github.com/kai443/go-agent-memory-system/internal/store/postgres"
)

const serverPhase = "postgres-fts"

func main() {
	if err := run(); err != nil {
		slog.Error("server stopped", "error", err)
		os.Exit(1)
	}
}

func run() error {
	address := flag.String("addr", "127.0.0.1:8080", "HTTP listen address")
	flag.Parse()
	databaseURL := os.Getenv("DATABASE_URL")
	if strings.TrimSpace(databaseURL) == "" {
		return errors.New("PostgreSQL connection URL is required: set DATABASE_URL")
	}

	startupContext, cancelStartup := context.WithTimeout(context.Background(), 30*time.Second)
	if err := migrations.Apply(startupContext, databaseURL); err != nil {
		cancelStartup()
		return fmt.Errorf("apply database migrations: %w", err)
	}
	storage, err := postgres.Open(startupContext, databaseURL)
	cancelStartup()
	if err != nil {
		return fmt.Errorf("open PostgreSQL store: %w", err)
	}
	defer storage.Close()

	service, err := app.New(storage, storage)
	if err != nil {
		return fmt.Errorf("create service: %w", err)
	}
	handler, err := api.NewHandler(
		service,
		api.WithPhase(serverPhase),
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

	slog.Info("starting HTTP server", "address", *address, "storage", "postgresql", "retrieval", serverPhase)
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
