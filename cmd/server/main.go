package main

import (
	"context"
	"errors"
	"flag"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/kai443/go-agent-memory-system/internal/api"
	"github.com/kai443/go-agent-memory-system/internal/app"
	"github.com/kai443/go-agent-memory-system/internal/retrieval"
	"github.com/kai443/go-agent-memory-system/internal/store/memstore"
)

func main() {
	address := flag.String("addr", "127.0.0.1:8080", "HTTP listen address")
	flag.Parse()

	storage := memstore.New()
	retriever, err := retrieval.NewBM25(storage)
	if err != nil {
		slog.Error("create retriever", "error", err)
		os.Exit(1)
	}
	service, err := app.New(storage, retriever)
	if err != nil {
		slog.Error("create service", "error", err)
		os.Exit(1)
	}
	handler, err := api.NewHandler(service)
	if err != nil {
		slog.Error("create HTTP handler", "error", err)
		os.Exit(1)
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
	go func() {
		<-shutdownContext.Done()
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := server.Shutdown(ctx); err != nil {
			slog.Error("shutdown HTTP server", "error", err)
		}
	}()

	slog.Info("starting HTTP server", "address", *address, "storage", "in-memory")
	if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		slog.Error("HTTP server failed", "error", err)
		os.Exit(1)
	}
}
