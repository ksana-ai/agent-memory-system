package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/ksana-ai/agent-memory-system/internal/migrations"
)

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() error {
	if len(os.Args) != 1 {
		return fmt.Errorf("migrate accepts no command-line arguments; set DATABASE_URL in the environment")
	}
	databaseURL := os.Getenv("DATABASE_URL")
	if strings.TrimSpace(databaseURL) == "" {
		return fmt.Errorf("database URL is required: set DATABASE_URL")
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if err := migrations.Apply(ctx, databaseURL); err != nil {
		return err
	}
	log.Print("database migrations applied")
	return nil
}
