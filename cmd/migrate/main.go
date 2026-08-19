package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/kai443/go-agent-memory-system/internal/migrations"
)

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() error {
	databaseURL := flag.String("database-url", os.Getenv("DATABASE_URL"), "PostgreSQL connection URL (or set DATABASE_URL)")
	flag.Parse()

	if strings.TrimSpace(*databaseURL) == "" {
		return fmt.Errorf("database URL is required: set DATABASE_URL or pass -database-url")
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if err := migrations.Apply(ctx, *databaseURL); err != nil {
		return err
	}
	log.Print("database migrations applied")
	return nil
}
