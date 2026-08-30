// Command api serves the ledger's public HTTP surface.
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	nethttp "net/http"
	"os"
	"os/signal"
	"syscall"

	"golang.org/x/sync/errgroup"

	"github.com/satyamsipah/ledger-core/internal/config"
	"github.com/satyamsipah/ledger-core/internal/db"
	ledgerhttp "github.com/satyamsipah/ledger-core/internal/http"
	"github.com/satyamsipah/ledger-core/internal/observability"
)

const serviceName = "api"

func main() {
	if err := run(); err != nil {
		// Config or logger construction can fail before a logger exists, so
		// this one line is stderr rather than structured output.
		fmt.Fprintf(os.Stderr, "fatal: %v\n", err)
		os.Exit(1)
	}
}

func run() error {
	cfg, err := config.Load(serviceName)
	if err != nil {
		return err
	}

	logger := observability.NewLogger(cfg.Observability, serviceName, cfg.Env)
	logger.Info("starting", slog.String("env", cfg.Env))

	// NotifyContext rather than a signal channel: every component below takes a
	// context already, so shutdown is one cancellation rather than a fan-out of
	// bespoke stop channels.
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	shutdownTracing, err := observability.NewTracerProvider(ctx, cfg.Observability, serviceName, cfg.Env, logger)
	if err != nil {
		return fmt.Errorf("init tracing: %w", err)
	}
	defer func() {
		if err := shutdownTracing(context.Background()); err != nil {
			logger.Error("flush traces", slog.String("error", err.Error()))
		}
	}()

	pool, err := db.NewPool(ctx, cfg.Postgres, logger)
	if err != nil {
		return fmt.Errorf("init postgres: %w", err)
	}
	defer pool.Close()

	metrics := observability.NewMetrics(serviceName)

	router := ledgerhttp.NewRouter(ledgerhttp.Deps{
		Service:  serviceName,
		Logger:   logger,
		Metrics:  metrics,
		Checkers: []ledgerhttp.Checker{pool},
	})

	apiServer := ledgerhttp.NewServer("api", cfg.HTTP, router, logger)

	// Metrics listen on their own port so the scrape endpoint is never exposed
	// through whatever fronts the public API.
	metricsMux := nethttp.NewServeMux()
	metricsMux.Handle("/metrics", metrics.Handler())
	metricsCfg := cfg.HTTP
	metricsCfg.Addr = cfg.Observability.MetricsAddr
	metricsServer := ledgerhttp.NewServer("metrics", metricsCfg, metricsMux, logger)

	group, groupCtx := errgroup.WithContext(ctx)
	group.Go(func() error { return apiServer.Run(groupCtx) })
	group.Go(func() error { return metricsServer.Run(groupCtx) })

	if err := group.Wait(); err != nil && !errors.Is(err, context.Canceled) {
		return err
	}

	logger.Info("stopped cleanly")
	return nil
}
