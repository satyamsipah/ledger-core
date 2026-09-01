// Command saga-orchestrator drives multi-step money movements that cannot be a
// single database transaction.
//
// It runs two loops. The claim loop advances sagas that are ready to move; the
// sweeper finds sagas whose step deadline has passed and either retries them,
// probes an unresolved gateway call, or compensates. Both claim work through
// FOR UPDATE SKIP LOCKED, so any number of replicas may run concurrently with
// no leader election -- the same competing-consumers shape the polling outbox
// publisher uses.
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
	"github.com/satyamsipah/ledger-core/internal/gateway"
	ledgerhttp "github.com/satyamsipah/ledger-core/internal/http"
	"github.com/satyamsipah/ledger-core/internal/ledger"
	"github.com/satyamsipah/ledger-core/internal/ledger/pgledger"
	"github.com/satyamsipah/ledger-core/internal/observability"
	"github.com/satyamsipah/ledger-core/internal/saga/payout"
	"github.com/satyamsipah/ledger-core/internal/saga/pgsaga"
)

const serviceName = "saga-orchestrator"

func main() {
	if err := run(); err != nil {
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
	logger.Info("starting",
		slog.String("env", cfg.Env),
		slog.String("worker_id", cfg.Saga.WorkerID))

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

	retrier := db.NewRetrier(logger, metrics, cfg.Ledger.MaxTxAttempts,
		cfg.Ledger.RetryBaseBackoff, cfg.Ledger.RetryMaxBackoff)
	repository := pgledger.New(pool.Pool, cfg.Postgres.QueryTimeout,
		pgledger.WithRetrier(retrier),
		pgledger.WithAdvisoryLocks(cfg.Ledger.AdvisoryLocks))

	orchestrator := payout.New(
		pgsaga.New(pool.Pool, cfg.Postgres.QueryTimeout),
		ledger.NewService(repository),
		gateway.NewHTTPClient(cfg.Gateway.URL, cfg.Gateway.Timeout, cfg.Gateway.ProbeTimeout),
		logger,
		metrics,
		payout.Config{
			WorkerID:                cfg.Saga.WorkerID,
			ClaimInterval:           cfg.Saga.ClaimInterval,
			ClaimBatch:              cfg.Saga.ClaimBatch,
			Lease:                   cfg.Saga.Lease,
			StepTimeout:             cfg.Saga.StepTimeout,
			MaxStepAttempts:         cfg.Saga.MaxStepAttempts,
			MaxCompensationAttempts: cfg.Saga.MaxCompensationAttempts,
			SweepInterval:           cfg.Saga.SweepInterval,
			MaxProbes:               cfg.Gateway.MaxProbes,
		})

	mux := nethttp.NewServeMux()
	mux.Handle("/metrics", metrics.Handler())
	mux.Handle("/", ledgerhttp.NewRouter(ledgerhttp.Deps{
		Service:  serviceName,
		Logger:   logger,
		Metrics:  metrics,
		Checkers: []ledgerhttp.Checker{pool},
	}))

	// One listener, on the metrics address, serving health and metrics
	// together. This process has no public API, so there is no second port to
	// bind -- and D35 records what happens when a compose file assumes there is.
	adminCfg := cfg.HTTP
	adminCfg.Addr = cfg.Observability.MetricsAddr
	adminServer := ledgerhttp.NewServer("admin", adminCfg, mux, logger)

	group, groupCtx := errgroup.WithContext(ctx)
	group.Go(func() error { return adminServer.Run(groupCtx) })
	group.Go(func() error { return orchestrator.Run(groupCtx) })
	group.Go(func() error { return orchestrator.Sweep(groupCtx) })

	if err := group.Wait(); err != nil && !errors.Is(err, context.Canceled) {
		return err
	}

	logger.Info("stopped cleanly")
	return nil
}
