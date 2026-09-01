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
	"github.com/satyamsipah/ledger-core/internal/idempotency"
	"github.com/satyamsipah/ledger-core/internal/idempotency/pgidem"
	"github.com/satyamsipah/ledger-core/internal/ledger"
	"github.com/satyamsipah/ledger-core/internal/ledger/pgledger"
	"github.com/satyamsipah/ledger-core/internal/observability"
	"github.com/satyamsipah/ledger-core/internal/saga/payout"
	"github.com/satyamsipah/ledger-core/internal/saga/pgsaga"
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

	// The retrier re-runs only 40001 and 40P01, the two aborts PostgreSQL
	// guarantees rolled back nothing. Advisory locking is a process-wide switch
	// rather than a per-request one, because a second lock space entered by only
	// some write paths would replace one global lock ordering with two.
	retrier := db.NewRetrier(logger, metrics,
		cfg.Ledger.MaxTxAttempts, cfg.Ledger.RetryBaseBackoff, cfg.Ledger.RetryMaxBackoff)

	repository := pgledger.New(pool.Pool, cfg.Postgres.QueryTimeout,
		pgledger.WithRetrier(retrier),
		pgledger.WithAdvisoryLocks(cfg.Ledger.AdvisoryLocks))

	ledgerService := ledger.NewService(repository)

	// NoopCache: Redis is deliberately absent. Correctness never depended on it,
	// and the cache hit-rate counter is what will decide whether the dependency
	// is worth taking on. See docs/DECISIONS.md D22.
	idempotencyManager := idempotency.NewManager(
		pgidem.New(pool.Pool, cfg.Postgres.QueryTimeout),
		idempotency.NoopCache{},
		metrics,
		cfg.Ledger.IdempotencyTTL,
		cfg.Ledger.IdempotencyLease,
	)

	sweeper := idempotency.NewSweeper(
		pgidem.New(pool.Pool, cfg.Postgres.QueryTimeout),
		logger, metrics,
		cfg.Ledger.SweepInterval, cfg.Ledger.SweepBatch,
	)

	// The API starts sagas and reads them; it never drives one. Driving is
	// cmd/saga-orchestrator's job, because a step advanced inside a request
	// would tie a customer's money to an HTTP connection that can be cut at
	// any moment -- manufacturing exactly the ambiguity the saga design works
	// to avoid. The gateway client is therefore not wired here at all: this
	// process has no reason to hold one.
	sagaStore := pgsaga.New(pool.Pool, cfg.Postgres.QueryTimeout)
	payoutStarter := payout.New(sagaStore, ledgerService, nil, logger, metrics, payout.Config{
		WorkerID:    cfg.Saga.WorkerID,
		StepTimeout: cfg.Saga.StepTimeout,
	})

	router := ledgerhttp.NewRouter(ledgerhttp.Deps{
		Service:          serviceName,
		Logger:           logger,
		Metrics:          metrics,
		Checkers:         []ledgerhttp.Checker{pool},
		Ledger:           ledgerService,
		Idempotency:      idempotencyManager,
		Payout:           payoutStarter,
		Sagas:            sagaStore,
		TrustedProxyHops: cfg.HTTP.TrustedProxyHops,
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
	// Every replica sweeps. There is no leader election on purpose: the delete
	// uses FOR UPDATE SKIP LOCKED, so replicas divide the work rather than
	// queue behind each other, and no rolling deploy has a window with nobody
	// sweeping. See internal/idempotency/sweeper.go.
	group.Go(func() error { return sweeper.Run(groupCtx) })

	if err := group.Wait(); err != nil && !errors.Is(err, context.Canceled) {
		return err
	}

	logger.Info("stopped cleanly")
	return nil
}
