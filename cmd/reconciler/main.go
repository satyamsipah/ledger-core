// Command reconciler proves the ledger's invariants against the data at rest,
// on a schedule, rather than trusting that the write path held them.
//
// Its first job once the projector exists is comparing three independently
// derived numbers per account: the synchronous balance in account_balances, the
// event-sourced projection, and a full aggregate of journal_entries. Any two
// agreeing while the third dissents localises the bug immediately.
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
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/satyamsipah/ledger-core/internal/config"
	"github.com/satyamsipah/ledger-core/internal/db"
	ledgerhttp "github.com/satyamsipah/ledger-core/internal/http"
	"github.com/satyamsipah/ledger-core/internal/observability"
	"github.com/satyamsipah/ledger-core/internal/reconciliation"
	"github.com/satyamsipah/ledger-core/internal/reconciliation/pgreconciliation"
)

const serviceName = "reconciler"

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
	logger.Info("starting", slog.String("env", cfg.Env))

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

	mux := nethttp.NewServeMux()
	mux.Handle("/metrics", metrics.Handler())
	mux.Handle("/", ledgerhttp.NewRouter(ledgerhttp.Deps{
		Service:  serviceName,
		Logger:   logger,
		Metrics:  metrics,
		Checkers: []ledgerhttp.Checker{pool},
	}))

	adminCfg := cfg.HTTP
	adminCfg.Addr = cfg.Observability.MetricsAddr
	adminServer := ledgerhttp.NewServer("admin", adminCfg, mux, logger)

	group, groupCtx := errgroup.WithContext(ctx)
	group.Go(func() error { return adminServer.Run(groupCtx) })

	if cfg.Reconciler.PSPStatementPath == "" {
		logger.Warn("reconciliation disabled: no LEDGER_RECONCILER_PSP_CSV_PATH configured")
	} else {
		store := pgreconciliation.New(pool.Pool, cfg.Postgres.QueryTimeout)
		engine := reconciliation.NewEngine(store, logger, metrics, cfg.Reconciler.TimingWindow, cfg.Reconciler.Lookback)
		group.Go(func() error {
			runReconciliationLoop(groupCtx, engine, cfg.Reconciler.PSPStatementPath, cfg.Reconciler.Interval, logger)
			return nil
		})
	}

	if err := group.Wait(); err != nil && !errors.Is(err, context.Canceled) {
		return err
	}

	logger.Info("stopped cleanly")
	return nil
}

// runReconciliationLoop runs one reconciliation immediately, then on
// interval, until ctx is cancelled. Never returns an error: a failed run is
// logged and the loop keeps going, the same resilience
// internal/idempotency.Sweeper gives its own tick failures -- a reconciler
// that crashed the whole process because one day's statement was malformed
// would turn a data-quality problem into an availability one.
//
// The file is re-read and re-parsed on every tick rather than once at
// startup, because this is meant to run against wherever a real settlement
// file actually lands: re-reading is what lets a fresh statement dropped at
// the same path be picked up without a restart.
func runReconciliationLoop(ctx context.Context, engine *reconciliation.Engine, path string, interval time.Duration, logger *slog.Logger) {
	logger.Info("reconciliation loop started",
		slog.String("psp_statement_path", path), slog.Duration("interval", interval))

	runOnce := func() {
		f, err := os.Open(path)
		if err != nil {
			logger.ErrorContext(ctx, "open PSP statement", slog.String("path", path), slog.String("error", err.Error()))
			return
		}
		defer f.Close()

		records, err := reconciliation.ParsePSPStatement(f)
		if err != nil {
			logger.ErrorContext(ctx, "parse PSP statement", slog.String("path", path), slog.String("error", err.Error()))
			return
		}

		if _, err := engine.Run(ctx, path, records); err != nil {
			logger.ErrorContext(ctx, "reconciliation run", slog.String("path", path), slog.String("error", err.Error()))
		}
	}

	runOnce()

	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			logger.Info("reconciliation loop stopped")
			return
		case <-ticker.C:
			runOnce()
		}
	}
}
