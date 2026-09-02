// Command reconciler proves the ledger's invariants against the data at rest,
// on a schedule, rather than trusting that the write path held them.
//
// Two independent jobs run in this process, on independent tickers:
//
//   - The internal structural checks (internal/consistency) always run, need
//     no configuration, and compare independently derived numbers against
//     each other: the global invariant across the whole journal, the
//     synchronous balance in account_balances against a full recomputation
//     from journal_entries, and two structural shape checks. Any two
//     agreeing while a third dissents localises the bug immediately.
//   - The PSP three-way match (internal/reconciliation) runs only when
//     LEDGER_RECONCILER_PSP_CSV_PATH is configured, comparing the ledger's
//     own transactions against a saga and an external settlement file.
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

	"github.com/jackc/pgx/v5/pgxpool"
	"golang.org/x/sync/errgroup"

	"github.com/satyamsipah/ledger-core/internal/config"
	"github.com/satyamsipah/ledger-core/internal/consistency"
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

	// Unlike the PSP match above, the internal structural checks need no
	// external file and no operator configuration to be worth running -- "is
	// our own data internally consistent" is not something that should wait
	// on a settlement file ever being pointed at this process.
	group.Go(func() error {
		runConsistencyLoop(groupCtx, pool.Pool, metrics, cfg.Reconciler.ConsistencyInterval, logger)
		return nil
	})

	if err := group.Wait(); err != nil && !errors.Is(err, context.Canceled) {
		return err
	}

	logger.Info("stopped cleanly")
	return nil
}

// runConsistencyLoop runs the three internal structural checks
// (internal/consistency) on interval, until ctx is cancelled. Like
// runReconciliationLoop, a single failing check is logged and never stops the
// loop: a database hiccup on one tick should not silence the next one, and
// least of all should it silence the global invariant check, which is the one
// this whole loop exists to keep running.
func runConsistencyLoop(ctx context.Context, pool *pgxpool.Pool, metrics *observability.Metrics, interval time.Duration, logger *slog.Logger) {
	logger.Info("consistency check loop started", slog.Duration("interval", interval))

	runOnce := func() {
		checkGlobalInvariant(ctx, pool, metrics, logger)
		checkProjectionDrift(ctx, pool, metrics, logger)
		checkOrphans(ctx, pool, metrics, logger)
	}

	runOnce()

	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			logger.Info("consistency check loop stopped")
			return
		case <-ticker.C:
			runOnce()
		}
	}
}

// checkGlobalInvariant is THE PAGE. The gauge is reset before every check --
// not merely overwritten -- so a currency that stops violating does not leave
// a stale nonzero series on the dashboard forever; Set(0) would look
// identical to "just checked and it's fine," where Reset followed by only
// setting today's actual violations makes "this currency is not even in the
// journal, or is and it balances" and "we checked this and found nothing
// wrong" the same, correct state: absent from the series entirely.
func checkGlobalInvariant(ctx context.Context, pool *pgxpool.Pool, metrics *observability.Metrics, logger *slog.Logger) {
	result, err := consistency.CheckGlobalInvariant(ctx, pool)
	if err != nil {
		logger.ErrorContext(ctx, "check global invariant", slog.String("error", err.Error()))
		return
	}

	metrics.GlobalInvariantViolation.Reset()
	for _, v := range result.Violations {
		metrics.GlobalInvariantViolation.WithLabelValues(v.Currency).Set(float64(v.SignedTotal))
	}

	if !result.OK() {
		logger.ErrorContext(ctx, "GLOBAL INVARIANT VIOLATED: journal_entries does not sum to zero",
			slog.Any("violations", result.Violations))
	}
}

func checkProjectionDrift(ctx context.Context, pool *pgxpool.Pool, metrics *observability.Metrics, logger *slog.Logger) {
	result, err := consistency.CheckProjectionDrift(ctx, pool)
	if err != nil {
		logger.ErrorContext(ctx, "check projection drift", slog.String("error", err.Error()))
		return
	}

	metrics.ProjectionDriftAccounts.Set(float64(result.TotalDrifted))

	if !result.OK() {
		logger.ErrorContext(ctx, "projection drift: account_balances disagrees with the journal",
			slog.Int("accounts_compared", result.AccountsCompared),
			slog.Int("total_drifted", result.TotalDrifted),
			slog.Any("drifted", result.Drifted))
	}
}

func checkOrphans(ctx context.Context, pool *pgxpool.Pool, metrics *observability.Metrics, logger *slog.Logger) {
	result, err := consistency.CheckOrphans(ctx, pool)
	if err != nil {
		logger.ErrorContext(ctx, "check orphans", slog.String("error", err.Error()))
		return
	}

	metrics.OrphanTransactions.Set(float64(result.TotalFewEntry))
	metrics.OrphanEntries.Set(float64(result.TotalOrphanEntries))

	if !result.OK() {
		logger.ErrorContext(ctx, "orphan check failed: transactions or entries have an impossible shape",
			slog.Int("total_few_entry_transactions", result.TotalFewEntry),
			slog.Any("few_entry_transactions", result.FewEntryTransactions),
			slog.Int("total_orphan_entries", result.TotalOrphanEntries),
			slog.Any("orphan_entries", result.OrphanEntries))
	}
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
