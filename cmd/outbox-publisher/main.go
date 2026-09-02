// Command outbox-publisher carries committed outbox rows to Kafka.
//
// Which mechanism actually does that is LEDGER_OUTBOX_PUBLISHER: "polling"
// runs a SELECT ... FOR UPDATE SKIP LOCKED loop in this process and produces
// with a Kafka client directly; "debezium" (the default) runs no such loop at
// all, because Kafka Connect's Debezium connector is already reading the
// write-ahead log independently, and this process instead reports the
// connector's health. See docs/DECISIONS.md D31 for why Debezium is the
// default and what each arm costs.
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math"
	nethttp "net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/twmb/franz-go/pkg/kgo"
	"golang.org/x/sync/errgroup"

	"github.com/satyamsipah/ledger-core/internal/config"
	"github.com/satyamsipah/ledger-core/internal/db"
	ledgerhttp "github.com/satyamsipah/ledger-core/internal/http"
	"github.com/satyamsipah/ledger-core/internal/observability"
	"github.com/satyamsipah/ledger-core/internal/outbox/publish"
	"github.com/satyamsipah/ledger-core/internal/outbox/publish/debezium"
	"github.com/satyamsipah/ledger-core/internal/outbox/publish/polling"
)

const serviceName = "outbox-publisher"

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
	logger.Info("starting", slog.String("env", cfg.Env), slog.String("publisher", cfg.Outbox.Publisher))

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

	metrics := observability.NewMetrics(serviceName)
	checkers := []ledgerhttp.Checker{}

	// Opened unconditionally, regardless of which publisher arm is selected
	// below. Backlog and lag are properties of the outbox TABLE, not of
	// whichever mechanism happens to be draining it (D31) -- the Debezium arm
	// otherwise never queries Postgres directly at all, and without this it
	// would run with no visibility into the one number its own connector
	// status cannot report: how far behind the table itself actually is.
	pool, err := db.NewPool(ctx, cfg.Postgres, logger)
	if err != nil {
		return fmt.Errorf("init postgres: %w", err)
	}
	defer pool.Close()
	checkers = append(checkers, pool)

	var publisher publish.Publisher

	switch cfg.Outbox.Publisher {
	case "polling":
		client, err := kgo.NewClient(
			kgo.SeedBrokers(cfg.Kafka.Brokers...),
			// A record that can never be acknowledged -- a broker down for
			// good, not merely for a retry or two -- must eventually fail
			// rather than block ProduceSync, and therefore this process,
			// forever. 30s is comfortably above any transient blip this
			// stack sees in practice (a broker restart, a leader election)
			// and short enough that a genuine outage surfaces as the error
			// path drainOnce already handles, not as a process that stopped
			// responding to its own liveness probe.
			kgo.RecordDeliveryTimeout(30*time.Second),
		)
		if err != nil {
			return fmt.Errorf("init kafka client: %w", err)
		}
		defer client.Close()

		publisher = polling.New(pool.Pool, client, logger, metrics, polling.Config{
			Interval:  cfg.Outbox.PollInterval,
			BatchSize: cfg.Outbox.BatchSize,
		})

	case "debezium":
		monitor := debezium.New(debezium.Config{
			ConnectURL:    cfg.Outbox.ConnectURL,
			ConnectorName: cfg.Outbox.ConnectorName,
		}, logger)
		checkers = append(checkers, monitor)
		publisher = monitor

	default:
		// config.Load already validates this; reaching here is a bug in that
		// validation, not a runtime condition an operator can hit.
		return fmt.Errorf("unknown outbox publisher %q", cfg.Outbox.Publisher)
	}

	mux := nethttp.NewServeMux()
	mux.Handle("/metrics", metrics.Handler())
	mux.Handle("/", ledgerhttp.NewRouter(ledgerhttp.Deps{
		Service:  serviceName,
		Logger:   logger,
		Metrics:  metrics,
		Checkers: checkers,
	}))

	adminCfg := cfg.HTTP
	adminCfg.Addr = cfg.Observability.MetricsAddr
	adminServer := ledgerhttp.NewServer("admin", adminCfg, mux, logger)

	group, groupCtx := errgroup.WithContext(ctx)
	group.Go(func() error { return adminServer.Run(groupCtx) })
	group.Go(func() error { return publisher.Run(groupCtx) })
	group.Go(func() error {
		runBacklogMonitor(groupCtx, pool.Pool, metrics, cfg.Outbox, logger)
		return nil
	})

	if err := group.Wait(); err != nil && !errors.Is(err, context.Canceled) {
		return err
	}

	logger.Info("stopped cleanly")
	return nil
}

// runBacklogMonitor ticks the query that feeds ledger_outbox_backlog and
// ledger_outbox_lag_seconds -- but WHICH query depends entirely on which
// publisher arm is configured, because the two arms leave genuinely
// different evidence of their own progress.
//
// THIS WAS WRONG IN AN EARLIER VERSION OF THIS FUNCTION, discovered only by
// actually running `docker compose up` and posting real transactions through
// it -- exactly the class of defect D35 already named this Definition of
// Done exists to catch. The polling arm's own published_at UPDATE is real
// evidence: outbox.published_at IS NULL genuinely means "not yet produced."
// The Debezium arm NEVER writes that column at all (D4, D46) -- it reads the
// write-ahead log directly and tracks its own position in the replication
// slot, not in this table. Querying published_at under Debezium does not
// find a smaller number when Debezium is behind; it finds the same
// ever-growing count REGARDLESS of whether Debezium has published
// everything a second ago or hasn't run in a week, because every row this
// arm ever produces stays NULL forever. D46 already says the fix in as many
// words: "backlog must be judged from the topic, not from that column" --
// this function is what finally does that instead of quietly ignoring it.
//
// Runs regardless of which publisher arm is selected, following
// runReconciliationLoop and runConsistencyLoop's own shape in cmd/reconciler:
// one query failing is logged and never stops the loop, because a transient
// database hiccup on one tick should not silence monitoring for the next one.
func runBacklogMonitor(ctx context.Context, pool *pgxpool.Pool, metrics *observability.Metrics, cfg config.Outbox, logger *slog.Logger) {
	logger.Info("outbox backlog monitor started",
		slog.Duration("interval", cfg.BacklogCheckInterval), slog.String("publisher", cfg.Publisher))

	checkOnce := func() {
		if cfg.Publisher == "polling" {
			checkPollingBacklog(ctx, pool, metrics, logger)
		} else {
			checkDebeziumLag(ctx, pool, metrics, cfg.ReplicationSlotName, logger)
		}
	}

	checkOnce()

	ticker := time.NewTicker(cfg.BacklogCheckInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			logger.Info("outbox backlog monitor stopped")
			return
		case <-ticker.C:
			checkOnce()
		}
	}
}

// checkPollingBacklog is exactly right for the polling arm: published_at is
// this arm's own bookkeeping, written inside the same transaction as the
// Kafka produce it describes (internal/outbox/publish/polling), so
// "IS NULL" genuinely means "not yet produced."
func checkPollingBacklog(ctx context.Context, pool *pgxpool.Pool, metrics *observability.Metrics, logger *slog.Logger) {
	var backlog int64
	var lagSeconds float64
	err := pool.QueryRow(ctx, `
		SELECT count(*), COALESCE(EXTRACT(EPOCH FROM now() - min(created_at)), 0)
		  FROM outbox
		 WHERE published_at IS NULL`).Scan(&backlog, &lagSeconds)
	if err != nil {
		logger.ErrorContext(ctx, "check outbox backlog (polling)", slog.String("error", err.Error()))
		return
	}

	metrics.OutboxBacklog.Set(float64(backlog))
	metrics.OutboxLagSeconds.Set(lagSeconds)
}

// checkDebeziumLag watches what Debezium actually leaves evidence in:
// PostgreSQL's own pg_stat_replication.replay_lag for the connector's
// logical replication connection, joined to the slot by pid. This is a
// TIME duration Postgres itself computes -- the honest, already-correct
// answer to "how far behind is this consumer", not a byte count this
// function would have to guess a conversion factor for.
//
// ledger_outbox_backlog is not set here at all -- there is no row-count
// signal available without asking Kafka how far the topic has actually
// advanced, which is a heavier integration than this monitor is for. Left
// at its zero value, which is honestly what it is: not measured under this
// arm.
//
// A slot that exists but has NO active replication connection
// (active_pid IS NULL) is worse than merely "behind" -- Debezium is not
// connected to it AT ALL, which is exactly the abandoned-slot failure mode
// D31 warns fills disk without limit. That state is reported as +Inf
// seconds of lag, deliberately: it is the honest value ("no upper bound on
// how long this has been true") and it is what makes
// ledger_outbox_lag_seconds > 30 correctly fire for the single worst case
// this metric exists to catch, rather than silently reporting nothing.
func checkDebeziumLag(ctx context.Context, pool *pgxpool.Pool, metrics *observability.Metrics, slotName string, logger *slog.Logger) {
	var connected bool
	var lagSeconds float64
	err := pool.QueryRow(ctx, `
		SELECT s.active_pid IS NOT NULL, COALESCE(EXTRACT(EPOCH FROM r.replay_lag), 0)
		  FROM pg_replication_slots s
		  LEFT JOIN pg_stat_replication r ON r.pid = s.active_pid
		 WHERE s.slot_name = $1`, slotName).Scan(&connected, &lagSeconds)
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		// The slot itself does not exist -- migration 000008 not applied, or
		// dropped by hand. Same honest answer as "not connected": unbounded.
		logger.ErrorContext(ctx, "check outbox lag (debezium): replication slot does not exist",
			slog.String("slot", slotName))
		metrics.OutboxLagSeconds.Set(math.Inf(1))
		return
	case err != nil:
		logger.ErrorContext(ctx, "check outbox lag (debezium)", slog.String("error", err.Error()))
		return
	}

	if !connected {
		logger.ErrorContext(ctx, "check outbox lag (debezium): replication slot has no active connection",
			slog.String("slot", slotName))
		metrics.OutboxLagSeconds.Set(math.Inf(1))
		return
	}

	metrics.OutboxLagSeconds.Set(lagSeconds)
}
