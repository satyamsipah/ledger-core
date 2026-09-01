// Command projector consumes ledger events from Kafka and maintains the
// read-side balance projection.
//
// The projection it builds is deliberately not the balance the write path
// enforces against. account_balances is authoritative and updated synchronously
// inside the posting transaction; this consumer builds an independent view from
// the event stream. Two independently derived numbers that must agree is what
// gives the reconciliation engine something real to check.
//
// Run with -rebuild to skip Kafka entirely and instead recompute every
// account's balance directly from journal_entries, diffing the result against
// the live projection and exiting non-zero on any disagreement -- a
// standalone check of the whole pipeline (outbox write, publish, consume,
// apply), not merely of this process's own bookkeeping.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	nethttp "net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/google/uuid"
	"github.com/twmb/franz-go/pkg/kadm"
	"golang.org/x/sync/errgroup"

	"github.com/satyamsipah/ledger-core/internal/config"
	"github.com/satyamsipah/ledger-core/internal/db"
	ledgerhttp "github.com/satyamsipah/ledger-core/internal/http"
	"github.com/satyamsipah/ledger-core/internal/observability"
	"github.com/satyamsipah/ledger-core/internal/projector"
)

const serviceName = "projector"

func main() {
	rebuild := flag.Bool("rebuild", false, "recompute balances from journal_entries and diff against the live projection, then exit")
	accountsFlag := flag.String("accounts", "", "comma-separated account UUIDs to scope -rebuild to; empty means every account")
	flag.Parse()

	var err error
	if *rebuild {
		err = runRebuild(*accountsFlag)
	} else {
		err = run()
	}
	if err != nil {
		fmt.Fprintf(os.Stderr, "fatal: %v\n", err)
		os.Exit(1)
	}
}

// runRebuild is a one-shot: connect, compare, report, exit. It does not touch
// Kafka at all, on purpose -- see projector.Rebuild's doc comment for why
// comparing against journal_entries directly, rather than against anything
// this process itself produced, is what makes the check meaningful.
func runRebuild(accountsFlag string) error {
	cfg, err := config.Load(serviceName)
	if err != nil {
		return err
	}

	accountIDs, err := parseAccountIDs(accountsFlag)
	if err != nil {
		return err
	}

	logger := observability.NewLogger(cfg.Observability, serviceName, cfg.Env)

	ctx, cancel := context.WithTimeout(context.Background(), cfg.Postgres.QueryTimeout*10)
	defer cancel()

	pool, err := db.NewPool(ctx, cfg.Postgres, logger)
	if err != nil {
		return fmt.Errorf("init postgres: %w", err)
	}
	defer pool.Close()

	result, err := projector.Rebuild(ctx, pool.Pool, accountIDs...)
	if err != nil {
		return fmt.Errorf("rebuild: %w", err)
	}

	fmt.Printf("compared %d accounts\n", result.AccountsCompared)
	if result.OK() {
		fmt.Println("OK: the live projection matches journal_entries exactly")
		return nil
	}

	fmt.Printf("MISMATCH: %d account(s) disagree\n", len(result.Mismatches))
	for _, m := range result.Mismatches {
		fmt.Printf("  account %s (%s): rebuilt=%d live=%d (projection version %d)\n",
			m.AccountID, m.Currency, m.RebuiltAvailable, m.LiveAvailable, m.LiveVersion)
	}
	return fmt.Errorf("%d account(s) disagree between the live projection and journal_entries", len(result.Mismatches))
}

// parseAccountIDs splits -accounts into UUIDs, or returns nil for "every
// account" when the flag was not given.
func parseAccountIDs(raw string) ([]uuid.UUID, error) {
	if raw == "" {
		return nil, nil
	}
	parts := strings.Split(raw, ",")
	ids := make([]uuid.UUID, len(parts))
	for i, p := range parts {
		id, err := uuid.Parse(strings.TrimSpace(p))
		if err != nil {
			return nil, fmt.Errorf("-accounts: %q is not a UUID: %w", p, err)
		}
		ids[i] = id
	}
	return ids, nil
}

func run() error {
	cfg, err := config.Load(serviceName)
	if err != nil {
		return err
	}

	logger := observability.NewLogger(cfg.Observability, serviceName, cfg.Env)
	logger.Info("starting",
		slog.String("env", cfg.Env),
		slog.Any("brokers", cfg.Kafka.Brokers),
		slog.String("consumer_group", cfg.Kafka.ConsumerGroup))

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

	applier := projector.NewApplier(pool.Pool)
	consumer, err := projector.NewConsumer(cfg.Kafka.Brokers, cfg.Kafka.ConsumerGroup, applier, logger, metrics)
	if err != nil {
		return fmt.Errorf("init consumer: %w", err)
	}
	defer consumer.Close()

	admin := kadm.NewClient(consumer.Client())
	defer admin.Close()
	lagReporter := projector.NewLagReporter(admin, cfg.Kafka.ConsumerGroup, 0, logger, metrics)

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
	group.Go(func() error { return consumer.Run(groupCtx) })
	group.Go(func() error { return lagReporter.Run(groupCtx) })

	if err := group.Wait(); err != nil && !errors.Is(err, context.Canceled) {
		return err
	}

	logger.Info("stopped cleanly")
	return nil
}
