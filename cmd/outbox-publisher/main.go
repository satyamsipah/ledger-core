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
	nethttp "net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

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

	var publisher publish.Publisher

	switch cfg.Outbox.Publisher {
	case "polling":
		pool, err := db.NewPool(ctx, cfg.Postgres, logger)
		if err != nil {
			return fmt.Errorf("init postgres: %w", err)
		}
		defer pool.Close()
		checkers = append(checkers, pool)

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

	if err := group.Wait(); err != nil && !errors.Is(err, context.Canceled) {
		return err
	}

	logger.Info("stopped cleanly")
	return nil
}
