// Command mock-gateway is a stand-in for the external payment gateway the
// payout saga calls.
//
// It is a real HTTP server rather than an in-process stub because
// .claude/rules/testing.md requires failure tests to kill things rather than
// simulate failure with a flag, and names the gateway specifically. A real
// listener can be paused, killed, made slow, and made to stop answering
// altogether -- and the orchestrator that talks to it carries no test-only
// branch at all.
//
// It holds payments in memory, deliberately. Killing it loses them, which is
// the only faithful way to produce the worst case in the whole design: a
// gateway that cannot tell you what it did.
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
	"github.com/satyamsipah/ledger-core/internal/gateway/mock"
	ledgerhttp "github.com/satyamsipah/ledger-core/internal/http"
	"github.com/satyamsipah/ledger-core/internal/observability"
)

const serviceName = "mock-gateway"

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
	logger.Warn("this is a MOCK payment gateway and must never run outside local development")

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	metrics := observability.NewMetrics(serviceName)
	server := mock.New(logger)

	mux := nethttp.NewServeMux()
	mux.Handle("/metrics", metrics.Handler())
	mux.Handle("/", server.Handler())

	gatewayServer := ledgerhttp.NewServer("gateway", cfg.HTTP, mux, logger)

	group, groupCtx := errgroup.WithContext(ctx)
	group.Go(func() error { return gatewayServer.Run(groupCtx) })

	if err := group.Wait(); err != nil && !errors.Is(err, context.Canceled) {
		return err
	}

	logger.Info("stopped cleanly")
	return nil
}
