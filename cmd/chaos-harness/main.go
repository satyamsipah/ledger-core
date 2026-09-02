// Command chaos-harness injects the six faults Phase 6 asks for -- DB
// connection failure, Kafka unavailability, gateway timeout, gateway 500,
// slow query, clock skew -- through real mechanisms rather than a boolean
// flag standing in for one, per .claude/rules/testing.md's own rule for
// failure tests: "they actually kill things... they do not simulate
// failure with a boolean flag." That rule is written for tests, but the
// same reasoning applies with more force to a tool tests are built on: a
// harness that only pretends to break something proves nothing about
// whether the system survives the real version.
//
// Every fault here is one of exactly two kinds:
//
//   - A real thing actually happening: docker pause/unpause on the postgres
//     or redpanda container (docker.go's dockerClient, over the Docker
//     Engine API's own Unix socket -- no SDK dependency, the API is three
//     HTTP verbs), or a real competing transaction holding a real row lock
//     for the slow-query fault.
//   - A control call into a mechanism THIS CODEBASE ALREADY BUILT for
//     exactly this purpose: mock-gateway's /control/behaviour (D45) for the
//     gateway faults, and internal/http.HandleClockSkew (gated behind
//     LEDGER_FAULT_INJECTION_ENABLED, mounted only on the admin listener --
//     see that handler's own doc comment) for clock skew.
//
// This binary needs the Docker socket, which is a root-equivalent
// capability on the host it runs on. It is built from its own Dockerfile
// (deploy/Dockerfile.chaos-harness), not the shared one every real service
// uses, and is started only by explicitly layering
// deploy/docker-compose.chaos.yml over the base compose file --
// `docker compose -f docker-compose.yml -f docker-compose.chaos.yml up`, or
// `make chaos-up` -- never plain `up`. See docs/DECISIONS.md D51 for the
// full reasoning, including why clock skew has exactly two legitimate
// targets and not, say, the reconciler.
package main

import (
	"context"
	"fmt"
	"log/slog"
	nethttp "net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "fatal: %v\n", err)
		os.Exit(1)
	}
}

// config is deliberately its own small struct rather than a reuse of
// internal/config.Load: that function validates and defaults a full
// ledger-service shape (a Postgres pool sized for production load, Kafka
// broker lists, a saga configuration) that this tool has no use for. A
// chaos harness has six knobs, not sixty.
type config struct {
	addr string

	postgresDSN       string
	postgresContainer string
	redpandaContainer string
	hotAccountRef     string

	mockGatewayURL      string
	apiAdminURL         string
	sagaOrchestratorURL string
}

func loadConfig() config {
	return config{
		addr:                envOr("CHAOS_ADDR", ":9199"),
		postgresDSN:         envOr("CHAOS_POSTGRES_DSN", "postgres://ledger:ledger@postgres:5432/ledger?sslmode=disable"),
		postgresContainer:   envOr("CHAOS_POSTGRES_CONTAINER", "ledger-postgres"),
		redpandaContainer:   envOr("CHAOS_REDPANDA_CONTAINER", "ledger-redpanda"),
		hotAccountRef:       envOr("CHAOS_HOT_ACCOUNT_REF", "platform-bank-inr"),
		mockGatewayURL:      envOr("CHAOS_MOCK_GATEWAY_URL", "http://mock-gateway:8090"),
		apiAdminURL:         envOr("CHAOS_API_ADMIN_URL", "http://api:9090"),
		sagaOrchestratorURL: envOr("CHAOS_SAGA_ORCHESTRATOR_URL", "http://saga-orchestrator:9094"),
	}
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func run() error {
	cfg := loadConfig()

	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil)).With(slog.String("service", "chaos-harness"))
	logger.Warn("starting -- this binary holds Docker-socket access and must never run outside the chaos compose profile")

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// A small pool, held for the life of this process, used only by the
	// slow-query fault -- one connection actually holding the lock, plus
	// headroom. This is not a service serving real traffic.
	pool, err := pgxpool.New(ctx, cfg.postgresDSN)
	if err != nil {
		return fmt.Errorf("init postgres pool: %w", err)
	}
	defer pool.Close()

	h := &harness{
		cfg:    cfg,
		pool:   pool,
		logger: logger,
		// A generous client timeout for the mock-gateway/admin proxy calls
		// this makes -- those are the fast, instant control calls (D45's
		// /control/behaviour, HandleClockSkew), never the long time.Sleep
		// each handler does itself. The maxFaultDuration-bounded sleep is
		// plain Go code, not an HTTP call, so this timeout never bounds it.
		httpClient: &nethttp.Client{Timeout: 10 * time.Second},
		docker:     newDockerClient(),
	}

	r := chi.NewRouter()
	r.Get("/healthz", func(w nethttp.ResponseWriter, _ *nethttp.Request) { w.WriteHeader(nethttp.StatusOK) })
	r.Post("/faults/db-down", h.handleDBDown)
	r.Post("/faults/kafka-down", h.handleKafkaDown)
	r.Post("/faults/slow-query", h.handleSlowQuery)
	r.Post("/faults/gateway-timeout", h.handleGatewayTimeout)
	r.Post("/faults/gateway-500", h.handleGateway500)
	r.Post("/faults/clock-skew", h.handleClockSkew)

	server := &nethttp.Server{
		Addr:              cfg.addr,
		Handler:           r,
		ReadHeaderTimeout: 5 * time.Second,
	}

	go func() {
		<-ctx.Done()
		// Deliberately built from context.Background(), matching
		// internal/http/server.go's identical shutdown: ctx is already
		// cancelled by the time we get here, so deriving the shutdown
		// deadline from it would abort the drain instantly.
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		//nolint:contextcheck // see comment above; shutdownCtx must not inherit the already-cancelled ctx
		_ = server.Shutdown(shutdownCtx)
	}()

	logger.Info("listening", slog.String("addr", cfg.addr))
	if err := server.ListenAndServe(); err != nil && err != nethttp.ErrServerClosed {
		return fmt.Errorf("serve: %w", err)
	}

	logger.Info("stopped cleanly")
	return nil
}
