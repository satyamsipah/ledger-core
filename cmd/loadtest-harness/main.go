// Command loadtest-harness runs this repo's k6 scenarios against a running
// stack and turns the result into the numbers docs/BENCHMARKS.md and the
// README's benchmarks table are built from.
//
// It does not start the stack itself -- `make loadtest` brings the compose
// stack up fresh and seeds it before invoking this binary, reusing the same
// `make up` / `make seed` targets a developer would run by hand, rather than
// this binary re-implementing compose lifecycle management in Go. What this
// binary owns is the part that actually needed a real program rather than
// shell: running k6, querying Prometheus, sampling `docker stats`, running
// the correctness proof, and rendering the report. See docs/DECISIONS.md's
// Phase 7 entry for the trade-off this split was chosen over (a pure bash
// harness, or a Go program that also drives compose).
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"time"

	"github.com/satyamsipah/ledger-core/internal/auth/pgauth"
	"github.com/satyamsipah/ledger-core/internal/config"
	"github.com/satyamsipah/ledger-core/internal/db"
	"github.com/satyamsipah/ledger-core/internal/observability"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "fatal: %v\n", err)
		os.Exit(1)
	}
}

func run() error {
	scenarioFlag := flag.String("scenario", "all", `scenario to run, or "all"`)
	baseURL := flag.String("base-url", "http://localhost:8080", "the api's base URL")
	postgresDSN := flag.String("postgres-dsn", "postgres://ledger:ledger@localhost:5432/ledger?sslmode=disable", "Postgres DSN, reached from the host running this binary")
	prometheusURL := flag.String("prometheus-url", "http://localhost:9099", "Prometheus's host-mapped URL")
	composeFile := flag.String("compose-file", "deploy/docker-compose.yml", "path to the compose file, for resolving container names and reading the api container's own environment")
	k6Bin := flag.String("k6-bin", "k6", "the k6 binary to invoke")
	principal := flag.String("principal", "loadtest", "the principal this harness authenticates load-test traffic as (internal/auth/pgauth.Issue)")
	resultsDir := flag.String("results-dir", "docs", "directory to write benchmarks.json and BENCHMARKS.md into")
	drainTimeout := flag.Duration("drain-timeout", 120*time.Second, "how long to wait for the outbox backlog and consumer lag to reach zero (twice, across a gap longer than Prometheus's own scrape interval) before running the correctness proof")
	statsInterval := flag.Duration("stats-interval", 2*time.Second, "docker stats sampling interval")
	flag.Parse()

	var toRun []scenario
	if *scenarioFlag == "all" {
		toRun = scenarios
	} else {
		s, ok := findScenario(*scenarioFlag)
		if !ok {
			return fmt.Errorf("unknown scenario %q", *scenarioFlag)
		}
		toRun = []scenario{s}
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	logger := observability.NewLogger(config.Observability{LogLevel: slog.LevelInfo}, "loadtest-harness", "local")

	pool, err := db.NewPool(ctx, postgresConfig(*postgresDSN), logger)
	if err != nil {
		return fmt.Errorf("connect to postgres at %s: %w", *postgresDSN, err)
	}
	defer pool.Close()

	authStore := pgauth.New(pool.Pool, 10*time.Second)
	apiKey, err := authStore.Issue(ctx, *principal)
	if err != nil {
		return fmt.Errorf("issue api key for principal %s: %w", *principal, err)
	}
	logger.Info("minted load-test api key", slog.String("principal", *principal))

	containerNames, err := resolveContainerNames(ctx, *composeFile, []string{"api", "postgres"})
	if err != nil {
		return fmt.Errorf("resolve container names: %w", err)
	}

	summaryDir, err := os.MkdirTemp("", "loadtest-harness-*")
	if err != nil {
		return fmt.Errorf("create temp dir for k6 summaries: %w", err)
	}
	defer func() { _ = os.RemoveAll(summaryDir) }()

	var results []scenarioResult
	anyFailed := false

	for _, s := range toRun {
		logger.Info("running scenario", slog.String("scenario", s.Name))

		sampler := startStatsSampler(ctx, containerNames, *statsInterval)
		startedAt := time.Now()

		summaryPath := filepath.Join(summaryDir, s.Name+".json")
		summary, thresholdsPassed, runErr := runK6(ctx, *k6Bin, s, map[string]string{
			"BASE_URL": *baseURL,
			"API_KEY":  apiKey,
		}, summaryPath)

		duration := time.Since(startedAt)
		resources := sampler.stop()

		if runErr != nil {
			return fmt.Errorf("run scenario %s: %w", s.Name, runErr)
		}
		if !thresholdsPassed {
			logger.Warn("scenario thresholds failed", slog.String("scenario", s.Name))
			anyFailed = true
		}

		logger.Info("waiting for the async pipeline to drain before proving correctness",
			slog.String("scenario", s.Name))
		if err := waitForPipelineDrained(ctx, *prometheusURL, *drainTimeout); err != nil {
			return fmt.Errorf("scenario %s: %w", s.Name, err)
		}

		retries, err := promInstantQuery(ctx, *prometheusURL,
			fmt.Sprintf(`sum(increase(ledger_db_tx_retries_total{job="api"}[%ds]))`, int(duration.Seconds())+5))
		if err != nil {
			return fmt.Errorf("query serialization retries for %s: %w", s.Name, err)
		}
		outboxLag, err := promInstantQuery(ctx, *prometheusURL, `max(ledger_outbox_lag_seconds{job="outbox-publisher"})`)
		if err != nil {
			return fmt.Errorf("query outbox lag for %s: %w", s.Name, err)
		}
		consumerLag, err := promInstantQuery(ctx, *prometheusURL, `sum(ledger_projector_consumer_lag{job="projector"})`)
		if err != nil {
			return fmt.Errorf("query consumer lag for %s: %w", s.Name, err)
		}

		correctness, err := runCorrectnessProof(ctx, pool.Pool)
		if err != nil {
			return fmt.Errorf("correctness proof for %s: %w", s.Name, err)
		}
		if !correctness.OK {
			logger.Error("CORRECTNESS PROOF FAILED", slog.String("scenario", s.Name))
			anyFailed = true
		}

		result := scenarioResult{
			Name:                  s.Name,
			Description:           s.Description,
			StartedAt:             startedAt,
			DurationSec:           duration.Seconds(),
			TotalRequests:         int64(metricOrZero(summary, "http_reqs", "count")),
			ThroughputRPS:         metricOrZero(summary, "http_reqs", "rate"),
			P50Ms:                 latencyMetric(summary, s.EndpointTag, "p(50)"),
			P95Ms:                 latencyMetric(summary, s.EndpointTag, "p(95)"),
			P99Ms:                 latencyMetric(summary, s.EndpointTag, "p(99)"),
			ErrorRatePct:          metricOrZero(summary, "http_req_failed", "value") * 100,
			ThresholdsPassed:      thresholdsPassed,
			SerializationRetries:  retries,
			OutboxLagSecondsAtEnd: outboxLag,
			ConsumerLagAtEnd:      consumerLag,
			APIResources:          resources["api"],
			PostgresResources:     resources["postgres"],
			Correctness:           correctness,
		}
		results = append(results, result)

		logger.Info("scenario complete",
			slog.String("scenario", s.Name),
			slog.Int64("requests", result.TotalRequests),
			slog.Float64("throughput_rps", result.ThroughputRPS),
			slog.Float64("p99_ms", result.P99Ms),
			slog.Bool("ok", result.overallOK()))
	}

	envInfo, err := gatherEnvironmentInfo(ctx, *composeFile, pool.Pool)
	if err != nil {
		return fmt.Errorf("gather environment info: %w", err)
	}

	report := runReport{
		GeneratedAt: time.Now(),
		Environment: envInfo,
		Scenarios:   results,
	}

	if err := os.MkdirAll(*resultsDir, 0o750); err != nil {
		return fmt.Errorf("create results dir %s: %w", *resultsDir, err)
	}
	jsonPath := filepath.Join(*resultsDir, "benchmarks.json")
	mdPath := filepath.Join(*resultsDir, "BENCHMARKS.md")

	if err := writeJSONReport(jsonPath, report); err != nil {
		return err
	}
	if err := renderMarkdown(mdPath, report); err != nil {
		return err
	}
	logger.Info("report written", slog.String("json", jsonPath), slog.String("markdown", mdPath))

	if anyFailed {
		return errors.New("one or more scenarios failed thresholds or the correctness proof; see the report")
	}
	return nil
}

// postgresConfig builds the minimal config.Postgres this binary's own pool
// needs. It is not read from the environment the way every real service's
// pool is (internal/config.Load) because this process's job is to reach the
// stack from the HOST, not to BE one of the stack's own services -- its DSN
// points at the host-mapped port, never the compose-internal one.
func postgresConfig(dsn string) config.Postgres {
	return config.Postgres{
		DSN:             dsn,
		MaxConns:        5,
		MinConns:        1,
		MaxConnLifetime: time.Hour,
		MaxConnIdleTime: 10 * time.Minute,
		ConnectTimeout:  10 * time.Second,
		QueryTimeout:    30 * time.Second,
	}
}

func metricOrZero(summary k6Summary, metric, field string) float64 {
	v, ok := summary.Metrics[metric]
	if !ok {
		return 0
	}
	switch field {
	case "count":
		return v.f(v.Count)
	case "rate":
		return v.f(v.Rate)
	case "value":
		return v.f(v.Value)
	default:
		return 0
	}
}

// latencyMetric reads a percentile off the scenario's own tagged
// http_req_duration submetric (e.g. "http_req_duration{endpoint:post_transaction}"),
// falling back to the untagged metric if the scenario's script never tagged
// its requests.
func latencyMetric(summary k6Summary, endpointTag, percentile string) float64 {
	key := "http_req_duration"
	if endpointTag != "" {
		key = fmt.Sprintf(`http_req_duration{endpoint:%s}`, endpointTag)
	}
	v, ok := summary.Metrics[key]
	if !ok {
		v, ok = summary.Metrics["http_req_duration"]
		if !ok {
			return 0
		}
	}
	switch percentile {
	case "p(50)":
		return v.f(v.P50)
	case "p(95)":
		return v.f(v.P95)
	case "p(99)":
		return v.f(v.P99)
	default:
		return 0
	}
}
