package main

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"time"
)

type scenarioResult struct {
	Name        string    `json:"name"`
	Description string    `json:"description"`
	StartedAt   time.Time `json:"started_at"`
	DurationSec float64   `json:"duration_seconds"`

	TotalRequests int64   `json:"total_requests"`
	ThroughputRPS float64 `json:"throughput_rps"`
	P50Ms         float64 `json:"p50_ms"`
	P95Ms         float64 `json:"p95_ms"`
	P99Ms         float64 `json:"p99_ms"`
	ErrorRatePct  float64 `json:"error_rate_percent"`

	ThresholdsPassed bool `json:"thresholds_passed"`

	SerializationRetries  float64 `json:"serialization_retries"`
	OutboxLagSecondsAtEnd float64 `json:"outbox_lag_seconds_at_end"`
	ConsumerLagAtEnd      float64 `json:"consumer_lag_at_end"`

	APIResources      resourceSummary `json:"api_resources"`
	PostgresResources resourceSummary `json:"postgres_resources"`

	Correctness correctnessReport `json:"correctness"`
}

type runReport struct {
	GeneratedAt time.Time        `json:"generated_at"`
	Environment environmentInfo  `json:"environment"`
	Scenarios   []scenarioResult `json:"scenarios"`
}

func (r scenarioResult) overallOK() bool {
	return r.ThresholdsPassed && r.Correctness.OK
}

func writeJSONReport(path string, report runReport) error {
	f, err := os.Create(path)
	if err != nil {
		return fmt.Errorf("create %s: %w", path, err)
	}
	defer func() { _ = f.Close() }()

	enc := json.NewEncoder(f)
	enc.SetIndent("", "  ")
	if err := enc.Encode(report); err != nil {
		return fmt.Errorf("encode %s: %w", path, err)
	}
	return nil
}

// renderMarkdown writes docs/BENCHMARKS.md from scratch on every run. The
// results JSON (writeJSONReport) is the source of truth; this is a rendering
// of it, so there is nothing to merge or append -- a stale row from a
// scenario dropped from a later run would otherwise linger here forever.
func renderMarkdown(path string, report runReport) error {
	var b strings.Builder

	fmt.Fprintf(&b, "# Load test benchmarks\n\n")
	fmt.Fprintf(&b, "Generated %s by `make loadtest` (cmd/loadtest-harness). Reproduce with:\n\n", report.GeneratedAt.Format(time.RFC3339))
	fmt.Fprintf(&b, "```\nmake loadtest\n```\n\n")
	fmt.Fprintf(&b, "> **%s**\n\n", report.Environment.Caveat)
	fmt.Fprintf(&b, "> Latency percentiles measured on a single developer machine vary noticeably "+
		"run to run (observed p99 swinging roughly 2-4x across otherwise-identical runs while this "+
		"harness was built) -- background processes, thermal throttling, and the Docker Desktop VM's "+
		"own scheduling all add noise a dedicated benchmarking host would not have. Throughput and "+
		"error rate are stable; p99 specifically should be read as an order of magnitude, not a precise figure.\n\n")

	fmt.Fprintf(&b, "## Environment\n\n")
	fmt.Fprintf(&b, "| | |\n|---|---|\n")
	fmt.Fprintf(&b, "| OS / arch | %s / %s |\n", report.Environment.OS, report.Environment.Arch)
	fmt.Fprintf(&b, "| CPUs visible to the harness process (runtime.NumCPU) | %d |\n", report.Environment.CPUCount)
	fmt.Fprintf(&b, "| PostgreSQL | %s |\n", report.Environment.PostgresVersion)
	fmt.Fprintf(&b, "| `shared_buffers` | %s |\n", report.Environment.PostgresSharedBuffers)
	fmt.Fprintf(&b, "| `max_connections` | %s |\n", report.Environment.PostgresMaxConns)
	fmt.Fprintf(&b, "| `wal_level` | %s |\n", report.Environment.PostgresWALLevel)
	fmt.Fprintf(&b, "| `LEDGER_POSTGRES_MAX_CONNS` (api) | %s |\n", report.Environment.PoolMaxConns)
	fmt.Fprintf(&b, "| `LEDGER_POSTGRES_MIN_CONNS` (api) | %s |\n\n", report.Environment.PoolMinConns)

	fmt.Fprintf(&b, "## Summary\n\n")
	fmt.Fprintf(&b, "| Scenario | Requests | Throughput (req/s) | p50 (ms) | p95 (ms) | p99 (ms) | Error rate | Thresholds | Correctness |\n")
	fmt.Fprintf(&b, "|---|---|---|---|---|---|---|---|---|\n")
	for _, s := range report.Scenarios {
		fmt.Fprintf(&b, "| %s | %d | %.1f | %.2f | %.2f | %.2f | %.3f%% | %s | %s |\n",
			s.Name, s.TotalRequests, s.ThroughputRPS, s.P50Ms, s.P95Ms, s.P99Ms, s.ErrorRatePct,
			passFail(s.ThresholdsPassed), passFail(s.Correctness.OK))
	}
	fmt.Fprintf(&b, "\n")

	for _, s := range report.Scenarios {
		fmt.Fprintf(&b, "## %s\n\n%s\n\n", s.Name, s.Description)
		fmt.Fprintf(&b, "- Duration: %.1fs, %d requests, %.1f req/s\n", s.DurationSec, s.TotalRequests, s.ThroughputRPS)
		fmt.Fprintf(&b, "- Latency: p50=%.2fms p95=%.2fms p99=%.2fms\n", s.P50Ms, s.P95Ms, s.P99Ms)
		fmt.Fprintf(&b, "- Error rate: %.3f%%\n", s.ErrorRatePct)
		fmt.Fprintf(&b, "- Serialization retries (`ledger_db_tx_retries_total` increase over the run): %.0f\n", s.SerializationRetries)
		fmt.Fprintf(&b, "- Outbox lag at end of run: %.2fs (Debezium arm: replication-slot confirmation lag, bounded below by Kafka Connect's `offset.flush.interval.ms` -- not a queue depth; see cmd/loadtest-harness/prometheus.go's doc comment)\n", s.OutboxLagSecondsAtEnd)
		fmt.Fprintf(&b, "- Consumer lag at end of run: %.0f\n", s.ConsumerLagAtEnd)
		fmt.Fprintf(&b, "- `api` container: CPU avg %.1f%% / max %.1f%%, memory avg %.1fMB / max %.1fMB\n",
			s.APIResources.CPUAvgPercent, s.APIResources.CPUMaxPercent, s.APIResources.MemAvgMB, s.APIResources.MemMaxMB)
		fmt.Fprintf(&b, "- `postgres` container: CPU avg %.1f%% / max %.1f%%, memory avg %.1fMB / max %.1fMB\n",
			s.PostgresResources.CPUAvgPercent, s.PostgresResources.CPUMaxPercent, s.PostgresResources.MemAvgMB, s.PostgresResources.MemMaxMB)
		fmt.Fprintf(&b, "- Thresholds: %s\n", passFail(s.ThresholdsPassed))
		fmt.Fprintf(&b, "- Correctness-under-load proof (global invariant, projection drift, orphans, async-pipeline rebuild, all against this run's own data): %s\n\n",
			passFail(s.Correctness.OK))
	}

	return os.WriteFile(path, []byte(b.String()), 0o644)
}

func passFail(ok bool) string {
	if ok {
		return "PASS"
	}
	return "FAIL"
}
