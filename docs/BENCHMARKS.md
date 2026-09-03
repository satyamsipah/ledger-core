# Load test benchmarks

Generated 2026-09-03T06:31:09+05:30 by `make loadtest` (cmd/loadtest-harness). Reproduce with:

```
make loadtest
```

> **Single-node local/dev environment (Docker Compose on one developer machine), not a production cluster. Absolute numbers describe this machine; do not quote them as production capacity.**

> Latency percentiles measured on a single developer machine vary noticeably run to run (observed p99 swinging roughly 2-4x across otherwise-identical runs while this harness was built) -- background processes, thermal throttling, and the Docker Desktop VM's own scheduling all add noise a dedicated benchmarking host would not have. Throughput and error rate are stable; p99 specifically should be read as an order of magnitude, not a precise figure.

## Environment

| | |
|---|---|
| OS / arch | darwin / arm64 |
| CPUs visible to the harness process (runtime.NumCPU) | 8 |
| PostgreSQL | PostgreSQL 16.15 on aarch64-unknown-linux-musl, compiled by gcc (Alpine 15.2.0) 15.2.0, 64-bit |
| `shared_buffers` | 128MB |
| `max_connections` | 100 |
| `wal_level` | logical |
| `LEDGER_POSTGRES_MAX_CONNS` (api) | 20 (internal/config default; not overridden) |
| `LEDGER_POSTGRES_MIN_CONNS` (api) | 2 (internal/config default; not overridden) |

## Summary

| Scenario | Requests | Throughput (req/s) | p50 (ms) | p95 (ms) | p99 (ms) | Error rate | Thresholds | Correctness |
|---|---|---|---|---|---|---|---|---|
| baseline_simple_transfer | 7248 | 85.3 | 3.34 | 11.84 | 260.65 | 0.000% | PASS | PASS |

## baseline_simple_transfer

Two fixed accounts, moderate steady-state rate. Every other scenario is read against this one.

- Duration: 86.2s, 7248 requests, 85.3 req/s
- Latency: p50=3.34ms p95=11.84ms p99=260.65ms
- Error rate: 0.000%
- Serialization retries (`ledger_db_tx_retries_total` increase over the run): 0
- Outbox lag at end of run: 31.31s (Debezium arm: replication-slot confirmation lag, bounded below by Kafka Connect's `offset.flush.interval.ms` -- not a queue depth; see cmd/loadtest-harness/prometheus.go's doc comment)
- Consumer lag at end of run: 0
- `api` container: CPU avg 12.4% / max 35.9%, memory avg 31.4MB / max 35.1MB
- `postgres` container: CPU avg 23.6% / max 91.3%, memory avg 155.2MB / max 197.9MB
- Thresholds: PASS
- Correctness-under-load proof (global invariant, projection drift, orphans, async-pipeline rebuild, all against this run's own data): PASS

