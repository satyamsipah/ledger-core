# Load test benchmarks

Generated 2026-09-03T06:55:09+05:30 by `make loadtest` (cmd/loadtest-harness). Reproduce with:

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
| mixed_realistic | 11320 | 98.4 | 3.37 | 11.11 | 52.53 | 0.000% | PASS | PASS |

## mixed_realistic

A weighted concurrent blend of the other four scenarios (60% transfers, 20% hot-account, 15% idempotent retries, 5% payouts) via k6's own multi-scenario executor.

- Duration: 115.7s, 11320 requests, 98.4 req/s
- Latency: p50=3.37ms p95=11.11ms p99=52.53ms
- Error rate: 0.000%
- Serialization retries (`ledger_db_tx_retries_total` increase over the run): 0
- Outbox lag at end of run: 32.12s (Debezium arm: replication-slot confirmation lag, bounded below by Kafka Connect's `offset.flush.interval.ms` -- not a queue depth; see cmd/loadtest-harness/prometheus.go's doc comment)
- Consumer lag at end of run: 0
- `api` container: CPU avg 11.0% / max 30.9%, memory avg 25.6MB / max 30.0MB
- `postgres` container: CPU avg 22.6% / max 101.9%, memory avg 346.7MB / max 358.0MB
- Thresholds: PASS
- Correctness-under-load proof (global invariant, projection drift, orphans, async-pipeline rebuild, all against this run's own data): PASS

