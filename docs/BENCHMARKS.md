# Load test benchmarks

Generated 2026-09-03T07:17:35+05:30 by `make loadtest` (cmd/loadtest-harness). Reproduce with:

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
| baseline_simple_transfer | 7249 | 85.3 | 3.20 | 8.55 | 22.56 | 0.000% | PASS | PASS |
| hot_account | 7235 | 85.1 | 3.38 | 12.82 | 319.99 | 0.000% | PASS | PASS |
| idempotent_retry_storm | 7249 | 85.3 | 3.35 | 11.01 | 94.98 | 0.000% | PASS | PASS |
| saga_heavy | 8092 | 76.5 | 2.03 | 7.26 | 16.76 | 0.000% | PASS | PASS |
| mixed_realistic | 10998 | 95.6 | 3.52 | 17.03 | 194.71 | 0.000% | PASS | PASS |

## baseline_simple_transfer

Two fixed accounts, moderate steady-state rate. Every other scenario is read against this one.

- Duration: 86.1s, 7249 requests, 85.3 req/s
- Latency: p50=3.20ms p95=8.55ms p99=22.56ms
- Error rate: 0.000%
- Serialization retries (`ledger_db_tx_retries_total` increase over the run): 0
- Outbox lag at end of run: 41.09s (Debezium arm: replication-slot confirmation lag, bounded below by Kafka Connect's `offset.flush.interval.ms` -- not a queue depth; see cmd/loadtest-harness/prometheus.go's doc comment)
- Consumer lag at end of run: 0
- `api` container: CPU avg 12.3% / max 23.2%, memory avg 30.1MB / max 32.7MB
- `postgres` container: CPU avg 23.4% / max 61.4%, memory avg 140.9MB / max 170.6MB
- Thresholds: PASS
- Correctness-under-load proof (global invariant, projection drift, orphans, async-pipeline rebuild, all against this run's own data): PASS

## hot_account

Same rate and shape as baseline_simple_transfer; 90% of traffic credits one account instead of spreading out, isolating row-lock contention's cost (D11).

- Duration: 85.9s, 7235 requests, 85.1 req/s
- Latency: p50=3.38ms p95=12.82ms p99=319.99ms
- Error rate: 0.000%
- Serialization retries (`ledger_db_tx_retries_total` increase over the run): 0
- Outbox lag at end of run: 20.95s (Debezium arm: replication-slot confirmation lag, bounded below by Kafka Connect's `offset.flush.interval.ms` -- not a queue depth; see cmd/loadtest-harness/prometheus.go's doc comment)
- Consumer lag at end of run: 0
- `api` container: CPU avg 14.4% / max 61.5%, memory avg 33.7MB / max 35.5MB
- `postgres` container: CPU avg 28.8% / max 208.1%, memory avg 212.0MB / max 241.5MB
- Thresholds: PASS
- Correctness-under-load proof (global invariant, projection drift, orphans, async-pipeline rebuild, all against this run's own data): PASS

## idempotent_retry_storm

30% of requests replay an exact earlier (key, body) pair from the same VU, exercising the idempotency read path (D20) instead of PostTransaction.

- Duration: 85.9s, 7249 requests, 85.3 req/s
- Latency: p50=3.35ms p95=11.01ms p99=94.98ms
- Error rate: 0.000%
- Serialization retries (`ledger_db_tx_retries_total` increase over the run): 0
- Outbox lag at end of run: 1.14s (Debezium arm: replication-slot confirmation lag, bounded below by Kafka Connect's `offset.flush.interval.ms` -- not a queue depth; see cmd/loadtest-harness/prometheus.go's doc comment)
- Consumer lag at end of run: 0
- `api` container: CPU avg 14.6% / max 70.3%, memory avg 34.9MB / max 36.6MB
- `postgres` container: CPU avg 25.0% / max 163.6%, memory avg 257.1MB / max 273.9MB
- Thresholds: PASS
- Correctness-under-load proof (global invariant, projection drift, orphans, async-pipeline rebuild, all against this run's own data): PASS

## saga_heavy

Full RESERVE/GATEWAY/SETTLE marketplace payouts against a gateway failing 5% of calls ambiguously; each iteration polls the saga to a settled state rather than trusting the 202.

- Duration: 106.7s, 8092 requests, 76.5 req/s
- Latency: p50=2.03ms p95=7.26ms p99=16.76ms
- Error rate: 0.000%
- Serialization retries (`ledger_db_tx_retries_total` increase over the run): 0
- Outbox lag at end of run: 2.22s (Debezium arm: replication-slot confirmation lag, bounded below by Kafka Connect's `offset.flush.interval.ms` -- not a queue depth; see cmd/loadtest-harness/prometheus.go's doc comment)
- Consumer lag at end of run: 0
- `api` container: CPU avg 8.4% / max 35.8%, memory avg 37.7MB / max 40.1MB
- `postgres` container: CPU avg 18.6% / max 47.5%, memory avg 317.8MB / max 336.9MB
- Thresholds: PASS
- Correctness-under-load proof (global invariant, projection drift, orphans, async-pipeline rebuild, all against this run's own data): PASS

## mixed_realistic

A weighted concurrent blend of the other four scenarios (60% transfers, 20% hot-account, 15% idempotent retries, 5% payouts) via k6's own multi-scenario executor.

- Duration: 116.5s, 10998 requests, 95.6 req/s
- Latency: p50=3.52ms p95=17.03ms p99=194.71ms
- Error rate: 0.000%
- Serialization retries (`ledger_db_tx_retries_total` increase over the run): 0
- Outbox lag at end of run: 20.16s (Debezium arm: replication-slot confirmation lag, bounded below by Kafka Connect's `offset.flush.interval.ms` -- not a queue depth; see cmd/loadtest-harness/prometheus.go's doc comment)
- Consumer lag at end of run: 0
- `api` container: CPU avg 13.6% / max 73.8%, memory avg 30.9MB / max 41.0MB
- `postgres` container: CPU avg 29.5% / max 180.5%, memory avg 350.7MB / max 355.2MB
- Thresholds: PASS
- Correctness-under-load proof (global invariant, projection drift, orphans, async-pipeline rebuild, all against this run's own data): PASS

