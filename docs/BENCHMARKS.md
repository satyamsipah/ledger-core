# Load test benchmarks

Generated 2026-09-03T07:57:38+05:30 by `make loadtest` (cmd/loadtest-harness). Reproduce with:

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
| `LEDGER_POSTGRES_MAX_CONNS` (api) | 20 |
| `LEDGER_POSTGRES_MIN_CONNS` (api) | 2 |

## Summary

| Scenario | Requests | Throughput (req/s) | p50 (ms) | p95 (ms) | p99 (ms) | Error rate | Thresholds | Correctness |
|---|---|---|---|---|---|---|---|---|
| baseline_simple_transfer | 7249 | 85.3 | 3.38 | 12.99 | 38.75 | 0.000% | PASS | PASS |
| hot_account | 7249 | 85.3 | 3.47 | 18.68 | 167.39 | 0.000% | PASS | PASS |
| idempotent_retry_storm | 7249 | 85.3 | 3.35 | 10.87 | 46.42 | 0.000% | PASS | PASS |
| saga_heavy | 8035 | 76.8 | 1.83 | 6.14 | 12.57 | 0.000% | PASS | PASS |
| mixed_realistic | 10507 | 91.3 | 3.84 | 29.52 | 168.73 | 0.000% | PASS | PASS |

## baseline_simple_transfer

Two fixed accounts, moderate steady-state rate. Every other scenario is read against this one.

- Duration: 86.0s, 7249 requests, 85.3 req/s
- Latency: p50=3.38ms p95=12.99ms p99=38.75ms
- Error rate: 0.000%
- Serialization retries (`ledger_db_tx_retries_total` increase over the run): 0
- Outbox lag at end of run: 41.33s (Debezium arm: replication-slot confirmation lag, bounded below by Kafka Connect's `offset.flush.interval.ms` -- not a queue depth; see cmd/loadtest-harness/prometheus.go's doc comment)
- Consumer lag at end of run: 0
- `api` container: CPU avg 12.9% / max 25.4%, memory avg 30.5MB / max 33.1MB
- `postgres` container: CPU avg 27.9% / max 69.0%, memory avg 140.6MB / max 170.7MB
- Thresholds: PASS
- Correctness-under-load proof (global invariant, projection drift, orphans, async-pipeline rebuild, all against this run's own data): PASS

## hot_account

Same rate and shape as baseline_simple_transfer; 90% of traffic credits one account instead of spreading out, isolating row-lock contention's cost (D11).

- Duration: 86.2s, 7249 requests, 85.3 req/s
- Latency: p50=3.47ms p95=18.68ms p99=167.39ms
- Error rate: 0.000%
- Serialization retries (`ledger_db_tx_retries_total` increase over the run): 0
- Outbox lag at end of run: 19.97s (Debezium arm: replication-slot confirmation lag, bounded below by Kafka Connect's `offset.flush.interval.ms` -- not a queue depth; see cmd/loadtest-harness/prometheus.go's doc comment)
- Consumer lag at end of run: 0
- `api` container: CPU avg 14.1% / max 30.4%, memory avg 34.2MB / max 36.5MB
- `postgres` container: CPU avg 31.2% / max 110.0%, memory avg 215.7MB / max 242.9MB
- Thresholds: PASS
- Correctness-under-load proof (global invariant, projection drift, orphans, async-pipeline rebuild, all against this run's own data): PASS

## idempotent_retry_storm

30% of requests replay an exact earlier (key, body) pair from the same VU, exercising the idempotency read path (D20) instead of PostTransaction.

- Duration: 85.7s, 7249 requests, 85.3 req/s
- Latency: p50=3.35ms p95=10.87ms p99=46.42ms
- Error rate: 0.000%
- Serialization retries (`ledger_db_tx_retries_total` increase over the run): 0
- Outbox lag at end of run: 1.28s (Debezium arm: replication-slot confirmation lag, bounded below by Kafka Connect's `offset.flush.interval.ms` -- not a queue depth; see cmd/loadtest-harness/prometheus.go's doc comment)
- Consumer lag at end of run: 0
- `api` container: CPU avg 12.0% / max 24.2%, memory avg 27.9MB / max 37.8MB
- `postgres` container: CPU avg 23.7% / max 69.7%, memory avg 259.4MB / max 297.6MB
- Thresholds: PASS
- Correctness-under-load proof (global invariant, projection drift, orphans, async-pipeline rebuild, all against this run's own data): PASS

## saga_heavy

Full RESERVE/GATEWAY/SETTLE marketplace payouts against a gateway failing 5% of calls ambiguously; each iteration polls the saga to a settled state rather than trusting the 202.

- Duration: 105.2s, 8035 requests, 76.8 req/s
- Latency: p50=1.83ms p95=6.14ms p99=12.57ms
- Error rate: 0.000%
- Serialization retries (`ledger_db_tx_retries_total` increase over the run): 0
- Outbox lag at end of run: 0.99s (Debezium arm: replication-slot confirmation lag, bounded below by Kafka Connect's `offset.flush.interval.ms` -- not a queue depth; see cmd/loadtest-harness/prometheus.go's doc comment)
- Consumer lag at end of run: 0
- `api` container: CPU avg 7.0% / max 24.6%, memory avg 22.9MB / max 24.2MB
- `postgres` container: CPU avg 17.9% / max 59.6%, memory avg 315.5MB / max 333.1MB
- Thresholds: PASS
- Correctness-under-load proof (global invariant, projection drift, orphans, async-pipeline rebuild, all against this run's own data): PASS

## mixed_realistic

A weighted concurrent blend of the other four scenarios (60% transfers, 20% hot-account, 15% idempotent retries, 5% payouts) via k6's own multi-scenario executor.

- Duration: 116.1s, 10507 requests, 91.3 req/s
- Latency: p50=3.84ms p95=29.52ms p99=168.73ms
- Error rate: 0.000%
- Serialization retries (`ledger_db_tx_retries_total` increase over the run): 0
- Outbox lag at end of run: 12.88s (Debezium arm: replication-slot confirmation lag, bounded below by Kafka Connect's `offset.flush.interval.ms` -- not a queue depth; see cmd/loadtest-harness/prometheus.go's doc comment)
- Consumer lag at end of run: 0
- `api` container: CPU avg 13.6% / max 52.4%, memory avg 25.3MB / max 26.7MB
- `postgres` container: CPU avg 31.2% / max 100.7%, memory avg 352.3MB / max 374.6MB
- Thresholds: PASS
- Correctness-under-load proof (global invariant, projection drift, orphans, async-pipeline rebuild, all against this run's own data): PASS

