# Ledger-Core

A production-grade, event-driven double-entry ledger — the accounting core that
sits underneath a payments platform.

Correctness under concurrency is the point of this system, so the invariants
that matter are enforced by PostgreSQL rather than by application code alone.
Application checks describe what the code intends; the database constraints are
what stay true when a migration, an admin session, or a service nobody has
written yet does something unanticipated.

## Status

**Phase 1 complete: skeleton, schema, and local environment.** No posting logic
yet — the schema, its invariants, and the tests that prove them are in place,
and `docker compose up` brings the whole stack up from scratch.

## Quick start

Requires Docker and Go 1.25+.

```bash
make up      # postgres + redpanda + kafka-connect + redis + api, migrations applied
make seed    # load the development chart of accounts
```

| Endpoint | URL |
|---|---|
| API liveness | http://localhost:8080/healthz |
| API readiness | http://localhost:8080/readyz |
| Metrics | http://localhost:9090/metrics |
| Kafka Connect | http://localhost:8083/connectors |

```bash
make test-race   # full suite under the race detector (starts Testcontainers)
make down        # tear down, including volumes
make help        # every target
```

## Layout

```
cmd/api             public HTTP surface
cmd/projector       Kafka consumer maintaining the read-side balance projection
cmd/reconciler      scheduled invariant checks against data at rest
internal/ledger     double-entry domain: accounts, transactions, entries
internal/idempotency  request de-duplication (invariant 5)
internal/outbox     transactional outbox (invariant 6)
internal/saga       multi-step orchestration with compensating transactions
internal/http       router, middleware, health, server lifecycle
internal/db         pgx pool and query-timeout conventions
internal/config     environment configuration
internal/observability  slog, Prometheus, OpenTelemetry
migrations/         golang-migrate SQL, up and down
test/               integration tests against real PostgreSQL
deploy/             Docker Compose stack, Dockerfile, Debezium connector, seed
```

## The schema

Six tables: `accounts`, `transactions`, `journal_entries`, `account_balances`,
`idempotency_keys`, `outbox`.

### How each invariant is enforced

| # | Invariant | Mechanism |
|---|---|---|
| 1 | Transactions balance | `CONSTRAINT TRIGGER ... DEFERRABLE INITIALLY DEFERRED` on `journal_entries`, grouped by `(transaction_id, currency)` |
| 2 | Journal is append-only | `BEFORE UPDATE OR DELETE` row trigger, plus a `BEFORE TRUNCATE` statement trigger |
| 3 | Integer money only | `amount_minor BIGINT CHECK (> 0)`; sign lives in `direction` |
| 4 | No negative balances | `CHECK (allow_negative OR available_minor >= 0)` on `account_balances`, plus `SELECT … FOR UPDATE` on the write path |
| 5 | Idempotent writes | Primary key on `idempotency_keys.key`, plus a partial unique index on `transactions.idempotency_key` |
| 6 | No dual writes | `outbox` table written in the same transaction as the journal; Debezium publishes from the WAL |

### Why the balance trigger is deferred

Entries are inserted one row at a time, so after the first `INSERT` a
transaction is unbalanced by necessity. An ordinary trigger fires at statement
time and would reject that first row every time, making the invariant
unenforceable rather than merely awkward.

The invariant is a property of the database transaction, not of a row or a
statement, so it is checked at the only moment where it is meaningful: `COMMIT`.
Full reasoning is in
[000005_balance_invariant.up.sql](migrations/000005_balance_invariant.up.sql).

## Tests

No mocks for database behaviour — every test runs against real PostgreSQL via
Testcontainers, because the bugs that matter here live in the database.

- `TestBalanceInvariant_DeferredUntilCommit` — inserts must succeed for
  unbalanced sets and the `COMMIT` must fail, including per-currency cases
- `TestBalanceInvariant_UnderConcurrency` — 120 concurrent writers, half of them
  deliberately one paisa out; all balanced ones commit, all unbalanced ones are
  rejected and leave nothing behind
- `TestJournalEntries_AppendOnly` — `UPDATE`, `DELETE`, and `TRUNCATE` rejected
- `TestMigrations_RoundTrip` — up, all the way down, up again, then one step at a
  time, asserting no table, function, or publication is left behind
- plus the overdraft CHECK, the currency composite FK, idempotency-key
  uniqueness, and the `posted_at`/`status` constraint

## Documentation

- [CLAUDE.md](CLAUDE.md) — invariants, stack, and working agreements
- [docs/DECISIONS.md](docs/DECISIONS.md) — every significant decision, what was
  rejected, and why
