# Ledger-Core

A production-grade, event-driven double-entry ledger — the accounting core that
sits underneath a payments platform.

Correctness under concurrency is the point of this system, so the invariants
that matter are enforced by PostgreSQL rather than by application code alone.
Application checks describe what the code intends; the database constraints are
what stay true when a migration, an admin session, or a service nobody has
written yet does something unanticipated.

## Status

**Phase 2 complete: the ledger core.** Transactions post and reverse, balances
move under row locks, and every write emits an outbox event. There is no HTTP
layer yet — `LedgerService` is the boundary, and it is exercised directly by the
tests.

Phase 1 (skeleton, schema, local environment) is unchanged and still the
foundation: `docker compose up` brings the whole stack up from scratch.

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
internal/ledger     double-entry domain: Money, entries, LedgerService
internal/ledger/pgledger  the PostgreSQL repository: locking and SQL
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

## Posting

`LedgerService.PostTransaction` does all of its work in one database
transaction, in this order:

1. **Lock** every account the transaction touches, in one statement, ordered by
   account id. `FOR UPDATE` on `account_balances`, `FOR NO KEY UPDATE` on
   `accounts`.
2. **Validate** against the locked state: every account exists, is `ACTIVE`, and
   holds the entry's currency.
3. **Insert** the header and all journal entries.
4. **Move** each balance by one aggregated delta, guarded by its expected
   version, refusing any move that would overdraw a restricted account.
5. **Append** the outbox event, so it commits with the journal it describes.
6. **Commit**, where the deferred trigger has the last word.

### Isolation and locking

The write path runs at **READ COMMITTED** and prevents lost updates with
explicit row locks rather than with a stronger isolation level. Every invariant
here is per-row or per-transaction, so there is no write skew for `SERIALIZABLE`
to catch, and `REPEATABLE READ` would convert contention on hot accounts into
retry storms where a row lock converts it into a queue. Full reasoning, and what
the trade-off obliges every write path to do, is in
[DECISIONS.md D10](docs/DECISIONS.md).

Locks are always acquired in ascending account-id order, which is what makes
deadlock unreachable rather than merely rare. Replacing that ordered statement
with the obvious per-account version makes
`TestPostTransaction_ConcurrentOppositeTransfersDoNotDeadlock` fail with real
`40P01` errors within seconds.

### Two sign conventions

A transaction balances under `DEBIT = +, CREDIT = −`, summed per currency. An
account's *balance* is signed differently: positive when an entry's direction
matches that account's normal balance. Without the second convention, a funded
customer wallet — a `CREDIT`-normal `LIABILITY` — would store a negative
balance and trip the overdraft `CHECK`. The two coincide on `DEBIT`-normal
accounts, which is why the tests use wallets. See the block comment in
[types.go](internal/ledger/types.go).

### Money

`Money` is `int64` minor units plus an ISO-4217 code, with no float constructor
and no float accessor. `Add`, `Sub` and `Neg` return an error on currency
mismatch and on int64 overflow — including `Neg(math.MinInt64)`, which has no
positive counterpart and would otherwise silently keep its sign.

It crosses the wire as `{"amount":"1250","currency":"INR","scale":2}`. The
amount is a **string** because the dashboard is TypeScript, where every JSON
number is a float64 and amounts past 2^53 lose precision silently. Decoding
rejects a JSON number outright, and rejects a `scale` that contradicts the
currency — `1250` at scale 0 is a hundred times `1250` at scale 2, and there is
no way to tell them apart after the fact.

### Reversal

`ReverseTransaction` writes a **new** transaction with every leg's direction
mirrored, and touches the original's `status` column and nothing else. It can
legitimately fail with `ErrInsufficientFunds`: undoing a transfer moves money
back out of the receiving account, which may have spent it.

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

Phase 2 adds, against the same real PostgreSQL:

- `TestPostTransaction_UnderConcurrency` — 200 goroutines moving money between
  five accounts. Final balances must equal, to the paisa, both the sum of the
  committed transfers and what the journal independently says, and the five
  accounts together must still hold exactly what they were funded with
- `TestPostTransaction_ConcurrentOppositeTransfersDoNotDeadlock` — 120 writers
  transferring in opposite directions; zero deadlocks permitted
- `TestPostTransaction_OverdraftUnderConcurrency` — ten goroutines withdrawing
  from an account that can fund four of them; exactly four may succeed
- `TestPostTransaction_RandomTransactionsStayBalanced` — 10,000 randomly shaped
  transactions, after which the global signed sum is still exactly zero and
  every stored balance still agrees with the journal (~20s; `-short` skips it)
- `TestReverseTransaction_ConcurrentReversals` — twenty simultaneous reversals
  of one transaction; exactly one may commit, because two would each balance
  perfectly and refund the money twice
- `TestGetStatement_RunsTheBalanceForward` — running balances stay continuous
  across keyset pages, and a full statement closes on the account's real balance
- `Money` arithmetic across the int64 corners, and `signedAmount` over all four
  direction/normal-balance combinations

Coverage across `internal/ledger` and `internal/outbox` is 85.5%.

## Documentation

- [CLAUDE.md](CLAUDE.md) — invariants, stack, and working agreements
- [docs/DECISIONS.md](docs/DECISIONS.md) — every significant decision, what was
  rejected, and why
