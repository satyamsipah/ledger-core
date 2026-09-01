# Ledger-Core

A production-grade, event-driven double-entry ledger — the accounting core that
sits underneath a payments platform.

Correctness under concurrency is the point of this system, so the invariants
that matter are enforced by PostgreSQL rather than by application code alone.
Application checks describe what the code intends; the database constraints are
what stay true when a migration, an admin session, or a service nobody has
written yet does something unanticipated.

## Status

**Phase 3 complete: the write path under duplicates and concurrency.** There is
an HTTP API now — `POST /v1/transactions`, `POST /v1/transactions/{id}/reverse`,
`GET /v1/accounts/{id}/balance` and `/statement` — behind required idempotency
keys, a retry wrapper for aborted transactions, and optional sub-account
sharding for hot accounts. See [the API](#the-api) and
[Idempotency](#idempotency).

Phase 2 (the ledger core) and Phase 1 (skeleton, schema, local environment) are
unchanged and still the foundation: `docker compose up` brings the whole stack
up from scratch.

Two gaps are open and worth knowing about before deploying this anywhere real:
the client IP is spoofable (D19), and idempotency keys share one namespace with
no authentication behind them (D24). Both are recorded in
[docs/DECISIONS.md](docs/DECISIONS.md) rather than papered over.

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
| Post a transaction | `POST` http://localhost:8080/v1/transactions |
| Metrics | http://localhost:9090/metrics |
| Kafka Connect | http://localhost:8083/connectors |

```bash
curl -X POST http://localhost:8080/v1/transactions \
  -H "Idempotency-Key: $(uuidgen)" \
  -H "Content-Type: application/json" \
  -d @- <<'JSON'
{
  "type": "TRANSFER",
  "entries": [
    {"account_id": "...", "direction": "DEBIT",  "amount": {"amount": "1250", "currency": "INR"}},
    {"account_id": "...", "direction": "CREDIT", "amount": {"amount": "1250", "currency": "INR"}}
  ]
}
JSON
```

Send it twice with the same key: the second response is byte-identical and
carries `Idempotent-Replay: true`.

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
internal/ledger     double-entry domain: Money, entries, ledger.Service
internal/ledger/pgledger  the PostgreSQL repository: locking and SQL
internal/idempotency  request de-duplication (invariant 5)
internal/idempotency/pgidem  the idempotency_keys statements, including the
                    completion that runs inside the ledger transaction
internal/outbox     transactional outbox (invariant 6)
internal/saga       multi-step orchestration with compensating transactions
internal/http       router, middleware, health, server lifecycle
internal/db         pgx pool, query-timeout conventions, 40001/40P01 retrier
internal/config     environment configuration
internal/observability  slog, Prometheus, OpenTelemetry
migrations/         golang-migrate SQL, up and down
test/               integration tests against real PostgreSQL
deploy/             Docker Compose stack, Dockerfile, Debezium connector, seed
api/openapi.yaml    OpenAPI 3.1, checked against the router by the test suite
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

`ledger.Service.PostTransaction` does all of its work in one database
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

## The API

`api/openapi.yaml` is the specification, and the test suite checks it against
the router in both directions — every registered route must be documented and
every documented path must exist. A specification nobody validates rots, and the
drift is discovered by a client integrating against a path that is gone.

| Method | Path | Idempotency-Key |
|---|---|---|
| `POST` | `/v1/transactions` | required |
| `POST` | `/v1/transactions/{id}/reverse` | required |
| `GET` | `/v1/accounts/{id}/balance` | — |
| `GET` | `/v1/accounts/{id}/statement` | — |
| `GET` | `/healthz`, `/readyz` | — |

Errors are RFC 9457 `application/problem+json`. The `type` URI is the
machine-readable discriminator and the thing to switch on; `title` and `detail`
are prose. `detail` is omitted on 5xx, because a constraint or table name in a
public error body is free reconnaissance.

`GET /v1/accounts/{id}/balance?as_of=<RFC3339>` reconstructs the balance from
the journal at an instant. That answer is bounded-stale by design — entries
carry transaction *start* time — so the endpoint echoes `as_of` back, making it
obvious which question was answered. Statements are keyset-paginated with an
opaque cursor; the position is a `(created_at, id)` pair because timestamps tie,
and a client that could see both fields would eventually build its own and skip
rows.

## Idempotency

The guarantee: the same key with the same body never creates a second
transaction, at any level of concurrency.

| Same key, and… | Result |
|---|---|
| same body, original completed | `201` replay, `Idempotent-Replay: true`, byte-identical |
| same body, original still running | `409` with `Retry-After` |
| **different** body | `422` |
| replay record expired after 24h | `409` — refused, never re-executed |

Two bodies that differ only in whitespace, key order or number formatting are
the same request: fingerprints are SHA-256 over RFC 8785 canonical JSON, plus
the method and route so one key cannot replay across two endpoints. Duplicate
JSON keys are rejected rather than resolved, because parsers disagree about
which wins and a document with no single meaning cannot be fingerprinted.

### The one property everything rests on

```
                      ┌──────────┐
   sweeper (24h) ────▶│  ABSENT  │◀──── release (guarded on IN_PROGRESS)
                      └────┬─────┘                     ▲
      reserve: INSERT … ON CONFLICT DO NOTHING         │
      (its own transaction, commits alone)             │
                           ▼                           │
    reclaim   ┌────────────────────────────┐           │
   (lease  ┌─▶│        IN_PROGRESS         │──▶ 409 + Retry-After
    dead)  └──┤   lease_expires_at = …     │           │
              └──────┬──────────────┬──────┘           │
                     │              │                  │
   journal + balances│              │ deterministic    │ transient
   + outbox + record │              │ rejection        │ rejection
   ALL IN ONE TXN    ▼              ▼                  │
              ┌───────────┐  ┌───────────┐             │
              │ COMPLETED │  │  FAILED   │─────────────┘
              └───────────┘  └───────────┘
```

> A record in `IN_PROGRESS` is **proof that no transaction committed under that
> key** — because the move to `COMPLETED` happens in the same database
> transaction as the journal entries.

That single fact is what lets a stale lease be reclaimed with no fencing token
and no lock service, and it is why a crash can leave a delayed retry but never a
duplicate payment. Writing the record in a *separate* transaction — the textbook
shape — breaks it: the work commits, the completion fails, and the retry
correctly reasons that `IN_PROGRESS` means no commit, and is wrong for the first
time. `pgidem.Complete` takes a `pgx.Tx` rather than a pool so that version
cannot be written.

Three defences stand behind it, and the last owes nothing to the first two being
correct: the primary key of `idempotency_keys`; the `status = 'IN_PROGRESS'`
guard on the completing `UPDATE`, which aborts the loser of a reclaim race and
rolls back its journal entries; and `transactions_idempotency_key_key`.

The 24-hour TTL bounds storage, never correctness. Sweeping removes the replay
record; the key stays reserved forever in `transactions`, so expiry can lose you
a response but never buy you a second transaction.

Redis is specified and not implemented. `Cache` is an interface, `NoopCache` is
the default, and `ledger_idempotency_outcomes_total{outcome="cache_hit"}` is
what will decide whether the dependency is worth taking on (D23).

## Concurrency

Aborted transactions are retried — but only `40001` and `40P01`, the two
SQLSTATEs PostgreSQL guarantees rolled back nothing. Five attempts, full jitter
uniform over `[0, window)`, and the parent context bounds the whole sequence so
five retries cannot consume five times the deadline the HTTP layer granted.

Everything else is excluded for cause. A deadline that expired mid-COMMIT, a
reset connection, or any error from COMMIT itself is **ambiguous** — the
transaction may have committed while the answer was lost, and retrying an
ambiguous write in a ledger is how money moves twice. Domain errors are
deterministic; retrying only makes the caller wait longer for the same no.

`ledger_db_tx_retries_total{sqlstate="40P01"}` is a continuous proof of the
ordered locking in D11: a series that stays at zero says the lock ordering still
holds, which is stronger than any single test. Measured, not assumed — the
hot-account contention test reports **0.0000% retries and zero deadlocks over
500 transactions on one account**.

`LEDGER_LEDGER_ADVISORY_LOCKS=true` adds `pg_advisory_xact_lock` per account
before the row locks. Off by default and honestly second-order: with ordered
locking already in place it moves where writers queue rather than removing the
queue. It is process-wide, never per request, because a second lock space
entered by only some write paths replaces one global ordering with two.

## Sharding hot accounts

```sql
SELECT ledger_shard_account('<account-uuid>', 8);
```

Writes then hash to a random child and the logical balance is the `SUM` over
children. Shards are ordinary rows in `accounts`, so the composite foreign key,
the deferred trigger, the overdraft `CHECK` and the ordered locking all keep
working untouched.

**Read this before enabling it.** The overdraft check is per row, so:

- **Safety survives.** Every shard is individually non-negative, so their sum is.
  You cannot overdraw the logical account without overdrawing a shard first.
  Invariant 4 still holds.
- **Liveness does not.** 800 spread as 100 across eight shards will refuse a
  debit of 500 the account plainly holds.

So shard only accounts whose traffic is effectively one-directional — house
floats, revenue, fee collection. **A drainable customer wallet must not be
sharded.** A sibling-to-sibling rebalancer is the fix and is Phase 4.

Benchmark, 32 writers × 8 posts into one logical account, three runs:

| Arm | Throughput |
|---|---|
| Single account | 371–444 tx/s |
| 16 shards | 1621–1965 tx/s |

**4.4×–4.8×, not 16×.** Past a handful of shards the row lock stops being the
bottleneck and the connection pool, WAL fsync and CPU take over — so 4–8 shards
recovers most of the available gain and the rest is largely wasted. The ratio is
the transferable result; the absolute numbers describe one laptop.

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

Phase 3 adds:

- `TestIdempotency_SameKeyConcurrentlyPostsExactlyOnce` and
  `TestAPI_SameKeyUnderConcurrencyPostsOnce` — 100 goroutines firing one key at
  the service and at the HTTP API; exactly one transaction may exist afterwards,
  and every `201` returned, executed or replayed, must be the identical document
- `TestIdempotency_CrashBeforeCompletionLeavesNoTransaction` — parks a posting
  transaction between its journal insert and its idempotency completion, then
  **terminates the backend with `pg_terminate_backend`**. Asserts the rollback
  left no transaction, a record still `IN_PROGRESS` (the proof no money moved),
  and a retry that succeeds exactly once
- `TestIdempotency_CompletingALostLeaseAbortsTheTransaction` — two executions
  racing after a reclaim; the loser's journal entries must roll back with its
  completion rather than commit alongside the winner's
- `TestIdempotency_ExpiredKeyIsRefusedRatherThanReexecuted` — after the TTL
  sweeps the replay record, the key still refuses a second transaction
- `TestContention_HotAccountReportsItsRetryRate` — 100 writers, 500 transactions
  on one account, reporting the retry rate and asserting zero deadlocks
- `TestSharding_CanRefuseADebitTheLogicalAccountCouldAfford` — pins the
  documented liveness cost of sharding as a reproducible property
- `TestOpenAPI_MatchesTheRegisteredRoutes` — `chi.Walk` against the spec, both
  directions
- `Canonicalize` against the RFC 8785 worked example, the ECMAScript number
  boundaries at 1e21 and 1e-7, and duplicate-key rejection

Coverage across `internal/ledger` and `internal/outbox` is 85.5%.

## Documentation

- [CLAUDE.md](CLAUDE.md) — invariants, stack, and working agreements
- [api/openapi.yaml](api/openapi.yaml) — the HTTP contract, validated against
  the router by the test suite
- [docs/DECISIONS.md](docs/DECISIONS.md) — every significant decision, what was
  rejected, and why. Phase 3 is D20–D29; the open gaps are listed at the end
