# Ledger-Core

A production-grade, event-driven double-entry ledger — the accounting core that
sits underneath a payments platform.

Correctness under concurrency is the point of this system, so the invariants
that matter are enforced by PostgreSQL rather than by application code alone.
Application checks describe what the code intends; the database constraints are
what stay true when a migration, an admin session, or a service nobody has
written yet does something unanticipated.

## Status

**Phase 4 complete: events out of Postgres, without losing or inventing one.**
Every write still lands in `outbox` inside the same transaction as the journal
(invariant 6), and now two independent publishers can carry it to Kafka —
Debezium reading the write-ahead log (the default) or a polling publisher this
repository runs itself — behind one interface and one config flag. A balance
projector consumes the stream, dedupes by `event_id`, and can rebuild itself
from `journal_entries` and diff against its own live state on demand. See
[Events](#events) and [The projector](#the-projector).

Phase 3 (idempotency, concurrency, sharding), Phase 2 (the ledger core) and
Phase 1 (skeleton, schema, local environment) are unchanged and still the
foundation: `docker compose up` brings the whole stack up from scratch,
including the topic layout, the connector, the publisher and the projector.

Three gaps are open and worth knowing about before deploying this anywhere
real: the client IP is spoofable (D19), idempotency keys share one namespace
with no authentication behind them (D24), and `SagaStepCompleted` is a
declared event type with no orchestrator behind it yet — `internal/saga` is
still the Phase 1 stub. All recorded in
[docs/DECISIONS.md](docs/DECISIONS.md) rather than papered over.

## Quick start

Requires Docker and Go 1.25+.

```bash
make up      # postgres, redpanda, kafka-connect, redis, api, outbox-publisher, projector
make seed    # load the development chart of accounts
```

| Endpoint | URL |
|---|---|
| API liveness | http://localhost:8080/healthz |
| API readiness | http://localhost:8080/readyz |
| Post a transaction | `POST` http://localhost:8080/v1/transactions |
| API metrics | http://localhost:9090/metrics |
| Kafka Connect | http://localhost:8083/connectors |
| Outbox publisher health + metrics | http://localhost:9091/readyz — one port for both; see [Events](#events) |
| Projector health + metrics | http://localhost:9093/readyz — same reason |

```bash
make rebuild   # recompute balances from journal_entries, diff against the live projection
```

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
cmd/outbox-publisher  runs whichever outbox publisher LEDGER_OUTBOX_PUBLISHER
                    names -- polling, or a Debezium connector health monitor
cmd/kafka-init      one-shot: provisions the Kafka topic layout, then exits
cmd/projector       Kafka consumer maintaining the read-side balance
                    projection; -rebuild recomputes it from journal_entries
                    and diffs against the live one
cmd/reconciler      scheduled invariant checks against data at rest
internal/ledger     double-entry domain: Money, entries, ledger.Service
internal/ledger/pgledger  the PostgreSQL repository: locking and SQL
internal/idempotency  request de-duplication (invariant 5)
internal/idempotency/pgidem  the idempotency_keys statements, including the
                    completion that runs inside the ledger transaction
internal/outbox     transactional outbox (invariant 6); the event envelope
                    every publisher and consumer agrees on
internal/outbox/publish  the Publisher interface, and both implementations:
                    publish/polling, publish/debezium
internal/kafka      topic names, partition counts, explicit per-topic config
internal/projector  consumes and applies events, dedupes by event_id, rebuilds
internal/saga       multi-step orchestration with compensating transactions
internal/http       router, middleware, health, server lifecycle
internal/db         pgx pool, query-timeout conventions, 40001/40P01 retrier
internal/config     environment configuration
internal/observability  slog, Prometheus, OpenTelemetry
migrations/         golang-migrate SQL, up and down
test/               integration tests against real PostgreSQL and Kafka
deploy/             Docker Compose stack, Dockerfile, Debezium connector, seed
api/openapi.yaml    OpenAPI 3.1, checked against the router by the test suite
```

## The schema

Eight tables: `accounts`, `transactions`, `journal_entries`, `account_balances`,
`idempotency_keys`, `outbox`, `balance_projections`, `processed_events`. The
last two belong to the projector, not the write path — see
[The projector](#the-projector).

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

## Events

Every write that changes state appends a row to `outbox`, inside the same
database transaction as the journal entries it describes — invariant 6, and
the mechanism the [dual-write problem](docs/DECISIONS.md) does and does not
solve is spelled out in D30. What follows is turning that committed row into a
Kafka message, and this repository ships two ways to do it, behind one
interface, selected by `LEDGER_OUTBOX_PUBLISHER` (`debezium`, the default, or
`polling`):

| | Debezium (default) | Polling |
|---|---|---|
| How | Reads the write-ahead log via a registered connector | `SELECT … FOR UPDATE SKIP LOCKED LIMIT 100`, produces, marks `published_at`, one transaction |
| Ordering | Strict commit (LSN) order | Insertion order — not quite the same thing under concurrency |
| Ops cost | A Kafka Connect cluster, a replication slot to monitor | One more Go binary |

Full comparison — latency, crash behaviour, why `SKIP LOCKED` is what makes
running several polling replicas safe rather than merely running — is D31.

**The wire format is identical either way.** The full envelope —
`event_id` (UUIDv7), `event_type`, `event_version`, `aggregate_id`,
`occurred_at`, `trace_id`, `payload` — is assembled once, in Go, and stored as
the *entirety* of `outbox.payload`. Debezium's connector does nothing but
relay that column verbatim; that is what makes switching the config flag a
real choice rather than a change in message shape too (D31's closing
argument, D32 for the envelope itself).

Three topics, each on `ledger.events.<name>`, explicit partition counts and
retention rather than broker defaults (`internal/kafka`, provisioned by
`cmd/kafka-init` before anything else starts):

| Topic | Carries | Keyed by | Partitions |
|---|---|---|---|
| `transaction` | `TransactionPosted`, `TransactionReversed` | `transaction_id` | 6 |
| `account` | `AccountCreated`, `BalanceUpdated` | `account_id` | 12 |
| `saga` | `SagaStepCompleted` (declared; no orchestrator emits it yet) | saga id | 3 |
| `dlq` | poison messages, from Connect or from the projector | — | 3 |

**What keying by `account_id` guarantees, and what it does not.** Every event
that ever mentions a given account is delivered to one consumer in commit
order — real per-account ordering. What it does *not* give: a single
transfer's debit and credit are two independent `BalanceUpdated` messages on
two different partitions, with no ordering relationship between them. A
transaction can't be keyed by one account it isn't only about — see D32 for
why `TransactionPosted` is keyed by `transaction_id` instead, and why the
projector below doesn't need the per-account guarantee anyway.

## The projector

`cmd/projector` consumes `TransactionPosted`/`TransactionReversed` and
maintains `balance_projections` — a read model built **entirely from the
Kafka stream**, independently of `account_balances`, which the write path
updates synchronously under its own row lock. Two numbers computed by
different code paths agreeing is what gives reconciliation a real job.

Applying an event is a version compare-and-set
(`UPDATE … WHERE version < $new`), not a delta — so it converges correctly
regardless of redelivery or arrival order, without needing the per-account
ordering guarantee above at all. `processed_events`, written in the same
local transaction as the projection update, closes the one gap the
compare-and-set alone doesn't: the window between that commit and this
consumer's own Kafka offset commit (D33). Offsets are committed manually,
only after the local transaction succeeds.

An event type this build doesn't recognise is routed to `ledger.events.dlq`
— tagged with its source topic, partition, offset and the error — rather
than wedging every message behind it (D34).

```bash
make rebuild   # or: docker compose run --rm projector -rebuild -accounts <uuid,...>
```

Recomputes every account's balance **directly from `journal_entries`** —
bypassing Kafka, the outbox and every publisher entirely — and diffs it
against the live projection, account by account, exiting non-zero on any
disagreement. `-accounts` scopes it to specific accounts, which is both a
real operational need (a targeted investigation into one customer's balance)
and what makes the check usable against a database other tests also touch.

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

Coverage across `internal/ledger` and `internal/outbox` is 85.5% as of Phase 3;
Phase 4's new packages are covered by the integration suite below rather than
by isolated unit tests, since almost everything they do only means something
against a real broker and a real database.

Phase 4 adds, actually killing things per `.claude/rules/testing.md`:

- `TestOutboxPublish_KafkaOutage` — pauses the real Redpanda container
  (`docker pause`, not `Stop`/`Start`; see below) mid-run, posts more
  transactions while it's down, asserts the backlog accumulates and doesn't
  silently drain, then unpauses and asserts zero loss by exact row count
- `TestOutboxPublish_PollingCrashBetweenPublishAndMark` — a hook fires after
  Kafka has genuinely acknowledged a batch and before the marking transaction
  commits; `pg_terminate_backend` kills the publisher's connection at exactly
  that instant. Confirms the row is still unpublished, the message reached
  Kafka anyway, a retry produces a real duplicate `event_id` on the wire, and
  `projector.Applier` recognises and skips the second delivery
- `TestProjector_RebuildMatchesLive` — posts a mixed workload through the real
  write path, drains it to a real broker with the real polling publisher,
  consumes and applies it with the real consumer, then diffs the result
  against `journal_entries` directly — the required end-to-end check, not a
  check of any one component's self-consistency
- `TestProjector_UnknownEventTypeIsDeadLettered` — a message this build can't
  apply lands on the DLQ tagged with its origin, and its offset still commits

Two things learned writing these, worth knowing before relying on either
mechanism elsewhere: franz-go's idempotent producer won't honour
`RecordDeliveryTimeout` for a record already sent with no response — correct
for exactly-once delivery, and simply not what this at-least-once publisher
needs (D30, D36); and the Testcontainers Redpanda module can't be cleanly
stopped and restarted via `container.Stop()`/`Start()`, because its custom
entrypoint waits on a lifecycle hook that only runs during the original
`Run()` — `docker pause`/`unpause` sidesteps the problem rather than working
around it.

## Documentation

- [CLAUDE.md](CLAUDE.md) — invariants, stack, and working agreements
- [api/openapi.yaml](api/openapi.yaml) — the HTTP contract, validated against
  the router by the test suite
- [docs/DECISIONS.md](docs/DECISIONS.md) — every significant decision, what was
  rejected, and why. Phase 3 is D20–D29, Phase 4 is D30–D36; the open gaps are
  listed at the end of the Phase 3 section
