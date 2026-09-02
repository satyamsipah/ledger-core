# Ledger-Core

A production-grade, event-driven double-entry ledger — the accounting core that
sits underneath a payments platform.

Correctness under concurrency is the point of this system, so the invariants
that matter are enforced by PostgreSQL rather than by application code alone.
Application checks describe what the code intends; the database constraints are
what stay true when a migration, an admin session, or a service nobody has
written yet does something unanticipated.

## Status

**Security fix, ahead of Phase 6: D19 (spoofable client IP) and D24
(unauthenticated, globally-shared idempotency namespace) are closed**, three
phases after they were first flagged and deliberately left open rather than
guessed at. Every write route now requires `Authorization: Bearer <key>`, and
every idempotency key — ledger-level and saga-level — is scoped to the
principal that presented it. See [Authentication](#authentication) and D19/D24
in [docs/DECISIONS.md](docs/DECISIONS.md).

**Phase 5 complete: multi-step, multi-party money movement that stays correct
when a step fails halfway.** A marketplace payout debits a customer wallet into
platform suspense, calls an external payment gateway, and settles to the
merchant and to fee revenue — compensating with a reversing transaction if any
of that fails. The orchestrator drives itself from durable Postgres state, every
step commits its ledger entries and its saga transition in one COMMIT, and an
*unknown* gateway outcome is resolved by asking the gateway rather than by
guessing. See [Sagas](#sagas).

**Phase 4: events out of Postgres, without losing or inventing one.**
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
with no authentication behind them (D24) — which `saga_instances.idempotency_key`
now inherits — and a saga in `NEEDS_MANUAL_REVIEW` has no in-product resolution
path, so an operator fixes it by hand (D43). All recorded in
[docs/DECISIONS.md](docs/DECISIONS.md) rather than papered over.

## Quick start

Requires Docker and Go 1.25+.

```bash
make up      # postgres, redpanda, kafka-connect, redis, api, outbox-publisher,
             # projector, saga-orchestrator, mock-gateway
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
cmd/saga-orchestrator  claim loop + timeout sweeper driving payout sagas
cmd/mock-gateway    LOCAL ONLY: a real payment gateway stand-in with
                    injectable failure, latency and two flavours of hang
cmd/issue-api-key   one-shot: mints one API key for one principal, then exits
internal/auth       API key authentication; hashed at rest, never stored raw
internal/auth/pgauth  the api_keys statements
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
internal/saga       saga vocabulary and persistence port; imports no ledger,
                    so internal/ledger can import it and offer AdvanceSaga
internal/saga/pgsaga  the saga_instances/saga_steps statements, including the
                    step commit that runs inside the ledger transaction
internal/saga/payout  the marketplace payout state machine and orchestrator
internal/gateway    external payment gateway client; three-valued outcome
internal/gateway/mock  the mock server behind cmd/mock-gateway
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

| Method | Path | Auth | Idempotency-Key |
|---|---|---|---|
| `POST` | `/v1/transactions` | required | required |
| `POST` | `/v1/transactions/{id}/reverse` | required | required |
| `GET` | `/v1/accounts/{id}/balance` | — | — |
| `GET` | `/v1/accounts/{id}/statement` | — | — |
| `POST` | `/v1/payouts` | required | required |
| `GET` | `/v1/sagas` | — | — |
| `GET` | `/v1/sagas/{id}` | — | — |
| `GET` | `/healthz`, `/readyz` | — | — |

Every write route requires `Authorization: Bearer <key>`. Issue one with
`cmd/issue-api-key -principal <id>` (a one-shot CLI, in the shape of `migrate`
and `kafka-init` — not an admin API, since this service has none yet). The key
is printed once and is never stored anywhere; `api_keys.key_hash` is a SHA-256
digest, the same shape idempotency fingerprints already use, for the same
reason. See [Authentication](#authentication).

`POST /v1/payouts` answers **202, not 201**: no money has moved when it returns.
Its `Idempotency-Key` dedupes the whole saga rather than a single transaction,
and it is carried by `saga_instances.idempotency_key` rather than by the
idempotency middleware — that machinery completes a key inside the ledger's
transaction, and a saga has no ledger transaction at the moment it is created.

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

## Authentication

`internal/auth` closes docs/DECISIONS.md D24: every idempotency key, and every
saga's own dedupe, is scoped to the caller who presented it, and there is no
way to be "the caller who presented it" without a secret this service issued.

```bash
go run ./cmd/issue-api-key -principal acme-corp
# principal: acme-corp
# key:       lk_live_9f2a...
#
# This key is shown once and is not recoverable. Store it now.
```

The key is checked into nothing and stored nowhere as plaintext — `api_keys.key_hash`
is a SHA-256 digest, the same shape `idempotency_keys.request_fingerprint`
already uses. Authentication is one indexed lookup by that hash; there is no
byte-by-byte comparison of a secret against attacker input for a timing side
channel to measure.

**Deliberately not here:** key rotation, listing, an admin API, expiry, scope.
`cmd/issue-api-key` is the entire provisioning surface — a one-shot CLI in the
shape of `migrate` and `kafka-init` — because those are real admin-dashboard
features, and building them to close a namespace-collision bug would be later
work borrowing this fix's authority. Revocation works today
(`UPDATE api_keys SET status = 'REVOKED', revoked_at = now() ...`); only the
API for it is absent.

**What this does not provide: authorization.** An authenticated principal may
read or post against any account — there is no per-principal ownership check
on `accounts`. Closing that is real Phase 6 design work (which accounts a
principal owns, single- or multi-tenant, how it interacts with sharding's
`parent_account_id`) and is out of scope for what D24 needed. See D47.

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

## Sagas

A marketplace payout spans an external payment gateway, so it cannot be one
database transaction. What it is instead is three steps, a compensation, and a
defined answer for the case where the gateway's outcome is unknown.

### Three steps, not five

The business description names five movements. Two of them cannot be separate
steps: a transaction carrying only "debit the customer wallet" sums to a
non-zero value and the deferred balance trigger rejects it at `COMMIT`.
Invariant 1 is not something a saga gets to step around.

| Step | Ledger movement | Compensation |
|---|---|---|
| `RESERVE` | DEBIT wallet / CREDIT platform suspense | reverse it |
| `GATEWAY` | none — the external call | none; resolved by probe, not undone |
| `SETTLE` | DEBIT suspense / CREDIT merchant / CREDIT fee revenue | reverse it |

### The state machine

```
  POST /v1/payouts
        |
        v
   [ PENDING ] --reserve--> [ RESERVED ] --intent--> [ GATEWAY_PENDING ]
        |                                              |     ^      |
        | insufficient funds                     200 OK|     |probe |declined
        v                                              v     |      v
   [ FAILED ]                              [ GATEWAY_SUCCEEDED ]  [ GATEWAY_FAILED ]
   nothing moved                                       |                 |
   nothing to undo                              settle |                 v
                                                       v          [ COMPENSATING ]
                                               [ COMPLETED ]             |
                                                                         v
                                                                  [ COMPENSATED ]
                                                                  every balance
                                                                  exactly restored

  probes exhausted, or compensation exhausted  -->  [ NEEDS_MANUAL_REVIEW ]
                                                    money held in suspense;
                                                    event + metric + log + API
```

Every value of `status` is a **settled** state. There is deliberately no
`RESERVING` or `SETTLING`: a step commits its journal entries and its transition
together, so no crash can leave a saga describing itself as halfway through
something. In-flight-ness is a lease (`lease_owner`, `lease_expires_at`), the
same shape `idempotency_keys` uses, resting on the same property — a lapsed
lease is proof its owner committed nothing.

### The step and the money commit together

`ledger.Tx` gained `CommitSagaStep` and `ApplyPendingDelta`, and
`ledger.TransactionRequest` gained a `Record` hook. A forward step's journal
entries, balance updates, `pending_minor` movement, saga transition, audit row
and outbox event are one `COMMIT`. That is D20's argument with higher stakes:
there, a lost bookkeeping write duplicated a response; here it would re-run a
debit against a customer's wallet.

Consequence: **the saga never writes a `PENDING` transaction header.** This
closes the gap carried since Phase 1 — a `transactions` row reaching `POSTED`
with zero entries — by never taking that path.

### What actually stops a double-spend

Not `pending_minor`. `account_balances_no_overdraft_check` is
`allow_negative OR available_minor >= 0` and does not mention that column, so a
hold written only there is invisible to the constraint and stops nothing.

The guard is the **suspense debit itself**: once the wallet is debited, a second
payout is refused by invariant 4's existing `CHECK` under the existing row lock —
the ordinary write path's protection, reused unchanged.
`pending_minor` on the suspense account says how much of what is sitting there
belongs to an unfinished saga, which is what makes the intermediate state
self-describing rather than merely visible. It also gives a reconciliation
invariant: `suspense.pending_minor` must equal the summed amount of every
non-terminal saga.

### An unknown gateway outcome

A timeout, a severed connection and an orchestrator crash mid-call are
indistinguishable from this side, and all three mean the same thing: a payment
may or may not exist. The saga does **nothing** — it sits in `GATEWAY_PENDING`
and the sweeper probes until it gets a conclusive answer.

Assuming failure refunds a customer whose money really left. Assuming success
pays a merchant for a payment that never happened. Waiting is affordable because
the money is in a named suspense account — taken from the customer, not given to
the merchant, owned by nobody and lost by nobody for as long as it lasts.

Three things make asking possible: the gateway key is `<saga_id>:GATEWAY`, a
pure function of the saga id and therefore recomputable after any crash; the
intent row and the move to `GATEWAY_PENDING` are committed *before* the call
goes out; and resolution is a `GET`, which cannot itself create a payment.

### When it gives up

After `LEDGER_GATEWAY_MAX_PROBES` inconclusive probes, or
`LEDGER_SAGA_MAX_COMPENSATION_ATTEMPTS` failed compensations, the saga stops in
`NEEDS_MANUAL_REVIEW`. It is never silently dropped: a `SagaNeedsManualReview`
event goes to `ledger.events.saga`, `ledger_saga_manual_review_total`
increments, an ERROR line is logged, and it is listed by
`GET /v1/sagas?status=NEEDS_MANUAL_REVIEW`.

Automatic resolution is refused on purpose. A compensation that burned its whole
budget failed for a reason the orchestrator does not understand, and the only
automatic fixes available — force-posting with `allow_negative`, or an
`ADJUSTMENT` — mint money no business event justifies. A ledger that can silently
repair itself is one whose balances are no longer evidence of anything. See D43.

### Running it

```bash
curl -X POST localhost:8080/v1/payouts \
  -H "Idempotency-Key: $(uuidgen)" -H 'Content-Type: application/json' \
  -d '{"customer_wallet_id":"01920000-0000-7000-8000-000000000011",
       "platform_suspense_id":"01920000-0000-7000-8000-000000000005",
       "merchant_payable_id":"01920000-0000-7000-8000-000000000021",
       "fee_revenue_id":"01920000-0000-7000-8000-000000000003",
       "amount":{"amount":"20000","currency":"INR","scale":2},
       "fee":{"amount":"500","currency":"INR","scale":2}}'
```

Returns **202**, not 201: no money has moved yet. Poll `GET /v1/sagas/{id}` for
the outcome and the full attempt history.

To drive the failure paths against the running stack:

```bash
make gateway-behaviour BEHAVIOUR='{"outcome":"decline"}'   # compensation
make gateway-behaviour BEHAVIOUR='{"hang":"after"}'        # ambiguity
docker compose -f deploy/docker-compose.yml pause mock-gateway
make sagas-stuck                                           # the triage list
```

## Reconciliation

`cmd/reconciler` proves the ledger correct against a source of truth this
service does not control: a PSP settlement statement. On a schedule (daily by
default, `LEDGER_RECONCILER_INTERVAL`) it reads a CSV from
`LEDGER_RECONCILER_PSP_CSV_PATH` and three-way matches it against this
ledger's own `transactions` and the saga orchestrator's `saga_instances`,
keyed on `external_ref`.

Every mismatch is classified into one of six categories:

| Category | Meaning |
|---|---|
| `MISSING_IN_LEDGER` | The statement names a reference no transaction carries |
| `MISSING_IN_PSP` | The ledger posted it; the statement never mentions it |
| `AMOUNT_MISMATCH` | The amount or currency disagrees |
| `STATUS_MISMATCH` | e.g. the ledger says `POSTED`, the PSP says `FAILED` |
| `TIMING_DIFFERENCE` | Only the settlement instant disagrees |
| `DUPLICATE` | The statement lists one reference more than once |

Only `TIMING_DIFFERENCE` auto-resolves, and only when the gap is inside
`LEDGER_RECONCILER_TIMING_WINDOW` (default two hours). Every other category
becomes an `OPEN` exception requiring review. The report is
`GET /v1/reconciliation/runs/{id}` — a run's own counts, a breakdown by
category, and every exception it raised.

**What counts as "the ledger's amount" for a reference.** A single payout saga
posts two transactions sharing one `external_ref` (RESERVE and SETTLE), so the
match takes the *latest* transaction per reference and reads its amount as the
sum of its `DEBIT`-side entries — safe for any balanced transaction, because
invariant 1 already guarantees debits equal credits, not a rule specific to
payouts.

```bash
curl -H "Authorization: Bearer $KEY" localhost:8080/v1/reconciliation/runs
```

Authenticated, unlike the saga read routes — see [Authentication](#authentication)
for `$KEY`. An exception carries amounts and external references, the same
class of information D24 already scopes idempotency responses to a principal
to protect.

See `docs/DECISIONS.md` (Phase 6) for the match query, the classification
priority order, and why the join runs in SQL while classification runs in Go.

`deploy/seed/psp_statement.example.csv` shows the format and one row per
category. It is not wired into `docker compose up` by default — unlike
`deploy/seed/seed.sql`, which seeds accounts with no transactions, a
meaningful reconciliation demo needs transactions actually posted with
matching `external_ref` values first. To try it: post a few transactions with
`external_ref` set (e.g. `demo-clean`, `demo-amount-mismatch`), copy the
example file, edit its rows to match what you posted, then point
`LEDGER_RECONCILER_PSP_CSV_PATH` at it and restart `cmd/reconciler`.

## Consistency checks

Alongside the PSP match, `cmd/reconciler` runs three structural checks
against the ledger's own data on a short ticker
(`LEDGER_RECONCILER_CONSISTENCY_INTERVAL`, default one minute) — no
configuration required, unlike the PSP match, because "is our own data
internally consistent" should not wait on an operator pointing this process
at a settlement file:

| Check | What it proves |
|---|---|
| Global invariant | `SUM` of every signed journal entry, per currency, is still zero across the *entire* journal — not merely for the one transaction the deferred trigger last fired on |
| Projection drift | `account_balances` (the synchronous balance, updated under lock) agrees with a full recomputation from `journal_entries` |
| Orphan detection | No `POSTED`/`REVERSED` transaction has fewer than two entries, and no journal entry lacks a parent transaction |

Projection drift here is deliberately not the same check `make rebuild`
already runs — that one diffs the journal against `balance_projections`, the
Kafka-driven read model, to prove the async pipeline agrees with the ledger.
This one diffs against `account_balances`, the write path's own synchronous
balance, closing the three-way triangle D1 in `docs/DECISIONS.md` originally
described.

Each check reports itself as a Prometheus gauge (`ledger_consistency_*`,
scraped from the metrics port) and, on a violation, an `ERROR` log line
naming the offending currency, account, or transaction ids. The global
invariant gauge is the one to page on: any nonzero value means invariant 1 no
longer holds somewhere in the journal.

See `docs/DECISIONS.md` D49 for why these three are plain query functions
rather than a persisted store like the PSP match, and why proving they
actually *detect* a violation needed a private database rather than the test
suite's shared one.

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

Phase 5 adds the saga failure paths. Every failure below is a real one — a real
unanswered HTTP request, a real killed database backend, a real frozen account —
because `.claude/rules/testing.md` requires failure tests to kill things rather
than flip a boolean, and names the gateway specifically. The orchestrator has no
test-only branch; its one seam is `WithCrashHook`, exported and kept off
`Config` so ordinary configuration cannot reach it.

- `TestSagaPayout_GatewayFailureCompensatesToTheExactPreSagaState` — every
  balance, including the pending hold, restored to the unit
- `TestSagaPayout_AmbiguousGatewayOutcomeIsResolvedByQueryNotByGuess` — three
  sub-cases against a genuinely hanging gateway: the payment really succeeded
  (settle, and do not refund it), it never happened (compensate), and the
  gateway is killed so it can never say (manual review). The first two are
  indistinguishable to the caller and opposite in fact
- `TestSagaPayout_CompensationExhaustionNeedsManualReview` — the wallet is
  frozen between reserve and compensation, which makes the reversal genuinely
  impossible; asserts the escalation, the alert event, the metric, and that the
  money is still held rather than invented back
- `TestSagaPayout_CrashMidFlightResumesExactlyOnce` — `pg_terminate_backend`
  inside the settle transaction, after its entries are inserted and before the
  saga advances. Asserts the rollback, then that a second orchestrator settles
  exactly once and the gateway was charged exactly once
- `TestSagaPayout_ConcurrentSagasOnOneWalletCannotDoubleSpend` — 100 sagas
  against a wallet that can afford 40, driven by 8 concurrent orchestrator
  replicas; exactly 40 complete, 60 are refused, the wallet lands on exactly
  zero and every hold is released

The three central guarantees were also checked by mutation: guessing on an
ambiguous outcome, forgetting to release the hold, and dropping the
compare-and-set guard from the state transition each make the corresponding test
fail. A test that cannot fail is not evidence.

## Documentation

- [CLAUDE.md](CLAUDE.md) — invariants, stack, and working agreements
- [api/openapi.yaml](api/openapi.yaml) — the HTTP contract, validated against
  the router by the test suite
- [docs/DECISIONS.md](docs/DECISIONS.md) — every significant decision, what was
  rejected, and why. Phase 3 is D20–D29, Phase 4 is D30–D36; the open gaps are
  listed at the end of the Phase 3 section
