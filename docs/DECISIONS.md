# Architecture Decision Log

Every significant decision, the options rejected, and why.

---

## Phase 1 — Skeleton, schema, and local environment

### D1. `account_balances` is synchronous and authoritative

**Decided.** The balance row is updated inside the same database transaction
that writes the journal entries, under `SELECT … FOR UPDATE` on the account, and
guarded by a `CHECK` constraint.

**Rejected:** maintaining `account_balances` only from the Kafka event stream.
It reads well against the architecture diagram, but invariant 4 asks for a
`CHECK` constraint plus in-transaction locking, and you cannot enforce a
non-negative balance against a row that some other process will update later.
Overdraft protection would have degraded into eventual detection of overdrafts
already granted.

**Consequence:** `allow_negative` is denormalised from `accounts` onto
`account_balances`, because a `CHECK` constraint cannot reference another table.
A trigger on `accounts` keeps the copy in step. Flipping the flag to `false` on
an account already overdrawn fails that trigger's `UPDATE` — deliberately, since
a loud error beats a silently violated invariant, though the admin path will
need to handle it explicitly in a later phase.

**Consequence:** the Kafka-driven projector builds a *separate* read model. Two
independently derived balances that must agree is what gives the reconciliation
engine a real job: compare the synchronous balance, the event-sourced
projection, and a full aggregate of `journal_entries`. Any two agreeing while
the third dissents localises the bug immediately.

### D2. `TEXT` + `CHECK` rather than native `ENUM` types

**Decided.** Every enumerated column is `TEXT` with a `CHECK` constraint.

**Rejected:** PostgreSQL native `ENUM`. It is more compact and type-safe, but
`ALTER TYPE … ADD VALUE` cannot be undone — there is no `DROP VALUE` — which
collides head-on with the requirement that every migration be reversible. The
day someone adds `SETTLED` to a status enum, they have written a migration whose
`.down.sql` is a lie.

**Also considered:** `CREATE DOMAIN`, which is alterable and droppable and would
have given reuse across tables. Rejected as a layer that buys little over a
plain `CHECK` at this size.

**Cost accepted:** a few bytes per row, and type-safety pushed into the Go
sentinel errors in `internal/ledger` rather than the database type system.

### D3. UUIDv7 primary keys, generated in Go

**Decided.** All UUID primary keys are v7, generated application-side with
`google/uuid`.

**Rejected:** UUIDv4. Uniformly random keys land on random B-tree leaves, which
means page splits, poor cache locality, and index write amplification that
compounds as `journal_entries` grows. v7's time-ordered prefix makes index
growth near-append-only and gives `id DESC` a usable rough time ordering.

**Note:** PostgreSQL 16 has no native `uuidv7()` — that arrived in 18 — so
application-side generation was required regardless. It also keeps ID generation
off the database round-trip.

### D4. Debezium is the outbox publisher; no polling fallback in Phase 1

**Decided.** Rows appended to `outbox` are published by Debezium reading the
write-ahead log. `published_at` is retained but nothing writes it on the happy
path.

**Rejected:** a Go polling publisher that claims unpublished rows, writes to
Kafka, and stamps `published_at`. It duplicates what CDC already does and adds a
second thing that can fall behind.

**Honest consequence:** `outbox_unpublished_idx` therefore serves monitoring
("how deep is the backlog?") and any future fallback publisher, not the hot
path. It is kept because both of those are real, not because the textbook
outbox pattern has that index.

**Consequence:** delivery is at-least-once. Every consumer must be idempotent on
the event id.

### D5. The balance check is a deferred constraint trigger

**Decided.** `CREATE CONSTRAINT TRIGGER … AFTER INSERT … DEFERRABLE INITIALLY
DEFERRED`, aggregating by `(transaction_id, currency)`.

**Rejected:** an ordinary `AFTER … FOR EACH ROW` trigger. Entries are inserted
one row at a time, so a transaction is unbalanced after its first `INSERT` by
necessity. A non-deferred trigger rejects that first row every time, which makes
the invariant unenforceable rather than merely inconvenient.

**Rejected:** validating only in application code. That leaves the invariant true
only for code paths that remember to check, which is exactly the set that shrinks
over a system's lifetime.

**Cost accepted:** PostgreSQL does not allow deferred statement-level triggers,
so the trigger is necessarily `FOR EACH ROW` and a two-leg transaction runs the
check twice at commit. The aggregate is an index-only scan over a handful of
rows. Not worth optimising, and the obvious optimisation (making it
non-deferred) does not work at all.

**Free consequence:** because `amount_minor > 0`, a single entry can never sum
to zero, so the check also enforces "at least two legs per currency" without a
separate constraint.

### D6. The CDC publication lives in the migration chain

**Decided.** `CREATE PUBLICATION ledger_outbox_pub FOR TABLE outbox` is part of
migration `000008`, and the Debezium connector runs with
`publication.autocreate.mode: disabled`.

**Rejected:** an initial bootstrap script in `deploy/`, which was the original
plan. The publication must name the `outbox` table, and that table only exists
once migrations have run — so the bootstrap script would have had nothing to
reference. Versioning the replicated table set alongside the schema it describes
is also simply more honest about what it is.

**Rejected:** letting Debezium autocreate the publication. That requires
superuser, which the connector's database role should not have in production.

**Requirement this creates:** the migration role needs `CREATE` on the database.

### D7. The journal's currency correctness is a composite foreign key

**Decided.** `journal_entries (account_id, currency)` references
`accounts (id, currency)`, backed by a `UNIQUE (id, currency)` on `accounts`.

**Rejected:** a validating trigger, or checking in application code. The foreign
key makes "post USD into an INR wallet" unrepresentable rather than merely
discouraged, and it costs one redundant unique index on a small table.

### D8. Strict `normal_balance` / `account_type` CHECK; no contra accounts

**Decided.** `ASSET`/`EXPENSE` must be `DEBIT`-normal, and
`LIABILITY`/`EQUITY`/`REVENUE` must be `CREDIT`-normal.

**Rejected:** leaving the two independent to allow contra accounts (a
contra-asset carries a `CREDIT` normal balance). Payments ledgers rarely need
them, and letting type and normal balance disagree means every report that
derives sign from type silently reports the opposite of the truth.

**Reversal path:** drop one constraint, in one migration, the day a real contra
account appears.

### D9. Go 1.25 as the module floor

**Decided.** `go 1.25` in `go.mod`, and the same version in CI and the
Dockerfile.

**Why, given CLAUDE.md says 1.22+:** pgx/v5, OpenTelemetry,
prometheus/client_golang and testcontainers-go all now declare `go >= 1.25`.
Holding the module at 1.22 would have meant pinning older versions of every core
dependency. 1.25 satisfies "1.22+" as a floor; noting it so the number in
CLAUDE.md is not read as a ceiling later.

### Index decisions

Every index is tied to a query that exists today. Recorded here because the
rejections matter as much as the additions.

| Index | Query it serves |
|---|---|
| `journal_entries (transaction_id, entry_seq)` UNIQUE | Rendering a transaction's legs in order; also the aggregate the deferred trigger runs at COMMIT |
| `journal_entries (account_id, created_at DESC, id DESC) INCLUDE (…)` | Account statement, keyset-paginated. `INCLUDE` makes it index-only, which works here because an append-only table's pages go all-visible and stay there |
| `transactions (idempotency_key) WHERE NOT NULL` UNIQUE | Invariant 5 at the database level. Partial, so the keyless reversals and adjustments do not collide |
| `transactions (created_at) WHERE status = 'PENDING'` | Saga timeout sweeper. Partial keeps it at a few hundred rows against a table of hundreds of millions |
| `transactions (external_ref) WHERE NOT NULL` | Support tooling: find the transaction for a gateway reference |
| `accounts (external_ref)` UNIQUE | External systems address accounts by their own reference on nearly every request |
| `accounts (owner_id, currency) WHERE NOT NULL` | Wallet lists and per-owner rollups |
| `idempotency_keys (expires_at)` | The TTL reaper |
| `outbox (id) WHERE published_at IS NULL` | Backlog depth, and any future polling publisher |
| `outbox` BRIN on `created_at` | Retention purge. BRIN because the table is append-only and physically ordered by time — kilobytes where a btree costs gigabytes |

**Rejected:** GIN on `transactions.metadata` (nothing queries into it, and GIN on
a write-hot table costs real throughput); any index on `account_balances` beyond
its primary key (it is the most-written table, has only point lookups, and
staying index-free is what makes `fillfactor = 70` HOT updates work); indexes on
`accounts.account_type` and `accounts.status` (low cardinality on a small table);
an index on `idempotency_keys.transaction_id` (ops-only reverse lookup);
standalone indexes to back foreign keys (child-side indexes only matter for
cascading deletes, and nothing here is ever deleted).

### Known gap carried into Phase 2

The deferred trigger fires on `INSERT` into `journal_entries`, so a
`transactions` row with **zero** entries never triggers it. A transaction can
therefore reach `POSTED` with no legs at all. This is legitimate while `PENDING`
— the saga writes the header first — and a bug once posted. Enforcing it needs a
deferred check on the status transition, which is business logic and belongs
with the posting service.

---

## Phase 2 — The ledger core

### D10. READ COMMITTED plus explicit row locks, not REPEATABLE READ or SERIALIZABLE

**Decided.** The posting path runs at READ COMMITTED and serialises concurrent
writers with `SELECT … FOR UPDATE` on `account_balances`, taken in ascending
account-id order.

The argument turns on what the invariants actually are. Every one of them is
either a property of a single row — `allow_negative OR available_minor >= 0`, on
one account — or of one transaction's own entries — the deferred sum, evaluated
at COMMIT with every leg present. None is a predicate spanning rows that a
concurrent transaction could invalidate without touching the same rows. That is
the shape write skew takes, and we do not have it.

**Rejected:** SERIALIZABLE. It defends the anomaly we do not have, at the price
of mandatory retries everywhere and predicate locks (`SIReadLock`) taken over
whatever `journal_entries` ranges the statement and temporal queries scan. Those
escalate to page level on a large table, so a reporting query would begin
aborting writers it never conflicted with.

**Rejected:** REPEATABLE READ. It does catch the anomaly that threatens us — a
lost update on a balance — but it catches it by raising `40001` and rolling
back. A payments ledger has permanently hot accounts: every pay-in credits the
same house float. On those, aborting turns contention into wasted work plus a
retry storm, while a row lock turns it into a queue. Blocking degrades linearly
and predictably; aborting degrades all at once, and the retries collide with
each other.

**Weakness accepted, and what covers it:** READ COMMITTED plus explicit locks is
only correct while every write path remembers to lock. What makes that
survivable is that it is not the enforcement mechanism — the overdraft `CHECK`
and the deferred balance trigger fire unconditionally, so a path that forgets to
lock produces a loud constraint violation rather than a quietly wrong balance.
The application check exists to give a good error; the database check exists to
be true.

**Consequence:** there is no retry loop in the posting path, which keeps the
interaction with idempotency in Phase 3 simple — a retried transaction would
otherwise need to decide whether it was replaying its own earlier attempt.

**Consequence:** read paths that need more than one round trip must run in a
`REPEATABLE READ READ ONLY` transaction, because two statements at READ
COMMITTED can straddle a concurrent commit. Phase 2 sidesteps this by making
`GetBalanceAsOf` and `GetStatement` single statements, but the rule is written
down because the next temporal query will not be.

### D11. `ORDER BY id` inside the locking statement is the deadlock prevention

**Decided.** One statement locks every account a transaction touches:
`… WHERE a.id = ANY($1) ORDER BY a.id FOR NO KEY UPDATE OF a FOR UPDATE OF ab`.

**Rejected:** locking each account in turn, in the order the client listed them.
This is the obvious implementation and it deadlocks: concurrent `A→B` and `B→A`
transfers each hold one row and wait for the other, and PostgreSQL kills one of
them about a second later. Under load that is a steady drip of failed payments
that retrying cannot fix, because the retries deadlock too.

**Verified, not assumed.** That "PostgreSQL places `LockRows` above the sort" is
a property of the plan shape rather than anything the standard promises, so
`TestPostTransaction_ConcurrentOppositeTransfersDoNotDeadlock` asserts it by
experiment. Replacing the ordered statement with per-account locking makes that
test fail with real `40P01` errors within seconds, which is how we know it is
testing the mechanism and not merely passing.

**Sub-decision: two lock strengths.** `accounts` is taken `FOR NO KEY UPDATE`,
not `FOR UPDATE`. Inserting into `journal_entries` takes `FOR KEY SHARE` on the
referenced account row to check the foreign key; `FOR UPDATE` conflicts with
that and `FOR NO KEY UPDATE` does not. Locking `accounts` the stronger way would
make every posting block every other posting's foreign-key check, serialising
transactions that share no account at all.

### D12. The optimistic version bump is a tripwire, not concurrency control

**Decided.** Kept, with `WHERE version = $expected` on the balance UPDATE, and a
zero-row result raises `ErrBalanceVersionConflict` — which callers must never
retry.

**Being honest about it:** once the row's `FOR UPDATE` lock is held, nothing else
can write it before this transaction ends, so the version predicate cannot fail
legitimately. It is not optimistic concurrency control here, and describing it
as such in the code would have been a lie that survives until someone builds a
retry loop on top of it. If it ever matches nothing, a write path mutated the
row without taking the lock, which is a bug to surface rather than a conflict to
retry.

**Why keep it at all:** the version bump itself is load-bearing for the
Kafka-driven projector. Outbox delivery is at-least-once by design (D4), so a
consumer needs a monotonic per-account version to discard a redelivered event
instead of applying a balance change twice. The event payload therefore carries
every touched balance and its resulting version.

**Consequence:** entries are aggregated into one delta per account before being
applied. A transaction touching the same account twice would otherwise consume
two versions for one logical change, and the projector would see a version
sequence it cannot reconcile against a single event.

### D13. Balances are signed by the account's own normal balance

**Decided.** `signedAmount(direction, normalBalance, amountMinor)` returns a
positive value when the entry's direction matches the account's normal balance
and a negative one otherwise. `account_balances.available_minor` therefore means
"value this account holds, counted in its natural direction" on every account
regardless of type.

**Rejected:** reusing the transaction-level convention (`DEBIT = +`,
`CREDIT = −`) for balances as well. It is the right convention for deciding
whether a transaction balances — that is a property of the transaction, not of
the accounts — but it is wrong for storage. A customer wallet is a `LIABILITY`
and so `CREDIT`-normal; under the transaction convention a funded wallet holds a
negative balance, and `account_balances_no_overdraft_check` would fire on every
wallet with money in it while ignoring genuinely overdrawn asset accounts.

**The trap this leaves:** the two conventions coincide for `DEBIT`-normal
accounts, so a sign bug is invisible to any test written only against `ASSET`
accounts. The tests use `LIABILITY` wallets deliberately, and the block comment
in `types.go` says so.

### D14. Phase 2 posts one currency per transaction

**Decided.** `PostTransaction` rejects entries spanning several currencies with
`ErrMixedCurrency`.

**Noting the tension:** the schema is deliberately more permissive. The deferred
trigger balances per `(transaction_id, currency)` precisely so an FX transaction
can carry both legs, the seed data provides USD accounts for it, and `FX` is a
valid `transaction_type`. This restriction is in the service, not the database.

**Why anyway:** nothing in Phase 2 decides an exchange rate or where the
sub-unit residue lands, and a multi-currency transaction without those answers
is a rounding bug waiting for a quiet moment. `FX` is therefore currently
unreachable through this path. Lifting the restriction is deleting one check,
once the FX pricing it depends on exists.

**Reversal is exempt**, and correctly so: mirroring is per-leg, so a reversal
stays balanced per currency without knowing anything about rates.

### D15. Reversal links through metadata; the status transition is the guard

**Decided.** A reversal is a new `REVERSAL` transaction with mirrored directions.
The original is touched only by `UPDATE transactions SET status = 'REVERSED'
WHERE id = $1 AND status = 'POSTED'`. The back-link lives in
`transactions.metadata`.

**Rejected for now:** a `reverses_transaction_id` column with a partial unique
index. It would be the better home for the link, but it is not needed for
correctness, and the schema addition was deliberately deferred.

**What actually prevents a double reversal:** the conditional UPDATE, backed by
the `FOR UPDATE` the reversal already holds on the header. Under READ COMMITTED
a blocked UPDATE re-evaluates its `WHERE` clause against the row version the
winner committed, so the second reversal finds `REVERSED` and matches nothing.
`TestReverseTransaction_ConcurrentReversals` runs twenty at once and asserts
exactly one commits — worth asserting, because two reversals would each balance
perfectly on their own and the balance invariant would not notice the money
being refunded twice.

**Consequence:** the metadata link is an audit trail, not a mechanism. Nothing
reads it to make a decision.

### D16. Temporal queries are bounded-stale, and this is documented rather than fixed

**Decided.** `GetBalanceAsOf` sums the journal on `created_at`, which defaults to
`now()` — transaction *start* time in PostgreSQL, not commit time.

**The flaw, stated plainly:** a transaction beginning at 12:00:00 and committing
at 12:00:03 writes entries stamped 12:00:00. A query for the balance at
12:00:01, run at 12:00:02, misses entries that a later identical query will
include. The answer is monotonic only once no transaction older than the
requested instant is still in flight.

**Rejected:** `clock_timestamp()`. It would order entries by real time, but the
legs of one transaction would then carry different timestamps, so a statement
could render half a transfer. The atomicity the rest of the system works to
guarantee would stop being visible in the one place users actually look.

**Rejected for Phase 2:** a commit-ordered sequence, which is the only thing that
makes the temporal view exactly monotonic. It is real work and it belongs with
the reconciliation engine.

**Consequence:** whenever snapshots arrive, they may only be taken for an
instant with no older transaction still in flight.

### D17. `balance_snapshots` deferred; the read path ships with the seam for it

**Decided.** `GetBalanceAsOf` is a full journal sum today. The query carries a
`baseline` CTE that is a constant zero, and a `(created_at, id)` boundary
comparison that already does the hand-off correctly.

**Why the pair and not just a timestamp:** entries can share a `created_at`, so a
snapshot cutting between two of them on time alone would either double-count the
ties or drop them. Getting that wrong is the classic snapshot bug, and it is
cheaper to be right about it now than to debug it later against real data.

**Consequence:** introducing the table changes one CTE. Nothing else in the
query, and nothing in the service, has to move.

### D18. Balance rows are created by a trigger (migration 000009)

**Decided.** `AFTER INSERT ON accounts` creates the zeroed `account_balances`
row.

**This closed a live hole rather than adding a convenience.** The posting path
serialises on `SELECT … FROM account_balances … FOR UPDATE`. A row lock on a row
that does not exist locks nothing *and reports no error while doing so*: two
concurrent transfers against an account whose balance row was never inserted
would both sail past the lock and neither would block the other. The
serialisation point disappears silently at exactly the moment it matters.
Guaranteeing the row exists lets the service treat "no row returned" as a
missing account, which is a real error, rather than as an empty balance, which
is a plausible-looking lie.

**Consequence:** `test/testdb.go` no longer inserts the balance row, and asserts
it exists instead. `deploy/seed/seed.sql` is unaffected — its insert is already
`ON CONFLICT DO NOTHING`, and the trigger function is too.

**Down migration keeps the backfilled rows.** The migration adds a trigger;
reversing it removes the trigger. Deleting balance rows on the way down would
discard live balances for every account created while it was installed, which is
data loss dressed up as a rollback.

### D19. Client IP: bounded trusted-hop parsing replaces `chi middleware.RealIP`

**Decided, resolving the gap this entry carried since Phase 1.** The deferral's
own reasoning was sound at the time — getting this right needs a real topology,
and guessing one and encoding the guess as a security control is how these
holes are created — but three phases is long enough for "we don't know the
topology yet" to become indistinguishable from "we never will," and the actual
fix does not require knowing it after all. It requires being **honest** about
not knowing it, which is a different and answerable problem.

**What was wrong, restated for the record.** `chi`'s `middleware.RealIP`
(deprecated upstream as a security issue: GHSA-3fxj-6jh8-hvhx,
GHSA-rjr7-jggh-pgcp, GHSA-9g5q-2w5x-hmxf) overwrote `r.RemoteAddr` from the
leftmost `X-Forwarded-For` entry or from `X-Real-IP`, **unconditionally** —
whether or not anything in front of this service actually set those headers.
Every one of them is client-controlled by default, so any caller could assert
any source address, evading an IP-based control and attributing its own traffic
to someone else's address in the same motion.

**The fix: `internal/http/clientip.go`, a bounded trusted-hop parser of
`X-Forwarded-For`, configured by `LEDGER_TRUSTED_PROXY_HOPS` (default `0`).**
Each proxy between a client and this service appends the address it received
the request *from* to the right of the header. With `hops` proxies this
deployment actually trusts, the real client address sits exactly `hops`
positions from the right; anything further left was written by the client
itself or by an untrusted hop. `hops=0` — today's actual deployment, nothing
sits in front of this service — means the header is **ignored outright** and
the resolved address is always the raw TCP peer, which a caller cannot forge
without controlling the network path itself. That is what makes `0` a safe
default rather than a placeholder: it closes the vulnerability without
asserting a topology that does not exist.

**`X-Real-IP` and `True-Client-IP` are no longer read, at any hop count.**
Both are single-value headers with no chaining semantics: trusting one means
trusting that a specific proxy unconditionally *overwrites* whatever the
client sent, a claim this code has no way to verify from where it sits.
`X-Forwarded-For`'s comma-separated chain is the one signal whose trustworthy
prefix is actually computable from a hop count, which is why it is the only
header this bounds. This is a strictly narrower trust surface than `RealIP`
had, not merely a reimplementation of it.

**Rejected: dropping IP trust entirely, with no replacement and no config
knob.** `r.RemoteAddr` would always be the raw socket peer, correct in every
deployment this service runs in today. Rejected because it reproduces the
exact shape of gap this entry already went through once: the day a real
reverse proxy is placed in front of this service, someone has to come back and
touch this code again rather than set an environment variable. A config knob
that defaults to the safe answer costs nothing today and removes that future
trip entirely.

**Consequence: `remote_addr` is now logged.** The request logger
(`internal/http/middleware.go`) previously recorded no client address at all —
the promise that `r.RemoteAddr` "is what the request log records" was aspirational,
not actual. It is safe to make good on now: every value reaching the logger has
already passed through `clientIP`, so it is either the unforgeable TCP peer or
an address a configured, trusted hop actually vouched for, never raw client
input.

**Verification.** `TestClientIP_IgnoresSpoofedHeadersByDefault` sends
`X-Forwarded-For`, `X-Real-IP` and `True-Client-IP` all carrying a spoofed
address at `hops=0` and asserts the resolved address is the real TCP peer — the
exact scenario `chi.RealIP` got wrong. `TestClientIP_TrustsExactlyTheConfiguredHopCount`
covers one and two trusted hops, a chain shorter than the configured hop
count (falls back to the socket peer rather than guessing), a missing header,
and a trusted entry that fails to parse as an address.

### Known gaps carried into Phase 3

- The client IP is spoofable; see D19 above. Resolve it with the gateway design,
  not with a `nolint`.
- The Phase 1 gap — a `transactions` row reaching `POSTED` with zero entries —
  is closed on the `PostTransaction` path, which always writes its legs in the
  same transaction. It remains open for the saga's write-header-first path.
- `pending_minor` is read and carried but never moved. Holds and authorisations
  are Phase 3 work.
- A reversal currently fails if any account it touches has since been `FROZEN`,
  because it goes through the same `Postable()` check as a fresh post. That is
  the stricter reading and it is deliberate for now, but it is arguable: freezing
  an account is meant to stop new activity, and a reversal is a correction of
  activity that already happened. Revisit when the admin path exists to say
  which it wants.
- A duplicate `transactions.idempotency_key` currently surfaces as a wrapped
  `unique_violation` rather than a domain error. Mapping it belongs with the
  idempotency service, which owns what a replay means.

---

## Phase 3 — The write path under duplicates and concurrency

### D20. The idempotency key is completed inside the ledger transaction; the reservation is not

**Decided.** Two database transactions, and precisely which work lives in which
is the whole design.

1. **Reservation** — `INSERT … status = 'IN_PROGRESS' … ON CONFLICT DO NOTHING`,
   committed on its own, before any ledger work.
2. **The work** — journal entries, balance updates, outbox row, **and**
   `UPDATE idempotency_keys SET status = 'COMPLETED', response_status,
   response_body, transaction_id`, all in one transaction.

Everything rests on one property, and it is worth stating as a proposition:

> A record in `IN_PROGRESS` is **proof that no transaction committed under that
> key**, because the move to `COMPLETED` happens in the same transaction as the
> journal entries.

That is what makes a stale lease safe to reclaim with no fencing token, no lock
service, and no distributed coordination of any kind.

**Rejected: one transaction, no observable IN_PROGRESS.** Write the record with
the journal entries and let duplicates block on the unique index
(`ON CONFLICT DO UPDATE … RETURNING` to read the winner's row once it commits).
Atomicity is perfect and the crash window is not merely handled but
*unreachable*. It was rejected for two reasons. The 409-with-`Retry-After`
behaviour becomes impossible to express, because a row written inside the
ledger's transaction is invisible until commit, and by then its status is
already `COMPLETED`. And a hundred duplicates of one key park a hundred
connections on the winner for the full duration of a posting transaction —
against `LEDGER_POSTGRES_MAX_CONNS` of 20 that is service-wide pool exhaustion,
caused by retries of a single request and affecting every unrelated account.
`ErrRequestInProgress`'s doc comment warned about exactly this in Phase 1.

**Rejected: reserve, work, complete — three transactions.** The textbook
version, and *the bug this phase exists to remove*. The work commits, the
completion fails, and the key is left `IN_PROGRESS` over a transaction that
really posted. The retry then finds a stale lease, correctly concludes that a
stale lease means no commit — and is wrong for the first time, because that
inference is only sound while `COMPLETED` and the journal are atomic. It posts
the money twice. A Redis-only record fails the same way and faster, since a
cache eviction is not even a crash.

**Why the reservation may live outside the atomic step.** It carries no
consequence beyond "somebody is trying". Losing one, duplicating one, or
abandoning one costs availability and never correctness. Every crash window
degrades in one direction only:

| Crash point | State left behind | Consequence |
|---|---|---|
| After reservation, before/during the work | `IN_PROGRESS`, no transaction | Stale lease, reclaimable. Liveness only |
| **At the work's COMMIT** (the ambiguous one) | Atomic: both or neither | No window at all |
| After commit, before the response | `COMPLETED` | The retry replays |

**Three independent defences**, in the order a duplicate meets them: the primary
key of `idempotency_keys`; the `status = 'IN_PROGRESS'` guard on the completing
`UPDATE`, which aborts the loser of a reclaim race and takes its journal entries
down with it; and `transactions_idempotency_key_key`, which fires inside the
ledger transaction regardless of anything `internal/idempotency` does. The third
is the one that would still be standing if the other two were deleted. It also
closes the Phase 2 gap that left a duplicate key surfacing as a raw
`unique_violation`.

**Consequence: the response must be rendered before COMMIT.** It has to be
durable at the same instant the journal entries are, so the usual middleware
shape — wrap the `ResponseWriter`, persist what the handler wrote — cannot
supply it. Hence `ledger.ResponseRenderer`, a callback that runs inside the
transaction. The middleware owns the read path (replay, 409, 422); the write
happens in the service. Splitting it that way is the price of the guarantee.

**Consequence: the lease needs its own clock.** `expires_at` is the 24-hour TTL
on the replay record; `lease_expires_at` is how long one request may hold the
key. Seconds against a day. Conflating them means choosing between a crashed
request blocking its own retry for 24 hours and a replay record vanishing while
its owner is still running.

### D21. The 24-hour TTL bounds storage, never correctness

**Decided.** Sweeping deletes the *replay record*. The key itself stays reserved
permanently by `transactions_idempotency_key_key`, so a retry arriving after the
TTL is refused with `ErrKeyExpired` rather than executed again.

**Rejected:** having the sweeper also `NULL` out `transactions.idempotency_key`
so the key becomes reusable. That mutates history to tidy up a cache, and it
destroys the audit link between a client's key and the transaction it created.

The property this buys is worth naming: **expiry can never cause a double post.**
The worst it can do is fail to replay a response, which is a strictly smaller
problem, and the client is told so explicitly rather than being handed a second
transaction.

### D22. Deterministic rejections are cached; transient ones release the key

**Decided.** A rejection that is a property of the **request** is stored as
`FAILED` and replayed. A rejection that is a property of the **world** hands the
key back.

- Cached: unbalanced, too few entries, mixed currency, currency mismatch,
  invalid currency or scale, overflow, unknown type, malformed entry, malformed
  body, missing reversal reason, already reversed, not posted.
- Released: insufficient funds, account frozen, account or transaction not
  found, any 5xx, timeouts, a lost lease.

**Why not cache everything**, which is what Stripe does and is simpler:
`ErrInsufficientFunds` is transient. The account may be funded a second later,
and burning the key permanently would force an honest client to mint a new one
for what is, to it, the same operation — the exact situation idempotency keys
exist to avoid. Frozen accounts get unfrozen and missing accounts get created,
so both are treated the same way.

**Why not release everything**, which is also simpler: an unbalanced transaction
will never balance. Re-running the whole posting path to re-derive a foregone
conclusion is wasted work on every retry of a client that has a bug.

**Getting this wrong is asymmetric, and mildly so in both directions.** Caching
a transient failure costs availability; releasing a deterministic one costs a
little wasted work. Neither can double-post, because the release is guarded on
`status = 'IN_PROGRESS'` — so if the ledger transaction did commit and the
release ran anyway, it matches nothing.

### D23. Redis is not a dependency yet; `Cache` is an interface with a no-op default

**Decided.** `idempotency.Cache` is declared, `NoopCache` is wired, and
`redis/go-redis/v9` is not in `go.mod`.

The read-through fast path is specified — it may hold **terminal records only**,
so a stale read is indistinguishable from a fresh one and the cache cannot
produce a wrong answer — but the implementation is deferred. Correctness never
depended on Redis, which is the property that makes deferring it possible at all,
and `ledger_idempotency_outcomes_total{outcome="cache_hit"}` against
`cache_miss` is what will say whether the dependency has earned itself.

**Rejected:** adding the client now because the architecture diagram has Redis in
it. A dependency added before the measurement that justifies it is one nobody
ever removes.

### D24. Idempotency keys, and saga instances, are scoped to an authenticated principal

**Decided, resolving the gap this entry carried since Phase 3.** The gap's own
text specified the fix in advance — a composite primary key of
`(principal, key)`, so a cross-tenant probe cannot even observe that a key
exists — and that is exactly what shipped, once there was a principal to key
by.

**`internal/auth` and `api_keys`.** A new, deliberately small package and
table: server-generated secrets (`auth.GenerateKey`, 256 bits, prefixed
`lk_live_` so a leaked key is recognisable at a glance), stored only as a
SHA-256 hash — the same reasoning `idempotency_keys.request_fingerprint`
already uses, for the same reason: a database that is read (a backup, a
replica, a careless `SELECT *`) must not hand out a working credential.
Callers present `Authorization: Bearer <key>`; `requireAuth`
(`internal/http/auth.go`) resolves it to a `principalID` and makes it available
to everything downstream. This is what the gap's own text rejected a
placeholder for: a value the caller cannot merely assert, because possessing it
requires a secret this service issued and never stored.

**What this package deliberately is not.** There is no expiry, no scope, no
rate limit, and the only mutation `auth.Store` exposes is `Issue` — no
revocation-by-API, no listing, no rotation. `cmd/issue-api-key` is the entire
provisioning surface, a one-shot CLI in the shape of `migrate` and
`kafka-init`, because those are real admin-surface features that belong with
the admin dashboard when it exists, and building them here would be Phase 6
work wearing a Phase 3 bugfix's clothes. Revocation itself works today — flip
`api_keys.status` to `'REVOKED'` — it is only the *API* for doing so that is
absent.

**The composite key, exactly as specified, plus one place the original
analysis did not reach.** `idempotency_keys`' primary key is now
`(principal_id, key)` (migration `000016_authentication`). But D20 — a
different, separately-decided entry — documents `transactions_idempotency_key_key`
as a **third, independent** defence of invariant 5, "the one that would still
be standing if the other two were deleted." Scoping `idempotency_keys` alone
and leaving that constraint global would still let two different principals
collide there: principal B submitting principal A's exact key would pass B's
own, now-scoped reservation, then fail at `transactions_idempotency_key_key`
with "already exists" — the identical existence leak, one layer down. This
migration scopes that constraint too, by the same mechanism, plus
`saga_instances_idempotency_key_key` for the saga-level dedupe D24's own text
already named as inheriting the gap. All three tables now carry a
`principal_id` column and a composite unique constraint over it.

**Why `NOT NULL DEFAULT ''`, not a nullable column.** Postgres never treats two
NULLs as equal for uniqueness, so a nullable `principal_id` would silently
*exempt* every unauthenticated or pre-migration row from the composite
constraint — reopening precisely the hole this migration exists to close, for
exactly the rows most likely to still be colliding. `''` is a real,
comparable value: two legacy rows sharing a key still collide with each other,
which is correct, because before this migration they shared one namespace on
purpose.

**Rejected: composing a scoped string (`principal + ":" + key`) and storing it
in the existing single `key`/`idempotency_key` columns, with no schema
restructuring at all.** This was seriously considered — it is provably
unambiguous given `key` is always a canonical 36-character UUID, and it avoids
touching `transactions` and `saga_instances` at all. It was rejected because it
was not the design put to the user for approval, which showed real
`principal_id` columns and composite constraints, and because a real column
beats an encoded one on every axis that matters for a payments ledger:
`principal_id` is directly queryable ("every key this principal holds"),
directly indexable, and directly auditable, where a composed string requires
parsing to answer any of those questions. The smaller migration was available
and was not taken, on purpose.

**Verification.** `TestAPI_WriteRoutesRequireAuthentication` covers a missing
and an unrecognised key. `TestAPI_IdempotencyKeysAreScopedToThePrincipal` has
two real, distinct principals hold the identical key at once with different
bodies and asserts both succeed as two distinct transactions — the scenario
that previously hit `ErrIdempotencyConflict`. `TestAPI_ACallerCannotReplayAnothersResponseWithTheSameKey`
goes further: principal B sends A's exact key *and* A's exact body, and the
response must not carry `Idempotent-Replay` or A's transaction id — the leak
D24 named, stated as a byte-level assertion.
`TestSagaPayout_IdempotencyKeysAreScopedToThePrincipal` is the same proof at
the saga layer. `TestAuth_RevokedKeyStopsAuthenticating` and
`TestAuth_UnknownKeyDoesNotAuthenticate` cover the store directly.

**Confirmed against the running stack, not only the test suite.** Two real
principals (`local-dev`, seeded; `tenant-b`, issued live) posted transactions
under the identical `Idempotency-Key`, with different bodies, over HTTP against
the actual `docker compose` deployment: both returned `201` with two distinct
transaction ids. The same run also confirmed the down migration's own honesty —
having just made `transactions.idempotency_key` non-unique across principals on
purpose, `migrate down` on `000016` refused, loudly, exactly as its leading
comment says it will rather than silently collapsing two principals' history
into one row.

**What remains open.** There is still no authorization model — an
authenticated principal may read or write against *any* account, because this
schema has no notion of which accounts a principal may touch. D24 was
specifically about the idempotency namespace, and closing it did not require
solving that; building it here would again be Phase 6 work borrowing this
bugfix's authority. `GET /v1/sagas/{id}` also remains unauthenticated — its
exposure is a different, narrower gap (an unguessable-ID lookup, not an
idempotency-key collision) and is recorded separately below rather than folded
into a D24 fix it was never part of.

### D25. Sharding preserves invariant 4 and weakens liveness

**Decided.** An account may be split into N child accounts (migration 000012).
Writes route to a random shard; the logical balance is the `SUM` over shards.
Shards are ordinary rows in `accounts`, so the composite foreign key, the
deferred trigger, the overdraft `CHECK`, the balance-row trigger and D11's
ordered locking all keep working untouched.

**The invariant analysis, because it is subtle enough to get backwards.**
`account_balances_no_overdraft_check` is per row, so a debit routed to shard 7
is checked against shard 7, not the logical total. Therefore:

- **Safety is preserved.** Every shard is individually non-negative, so their sum
  is non-negative. The logical account cannot be overdrawn without overdrawing
  some shard first, and the constraint stops that. **Invariant 4 still holds.**
- **Liveness is weakened.** 800 spread as 100 across eight shards refuses a debit
  of 500 the account plainly holds. The check is conservative, not wrong.

Sharding therefore trades false refusals for throughput — the correct direction
for a ledger, since the failure is a refusal rather than a silent overdraft.

**Consequence, and the restriction it forces:** sharding is only correct on
accounts where a shard running dry is not a real outcome, meaning accounts whose
traffic is effectively one-directional — house floats, revenue, fee collection.
**A drainable customer wallet must not be sharded.** The database cannot check
traffic direction, so this is a policy the operator holds;
`TestSharding_CanRefuseADebitTheLogicalAccountCouldAfford` pins the refusal as a
known, reproducible property rather than something to be rediscovered from a
support ticket.

**Rejected:** a cross-row check summing the shards before permitting a debit.
`CHECK` cannot span rows, so it would have to be a trigger aggregating every
shard — which re-serialises exactly what sharding de-serialised, and deletes the
entire point.

**Deferred to Phase 4:** a rebalancer moving value between sibling shards as
ordinary internal transactions. It fixes the false refusal over time without
touching the hot path, and it is the reason the down migration refuses to move
balances itself — a ledger movement inside a migration is one with no journal
entries behind it, which breaks invariant 2 to tidy up a rollback.

**Reversal deliberately does not re-route.** The original's entries already name
the shards the money went to, so mirroring returns it to those same shards.
Routing a reversal afresh would pick at random and could drive one shard negative
while a sibling held the funds — turning a correction into an insufficient-funds
failure on an account that plainly has the money.

**Benchmark.** 32 writers × 8 posts into one logical account, PostgreSQL 16 in a
container on a developer laptop, three runs:

| Arm | Transactions | Elapsed | Throughput |
|---|---|---|---|
| Single account | 256 | 576–691 ms | **371–444 tx/s** |
| 16 shards | 256 | 130–158 ms | **1621–1965 tx/s** |

**4.4×–4.8×, not 16×, and the gap is the finding.** Sixteen shards do not buy
sixteen times the throughput because the row lock stops being the bottleneck
well before that: the 25-connection pool, WAL fsync and CPU take over. The
practical reading is that the first few shards recover most of the available
gain and the rest is largely wasted contention-management, so the sensible
default for a hot account is 4–8 rather than 16. Absolute figures describe this
laptop and should not be quoted; the ratio is the transferable result.

### D26. Retries are for two SQLSTATEs only — and this amends D10

**Decided.** `internal/db.Retrier` re-runs a transaction aborted with `40001`
(serialization failure) or `40P01` (deadlock detected). Five attempts, full
jitter, and the parent context bounds the whole sequence rather than each
attempt.

**D10 said there is no retry loop in the posting path.** That reasoning still
holds — READ COMMITTED plus ordered row locks converts contention into queueing
rather than aborts — and this does not weaken it. The loop exists because
"deadlocks cannot happen" is a claim about a lock ordering that a future write
path could break, and because the optional advisory locks introduce a second
lock space. **`ledger_db_tx_retries_total{sqlstate="40P01"}` staying at zero is a
continuous proof of D11 that no single test can give.** Measured, not assumed:
the hot-account contention test reports 0.0000% over 500 transactions.

**The exclusions are the important part**, and each is excluded for a different
reason:

- `context.DeadlineExceeded`, connection resets, and anything surfacing from
  COMMIT itself are **ambiguous** — the transaction may have committed while the
  answer was lost. Retrying an ambiguous write in a ledger is how money moves
  twice. This retrier must not manufacture the bug the idempotency key exists to
  catch.
- Domain errors are **deterministic**. `ErrBalanceVersionConflict` is the
  sharpest case: retrying it would re-read state a row lock was supposed to have
  protected.

Classification is on SQLSTATE alone, through `errors.As` so that pgledger's
wrapped errors still match. A type assertion would have disabled the whole
mechanism in production while passing every test written against a bare error.

**Full jitter, uniform over `[0, window)` with no floor.** When N writers are
aborted by one deadlock, a deterministic delay wakes them together and they
collide again — the retries reproduce the contention that caused them. A minimum
delay would resynchronise exactly the writers jitter is meant to separate.

**The transaction id is generated once, outside the loop, and reused.** A fresh
id per attempt is simpler and wrong in one specific way: if an attempt ever did
commit and was reported as aborted, a second id would post the money twice,
where a reused one collides on the primary key and says so.

### D27. Advisory locks are available, off by default, and honestly second-order

**Decided.** `pg_advisory_xact_lock(classid, objid)` per account behind
`LEDGER_LEDGER_ADVISORY_LOCKS`, taken before any row lock and in the same
ascending id order.

**What it actually buys, without overselling it.** With D11's ordered row
locking already in place, this does not remove contention — it moves where
writers queue. The gain is that they queue *earlier*: an advisory lock is a hash
table entry, so a blocked writer stops before touching the heap, before the
visibility checks on `accounts` and `account_balances`, and before the buffer
pins those take. On an extremely hot account that shortens the critical section
by the reads the losers no longer perform. Expect roughly neutral in the common
case; it exists for the pathological one and to be measured.

**Process-wide, never per request.** Advisory locks are a separate lock space, so
a deployment where only some write paths take them has two independent lock
orderings instead of D11's single global one — which is the shape a deadlock
needs. The flag is read once at startup.

**Batched rather than one statement over `unnest`.** Relying on the order in
which PostgreSQL evaluates a function across the rows of a projection is relying
on a plan shape, which is the same caveat D11 records about `ORDER BY`.
Statements in a batch execute in the order queued, which is a guarantee, and
they still cost one round trip.

### D28. `response_body` is `BYTEA`, not `JSONB`

**Decided**, in migration 000011, after the defect was caught by a test.

The API promises that a replay returns the stored response **byte for byte**.
`JSONB` cannot keep that promise, because it is a *parsed* representation rather
than a stored one: it reorders object keys into its own internal order, discards
insignificant whitespace, drops duplicate keys and normalises numbers. A
response round-tripped through it comes back as a different sequence of bytes.

The document is semantically identical, which is exactly why this is easy to
miss and unpleasant to find — `Content-Length` changes, any signature or ETag
over the body changes, and two callers holding what should be one answer can no
longer compare them.

`BYTEA` is the honest type for an opaque payload the database never looks
inside. Nothing queries into `response_body`; migration 000007 had already
declined to index it, so `JSONB`'s one advantage was never claimed while its
normalisation actively broke the guarantee.

**Worth recording how it was found**, because the weaker test would have
shipped it: `TestAPI_ReplayReturnsTheStoredResponseByteForByte` compares raw
bytes. The obvious version — unmarshal both and compare the objects — passes
against `JSONB`.

### D29. RFC 9457 problem details, a required key, and an opaque cursor

Three smaller API decisions, grouped because each is a rejection of the obvious
alternative.

**Errors are `application/problem+json`.** Rejected: inventing
`{"error": "..."}`. Error shapes are the part of an API clients hard-code most
and change least willingly, and RFC 9457 is the one shape a generic HTTP client
already understands. The `type` URI is the machine-readable discriminator;
`title` and `detail` are prose and may be reworded. `detail` is suppressed on
5xx, because a constraint or table name in a public error body is free
reconnaissance.

**`Idempotency-Key` is required on writes, not optional.** An optional key is one
a client forgets under precisely the conditions — timeouts, retries, a partial
outage — that make it matter, and the first time that happens it is a duplicate
payment rather than a lesson.

**The statement cursor is opaque.** Rejected: exposing `created_at` and `id` as
two query parameters. The position must be a pair because timestamps tie (D17),
and a client that could see both fields would eventually construct its own, get
the tie-breaking subtly wrong, and silently skip entries from a statement. An
opaque token makes the only supported cursor the one this service issued.

**The OpenAPI spec is checked in both directions by the suite** — `chi.Walk`
against the spec's paths, and the problem-type table against the spec's enum. A
specification nobody validates rots, and the drift is discovered by a client
integrating against a path that no longer exists.

### Phase 2 gaps, resolved

- A duplicate `transactions.idempotency_key` now maps to
  `ErrDuplicateIdempotencyKey` and a 409, instead of a wrapped
  `unique_violation`. See D20.
- `PostTransaction` has a retry loop after all, for the reasons and with the
  restrictions in D26. D10's consequence ("there is no retry loop in the posting
  path") is superseded.

### Known gaps carried into Phase 4

- **The client IP is still spoofable (D19).** Phase 3 was expected to settle
  this with the gateway design and did not, because the phase was scoped to the
  write path rather than to deployment topology. Nothing added in Phase 3 reads
  `r.RemoteAddr` — the idempotency, retry and sharding paths never consult it —
  so no new surface inherits the spoofability, but the gap is unchanged and now
  overdue.
- **The idempotency key namespace is global and unauthenticated (D24).** This is
  the one that must be closed before the service faces mutually untrusting
  callers.
- **Redis is unimplemented (D23).** The hit-rate counter exists; the client does
  not.
- **A shard rebalancer does not exist (D25).** Until it does, a sharded account
  can refuse a debit it could afford, and shard balances drift apart under
  one-directional traffic without ever converging.
- **`pending_minor` is still read and carried but never moved.** Holds and
  authorisations did not arrive in Phase 3.
- **The saga's write-header-first path can still reach `POSTED` with zero
  entries.** Closed on `PostTransaction`, open on the saga.
- **A reversal still fails if any account it touches has since been `FROZEN`.**
  Unchanged from Phase 2, and still arguable.
- **Shard accounts are created with `gen_random_uuid()` (v4), not v7.** A
  deliberate deviation from D3: shards are created by a rare admin operation on
  a small table, so index locality on `accounts` is not the concern that
  motivated v7 for `journal_entries`. Worth noting so the inconsistency is not
  read as an oversight.

---

## Phase 4 — Events out of Postgres, without losing or inventing one

### D30. The dual-write problem, and exactly what the outbox does and does not solve

**The problem, stated plainly.** A write path that must update Postgres and
publish to Kafka cannot do both atomically, because the two are different
systems with no shared transaction coordinator. Write Postgres first, then
Kafka: the process dies in the gap, the transaction commits, and no event is
ever published — a transfer moved real money and every downstream
consumer (the projector, a reconciliation job, a notification service) has no
idea it happened. Write Kafka first, then Postgres: the Postgres write then
fails or rolls back, and every consumer now believes a transfer happened that
the ledger itself will deny. Neither ordering closes the window, because
closing it requires exactly the thing that is missing — one atomic commit
spanning both systems, which does not exist for two independently-operated
stores.

**What the outbox actually does.** It deletes the second system from the
transaction. `outbox.Append` writes a row inside the *same* Postgres
transaction as the journal entries it describes (`internal/outbox/writer.go`);
one system, one commit, and invariant 6 holds by construction rather than by
discipline. What is left afterwards — getting a row that is durably committed
in Postgres onto Kafka — is a **replication** problem, not a dual-write one.
Replication can retry indefinitely against its own bookkeeping (a
`published_at` column, or Debezium's replication-slot LSN) without ever putting
the ledger's own consistency at risk, because the ledger's own consistency was
already settled the moment the outbox row committed.

**What it does not solve, stated exactly, because this is the part that gets
lost.** The gap between "the row committed in Postgres" and "the row's
publication is durably recorded" still exists — it has simply moved from
*across two systems* to *inside the publisher's own bookkeeping*. A publisher
(either implementation; see D31) can produce a message to Kafka, receive the
broker's acknowledgment, and crash before recording that fact. On restart it
finds the row still looking unpublished and produces it again. The outbox
therefore gives **at-least-once delivery, never exactly-once**, and this is not
a residual bug to be tolerated — it is the direct, permanent consequence of
choosing atomicity on the side that can actually have it (Postgres) over
atomicity on the side that cannot (a second system reached over a network).
**Every consumer of these events must be idempotent on `event_id`.** That
requirement is why `processed_events` exists in the projector (D33) rather than
a nice-to-have: without it, a duplicate delivery is a duplicate balance
mutation, which is exactly the failure invariant 6 was written to prevent one
layer up.

### D31. Two publishers, one interface, Debezium the default

**Decided.** `internal/outbox/publish.Publisher` is implemented twice —
`polling` (a Go process running `SELECT … FOR UPDATE SKIP LOCKED` and producing
via `franz-go`) and `debezium` (a status monitor over Kafka Connect's REST API;
the actual publishing is Debezium reading the write-ahead log, entirely outside
this codebase's control) — selected by `LEDGER_OUTBOX_PUBLISHER`. Debezium is
the default.

|  | Polling | Debezium CDC |
|---|---|---|
| **Latency** | Bounded below by the poll interval (200ms–1s) plus batch size; a real, constant floor even when idle. | Sub-second, typically tens of milliseconds — tailing the WAL as it is written, not waiting for a clock tick. |
| **Ordering** | `ORDER BY id`, i.e. *insertion* order. Insertion order is not commit order under concurrent transactions: a lower-`id` row can commit after a higher-`id` row from a faster concurrent transaction, so a narrow but real reordering window exists. | Strict LSN order. The WAL **is** the commit order — this is the one categorical advantage, and no amount of polling cleverness closes the insertion-vs-commit-order gap the polling arm has, because that gap is a property of `id` being assigned at insert rather than at commit. |
| **Crash behaviour** | Transaction-scoped and simple to reason about because it is code in this repository: row locks, the Kafka produce, and the `published_at` UPDATE all happen inside one database transaction. A crash mid-publish leaves the row unlocked and unmarked; the next poll cycle re-selects and re-publishes it. | Debezium tracks its own position (the replication slot's LSN plus a Kafka Connect offset topic). A crash replays from the last committed offset — the same at-least-once guarantee, but the recovery mechanism lives inside Kafka Connect rather than in a table this repository can `SELECT` from. |
| **Operational complexity** | One more Go binary, run alone; no new infrastructure. | A Kafka Connect cluster; a replication slot that **must** be monitored, because an abandoned slot means the WAL is never recycled — a runaway disk-fill failure mode with a delayed, ugly blast radius; connector JSON to version and deploy; SMT configuration. Materially more moving parts. |
| **Scaling out** | `SKIP LOCKED` (below) makes N replicas trivial and stateless. | One task per connector by default for a single-table source; scaling is a Kafka Connect exercise, not "start another process." |
| **Cost to the write path** | Row locks held across a network call (the Kafka produce), bounded by a small batch size and a context deadline — a real anti-pattern in general, kept narrow here on purpose. | None. WAL reading is fully decoupled from the write path; no lock is ever taken by the connector. |

**Why `FOR UPDATE SKIP LOCKED` is essential rather than a tuning knob.** Without
it, a second polling replica's `SELECT … FOR UPDATE` **blocks** on any row the
first replica has already locked, until the first replica's transaction
commits. Every replica beyond the first then adds queueing rather than
throughput — N replicas would have the effective concurrency of one, while
paying for N. `SKIP LOCKED` makes a locked row invisible to a second
transaction instead of a blocking one: replica B simply skips what A is
holding and claims a *different* batch of up to 100 rows. N replicas therefore
partition the backlog with zero coordination — no leader election, no assigned
shard, nothing to rebalance when a replica dies mid-batch. That is the entire
mechanism, and it is why this is the standard shape for a competing-consumers
polling publisher rather than an incidental detail of the query.

**Decided: Debezium is the default.** The deciding factor is not latency or
operational cost — it is that LSN-ordered delivery is a correctness property
the polling arm cannot match without independently reconstructing what the WAL
already gives for free (a commit-ordered sequence, which is exactly the
unsolved problem D16/D17 named for temporal balance queries in Phase 2).
Polling is not a lesser fallback, though: it is the arm that makes the
crash-recovery test in `TestOutbox_PollingPublisher_CrashBetweenPublishAndMark`
possible to drive deterministically, because it is a process this repository
starts and stops on purpose — interrupting Debezium's internal offset commit at
a chosen instant is not something a test can arrange from outside Kafka
Connect.

**Consequence: the wire format cannot depend on which publisher wrote it.** For
the config flag to be a real choice rather than a de facto one, both arms must
produce equivalent Kafka messages. That is why the full event envelope
(`event_id`, `event_type`, `event_version`, `aggregate_id`, `occurred_at`,
`trace_id`, `payload`) is assembled once, in Go, and stored as the entirety of
`outbox.payload` — see D32. The Debezium connector's job shrinks to "put the
`payload` column on Kafka verbatim," which is the only shape in which its
output and the polling publisher's output are indistinguishable to a consumer.

---

## Phase 4 continued — the event schema, ordering, and the projector

### D32. The event envelope, event types, and what "partition key = account_id" actually guarantees

**Decided.** Every event carries `event_id` (UUIDv7), `event_type`, `event_version`
(int, replacing the `.v1`-suffix scheme Phase 3 used — see D31's opening), `aggregate_id`,
`occurred_at`, `trace_id`, and `payload`, assembled once in `outbox.Append` and stored as
the *entirety* of `outbox.payload`.

Five event types: `TransactionPosted`, `TransactionReversed`, `AccountCreated`,
`BalanceUpdated`, `SagaStepCompleted` (declared; the saga orchestrator that would emit it
does not exist yet — Phase 1's `internal/saga` package is still a reserved stub).

**The keying decision, stated precisely, because it is the one place this design could
have quietly been wrong.** A transaction inherently touches two or more accounts
(double-entry), so it cannot be honestly keyed by a single `account_id` — the alternative
would be picking one account arbitrarily (misleading) or fanning one domain fact out into
several outbox rows (multiplies writes for one event). So:

- `TransactionPosted` / `TransactionReversed` land on `ledger.events.transaction`, keyed by
  **transaction_id**.
- `BalanceUpdated` — new this phase, one emitted per account a transaction actually
  touched, carrying that account's *resulting* balance and version (a set, not a delta,
  for the same reason `eventBalance` already worked this way inside `transactionEvent`) —
  lands on `ledger.events.account`, keyed by **account_id**. This is where "partition key
  = account_id" actually applies.

**What that keying guarantees, precisely:** every event that ever mentions a given
account — across every transaction that has touched it, from the beginning of that
account's history — is delivered to one consumer, in the same order those rows committed
(WAL/LSN order under Debezium; see D31). True per-account ordering, for free.

**What it does not guarantee, and this is the part worth being explicit about:** a single
transfer's debit-side and credit-side `BalanceUpdated` events land on **two different
accounts' partitions**, with **no ordering relationship between them**. A consumer that
has processed the debit cannot assume the credit has arrived, or vice versa. Anything that
needs both sides visible together — reconstructing one transfer's net effect, say — cannot
get that from Kafka partition ordering alone and must join on `aggregate_id`
(`transaction_id`, carried inside `BalanceUpdated`'s payload) or read `TransactionPosted`
instead, which carries every touched account's resulting balance in one message.

**Why the projector consumes `TransactionPosted`, not `BalanceUpdated`, and why that
sidesteps the gap above entirely:** because the payload carries the *resulting* balance
and version per account rather than a delta, applying it is a version compare-and-set —
`UPDATE ... WHERE version < $new`. That is correct regardless of arrival order or
redelivery, so the projector never needs the per-account ordering `BalanceUpdated`'s
keying provides. `BalanceUpdated` is kept anyway, for a different, real audience: a
lighter-weight, per-account consumer (cache invalidation, a notification service) that
wants the ordering guarantee and does not want the whole transaction shape.

**`AccountCreated` needed a service method that did not exist.** Before this phase,
accounts only ever came from seed SQL or raw migrations — there was no service-layer
creation path to hang an event off of. `ledger.Service.CreateAccount` was added, mirroring
`PostTransaction`'s shape (insert the row, append the event, one transaction). A DB
trigger mirroring `account_balances`'s autocreate trigger (migration 000009) was the
alternative and was rejected: a trigger cannot see `trace_id` or span context, and every
other event-emitting path in this codebase deliberately keeps that logic in Go, not SQL.

### D33. `processed_events` alongside the version compare-and-set, not instead of it

**Decided.** `internal/projector.Applier.Apply` checks `processed_events` (INSERT ... ON
CONFLICT DO NOTHING) and applies the version-CAS to `balance_projections`, both inside one
local Postgres transaction.

**Why the CAS alone is not enough**, stated exactly, because the two are easy to conflate:
the CAS makes *re-applying an event already seen* a no-op — the incoming version is not
greater than the stored one, so the UPDATE's WHERE clause matches nothing. It says nothing
about the window between that local transaction committing and this consumer's Kafka
offset commit succeeding. A crash in that window means the *next* delivery of the same
message re-enters `Apply` with an event the CAS has already silently absorbed — harmless
for the CAS itself, but there is then no durable record that the event was ever
definitively handled, which is the seam any future side effect of applying an event (a
notification, an exactly-once-intent metric) would need. `processed_events`, checked and
written in the same transaction as the projection update, is what makes "have I handled
event_id X" an answerable fact rather than an inference from projection state — identical
reasoning to `idempotency_keys` on the write path in Phase 3, transplanted to the read
side.

**Offsets are committed manually, and only after that local transaction commits — never
before.** `kgo.DisableAutoCommit()` plus an explicit `CommitRecords` call after `Apply`
succeeds. Committing first and applying second would mean a crash between the two loses
the event with Kafka believing it was consumed — silently dropping exactly the event
at-least-once delivery is supposed never to drop.

**A batch stops, rather than skips, at the first transient failure.** `processBatch`
commits everything successfully applied before a failing record and then returns without
touching the rest of the batch. Committing offset N+1 while N failed (say, Postgres
briefly unreachable) would let a restart resume past a message never actually applied,
turning a transient outage into a silent gap. The unprocessed remainder is simply
re-fetched next poll.

### D34. The dead-letter topic, and what does and does not reach it

**Decided.** One shared `ledger.events.dlq`, fed from two independent sources: Kafka
Connect's own `errors.deadletterqueue` (a message the SMT or converter could not process
at all — a connector-internal failure) and the projector's own consumer-side DLQ (a
message that parsed fine but this build does not know how to apply — currently only
`ErrUnknownEventType`).

**One topic, not one per source.** The operational question during an incident is always
the same — "what's in the DLQ and why" — and a replay procedure that has to check two
places is one that gets one of them forgotten under pressure. Each record carries headers
naming its origin (`dlq.source_topic`, `dlq.source_partition`, `dlq.source_offset`,
`dlq.error`) so provenance survives being merged into one topic.

**What reaches the DLQ, and what does not.** `ErrUnknownEventType` does — a future event
type this build has not been taught yet is a deployment-ordering fact, not a poison
message, and belongs on the DLQ so it can be replayed once the consumer understands it. A
transient failure applying a *known* event type (Postgres briefly unreachable) does
**not** — that is exactly what the "stop the batch, don't commit" behaviour in D33 is for,
and routing it to the DLQ instead would make a temporary outage into a permanent,
manually-triaged backlog for no reason.

**Replay procedure.** `cmd/projector` gained no `-dlq-replay` mode this phase — the DLQ
records carry everything needed (the original envelope as the message value, source
topic/partition/offset as headers) to re-produce them onto their original topic by hand
via `rpk topic produce` once whatever was wrong is fixed, and that manual step is
deliberately not automated yet: an operator should look at *why* a message reached the DLQ
before deciding it is safe to replay, and a one-command replay tool makes it too easy to
skip that look. Automating it is reasonable future work once there is a real incident's
worth of experience about what actually lands there.

### D35. Deployment corrections found only by actually running the stack

Three real defects surfaced only once the full `docker compose up` stack was exercised
end to end, none of them caught by any unit or integration test, because none of them were
in Go code at all.

**The Debezium connector's own config validation requires
`topic.creation.default.replication.factor` and `topic.creation.default.partitions` to be
*present*, even with `topic.creation.enable: false`.** Removing them (on the reasoning
that they are meaningless once creation is disabled) produced
`ConfigException: Missing required configuration ... which has no default value`, and the
connector never started. They are back, set to `1`/`1` — inert in practice, since
`kafka-init` provisions every real topic with its actual configuration *before* Connect
ever starts (D31's provisioning ordering), so Connect's own topic-creation path never
fires. The values only exist to satisfy a config validator that does not know they are
unreachable.

**`cmd/outbox-publisher` and `cmd/projector` do not open a second HTTP listener.** Both
follow the existing `cmd/reconciler` pattern from Phase 1 — one admin server, serving
`/healthz`, `/readyz`, and `/metrics` together on `LEDGER_METRICS_ADDR`, with no separate
`LEDGER_HTTP_ADDR` listener at all, because neither process has a public API. The first
`docker-compose.yml` draft for this phase copied `cmd/api`'s two-port shape (app port +
metrics port) without checking that assumption, publishing a port nothing was listening on
and pointing both healthchecks at it — so both containers reported "Started" while their
actual health endpoint (on the metrics port) was never checked, and a real client hitting
the documented app port got connection-reset. Caught by curling every service's health
endpoint by hand against the running stack, not by any automated check; the compose file
and the `make up` output now name the correct, single port for each.

**`docker compose run <service> /usr/local/bin/service -flags` re-executes the entrypoint
binary as its own first argument.** The image's `ENTRYPOINT` already is that binary,
so `docker compose run` appends whatever command is given as its `argv` — passing the
binary path again makes it `argv[0]`'s *value*, i.e. Go's `flag.Parse()` sees a non-flag
string in position zero of `os.Args[1:]` and — because `flag` stops looking for flags at
the first positional argument — silently ignores every flag after it. `make rebuild` ran
the long-lived Kafka consumer instead of the one-shot rebuild-and-diff, with no error of
any kind, discovered only by watching its logs print ordinary consumer output instead of a
comparison report. Fixed by dropping the redundant binary path from both the ad hoc
invocation and the Makefile target; the Makefile now carries a comment stating exactly why
that argument must never come back.

**The throughline.** All three passed `go build`, `go vet`, `golangci-lint`, and the full
test suite under `-race`. None of them are things a unit test or an integration test
against a single package could have caught, because each is a fact about how several
processes are wired together at the deployment layer — which is exactly why CLAUDE.md's
Definition of Done requires `docker compose up` to actually be run, not merely trusted to
work because the code that runs inside it does.

### D36. Two things learned from writing the failure tests, worth keeping on record

**franz-go's idempotent producer (the client-side default) will not honour
`RecordDeliveryTimeout` for a record that was already sent and got no response.** Its own
doc comment says as much: the timeout is only enforced when doing so is "safe" — a record
never issued, or one issued that received a *response*. A record in flight when the
connection dies mid-request is neither, because the client cannot tell whether the broker
received it, and enforcing the timeout there could create a gap in the idempotent sequence
number. So it waits forever instead, by design.

This is the right default for a producer whose job is exactly-once-per-partition
delivery. It is not this publisher's job — the polling publisher is deliberately
at-least-once (D30) — so idempotent production was never buying anything here, and
`internal/outbox/publish/polling`'s test client disables it
(`kgo.DisableIdempotentWrite()`) so the delivery timeout can actually fire during the
outage test. Found by that test hanging for its full timeout before the option was added,
not by reading the documentation first. Worth knowing before tuning
`RecordDeliveryTimeout` for `cmd/outbox-publisher`'s real client in production: the 30s
bound set there is real for connection-refused and broker-error cases, and not an absolute
guarantee against a connection that dies silently mid-request without a TCP reset.

**`container.Stop()` + `container.Start()` does not cleanly restart the Testcontainers
Redpanda module.** That module ships a custom entrypoint
(`mounts/entrypoint-tc.sh`) that waits for its node configuration to be written by a
lifecycle hook Testcontainers runs only as part of the original `Run()` call.
`container.Start()` on an already-created container re-executes that entrypoint from
scratch without re-running the hook, so it waits for a signal that will never come again —
the container reports `running`, but the Redpanda process inside it never actually starts.
`TestOutboxPublish_KafkaOutage` used `docker pause`/`docker unpause` instead (reached via
`os/exec`, since the `testcontainers.Container` interface exposes neither): pausing
freezes the *already-running* process via the kernel cgroup freezer rather than restarting
it, so every connection to it goes genuinely unresponsive — a faithful simulation of an
outage — with no startup sequence to break. Recorded here because the failure mode (a
container silently never coming back, discovered only by a five-minute test timeout) would
otherwise cost someone else the same hour it cost here.

---

## Phase 5 — Money that moves through a system we do not control

### D37. Orchestration, not choreography

**Decided.** One component owns the payout state machine, reads its own progress
from `saga_instances`, and calls the ledger and the gateway itself.
`internal/saga/payout.Orchestrator` is that component; the participants know
nothing about each other.

**Rejected: choreography** — each step emitting an event that triggers the next,
with no central coordinator. It is the more fashionable shape and it is genuinely
better at one thing: adding a participant costs nobody a code change.

**Why it loses here, and the reason is specific rather than general.** The
hardest question this saga ever has to answer is *"what happened to the gateway
call?"* — and answering it requires knowing three things at once: that a call was
made, which idempotency key it was made under, and what the saga would do with
each possible answer. In a choreography, those three facts live in three
different services and none of them is responsible for reconciling them. The
recovery logic has to exist somewhere regardless; choreography does not remove
it, it spreads it out and then makes it nobody's job.

The second reason is diagnostic. In an orchestration, `SELECT status FROM
saga_instances WHERE id = $1` is a complete answer to "where is this payout".
Under choreography, that question is answered by replaying several topics and
inferring — during an incident, about a customer's money, under time pressure.

**Cost accepted:** the orchestrator is a coupling point and a place a bug stops
everything. Mitigated by it being stateless, horizontally scalable with no leader
election (D40), and by the state machine being data in a table rather than
control flow in a process.

**What is not given up:** the saga still *emits* `SagaStepCompleted` to
`ledger.events.saga`. Anything that wants to react to a payout — notifications,
analytics, a merchant-facing webhook — subscribes without the orchestrator
knowing. Orchestration governs the money; events broadcast the news. The two are
not in tension, and conflating them is the usual reason this decision gets made
badly.

### D38. Sagas give ACD but not I, and the suspense account is the mitigation

**The property, stated exactly.** A saga is Atomic (compensation drives it to
all-or-nothing), Consistent (every step is itself a balanced, constraint-checked
transaction — invariants 1 through 4 hold at every instant) and Durable (state is
in Postgres). It is **not Isolated**: `RESERVE` commits, and its effects are
visible to everyone while the gateway call is still outstanding. There is no
snapshot hiding the intermediate state, because there is no enclosing transaction
to take one. Every reader sees a customer's wallet debited for a payout that has
not yet been made and might never be.

**This is not fixable, only mitigated.** Isolation across a network call to
another company would require holding a database transaction open across that
call — locks held for the duration of somebody else's outage — which is the
failure mode sagas exist to escape. The dirty read is the price of not doing
that.

**What the pending-balance pattern actually buys.** The mitigation is not to hide
the intermediate state but to make it **self-describing**. Between `RESERVE` and
`SETTLE` the money sits in `payout-suspense`, a real account with a real balance
whose name says what it is, and `pending_minor` on that account says how much of
it belongs to sagas that have not finished. A reader is never misled; they are
told, in the ledger's own vocabulary, that this value is in flight.

Compare the alternative the requirement's wording suggests (D39): a hold recorded
only against the wallet, where the intermediate state is "the wallet says 30000
but 5000 of it is spoken for" — a number every reader must remember to subtract,
and which every report that forgets will get wrong.

**The second thing it buys is a reconciliation invariant**, in the spirit of D1's
three independently derived balances:

```
suspense.pending_minor  ==  SUM(amount) over saga_instances in non-terminal states
```

Two numbers computed by different mechanisms — one by `ApplyPendingDelta` inside
each ledger transaction, one by counting rows in a different table — that must
agree. Disagreement localises the bug immediately.

**Being precise about what does NOT prevent double-spending.** See D39. It is not
`pending_minor`.

### D39. The suspense debit is the semantic lock; `pending_minor` is not

**Decided.** `RESERVE` posts real journal entries: DEBIT customer wallet, CREDIT
platform suspense. The money genuinely leaves the wallet.

**The trap this avoids, stated plainly**, because the requirement as originally
worded ("funds are held in `pending_minor` so the user cannot double-spend")
describes something that does not work against this schema.
`account_balances_no_overdraft_check` is:

```sql
CHECK (allow_negative OR available_minor >= 0)
```

It **does not mention `pending_minor`**. A hold written only into that column is
invisible to the constraint, so a concurrent debit sails straight past it. The
hold would look like a lock, be described in the code as a lock, and stop
nothing.

**So the guard is the debit itself.** Once the wallet is debited, a second payout
against the same wallet is refused by invariant 4's existing `CHECK` under the
existing `SELECT … FOR UPDATE` — the ordinary write path's protection, already
proven under 100 concurrent writers in Phase 2, and reused here without
modification. `TestSagaPayout_ConcurrentSagasOnOneWalletCannotDoubleSpend` runs
100 payouts against a wallet that can afford 40 and asserts exactly 40 succeed.

**Rejected: an authorization hold, and strengthening the CHECK to
`available_minor - pending_minor >= 0`.** This is the card-network shape — auth
then capture — and it is a legitimate design. It was rejected for two reasons.
It would rewrite invariant 4, which CLAUDE.md declares non-negotiable (the
rewrite is *strictly stronger*, so it would have been safe, but "safe" is not the
bar for changing a stated invariant). And it removes the platform-suspense leg
entirely: with no journal entries at `RESERVE`, compensation becomes a hold
release with nothing to reverse — which is tidier, and which would have made the
compensating-transaction machinery this phase exists to build almost vacuous.

**Consequence, and it is the argument that settles it:** because the money is in
a named account rather than merely flagged, an unresolved gateway outcome is
*survivable*. The funds are debited from the customer and not credited to the
merchant — owned by nobody, lost by nobody — for as long as the ambiguity lasts.
Under an auth-hold the equivalent state is a hold on a live wallet that a user is
still spending against, and the longer it lasts the more it is in the way.

### D40. The orchestrator drives itself from Postgres, not from Kafka

**Decided, and it is a deliberate deviation from CLAUDE.md's architecture
sketch**, which shows `Saga Orchestrator <- Kafka`. Recorded here rather than by
quietly editing the diagram.

`cmd/saga-orchestrator` claims work with `UPDATE … WHERE id IN (SELECT … FOR
UPDATE SKIP LOCKED)` against `saga_instances`, exactly the competing-consumers
shape D31 chose for the polling publisher: N replicas partition the backlog with
no leader election, no assigned shard, and nothing to rebalance when one dies
mid-batch.

**Why not Kafka-driven.** An orchestrator whose state machine is advanced by
consuming events has its state split across two systems, and can no longer answer
"what step is this saga on" without replaying a topic. It also inherits
at-least-once redelivery as a *correctness* concern on the state machine rather
than merely on the read model — and it is halfway to the choreography D37
rejected: a component reacting to events about itself is not obviously in charge
of anything. The timeout sweeper needs a Postgres scan regardless (a message that
never arrives generates no event), so the Kafka path would have been a second
mechanism alongside a first, not instead of it.

**Claims are scoped by `saga_type`.** Without the scope, two orchestrators
deployed side by side each spend their claim budget taking the other's sagas
hostage for a lease at a time. With it, an orchestrator only ever leases work it
can actually do.

### D41. A saga step and the money it describes commit together

**Decided.** `ledger.Tx` gained `CommitSagaStep` and `ApplyPendingDelta`, and
`ledger.TransactionRequest` gained a `Record` hook. A forward step's journal
entries, its balance updates, its `pending_minor` movement, its saga status
transition, its audit row, and its outbox event are **one COMMIT**.

**This is D20's argument, transplanted and with higher stakes.** That entry
rejected "reserve, work, complete" as three transactions because the work commits,
the bookkeeping does not, and the resumed process re-runs it. On the write path
the cost was a duplicated response. Here the step being re-run is *a debit against
a customer's wallet*.

**Rejected: drive through `Service.PostTransaction` and update saga state in a
second transaction**, treating `ErrDuplicateIdempotencyKey` on resume as "this
step already ran". It requires no change to `internal/ledger` at all, which is a
real argument in favour of it — that package is the crown jewels. It was rejected
because it is precisely the shape D20 identified as the bug, rescued only by the
unique index; because resume would have to reverse-look-up a transaction by key
to recover its id; and because it cannot move `pending_minor` regardless, so a
ledger change was unavoidable either way.

**Rejected: exposing the raw `pgx.Tx` through a `Raw()` accessor**, which would
have been one line instead of two delegations. It would let any future caller take
locks outside the single global ordering D11 depends on — the one thing
`pgledger`'s package doc says the package exists to prevent. The two new methods
follow `AppendEvent → outbox.Append` verbatim: a package-level function taking
`pgx.Tx`, delegated from `*txn`, with the statements living beside the table they
own.

**Consequence: the saga never writes a `PENDING` transaction header.** `RESERVE`
and `SETTLE` are each complete, balanced, immediately-`POSTED` transactions, and
in-flight-ness lives in `saga_instances` instead. **This closes the gap carried
since Phase 1** — "the saga's write-header-first path can still reach POSTED with
zero entries" — by never taking that path. `transactions` needed no migration and
`ErrTransactionNotPending` remains unreferenced.

**Consequence: `status` only ever holds settled states.** There is no
`RESERVING`/`SETTLING`. In-flight-ness is a lease, exactly as D20 uses
`IN_PROGRESS` plus `lease_expires_at`, and it rests on the same property: a lease
that has lapsed is proof its owner committed nothing.

### D42. An unknown gateway outcome is resolved by asking, never by assuming

**The situation.** The gateway is the one participant outside the database
transaction, so it is the only one whose outcome can be genuinely unknown. A
timeout, a severed connection and an orchestrator crash mid-call are
**indistinguishable from this side**, and all three mean the same thing: a payment
may or may not exist.

**Decided.** The saga does nothing. It sits in `GATEWAY_PENDING` and the sweeper
issues `GET /v1/payments/{key}` until it gets a conclusive answer or gives up.

**The two tempting alternatives are each wrong in an expensive direction.**
Assuming failure and compensating refunds a customer whose money really left, and
the merchant is never paid. Assuming success and settling pays a merchant for a
payment that never happened, out of the platform's own funds. Doing nothing is
the only action that cannot create a discrepancy — and it is affordable precisely
because the money is parked in suspense (D39) rather than in limbo.

**Three mechanisms make asking possible**, and all three are load-bearing:

1. **The key is a pure function of the saga id** — `<saga_id>:GATEWAY`, never the
   attempt number. It survives a crash that loses everything in memory, because
   it can be recomputed from the saga's own identity. Deriving it per-attempt is
   the single most expensive bug available in this design: every retry becomes a
   fresh charge.
2. **The intent is committed before the call.** `BeginStep` writes the
   `ATTEMPTED` row and the move to `GATEWAY_PENDING` in one transaction, and only
   then does the HTTP request go out. Reversing that order loses the record in
   exactly the crash that makes it necessary.
3. **Resolution is a GET.** Re-POSTing would also be safe — the key is stable and
   the gateway is idempotent — but only because of a property of *someone else's
   system*. Staking a customer's money on another company's correctness when you
   do not have to is a bad trade.

**The assumption this encodes, and it is not free.** A probe returning 404 is
treated as conclusive evidence that no payment was made, and the saga
compensates. That is sound only while the gateway's record of accepted payments
is durable and complete. If it accepted a payment and then lost the record, this
answer is a lie and a customer is refunded money that really left. Real gateways
provide that durability and every payments reconciliation rests on it. It is
written down here so that the day one is found not to, the consequence is already
on record rather than rediscovered from a support ticket. The sentinel's doc
comment says the same thing at the point of use.

**Giving up is a real outcome.** After `LEDGER_GATEWAY_MAX_PROBES` inconclusive
probes the saga goes to `NEEDS_MANUAL_REVIEW`. That does not resolve the
ambiguity — nothing available here can — it hands it to a human with the money
still held and every attempt on the record.

### D43. Why an exhausted compensation must not be resolved automatically

**Decided.** `NEEDS_MANUAL_REVIEW` is terminal for automation. The orchestrator
stops, the funds stay in the suspense account, `SagaNeedsManualReview` goes to
`ledger.events.saga`, `ledger_saga_manual_review_total` increments, an ERROR line
is logged with the saga id and the last error, and
`GET /v1/sagas?status=NEEDS_MANUAL_REVIEW` lists it.

**The argument, since this is the decision most likely to be second-guessed by
somebody looking at a stuck-saga dashboard.** A compensation that has burned its
whole budget failed for a reason the orchestrator does not understand. Either it
was transient — and the retries already covered that — or it is *semantically
impossible*: the account is frozen, the suspense funds were moved by another
path, the reversal would drive an account negative. In the second case no number
of further retries helps, and the only "automatic resolutions" actually available
are force-posting with `allow_negative` or writing a balancing `ADJUSTMENT`.

Both of those **mint money that no business event justifies**. And the deeper
cost is not the one transaction: a ledger that can silently repair itself is a
ledger whose balances are no longer evidence of anything, because any figure
might be a real movement or might be automation papering over something it did
not understand. The auditability that justifies double-entry in the first place
is exactly what self-healing spends.

So the system stops in a state that is *wrong but named*: the money is in
`payout-suspense`, the amount is exact, `pending_minor` says it is unfinished,
every attempt and its error are in `saga_steps`, and a person decides. Worse for
the dashboard, better for the ledger.

**Being stuck is also not silent, which is the other half of the requirement.**
Three independent channels carry it — an event, a metric, and a log line — so
losing any one of them does not lose the alert. The status is terminal *for the
sweeper* specifically so that a saga a human has been paged about does not
generate a fresh page every ten seconds and bury the entry they are reading.

**Compensations are idempotent, and the mechanism is inherited rather than
invented.** A compensation is a `REVERSAL`, guarded by D15's
`UPDATE … WHERE id = $1 AND status = 'POSTED'`; a second one matches nothing and
gets `ErrAlreadyReversed`. The saga transition is independently guarded on
`status = 'COMPENSATING'`. Either guard alone would prevent the money being
returned twice — which matters, because two reversals would each balance
perfectly and the balance invariant would never notice.

**The retry budgets are deliberately asymmetric.** `MaxCompensationAttempts`
(8) exceeds `MaxStepAttempts` (5), and config validation refuses a deployment
where it does not. Abandoning a forward step costs nothing — the customer is
untouched. Abandoning a compensation strands real money. The two failures are not
comparable, so their budgets are not equal.

### D44. `ErrInsufficientFunds` is terminal for a saga and transient for an
### idempotency key

**Decided.** `terminalLedgerError` fails a payout immediately on insufficient
funds, a frozen account, or a missing account.

**This deliberately disagrees with D22**, which classifies the same error as
*transient* and releases the idempotency key so a client can retry once the
account is funded. Both are right, because they are answering different
questions. An idempotency key represents *the client's intent*, which survives
waiting; burning it would force an honest client to mint a new key for what is,
to it, the same operation. A saga represents *an attempt to move a specific
amount now*, and a payout is not a standing order. Retrying it leaves sagas
circling for hours against wallets that are simply empty, holding leases and
claim budget the whole time.

Noting it explicitly because the two tables sit in the same codebase and look
like they disagree by accident.

### D45. The mock gateway is a real process that holds payments in memory

**Decided.** `cmd/mock-gateway` is a real HTTP server with a control plane
(`POST /control/behaviour`) for outcome, latency and two flavours of hang.

`.claude/rules/testing.md` requires failure tests to kill things rather than
simulate failure with a boolean flag, and names the gateway specifically. What
that forbids is a flag *inside the orchestrator* making it pretend a call failed
— a test of Go's error handling that says nothing about surviving a real one. The
orchestrator therefore has no test-only branch at all; the only test seam is
`WithCrashHook`, exported and deliberately separate from `Config` so it cannot be
set by ordinary configuration loading, following the polling publisher's
precedent.

**The two hang modes are separate because they leave the world in opposite states
while looking identical to the caller.** `HangBeforeRecording` means no payment
exists and compensating is correct. `HangAfterRecording` means the payment exists
and compensating would refund a customer whose money really left. A saga that
handles one but not the other is broken in the more expensive direction, and only
a probe can tell them apart — which is the whole of D42, made testable.

**Payments are held in memory on purpose.** Killing the process loses them, which
is the only faithful way to produce the genuinely unrecoverable case: a gateway
that cannot tell you what it did. That is what
`should need manual review when the gateway can never say what it did` kills the
listener to produce.

### D46. What running the stack found, and what it confirmed

Verified against `docker compose up` rather than trusted, per the Definition of
Done and for the reason D35 exists.

**A real payout completed end to end in ~550ms**, and the balances were exact:
wallet 50000 → 30000, merchant 19500, fee revenue 500, suspense back to 0 with no
residual hold. Both `SagaStepCompleted` events reached `ledger.events.saga`
through Debezium — the event type D32 declared in Phase 4 against an orchestrator
that did not exist, now actually emitted.

**A declining gateway compensated to the exact prior balance**, live, with the
audit log reading `RESERVE/FORWARD/SUCCEEDED`, `GATEWAY/FORWARD/FAILED`,
`RESERVE/COMPENSATION/SUCCEEDED`.

**`docker compose pause mock-gateway` mid-payout produced a genuine ambiguity**
and the saga resolved it correctly: it sat in `GATEWAY_PENDING` with 3000 held in
suspense and `pending_minor` at 3000, and on unpause the probe returned 404, the
saga compensated, and the wallet returned to exactly its prior balance. The
gateway was then queried directly to confirm the saga had not guessed: it held a
`SUCCEEDED` payment for the completed saga, a `FAILED` one for the declined saga,
and **no payment at all** under the paused saga's key. Compensating was the
correct answer and the saga reached it by asking.

**Two things worth recording for whoever runs this next.** `rpk topic consume`
without `--fetch-max-wait` blocks rather than returning what is there, which
looks exactly like an empty topic. And `published_at` staying NULL on saga outbox
rows is not a stalled publisher — D4 says nothing writes it on the Debezium happy
path — so backlog must be judged from the topic, not from that column.

### Known gaps carried into Phase 6

- **D19 and D24 are closed**, ahead of Phase 6 rather than as part of it — see
  D19 and D24 above for the fix, and D47 below for two gaps the fix
  deliberately left open rather than folding into a standalone security patch.
- **Redis is still unimplemented (D23).**
- **A shard rebalancer still does not exist (D25).** A sharded payout suspense
  account would be subject to the same false refusals; the seeded one is not
  sharded, and a drainable suspense account should not be.
- **`NEEDS_MANUAL_REVIEW` has no resolution path in the product.** An operator
  can see a stuck saga through `GET /v1/sagas` and must fix it with a manual
  `ADJUSTMENT` or reversal by hand. A guarded admin endpoint — "I have checked
  the gateway, record this saga as settled/compensated" — is real work and
  belongs with the admin surface, not bolted onto the orchestrator. Until it
  exists, the runbook is: query the gateway for `<saga_id>:GATEWAY`, then post
  the corresponding correction by hand and move the saga by SQL.
- **The saga has one definition.** `internal/saga` is deliberately generic and
  `internal/saga/payout` is the only implementation of it. The second saga is
  what will show whether that seam is in the right place; one implementation
  never does.
- **`SagaStepCompleted` has no consumer.** It is emitted and nothing reads it.
  That is the correct order to build it in, but it means the event's shape is
  unvalidated by any real subscriber.
- **The reconciliation invariant from D38 is documented, not enforced.** Nothing
  yet compares `suspense.pending_minor` against the sum of non-terminal sagas.
  It belongs in the reconciliation engine, which is still a Phase 1 skeleton.

---

## Standalone fix — D19 and D24, ahead of Phase 6

### D47. What closing D19 and D24 deliberately left open

Two gaps, named rather than folded silently into "done," per the same
discipline D19 and D24 themselves modeled: a gap that looks closed is worse
than one that is visibly open.

**There is still no authorization model.** `requireAuth` establishes *who* a
caller is; nothing establishes *what* that caller may touch. An authenticated
principal may read or post against any account in the schema, because
`accounts` has no notion of principal ownership distinct from `owner_id`
(free-text domain metadata, not a security boundary). D24 was scoped to the
idempotency-key collision specifically — one principal being handed another's
*stored response* — and closing that did not require solving general
authorization. Building it here would be exactly the trap D24's own original
text warned about: a placeholder that looks like a real boundary invites the
next reader to assume the problem is solved.

**`GET /v1/sagas/{id}` remains unauthenticated.** It was flagged as a
Phase-5-adjacent concern when D24 was first widened to include
`saga_instances`, and on inspection it is a **different** gap, not the one D24
names: this endpoint dedupes nothing and completes no idempotency key, so
there is no collision to scope. Its exposure is an unguessable-ID problem — a
UUIDv7 saga id is not secret, only hard to enumerate — the same category as
any resource addressed by primary key with no ownership check. It is left open
because authorization (the gap above) is what actually closes it, and a
narrower, saga-specific patch here would be one more placeholder to walk past
later.

**Why this was scoped as a standalone fix rather than the start of Phase 6.**
Both D19 and D24 were three phases overdue and named specific, exploitable
holes — a spoofable audit trail, a cross-tenant response leak — with proven
minimal fixes available (a config default, a composite key). The two gaps
above are neither: they are the honest boundary of what "authentication
exists" implies, not implies *authorization does*, and building the latter
needs the same design discussion CLAUDE.md asks for before any new subsystem —
which accounts a principal owns, whether ownership is single- or multi-tenant,
how it interacts with sharding's `parent_account_id`. That is real Phase 6
work, and this fix does not pre-empt it by picking an answer under the cover
of a security patch.

---

## Phase 6 — Proving the ledger correct, continuously

### D48. The reconciliation engine: what a three-way match actually checks, and why

**The gap every other invariant in this codebase leaves open.** Everything
before this phase checks the ledger against *itself* — the deferred trigger,
the overdraft `CHECK`, the projector's rebuild-and-diff against
`balance_projections` (D33, `make rebuild`). None of that can catch this
system agreeing with itself while disagreeing with the bank: a transaction the
gateway silently dropped, one it silently duplicated, an amount that drifted
in transit. `internal/reconciliation` and `cmd/reconciler` exist to find
exactly that, by matching three independently-maintained records on
`external_ref`: the ledger's own `transactions`, the saga orchestrator's
`saga_instances`, and a PSP settlement statement this service does not
control.

**Decided: the join is SQL, the classification is Go.** Assembling the three
sides by `external_ref` is a join, and it runs in one query
(`pgreconciliation.Repository.Match`) for the same reason
`internal/projector.Rebuild` already does its own comparison in SQL: loading a
day's transactions into Go to match by hand against a CSV a fraction of its
size would mean holding the larger side in memory to compare it against the
smaller one, for no benefit. Deciding *which category* a mismatch falls into,
and whether a timing gap is small enough to auto-resolve, is a business rule
with a configurable parameter (the timing window) — exactly the kind of
decision this codebase's layering (`handler -> service -> repository`) already
keeps out of SQL. `classify` (`internal/reconciliation/classify.go`) is the
one place that decision is made, and it is pure — no database, no clock
outside what it is handed — so every rule in the table below is pinned by
`TestClassify` without a container.

**What "the ledger's amount" means for one `external_ref`, and why it needed a
rule at all.** A single marketplace payout posts *two* transactions sharing
one `external_ref` — RESERVE (D39's suspense debit) and SETTLE (D41) — so
`external_ref` does not name a transaction uniquely. Two things were rejected
before the one that shipped:

- **Hard-code the payout shape** (read `metadata->>'saga_step'`, prefer
  `SETTLE`). Rejected because it makes this package know something about
  payouts specifically, and the whole point of keying on `external_ref` rather
  than on saga internals is that a second saga type — or a transaction posted
  outside any saga at all — should reconcile through the identical path.
- **Sum every transaction sharing the reference.** Rejected because RESERVE
  and SETTLE are not two payments to add together; they are the same payout's
  two ledger-internal legs, and summing them double-counts it.

**Decided: the *latest* transaction by `created_at`, and its amount is the sum
of its `DEBIT`-side entries.** "Latest" needs no knowledge of payouts —
"whatever the ledger most recently believes about this reference" is a
generically correct answer for any workflow that corrects itself with a
second transaction under one reference, and it happens to coincide with
SETTLE for a payout because SETTLE is created after RESERVE. The amount
definition is the more interesting piece: summing `DEBIT`-side entries is safe
for *any* balanced transaction, of *any* type, because invariant 1 already
guarantees debits equal credits per `(transaction_id, currency)` — that is not
a fact this package asserts, it is a fact the deferred trigger already
enforces on every row that exists. A reconciliation engine that had to be told
what "amount" means per transaction type would need updating every time a new
`transaction_type` shipped; this one does not.

**Decided: `Match` is bounded by a lookback window, not the whole table.**
Finding `MISSING_IN_PSP` references requires considering every ledger
transaction that carries an `external_ref`, not only the ones the statement
mentions — that is inherent to the check, not an implementation shortcut. Left
unbounded, every run would scan `transactions` in full, forever, for a table
this codebase otherwise never scans outside a migration. `since` (default
seven days, `LEDGER_RECONCILER_LOOKBACK`) keeps it a bounded scan against a
partial-index-backed column (`transactions (external_ref) WHERE NOT NULL`,
D1) instead.

**Decided: classification priority is DUPLICATE, then MISSING\_\*, then
AMOUNT_MISMATCH, then STATUS_MISMATCH, then TIMING_DIFFERENCE.** More than one
condition can describe the same reference, and a caller needs exactly one
category. DUPLICATE goes first because a PSP statement listing one reference
twice is a defect in the *statement*, independent of what the ledger says —
reporting it as an amount mismatch instead would send an operator
investigating the wrong system. AMOUNT_MISMATCH outranks STATUS_MISMATCH
because a transaction whose amount is simply wrong is the more actionable
defect, and a status disagreement alongside a wrong amount is very likely the
same underlying problem wearing two symptoms, not two problems.
TIMING_DIFFERENCE is last because it is the only category this function ever
resolves on the spot, and everything checked before it (amount, status) must
already agree for a timing gap to be the *only* thing wrong.

**Decided: only `TIMING_DIFFERENCE`, and only inside the window, auto-resolves
— and it is still recorded, as `AUTO_RESOLVED`, not silently dropped.** The
alternative — matching cleanly and writing nothing — was rejected because it
would make a run's exception count a lossy summary: an operator re-deriving
"how many references had *any* timing gap" from a report that only shows the
gaps that mattered would get it wrong. Recording the resolution and closing it
in the same motion is what keeps `exception_count` on `reconciliation_runs`
an honest total of everything the run found, not just what it left open.

**Rejected: an authorization hold on the reference used as a lock, mirroring
D39's own rejection.** Not applicable here — there is no write path in this
package at all. `Engine.Run` reads `transactions` and `saga_instances`; it
never posts, reverses, or updates either. That is also why several replicas of
`cmd/reconciler` running the identical statement concurrently is a nuisance
(duplicate runs, duplicate exception rows) rather than a correctness bug: two
reads racing each other cannot double-spend anything. Compare D40's
claim-based coordination for the saga orchestrator, which exists precisely
because *that* process moves money and this one does not.

**Decided: the report is read-only HTTP, and authenticated from day one.**
`GET /v1/reconciliation/runs` and `GET /v1/reconciliation/runs/{id}` require
`requireAuth`, unlike `GET /v1/sagas/{id}` — which D47 named as an open gap,
not a template to repeat. A reconciliation exception carries external
references and amounts, the same class of information D24 scoped idempotency
responses to a principal to protect; there was no reason to reopen that
question for a new endpoint when the answer was already settled.

**What remains open, carried forward rather than folded in here.** Scheduled
internal consistency checks (the global invariant page, projection-drift
detection beyond `make rebuild`'s manual invocation, orphan detection),
full OpenTelemetry/Kafka-header trace propagation, the fault-injection harness
and its chaos test, and the Grafana/Prometheus deployment itself are the rest
of Phase 6 and are not implemented by this entry — see the phase's own
tracking for status. `NEEDS_MANUAL_REVIEW`-style resolution for an `OPEN`
reconciliation exception also does not exist yet, for the identical reason
D43 gives for sagas: an admin surface for "I have investigated this, mark it
resolved" is real work belonging with the admin dashboard, not a field bolted
onto this phase's first cut.

### D49. The internal consistency checks: a query package, not a second store

**Decided.** `internal/consistency` adds three functions —
`CheckGlobalInvariant`, `CheckProjectionDrift`, `CheckOrphans` — each taking a
bare `*pgxpool.Pool` and returning a result struct. `cmd/reconciler` runs all
three on one ticker (`LEDGER_RECONCILER_CONSISTENCY_INTERVAL`, default one
minute) alongside the PSP match loop D48 already added, and turns the outcome
into a Prometheus gauge plus, on a violation, an `ERROR` log line.

**Rejected: the same `Store`/persisted-run shape D48 used for the PSP
match.** Seriously considered, since it is already built and already wired
into this exact process. Rejected because the two jobs answer different kinds
of question. A PSP mismatch is an audit finding — evidence someone reviews
later, with a lifecycle (`OPEN` → resolved) worth persisting and worth an API
to browse historically. A structural check is a yes/no question about the
ledger's own state *right now*: "does the journal still sum to zero" has no
useful history beyond its current answer, and a table of a year of "yes, it
balanced" rows would be pure write load with no reader. `internal/projector`'s
own `Rebuild` function already established the right shape for this — a plain
function over a pool, called from `make rebuild` and now from a ticker — and
these three functions follow it rather than reinventing D48's heavier
machinery for a job that does not need it.

**The three checks answer three different questions, and conflating any two
of them would quietly lose one:**

- `CheckGlobalInvariant` proves the deferred trigger (migration 000005) is
  still holding across the *entire* journal, not merely for the one
  transaction it last fired on. Grouped by currency, not summed globally —
  invariant 1 balances per `(transaction_id, currency)`, so a bug that is
  simultaneously +100 in one currency and −100 in another would net to zero
  and hide behind a single ungrouped total. Two unrelated defects must not be
  able to cancel each other out of the one check meant to catch either of
  them.
- `CheckProjectionDrift` recomputes every account's balance from
  `journal_entries` and diffs it against `account_balances` — the
  *synchronous* balance the write path updates under lock (D1). This is
  deliberately **not** the comparison `internal/projector.Rebuild` already
  makes, which diffs the same recomputation against `balance_projections`,
  the *Kafka-driven* read model. D1 names three independently derived
  balances specifically so any two agreeing while a third dissents localises
  the bug; Phase 4 built the async-pipeline leg of that triangle, and this is
  the write-path leg nothing before Phase 6 checked on a schedule. Running
  both is what actually closes D1's original design, not a duplicate of
  either alone.
- `CheckOrphans` proves two structural claims: no `POSTED` or `REVERSED`
  transaction has fewer than two entries (excluding `PENDING`, which is a
  legitimate transient state per Phase 1's own carried-forward gap — a saga
  writing a header before its legs), and no `journal_entries` row lacks a
  parent transaction. The second is unconstructible given the foreign key the
  table already carries, and is checked anyway: one query, and it is what
  would catch a future migration that weakened that constraint by mistake,
  which a schema-level promise nobody re-verifies at runtime cannot.

**Every result is capped at `maxReported` (200) offending rows, with the true
count carried alongside the capped list.** A check that found fifty thousand
violations has a much bigger problem than this process's memory or a log line
can usefully describe, and the failure mode to avoid is a check that makes
things worse by holding an unbounded result set during an incident that is
already resource-constrained. The true count is never lost — `TotalDrifted`,
`TotalFewEntry`, `TotalOrphanEntries` are separate fields from the capped
slices — so "3 accounts drifted" and "50,000 accounts drifted" stay
distinguishable even when only the first 200 are named.

**The gauge is `Reset()` before every tick, not merely overwritten.**
`GlobalInvariantViolation` is labelled by currency; without the reset, a
currency that stops violating would leave a stale nonzero series on the
dashboard forever, because nothing would ever write a `0` for a label that
has simply stopped being reported. `Reset` followed by setting only today's
actual violations makes "never seen" and "checked and clean" the same,
correct state: absent from the series entirely, which is also what makes the
Prometheus alert this sets up for (Phase 6's observability item, not yet
written) a simple `!= 0` rather than a stateful "was previously nonzero"
query.

**Verification had to defeat the very safeguards being verified, which is
why this test does not run against the shared suite database.**
`TestConsistency_Checks` proves each check actually *detects* a violation, not
merely that it reports clean against healthy data — and the only way to
produce a genuinely unbalanced journal or a genuinely drifted balance is to
write around the deferred trigger and the append-only trigger that make both
otherwise unreachable through any real write path. Doing that against
`sharedPool` would corrupt every other test in the suite running concurrently
against it. `TestMigrations_RoundTrip` already established the precedent of a
private container for exactly this reason ("it tears the schema down"); this
test tears the *safety* down instead, which is no less disqualifying for a
database other tests depend on staying clean. `ALTER TABLE journal_entries
DISABLE TRIGGER journal_entries_balanced` (and, for the same subtest's own
cleanup, `journal_entries_no_mutation`) is scoped to that one private
database, re-enabled before the subtest ends, and never touches `sharedPool`
at all.

**What remains open.** The Prometheus alert rules that turn these gauges into
an actual page, and the Grafana dashboard, are Phase 6's observability item
and are not written by this entry. `ConsistencyInterval`'s one-minute default
is untuned against real data volume — `CheckGlobalInvariant` and
`CheckProjectionDrift` are both full scans of `journal_entries` with no index
that makes them cheap at scale, and the honest answer to "what should this be
in production" is "measured against a real database," not guessed here.

### D50. Observability: the metrics that were missing, a real trace across the async boundary, and what got deployed

Four pieces, tied together by one theme: closing gaps that looked closed. Every
metric this phase's Definition of Done names by name already existed under a
different, defensible name (`ledger_saga_instances` for "saga_state_total",
`ledger_db_tx_retries_total` for "serialization_retries_total by sqlstate",
`ledger_projector_consumer_lag` for "consumer_lag",
`ledger_reconciliation_exceptions_total` for "reconciliation_exceptions_total
by category" — already exact). What genuinely did not exist is recorded here.

**`transactions_posted_total` and `transaction_duration` are recorded in
`internal/ledger.Service`, not the HTTP layer.** `ledger_http_requests_total`
already counts HTTP traffic; recording a second, domain-level counter at the
handler would double-count every API-originated post while still missing
every saga-originated one, since `payout.Orchestrator` calls
`PostTransaction`/`ReverseTransaction*` directly and never passes through an
HTTP handler. The instrumentation wraps `PostTransaction` and the shared
`reverse` implementation with a two-line measuring shim
(`postTransaction`/`doReverse` hold the unexported, untouched bodies) rather
than threading `time.Now()` calls through either — the smallest diff against
code that already carries a great deal of load-bearing commentary about
ordering, and the two are behaviourally identical.

**The type label is guarded against cardinality, not merely typed.**
`TransactionType` is a small closed enum in practice, but `req.Type` is read
for the metric label *before* `validate()` runs, so a caller sending garbage
would otherwise mint one Prometheus series per distinct garbage string it
tried. `observeTransaction` folds every `!t.Valid()` value into a single
`"invalid"` label — cheap, and it is the same cardinality discipline
`HTTPRequests`' own doc comment already states for routes ("route rather than
raw path, so a UUID in the URL cannot explode cardinality").

**`ledger.NewService` gained a `metrics` parameter, nil-safe.** The
alternative — a setter, or a second constructor — was rejected because every
other metrics-consuming type in this codebase (`db.Retrier`,
`polling.Publisher`, `payout.Orchestrator`) takes it as a constructor argument,
and a service built once at startup has no legitimate later point at which
metrics would arrive. `nil` is accepted and checked, matching `Retrier`'s own
convention, because the large majority of this codebase's tests construct a
service purely to drive `PostTransaction` and have no reason to also build a
registry — see `newLedgerService` in `test/testdb.go` versus
`newRetryingLedgerService`, which already existed and already built one for
the retrier; it now hands the same instance to `NewService` too, rather than
constructing a second, disconnected one.

**`ledger_outbox_lag_seconds` needed a place to run that both publisher arms
share, and neither arm's own process had one.** The polling publisher already
holds a pool; the Debezium arm is a pure HTTP status monitor over Kafka
Connect's REST API and, before this entry, opened no direct connection to
Postgres *at all* — D31 states plainly that Debezium's data path never touches
this codebase, and that was taken further than intended: the *monitoring*
path did not need to make the same abstention, and hadn't been given the
chance not to. `cmd/outbox-publisher` now opens a pool unconditionally,
regardless of which arm is selected, purely for
`runBacklogMonitor` — a `SELECT count(*), EXTRACT(EPOCH FROM now() -
min(created_at))` against `outbox_unpublished_idx`, the same partial index
D4 built for "any future fallback publisher" and monitoring, finally used for
the second of those two stated purposes. `ledger_outbox_backlog` existed as a
declared, registered metric since Phase 4 and had never once been `.Set()` —
found only because writing the lag gauge alongside it meant looking at the
one query that should have been feeding both.

**`ledger_saga_oldest_overdue_seconds` is a new `saga.Store` method, not a
second query bolted onto the metric.** The phase asks to alert on "saga stuck
> 5 min", and `ledger_saga_instances` — a population count by status — cannot
express that: a healthy system with a dozen sagas in flight and a system with
one saga stuck for an hour produce the same non-zero gauge. The new
`OldestOverdueSeconds` query reuses `saga_instances_deadline_idx` exactly —
the same partial index, same four excluded terminal statuses — that the
sweeper's own `ClaimExpired` already relies on, so this is a read-only cousin
of a query this package already had to get right. `NEEDS_MANUAL_REVIEW` is
excluded on purpose: it is alerted on separately
(`ledger_saga_manual_review_total`), and including it here would let one old,
unresolved escalation permanently dominate the gauge and hide a second,
freshly-stuck saga behind it. Wired into `refreshGauges`, which `SweepOnce`
already called on every sweep tick — no new loop, no new ticker.

**Kafka header trace propagation: a new column, not a repurposed one.**
`outbox.trace_id` (D32) stays exactly what it was — an informational JSON
envelope field nothing extracts back into a span. The gap named in this
phase's own kickoff is real: "one trace spans the whole async flow" was not
true before this entry, because nothing on the consumer side ever called
`Extract`. Closing it needed the full W3C `traceparent` value (trace id *and*
span id, plus flags), which `trace_id` alone cannot reconstruct into a valid
remote `SpanContext`. `outbox.trace_parent` (migration `000018`) holds that
value, computed once via `otel.GetTextMapPropagator().Inject` — reusing the
exact propagator `NewTracerProvider` installs for HTTP, rather than
hand-formatting the W3C string, so this can never drift from what `otelhttp`
already relies on elsewhere in this codebase. Kept as a separate column
alongside `trace_id` rather than replacing it: `trace_id` is read by nothing
today, but it is a smaller, human-readable value a support engineer might
grep a database dump for, and there was no correctness reason to delete it.

**The wire format still cannot depend on which publisher wrote it (D31's own
rule), so both arms promote the SAME column to the SAME header.** The polling
publisher sets `traceparent` as a real `kgo.RecordHeader` when
`trace_parent` is non-empty. The Debezium connector promotes it via the
EventRouter SMT's `table.fields.additional.placement` config
(`trace_parent:header:traceparent`) — a declarative connector-config change,
not Go code, which is exactly the asymmetry D31 already documented between
the two arms' data paths. Neither arm touches the JSON payload: the header is
a purely transport-level addition, so every existing byte-for-byte assertion
about the wire body (D28's reasoning for `response_body`, applied here to the
event envelope) stays true unchanged.

**The consumer extracts and starts a genuine child span, verified by an
in-memory span recorder, not by comparing two independently-read strings.**
`internal/projector.Consumer.handle` extracts `traceparent` from the record's
headers into a `propagation.MapCarrier`, calls
`otel.GetTextMapPropagator().Extract`, and starts `"projector.apply_event"`
from the resulting context. `tracer := otel.Tracer(...)` is the first manually
started span anywhere in this codebase — every other one comes from
`otelhttp`'s automatic instrumentation at the HTTP boundary — named for the
package it instruments per the ordinary Go OpenTelemetry convention, since
there was no existing local precedent to follow instead.

`TestProjector_TraceContextPropagatesThroughKafkaHeaders` installs a real
`sdktrace.TracerProvider` backed by `tracetest.SpanRecorder` as the
process-global provider and propagator, for the test's own duration, and
asserts the recorded consumer span's `TraceID` equals the producing span's —
proving the SDK's own machinery genuinely linked the two, which a comparison
of `outbox.trace_id` against a logged string could not: two independently
computed values that happen to match is a weaker claim than "the propagator
extracted a real parent," and only the second is what the phase actually
asked for.

**Swapping `otel`'s global tracer provider and propagator inside one test in
a package that runs many tests in parallel is safe, and the reasoning is
worth recording because it looks unsafe.** The test does not call
`t.Parallel()`. Go's test runner does not begin executing ANY `t.Parallel()`
test's body in a package until every non-parallel test — this one included,
cleanup and all — has already returned; a parallel test that has reached its
own `Parallel()` call is paused, not running, for the whole of that window.
`TestProjector_ConsumesAndAppliesTransactionPosted` and its neighbours are
already non-parallel in this same file, for the unrelated reason that each
spins its own Redpanda container, which is what made this test's placement
unremarkable rather than a special case.

**Deployment: Prometheus and Grafana are new `docker-compose.yml` services,
provisioned rather than clicked together.** `deploy/prometheus/prometheus.yml`
scrapes every service that exposes `/metrics` by its compose-internal
hostname and port — the same ports each service's own admin listener binds
to, not the host-mapped ones, because Prometheus runs on the same compose
network rather than the host. `mock-gateway` is deliberately absent from the
scrape list: it stands in for an external gateway (D45), and a real one would
not let this stack scrape it either. Grafana's datasource and dashboard are
both auto-provisioned (`deploy/grafana/provisioning/`), so `docker compose up`
produces a working, populated dashboard rather than an empty Grafana asking
to be configured by hand — consistent with the Definition of Done's own
requirement that the stack come up clean from scratch, not clean-plus-manual-steps.

**The five alerts `deploy/prometheus/alerts.yml` defines are exactly the five
the phase names, no more.** Each one is backed by a metric this phase built
specifically to make it possible to write honestly — see this entry and D49
above for which. One is worth flagging on its own:
**`LedgerReconciliationExceptions` uses `increase(...[1d]) > 0`, not a bare
`> 0`.** `ledger_reconciliation_exceptions_total` is a counter, and counters
never go down; a bare `> 0` threshold would fire once, on the first exception
this deployment ever records, and then never resolve again regardless of
whether the ledger is fine for the next five years. `increase` over the
reconciliation job's own default interval is what the phase's "reconciliation
exceptions > 0" wording actually means: a NEW exception, not the existence of
history.

**`docs/RUNBOOK.md` is one section per alert, in the order they appear in
`alerts.yml`**, each covering what firing means, how to confirm and localise
it (mostly the exact SQL the alerting metric's own Go query runs, so an
operator is never left re-deriving a query this codebase already wrote), and
what to actually do. None of the five sections ends in "restart the
service" — restarting fixes nothing about an unbalanced journal, a drifted
balance, or an unresolved reconciliation exception, and a runbook that
suggests otherwise teaches the wrong reflex for the one class of alert this
system is built to take seriously.

**What remains open.** Structured logs already carry a correlation id on
every HTTP-originated line (`chimiddleware.RequestID`, threaded through by
`requestLogger`) — that half of the phase's ask predates this entry and
needed nothing added. The fault-injection harness and its chaos test are the
one item of Phase 6 this entry does not touch.

### D51. The fault-injection harness: six faults, and why clock skew is only two of them

**Decided.** `cmd/chaos-harness` is a new, deliberately separate binary
(`deploy/Dockerfile.chaos-harness`, never the shared one every real service
uses) that injects the six faults the phase names, live, against a running
`docker compose` stack — never against `go test`'s own Testcontainers, and
never against production. Every fault is one of exactly two kinds:
something real actually happening (`docker pause`/`unpause` over the Docker
Engine API's own Unix socket, or a real transaction genuinely holding a real
row lock), or a control call into a mechanism this codebase already built
for the purpose (mock-gateway's `/control/behaviour`, D45; the new
`internal/http.HandleClockSkew`, below). `.claude/rules/testing.md`'s own
rule for failure tests — "they actually kill things... they do not simulate
failure with a boolean flag" — is written for tests, but it applies with
more force to a tool tests are built on, so it governs every fault here
identically.

**DB connection failure and Kafka unavailability: `docker pause`, exactly
the mechanism D36 already validated.** `TestOutboxPublish_KafkaOutage`
established that pausing (the cgroup freezer, freezing an
already-running process) is the faithful way to simulate an outage against
this specific Redpanda testcontainers module, because a stop/start cycle
leaves its custom entrypoint waiting for a lifecycle hook that only runs
once. The harness reuses the identical mechanism against the compose
stack's real `postgres` and `redpanda` containers, addressed by name over
the Docker socket — which is why those two services gained a pinned
`container_name` in `docker-compose.yml`: Compose's own auto-generated name
depends on which directory the repo happens to be checked out into, and a
chaos harness whose default target name is wrong on every machine but the
one it was tuned on is not a tool, it is a trap for the next person who runs
it.

**Gateway timeout and gateway 500: zero new mechanism.** Both are
`POST /control/behaviour` calls to mock-gateway (D45), which already changes
real HTTP behaviour rather than a flag an orchestrator checks internally.
The harness's only job is to drive it and restore `{"outcome":"succeed"}`
afterward — a proxy, not a reimplementation.

**Slow query: a real held row lock, not `time.Sleep`.** The harness opens
its own connection, takes `FOR UPDATE` on a real, configurable "hot"
account's `account_balances` row (default `platform-bank-inr`), and runs
`SELECT pg_sleep($1)` *inside that same transaction* before committing. Any
real posting transaction that touches the same account queues behind it
exactly as it would under genuine contention — the same row-lock mechanism
D11's ordered locking already depends on, made slow on purpose rather than
merely described as slow in a log line.

**Clock skew has exactly two legitimate targets, discovered by looking for
them rather than assumed.** The obvious design — an injectable `Clock`
threaded through the reconciler's timing-window comparison and the saga
sweeper's timeout checks — turned out to test nothing: the reconciler
compares two already-fixed, already-recorded timestamps (never "now" at
all), and every saga deadline *after* the first is deliberately computed by
PostgreSQL's own `now()`, specifically because "deadlines computed from a Go
process's wall clock and compared against the database's would drift" (see
`saga.Transition`'s own doc comment, Phase 5). Threading a fault into either
would have exercised a seam that does not affect any real decision — a
vulnerability manufactured to have something to test, not a real one made
testable.

Two places do read a Go process's own clock for a decision that matters, and
both were found by grepping for `time.Now()` and asking, for each hit,
"is this compared against something Postgres computed with its own clock?"
— `internal/idempotency.Manager.resolveExisting`'s `expires_at` check, and
`payout.Orchestrator.Start`'s very first `step_deadline_at` (every
subsequent one is server-computed; this one cannot be, because the saga does
not exist in the database yet to compute it against). `internal/clock` makes
exactly these two calls injectable (`clock.Now()` in place of `time.Now()`)
and nowhere else — a log timestamp or a metrics timer skewing along with an
injected fault would be noise, not signal, and the whole value of a narrow
seam is that everything outside it keeps behaving normally while the fault
runs.

**The clock-skew endpoint is deliberately not part of `Deps`/`NewMux`.**
Every route registered there ends up on either the public API listener
(`cmd/api`) or a worker's one-and-only listener — and a clock a stranger on
the internet could skew is not a chaos-testing feature, it is a
vulnerability wearing one. `internal/http.HandleClockSkew` is mounted
directly on each process's admin/metrics mux, standalone, gated by
`LEDGER_FAULT_INJECTION_ENABLED` (default `false`, flipped to `true` only by
`deploy/docker-compose.chaos.yml`'s override of the two services that have a
genuine clock fault to offer). `cmd/api`'s admin listener in particular
carries no other routes at all today — this is the first thing mounted
there beyond `/metrics` — which is exactly why it could not be added to the
shared router: that router is also what `cmd/api`'s *public* listener serves.

**Why an overlay compose file, not a Compose profile alone.** A profile
controls whether a service starts; it cannot conditionally change an
*already-defined* service's environment. Flipping
`LEDGER_FAULT_INJECTION_ENABLED` on `api` and `saga-orchestrator` needed
that second thing, so `deploy/docker-compose.chaos.yml` is a genuine overlay
(`-f docker-compose.yml -f docker-compose.chaos.yml`, or `make chaos-up`),
relying on Compose's own key-by-key merge of `environment:` maps across
`-f` files — every other setting those two services already have survives
untouched, only the one named flag changes. The base file states the
default explicitly (`LEDGER_FAULT_INJECTION_ENABLED: "false"`) rather than
leaving it merely absent, so the compose file itself documents what the
overlay is overriding *from*.

**Why `chaos-harness` gets its own Dockerfile and runs as root.** Every real
service's image runs as an unprivileged uid specifically so that "this
image needs no elevated privilege" stays a property CLAUDE.md's own
Definition of Done can rely on. `chaos-harness` cannot make that claim: it
holds `/var/run/docker.sock`, which is root-equivalent access to the host by
construction, regardless of which uid the process inside the container
reports. Giving it the same non-root `USER` as everything else would not
make it safe, it would make it broken (permission denied against the
socket) in a way that looks like a security posture and is not one. A
second, visibly different Dockerfile makes the divergence the point rather
than a thing to notice only by diffing two otherwise-identical files.

**The chaos test drives an already-running stack; it does not orchestrate
one.** `TestChaos_InvariantHoldsUnderRandomFaults` is unlike every other
test in this suite in one specific way: it does not use Testcontainers.
It cannot — `cmd/chaos-harness` pauses and unpauses containers *by name*
over the Docker Engine API, which only means something against a stack
`docker compose` actually started, not one Testcontainers spun up in
isolation for this one test. The alternative — teaching
`testcontainers-go`'s `compose` module to drive the entire real
`docker-compose.yml` (Postgres, Redpanda, Kafka Connect, every service, the
chaos harness itself) from inside Go, fully automated — was considered and
rejected as disproportionate: real value at a small fraction of the
complexity is what a `t.Skip` with a clear message and a `make chaos-up` /
`make chaos-test` pair buys instead, and the trade is recorded here rather
than discovered later as an unexplained gap. Reused directly rather than
reimplemented: `newAccount`, `newTypedAccount`, `assertGlobalInvariant` and
`internal/consistency`'s own checks, all of which already take a
`*pgxpool.Pool` as a parameter rather than reaching for the test suite's
`sharedPool` — the one design choice in the existing test helpers that made
pointing them at a different, real database (`localhost:5432`, the chaos
stack's own host-mapped port) a parameter change rather than a rewrite.

**The load generator tolerates failure on purpose, and asserts something
narrower and stronger instead.** The test does not assert most transfers
succeed — some are SUPPOSED to fail while a fault is active, and asserting
otherwise would just be asserting the faults do nothing. What it asserts is
that after forty seconds of real faults fired at random against real load,
`assertGlobalInvariant` and `internal/consistency.CheckGlobalInvariant`/
`CheckOrphans` are still true. That is the one claim this whole phase is
built to make defensible under pressure, and it is the only one this test
makes.

**A real defect surfaced only by actually running the full suite again
after this entry's own new test was added — recorded per D35's own
precedent.** `TestIdempotency_ClockSkewCausesReadPathToTreatALiveRecordAsExpired`
posts one real transaction and, unlike every other test that touches Kafka,
runs no publisher against it. Its outbox rows sat unpublished when the test
ended, and `TestOutboxPublish_KafkaOutage` — whose polling publisher drains
the *entire* shared `outbox` table by design, not merely the rows its own
test created — swept them up during its own pre-outage drain and inflated
its before/after delta by exactly one transfer's worth (three rows: one
`TransactionPosted`, two `BalanceUpdated`), failing an assertion about a
code path this entry never touched. Not a race and not flaky: fully
deterministic, reproduced identically across three separate runs, because
Go orders test files alphabetically and `idempotency_test.go` precedes
`outbox_publish_test.go`. Fixed by having the offending test delete its own
unpublished rows in `t.Cleanup` rather than leave them for the next
publisher-running test to trip over — the correct owner of the fix, since
the alternative (loosening `TestOutboxPublish_KafkaOutage`'s own assertion)
would have weakened a real zero-loss guarantee to work around a different
test's mess rather than cleaning up the mess.

---

## Phase 7 — Defensible performance numbers

### D52. The harness is Go for measurement, Make/compose for lifecycle; three findings from actually running it

**Decided.** `cmd/loadtest-harness` runs k6, queries Prometheus, samples
`docker stats`, and proves correctness under load by calling
`internal/consistency`'s three checks and `internal/projector.Rebuild`
directly against the pool it already holds. `make loadtest` owns bringing
the stack up fresh and seeding it (`docker compose down --volumes`, `up
--build -d`, the existing seed target) before invoking the binary, reusing
exactly the same primitives a developer runs by hand rather than
reimplementing compose lifecycle management in Go.

**Rejected: a pure bash harness.** k6's own `--summary-export` and
Prometheus's HTTP API both return JSON; turning that into the JSON results
file and the `docs/BENCHMARKS.md` table the phase asks for is straightforward
in Go and painful in bash, and this repo already has five `cmd/` binaries for
exactly this kind of "needed a real program" job.

**Rejected: a Go program that also drives compose.** Compose's own
healthcheck-ordered `up -d` is already the tool for "bring this stack up
correctly"; reimplementing it in Go over the Docker API would duplicate
`docker-compose.yml`'s own dependency graph for no behavioural gain.

**`cmd/reconciler` gained `-check`, mirroring `cmd/projector -rebuild`.** The
three internal structural checks (D49) previously only ran on a ticker inside
a long-lived process, reporting through a Prometheus gauge -- the right shape
for continuous monitoring, wrong shape for "did this load run corrupt
anything, yes or no, exit code please." `-check` runs all three once, prints
JSON, and exits non-zero on any violation. The harness itself calls the same
underlying `internal/consistency` functions directly rather than shelling out
to this flag -- it already holds the pool `-check` would open a second one
for -- but the flag stands on its own as something a person can run by hand
after a load run they started manually, which is the reproducibility the
phase asks for.

**`internal/gateway/mock.Behaviour` gained `FailureRatePercent`.**
`saga_heavy` (not yet built) needs a real per-request chance of an ambiguous
gateway failure, and the existing `Outcome` field is deterministic --
"succeed", "decline", or "error" until told otherwise. Deliberately resolves
to `"error"`, never `"decline"`: a 500 is UNKNOWN (the gateway may have
processed the payment and failed to say so), which is what exercises the
saga's probe-then-decide machinery, where a deterministic decline is already
covered by `Outcome` on its own and would not add coverage.

**Three things this entry would rather have not discovered, found only by
actually running the harness against the real stack repeatedly rather than
reasoning about it from the code:**

1. **k6's `--summary-export` `"thresholds"` object does not mean what its key
   suggests.** A run whose CLI output showed `✓ PASSED` for every threshold
   wrote `"thresholds": {"p(99)<500": false}` into the JSON for that exact
   metric. Parsing that boolean as "did this threshold pass" would have
   silently inverted every threshold result in the JSON report. The harness
   does not read it at all -- `ThresholdsPassed` comes from k6's own process
   exit code, which is what k6's documentation actually commits to and what
   CI would act on.

2. **`ledger_outbox_lag_seconds` is not a usable drain signal under the
   Debezium publisher arm.** Its own doc comment ("age of the oldest
   unpublished outbox row... zero when nothing is unpublished") describes the
   *polling* arm. Under Debezium (this stack's default, D31) it is
   `pg_stat_replication.replay_lag` on the outbox replication slot -- the WAL
   publisher's own confirmation lag, not a queue depth -- and it is bounded
   below by Kafka Connect's `offset.flush.interval.ms` (60s default), which
   governs how often the connector's own offset commit advances the slot's
   confirmed position. Observed directly: 40-80s of reported "lag" a full
   minute after every event a run produced had already been produced to
   Kafka *and* consumed by the projector, confirmed by cross-checking
   `pg_stat_replication` and the connector's own `/status` endpoint by hand.
   `waitForPipelineDrained` gates on `ledger_outbox_backlog` (meaningful for
   the polling arm) and `ledger_projector_consumer_lag` (real Kafka
   consumer-group lag, meaningful for both arms, and what actually answers
   "has the projector seen everything this run produced") instead.
   `ledger_outbox_lag_seconds` is still reported in the benchmark output --
   it is a real number, just not a drain signal -- with the caveat inline so
   a reader does not mistake "31s" for a queue backlog.

3. **`make loadtest`'s own `docker compose up --build` measurably inflates
   the first scenario's tail latency.** A run immediately following a fresh
   image rebuild recorded p99=1.67s against a p50 of 3.6ms on the identical
   scenario that otherwise runs at p99 in the 100-300ms range -- the system
   was fine, the machine was still finishing Docker's own layer-write and
   build-cache bookkeeping, plus whatever else shares this host. Two
   responses, not one: `make loadtest` now sleeps 15s after the stack reports
   healthy before starting the timed run, and `baseline_simple_transfer`'s
   own p99 threshold is deliberately loose (2s) rather than tuned to a number
   that turned out to depend on what else this specific machine was doing at
   the time. `docs/BENCHMARKS.md` states plainly that p99 on a shared
   development machine should be read as an order of magnitude, not a
   precise figure -- throughput and error rate did not show the same
   variance and are the numbers worth trusting precisely.

**What remains open.** Four scenarios (`hot_account`, `idempotent_retry_storm`,
`saga_heavy`, `mixed_realistic`), the optimisation cycle (pool tuning,
statement caching, batch inserts, the sharding comparison, index
effectiveness, read-replica routing), and the README numbers table are not
built by this entry.

### D53. The optimisation cycle: five measurements, and what each one actually found

**Decided.** Five of the six optimisation steps the phase names, each
measured against this same machine rather than guessed, with the sixth
(read-replica routing) recorded separately in D54 because it is a real
subsystem addition rather than a measurement. Per CLAUDE.md's own rule for
this cycle -- revert a regression, document why, move on -- three of these
five findings are NOT "the obvious answer was right"; they are surprises
worth recording exactly because they were not predicted.

**Prepared statement caching: already the default, and the alternative does
not merely lose, it breaks.** pgx v5's own source (`conn.go`) states
`QueryExecModeCacheStatement` is already its default, unconditionally, and
`internal/db/db.go` never overrode it -- so this codebase has been getting
server-side prepared statements for free since Phase 1. `config.Postgres`
gained `QueryExecMode` (`LEDGER_POSTGRES_QUERY_EXEC_MODE`, default
`cache_statement`) purely to make the alternative reachable for measurement.
Setting it to `simple_protocol` and rebuilding `api` did not produce a
slower number -- it produced `unable to encode []uuid.UUID{...} into text
format for unknown type (OID 0): cannot find encode plan` on every single
write, reproduced directly against a standalone pgx connection to confirm
the exact cause: `QueryExecModeSimpleProtocol` has no server round trip to
learn a parameter's PostgreSQL type from, so it cannot encode the
`unnest($1::uuid[], ...)` array parameters `pgledger`'s own journal-entry
insert depends on (see this entry's own next finding). This is not "the
default is faster" -- it is "the alternative cannot run this codebase's
write path at all," which is a stronger reason to keep the default than any
benchmark number would have been.

**Batch inserts: already shipped (`unnest`, not a loop) -- the "before"
this phase asked to measure never ran in production, so it was measured
directly instead of reintroduced as real code just to revert it.**
`test/batch_insert_bench_test.go`'s `BenchmarkJournalEntryInsert` runs both
shapes -- the real `unnest($1::uuid[], ...)` statement and a naive per-row
`INSERT` loop -- each inside `BEGIN`/`ROLLBACK` so the deferred balance
trigger (which only fires at COMMIT) never has to be satisfied and the
benchmark leaves no trace in the database it runs against:

| Entries per transaction | `unnest` batch | naive loop | Speedup |
|---|---|---|---|
| 2  |  306.5 µs/op |  404.1 µs/op | 1.32x |
| 10 |  499.7 µs/op | 2167.2 µs/op | 4.34x |
| 50 | 1720.2 µs/op | 10257.7 µs/op | 5.96x |

The speedup grows with entry count because the naive loop pays one Postgres
round trip per row while the batch pays one round trip total -- exactly the
mechanism the batch insert was built to avoid, now with a number attached.

**Index effectiveness: a genuine surprise, and the honest finding is
scale-dependent, not "index good."** `journal_entries_account_id_created_at_idx`
backs the account-statement query (D-index-decisions table, migration
000004). Measured with `EXPLAIN (ANALYZE, BUFFERS)` against the live,
loaded dev database (57,575 journal entries after the full `make loadtest`
run), comparing the index present against `BEGIN; DROP INDEX ...; EXPLAIN
...; ROLLBACK;` -- the same technique the batch-insert benchmark uses, for
the same reason: real data, nothing altered. Plans saved under
`docs/explain/`.

- **Full account history** (`From` = account inception, ~26,162 of 57,575
  rows match): **41.7ms without the index, 126.4ms with it** -- the index
  makes this query THREE TIMES SLOWER. `Buffers` explains why:
  the WITH-index plan is a Bitmap Heap Scan that visits heap pages in the
  order the bitmap collects them, not physical order, costing 512 real disk
  reads against `shared_buffers=128MB`; the WITHOUT-index plan is a
  sequential scan that reads the same pages in physical order and stays
  entirely cache-resident (0 disk reads, verified in `Buffers:`).
- **Recent window** (`From` = 2 minutes before the newest entry, 6,561
  rows match -- the query's actual intended use, a client paginating recent
  activity): **29.6ms with the index, 33.1ms without** -- the index wins,
  but by ~10%, not dramatically, because at this table's current size both
  plans are already cache-resident and neither pays for a disk read.
- **The honest conclusion, stated so it cannot be quoted out of context:**
  this index is not wrong and is not being dropped. At THIS data volume (a
  single append-only table small enough that `shared_buffers` holds nearly
  all of it), PostgreSQL's own cost-based planner and the buffer cache
  mostly erase the gap an index is supposed to provide, and a bitmap-scan's
  scattered heap access can, in one specific access pattern, cost more than
  reading the whole table in physical order. The index's real advantage
  is at PRODUCTION scale -- millions of rows spanning many gigabytes, where
  neither plan is cache-resident and a sequential scan means reading the
  entire table off disk regardless of how selective the query is. Measuring
  "index effectiveness" honestly means reporting that the effect is
  real but small on today's data and would be reported backwards if this
  measurement were mistaken for the production answer.

**Connection pool tuning: two findings, not one, and the first sweep had to
be thrown out.** The first sweep (MaxConns ∈ {5, 10, 20, 40, 80}) ran
concurrently with other benchmarks on this same machine (the sharding
rerun, the batch-insert benchmark) and produced an incoherent curve --
MaxConns=20 measuring WORSE than MaxConns=10 -- that was host contention,
not a property of the pool, and was discarded rather than reported as a
result. Two real findings survived a clean rerun with nothing else
competing for the host:

1. **MaxConns=80 is not merely suboptimal, it is broken**, reproducibly:
   96.35% of requests failed with 500s, traced to Postgres itself refusing
   connections -- `FATAL: sorry, too many clients already`, reproduced by
   hand against `psql` during the same load. `max_connections=100` (the
   docker-compose default) is a budget SHARED across every service that
   opens a pool against this one Postgres instance: `outbox-publisher`,
   `projector`, `saga-orchestrator` and `reconciler` each default to 20
   (`internal/config`'s own default), so 4×20=80 is already committed
   before `api`'s own pool opens a single connection. Setting `api` alone
   to 80 pushes the honest total toward 160 against a ceiling of 100. This
   is the actual optimum-finding result the phase asked for: the ceiling
   here is a structural, shared budget, not row-lock contention, and no
   amount of tuning `api`'s own `LEDGER_POSTGRES_MAX_CONNS` in isolation
   escapes it.
2. **Within the range that stays safely under that shared budget
   (MaxConns ∈ {5, 10, 15, 20, 25, 30}), throughput is flat**: 396-458 req/s
   across the whole range, a ~15% band consistent with this machine's own
   established run-to-run noise (docs/BENCHMARKS.md's own variance
   caveat) rather than a real trend. The reason is `baseline_simple_transfer`'s
   own design: it deliberately hits only TWO fixed accounts (this entry's
   own scenario doc comment), so at 100 concurrent VUs the bottleneck is
   D11's row-lock queueing on those two `account_balances` rows, which a
   bigger connection pool cannot relieve -- there is always a writer ready
   to grab a connection the instant the lock frees, whether the pool holds
   5 spares or 30. A pool-sizing effect would show up on a workload spread
   across many distinct accounts instead, which is not what this scenario
   is for.

**Decided: `LEDGER_POSTGRES_MAX_CONNS` stays at its default (20).** It sits
comfortably inside the safe, near-flat range the clean sweep found, well
clear of the shared-budget ceiling the confirmed-broken 80 demonstrated, and
no value in the measured range showed a throughput advantage worth trading
that safety margin for.

**The sharding comparison: rerun the existing benchmark, not reimplemented
at the HTTP layer.** D25 already built and shipped
`TestSharding_ThroughputSingleVersusSixteen`
(`test/sharding_test.go`) -- the authoritative, already-correct comparison,
run against the identical machine every other Phase 7 number in this
document comes from. Building a second, k6-level version (which would need
an HTTP-reachable way to create shards for a seeded account, and there is
none -- D25 states plainly that shard creation is a rare, direct-SQL admin
operation) would have been a less rigorous reimplementation of a comparison
that already exists and is already tested, not a genuinely new measurement.
Rerun three times (`go test -run TestSharding_ThroughputSingleVersusSixteen
-count=3`):

| Run | Single account | 16 shards | Speedup |
|---|---|---|---|
| 1 | 186 tx/s | 817 tx/s | 4.40x |
| 2 | 231 tx/s | 691 tx/s | 2.99x |
| 3 | 241 tx/s | 482 tx/s | 2.00x |

Consistent with D25's own range (4.4x-4.8x) and its own caveat that
absolute figures and even the exact ratio describe this laptop and its
concurrent load at measurement time, not a portable constant -- the range
here is wider than D25's three original runs, and that is itself consistent
with this being a shared, busier machine than whatever D25 was measured
against, not a regression in sharding's own benefit.

### D54. A real streaming replica, and a pg_hba discovery it took a rejected `pg_basebackup` to find

**Decided.** `postgres-replica` (`deploy/docker-compose.yml`) is genuine
PostgreSQL 16 streaming (physical) replication -- a real hot standby that
bootstraps itself from the primary via `pg_basebackup`, not a second pool
pointed at the same database. Per the trade-off approved before this phase
began: the alternative (a fake replica) would have made replica lag a
number that could never be anything but zero, which is not a caveat, it is
a fabricated result.

**The bootstrap: `pg_basebackup -R -S ledger_replica_slot -C`, run from the
replica's own entrypoint on first start only.** `-R` writes
`standby.signal` and `primary_conninfo` for PostgreSQL 16's own recovery
configuration in one flag, rather than hand-writing it. `-S ... -C` creates
and uses a dedicated, named replication slot -- the same mechanism the
Debezium outbox connector already holds on this same primary
(`max_replication_slots=10` already had headroom for a second, migration
000008) -- so the primary retains exactly the WAL this replica has not yet
consumed rather than streaming without a slot and risking a standby that
silently falls behind what the primary has already recycled. The
entrypoint checks `PGDATA/PG_VERSION` before running `pg_basebackup` at
all: present means an earlier start already cloned the primary, so a
restart against the existing volume skips straight to `docker-entrypoint.sh
postgres` -- re-running `pg_basebackup` against a non-empty data directory
fails outright, and the same check is what makes this safe to leave in a
`docker compose up` a developer runs repeatedly, not just once.

**The discovery: the primary's own generated `pg_hba.conf` does not allow
replication connections from anywhere but localhost, and it takes reading
the actual generated file to know that -- the catch-all line does not
cover it.** The official image's default `pg_hba.conf`, read directly off
the running primary rather than assumed from documentation, is:

```
local   all             all                                     trust
host    all             all             127.0.0.1/32            trust
host    all             all             ::1/128                 trust
local   replication     all                                     trust
host    replication     all             127.0.0.1/32            trust
host    replication     all             ::1/128                 trust
host all all all scram-sha-256
```

The final catch-all line matches every DATABASE except one:
`replication` is a separate pseudo-database in `pg_hba`'s own matching
rules, and a bare `all` in the DATABASE column does not include it. Every
explicit `replication` line above is scoped to `127.0.0.1`/`::1` --
loopback only. `postgres-replica` connects from a different container, so
the first `pg_basebackup` attempt was rejected outright before any of this
entry's other work could be verified. `deploy/postgres-initdb/10-allow-replication.sh`
-- mounted read-only into the primary's own
`/docker-entrypoint-initdb.d`, so it runs automatically and only once, on
the first initialization of an empty volume, the same mechanism the
official image already uses for its own init scripts -- appends one line,
`host replication all all scram-sha-256`, closing exactly this gap and no
more: still password-authenticated, still scoped to the `replication`
pseudo-database specifically, never broadened to a `trust` method or to
ordinary data access.

**`WithReadReplica`, and what it deliberately does not touch.**
`internal/ledger/pgledger.Repository` gained a `readPool` field, defaulted
to the primary pool in `New` when the option is never applied -- so every
existing caller, every test, and any deployment with no
`LEDGER_POSTGRES_REPLICA_DSN` configured reads its own writes with zero
added staleness, exactly as before this entry. Only `GetBalanceAsOf` and
`GetStatement` route to it. Every other read -- the `account_balances` row
a posting transaction locks under `FOR UPDATE` (D1, D18), the idempotency
lease check, the saga claim query -- stays on the primary, because those
reads are load-bearing for a WRITE's own consistency, and a replica is
asynchronous by construction: deciding whether a debit is permitted from a
row that might be milliseconds stale would reopen the exact lost-update
class of bug D11's row lock exists to close. `GetBalanceAsOf` and
`GetStatement` carry no equivalent risk -- D16 already documents that a
temporal or paginated read is bounded-stale by nature, so the additional
staleness a replica adds is a difference in degree, not in kind, which is
the entire reason this option is safe to offer at all.

**`/readyz` needed a second checker, and a naming collision very nearly
made it invisible.** `*db.Pool.Name()` unconditionally returns
`"postgres"`, so registering the replica pool as a second `Checker`
alongside the primary would have reported both under the identical map key
in `/readyz`'s JSON -- one silently overwriting the other, hiding
precisely the "is the replica actually reachable" signal this checker
exists to add. `cmd/api`'s own `namedChecker` wraps the replica's `*db.Pool`
purely to override its name to `"postgres-replica"`, rather than changing
`db.Pool.Name()` itself, which every other service's health check still
correctly reports as `"postgres"` against its own single pool.

**Verification, in increasing order of how directly it proves the claim.**
`/readyz` reports `{"postgres": "ok", "postgres-replica": "ok"}` as two
distinct entries. `pg_stat_replication` on the primary shows a `walreceiver`
entry, `state=streaming`, `sync_state=async`, with `replay_lag` around
90ms against an idle system. `pg_is_in_recovery()` on the replica returns
true, and a direct `INSERT` against it is refused with `cannot execute
INSERT in a read-only transaction` -- the standby genuinely cannot be
written to, not merely documented as read-only. A row inserted on the
primary was visible on the replica by the time the next `psql` command
ran. Most direct of all: `SELECT query, state FROM pg_stat_activity` run
ON THE REPLICA, while issuing five concurrent
`GET /v1/accounts/{id}/statement` requests through the real HTTP API, shows
five backends holding the EXACT text of `GetStatement`'s own query --
proof the query executed there, not an inference from timing or
configuration. (A `docker pause` on the replica, tried first as a more
dramatic proof, returned successes rather than the expected hang; not
pursued further once `pg_stat_activity` gave an unambiguous, directly
observed answer instead of one resting on assumptions about how cgroup
freezing interacts with already-established connections.)

**What remains open.** No failover: if the replica falls behind or dies,
`GetBalanceAsOf`/`GetStatement` fail rather than falling back to the
primary -- a deliberate scope boundary (this phase asks for read-replica
ROUTING, not a highly-available read path) rather than an oversight, but
worth stating rather than leaving implicit. Replica lag under real write
load (this entry measured it idle) is exactly what `docs/BENCHMARKS.md`'s
`mixed_realistic` numbers with the replica in place are for.
