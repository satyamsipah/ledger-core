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

### D19. `chi middleware.RealIP` is a known vulnerability, deliberately left in place

**Not decided.** Deferred to Phase 3, when the HTTP gateway design specifies what
sits in front of this service.

`internal/http/router.go` uses `chi`'s `middleware.RealIP`, which upstream has
deprecated as a security issue rather than merely as an old API
(GHSA-3fxj-6jh8-hvhx, GHSA-rjr7-jggh-pgcp, GHSA-9g5q-2w5x-hmxf). It overwrites
`r.RemoteAddr` from the leftmost `X-Forwarded-For` value, or from
`True-Client-IP` / `X-Real-IP`, **whether or not the infrastructure in front
actually sets those headers**. Every one of them is client-controlled, so with
no trusted proxy stripping them, any caller can assert any source address.

**Why this matters here specifically.** `r.RemoteAddr` is what the request log
records, and it is what per-IP rate limiting and fraud signals will read once
they exist. A spoofable client IP means an attacker can both evade an IP-based
limit and attribute their own requests to somebody else's address in the audit
trail — and an audit trail that can be written by the party being audited is
worse than none, because it is trusted.

**Why it is not fixed in this phase.** The correct behaviour is not a library
choice, it is a deployment fact. Getting it right means either trusting exactly
the *n* rightmost hops of `X-Forwarded-For`, where *n* is the number of proxies
actually in the path, or trusting one specific header that one specific load
balancer is known to set and to strip from inbound requests. Both require
knowing the topology — which reverse proxy or load balancer, how many hops,
whether the service is ever reachable directly — and that is exactly what the
Phase 3 gateway design has to pin down. Picking a header now would mean guessing
the topology and encoding the guess as a security control, which is how these
holes are created in the first place.

**Status:** the call site carries a scoped `//nolint:staticcheck` pointing back
at this entry, so CI is green and a real regression elsewhere is still visible —
a permanently red required job stops being a signal within about a week, and the
next genuine failure hides inside it.

The cost of that is honest: the reminder is now a comment rather than a failing
build, and comments are easier to walk past. Two things carry it instead. The
suppression is on the single line, so it expires the moment the middleware is
touched. And **until the trust model is chosen, `r.RemoteAddr` must not be
treated as trustworthy** — not for rate limiting, not for fraud signals, not for
audit. Anything built on it before Phase 3 inherits the spoofability.

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
