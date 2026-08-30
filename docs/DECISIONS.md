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
