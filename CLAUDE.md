# CLAUDE.md — Project Context

## What we are building

Ledger-Core: a production-grade, event-driven double-entry ledger — the
accounting core that sits underneath a payments platform. This is NOT a CRUD
app. Correctness under concurrency is the entire point of the system.

## Non-negotiable invariants

These must hold at all times. If any change risks violating one, stop and
flag it instead of implementing it.

1. Balanced transactions. For every transaction, the sum of all signed
   journal entry amounts is exactly zero. Enforced at the DATABASE level via
   a deferred constraint trigger, not only in application code.
2. Immutable journal. journal_entries is append-only. No UPDATE, no DELETE,
   ever. Corrections happen through reversing entries.
3. Integer money only. All amounts are BIGINT in minor units (paise, cents)
   plus an ISO-4217 currency code. Floating point for money is a bug.
4. No negative balances on accounts flagged allow_negative = false, enforced
   by a database CHECK constraint plus in-transaction locking.
5. Idempotent writes. The same idempotency key with the same request body
   must never create a second transaction, under any level of concurrency.
6. No dual writes. The database and Kafka are never written in the same
   logical step. All events go through the transactional outbox.

## Architecture

API Gateway
  -> Payment Service -> Postgres (journal, accounts, idempotency, outbox)
                          -> Debezium CDC -> Kafka
  -> Saga Orchestrator     <- Kafka
  -> Balance Projector     <- Kafka
  -> Reconciliation Engine <- Kafka
  -> Admin Dashboard (ledger explorer, temporal queries, recon reports)

## Tech stack (do not substitute without asking)

- Language: Go 1.22+ (stdlib net/http, chi router, pgx/v5)
- Database: PostgreSQL 16
- Migrations: golang-migrate (plain SQL, up + down, always reversible)
- Broker: Kafka (Redpanda locally — Kafka-API compatible, lighter)
- CDC: Debezium Postgres connector via Kafka Connect
- Cache / idempotency fast path: Redis 7
- Observability: OpenTelemetry traces, Prometheus metrics, slog JSON logs
- Testing: testify, Testcontainers (real Postgres + Kafka, never mocks for
  DB behaviour), go test -race always
- Load testing: k6
- Dashboard: Next.js 14 (App Router) + TypeScript + Tailwind + shadcn/ui
- Local orchestration: Docker Compose
- CI: GitHub Actions

## Code standards

- Layered: handler -> service -> repository. No SQL in handlers.
- All exported functions have doc comments explaining WHY, not what.
- Errors wrapped with context: fmt.Errorf("credit account %s: %w", id, err)
- Domain errors are typed sentinels (ErrInsufficientFunds,
  ErrIdempotencyConflict) — never bare strings.
- Every DB write path has a concurrency test that runs N goroutines against
  a real Postgres and asserts the invariant still holds.
- context.Context threaded everywhere; every DB call has a timeout.
- No global state. Dependencies injected via constructors.
- Table-driven tests. Target >80% coverage on internal/ledger.

## How to work with me

- Before writing code for a new subsystem, propose the design in 5-10 bullet
  points and WAIT for my approval.
- When there is a meaningful trade-off (isolation level, locking strategy,
  partitioning key), present at least two options with pros/cons and give
  your recommendation with reasoning. Do not silently pick one.
- Write tests in the same commit as the code they test.
- After each phase, append to docs/DECISIONS.md: what was decided, what was
  rejected, and why.
- Commit messages: Conventional Commits (feat:, fix:, test:, docs:, perf:,
  refactor:). One logical change per commit.

## Definition of Done for any phase

- docker compose up brings the whole stack up cleanly from scratch
- All tests pass with -race
- Migrations run forward and backward without error
- New behaviour is covered by at least one concurrency or failure test
- docs/DECISIONS.md updated
- README section updated
