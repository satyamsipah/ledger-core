# Contributing

## Before you start

Read [CLAUDE.md](CLAUDE.md) first — it states the non-negotiable invariants
this codebase exists to protect, and they constrain every change more than
any style guide would. Read [docs/DECISIONS.md](docs/DECISIONS.md) for the
reasoning behind the current design before proposing a different one;
there's a good chance the alternative was already considered and rejected,
with the reason on record.

If a change risks weakening an invariant — balanced transactions, an
append-only journal, integer money, no negative balances on a restricted
account, idempotent writes, no dual writes — stop and open an issue before
writing code. These are enforced at the database level on purpose; a patch
that routes around the enforcement rather than working with it will be
asked to change shape, not just its diff.

## Development setup

Requires Docker and Go 1.25+.

```bash
git clone https://github.com/satyamsipah/ledger-core.git && cd ledger-core
make up      # full stack: postgres, redpanda, kafka-connect, redis, api,
             # outbox-publisher, projector, saga-orchestrator, reconciler,
             # mock-gateway, prometheus, grafana
make seed    # development chart of accounts
make test-race   # full suite under the race detector (starts Testcontainers)
make lint
make down    # tear down, including volumes
```

For the admin dashboard: `cd web && pnpm install && pnpm dev` — see
[web/README.md](web/README.md), including how to run it against mock data
with no backend at all.

## Code conventions

These are enforced by `golangci-lint` and CI where mechanical, and by
review where they're not:

- **Layered**: handler → service → repository. No SQL in handlers.
- **Errors wrapped with context**: `fmt.Errorf("credit account %s: %w", id, err)`.
- **Domain errors are typed sentinels** (`ErrInsufficientFunds`,
  `ErrIdempotencyConflict`, …) — never bare strings a caller has to
  string-match.
- **`context.Context` threaded everywhere**; every database call has a
  timeout.
- **No global state.** Dependencies are injected via constructors.
- **Exported functions have doc comments explaining WHY, not what** — the
  signature and a competent reader already tell you what; the comment
  earns its place by carrying the constraint, trade-off, or history a
  reader can't get from the code alone. If removing a comment wouldn't
  confuse a future reader, it shouldn't have been written.
- **Table-driven tests.** Target >80% coverage on `internal/ledger`.
- **No mocks for database behaviour.** Every test that exercises write-path
  logic runs against a real PostgreSQL container via Testcontainers,
  because the invariants that matter here are enforced by the database,
  and a hand-written mock of that enforcement proves nothing about it.
- **A failure test kills something real.** A paused container, a
  `pg_terminate_backend` at a specific instant, a frozen account — not a
  boolean an orchestrator checks internally. See
  [docs/ARCHITECTURE.md#tests](docs/ARCHITECTURE.md#tests) for worked
  examples.

## Making a change

1. **Write tests in the same commit as the code they test.** A PR that
   adds behaviour without a test that would fail without it will be asked
   to add one.
2. **Every DB write path needs a concurrency test** — N goroutines against
   a real Postgres, asserting the invariant still holds under contention,
   not just in isolation.
3. **Migrations are plain SQL, always reversible.** `golang-migrate`,
   `up` and `down` both real — a `.down.sql` that can't actually undo its
   `.up.sql` is worse than no down migration, because it lies about being
   one.
4. **Commit messages follow [Conventional Commits](https://www.conventionalcommits.org/)**
   (`feat:`, `fix:`, `test:`, `docs:`, `perf:`, `refactor:`), one logical
   change per commit.
5. **When there's a real trade-off** (isolation level, locking strategy,
   partitioning key, and similar), state at least two options with their
   costs, and record the decision and what was rejected in
   [docs/DECISIONS.md](docs/DECISIONS.md) — the format each entry already
   follows is: decided, rejected (and why), consequences accepted.

## Definition of done

Before opening a PR, all of the following should be true:

- [ ] `docker compose up` (`make up`) brings the whole stack up cleanly
      from scratch
- [ ] All tests pass with `-race` (`make test-race`)
- [ ] Migrations run forward and backward without error
- [ ] New behaviour is covered by at least one concurrency or failure test
- [ ] `docs/DECISIONS.md` updated, if the change involved a real trade-off
- [ ] Relevant `README.md` / `docs/ARCHITECTURE.md` sections updated

## Opening a pull request

CI runs four jobs on every push and PR: `build and vet`, `golangci-lint`,
`test -race` (Testcontainers-backed, ~3 minutes), and `migration
round-trip`. All four need to pass before merge. Describe *why* the change
is needed in the PR body, not just what changed — the commit history and
diff already show what; the reasoning is what a reviewer six months from
now will actually need.
