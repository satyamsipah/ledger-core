# Ledger-Core Admin Dashboard

Next.js 14 (App Router) + TypeScript + Tailwind + shadcn/ui. Five views:
ledger explorer, account view, saga monitor, reconciliation, system health.

## Running it

```bash
pnpm install
pnpm dev
```

Opens on <http://localhost:3000>. No backend required by default -- see
**Data mode** below.

## Data mode

Every page reads through `lib/api/client.ts`, which switches on
`LEDGER_DATA_MODE`:

| `LEDGER_DATA_MODE` | Source |
|---|---|
| `mock` (default) | A self-consistent synthetic dataset in `lib/api/mock` -- every account balance is the sum of its own entries, every saga step names a real transaction, every reconciliation exception names a real transaction. No backend, no Docker, needed. |
| `live` | The real API, over HTTP. Requires `LEDGER_API_URL` and `LEDGER_API_KEY` (mint one with `go run ../cmd/issue-api-key -principal dashboard` from the repo root). `PROMETHEUS_URL` additionally powers the System Health page. |

Set these in `web/.env.local` (not committed):

```
LEDGER_DATA_MODE=live
LEDGER_API_URL=http://localhost:8080
LEDGER_API_KEY=lk_live_...
PROMETHEUS_URL=http://localhost:9099
```

`LEDGER_API_KEY` is read server-side only (`lib/api/client.ts` and
`lib/prometheus/client.ts` both import the `server-only` package) and never
reaches the browser.

The current mode is always shown in the nav -- a "Mock data" or "Live API"
badge -- so a screenshot or a shared link is never ambiguous about which one
produced it.

## Testing

```bash
pnpm test        # vitest: pure-logic unit tests (money formatting,
                  # pagination-cursor helpers, PromQL builders) plus one
                  # React Testing Library component test
pnpm lint
pnpm build
```

There is no browser/e2e suite in this phase; every view was verified by hand
against `LEDGER_DATA_MODE=mock` (all five views, populated data), then
again against a live `docker compose` stack -- rebuilt and reseeded in
this session, including a real payout saga driven to completion and a real
reconciliation run against a hand-built PSP statement, so every one of the
five views was checked against genuine populated data, not just its empty
state. See D56 in [docs/DECISIONS.md](../docs/DECISIONS.md) for the two
dashboard bugs the live pass caught (both fixed here) and one pre-existing
backend gap it surfaced (`ListRuns` never populating `by_category` --
filed as a follow-up, not fixed in this phase).

## Structure

- `app/` -- one route per view, App Router server components. Each route has
  its own `loading.tsx` (skeleton) and relies on the root `error.tsx` for
  error states; detail routes (`[id]`) also have `not-found.tsx`.
- `components/ui/` -- hand-written shadcn-style primitives (button, card,
  table, badge, tabs, select, sheet, skeleton...).
- `components/ledger/` -- dashboard-specific pieces: money formatting, status
  badges, the balance bar, the saga state machine, pagination links.
- `components/charts/` -- Recharts wrappers for the System Health page.
- `lib/api/` -- wire types mirroring `api/openapi.yaml`, the mock/live
  client, and the mock dataset.
- `lib/prometheus/` -- PromQL query builders (pure, unit-tested) and the
  mock/live Prometheus client for System Health.
