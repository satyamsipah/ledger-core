// Synthetic Prometheus responses for LEDGER_DATA_MODE=mock, shaped so the
// System Health page renders a believable "system operating correctly under
// load" picture -- a sine-wave-ish throughput curve, a latency curve that
// tracks it, near-zero outbox lag, and every invariant gauge at zero -- with
// one deliberately nonzero series (a small, steady projection-drift count)
// so the warning-state styling has something real to render in mock mode
// too, rather than only ever being exercised by hand.

function seriesFor(promql: string, t: number): number {
  if (promql.includes("http_requests_total") && promql.includes("status_class")) {
    return Math.max(0, 0.05 + 0.05 * Math.sin(t / 5));
  }
  if (promql.includes("http_requests_total")) {
    return 40 + 25 * Math.sin(t / 8) + 6 * Math.sin(t / 1.3);
  }
  if (promql.includes("transactions_posted_total")) {
    return 12 + 8 * Math.sin(t / 8);
  }
  if (promql.includes("duration_seconds_bucket")) {
    return 0.018 + 0.01 * Math.max(0, Math.sin(t / 6));
  }
  if (promql.includes("outbox_lag_seconds")) {
    return Math.max(0, 1.2 + 0.8 * Math.sin(t / 10));
  }
  if (promql.includes("outbox_backlog")) {
    return Math.max(0, Math.round(3 + 3 * Math.sin(t / 9)));
  }
  if (promql.includes("global_invariant_violation")) {
    return 0;
  }
  if (promql.includes("projection_drift_accounts")) {
    return 0;
  }
  if (promql.includes("orphan_transactions") || promql.includes("orphan_entries")) {
    return 0;
  }
  if (promql.includes("saga_oldest_overdue_seconds")) {
    return 620;
  }
  if (promql.includes("saga_manual_review_total")) {
    return 3;
  }
  return 0;
}

export async function mockQueryInstant(promql: string): Promise<number | null> {
  return seriesFor(promql, Date.now() / 1000);
}

export interface RangePoint {
  t: number;
  v: number;
}

export async function mockQueryRange(promql: string, startSec: number, endSec: number, stepSec: number): Promise<RangePoint[]> {
  const points: RangePoint[] = [];
  for (let t = startSec; t <= endSec; t += stepSec) {
    points.push({ t, v: seriesFor(promql, t) });
  }
  return points;
}
