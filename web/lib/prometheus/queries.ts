// PromQL builders for the System Health page. Pure string builders, kept
// separate from the fetch client so they're trivial to unit test without a
// network or a running Prometheus.
//
// Metric names mirror internal/observability/metrics.go exactly -- see that
// file's Namespace/Subsystem/Name fields for where each one comes from.

export function httpRequestRateQuery(window = "5m"): string {
  return `sum(rate(ledger_http_requests_total[${window}]))`;
}

export function httpRequestRateByRouteQuery(window = "5m"): string {
  return `sum by (route) (rate(ledger_http_requests_total[${window}]))`;
}

export function httpErrorRateQuery(window = "5m"): string {
  return `sum(rate(ledger_http_requests_total{status_class=~"5.."}[${window}]))`;
}

/** quantile is a fraction in [0, 1], e.g. 0.95 for p95. */
export function latencyQuantileQuery(quantile: number, window = "5m"): string {
  return `histogram_quantile(${quantile}, sum(rate(ledger_http_request_duration_seconds_bucket[${window}])) by (le))`;
}

export function transactionThroughputQuery(window = "5m"): string {
  return `sum(rate(ledger_transactions_posted_total[${window}]))`;
}

export function outboxLagSecondsQuery(): string {
  return `max(ledger_outbox_lag_seconds)`;
}

export function outboxBacklogQuery(): string {
  return `sum(ledger_outbox_backlog)`;
}

export function globalInvariantViolationQuery(): string {
  return `sum(ledger_consistency_global_invariant_violation_minor)`;
}

export function projectionDriftAccountsQuery(): string {
  return `sum(ledger_consistency_projection_drift_accounts)`;
}

export function orphanTransactionsQuery(): string {
  return `sum(ledger_consistency_orphan_transactions)`;
}

export function orphanEntriesQuery(): string {
  return `sum(ledger_consistency_orphan_entries)`;
}

export function sagaOldestOverdueSecondsQuery(): string {
  return `max(ledger_saga_oldest_overdue_seconds)`;
}

export function sagaManualReviewRateQuery(window = "1h"): string {
  return `sum(increase(ledger_saga_manual_review_total[${window}]))`;
}
