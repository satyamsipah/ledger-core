import { describe, expect, it } from "vitest";
import {
  globalInvariantViolationQuery,
  httpErrorRateQuery,
  httpRequestRateQuery,
  latencyQuantileQuery,
  orphanEntriesQuery,
  orphanTransactionsQuery,
  outboxBacklogQuery,
  outboxLagSecondsQuery,
  projectionDriftAccountsQuery,
  sagaOldestOverdueSecondsQuery,
  transactionThroughputQuery,
} from "@/lib/prometheus/queries";

describe("prometheus query builders", () => {
  it("should build a throughput rate query over the given window", () => {
    expect(httpRequestRateQuery("5m")).toBe("sum(rate(ledger_http_requests_total[5m]))");
  });

  it("should default the window to 5m when none is given", () => {
    expect(httpRequestRateQuery()).toContain("[5m]");
  });

  it("should filter the error-rate query to the 5xx status class", () => {
    const q = httpErrorRateQuery();
    expect(q).toContain('status_class=~"5.."');
  });

  it("should build a histogram_quantile query with the requested quantile and window", () => {
    const q = latencyQuantileQuery(0.95, "1m");
    expect(q).toBe(
      "histogram_quantile(0.95, sum(rate(ledger_http_request_duration_seconds_bucket[1m])) by (le))",
    );
  });

  it("should name the metrics that back every system-health tile, matching internal/observability/metrics.go", () => {
    expect(transactionThroughputQuery()).toContain("ledger_transactions_posted_total");
    expect(outboxLagSecondsQuery()).toContain("ledger_outbox_lag_seconds");
    expect(outboxBacklogQuery()).toContain("ledger_outbox_backlog");
    expect(globalInvariantViolationQuery()).toContain("ledger_consistency_global_invariant_violation_minor");
    expect(projectionDriftAccountsQuery()).toContain("ledger_consistency_projection_drift_accounts");
    expect(orphanTransactionsQuery()).toContain("ledger_consistency_orphan_transactions");
    expect(orphanEntriesQuery()).toContain("ledger_consistency_orphan_entries");
    expect(sagaOldestOverdueSecondsQuery()).toContain("ledger_saga_oldest_overdue_seconds");
  });
});
