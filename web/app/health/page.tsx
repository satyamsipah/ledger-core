import { Activity, Clock, Inbox, Timer } from "lucide-react";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { PageHeader } from "@/components/ledger/page-header";
import { StatTile } from "@/components/ledger/stat-tile";
import { InvariantTile } from "@/components/ledger/invariant-tile";
import { AutoRefresh } from "@/components/ledger/auto-refresh";
import { TimeSeriesChart } from "@/components/charts/time-series-chart";
import { queryInstant, queryRange } from "@/lib/prometheus/client";
import {
  globalInvariantViolationQuery,
  httpRequestRateQuery,
  latencyQuantileQuery,
  orphanEntriesQuery,
  orphanTransactionsQuery,
  outboxBacklogQuery,
  outboxLagSecondsQuery,
  projectionDriftAccountsQuery,
  transactionThroughputQuery,
} from "@/lib/prometheus/queries";

const WINDOW_MINUTES = 60;
const STEP_SECONDS = 60;

export default async function HealthPage() {
  const nowSec = Math.floor(Date.now() / 1000);
  const startSec = nowSec - WINDOW_MINUTES * 60;

  const [
    throughputNow,
    p95Now,
    outboxLag,
    outboxBacklog,
    globalInvariant,
    projectionDrift,
    orphanTx,
    orphanEntries,
    throughputSeries,
    p50Series,
    p95Series,
  ] = await Promise.all([
    queryInstant(httpRequestRateQuery()),
    queryInstant(latencyQuantileQuery(0.95)),
    queryInstant(outboxLagSecondsQuery()),
    queryInstant(outboxBacklogQuery()),
    queryInstant(globalInvariantViolationQuery()),
    queryInstant(projectionDriftAccountsQuery()),
    queryInstant(orphanTransactionsQuery()),
    queryInstant(orphanEntriesQuery()),
    queryRange(transactionThroughputQuery(), startSec, nowSec, STEP_SECONDS),
    queryRange(latencyQuantileQuery(0.5), startSec, nowSec, STEP_SECONDS),
    queryRange(latencyQuantileQuery(0.95), startSec, nowSec, STEP_SECONDS),
  ]);

  return (
    <div>
      <AutoRefresh seconds={15} />
      <PageHeader title="System health" description="Throughput, latency, outbox lag and invariant status. Refreshes automatically every 15s." />

      <div className="grid gap-4 sm:grid-cols-2 lg:grid-cols-4">
        <StatTile label="HTTP throughput" value={(throughputNow ?? 0).toFixed(1)} suffix="req/s" icon={Activity} />
        <StatTile label="p95 latency" value={((p95Now ?? 0) * 1000).toFixed(0)} suffix="ms" icon={Timer} />
        <StatTile label="Outbox lag" value={(outboxLag ?? 0).toFixed(1)} suffix="s" icon={Clock} tone={(outboxLag ?? 0) > 30 ? "warning" : "default"} />
        <StatTile label="Outbox backlog" value={String(Math.round(outboxBacklog ?? 0))} suffix="rows" icon={Inbox} />
      </div>

      <h2 className="mb-3 mt-8 text-sm font-medium text-muted-foreground">Invariant status</h2>
      <div className="grid gap-4 sm:grid-cols-2 lg:grid-cols-4">
        <InvariantTile label="Journal balance (invariant 1)" value={globalInvariant} />
        <InvariantTile label="Projection drift accounts" value={projectionDrift} />
        <InvariantTile label="Orphan transactions" value={orphanTx} />
        <InvariantTile label="Orphan entries" value={orphanEntries} />
      </div>

      <div className="mt-8 grid gap-4 lg:grid-cols-2">
        <Card>
          <CardHeader>
            <CardTitle>Transaction throughput (last hour)</CardTitle>
          </CardHeader>
          <CardContent>
            <TimeSeriesChart data={throughputSeries} unit="tx/s" valueFormat="rate" />
          </CardContent>
        </Card>

        <Card>
          <CardHeader>
            <CardTitle>HTTP latency, p50 vs p95 (last hour)</CardTitle>
          </CardHeader>
          <CardContent>
            <TimeSeriesChart data={p95Series} color="hsl(var(--warning))" unit="p95" valueFormat="latency-ms" />
            <div className="mt-2">
              <TimeSeriesChart data={p50Series} color="hsl(var(--primary))" unit="p50" valueFormat="latency-ms" />
            </div>
          </CardContent>
        </Card>
      </div>
    </div>
  );
}
