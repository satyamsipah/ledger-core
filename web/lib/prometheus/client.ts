import "server-only";

import { mockQueryInstant, mockQueryRange, type RangePoint } from "@/lib/prometheus/mock";

function dataMode(): "mock" | "live" {
  return process.env.LEDGER_DATA_MODE === "live" ? "live" : "mock";
}

function prometheusURL(): string {
  const url = process.env.PROMETHEUS_URL;
  if (!url) throw new Error("PROMETHEUS_URL must be set when LEDGER_DATA_MODE=live");
  return url.replace(/\/+$/, "");
}

interface InstantResponse {
  status: "success" | "error";
  data?: { resultType: string; result: { metric: Record<string, string>; value: [number, string] }[] };
  error?: string;
}

interface RangeResponse {
  status: "success" | "error";
  data?: { resultType: string; result: { metric: Record<string, string>; values: [number, string][] }[] };
  error?: string;
}

/** Runs one instant query, returning the first series' value or null if there was none. */
export async function queryInstant(promql: string): Promise<number | null> {
  if (dataMode() === "mock") return mockQueryInstant(promql);

  const res = await fetch(`${prometheusURL()}/api/v1/query?query=${encodeURIComponent(promql)}`, {
    next: { revalidate: 15 },
  });
  if (!res.ok) throw new Error(`prometheus query failed: ${res.status}`);

  const body = (await res.json()) as InstantResponse;
  if (body.status !== "success") throw new Error(`prometheus query error: ${body.error ?? "unknown"}`);

  const value = body.data?.result[0]?.value;
  return value ? Number(value[1]) : null;
}

/** Runs a range query over [startSec, endSec] in Unix seconds, at stepSec resolution. */
export async function queryRange(promql: string, startSec: number, endSec: number, stepSec: number): Promise<RangePoint[]> {
  if (dataMode() === "mock") return mockQueryRange(promql, startSec, endSec, stepSec);

  const params = new URLSearchParams({
    query: promql,
    start: String(startSec),
    end: String(endSec),
    step: String(stepSec),
  });
  const res = await fetch(`${prometheusURL()}/api/v1/query_range?${params}`, { next: { revalidate: 15 } });
  if (!res.ok) throw new Error(`prometheus query_range failed: ${res.status}`);

  const body = (await res.json()) as RangeResponse;
  if (body.status !== "success") throw new Error(`prometheus query_range error: ${body.error ?? "unknown"}`);

  const values = body.data?.result[0]?.values ?? [];
  return values.map(([t, v]) => ({ t, v: Number(v) }));
}

export type { RangePoint };
