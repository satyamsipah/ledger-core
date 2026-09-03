"use client";

import { useId } from "react";
import { Area, AreaChart, CartesianGrid, ResponsiveContainer, Tooltip, XAxis, YAxis } from "recharts";

export interface TimeSeriesPoint {
  t: number; // unix seconds
  v: number;
}

// A named format kind, not a formatter function -- this component is a
// Client Component and its props are passed from a Server Component page,
// which means every prop must survive serialization across that boundary.
// A function does not.
export type ValueFormat = "rate" | "latency-ms" | "plain";

function formatterFor(kind: ValueFormat): (v: number) => string {
  switch (kind) {
    case "latency-ms":
      return (v) => `${(v * 1000).toFixed(0)}ms`;
    case "rate":
      return (v) => v.toFixed(1);
    default:
      return (v) => v.toFixed(2);
  }
}

export function TimeSeriesChart({
  data,
  color = "hsl(var(--primary))",
  unit = "",
  valueFormat = "plain",
}: {
  data: TimeSeriesPoint[];
  color?: string;
  unit?: string;
  valueFormat?: ValueFormat;
}) {
  const format = formatterFor(valueFormat);
  const gradientId = `chart-fill-${useId()}`;

  return (
    <ResponsiveContainer width="100%" height={180}>
      <AreaChart data={data} margin={{ top: 8, right: 8, left: 0, bottom: 0 }}>
        <defs>
          <linearGradient id={gradientId} x1="0" y1="0" x2="0" y2="1">
            <stop offset="5%" stopColor={color} stopOpacity={0.35} />
            <stop offset="95%" stopColor={color} stopOpacity={0.02} />
          </linearGradient>
        </defs>
        <CartesianGrid strokeDasharray="3 3" stroke="hsl(var(--border))" vertical={false} />
        <XAxis
          dataKey="t"
          tickFormatter={(t: number) => new Date(t * 1000).toLocaleTimeString([], { hour: "2-digit", minute: "2-digit" })}
          tick={{ fontSize: 11, fill: "hsl(var(--muted-foreground))" }}
          axisLine={{ stroke: "hsl(var(--border))" }}
          tickLine={false}
          minTickGap={40}
        />
        <YAxis
          tick={{ fontSize: 11, fill: "hsl(var(--muted-foreground))" }}
          axisLine={false}
          tickLine={false}
          width={40}
          tickFormatter={(v: number) => format(v)}
        />
        <Tooltip
          contentStyle={{
            background: "hsl(var(--popover))",
            border: "1px solid hsl(var(--border))",
            borderRadius: 8,
            fontSize: 12,
          }}
          labelFormatter={(t: number) => new Date(t * 1000).toLocaleString()}
          formatter={(v: number) => [`${format(v)} ${unit}`, ""]}
        />
        <Area type="monotone" dataKey="v" stroke={color} fill={`url(#${gradientId})`} strokeWidth={2} isAnimationActive={false} />
      </AreaChart>
    </ResponsiveContainer>
  );
}
