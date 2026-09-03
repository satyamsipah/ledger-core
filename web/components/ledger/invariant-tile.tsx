import { CheckCircle2, HelpCircle, XCircle } from "lucide-react";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { cn } from "@/lib/utils";

/**
 * Every one of these gauges is documented in internal/observability/metrics.go
 * as "alert if ever nonzero" -- there is no acceptable nonzero value once the
 * metric is reporting. But "no data" (the reconciler that emits it isn't
 * running or hasn't checked yet) is a THIRD state, distinct from both: it
 * must never render as "violated" -- a missing metric is not evidence of a
 * broken invariant, and conflating the two would make an operator distrust
 * this tile the first time a scrape gap produced a false alarm.
 */
export function InvariantTile({ label, value }: { label: string; value: number | null }) {
  const state: "unknown" | "healthy" | "violated" = value === null ? "unknown" : value === 0 ? "healthy" : "violated";

  return (
    <Card className={cn(state === "violated" && "border-destructive/40 bg-destructive/5")}>
      <CardHeader className="flex-row items-center justify-between space-y-0 pb-1">
        <CardTitle>{label}</CardTitle>
        {state === "unknown" && <HelpCircle className="h-4 w-4 text-muted-foreground" />}
        {state === "healthy" && <CheckCircle2 className="h-4 w-4 text-success" />}
        {state === "violated" && <XCircle className="h-4 w-4 text-destructive" />}
      </CardHeader>
      <CardContent>
        <p
          className={cn(
            "text-2xl font-semibold tabular-nums",
            state === "healthy" && "text-success",
            state === "violated" && "text-destructive",
            state === "unknown" && "text-muted-foreground",
          )}
        >
          {value === null ? "—" : value}
        </p>
        <p className="mt-0.5 text-xs text-muted-foreground">
          {state === "healthy" && "Holding"}
          {state === "violated" && "Violated — page immediately"}
          {state === "unknown" && "No data — check the reconciler"}
        </p>
      </CardContent>
    </Card>
  );
}
