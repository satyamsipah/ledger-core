import { Badge } from "@/components/ui/badge";
import { dataMode } from "@/lib/api/client";

/**
 * Shown in the nav shell at all times, on every screen size. Which backend a
 * page is reading from is not an implementation detail an operator should
 * have to infer from whether the numbers look plausible -- especially since
 * "mock" data is deliberately built to look plausible.
 */
export function DataModeBadge() {
  const mode = dataMode();
  return (
    <Badge variant={mode === "live" ? "success" : "muted"} className="w-fit">
      {mode === "live" ? "Live API" : "Mock data"}
    </Badge>
  );
}
