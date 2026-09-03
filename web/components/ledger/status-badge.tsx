import { Badge, type BadgeProps } from "@/components/ui/badge";

const TONE: Record<string, NonNullable<BadgeProps["variant"]>> = {
  // Transactions
  POSTED: "success",
  PENDING: "warning",
  REVERSED: "muted",

  // Accounts
  ACTIVE: "success",
  FROZEN: "warning",
  CLOSED: "muted",

  // Sagas
  COMPLETED: "success",
  COMPENSATED: "muted",
  COMPENSATING: "warning",
  GATEWAY_PENDING: "warning",
  GATEWAY_SUCCEEDED: "success",
  GATEWAY_FAILED: "destructive",
  RESERVED: "warning",
  FAILED: "destructive",
  NEEDS_MANUAL_REVIEW: "destructive",

  // Reconciliation runs
  RUNNING: "warning",

  // Reconciliation exceptions
  OPEN: "destructive",
  AUTO_RESOLVED: "success",
  RESOLVED: "muted",
};

export function StatusBadge({ status, className }: { status: string; className?: string }) {
  return (
    <Badge variant={TONE[status] ?? "outline"} className={className}>
      {status.replaceAll("_", " ")}
    </Badge>
  );
}
