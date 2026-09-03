import Link from "next/link";
import { formatDistanceToNow } from "date-fns";
import { AlertOctagon, Workflow } from "lucide-react";
import { PageHeader } from "@/components/ledger/page-header";
import { AutoRefresh } from "@/components/ledger/auto-refresh";
import { EmptyState } from "@/components/ledger/empty-state";
import { StatusBadge } from "@/components/ledger/status-badge";
import { NativeSelect } from "@/components/ledger/native-select";
import { Button } from "@/components/ui/button";
import { Label } from "@/components/ui/label";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table";
import { listSagas } from "@/lib/api/client";
import type { SagaStatus } from "@/lib/api/types";
import { cn } from "@/lib/utils";

const STATUSES: SagaStatus[] = [
  "NEEDS_MANUAL_REVIEW",
  "GATEWAY_PENDING",
  "PENDING",
  "RESERVED",
  "GATEWAY_SUCCEEDED",
  "GATEWAY_FAILED",
  "COMPENSATING",
  "COMPLETED",
  "COMPENSATED",
  "FAILED",
];

// A non-terminal saga stuck past this age is flagged, independent of what
// the status itself already implies -- NEEDS_MANUAL_REVIEW is already an
// alert, but a GATEWAY_PENDING saga sitting for an hour is the "stuck sagas"
// case this view specifically exists to surface before it becomes one.
const STUCK_AFTER_MINUTES = 30;
const NON_TERMINAL: SagaStatus[] = ["PENDING", "RESERVED", "GATEWAY_PENDING", "COMPENSATING"];

export default async function SagasPage({
  searchParams,
}: {
  searchParams: Record<string, string | string[] | undefined>;
}) {
  const one = (v: string | string[] | undefined) => (Array.isArray(v) ? v[0] : v);
  const status = (one(searchParams.status) as SagaStatus | undefined) ?? "NEEDS_MANUAL_REVIEW";

  const page = await listSagas(status, 100);
  const now = Date.now();

  return (
    <div>
      <AutoRefresh seconds={15} />
      <PageHeader
        title="Saga monitor"
        description="Multi-step payout sagas. Defaults to the triage queue: what needs a human. Refreshes automatically every 15s."
      />

      <form method="GET" className="mb-6 flex flex-wrap items-end gap-3">
        <div className="grid gap-1.5">
          <Label htmlFor="status">Status</Label>
          <div className="w-56">
            <NativeSelect id="status" name="status" defaultValue={status} options={STATUSES} placeholder="Any" />
          </div>
        </div>
        <Button type="submit" variant="outline">
          Filter
        </Button>
      </form>

      {page.sagas.length === 0 ? (
        <EmptyState icon={Workflow} title={`No sagas in ${status.replaceAll("_", " ")}`} />
      ) : (
        <Table>
          <TableHeader>
            <TableRow>
              <TableHead>ID</TableHead>
              <TableHead>Status</TableHead>
              <TableHead>Current step</TableHead>
              <TableHead>Retries</TableHead>
              <TableHead>Updated</TableHead>
            </TableRow>
          </TableHeader>
          <TableBody>
            {page.sagas.map((s) => {
              const ageMinutes = (now - new Date(s.updated_at).getTime()) / 60000;
              const stuck = NON_TERMINAL.includes(s.status) && ageMinutes > STUCK_AFTER_MINUTES;
              return (
                <TableRow key={s.id} className={cn(stuck && "bg-destructive/5")}>
                  <TableCell className="font-mono text-xs">
                    <Link href={`/sagas/${s.id}`} className="text-primary hover:underline">
                      {s.id.slice(0, 8)}…
                    </Link>
                  </TableCell>
                  <TableCell>
                    <div className="flex items-center gap-1.5">
                      <StatusBadge status={s.status} />
                      {stuck && <AlertOctagon className="h-3.5 w-3.5 text-destructive" aria-label="Stuck" />}
                    </div>
                  </TableCell>
                  <TableCell>{s.current_step}</TableCell>
                  <TableCell>{s.retry_count}</TableCell>
                  <TableCell className="whitespace-nowrap text-muted-foreground">
                    {formatDistanceToNow(new Date(s.updated_at), { addSuffix: true })}
                  </TableCell>
                </TableRow>
              );
            })}
          </TableBody>
        </Table>
      )}
    </div>
  );
}
