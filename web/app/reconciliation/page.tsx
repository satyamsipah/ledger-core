import Link from "next/link";
import { ScrollText } from "lucide-react";
import { PageHeader } from "@/components/ledger/page-header";
import { EmptyState } from "@/components/ledger/empty-state";
import { StatusBadge } from "@/components/ledger/status-badge";
import { Badge } from "@/components/ui/badge";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table";
import { listReconciliationRuns } from "@/lib/api/client";

export default async function ReconciliationPage() {
  const { runs } = await listReconciliationRuns(50);

  return (
    <div>
      <PageHeader
        title="Reconciliation"
        description="Three-way match between the ledger, the saga orchestrator, and the PSP settlement statement. Produced on a schedule; read-only here."
      />

      {runs.length === 0 ? (
        <EmptyState icon={ScrollText} title="No reconciliation runs yet" />
      ) : (
        <Table>
          <TableHeader>
            <TableRow>
              <TableHead>Started</TableHead>
              <TableHead>Status</TableHead>
              <TableHead>PSP rows</TableHead>
              <TableHead>Matched</TableHead>
              <TableHead>Exceptions</TableHead>
              <TableHead>By category</TableHead>
            </TableRow>
          </TableHeader>
          <TableBody>
            {runs.map((r) => (
              <TableRow key={r.id}>
                <TableCell className="whitespace-nowrap">
                  <Link href={`/reconciliation/${r.id}`} className="font-medium text-primary hover:underline">
                    {new Date(r.started_at).toLocaleString()}
                  </Link>
                </TableCell>
                <TableCell>
                  <StatusBadge status={r.status} />
                </TableCell>
                <TableCell>{r.psp_row_count}</TableCell>
                <TableCell>{r.matched_count}</TableCell>
                <TableCell>
                  <span className={r.exception_count > 0 ? "font-medium text-destructive" : "text-muted-foreground"}>
                    {r.exception_count}
                  </span>
                </TableCell>
                <TableCell>
                  <div className="flex flex-wrap gap-1">
                    {r.by_category && Object.keys(r.by_category).length > 0 ? (
                      Object.entries(r.by_category).map(([cat, count]) => (
                        <Badge key={cat} variant="outline" className="text-[10px]">
                          {cat.replaceAll("_", " ")}: {count}
                        </Badge>
                      ))
                    ) : (
                      <span className="text-xs text-muted-foreground">none</span>
                    )}
                  </div>
                </TableCell>
              </TableRow>
            ))}
          </TableBody>
        </Table>
      )}
    </div>
  );
}
