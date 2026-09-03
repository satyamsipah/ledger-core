import Link from "next/link";
import { notFound } from "next/navigation";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table";
import { Button } from "@/components/ui/button";
import { StatusBadge } from "@/components/ledger/status-badge";
import { PageHeader } from "@/components/ledger/page-header";
import { EmptyState } from "@/components/ledger/empty-state";
import { CheckCircle2 } from "lucide-react";
import { ApiError, getReconciliationRun } from "@/lib/api/client";
import { withFilters } from "@/lib/pagination";

export default async function ReconciliationRunPage({
  params,
  searchParams,
}: {
  params: { id: string };
  searchParams: Record<string, string | string[] | undefined>;
}) {
  let run;
  try {
    run = await getReconciliationRun(params.id);
  } catch (err) {
    if (err instanceof ApiError && err.status === 404) notFound();
    throw err;
  }

  const one = (v: string | string[] | undefined) => (Array.isArray(v) ? v[0] : v);
  const categoryFilter = one(searchParams.category);

  const exceptions = run.exceptions ?? [];
  const filtered = categoryFilter ? exceptions.filter((e) => e.category === categoryFilter) : exceptions;
  const categories = Object.keys(run.by_category ?? {});

  return (
    <div>
      <PageHeader
        title={`Run ${new Date(run.started_at).toLocaleString()}`}
        description={run.source}
        actions={<StatusBadge status={run.status} />}
      />

      <div className="grid gap-4 sm:grid-cols-2 lg:grid-cols-4">
        <Tile label="PSP rows" value={run.psp_row_count} />
        <Tile label="Matched" value={run.matched_count} />
        <Tile label="Auto-resolved" value={run.auto_resolved_count} />
        <Tile label="Exceptions" value={run.exception_count} destructive={run.exception_count > 0} />
      </div>

      {categories.length > 0 && (
        <div className="mt-6 flex flex-wrap items-center gap-2">
          <span className="text-xs font-medium text-muted-foreground">Filter by category:</span>
          <Button asChild variant={categoryFilter ? "outline" : "secondary"} size="sm">
            <Link href={`/reconciliation/${run.id}`}>All ({exceptions.length})</Link>
          </Button>
          {categories.map((cat) => (
            <Button key={cat} asChild variant={categoryFilter === cat ? "secondary" : "outline"} size="sm">
              <Link href={`/reconciliation/${run.id}${withFilters({ category: cat })}`}>
                {cat.replaceAll("_", " ")} ({run.by_category?.[cat] ?? 0})
              </Link>
            </Button>
          ))}
        </div>
      )}

      <Card className="mt-4">
        <CardHeader>
          <CardTitle>Exceptions</CardTitle>
        </CardHeader>
        <CardContent>
          {filtered.length === 0 ? (
            <EmptyState icon={CheckCircle2} title="No exceptions in this category" />
          ) : (
            <Table>
              <TableHeader>
                <TableRow>
                  <TableHead>External ref</TableHead>
                  <TableHead>Category</TableHead>
                  <TableHead>Status</TableHead>
                  <TableHead>Ledger tx</TableHead>
                  <TableHead>Detail</TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {filtered.map((e) => (
                  <TableRow key={e.id}>
                    <TableCell className="font-mono text-xs">{e.external_ref}</TableCell>
                    <TableCell>{e.category.replaceAll("_", " ")}</TableCell>
                    <TableCell>
                      <StatusBadge status={e.status} />
                    </TableCell>
                    <TableCell className="font-mono text-xs">
                      {e.ledger_transaction_id ? (
                        <Link href={`/transactions/${e.ledger_transaction_id}`} className="text-primary hover:underline">
                          {e.ledger_transaction_id.slice(0, 8)}…
                        </Link>
                      ) : (
                        "—"
                      )}
                    </TableCell>
                    <TableCell className="text-xs text-muted-foreground">
                      {e.category === "AMOUNT_MISMATCH" && e.ledger_amount_minor !== undefined && e.psp_amount_minor !== undefined
                        ? `ledger ${e.ledger_amount_minor} vs psp ${e.psp_amount_minor} ${e.currency ?? ""}`
                        : e.category === "STATUS_MISMATCH"
                          ? `ledger ${e.ledger_status} vs psp ${e.psp_status}`
                          : "—"}
                    </TableCell>
                  </TableRow>
                ))}
              </TableBody>
            </Table>
          )}
        </CardContent>
      </Card>
    </div>
  );
}

function Tile({ label, value, destructive = false }: { label: string; value: number; destructive?: boolean }) {
  return (
    <Card>
      <CardHeader>
        <CardTitle>{label}</CardTitle>
      </CardHeader>
      <CardContent>
        <p className={destructive ? "text-lg font-semibold text-destructive" : "text-lg font-semibold"}>{value}</p>
      </CardContent>
    </Card>
  );
}
