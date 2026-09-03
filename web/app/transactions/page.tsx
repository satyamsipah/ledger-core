import Link from "next/link";
import { ArrowLeftRight, Search } from "lucide-react";
import { PageHeader } from "@/components/ledger/page-header";
import { EmptyState } from "@/components/ledger/empty-state";
import { StatusBadge } from "@/components/ledger/status-badge";
import { PaginationLink } from "@/components/ledger/pagination-link";
import { NativeSelect } from "@/components/ledger/native-select";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table";
import { searchTransactions } from "@/lib/api/client";
import type { TransactionStatus, TransactionType } from "@/lib/api/types";

const STATUSES: TransactionStatus[] = ["PENDING", "POSTED", "REVERSED"];
const TYPES: TransactionType[] = ["TRANSFER", "PAYIN", "PAYOUT", "FEE", "FX", "REVERSAL", "ADJUSTMENT"];

export default async function TransactionsPage({
  searchParams,
}: {
  searchParams: Record<string, string | string[] | undefined>;
}) {
  const one = (v: string | string[] | undefined) => (Array.isArray(v) ? v[0] : v);

  const externalRef = one(searchParams.external_ref) ?? "";
  const status = one(searchParams.status) ?? "";
  const type = one(searchParams.type) ?? "";
  const accountId = one(searchParams.account_id) ?? "";
  const cursor = one(searchParams.cursor);

  const page = await searchTransactions({
    external_ref: externalRef || undefined,
    status: (status || undefined) as TransactionStatus | undefined,
    type: (type || undefined) as TransactionType | undefined,
    account_id: accountId || undefined,
    cursor,
    limit: 50,
  });

  return (
    <div>
      <PageHeader
        title="Transactions"
        description="Search the journal, then drill into any transaction's balanced entries."
      />

      <form method="GET" className="mb-6 grid grid-cols-1 gap-3 sm:grid-cols-2 lg:grid-cols-5 lg:items-end">
        <div className="grid gap-1.5">
          <Label htmlFor="external_ref">External reference</Label>
          <Input id="external_ref" name="external_ref" placeholder="gw_8f2a41" defaultValue={externalRef} />
        </div>
        <div className="grid gap-1.5">
          <Label htmlFor="status">Status</Label>
          <NativeSelect id="status" name="status" defaultValue={status} options={["", ...STATUSES]} />
        </div>
        <div className="grid gap-1.5">
          <Label htmlFor="type">Type</Label>
          <NativeSelect id="type" name="type" defaultValue={type} options={["", ...TYPES]} />
        </div>
        <div className="grid gap-1.5">
          <Label htmlFor="account_id">Account ID</Label>
          <Input id="account_id" name="account_id" placeholder="uuid" defaultValue={accountId} className="font-mono text-xs" />
        </div>
        <Button type="submit" className="lg:w-fit">
          <Search className="h-4 w-4" />
          Search
        </Button>
      </form>

      {page.transactions.length === 0 ? (
        <EmptyState icon={ArrowLeftRight} title="No transactions match these filters" description="Try widening the search or clearing a filter." />
      ) : (
        <>
          <Table>
            <TableHeader>
              <TableRow>
                <TableHead>ID</TableHead>
                <TableHead>Type</TableHead>
                <TableHead>Status</TableHead>
                <TableHead>External ref</TableHead>
                <TableHead>Created</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {page.transactions.map((t) => (
                <TableRow key={t.id}>
                  <TableCell className="font-mono text-xs">
                    <Link href={`/transactions/${t.id}`} className="text-primary hover:underline">
                      {t.id.slice(0, 8)}…
                    </Link>
                  </TableCell>
                  <TableCell>{t.type}</TableCell>
                  <TableCell>
                    <StatusBadge status={t.status} />
                  </TableCell>
                  <TableCell className="text-muted-foreground">{t.external_ref ?? "—"}</TableCell>
                  <TableCell className="whitespace-nowrap text-muted-foreground">
                    {new Date(t.created_at).toLocaleString()}
                  </TableCell>
                </TableRow>
              ))}
            </TableBody>
          </Table>

          <PaginationLink
            basePath="/transactions"
            currentParams={searchParams}
            nextCursor={page.next_cursor}
            hasCursor={Boolean(cursor)}
          />
        </>
      )}
    </div>
  );
}
