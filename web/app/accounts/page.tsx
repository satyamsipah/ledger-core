import Link from "next/link";
import { Search, Wallet } from "lucide-react";
import { PageHeader } from "@/components/ledger/page-header";
import { EmptyState } from "@/components/ledger/empty-state";
import { StatusBadge } from "@/components/ledger/status-badge";
import { PaginationLink } from "@/components/ledger/pagination-link";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table";
import { searchAccounts } from "@/lib/api/client";

export default async function AccountsPage({
  searchParams,
}: {
  searchParams: Record<string, string | string[] | undefined>;
}) {
  const one = (v: string | string[] | undefined) => (Array.isArray(v) ? v[0] : v);

  const externalRef = one(searchParams.external_ref) ?? "";
  const ownerId = one(searchParams.owner_id) ?? "";
  const currency = one(searchParams.currency) ?? "";
  const cursor = one(searchParams.cursor);

  const page = await searchAccounts({
    external_ref: externalRef || undefined,
    owner_id: ownerId || undefined,
    currency: currency || undefined,
    cursor,
    limit: 50,
  });

  return (
    <div>
      <PageHeader title="Accounts" description="Search the chart of accounts, then open one for its balance and statement." />

      <form method="GET" className="mb-6 grid grid-cols-1 gap-3 sm:grid-cols-2 lg:grid-cols-4 lg:items-end">
        <div className="grid gap-1.5">
          <Label htmlFor="external_ref">External reference</Label>
          <Input id="external_ref" name="external_ref" placeholder="wallet_cust_amara" defaultValue={externalRef} />
        </div>
        <div className="grid gap-1.5">
          <Label htmlFor="owner_id">Owner ID</Label>
          <Input id="owner_id" name="owner_id" placeholder="cust_amara" defaultValue={ownerId} />
        </div>
        <div className="grid gap-1.5">
          <Label htmlFor="currency">Currency</Label>
          <Input id="currency" name="currency" placeholder="INR" defaultValue={currency} maxLength={3} />
        </div>
        <Button type="submit" className="lg:w-fit">
          <Search className="h-4 w-4" />
          Search
        </Button>
      </form>

      {page.accounts.length === 0 ? (
        <EmptyState icon={Wallet} title="No accounts match these filters" description="Try widening the search or clearing a filter." />
      ) : (
        <>
          <Table>
            <TableHeader>
              <TableRow>
                <TableHead>External ref</TableHead>
                <TableHead>Type</TableHead>
                <TableHead>Currency</TableHead>
                <TableHead>Owner</TableHead>
                <TableHead>Status</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {page.accounts.map((a) => (
                <TableRow key={a.id}>
                  <TableCell>
                    <Link href={`/accounts/${a.id}`} className="font-medium text-primary hover:underline">
                      {a.external_ref}
                    </Link>
                  </TableCell>
                  <TableCell>{a.account_type}</TableCell>
                  <TableCell>{a.currency}</TableCell>
                  <TableCell className="text-muted-foreground">{a.owner_id ?? "—"}</TableCell>
                  <TableCell>
                    <StatusBadge status={a.status} />
                  </TableCell>
                </TableRow>
              ))}
            </TableBody>
          </Table>

          <PaginationLink
            basePath="/accounts"
            currentParams={searchParams}
            nextCursor={page.next_cursor}
            hasCursor={Boolean(cursor)}
          />
        </>
      )}
    </div>
  );
}
