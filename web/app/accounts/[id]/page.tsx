import { notFound } from "next/navigation";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Button } from "@/components/ui/button";
import { StatusBadge } from "@/components/ledger/status-badge";
import { MoneyDisplay } from "@/components/ledger/money-display";
import { PageHeader } from "@/components/ledger/page-header";
import { PaginationLink } from "@/components/ledger/pagination-link";
import { EmptyState } from "@/components/ledger/empty-state";
import { Receipt } from "lucide-react";
import { ApiError, getAccount, getBalance, getBalanceAsOf, getStatement } from "@/lib/api/client";

function toDateInputValue(iso: string): string {
  return iso.slice(0, 10);
}

/** A plain "YYYY-MM-DD" from a native date input isn't RFC 3339, which the
 * API requires for `from`/`to`/`as_of` -- Go's time.Parse rejects a bare
 * date. Widened to midnight UTC on that day before it ever reaches the API
 * client. */
function toRFC3339(dateOnly: string | undefined): string | undefined {
  if (!dateOnly) return undefined;
  return new Date(`${dateOnly}T00:00:00.000Z`).toISOString();
}

/** Same widening as toRFC3339, but to the last instant of that day -- used
 * for `to`, so a period ending "today" includes today rather than excluding
 * it entirely. */
function toRFC3339EndOfDay(dateOnly: string | undefined): string | undefined {
  if (!dateOnly) return undefined;
  return new Date(`${dateOnly}T23:59:59.999Z`).toISOString();
}

export default async function AccountDetailPage({
  params,
  searchParams,
}: {
  params: { id: string };
  searchParams: Record<string, string | string[] | undefined>;
}) {
  const one = (v: string | string[] | undefined) => (Array.isArray(v) ? v[0] : v);
  const asOf = one(searchParams.as_of);
  const from = one(searchParams.from);
  const to = one(searchParams.to);
  const cursor = one(searchParams.cursor);

  let account;
  try {
    account = await getAccount(params.id);
  } catch (err) {
    if (err instanceof ApiError && err.status === 404) notFound();
    throw err;
  }

  const asOfRFC3339 = toRFC3339(asOf);
  const [balance, asOfBalance, statement] = await Promise.all([
    getBalance(params.id),
    asOfRFC3339 ? getBalanceAsOf(params.id, asOfRFC3339) : Promise.resolve(null),
    getStatement(params.id, { from: toRFC3339(from), to: toRFC3339EndOfDay(to), cursor, limit: 50 }),
  ]);

  return (
    <div>
      <PageHeader
        title={account.external_ref}
        description={`${account.account_type} · ${account.currency}${account.owner_id ? ` · owner ${account.owner_id}` : ""}`}
        actions={<StatusBadge status={account.status} />}
      />

      <div className="grid gap-4 sm:grid-cols-2 lg:grid-cols-3">
        <Card>
          <CardHeader>
            <CardTitle>Current balance</CardTitle>
          </CardHeader>
          <CardContent>
            <MoneyDisplay money={balance.available} className="text-lg" />
            <p className="mt-1 text-xs text-muted-foreground">version {balance.version}</p>
          </CardContent>
        </Card>

        <Card>
          <CardHeader>
            <CardTitle>Normal balance</CardTitle>
          </CardHeader>
          <CardContent>
            <p className="text-lg font-medium">{account.normal_balance}</p>
            <p className="mt-1 text-xs text-muted-foreground">{account.allow_negative ? "May go negative" : "No overdraft"}</p>
          </CardContent>
        </Card>

        <Card>
          <CardHeader>
            <CardTitle>Balance as of a date</CardTitle>
          </CardHeader>
          <CardContent>
            <form method="GET" className="flex items-center gap-2">
              <Input
                type="date"
                name="as_of"
                defaultValue={asOf ?? ""}
                max={toDateInputValue(new Date().toISOString())}
                className="h-8 text-xs"
              />
              <Button type="submit" size="sm" variant="outline">
                Go
              </Button>
            </form>
            {asOfBalance && (
              <div className="mt-2">
                <MoneyDisplay money={asOfBalance.balance} className="text-base" />
                <p className="mt-1 text-xs text-muted-foreground">
                  as of {new Date(asOfBalance.as_of).toLocaleString()} — bounded-stale by design
                </p>
              </div>
            )}
          </CardContent>
        </Card>
      </div>

      <Card className="mt-6">
        <CardHeader>
          <CardTitle>Statement</CardTitle>
        </CardHeader>
        <CardContent>
          <form method="GET" className="mb-4 flex flex-wrap items-end gap-3">
            <input type="hidden" name="as_of" value={asOf ?? ""} />
            <div className="grid gap-1.5">
              <Label htmlFor="from">From</Label>
              <Input id="from" type="date" name="from" defaultValue={from ?? ""} className="h-8 text-xs" />
            </div>
            <div className="grid gap-1.5">
              <Label htmlFor="to">To</Label>
              <Input id="to" type="date" name="to" defaultValue={to ?? ""} className="h-8 text-xs" />
            </div>
            <Button type="submit" size="sm" variant="outline">
              Apply
            </Button>
          </form>

          {statement.lines.length === 0 ? (
            <EmptyState icon={Receipt} title="No entries in this period" />
          ) : (
            <>
              <div className="mb-3 flex flex-wrap gap-6 text-sm">
                <span>
                  Opening: <MoneyDisplay money={statement.opening} />
                </span>
                <span>
                  Closing: <MoneyDisplay money={statement.closing} />
                </span>
              </div>

              <Table>
                <TableHeader>
                  <TableRow>
                    <TableHead>Date</TableHead>
                    <TableHead>Direction</TableHead>
                    <TableHead className="text-right">Signed amount</TableHead>
                    <TableHead className="text-right">Running balance</TableHead>
                  </TableRow>
                </TableHeader>
                <TableBody>
                  {statement.lines.map((line) => (
                    <TableRow key={line.entry.id}>
                      <TableCell className="whitespace-nowrap text-muted-foreground">
                        {new Date(line.entry.created_at).toLocaleString()}
                      </TableCell>
                      <TableCell>
                        <StatusBadge status={line.entry.direction} />
                      </TableCell>
                      <TableCell className="text-right">
                        <MoneyDisplay money={line.signed} />
                      </TableCell>
                      <TableCell className="text-right">
                        <MoneyDisplay money={line.running_balance} />
                      </TableCell>
                    </TableRow>
                  ))}
                </TableBody>
              </Table>

              <PaginationLink
                basePath={`/accounts/${account.id}`}
                currentParams={searchParams}
                nextCursor={statement.next_cursor}
                hasCursor={Boolean(cursor)}
              />
            </>
          )}
        </CardContent>
      </Card>
    </div>
  );
}
