import Link from "next/link";
import { notFound } from "next/navigation";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table";
import { StatusBadge } from "@/components/ledger/status-badge";
import { MoneyDisplay } from "@/components/ledger/money-display";
import { BalanceBar } from "@/components/ledger/balance-bar";
import { PageHeader } from "@/components/ledger/page-header";
import { ApiError, getTransaction } from "@/lib/api/client";

export default async function TransactionDetailPage({ params }: { params: { id: string } }) {
  let transaction;
  try {
    transaction = await getTransaction(params.id);
  } catch (err) {
    if (err instanceof ApiError && err.status === 404) notFound();
    throw err;
  }

  return (
    <div>
      <PageHeader
        title={`Transaction ${transaction.id.slice(0, 8)}…`}
        description={transaction.external_ref ? `External reference: ${transaction.external_ref}` : undefined}
        actions={<StatusBadge status={transaction.status} />}
      />

      <div className="grid gap-4 sm:grid-cols-2 lg:grid-cols-4">
        <Field label="Type" value={transaction.type} />
        <Field label="Created" value={new Date(transaction.created_at).toLocaleString()} />
        <Field label="Posted" value={transaction.posted_at ? new Date(transaction.posted_at).toLocaleString() : "—"} />
        <Field label="Idempotency key" value={transaction.idempotency_key ?? "—"} mono />
      </div>

      <Card className="mt-6">
        <CardHeader>
          <CardTitle>Debit / credit balance</CardTitle>
        </CardHeader>
        <CardContent>
          <BalanceBar entries={transaction.entries} />
        </CardContent>
      </Card>

      <Card className="mt-4">
        <CardHeader>
          <CardTitle>Journal entries</CardTitle>
        </CardHeader>
        <CardContent className="p-0 sm:p-0">
          <Table>
            <TableHeader>
              <TableRow>
                <TableHead>Seq</TableHead>
                <TableHead>Account</TableHead>
                <TableHead>Direction</TableHead>
                <TableHead className="text-right">Amount</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {transaction.entries.map((e) => (
                <TableRow key={e.id}>
                  <TableCell className="text-muted-foreground">{e.entry_seq}</TableCell>
                  <TableCell className="font-mono text-xs">
                    <Link href={`/accounts/${e.account_id}`} className="text-primary hover:underline">
                      {e.account_id}
                    </Link>
                  </TableCell>
                  <TableCell>
                    <StatusBadge status={e.direction} />
                  </TableCell>
                  <TableCell className="text-right">
                    <MoneyDisplay money={e.amount} />
                  </TableCell>
                </TableRow>
              ))}
            </TableBody>
          </Table>
        </CardContent>
      </Card>

      {transaction.metadata && Object.keys(transaction.metadata).length > 0 && (
        <Card className="mt-4">
          <CardHeader>
            <CardTitle>Metadata</CardTitle>
          </CardHeader>
          <CardContent>
            <pre className="overflow-x-auto rounded-md bg-muted p-3 text-xs">
              {JSON.stringify(transaction.metadata, null, 2)}
            </pre>
          </CardContent>
        </Card>
      )}
    </div>
  );
}

function Field({ label, value, mono = false }: { label: string; value: string; mono?: boolean }) {
  return (
    <Card>
      <CardHeader className="pb-1">
        <CardTitle>{label}</CardTitle>
      </CardHeader>
      <CardContent>
        <p className={mono ? "truncate font-mono text-xs" : "text-sm font-medium"}>{value}</p>
      </CardContent>
    </Card>
  );
}
