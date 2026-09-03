import Link from "next/link";
import { notFound } from "next/navigation";
import { formatDistanceStrict } from "date-fns";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { StatusBadge } from "@/components/ledger/status-badge";
import { SagaStateMachine } from "@/components/ledger/saga-state-machine";
import { PageHeader } from "@/components/ledger/page-header";
import { AutoRefresh } from "@/components/ledger/auto-refresh";
import { ApiError, getSaga } from "@/lib/api/client";

export default async function SagaDetailPage({ params }: { params: { id: string } }) {
  let saga;
  try {
    saga = await getSaga(params.id);
  } catch (err) {
    if (err instanceof ApiError && err.status === 404) notFound();
    throw err;
  }

  const compensating = saga.status === "COMPENSATING" || saga.status === "COMPENSATED";

  return (
    <div>
      <AutoRefresh seconds={10} />
      <PageHeader
        title={`Saga ${saga.id.slice(0, 8)}…`}
        description={`${saga.saga_type} · created ${new Date(saga.created_at).toLocaleString()}`}
        actions={<StatusBadge status={saga.status} />}
      />

      <Card>
        <CardHeader>
          <CardTitle>State machine</CardTitle>
        </CardHeader>
        <CardContent>
          <SagaStateMachine currentStep={saga.current_step} compensating={compensating} />
          {saga.last_error && (
            <p className="mt-4 rounded-md bg-destructive/10 p-3 text-sm text-destructive">{saga.last_error}</p>
          )}
        </CardContent>
      </Card>

      <Card className="mt-4">
        <CardHeader>
          <CardTitle>Attempt history</CardTitle>
        </CardHeader>
        <CardContent>
          {!saga.attempts || saga.attempts.length === 0 ? (
            <p className="text-sm text-muted-foreground">No attempts recorded yet.</p>
          ) : (
            <ol className="relative space-y-6 border-l border-border pl-6">
              {saga.attempts.map((a, i) => (
                <li key={i} className="relative">
                  <span className="absolute -left-[27px] top-1 h-2.5 w-2.5 rounded-full border-2 border-background bg-primary" />
                  <div className="flex flex-wrap items-center gap-2">
                    <span className="text-sm font-medium">{a.step}</span>
                    <span className="text-xs text-muted-foreground">{a.direction === "COMPENSATION" ? "compensation" : "forward"}</span>
                    <span className="text-xs text-muted-foreground">attempt {a.attempt}</span>
                    <StatusBadge status={a.status} />
                  </div>
                  <p className="mt-0.5 text-xs text-muted-foreground">
                    {new Date(a.started_at).toLocaleString()}
                    {a.finished_at &&
                      ` → took ${formatDistanceStrict(new Date(a.finished_at), new Date(a.started_at))}`}
                  </p>
                  {a.error && <p className="mt-1 text-xs text-destructive">{a.error}</p>}
                  {a.transaction_id && (
                    <Link href={`/transactions/${a.transaction_id}`} className="mt-1 block font-mono text-xs text-primary hover:underline">
                      transaction {a.transaction_id.slice(0, 8)}…
                    </Link>
                  )}
                </li>
              ))}
            </ol>
          )}
        </CardContent>
      </Card>
    </div>
  );
}
