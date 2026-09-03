import { cn } from "@/lib/utils";
import { formatMoney } from "@/lib/money";
import type { Entry } from "@/lib/api/types";

const SEGMENT_COLORS = [
  "bg-primary",
  "bg-sky-500",
  "bg-violet-500",
  "bg-amber-500",
  "bg-rose-500",
  "bg-emerald-500",
];

/**
 * Two stacked bars -- every DEBIT leg, every CREDIT leg -- each segment's
 * width proportional to its share of that side's total. A transaction is
 * balanced exactly when the two totals are equal (invariant 1), so both bars
 * always render the same total width; what differs is how each side's total
 * is divided among its accounts. This is a direct visual proof of the
 * invariant, not merely a decoration: if the two bars were ever unequal
 * lengths, that would mean this transaction should not exist.
 */
export function BalanceBar({ entries }: { entries: Entry[] }) {
  const debits = entries.filter((e) => e.direction === "DEBIT");
  const credits = entries.filter((e) => e.direction === "CREDIT");
  const currency = entries[0]?.amount.currency ?? "";

  const debitTotal = debits.reduce((s, e) => s + Number(e.amount.amount), 0);
  const creditTotal = credits.reduce((s, e) => s + Number(e.amount.amount), 0);
  const total = Math.max(debitTotal, creditTotal, 1);

  return (
    <div className="space-y-3">
      <BarRow label="Debit" entries={debits} total={total} />
      <BarRow label="Credit" entries={credits} total={total} />
      <p className="text-xs text-muted-foreground">
        {debitTotal === creditTotal ? (
          <span className="text-success">Balanced — debits equal credits at {formatMoney({ amount: String(debitTotal), currency })}</span>
        ) : (
          <span className="text-destructive">
            Unbalanced: debits {formatMoney({ amount: String(debitTotal), currency })}, credits{" "}
            {formatMoney({ amount: String(creditTotal), currency })}
          </span>
        )}
      </p>
    </div>
  );
}

function BarRow({ label, entries, total }: { label: string; entries: Entry[]; total: number }) {
  return (
    <div>
      <div className="mb-1 flex items-center justify-between text-xs text-muted-foreground">
        <span className="font-medium">{label}</span>
      </div>
      <div className="flex h-6 w-full overflow-hidden rounded-md bg-muted">
        {entries.map((e, i) => {
          const width = (Number(e.amount.amount) / total) * 100;
          return (
            <div
              key={e.id}
              className={cn("flex items-center justify-center text-[10px] font-medium text-white", SEGMENT_COLORS[i % SEGMENT_COLORS.length])}
              style={{ width: `${width}%` }}
              title={`${e.account_id}: ${formatMoney(e.amount)}`}
            >
              {width > 12 ? formatMoney(e.amount) : ""}
            </div>
          );
        })}
      </div>
    </div>
  );
}
