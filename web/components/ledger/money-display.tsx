import { cn } from "@/lib/utils";
import { formatMoney, isNegative } from "@/lib/money";
import type { Money } from "@/lib/api/types";

export function MoneyDisplay({ money, className }: { money: Money; className?: string }) {
  return (
    <span className={cn("font-mono tabular-nums", isNegative(money) && "text-destructive", className)}>
      {formatMoney(money)}
    </span>
  );
}
