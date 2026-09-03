import { TableSkeleton } from "@/components/ledger/skeletons";
import { Skeleton } from "@/components/ui/skeleton";

export default function Loading() {
  return (
    <div>
      <Skeleton className="mb-6 h-8 w-64" />
      <TableSkeleton rows={8} cols={5} />
    </div>
  );
}
