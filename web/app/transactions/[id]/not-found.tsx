import { ArrowLeftRight } from "lucide-react";
import { EmptyState } from "@/components/ledger/empty-state";

export default function NotFound() {
  return <EmptyState icon={ArrowLeftRight} title="No such transaction" description="It may not exist, or the id was mistyped." />;
}
