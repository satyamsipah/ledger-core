import { ScrollText } from "lucide-react";
import { EmptyState } from "@/components/ledger/empty-state";

export default function NotFound() {
  return <EmptyState icon={ScrollText} title="No such reconciliation run" description="It may not exist, or the id was mistyped." />;
}
