import { Workflow } from "lucide-react";
import { EmptyState } from "@/components/ledger/empty-state";

export default function NotFound() {
  return <EmptyState icon={Workflow} title="No such saga" description="It may not exist, or the id was mistyped." />;
}
