import { Wallet } from "lucide-react";
import { EmptyState } from "@/components/ledger/empty-state";

export default function NotFound() {
  return <EmptyState icon={Wallet} title="No such account" description="It may not exist, or the id was mistyped." />;
}
