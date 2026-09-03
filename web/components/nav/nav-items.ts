import type { LucideIcon } from "lucide-react";
import { Activity, ArrowLeftRight, ScrollText, Wallet, Workflow } from "lucide-react";

export interface NavItem {
  href: string;
  label: string;
  icon: LucideIcon;
}

export const navItems: NavItem[] = [
  { href: "/health", label: "System health", icon: Activity },
  { href: "/transactions", label: "Transactions", icon: ArrowLeftRight },
  { href: "/accounts", label: "Accounts", icon: Wallet },
  { href: "/sagas", label: "Sagas", icon: Workflow },
  { href: "/reconciliation", label: "Reconciliation", icon: ScrollText },
];
