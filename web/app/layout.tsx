import type { Metadata } from "next";
import "./globals.css";
import { Shell } from "@/components/nav/shell";
import { DataModeBadge } from "@/components/nav/data-mode-badge";

export const metadata: Metadata = {
  title: "Ledger-Core Admin",
  description: "Ledger explorer, account view, saga monitor, reconciliation and system health.",
};

export default function RootLayout({ children }: { children: React.ReactNode }) {
  return (
    <html lang="en">
      <body className="font-sans antialiased">
        <Shell banner={<DataModeBadge />}>{children}</Shell>
      </body>
    </html>
  );
}
