"use client";

import { useState } from "react";
import { Menu, ScrollText } from "lucide-react";
import { Button } from "@/components/ui/button";
import { Sheet, SheetContent, SheetTitle, SheetTrigger } from "@/components/ui/sheet";
import { NavLinks } from "@/components/nav/nav-links";

export function Shell({ children, banner }: { children: React.ReactNode; banner?: React.ReactNode }) {
  const [open, setOpen] = useState(false);

  return (
    <div className="flex min-h-screen w-full">
      <aside className="hidden w-60 shrink-0 border-r border-border bg-card/40 p-4 lg:flex lg:flex-col">
        <Brand />
        <div className="mt-6 flex-1">
          <NavLinks />
        </div>
        {banner}
      </aside>

      <div className="flex min-w-0 flex-1 flex-col">
        <header className="flex h-14 items-center gap-3 border-b border-border px-4 lg:hidden">
          <Sheet open={open} onOpenChange={setOpen}>
            <SheetTrigger asChild>
              <Button variant="outline" size="icon" aria-label="Open navigation">
                <Menu className="h-4 w-4" />
              </Button>
            </SheetTrigger>
            <SheetContent side="left">
              <SheetTitle className="sr-only">Navigation</SheetTitle>
              <Brand />
              <div className="mt-6 flex-1">
                <NavLinks onNavigate={() => setOpen(false)} />
              </div>
              {banner}
            </SheetContent>
          </Sheet>
          <Brand compact />
          <div className="ml-auto">{banner}</div>
        </header>

        <main className="min-w-0 flex-1 overflow-x-hidden p-4 sm:p-6 lg:p-8">{children}</main>
      </div>
    </div>
  );
}

function Brand({ compact = false }: { compact?: boolean }) {
  return (
    <div className="flex items-center gap-2">
      <ScrollText className="h-5 w-5 text-primary" />
      <span className="font-semibold tracking-tight">Ledger-Core</span>
      {!compact && <span className="text-xs text-muted-foreground">Admin</span>}
    </div>
  );
}
