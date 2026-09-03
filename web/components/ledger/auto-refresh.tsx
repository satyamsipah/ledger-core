"use client";

import { useEffect } from "react";
import { useRouter } from "next/navigation";

/**
 * Re-runs the current route's server-side data fetch on an interval, via
 * router.refresh() -- no client-side polling of the API, no websocket, just
 * the same server render Next.js already knows how to produce, repeated.
 * This is what makes the saga monitor and system health pages "live" without
 * either page owning any fetch logic of its own.
 */
export function AutoRefresh({ seconds = 10 }: { seconds?: number }) {
  const router = useRouter();

  useEffect(() => {
    const id = setInterval(() => router.refresh(), seconds * 1000);
    return () => clearInterval(id);
  }, [router, seconds]);

  return null;
}
