"use client";

import { AlertTriangle } from "lucide-react";
import { Button } from "@/components/ui/button";
import { ApiError } from "@/lib/api/types";

export function ErrorState({ error, reset }: { error: Error & { digest?: string }; reset: () => void }) {
  const isApiError = error instanceof ApiError;

  return (
    <div className="flex flex-col items-center gap-3 rounded-lg border border-destructive/30 bg-destructive/5 py-16 text-center">
      <AlertTriangle className="h-8 w-8 text-destructive" />
      <p className="text-sm font-medium">
        {isApiError ? `The API returned ${(error as ApiError).status}` : "Something went wrong loading this page"}
      </p>
      <p className="max-w-md text-sm text-muted-foreground">
        {isApiError ? (error as ApiError).problem?.detail ?? error.message : error.message}
      </p>
      <Button onClick={reset} variant="outline" size="sm" className="mt-2">
        Try again
      </Button>
    </div>
  );
}
