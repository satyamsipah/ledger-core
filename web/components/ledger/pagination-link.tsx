import Link from "next/link";
import { ArrowLeft, ArrowRight } from "lucide-react";
import { Button } from "@/components/ui/button";
import { withCursor } from "@/lib/pagination";

/**
 * "Next page" / "back to the first page" as plain links, not client-side
 * state: the URL is the whole of a list view's state, so a page reload or a
 * shared link lands on the same page of results.
 */
export function PaginationLink({
  basePath,
  currentParams,
  nextCursor,
  hasCursor,
}: {
  basePath: string;
  currentParams: Record<string, string | string[] | undefined>;
  nextCursor: string | null;
  hasCursor: boolean;
}) {
  return (
    <div className="mt-4 flex items-center justify-between">
      {hasCursor ? (
        <Button asChild variant="outline" size="sm">
          <Link href={basePath + withCursor(currentParams, null)}>
            <ArrowLeft className="h-3.5 w-3.5" />
            First page
          </Link>
        </Button>
      ) : (
        <span />
      )}

      {nextCursor && (
        <Button asChild variant="outline" size="sm">
          <Link href={basePath + withCursor(currentParams, nextCursor)}>
            Next page
            <ArrowRight className="h-3.5 w-3.5" />
          </Link>
        </Button>
      )}
    </div>
  );
}
