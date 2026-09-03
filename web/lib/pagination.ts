/**
 * Builds the query string for "next page": every existing search param
 * carried forward, with `cursor` replaced (or removed, when there is no next
 * page). Pagination on every list view in this dashboard is a plain link to
 * this URL rather than client-side state, so a page reload or a shared link
 * lands on the same page of results.
 */
export function withCursor(
  current: URLSearchParams | Record<string, string | string[] | undefined>,
  cursor: string | null,
): string {
  const params = current instanceof URLSearchParams ? new URLSearchParams(current) : toURLSearchParams(current);

  if (cursor) {
    params.set("cursor", cursor);
  } else {
    params.delete("cursor");
  }

  const qs = params.toString();
  return qs ? `?${qs}` : "";
}

/** Builds the query string for "apply these filters", always starting a fresh first page (no cursor). */
export function withFilters(filters: Record<string, string | undefined>): string {
  const params = new URLSearchParams();
  for (const [key, value] of Object.entries(filters)) {
    if (value) params.set(key, value);
  }
  const qs = params.toString();
  return qs ? `?${qs}` : "";
}

function toURLSearchParams(record: Record<string, string | string[] | undefined>): URLSearchParams {
  const params = new URLSearchParams();
  for (const [key, value] of Object.entries(record)) {
    if (value === undefined) continue;
    if (Array.isArray(value)) {
      if (value[0] !== undefined) params.set(key, value[0]);
    } else {
      params.set(key, value);
    }
  }
  return params;
}
