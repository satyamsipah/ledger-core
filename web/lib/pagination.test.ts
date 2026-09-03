import { describe, expect, it } from "vitest";
import { withCursor, withFilters } from "@/lib/pagination";

describe("withCursor", () => {
  it("should set the cursor param while keeping existing filters", () => {
    const current = new URLSearchParams({ status: "POSTED", limit: "20" });
    const url = withCursor(current, "abc123");
    const params = new URLSearchParams(url.slice(1));
    expect(params.get("status")).toBe("POSTED");
    expect(params.get("limit")).toBe("20");
    expect(params.get("cursor")).toBe("abc123");
  });

  it("should remove the cursor param when there is no next page", () => {
    const current = new URLSearchParams({ status: "POSTED", cursor: "stale" });
    const url = withCursor(current, null);
    const params = new URLSearchParams(url.slice(1));
    expect(params.has("cursor")).toBe(false);
    expect(params.get("status")).toBe("POSTED");
  });

  it("should return an empty string when there are no params at all", () => {
    expect(withCursor(new URLSearchParams(), null)).toBe("");
  });

  it("should accept a plain search-params record, taking the first value of an array", () => {
    const url = withCursor({ status: ["POSTED", "PENDING"], external_ref: undefined }, "xyz");
    const params = new URLSearchParams(url.slice(1));
    expect(params.get("status")).toBe("POSTED");
    expect(params.has("external_ref")).toBe(false);
    expect(params.get("cursor")).toBe("xyz");
  });
});

describe("withFilters", () => {
  it("should build a query string from every non-empty filter, dropping the rest", () => {
    const url = withFilters({ status: "POSTED", external_ref: "", type: "TRANSFER" });
    const params = new URLSearchParams(url.slice(1));
    expect(params.get("status")).toBe("POSTED");
    expect(params.has("external_ref")).toBe(false);
    expect(params.get("type")).toBe("TRANSFER");
  });

  it("should never carry a cursor forward -- changing filters always starts at page one", () => {
    const url = withFilters({ status: "POSTED" });
    expect(url).not.toContain("cursor");
  });
});
