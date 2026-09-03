import { describe, expect, it } from "vitest";
import { render, screen } from "@testing-library/react";
import "@testing-library/jest-dom/vitest";
import { InvariantTile } from "@/components/ledger/invariant-tile";

describe("InvariantTile", () => {
  it("should render healthy, in green, when the value is exactly zero", () => {
    render(<InvariantTile label="Journal balance" value={0} />);
    expect(screen.getByText("0")).toBeInTheDocument();
    expect(screen.getByText("Holding")).toBeInTheDocument();
  });

  it("should render violated, in red, when the value is nonzero", () => {
    render(<InvariantTile label="Journal balance" value={42} />);
    expect(screen.getByText("42")).toBeInTheDocument();
    expect(screen.getByText("Violated — page immediately")).toBeInTheDocument();
  });

  // Regression test: a live check against a Prometheus instance where the
  // reconciler (the only process that ever calls .WithLabelValues on this
  // particular GaugeVec) was not running returned an empty result -- no
  // series at all -- which the dashboard must render as "unknown", not as
  // "violated". Treating "no data" as "violated" would be a false alarm on
  // every scrape gap, exactly the kind of alert that trains an operator to
  // stop trusting this tile.
  it("should render unknown, never violated, when there is no data", () => {
    render(<InvariantTile label="Journal balance" value={null} />);
    expect(screen.getByText("—")).toBeInTheDocument();
    expect(screen.getByText("No data — check the reconciler")).toBeInTheDocument();
    expect(screen.queryByText("Violated — page immediately")).not.toBeInTheDocument();
  });
});
