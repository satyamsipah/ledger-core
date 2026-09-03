import { describe, expect, it } from "vitest";
import { formatMoney, isNegative } from "@/lib/money";

describe("formatMoney", () => {
  it("should format INR minor units with a decimal point at the currency's scale", () => {
    expect(formatMoney({ amount: "1250", currency: "INR", scale: 2 })).toBe("12.50 INR");
  });

  it("should add thousands separators", () => {
    expect(formatMoney({ amount: "123456789", currency: "USD", scale: 2 })).toBe("1,234,567.89 USD");
  });

  it("should pad an amount smaller than the scale", () => {
    expect(formatMoney({ amount: "5", currency: "INR", scale: 2 })).toBe("0.05 INR");
  });

  it("should render a negative amount with a minus sign, not embedded in the digits", () => {
    expect(formatMoney({ amount: "-500", currency: "INR", scale: 2 })).toBe("−5.00 INR");
  });

  it("should treat a zero-scale currency as having no fractional part", () => {
    expect(formatMoney({ amount: "1500", currency: "JPY", scale: 0 })).toBe("1,500 JPY");
  });

  it("should fall back to the known ISO-4217 exponent when scale is absent", () => {
    expect(formatMoney({ amount: "1500", currency: "JPY" })).toBe("1,500 JPY");
    expect(formatMoney({ amount: "1500", currency: "INR" })).toBe("15.00 INR");
  });

  it("should not lose precision on an amount too large for a JS number to round-trip safely", () => {
    // 2^53 + 1 -- the smallest integer a JS double cannot represent exactly.
    expect(formatMoney({ amount: "9007199254740993", currency: "INR", scale: 2 })).toBe("90,071,992,547,409.93 INR");
  });
});

describe("isNegative", () => {
  it("should report true for a negative amount", () => {
    expect(isNegative({ amount: "-100", currency: "INR" })).toBe(true);
  });

  it("should report false for a positive or zero amount", () => {
    expect(isNegative({ amount: "100", currency: "INR" })).toBe(false);
    expect(isNegative({ amount: "0", currency: "INR" })).toBe(false);
  });
});
