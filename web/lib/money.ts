import type { Money } from "@/lib/api/types";

// ISO-4217 exponents for currencies this ledger is seeded with. Falls back to
// 2 (the overwhelmingly common case) for anything unlisted rather than
// failing closed -- a wrong guess mis-places a decimal point, which is bad,
// but throwing on an unrecognised-but-valid currency code in an admin
// dashboard is worse.
const CURRENCY_EXPONENTS: Record<string, number> = {
  JPY: 0,
  KRW: 0,
  INR: 2,
  USD: 2,
  EUR: 2,
  GBP: 2,
  KWD: 3,
  BHD: 3,
};

function exponentFor(m: Money): number {
  if (typeof m.scale === "number") return m.scale;
  return CURRENCY_EXPONENTS[m.currency] ?? 2;
}

/**
 * Formats a Money value for display. Never routes through Number() on the
 * full-precision amount -- the API sends amount as a decimal string of minor
 * units specifically because a JSON number would silently lose precision
 * past 2^53, and this dashboard must not reintroduce that on the read side.
 * Splitting into whole/fractional parts by string manipulation instead keeps
 * every digit intact regardless of magnitude.
 */
export function formatMoney(m: Money): string {
  const exponent = exponentFor(m);
  const negative = m.amount.startsWith("-");
  const digits = negative ? m.amount.slice(1) : m.amount;
  const padded = digits.padStart(exponent + 1, "0");

  const whole = exponent === 0 ? padded : padded.slice(0, -exponent);
  const fraction = exponent === 0 ? "" : padded.slice(-exponent);

  const withThousands = whole.replace(/\B(?=(\d{3})+(?!\d))/g, ",");
  const sign = negative ? "−" : "";
  const value = fraction ? `${withThousands}.${fraction}` : withThousands;

  return `${sign}${value} ${m.currency}`;
}

/** True when the amount is negative, for callers that want to color it. */
export function isNegative(m: Money): boolean {
  return m.amount.startsWith("-");
}
