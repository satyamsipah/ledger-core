// Shared plumbing every k6 scenario in this directory needs: auth headers,
// a UUID generator (k6's JS runtime provides no crypto.randomUUID), and the
// wire shape POST /v1/transactions expects -- api/openapi.yaml's Money
// schema, where `amount` is a DECIMAL STRING of minor units, never a JSON
// number (docs/DECISIONS.md D28's reasoning for byte-exact bodies applies to
// request bodies too: a client and server must agree on what was sent, and a
// JSON number can lose precision a string cannot).

export const BASE_URL = __ENV.BASE_URL || 'http://localhost:8080';
export const API_KEY = __ENV.API_KEY || '';

// A UUIDv4-shaped string built from Math.random(). Not cryptographically
// random, and it does not need to be: every caller here only needs
// uniqueness across one load run's own requests, which this gives, and every
// write route requires the header be UUID-shaped at all (api/openapi.yaml's
// IdempotencyKey parameter).
export function randomUUID() {
  return 'xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx'.replace(/[xy]/g, (c) => {
    const r = (Math.random() * 16) | 0;
    const v = c === 'x' ? r : (r & 0x3) | 0x8;
    return v.toString(16);
  });
}

// authHeaders builds the headers every write route needs: a fresh
// Idempotency-Key and, when the harness supplied one, the bearer token
// cmd/loadtest-harness mints once per run via internal/auth/pgauth (D24 --
// every write route has required authentication since Phase 3).
export function authHeaders(idempotencyKey) {
  const headers = {
    'Content-Type': 'application/json',
    'Idempotency-Key': idempotencyKey,
  };
  if (API_KEY) {
    headers['Authorization'] = `Bearer ${API_KEY}`;
  }
  return headers;
}

export function money(amountMinor, currency) {
  return { amount: String(amountMinor), currency };
}

// transferBody builds a balanced two-leg PostTransactionRequest: `from` is
// debited, `to` is credited, for `amountMinor` of `currency`. Whether that
// increases or decreases either account's available balance depends on the
// account's own normal_balance (docs/DECISIONS.md D13) -- this function
// only shapes the request, it does not know or care which accounts it is
// pointed at.
export function transferBody(fromAccountID, toAccountID, amountMinor, currency, type) {
  return JSON.stringify({
    type: type || 'TRANSFER',
    entries: [
      { account_id: fromAccountID, direction: 'DEBIT', amount: money(amountMinor, currency) },
      { account_id: toAccountID, direction: 'CREDIT', amount: money(amountMinor, currency) },
    ],
  });
}
