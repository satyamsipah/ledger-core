// saga_heavy: full multi-step marketplace payouts (RESERVE -> GATEWAY ->
// SETTLE, docs/DECISIONS.md's payout package doc comment) against a gateway
// deliberately failing 5% of calls with an ambiguous (UNKNOWN) outcome --
// internal/gateway/mock's FailureRatePercent, added for this scenario.
//
// Heavier and slower than the transfer-shaped scenarios on purpose: each
// payout is three real ledger transactions plus a network round trip to the
// gateway, driven by cmd/saga-orchestrator's own claim loop
// (LEDGER_SAGA_CLAIM_INTERVAL, 250ms default) rather than resolved inline by
// the HTTP handler -- POST /v1/payouts returns 202 with no money moved yet
// (api/openapi.yaml's own words), so this scenario polls GET /v1/sagas/{id}
// until the saga reaches a settled state instead of treating the POST's
// response as the answer.
//
//   k6 run test/load/saga_heavy.js
//   BASE_URL=... API_KEY=... GATEWAY_URL=http://localhost:8090 k6 run test/load/saga_heavy.js

import http from 'k6/http';
import { check, sleep } from 'k6';
import { authHeaders, randomUUID, BASE_URL, API_KEY } from './lib/helpers.js';

const GATEWAY_URL = __ENV.GATEWAY_URL || 'http://localhost:8090';

// deploy/seed/seed.sql accounts. The customer wallet is funded once in
// setup() -- it is seeded at a zero balance, and RESERVE debits it -- so this
// scenario needs real, funded money to move rather than a zero-balance
// account that would fail on the first payout.
const CUSTOMER_WALLET = '01920000-0000-7000-8000-000000000011'; // wallet-user-1001-inr
const PLATFORM_SUSPENSE = '01920000-0000-7000-8000-000000000005'; // payout-suspense-inr
const MERCHANT_PAYABLE = '01920000-0000-7000-8000-000000000022'; // payable-merchant-2002-inr
const FEE_REVENUE = '01920000-0000-7000-8000-000000000003'; // fee-revenue-inr
const GATEWAY_SUSPENSE_INR = '01920000-0000-7000-8000-000000000002'; // funds the wallet in setup()

const CURRENCY = 'INR';
const PAYOUT_AMOUNT_MINOR = 1000; // 10.00 INR
const PAYOUT_FEE_MINOR = 50; // 0.50 INR
const GATEWAY_FAILURE_RATE_PERCENT = 5;

// Sized so 600 worst-case payouts (10/s steady state x 60s) leave the wallet
// nowhere near empty -- see setup()'s own funding transaction.
const WALLET_FUNDING_MINOR = 50000000; // 500,000.00 INR

// A saga reaches one of these without further automated action
// (api/openapi.yaml's SagaStatus enum). NEEDS_MANUAL_REVIEW counts as
// "resolved" for this scenario's purposes: under injected gateway faults it
// is a legitimate outcome (an ambiguous gateway call that also failed to
// compensate cleanly), and what this scenario proves is that the SAGA
// SETTLES -- into whichever terminal state is correct -- not that every
// payout succeeds outright.
const TERMINAL_STATUSES = new Set(['COMPLETED', 'COMPENSATED', 'FAILED', 'NEEDS_MANUAL_REVIEW']);

const POLL_INTERVAL_SECONDS = 0.3;
// ~75s. Measured empirically, not guessed: a saga hitting the injected
// ambiguous gateway failure probes and retries with backoff
// (LEDGER_SAGA_MAX_STEP_ATTEMPTS=5, LEDGER_SAGA_MAX_COMPENSATION_ATTEMPTS=8)
// before settling, and a live run against this exact scenario recorded two
// sagas taking 30-40s from GATEWAY_PENDING to COMPENSATED. 12s (the first
// value tried here) left 6% of a 150-payout run reported as unsettled when
// they were, in fact, still correctly resolving.
const POLL_MAX_ATTEMPTS = 250;

export const options = {
  scenarios: {
    saga_heavy: {
      executor: 'ramping-arrival-rate',
      startRate: 0,
      timeUnit: '1s',
      preAllocatedVUs: 50,
      maxVUs: 400,
      stages: [
        { target: 10, duration: '10s' }, // ramp-up
        { target: 10, duration: '60s' }, // steady state
        { target: 0, duration: '10s' }, // ramp-down
      ],
    },
  },
  summaryTrendStats: ['avg', 'min', 'med', 'p(50)', 'p(95)', 'p(99)', 'max'],
  thresholds: {
    // The POST itself (queuing the saga) failing is a real problem; a saga
    // resolving into NEEDS_MANUAL_REVIEW under injected faults is not, and is
    // deliberately not counted against http_req_failed at all -- see the
    // 'saga settled' check below instead.
    'http_req_failed{endpoint:start_payout}': ['rate<0.01'],
    // k6's own `checks` metric, filtered to this one named check via the tag
    // k6 sets automatically -- there is no standalone metric named after a
    // check, despite how a bare check name reads.
    'checks{check:saga_settled}': ['rate>0.99'],
  },
};

export function setup() {
  const behaviourRes = http.post(
    `${GATEWAY_URL}/control/behaviour`,
    JSON.stringify({ outcome: 'succeed', failure_rate_percent: GATEWAY_FAILURE_RATE_PERCENT }),
    { headers: { 'Content-Type': 'application/json' } }
  );
  if (behaviourRes.status !== 204) {
    throw new Error(`failed to configure mock-gateway failure rate: ${behaviourRes.status} ${behaviourRes.body}`);
  }

  const fundRes = http.post(
    `${BASE_URL}/v1/transactions`,
    JSON.stringify({
      type: 'TRANSFER',
      entries: [
        { account_id: GATEWAY_SUSPENSE_INR, direction: 'DEBIT', amount: { amount: String(WALLET_FUNDING_MINOR), currency: CURRENCY } },
        { account_id: CUSTOMER_WALLET, direction: 'CREDIT', amount: { amount: String(WALLET_FUNDING_MINOR), currency: CURRENCY } },
      ],
    }),
    { headers: authHeaders(randomUUID()) }
  );
  if (fundRes.status !== 201) {
    throw new Error(`failed to fund the payout scenario's customer wallet: ${fundRes.status} ${fundRes.body}`);
  }
}

export function teardown() {
  http.post(
    `${GATEWAY_URL}/control/behaviour`,
    JSON.stringify({ outcome: 'succeed' }),
    { headers: { 'Content-Type': 'application/json' } }
  );
}

export default function () {
  const startRes = http.post(
    `${BASE_URL}/v1/payouts`,
    JSON.stringify({
      customer_wallet_id: CUSTOMER_WALLET,
      platform_suspense_id: PLATFORM_SUSPENSE,
      merchant_payable_id: MERCHANT_PAYABLE,
      fee_revenue_id: FEE_REVENUE,
      amount: { amount: String(PAYOUT_AMOUNT_MINOR), currency: CURRENCY },
      fee: { amount: String(PAYOUT_FEE_MINOR), currency: CURRENCY },
    }),
    { headers: authHeaders(randomUUID()), tags: { endpoint: 'start_payout' } }
  );

  const accepted = check(startRes, { 'payout accepted (202)': (r) => r.status === 202 });
  if (!accepted) {
    return;
  }

  const sagaID = startRes.json('id');

  let settled = false;
  for (let attempt = 0; attempt < POLL_MAX_ATTEMPTS; attempt++) {
    const sagaRes = http.get(`${BASE_URL}/v1/sagas/${sagaID}`, {
      headers: API_KEY ? { Authorization: `Bearer ${API_KEY}` } : {},
      tags: { endpoint: 'get_saga' },
    });
    if (sagaRes.status === 200 && TERMINAL_STATUSES.has(sagaRes.json('status'))) {
      settled = true;
      break;
    }
    sleep(POLL_INTERVAL_SECONDS);
  }

  check(null, { saga_settled: () => settled });
}
