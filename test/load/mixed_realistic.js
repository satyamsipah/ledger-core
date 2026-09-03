// mixed_realistic: a weighted blend of the other four scenarios' traffic
// shapes, running concurrently in one k6 process via k6's own multi-scenario
// `options.scenarios` map (each named scenario below gets its own
// ramping-arrival-rate profile and executes independently, at the same
// time, against the same stack) -- this is what makes it "mixed" rather than
// a fifth, differently-shaped scenario of its own.
//
// Weights (rps at steady state, ~100 total): 60 simple transfers, 20 skewed
// toward one hot account, 15 idempotent-duplicate transfers, 5 full payout
// sagas. Payouts are weighted lightest deliberately -- see saga_heavy.js's
// own comment on why they are the heaviest single request this API serves.
//
//   k6 run test/load/mixed_realistic.js

import http from 'k6/http';
import { check, sleep } from 'k6';
import { authHeaders, transferBody, randomUUID, BASE_URL, API_KEY } from './lib/helpers.js';

const GATEWAY_URL = __ENV.GATEWAY_URL || 'http://localhost:8090';

// --- accounts (deploy/seed/seed.sql) ---
const PLATFORM_BANK_INR = '01920000-0000-7000-8000-000000000001';
const GATEWAY_SUSPENSE_INR = '01920000-0000-7000-8000-000000000002';
const HOT_ACCOUNT = '01920000-0000-7000-8000-000000000021'; // payable-merchant-2001-inr
const COLD_ACCOUNTS = [
  '01920000-0000-7000-8000-000000000022', // payable-merchant-2002-inr
  '01920000-0000-7000-8000-000000000012', // wallet-user-1002-inr
  '01920000-0000-7000-8000-000000000013', // wallet-user-1003-inr
];
const CUSTOMER_WALLET = '01920000-0000-7000-8000-000000000011'; // wallet-user-1001-inr
const PLATFORM_SUSPENSE = '01920000-0000-7000-8000-000000000005'; // payout-suspense-inr
const MERCHANT_PAYABLE = '01920000-0000-7000-8000-000000000022'; // payable-merchant-2002-inr
const FEE_REVENUE = '01920000-0000-7000-8000-000000000003'; // fee-revenue-inr

const CURRENCY = 'INR';
const AMOUNT_MINOR = 100;
const HOT_SHARE = 0.9;
const DUPLICATE_RATE = 0.3;

const PAYOUT_AMOUNT_MINOR = 1000;
const PAYOUT_FEE_MINOR = 50;
const GATEWAY_FAILURE_RATE_PERCENT = 5;
const WALLET_FUNDING_MINOR = 50000000; // see saga_heavy.js's own sizing comment
const TERMINAL_STATUSES = new Set(['COMPLETED', 'COMPENSATED', 'FAILED', 'NEEDS_MANUAL_REVIEW']);
const POLL_INTERVAL_SECONDS = 0.3;
const POLL_MAX_ATTEMPTS = 250; // see saga_heavy.js's own measured sizing

export const options = {
  scenarios: {
    mixed_transfers: {
      executor: 'ramping-arrival-rate',
      exec: 'transfer',
      startRate: 0,
      timeUnit: '1s',
      preAllocatedVUs: 30,
      maxVUs: 150,
      stages: [
        { target: 60, duration: '15s' },
        { target: 60, duration: '60s' },
        { target: 0, duration: '10s' },
      ],
    },
    mixed_hot: {
      executor: 'ramping-arrival-rate',
      exec: 'hotTransfer',
      startRate: 0,
      timeUnit: '1s',
      preAllocatedVUs: 20,
      maxVUs: 100,
      stages: [
        { target: 20, duration: '15s' },
        { target: 20, duration: '60s' },
        { target: 0, duration: '10s' },
      ],
    },
    mixed_retries: {
      executor: 'ramping-arrival-rate',
      exec: 'retryTransfer',
      startRate: 0,
      timeUnit: '1s',
      preAllocatedVUs: 20,
      maxVUs: 100,
      stages: [
        { target: 15, duration: '15s' },
        { target: 15, duration: '60s' },
        { target: 0, duration: '10s' },
      ],
    },
    mixed_payouts: {
      executor: 'ramping-arrival-rate',
      exec: 'payout',
      startRate: 0,
      timeUnit: '1s',
      preAllocatedVUs: 30,
      maxVUs: 200,
      stages: [
        { target: 5, duration: '15s' },
        { target: 5, duration: '60s' },
        { target: 0, duration: '10s' },
      ],
    },
  },
  summaryTrendStats: ['avg', 'min', 'med', 'p(50)', 'p(95)', 'p(99)', 'max'],
  thresholds: {
    http_req_failed: ['rate<0.01'],
    // Untagged (the whole mix), unlike the single-shape scenarios' own
    // per-endpoint threshold -- see baseline_simple_transfer.js's comment on
    // why this is loose rather than tuned, which applies here too.
    http_req_duration: ['p(99)<3000'],
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

// transfer: baseline_simple_transfer's own shape, tagged separately so its
// slice of the blend is independently readable in k6's own output.
export function transfer() {
  const res = http.post(
    `${BASE_URL}/v1/transactions`,
    transferBody(PLATFORM_BANK_INR, GATEWAY_SUSPENSE_INR, AMOUNT_MINOR, CURRENCY),
    { headers: authHeaders(randomUUID()), tags: { endpoint: 'mixed_transfer' } }
  );
  check(res, { 'transfer accepted': (r) => r.status === 201 });
}

// hotTransfer: hot_account.js's own shape.
export function hotTransfer() {
  const toAccount = Math.random() < HOT_SHARE
    ? HOT_ACCOUNT
    : COLD_ACCOUNTS[Math.floor(Math.random() * COLD_ACCOUNTS.length)];
  const res = http.post(
    `${BASE_URL}/v1/transactions`,
    transferBody(PLATFORM_BANK_INR, toAccount, AMOUNT_MINOR, CURRENCY),
    { headers: authHeaders(randomUUID()), tags: { endpoint: 'mixed_hot' } }
  );
  check(res, { 'hot transfer accepted': (r) => r.status === 201 });
}

// retryTransfer: idempotent_retry_storm.js's own shape. recentRequests is
// per-VU, and mixed_retries has its own dedicated VU pool (the executor
// config above), so this never shares state with transfer/hotTransfer/payout.
let recentRequests = [];
const MAX_RECENT = 20;

export function retryTransfer() {
  let idempotencyKey;
  let body;
  let isDuplicate = false;

  if (recentRequests.length > 0 && Math.random() < DUPLICATE_RATE) {
    const picked = recentRequests[Math.floor(Math.random() * recentRequests.length)];
    idempotencyKey = picked.key;
    body = picked.body;
    isDuplicate = true;
  } else {
    idempotencyKey = randomUUID();
    body = transferBody(PLATFORM_BANK_INR, GATEWAY_SUSPENSE_INR, AMOUNT_MINOR, CURRENCY);
    recentRequests.push({ key: idempotencyKey, body: body });
    if (recentRequests.length > MAX_RECENT) {
      recentRequests.shift();
    }
  }

  const res = http.post(`${BASE_URL}/v1/transactions`, body, {
    headers: authHeaders(idempotencyKey),
    tags: { endpoint: 'mixed_retry' },
  });
  check(res, {
    'retry request accepted': (r) => r.status === 201,
    'duplicate marked as replay': (r) => !isDuplicate || r.headers['Idempotent-Replay'] === 'true',
  });
}

// payout: saga_heavy.js's own shape.
export function payout() {
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
    { headers: authHeaders(randomUUID()), tags: { endpoint: 'mixed_payout' } }
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
      tags: { endpoint: 'mixed_get_saga' },
    });
    if (sagaRes.status === 200 && TERMINAL_STATUSES.has(sagaRes.json('status'))) {
      settled = true;
      break;
    }
    sleep(POLL_INTERVAL_SECONDS);
  }
  check(null, { saga_settled: () => settled });
}
