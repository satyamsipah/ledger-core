// hot_account: 90% of traffic credits ONE account; the remaining 10% spreads
// across four others. Same nominal rate, ramp shape and duration as
// baseline_simple_transfer -- the one thing this scenario varies on purpose
// is the account access pattern, so its numbers are read as a delta against
// baseline's, not as a scenario with its own unrelated shape.
//
// This is the scenario the sharding comparison (docs/DECISIONS.md D25) is
// read against during the optimisation cycle: D25's own benchmark used a
// hand-rolled Go harness on 32 writers; this is the same underlying
// contention (many writers, one account_balances row, D11's ordered row
// locking serialising them) measured through the same harness and
// methodology as every other number in docs/BENCHMARKS.md.
//
//   k6 run test/load/hot_account.js

import http from 'k6/http';
import { check } from 'k6';
import { authHeaders, transferBody, randomUUID, BASE_URL } from './lib/helpers.js';

// The funding leg is identical to baseline_simple_transfer's: DEBIT on
// platform-bank-inr always increases it (DEBIT matches its own DEBIT-normal
// balance -- docs/DECISIONS.md D13), so it is never at risk of a negative
// balance regardless of volume, and needs no pre-funding.
const PLATFORM_BANK_INR = '01920000-0000-7000-8000-000000000001';

// The hot account: 90% of every request's CREDIT leg lands here.
// payable-merchant-2001-inr is LIABILITY/CREDIT-normal, so CREDIT matches its
// own normal balance and always increases it -- safe under unlimited
// one-directional volume, and exactly the account_balances row D11's ordered
// locking exists to serialise access to.
const HOT_ACCOUNT = '01920000-0000-7000-8000-000000000021'; // payable-merchant-2001-inr

// The remaining 10% spreads across four other CREDIT-normal accounts, none
// of which risk a negative balance for the same reason as the hot account.
const COLD_ACCOUNTS = [
  '01920000-0000-7000-8000-000000000022', // payable-merchant-2002-inr
  '01920000-0000-7000-8000-000000000011', // wallet-user-1001-inr
  '01920000-0000-7000-8000-000000000012', // wallet-user-1002-inr
  '01920000-0000-7000-8000-000000000013', // wallet-user-1003-inr
];

const AMOUNT_MINOR = 100; // 1.00 INR
const CURRENCY = 'INR';
const HOT_SHARE = 0.9;

export const options = {
  scenarios: {
    hot_account: {
      executor: 'ramping-arrival-rate',
      startRate: 0,
      timeUnit: '1s',
      preAllocatedVUs: 50,
      maxVUs: 300,
      stages: [
        { target: 100, duration: '15s' }, // ramp-up
        { target: 100, duration: '60s' }, // steady state
        { target: 0, duration: '10s' }, // ramp-down
      ],
    },
  },
  summaryTrendStats: ['avg', 'min', 'med', 'p(50)', 'p(95)', 'p(99)', 'max'],
  thresholds: {
    http_req_failed: ['rate<0.01'],
    // Looser than baseline's on purpose: this scenario exists BECAUSE row-
    // lock queueing on one hot account_balances row is expected to cost
    // something over baseline's spread-out pattern (docs/DECISIONS.md D11) --
    // see baseline_simple_transfer.js's own comment on why 2s and not a
    // tighter, unverified number. The point of comparison is docs/BENCHMARKS.md's
    // summary table, not this ceiling.
    'http_req_duration{endpoint:post_transaction}': ['p(99)<3000'],
  },
};

export default function () {
  const toAccount = Math.random() < HOT_SHARE
    ? HOT_ACCOUNT
    : COLD_ACCOUNTS[Math.floor(Math.random() * COLD_ACCOUNTS.length)];

  const idempotencyKey = randomUUID();
  const res = http.post(
    `${BASE_URL}/v1/transactions`,
    transferBody(PLATFORM_BANK_INR, toAccount, AMOUNT_MINOR, CURRENCY),
    { headers: authHeaders(idempotencyKey), tags: { endpoint: 'post_transaction' } }
  );
  check(res, {
    'transfer accepted': (r) => r.status === 201,
  });
}
