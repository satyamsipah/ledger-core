// baseline_simple_transfer: the simplest possible write, at a moderate,
// non-adversarial rate. Every other scenario in this directory is a
// deliberate deviation from this one (skewed toward one account, duplicated
// keys, saga steps with injected failures); this is the number every other
// scenario's own number should be read against.
//
//   k6 run test/load/baseline_simple_transfer.js
//   BASE_URL=http://localhost:8080 API_KEY=lk_live_... k6 run test/load/baseline_simple_transfer.js
//
// cmd/loadtest-harness is what actually runs this against a freshly-started
// stack with a freshly-minted key (make loadtest); running it by hand like
// the above needs a key of your own (cmd/issue-api-key) and the seeded local
// stack (make up && make seed) already up.

import http from 'k6/http';
import { check } from 'k6';
import { authHeaders, transferBody, randomUUID, BASE_URL } from './lib/helpers.js';

// Two fixed, low-cardinality accounts, deliberately -- this scenario is
// named "simple" and "baseline" precisely because it is NOT hot_account's
// skewed-traffic profile. Both are from deploy/seed/seed.sql's chart of
// accounts, chosen so the transfer can run indefinitely with no pre-funding
// and can never fail on insufficient funds:
//
//   platform-bank-inr     ASSET, DEBIT-normal,  allow_negative=false
//   gateway-suspense-inr  ASSET, DEBIT-normal,  allow_negative=true
//
// DEBIT on platform-bank-inr matches its own normal balance, so it always
// INCREASES available_minor (docs/DECISIONS.md D13) -- never a negative-
// balance risk regardless of volume. CREDIT on gateway-suspense-inr
// mismatches its normal balance, so it always DECREASES available_minor, but
// that account is seeded with allow_negative=true specifically because real
// settlement files routinely arrive before the authorisation they settle
// (seed.sql's own comment) -- so it can absorb this scenario's one-directional
// traffic without limit or refill.
const PLATFORM_BANK_INR = '01920000-0000-7000-8000-000000000001';
const GATEWAY_SUSPENSE_INR = '01920000-0000-7000-8000-000000000002';

const AMOUNT_MINOR = 100; // 1.00 INR
const CURRENCY = 'INR';

export const options = {
  scenarios: {
    baseline_simple_transfer: {
      executor: 'ramping-arrival-rate',
      // Arrival-rate, not VU-based -- see smoke.js's own comment: a ledger's
      // capacity question is "can it absorb N requests per second", and a
      // VU-based profile silently slows its own request rate as latency
      // rises, hiding the answer rather than reporting it.
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
  // p(50)/p(99) are not in k6's own default summary trend stats -- only
  // avg/min/med/max/p(90)/p(95) are -- and docs/BENCHMARKS.md needs exactly
  // p50/p95/p99, so this is stated explicitly rather than post-processed out
  // of a coarser default.
  summaryTrendStats: ['avg', 'min', 'med', 'p(50)', 'p(95)', 'p(99)', 'max'],
  thresholds: {
    // Fails the run rather than merely printing a red number -- see
    // smoke.js's own comment, which this repeats because it is the whole
    // point of running thresholds at all rather than eyeballing output.
    http_req_failed: ['rate<0.01'],
    // A first-cut ceiling, not a tuned target: this is the scenario that
    // ESTABLISHES the baseline, so there is no prior number to regress
    // against yet. Set deliberately loose (2s, against a p50 consistently
    // under 5ms) because this threshold was FIRST tuned tighter (500ms) and
    // then observed to fail on a real, non-pathological run: `make loadtest`
    // rebuilds every image immediately before running k6, and on a shared
    // development machine that build's own tail CPU usage plus general
    // system load measurably inflates p99 -- one recorded run hit 1.67s
    // this way with p50 still at 3.6ms, i.e. the SYSTEM was fine and the
    // MACHINE was busy. docs/BENCHMARKS.md records what a given run actually
    // measured; once several runs on a dedicated (non-shared) host establish
    // a stable p99, tighten this to that number plus headroom rather than
    // leaving a ceiling nothing has ever approached.
    'http_req_duration{endpoint:post_transaction}': ['p(99)<2000'],
  },
};

export default function () {
  const idempotencyKey = randomUUID();
  const res = http.post(
    `${BASE_URL}/v1/transactions`,
    transferBody(PLATFORM_BANK_INR, GATEWAY_SUSPENSE_INR, AMOUNT_MINOR, CURRENCY),
    { headers: authHeaders(idempotencyKey), tags: { endpoint: 'post_transaction' } }
  );
  check(res, {
    'transfer accepted': (r) => r.status === 201,
  });
}
