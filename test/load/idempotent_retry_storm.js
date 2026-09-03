// idempotent_retry_storm: 30% of requests reuse a key (and its exact body)
// from earlier in the same VU's own history, exercising the replay path
// invariant 5 exists for -- api/openapi.yaml's IdempotencyKey parameter and
// docs/DECISIONS.md D20's fingerprint check both require the REPLAYED body to
// be byte-identical to the original's, not merely the key, so this scenario
// stores {key, body} pairs and resends both together rather than reusing a
// key against a freshly-built body, which would hit the fingerprint mismatch
// path (409 ErrIdempotencyConflict) instead of a genuine replay.
//
//   k6 run test/load/idempotent_retry_storm.js

import http from 'k6/http';
import { check } from 'k6';
import { authHeaders, transferBody, randomUUID, BASE_URL } from './lib/helpers.js';

// Same self-funding pair as baseline_simple_transfer -- this scenario is
// about the idempotency layer, not the ledger accounts, so it deliberately
// reuses the simplest account pattern rather than introducing a second
// variable.
const PLATFORM_BANK_INR = '01920000-0000-7000-8000-000000000001';
const GATEWAY_SUSPENSE_INR = '01920000-0000-7000-8000-000000000002';

const AMOUNT_MINOR = 100; // 1.00 INR
const CURRENCY = 'INR';

const DUPLICATE_RATE = 0.3;
const MAX_RECENT = 20; // bounds one VU's own replay pool; not a tuned value

export const options = {
  scenarios: {
    idempotent_retry_storm: {
      executor: 'ramping-arrival-rate',
      startRate: 0,
      timeUnit: '1s',
      preAllocatedVUs: 50,
      maxVUs: 300,
      stages: [
        { target: 100, duration: '15s' },
        { target: 100, duration: '60s' },
        { target: 0, duration: '10s' },
      ],
    },
  },
  summaryTrendStats: ['avg', 'min', 'med', 'p(50)', 'p(95)', 'p(99)', 'max'],
  thresholds: {
    http_req_failed: ['rate<0.01'],
    // See baseline_simple_transfer.js's own comment on why this is loose
    // rather than tuned: same machine, same noise. A replay is a fast read
    // path (internal/idempotency's lease check, no ledger work), so this
    // scenario is not expected to run slower than baseline -- if anything
    // faster, since 30% of requests skip PostTransaction entirely.
    'http_req_duration{endpoint:post_transaction}': ['p(99)<2000'],
  },
};

// recentRequests is per-VU: k6 gives each VU its own instance of this
// module's top-level state, so one VU's replay pool never mixes with
// another's, and DUPLICATE_RATE is therefore relative to each VU's own
// request history, not a global one this script would need cross-VU
// coordination to build.
let recentRequests = [];

export default function () {
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
    tags: { endpoint: 'post_transaction' },
  });

  check(res, {
    'request accepted (fresh or replayed)': (r) => r.status === 201,
    'a duplicate is marked as a replay': (r) => !isDuplicate || r.headers['Idempotent-Replay'] === 'true',
  });
}
