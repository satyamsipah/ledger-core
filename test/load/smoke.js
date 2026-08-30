// k6 load profile for the local stack.
//
// Phase 1 exposes no ledger endpoints, so this exercises the parts that exist:
// the API's liveness and readiness paths, which is enough to confirm the
// harness, thresholds and the compose stack all work before there is anything
// interesting to measure.
//
// The posting scenario at the bottom is the one that matters, and it is
// commented out rather than absent so that Phase 2 turns it on rather than
// inventing it under time pressure.
//
//   k6 run test/load/smoke.js
//   BASE_URL=http://localhost:8080 k6 run test/load/smoke.js

import http from 'k6/http';
import { check, group } from 'k6';

const BASE_URL = __ENV.BASE_URL || 'http://localhost:8080';

export const options = {
  scenarios: {
    steady: {
      executor: 'constant-arrival-rate',
      // Arrival-rate rather than VU-based: a ledger's capacity question is
      // "can it absorb N requests per second", and a VU-based profile silently
      // slows its own request rate as latency rises, hiding the answer.
      rate: 200,
      timeUnit: '1s',
      duration: '30s',
      preAllocatedVUs: 50,
      maxVUs: 200,
    },
  },
  thresholds: {
    // Fail the run rather than print a red number: a load test that reports
    // failure but exits 0 will be ignored by CI and then by people.
    http_req_failed: ['rate<0.01'],
    'http_req_duration{endpoint:healthz}': ['p(99)<50'],
    'http_req_duration{endpoint:readyz}': ['p(99)<250'],
  },
};

export default function () {
  group('probes', () => {
    const live = http.get(`${BASE_URL}/healthz`, { tags: { endpoint: 'healthz' } });
    check(live, {
      'healthz is 200': (r) => r.status === 200,
      'healthz reports ok': (r) => r.json('status') === 'ok',
    });

    const ready = http.get(`${BASE_URL}/readyz`, { tags: { endpoint: 'readyz' } });
    check(ready, {
      'readyz is 200': (r) => r.status === 200,
      'postgres check passes': (r) => r.json('checks.postgres') === 'ok',
    });
  });
}

// Phase 2: concurrent transfers against a fixed pair of seeded wallets, with a
// duplicated idempotency key on a fraction of requests, asserting that the
// duplicate returns the original transaction id rather than creating a second.
//
// export function postTransfer() {
//   const idempotencyKey = `k6-${__VU}-${__ITER}`;
//   const res = http.post(`${BASE_URL}/v1/transactions`, JSON.stringify({
//     transaction_type: 'TRANSFER',
//     currency: 'INR',
//     entries: [
//       { account_ref: 'wallet-user-1001-inr', direction: 'DEBIT',  amount_minor: 100 },
//       { account_ref: 'wallet-user-1002-inr', direction: 'CREDIT', amount_minor: 100 },
//     ],
//   }), {
//     headers: { 'Content-Type': 'application/json', 'Idempotency-Key': idempotencyKey },
//     tags: { endpoint: 'post_transaction' },
//   });
//   check(res, { 'transfer accepted': (r) => r.status === 201 });
// }
