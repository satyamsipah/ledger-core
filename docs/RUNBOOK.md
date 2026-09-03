# Runbook

One section per alert in `deploy/prometheus/alerts.yml`. Each follows the same
shape: what firing actually means, how to find out more, and what to do about
it. None of these is "restart the service" — every one of them is either a
data-integrity question that a restart cannot answer, or a queue that a
restart does not drain any faster.

---

## LedgerGlobalInvariantBroken

**What it means.** `SUM` of every signed journal entry, for the currency named
in the alert's `currency` label, is no longer zero. Every individual
transaction is supposed to balance unconditionally at `COMMIT` — the deferred
constraint trigger in migration `000005` enforces that on every transaction,
every time. This alert firing means something wrote to `journal_entries`
without going through that trigger: direct SQL against the database, a future
migration that grants `UPDATE`/`DELETE` on the table (invariant 2 forbids
this, and the schema itself refuses it — see `journal_entries_no_mutation` in
migration `000005`), or a defect in the trigger's own logic.

**Severity.** Critical. This is the one alert this system is built around;
CLAUDE.md's own words are "if any change risks violating one, stop and flag
it."

**Diagnose.**

1. Confirm it against the database directly, not only the metric — the
   `internal/consistency.CheckGlobalInvariant` query, verbatim:
   ```sql
   SELECT currency,
          SUM(CASE WHEN direction = 'DEBIT' THEN amount_minor ELSE -amount_minor END) AS signed_total
     FROM journal_entries
    GROUP BY currency
   HAVING SUM(CASE WHEN direction = 'DEBIT' THEN amount_minor ELSE -amount_minor END) <> 0;
   ```
2. Narrow to the transaction(s) responsible — the identical query grouped by
   `transaction_id` as well as `currency` finds the specific offenders:
   ```sql
   SELECT transaction_id, currency,
          SUM(CASE WHEN direction = 'DEBIT' THEN amount_minor ELSE -amount_minor END) AS imbalance
     FROM journal_entries
    GROUP BY transaction_id, currency
   HAVING SUM(CASE WHEN direction = 'DEBIT' THEN amount_minor ELSE -amount_minor END) <> 0;
   ```
3. Check `pg_stat_activity` and the Postgres logs around when the imbalance
   first appears (the reconciler's own consistency-check ticks bound this to
   within `LEDGER_RECONCILER_CONSISTENCY_INTERVAL`) for any connection that
   is not one of this stack's own services — that is the signature of direct
   SQL access.
4. Check `pg_trigger` to confirm `journal_entries_balanced` is still present,
   `DEFERRABLE`, and `INITIALLY DEFERRED` — `test/migrations_test.go`'s
   `constraintTriggerIsDeferred` is the same check, automated.

**Remediate.** There is no automated remediation, on purpose — see
`docs/DECISIONS.md` D43's argument against a system that repairs its own
ledger silently. A person must:

1. Identify exactly which transaction(s) are unbalanced and why.
2. Post a correcting `ADJUSTMENT` transaction that restores the sum to zero,
   with a `metadata` note explaining the incident.
3. If the cause was a code or schema defect, fix it and add a test that would
   have caught it — this class of bug is exactly what
   `TestConsistency_Checks` (`test/consistency_test.go`) exists to prove is
   detectable, so if it happened, something upstream of that test's own
   coverage changed.
4. Do not simply set `allow_negative` or otherwise paper over the symptom —
   the imbalance is the entire problem.

---

## LedgerOutboxLagHigh

**What it means.** The oldest row in `outbox` with `published_at IS NULL` has
been sitting unpublished for more than 30 seconds. Either publisher
(`LEDGER_OUTBOX_PUBLISHER`) is behind or has stopped.

**Severity.** Warning — no ledger invariant is at risk (the outbox row and
its journal entries already committed together per invariant 6), but every
downstream consumer (the projector, anything else that subscribes) is behind
by however long this has been true.

**Diagnose.**

1. Check which publisher is configured: `LEDGER_OUTBOX_PUBLISHER` on the
   `outbox-publisher` service.
2. **Debezium (the default):** check the connector's own status —
   `curl http://localhost:8083/connectors/ledger-outbox/status`. A `FAILED`
   task means the connector itself has stopped; the Kafka Connect log
   (`docker compose logs connect`) has the exception. Also check the
   replication slot has not been abandoned:
   ```sql
   SELECT slot_name, active, pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)) AS retained_wal
     FROM pg_replication_slots WHERE slot_name = 'ledger_outbox_slot';
   ```
   A large or growing `retained_wal` with `active = false` means the slot is
   not being consumed and disk will fill eventually — this is the "abandoned
   slot" failure mode D31 warns about.
3. **Polling:** check `outbox-publisher`'s own logs for `"publish outbox
   batch"` errors — almost always Kafka unreachable. Confirm with
   `rpk cluster health` (or `docker compose exec redpanda rpk cluster
   health`).
4. Either way, confirm the backlog itself:
   ```sql
   SELECT count(*), min(created_at) FROM outbox WHERE published_at IS NULL;
   ```

**Remediate.**

- Debezium: fix and re-register the connector
  (`deploy/debezium/outbox-connector.json` via `connect-init`'s same `PUT`),
  or restart the `connect` container if Kafka Connect itself is unhealthy.
- Polling: restore Kafka connectivity; the publisher retries on its own
  (`LEDGER_OUTBOX_POLL_INTERVAL`) once the broker answers again, and nothing
  is lost — see `docs/DECISIONS.md` D30's at-least-once guarantee.
- Once caught up, `ledger_outbox_lag_seconds` returns to (near) zero on its
  own; there is no manual "catch up" step for either arm.

---

## LedgerProjectionDrift

**What it means.** `account_balances` (the synchronous balance, updated
under lock inside the write path) disagrees with a full recomputation from
`journal_entries` for at least one account. Per `docs/DECISIONS.md` D1, these
are two independently maintained numbers that must always agree; a third,
the Kafka-driven `balance_projections`, exists precisely so that when two of
the three disagree the third localises which one is wrong.

**Severity.** Critical — this is a customer-facing balance being wrong, not
merely a monitoring pipeline being behind.

**Diagnose.**

1. Find the offending accounts (`internal/consistency.CheckProjectionDrift`,
   verbatim):
   ```sql
   WITH rebuilt AS (
       SELECT a.id AS account_id, a.currency,
              COALESCE(SUM(CASE WHEN je.direction = a.normal_balance
                                THEN je.amount_minor ELSE -je.amount_minor END), 0) AS available_minor
         FROM accounts a
         LEFT JOIN journal_entries je ON je.account_id = a.id
        GROUP BY a.id, a.currency
   )
   SELECT r.account_id, r.currency, r.available_minor AS rebuilt, COALESCE(ab.available_minor, 0) AS live
     FROM rebuilt r
     LEFT JOIN account_balances ab ON ab.account_id = r.account_id
    WHERE r.available_minor <> COALESCE(ab.available_minor, 0);
   ```
2. Cross-check against the THIRD number, the Kafka-driven projection, to
   localise the bug rather than guess at it: `go run ./cmd/projector -rebuild`
   (or `make rebuild`) diffs the same journal recomputation against
   `balance_projections` instead. If `balance_projections` agrees with the
   rebuilt total but `account_balances` does not, the defect is in the
   synchronous write path (`internal/ledger/pgledger`), not the async one.
3. Check `account_balances.version` against the transaction history for the
   account — a version that has not advanced despite new journal entries
   existing is the signature of a write that bypassed the locked update path.

**Remediate.** As with the global invariant, there is no automated fix.
Determine which number is actually correct (the journal is the source of
truth — `account_balances` is a cache of it, however load-bearing), correct
`account_balances` by hand if the journal is right, and — critically — find
and fix whatever wrote to `account_balances` outside `pgledger`'s locked
update path, since that is a live hole that will drift again.

---

## LedgerSagaStuck

**What it means.** The most-overdue non-terminal saga (excludes the four
terminal statuses, `NEEDS_MANUAL_REVIEW` included — see the alert's own
comment) has been past its `step_deadline_at` for more than 5 minutes. The
sweeper (`SAGA_SWEEP_INTERVAL`, default 10s) should have already picked it up
well before this fires; this alert firing means either the sweeper itself has
stopped, or a saga's steps keep failing and re-queuing faster than they
resolve.

**Severity.** Warning — the money involved is safe (see `docs/DECISIONS.md`
D39: the funds sit in the named `payout-suspense` account, not in limbo), but
a customer's payout is not completing.

**Diagnose.**

1. Confirm the sweeper is actually running: `saga-orchestrator`'s logs should
   show a `"payout saga sweeper started"` line at boot and no gap in
   activity since.
2. Find the specific saga(s):
   ```sql
   SELECT id, status, current_step, retry_count, last_error, step_deadline_at
     FROM saga_instances
    WHERE step_deadline_at < now()
      AND status NOT IN ('COMPLETED', 'COMPENSATED', 'FAILED', 'NEEDS_MANUAL_REVIEW')
    ORDER BY step_deadline_at
    LIMIT 20;
   ```
3. Read its attempt history for the actual failure:
   `GET /v1/sagas/{id}` (no auth required — saga reads are unauthenticated,
   unlike transaction and reconciliation reads; see
   [docs/ARCHITECTURE.md](ARCHITECTURE.md#authentication)) returns every
   `saga_steps` row with its error.
4. If the step is `GATEWAY`, check the gateway itself is reachable and
   answering — `mock-gateway`'s own `/healthz` locally, or the real payment
   gateway's status page in production.

**Remediate.**

- If the sweeper had stopped (crashed replica, all replicas down): restart
  `saga-orchestrator`. Recovery is automatic from there — leases are what
  make a stuck saga safe to pick back up with no manual bookkeeping (D40).
- If the gateway was unreachable and is now healthy again: nothing to do: the
  next sweep resumes the saga on its own.
- If the saga has genuinely exhausted its retry budget, it will reach
  `NEEDS_MANUAL_REVIEW` on its own (see that alert's absence here — it is
  covered by `ledger_saga_manual_review_total`, not this one) and needs the
  manual resolution path described in `docs/DECISIONS.md`'s "known gaps"
  for Phase 5: query the gateway for `<saga_id>:GATEWAY` directly, then post
  the corresponding correction by hand and move the saga by SQL.

---

## LedgerReconciliationExceptions

**What it means.** The daily PSP reconciliation run
(`internal/reconciliation`, `cmd/reconciler`) found at least one mismatch
between the ledger's own transactions and the settlement file that was not
an auto-resolved `TIMING_DIFFERENCE`.

**Severity.** Warning by default; treat as urgent if the category is
`MISSING_IN_LEDGER` or `AMOUNT_MISMATCH` — those mean money the PSP believes
moved is not reflected here, or reflected as the wrong amount.

**Diagnose.**

1. Find the run and its breakdown:
   `GET /v1/reconciliation/runs` then `GET /v1/reconciliation/runs/{id}`
   (both require auth) — the category, the `external_ref`, and (where
   applicable) the linked `ledger_transaction_id` and `saga_id` are all on
   the exception row.
2. Read `docs/DECISIONS.md` D48 for exactly what each category means and how
   the match was computed — in particular, "the ledger's amount" for a
   reference is the `DEBIT`-side sum of its *latest* transaction, which
   matters when interpreting an `AMOUNT_MISMATCH`.
3. For `DUPLICATE`: the PSP statement itself listed one `external_ref` more
   than once — this is very likely a data problem in the file, not the
   ledger, but confirm nothing on this side double-posted against that
   reference too.

**Remediate.** There is no automated resolution — an exception stays `OPEN`
until investigated. Once resolved (a correcting transaction posted, or the
PSP file confirmed to be the one at fault), there is currently no API to mark
it `RESOLVED`; see `docs/DECISIONS.md` D48's "what remains open" for why that
surface does not exist yet, and update the row by hand:
```sql
UPDATE reconciliation_exceptions
   SET status = 'RESOLVED', resolved_at = now()
 WHERE id = $1;
```
