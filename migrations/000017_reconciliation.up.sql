-- Phase 6: proving the ledger correct against an external source of truth.
--
-- reconciliation_runs is one row per three-way match against a PSP settlement
-- file; reconciliation_exceptions is one row per external_ref this service
-- could not silently reconcile. Both are additive, append-mostly tables, in
-- the same spirit as saga_instances/saga_steps: the run is the outcome, the
-- exceptions are the evidence, and neither is ever rewritten to make a past
-- run look like it found something different than it did.

CREATE TABLE reconciliation_runs (
    id         UUID        PRIMARY KEY,

    -- Where the PSP statement came from -- a file path today. Recorded so a
    -- run found to be wrong can be traced back to the exact input that
    -- produced it, the same reasoning transactions.idempotency_key exists for.
    source     TEXT        NOT NULL,

    started_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    finished_at TIMESTAMPTZ,

    status     TEXT        NOT NULL DEFAULT 'RUNNING',

    psp_row_count       INT NOT NULL DEFAULT 0,
    matched_count        INT NOT NULL DEFAULT 0,
    auto_resolved_count  INT NOT NULL DEFAULT 0,
    exception_count      INT NOT NULL DEFAULT 0,

    -- Populated only when status = 'FAILED' -- a malformed CSV, a database
    -- error mid-match. Kept short like saga_instances.last_error; this is an
    -- operator-facing summary, not a stack trace.
    error      TEXT,

    CONSTRAINT reconciliation_runs_status_check
        CHECK (status IN ('RUNNING', 'COMPLETED', 'FAILED')),

    CONSTRAINT reconciliation_runs_counts_check
        CHECK (psp_row_count >= 0 AND matched_count >= 0
               AND auto_resolved_count >= 0 AND exception_count >= 0),

    -- A run is either still going (no finish time) or done (one). Mirrors
    -- saga_steps_finished_at_check's reasoning: this is what keeps "which runs
    -- are still in flight" an honest query rather than an inference from
    -- status alone.
    CONSTRAINT reconciliation_runs_finished_at_check
        CHECK ((status = 'RUNNING') = (finished_at IS NULL))
);

-- The dashboard's "recent runs" view and the daily-report lookup: newest
-- first, cheap even once this table has years of daily rows.
CREATE INDEX reconciliation_runs_started_at_idx
    ON reconciliation_runs (started_at DESC);

CREATE TABLE reconciliation_exceptions (
    id           UUID        PRIMARY KEY,
    run_id       UUID        NOT NULL REFERENCES reconciliation_runs (id),

    external_ref TEXT        NOT NULL,
    category     TEXT        NOT NULL,
    status       TEXT        NOT NULL DEFAULT 'OPEN',

    -- The ledger and saga rows this external_ref resolved to, when it
    -- resolved to one -- NULL is itself informative for MISSING_IN_LEDGER.
    -- No ON DELETE behaviour needed: nothing in this schema ever deletes a
    -- transaction or a saga.
    ledger_transaction_id UUID REFERENCES transactions (id),
    saga_id               UUID REFERENCES saga_instances (id),

    ledger_amount_minor BIGINT,
    psp_amount_minor    BIGINT,
    currency            TEXT,
    ledger_status       TEXT,
    psp_status          TEXT,

    -- Freeform, structured detail for whatever a category needs and the
    -- columns above do not capture -- the PSP row count for a DUPLICATE, the
    -- computed time gap for a TIMING_DIFFERENCE. Not queried into; this is for
    -- a human reading one exception, not for filtering a report.
    details      JSONB       NOT NULL DEFAULT '{}',

    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    resolved_at  TIMESTAMPTZ,

    CONSTRAINT reconciliation_exceptions_category_check
        CHECK (category IN (
            'MISSING_IN_LEDGER', 'MISSING_IN_PSP', 'AMOUNT_MISMATCH',
            'STATUS_MISMATCH', 'TIMING_DIFFERENCE', 'DUPLICATE'
        )),

    CONSTRAINT reconciliation_exceptions_status_check
        CHECK (status IN ('OPEN', 'AUTO_RESOLVED', 'RESOLVED')),

    -- An OPEN exception has not been resolved by anything, automated or
    -- human; the other two both have a resolved_at, because "auto-resolved"
    -- IS a resolution, just one this service performed itself rather than
    -- routed to a person.
    CONSTRAINT reconciliation_exceptions_resolved_at_check
        CHECK ((status = 'OPEN') = (resolved_at IS NULL))
);

-- One run's report: every exception it raised, in one indexed range scan.
CREATE INDEX reconciliation_exceptions_run_id_idx
    ON reconciliation_exceptions (run_id);

-- The triage queue across every run. Partial on OPEN, mirroring
-- saga_instances_manual_review_idx: this is the one status anyone acts on,
-- and it must stay cheap to scan even when it is empty.
CREATE INDEX reconciliation_exceptions_open_idx
    ON reconciliation_exceptions (created_at) WHERE status = 'OPEN';

-- "What has ever been said about this reference" -- across runs, since a
-- reference that mismatched yesterday and matches today is exactly the
-- pattern an operator investigating a recurring discrepancy needs to find.
CREATE INDEX reconciliation_exceptions_external_ref_idx
    ON reconciliation_exceptions (external_ref);
