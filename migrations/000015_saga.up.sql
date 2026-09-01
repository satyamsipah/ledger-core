-- The saga orchestrator's durable state: the instance table that IS the state
-- machine, and the append-only audit log of every attempt made against it.
--
-- WHY THIS TABLE EXISTS AT ALL, rather than the orchestrator holding its
-- progress in memory: a saga spans an external system, so it necessarily
-- outlives any single process. An orchestrator that keeps "which step am I on"
-- in a goroutine loses it to a deploy, an OOM kill, or a node reboot -- and
-- loses it precisely when money is already out of a customer's wallet and not
-- yet in a merchant's. Recovering that from the journal alone is not possible:
-- the journal records what moved, never what was INTENDED to move next.
--
-- WHY status ONLY EVER HOLDS SETTLED STATES. There is deliberately no
-- RESERVING/SETTLING value here. Every forward step commits its ledger entries
-- and its status transition in ONE database transaction (see the AdvanceSaga
-- method on internal/ledger's Tx port), so there is no instant at which a
-- crash can strand this row halfway through a step. In-flight-ness is carried
-- by lease_expires_at instead, which is the same shape idempotency_keys uses
-- for IN_PROGRESS in migration 000010 and rests on the same property: a lease
-- that has run out is PROOF that its owner committed nothing, so reclaiming it
-- needs no fencing token and no lock service.
--
-- The single exception is GATEWAY, and it is the whole reason this phase is
-- interesting -- see saga_steps below.

CREATE TABLE saga_instances (
    id           UUID        PRIMARY KEY,
    saga_type    TEXT        NOT NULL,

    -- The step the orchestrator will attempt next, for the dashboard and for
    -- correlating with saga_steps. status is what drives behaviour; this is
    -- what a human reads.
    current_step TEXT        NOT NULL,
    status       TEXT        NOT NULL DEFAULT 'PENDING',

    -- The saga's inputs: account ids, amount, fee. IMMUTABLE once written --
    -- nothing in this codebase updates it, and nothing may. A resume after a
    -- crash must resume with the same inputs the earlier attempt used, or the
    -- compensation it eventually runs will reverse a different transfer than
    -- the one that was posted.
    payload      JSONB       NOT NULL,

    retry_count  INT         NOT NULL DEFAULT 0,
    last_error   TEXT,

    -- Saga-level idempotency: two POST /v1/payouts with one key start one
    -- saga. Distinct from transactions.idempotency_key, which dedupes a single
    -- ledger transaction; this dedupes the whole multi-step operation.
    idempotency_key TEXT,

    -- Who is driving this saga right now, and until when. Several orchestrator
    -- replicas claim work through these two columns and FOR UPDATE SKIP
    -- LOCKED, so no leader election is needed -- the same competing-consumers
    -- shape the polling outbox publisher uses (D31).
    lease_owner      TEXT,
    lease_expires_at TIMESTAMPTZ,

    -- Requirement 5: every step has a deadline. The sweeper finds rows past
    -- this instant and decides to retry, probe, or compensate.
    step_deadline_at TIMESTAMPTZ NOT NULL,

    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT now(),

    CONSTRAINT saga_instances_status_check
        CHECK (status IN (
            'PENDING',              -- created; no money has moved
            'RESERVED',             -- wallet debited into suspense
            'GATEWAY_PENDING',      -- intent recorded, outcome UNKNOWN
            'GATEWAY_SUCCEEDED',    -- outcome confirmed by the gateway
            'GATEWAY_FAILED',       -- outcome confirmed by the gateway
            'COMPENSATING',         -- running compensations in reverse order
            'COMPLETED',            -- terminal: settled to merchant and fees
            'COMPENSATED',          -- terminal: ledger back to pre-saga state
            'FAILED',               -- terminal: failed before anything moved
            'NEEDS_MANUAL_REVIEW'   -- terminal for automation; a human must act
        )),

    CONSTRAINT saga_instances_current_step_check
        CHECK (current_step IN ('RESERVE', 'GATEWAY', 'SETTLE', 'DONE')),

    CONSTRAINT saga_instances_retry_count_check CHECK (retry_count >= 0),

    -- A lease is a pair or neither. A lease_expires_at with no owner cannot be
    -- reclaimed sensibly, and an owner with no expiry never yields.
    CONSTRAINT saga_instances_lease_check
        CHECK ((lease_owner IS NULL) = (lease_expires_at IS NULL))
);

CREATE TRIGGER saga_instances_set_updated_at
    BEFORE UPDATE ON saga_instances
    FOR EACH ROW EXECUTE FUNCTION set_updated_at();

-- Saga-level idempotency, partial so that internally-started sagas carrying no
-- key do not all collide on NULL. Mirrors transactions_idempotency_key_key.
CREATE UNIQUE INDEX saga_instances_idempotency_key_key
    ON saga_instances (idempotency_key) WHERE idempotency_key IS NOT NULL;

-- The timeout sweeper's only query: "which live sagas have blown their step
-- deadline". Partial on the four terminal states because a completed saga is
-- never swept again -- which is what keeps this index a few hundred rows while
-- the table grows without bound.
CREATE INDEX saga_instances_deadline_idx
    ON saga_instances (step_deadline_at)
    WHERE status NOT IN ('COMPLETED', 'COMPENSATED', 'FAILED', 'NEEDS_MANUAL_REVIEW');

-- The dashboard's stuck-saga list, and the alerting query behind
-- ledger_saga_manual_review_total. Partial because this is the one status
-- anyone pages on, and it must stay cheap to count even when it is empty.
CREATE INDEX saga_instances_manual_review_idx
    ON saga_instances (created_at)
    WHERE status = 'NEEDS_MANUAL_REVIEW';

-- saga_steps is the audit log of EVERY attempt, successful or not.
--
-- WHY IT IS NOT MERELY A LOG, AND WHY THE GATEWAY STEP WRITES HERE BEFORE
-- CALLING OUT. A ledger step commits atomically with its status transition, so
-- its own row here is pure history. The gateway step cannot: an HTTP call to
-- another company is not in this transaction, and no amount of schema design
-- will put it there. So the orchestrator commits an ATTEMPTED row -- carrying
-- the gateway_key it is about to use -- and only THEN makes the call.
--
-- That ordering is the entire ambiguity strategy. If the process dies, or the
-- call times out, or the connection is lost, what survives is a durable record
-- saying "a payment MAY have been submitted under key K". The saga cannot know
-- the outcome, but it knows the question and it knows the key to ask it with,
-- which is enough to resolve the ambiguity by QUERY instead of by guess. The
-- opposite ordering -- call first, record after -- loses the key itself in the
-- crash, and a payment you cannot name is a payment you cannot reconcile.
--
-- Append-only in the same spirit as journal_entries: rows are INSERTed and the
-- outcome UPDATE only ever fills in status/error/finished_at on the row for
-- this attempt. A retry is a NEW row with a higher attempt number, never an
-- overwrite of the failed one, because "how many times did we try and what
-- went wrong each time" is exactly what an operator needs at 3am.
CREATE TABLE saga_steps (
    id             UUID        PRIMARY KEY,
    saga_id        UUID        NOT NULL REFERENCES saga_instances (id),
    step           TEXT        NOT NULL,
    attempt        INT         NOT NULL,
    direction      TEXT        NOT NULL,
    status         TEXT        NOT NULL,

    -- The ledger transaction this step posted, for the audit trail joining a
    -- saga to the money it moved. NULL for the gateway step, which posts none.
    transaction_id UUID        REFERENCES transactions (id),

    -- The idempotency key sent to the external gateway. Stable across every
    -- attempt of this step, so a retry after an ambiguous outcome is the same
    -- logical call rather than a second charge.
    gateway_key    TEXT,

    error          TEXT,
    started_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    finished_at    TIMESTAMPTZ,

    CONSTRAINT saga_steps_step_check
        CHECK (step IN ('RESERVE', 'GATEWAY', 'SETTLE')),
    CONSTRAINT saga_steps_direction_check
        CHECK (direction IN ('FORWARD', 'COMPENSATION')),
    CONSTRAINT saga_steps_status_check
        CHECK (status IN ('ATTEMPTED', 'SUCCEEDED', 'FAILED')),
    CONSTRAINT saga_steps_attempt_check CHECK (attempt >= 1),

    -- A terminal step has a finish time; an ATTEMPTED one does not. Keeps the
    -- "which gateway calls are unresolved" query honest.
    CONSTRAINT saga_steps_finished_at_check
        CHECK ((status = 'ATTEMPTED') = (finished_at IS NULL)),

    -- One row per attempt. This is what makes recording an attempt idempotent:
    -- a retry that re-INSERTs the same attempt number collides rather than
    -- silently logging the same try twice.
    CONSTRAINT saga_steps_attempt_key UNIQUE (saga_id, step, direction, attempt)
);

-- Rendering one saga's history in order, which is both the dashboard's detail
-- view and how the orchestrator finds the unresolved gateway attempt on resume.
CREATE INDEX saga_steps_saga_id_started_at_idx
    ON saga_steps (saga_id, started_at);
