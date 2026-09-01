-- The balance projector's own state: the read model it maintains, and the
-- dedupe table that makes applying it safe under at-least-once delivery.
--
-- Neither table is authoritative. account_balances (migration 000006) is the
-- balance the write path enforces the overdraft CHECK against and the only
-- one anything in internal/ledger ever reads. balance_projections is a
-- SEPARATE, independently-derived view built purely from the Kafka event
-- stream -- and the two disagreeing is not a bug to be prevented here, it is
-- the exact signal the reconciliation engine exists to catch. Building the
-- projection from a second, unrelated code path (a Kafka consumer, not the
-- posting transaction) is what makes that comparison mean anything: two
-- numbers computed the same way agreeing proves nothing.

CREATE TABLE balance_projections (
    account_id      UUID        PRIMARY KEY REFERENCES accounts (id),
    available_minor BIGINT      NOT NULL,
    currency        CHAR(3)     NOT NULL,

    -- Mirrors account_balances.version, and for the identical reason (D12):
    -- the guard that lets an out-of-order or redelivered TransactionPosted be
    -- applied safely. See processed_events below for why version alone is
    -- not sufficient on its own and processed_events is still required.
    version         BIGINT      NOT NULL,

    -- The event that produced this row's current state, for support tooling
    -- asking "why does the projection say this" without joining through
    -- processed_events.
    last_event_id   UUID        NOT NULL,
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now(),

    CONSTRAINT balance_projections_version_check CHECK (version >= 0)
);

CREATE TRIGGER balance_projections_set_updated_at
    BEFORE UPDATE ON balance_projections
    FOR EACH ROW EXECUTE FUNCTION set_updated_at();

-- processed_events is what turns at-least-once delivery into effectively-once
-- application. WHY THIS IS STILL NEEDED ALONGSIDE THE VERSION COMPARE-AND-SET
-- ON balance_projections, rather than the version guard alone being enough:
-- version-CAS makes REDELIVERY of an event already applied a no-op (the
-- incoming version is not greater than the stored one, so the UPDATE's WHERE
-- clause matches nothing) -- but it says nothing about a message that fails
-- AFTER the projection UPDATE but BEFORE the consumer's own offset commit.
-- Without a record of "this event_id was handled", a crash in that window
-- means the NEXT delivery of the same event re-enters the apply path, and
-- while the projection UPDATE itself is harmless (WHERE version < $new
-- matches nothing the second time), any other side effect of applying an
-- event -- a log line, a downstream notification this consumer might grow --
-- would not be. processed_events makes "have I handled this event_id" a fact
-- checked and recorded in the SAME local transaction as the projection
-- update, closing that gap the same way idempotency_keys closes it for the
-- write path in Phase 3.
CREATE TABLE processed_events (
    event_id     UUID        PRIMARY KEY,
    event_type   TEXT        NOT NULL,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- The retention purge scans by age; BRIN for the same reason as
-- outbox_created_at_brin_idx (migration 000008) -- append-only, physically
-- correlated with insertion order, kilobytes where a btree would cost
-- gigabytes.
CREATE INDEX processed_events_processed_at_brin_idx
    ON processed_events USING BRIN (processed_at);
