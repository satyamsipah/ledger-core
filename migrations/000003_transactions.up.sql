CREATE TABLE transactions (
    id               UUID        PRIMARY KEY,
    idempotency_key  TEXT,
    transaction_type TEXT        NOT NULL,
    status           TEXT        NOT NULL DEFAULT 'PENDING',
    external_ref     TEXT,
    metadata         JSONB       NOT NULL DEFAULT '{}'::jsonb,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    posted_at        TIMESTAMPTZ,

    CONSTRAINT transactions_status_check
        CHECK (status IN ('PENDING', 'POSTED', 'REVERSED')),
    CONSTRAINT transactions_type_check
        CHECK (transaction_type IN
            ('TRANSFER', 'PAYIN', 'PAYOUT', 'FEE', 'FX', 'REVERSAL', 'ADJUSTMENT')),

    -- posted_at is set exactly when the transaction leaves PENDING, so the two
    -- can never drift and "when did this post?" never needs a journal scan.
    -- REVERSED implies the transaction posted first, hence a non-NULL value.
    CONSTRAINT transactions_posted_at_check
        CHECK ((status = 'PENDING') = (posted_at IS NULL))
);

-- Invariant 5, enforced by the database rather than only by the idempotency
-- service. Partial because reversals and internal adjustments carry no client
-- key, and NULL keys must not collide with each other.
CREATE UNIQUE INDEX transactions_idempotency_key_key
    ON transactions (idempotency_key) WHERE idempotency_key IS NOT NULL;

-- The saga timeout sweeper: "PENDING and older than N seconds". Partial on
-- status because PENDING is a transient state -- this index stays a few hundred
-- rows while the table grows past a hundred million.
CREATE INDEX transactions_pending_created_at_idx
    ON transactions (created_at) WHERE status = 'PENDING';

-- Support tooling: "find the transaction for gateway reference X".
CREATE INDEX transactions_external_ref_idx
    ON transactions (external_ref) WHERE external_ref IS NOT NULL;

-- No GIN index on metadata on purpose: nothing queries into it yet, and a GIN
-- index on a write-hot table costs real insert throughput. Add it when a query
-- exists, not before.
