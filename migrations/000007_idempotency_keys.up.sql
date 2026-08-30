-- The primary key on `key` is not merely a lookup index: it IS the concurrency
-- mechanism behind invariant 5. Two simultaneous requests both attempt
-- INSERT ... ON CONFLICT DO NOTHING; exactly one wins the row, and the loser
-- reads the winner's record and either replays its response or waits. No
-- advisory locks, and no dependency on Redis for correctness -- Redis stays a
-- latency optimisation on the read path only.
CREATE TABLE idempotency_keys (
    key                 TEXT        PRIMARY KEY,
    request_fingerprint BYTEA       NOT NULL,
    status              TEXT        NOT NULL,
    response_status     INT,
    response_body       JSONB,
    transaction_id      UUID        REFERENCES transactions (id),
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    expires_at          TIMESTAMPTZ NOT NULL,

    CONSTRAINT idempotency_keys_status_check
        CHECK (status IN ('IN_PROGRESS', 'COMPLETED', 'FAILED')),

    -- SHA-256 is exactly 32 bytes. Storing bytea rather than 64 hex characters
    -- halves the row and makes a wrong-length write impossible.
    CONSTRAINT idempotency_keys_fingerprint_len_check
        CHECK (length(request_fingerprint) = 32),

    -- A COMPLETED record must actually carry the response it promises to
    -- replay, or the replay path cheerfully returns a 200 with an empty body.
    CONSTRAINT idempotency_keys_completed_check
        CHECK (status <> 'COMPLETED'
               OR (response_status IS NOT NULL AND response_body IS NOT NULL)),

    CONSTRAINT idempotency_keys_expiry_check
        CHECK (expires_at > created_at)
);

-- The TTL reaper: "delete everything past expires_at", run on a schedule.
-- Without it this table grows without bound and its primary key degrades with
-- it. The primary key already covers the lookup path.
CREATE INDEX idempotency_keys_expires_at_idx ON idempotency_keys (expires_at);

-- No index on transaction_id on purpose: the reverse lookup ("which key
-- created this transaction?") is ops-only and rare enough to scan.
