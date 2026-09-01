-- Authentication, and the migration that closes D24: idempotency keys, and now
-- saga instances, sharing one global namespace with no principal behind it.
--
-- api_keys is deliberately small. There is no expiry, no scope, no rate limit
-- here -- those are real admin-surface work and belong with it, not bolted on
-- to close a namespace collision. What this table exists to do is give every
-- write a PRINCIPAL: a string this service did not take on trust from the
-- caller, but issued itself and can prove possession of.

CREATE TABLE api_keys (
    id           UUID        PRIMARY KEY,

    -- The caller this key authenticates as. A free-text identifier rather than
    -- a foreign key to anything -- there is no tenant/customer table in this
    -- schema, and inventing one to satisfy a column type here would be
    -- building Phase 6's admin surface to close a Phase 3 gap.
    principal_id TEXT        NOT NULL,

    -- SHA-256 of the raw key. The raw value is generated once, at issuance,
    -- handed to the caller, and never stored -- the same shape idempotency
    -- fingerprints already use, for the same reason: a database that is read
    -- (a backup, a replica, a careless SELECT *) must not hand out working
    -- credentials.
    key_hash     BYTEA       NOT NULL,

    status       TEXT        NOT NULL DEFAULT 'ACTIVE',
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    revoked_at   TIMESTAMPTZ,

    CONSTRAINT api_keys_status_check
        CHECK (status IN ('ACTIVE', 'REVOKED')),

    -- SHA-256 is exactly 32 bytes, mirroring idempotency_keys_fingerprint_len_check.
    CONSTRAINT api_keys_hash_len_check
        CHECK (length(key_hash) = 32),

    -- A revoked key carries the instant it stopped working; an active one
    -- never does. Mirrors transactions_posted_at_check's shape for the same
    -- reason: the two columns can never independently drift.
    CONSTRAINT api_keys_revoked_at_check
        CHECK ((status = 'REVOKED') = (revoked_at IS NOT NULL))
);

-- Authentication is exactly this lookup: hash the presented key, find the row.
-- Unique because two different callers must never be able to collide on one
-- hash and be handed the same principal.
CREATE UNIQUE INDEX api_keys_key_hash_key ON api_keys (key_hash);

-- Listing a principal's live keys, for the day key rotation exists. Partial on
-- ACTIVE because a revoked key is history, not something anything needs to
-- scan quickly.
CREATE INDEX api_keys_principal_id_idx ON api_keys (principal_id) WHERE status = 'ACTIVE';

-- idempotency_keys: add the principal, then repoint its primary key at it.
--
-- Add nullable, backfill, constrain -- not because this table is large (it is
-- TTL-bounded), but because that is the house rule regardless, and it is what
-- gives every pre-authentication row a real, if empty, value to carry rather
-- than leaving a NULL that a later UNIQUE(principal_id, key) would silently
-- fail to enforce across (NULL is never equal to NULL in Postgres uniqueness,
-- so two NULL-principal rows sharing a key would NOT collide, reopening
-- exactly the hole this migration exists to close).
ALTER TABLE idempotency_keys ADD COLUMN principal_id TEXT;
UPDATE idempotency_keys SET principal_id = '' WHERE principal_id IS NULL;
ALTER TABLE idempotency_keys ALTER COLUMN principal_id SET NOT NULL;
-- DEFAULT '' too, not merely backfilled: production code always supplies
-- principal_id explicitly, but a default is what keeps any OTHER writer of
-- this table -- a test fixture, a future maintenance script -- from failing
-- a NOT NULL constraint it has no reason to know exists, rather than silently
-- reopening the NULL-uniqueness hole the backfill just closed.
ALTER TABLE idempotency_keys ALTER COLUMN principal_id SET DEFAULT '';

-- The composite primary key IS the fix D24 describes: namespacing rather than
-- merely fingerprinting, so that a cross-tenant probe using someone else's key
-- finds no row at all -- not a 422 that confirms the key is in use, nothing.
ALTER TABLE idempotency_keys DROP CONSTRAINT idempotency_keys_pkey;
ALTER TABLE idempotency_keys ADD CONSTRAINT idempotency_keys_pkey PRIMARY KEY (principal_id, key);

-- transactions: the same fix, for the same reason, at a different layer.
--
-- WHY THIS TABLE TOO, WHEN D24's OWN TEXT ONLY NAMED idempotency_keys. D20
-- documents transactions_idempotency_key_key as a THIRD, independent defence
-- of invariant 5 -- "the one that would still be standing if the other two
-- were deleted." Scoping idempotency_keys alone and leaving this constraint
-- global would still let two different principals collide here: principal B
-- submitting principal A's exact key would pass B's own (now-scoped)
-- reservation, then fail at THIS constraint with "already exists" -- an
-- existence leak this migration exists to remove, just one layer down.
ALTER TABLE transactions ADD COLUMN principal_id TEXT;
UPDATE transactions SET principal_id = '' WHERE principal_id IS NULL;
ALTER TABLE transactions ALTER COLUMN principal_id SET NOT NULL;
ALTER TABLE transactions ALTER COLUMN principal_id SET DEFAULT '';

DROP INDEX transactions_idempotency_key_key;
CREATE UNIQUE INDEX transactions_idempotency_key_key
    ON transactions (principal_id, idempotency_key) WHERE idempotency_key IS NOT NULL;

-- saga_instances: the same fix, for the saga-level dedupe D24's own text
-- specifically flagged as inheriting the gap.
ALTER TABLE saga_instances ADD COLUMN principal_id TEXT;
UPDATE saga_instances SET principal_id = '' WHERE principal_id IS NULL;
ALTER TABLE saga_instances ALTER COLUMN principal_id SET NOT NULL;
ALTER TABLE saga_instances ALTER COLUMN principal_id SET DEFAULT '';

DROP INDEX saga_instances_idempotency_key_key;
CREATE UNIQUE INDEX saga_instances_idempotency_key_key
    ON saga_instances (principal_id, idempotency_key) WHERE idempotency_key IS NOT NULL;
