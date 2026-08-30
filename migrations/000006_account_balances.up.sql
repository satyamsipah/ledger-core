CREATE TABLE account_balances (
    account_id      UUID        PRIMARY KEY REFERENCES accounts (id),
    available_minor BIGINT      NOT NULL DEFAULT 0,
    pending_minor   BIGINT      NOT NULL DEFAULT 0,

    -- Denormalised from accounts. A CHECK constraint cannot reference another
    -- table, and invariant 4 calls for a CHECK rather than a trigger, so the
    -- flag has to live on the row being written. Kept in step by
    -- ledger_sync_allow_negative() below.
    allow_negative  BOOLEAN     NOT NULL DEFAULT FALSE,

    last_entry_id   UUID,
    version         BIGINT      NOT NULL DEFAULT 0,
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now(),

    -- Invariant 4.
    CONSTRAINT account_balances_no_overdraft_check
        CHECK (allow_negative OR available_minor >= 0),

    -- pending_minor is the sum of outstanding holds, never a net delta, so it
    -- has no meaningful negative value.
    CONSTRAINT account_balances_pending_check
        CHECK (pending_minor >= 0),
    CONSTRAINT account_balances_version_check
        CHECK (version >= 0)
);

-- Keeps the denormalised flag honest. Note that flipping allow_negative to
-- false on an account that is currently overdrawn will fail
-- account_balances_no_overdraft_check here, which is the correct outcome: a
-- loud error beats a silently violated invariant.
CREATE OR REPLACE FUNCTION ledger_sync_allow_negative() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    UPDATE account_balances
       SET allow_negative = NEW.allow_negative
     WHERE account_id = NEW.id;
    RETURN NULL;
END;
$$;

CREATE TRIGGER accounts_sync_allow_negative
    AFTER UPDATE OF allow_negative ON accounts
    FOR EACH ROW
    WHEN (OLD.allow_negative IS DISTINCT FROM NEW.allow_negative)
    EXECUTE FUNCTION ledger_sync_allow_negative();

CREATE TRIGGER account_balances_set_updated_at
    BEFORE UPDATE ON account_balances
    FOR EACH ROW EXECUTE FUNCTION set_updated_at();

-- The most-written table in the system, and the only one updated in place.
-- Leaving 30% of each page free lets those updates stay HOT (heap-only tuple),
-- which skips the index write entirely. It works precisely because this table
-- carries no secondary indexes: every access is a primary-key point lookup
-- (WHERE account_id = $1 FOR UPDATE), and any secondary index would force an
-- index write on every balance change and undo the benefit.
ALTER TABLE account_balances SET (fillfactor = 70);
