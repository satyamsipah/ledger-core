-- Sub-account sharding for extremely hot accounts.
--
-- THE PROBLEM. Every write to an account serialises on one row lock in
-- account_balances. That is exactly what makes the overdraft CHECK meaningful
-- (D10), and it means a single account's write throughput is bounded by
-- 1/(lock hold time) no matter how much hardware is behind it. A payments
-- ledger always has at least one such account: every pay-in credits the same
-- house float.
--
-- THE SHAPE. A sharded account keeps its identity and gains N child accounts.
-- Writes hash to a random child, so contention spreads across N row locks, and
-- the logical balance is the SUM over the children. Shards are ordinary rows in
-- `accounts` rather than a new table, which is the whole reason this is a small
-- change: the composite foreign key from journal_entries, the deferred balance
-- trigger, the overdraft CHECK, the balance-row trigger and the ordered locking
-- of D11 all keep working untouched, because a shard is just an account.
--
-- WHAT THIS DOES TO INVARIANT 4, STATED PLAINLY BECAUSE IT IS SUBTLE.
--
-- account_balances_no_overdraft_check is per row. A debit routed to shard 7
-- checks shard 7's balance, not the logical total. So:
--
--   * SAFETY IS PRESERVED. Every shard is individually non-negative, so their
--     sum is non-negative. You cannot overdraw the logical account without
--     overdrawing some shard first, and the constraint stops that. Invariant 4
--     still holds.
--
--   * LIVENESS IS WEAKENED. A logical balance of 1600 spread as 100 across 16
--     shards will REFUSE a debit of 200 that the account can plainly afford.
--     The check is conservative, not wrong.
--
-- Sharding therefore trades false refusals for throughput, which is the correct
-- direction for a ledger -- but only on accounts where a shard running dry is
-- not a real outcome. That means accounts whose traffic is effectively
-- one-directional: house floats, revenue, fee collection. A drainable customer
-- wallet must NOT be sharded. The database cannot check traffic direction, so
-- that restriction lives in docs/DECISIONS.md D24 and in the function below,
-- which refuses to shard anything without an explicit call.
--
-- The fix for the false refusal is a rebalancer that moves value between
-- sibling shards as ordinary internal transactions. It is Phase 4 and is
-- deliberately not built here.

-- A shard is a child; shard_count is meaningful only on a parent.
ALTER TABLE accounts ADD COLUMN parent_account_id UUID REFERENCES accounts (id);
ALTER TABLE accounts ADD COLUMN shard_index       INT;
ALTER TABLE accounts ADD COLUMN shard_count       INT NOT NULL DEFAULT 1;

ALTER TABLE accounts
    ADD CONSTRAINT accounts_shard_count_check
        CHECK (shard_count >= 1),

    -- A shard has both a parent and a position, or neither. One without the
    -- other is a half-created shard, and the routing query would either skip it
    -- or hash to a position that does not exist.
    ADD CONSTRAINT accounts_shard_pair_check
        CHECK ((parent_account_id IS NULL) = (shard_index IS NULL)),

    ADD CONSTRAINT accounts_shard_index_check
        CHECK (shard_index IS NULL OR shard_index >= 0),

    -- No sharding a shard. Two levels would make the logical balance a
    -- recursive sum, and every read path here is deliberately one join deep.
    ADD CONSTRAINT accounts_no_nested_sharding_check
        CHECK (parent_account_id IS NULL OR shard_count = 1),

    -- An account cannot be its own parent. The FK permits it and the routing
    -- query would loop forever on it.
    ADD CONSTRAINT accounts_no_self_parent_check
        CHECK (parent_account_id IS DISTINCT FROM id);

-- One shard per position. Two accounts claiming shard 3 would both receive
-- traffic and the SUM would still be right, but "shard 3" would stop naming a
-- single row, and the rebalancer that arrives in Phase 4 needs it to.
CREATE UNIQUE INDEX accounts_shard_position_key
    ON accounts (parent_account_id, shard_index)
    WHERE parent_account_id IS NOT NULL;

-- Serves the two queries sharding adds: routing a write to the shard set, and
-- summing the logical balance. Both are "all children of this parent", on every
-- read of a sharded account, so this index is on the hot path rather than
-- speculative.
CREATE INDEX accounts_parent_account_id_idx
    ON accounts (parent_account_id)
    WHERE parent_account_id IS NOT NULL;

-- ledger_shard_account splits an existing account into n shards.
--
-- A function rather than an admin endpoint because the admin surface is Phase 4
-- work, and leaving this to hand-written INSERTs would mean every operator
-- reconstructing the invariants above from the constraint names. It is
-- idempotent on shard_count so a re-run is a no-op rather than a second set of
-- shards.
--
-- Note what it does NOT do: move any existing balance onto the shards. The
-- parent keeps whatever it holds and stays part of the SUM, so the logical
-- balance is unchanged by sharding. Draining the parent into its shards is the
-- rebalancer's job.
CREATE OR REPLACE FUNCTION ledger_shard_account(p_account_id UUID, p_shards INT)
RETURNS VOID
LANGUAGE plpgsql AS $$
DECLARE
    v_account accounts%ROWTYPE;
    i INT;
BEGIN
    IF p_shards < 2 THEN
        RAISE EXCEPTION 'shard count must be at least 2, got %', p_shards;
    END IF;

    SELECT * INTO v_account FROM accounts WHERE id = p_account_id FOR UPDATE;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'account % does not exist', p_account_id;
    END IF;
    IF v_account.parent_account_id IS NOT NULL THEN
        RAISE EXCEPTION 'account % is itself a shard and cannot be sharded', p_account_id;
    END IF;
    IF v_account.shard_count > 1 THEN
        RETURN; -- already sharded; idempotent by design
    END IF;

    UPDATE accounts SET shard_count = p_shards WHERE id = p_account_id;

    FOR i IN 0 .. p_shards - 1 LOOP
        INSERT INTO accounts (
            id, external_ref, account_type, normal_balance, currency, owner_id,
            allow_negative, status, parent_account_id, shard_index
        )
        VALUES (
            gen_random_uuid(),
            v_account.external_ref || '#shard-' || i,
            v_account.account_type,
            v_account.normal_balance,
            v_account.currency,
            v_account.owner_id,
            v_account.allow_negative,
            v_account.status,
            p_account_id,
            i
        );
    END LOOP;
END;
$$;

COMMENT ON FUNCTION ledger_shard_account(UUID, INT) IS
    'Split an account into n shards to spread row-level write contention. '
    'Only for accounts whose traffic is effectively one-directional; see '
    'docs/DECISIONS.md D24 for why a drainable wallet must not be sharded.';
