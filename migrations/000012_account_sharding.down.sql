-- Reversing sharding deletes no accounts and no journal entries, which means
-- it is only a partial reversal -- and that is deliberate.
--
-- Shards are real accounts holding real balances, and their journal entries are
-- append-only by invariant 2. Dropping parent_account_id therefore does not
-- un-shard anything: it turns each shard into an ordinary standalone account
-- that still holds its share of the balance, and the logical account stops
-- being able to see them. Money is not lost, but it stops being summed.
--
-- The alternative -- sweeping shard balances back into the parent on the way
-- down -- was rejected. That is a ledger movement, and a ledger movement that
-- happens inside a migration is one with no journal entries behind it, which
-- breaks invariant 2 to tidy up a rollback. Draining shards is the
-- rebalancer's job, through ordinary transactions, before this migration is
-- reversed.
--
-- So: reverse this only on an installation with no sharded accounts, or after
-- rebalancing them to zero. A migration that cannot be safely reversed under
-- all conditions is worth saying so about, rather than pretending otherwise.

DROP FUNCTION IF EXISTS ledger_shard_account(UUID, INT);

DROP INDEX IF EXISTS accounts_parent_account_id_idx;
DROP INDEX IF EXISTS accounts_shard_position_key;

ALTER TABLE accounts
    DROP CONSTRAINT accounts_no_self_parent_check,
    DROP CONSTRAINT accounts_no_nested_sharding_check,
    DROP CONSTRAINT accounts_shard_index_check,
    DROP CONSTRAINT accounts_shard_pair_check,
    DROP CONSTRAINT accounts_shard_count_check;

ALTER TABLE accounts DROP COLUMN shard_count;
ALTER TABLE accounts DROP COLUMN shard_index;
ALTER TABLE accounts DROP COLUMN parent_account_id;
