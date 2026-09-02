-- Partial, and honestly so, in the same spirit as 000009.down and 000012.down.
--
-- Restoring PRIMARY KEY (key) alone on idempotency_keys -- and the single-
-- column unique indexes on transactions and saga_instances -- FAILS if two
-- different principals used the identical raw key or idempotency value while
-- this migration was applied. Once scoping is in place that is an intended,
-- routine occurrence rather than a bug, so a rollback across live scoped data
-- is expected to need manual reconciliation first. Pretending otherwise here
-- would be a down-migration that looks safe and is not.

DROP INDEX IF EXISTS saga_instances_idempotency_key_key;
CREATE UNIQUE INDEX saga_instances_idempotency_key_key
    ON saga_instances (idempotency_key) WHERE idempotency_key IS NOT NULL;
ALTER TABLE saga_instances DROP COLUMN IF EXISTS principal_id;

DROP INDEX IF EXISTS transactions_idempotency_key_key;
CREATE UNIQUE INDEX transactions_idempotency_key_key
    ON transactions (idempotency_key) WHERE idempotency_key IS NOT NULL;
ALTER TABLE transactions DROP COLUMN IF EXISTS principal_id;

ALTER TABLE idempotency_keys DROP CONSTRAINT IF EXISTS idempotency_keys_pkey;
ALTER TABLE idempotency_keys ADD CONSTRAINT idempotency_keys_pkey PRIMARY KEY (key);
ALTER TABLE idempotency_keys DROP COLUMN IF EXISTS principal_id;

DROP INDEX IF EXISTS api_keys_principal_id_idx;
DROP INDEX IF EXISTS api_keys_key_hash_key;
DROP TABLE IF EXISTS api_keys;
