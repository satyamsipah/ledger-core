CREATE TABLE journal_entries (
    id             UUID        PRIMARY KEY,
    transaction_id UUID        NOT NULL REFERENCES transactions (id),
    account_id     UUID        NOT NULL,
    direction      TEXT        NOT NULL,
    amount_minor   BIGINT      NOT NULL,
    currency       CHAR(3)     NOT NULL,
    entry_seq      INT         NOT NULL,
    created_at     TIMESTAMPTZ NOT NULL DEFAULT now(),

    CONSTRAINT journal_entries_direction_check
        CHECK (direction IN ('DEBIT', 'CREDIT')),

    -- Invariant 3: sign lives in `direction`, never in the amount. A zero entry
    -- is meaningless, and a negative one means someone encoded direction twice.
    CONSTRAINT journal_entries_amount_check
        CHECK (amount_minor > 0),
    CONSTRAINT journal_entries_entry_seq_check
        CHECK (entry_seq >= 0),

    -- Composite foreign key against accounts (id, currency): an entry can only
    -- reference an account that actually holds that currency. Declarative, so
    -- no code path and no future service can post USD into an INR wallet.
    CONSTRAINT journal_entries_account_currency_fkey
        FOREIGN KEY (account_id, currency) REFERENCES accounts (id, currency)
);

-- Every read of a transaction renders its legs in order, and the deferred
-- balance trigger aggregates on exactly this key at COMMIT. UNIQUE also stops
-- two entries claiming the same slot within one transaction.
CREATE UNIQUE INDEX journal_entries_transaction_id_entry_seq_key
    ON journal_entries (transaction_id, entry_seq);

-- The account statement query: entries for one account over a time range,
-- newest first, keyset-paginated on (created_at, id) -- id breaks ties so
-- pagination cannot skip or repeat a row when two entries share a timestamp.
--
-- The INCLUDE columns make it index-only, and that pays off here specifically
-- because this table is append-only: its pages go all-visible after one vacuum
-- and stay there, so the visibility-map check that normally defeats index-only
-- scans always passes.
CREATE INDEX journal_entries_account_id_created_at_idx
    ON journal_entries (account_id, created_at DESC, id DESC)
    INCLUDE (direction, amount_minor, currency, transaction_id);

-- No standalone index on account_id (leading column of the above), none on
-- currency (low cardinality, never queried without an account or transaction),
-- and none added purely to back the foreign keys: a child-side index only
-- matters for cascading deletes, and nothing here is ever deleted.
