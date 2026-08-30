CREATE TABLE accounts (
    id              UUID        PRIMARY KEY,
    external_ref    TEXT        NOT NULL,
    account_type    TEXT        NOT NULL,
    normal_balance  TEXT        NOT NULL,
    currency        CHAR(3)     NOT NULL,
    owner_id        TEXT,
    allow_negative  BOOLEAN     NOT NULL DEFAULT FALSE,
    status          TEXT        NOT NULL DEFAULT 'ACTIVE',
    version         BIGINT      NOT NULL DEFAULT 1,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now(),

    CONSTRAINT accounts_account_type_check
        CHECK (account_type IN ('ASSET', 'LIABILITY', 'EQUITY', 'REVENUE', 'EXPENSE')),
    CONSTRAINT accounts_normal_balance_check
        CHECK (normal_balance IN ('DEBIT', 'CREDIT')),

    -- Standard accounting: an account's normal balance follows from its type.
    -- Letting the two disagree means every report that derives sign from type
    -- silently reports the opposite of the truth.
    --
    -- NOTE: this forbids contra accounts (e.g. a contra-asset carrying a CREDIT
    -- normal balance). Deliberate for a payments ledger; drop this one
    -- constraint the day a real contra account is needed.
    CONSTRAINT accounts_normal_balance_matches_type_check CHECK (
        (account_type IN ('ASSET', 'EXPENSE') AND normal_balance = 'DEBIT') OR
        (account_type IN ('LIABILITY', 'EQUITY', 'REVENUE') AND normal_balance = 'CREDIT')
    ),

    CONSTRAINT accounts_currency_check
        CHECK (currency ~ '^[A-Z]{3}$'),
    CONSTRAINT accounts_status_check
        CHECK (status IN ('ACTIVE', 'FROZEN', 'CLOSED')),
    CONSTRAINT accounts_version_check
        CHECK (version > 0),

    -- Exists solely so journal_entries can carry a composite foreign key on
    -- (account_id, currency). See journal_entries_account_currency_fkey.
    CONSTRAINT accounts_id_currency_key UNIQUE (id, currency)
);

-- External systems (payment gateway, user service) address accounts by their
-- own reference rather than by our UUID. This is the lookup on essentially
-- every inbound request.
CREATE UNIQUE INDEX accounts_external_ref_key ON accounts (external_ref);

-- "Every account belonging to this user or merchant" -- wallet lists in the
-- dashboard and per-owner balance rollups.
CREATE INDEX accounts_owner_id_currency_idx ON accounts (owner_id, currency)
    WHERE owner_id IS NOT NULL;

-- No index on account_type or status on purpose: both are low-cardinality on a
-- table measured in thousands, where a sequential scan beats the write cost of
-- maintaining the index.

CREATE TRIGGER accounts_set_updated_at
    BEFORE UPDATE ON accounts
    FOR EACH ROW EXECUTE FUNCTION set_updated_at();
