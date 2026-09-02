-- Chart of accounts for local development.
--
-- Re-runnable: every insert is ON CONFLICT DO NOTHING, so `make seed` twice is
-- the same as once.
--
-- The UUIDs are fixed rather than generated so that tests, k6 scripts and
-- dashboard bookmarks can reference an account by literal. They are
-- well-formed UUIDv7 values (version nibble 7, variant bits 10) sharing one
-- synthetic timestamp prefix, matching what the application generates at
-- runtime.

BEGIN;

-- A fixed, well-known local-dev API key, so `curl` examples in the README and
-- ad hoc local testing do not require a round trip through cmd/issue-api-key
-- first. The raw value is deliberately obvious (`lk_live_dev00...`) so it is
-- unmistakable in a log line or a diff, and it authenticates ONLY against a
-- database seeded from this file -- there is nothing "live" about it despite
-- the KeyPrefix. Real principals are issued through cmd/issue-api-key, never
-- through this file.
INSERT INTO api_keys (id, principal_id, key_hash, status)
VALUES
    ('01920000-0000-7000-8000-0000000000f1', 'local-dev',
     '\x4679851119d8ac9c02d65ed27cf78d64c1ad0cc1de65057a0e6c701d7b7d25ae', 'ACTIVE')
ON CONFLICT (id) DO NOTHING;

INSERT INTO accounts (id, external_ref, account_type, normal_balance, currency, owner_id, allow_negative, status)
VALUES
    -- Platform's own cash at the bank. The asset side of every pay-in.
    ('01920000-0000-7000-8000-000000000001', 'platform-bank-inr',
     'ASSET', 'DEBIT', 'INR', NULL, FALSE, 'ACTIVE'),

    -- Money the gateway has acknowledged but not yet settled to us. Allowed to
    -- go negative: settlement files routinely arrive before the corresponding
    -- authorisation, and blocking that would reject real, correct traffic.
    ('01920000-0000-7000-8000-000000000002', 'gateway-suspense-inr',
     'ASSET', 'DEBIT', 'INR', NULL, TRUE, 'ACTIVE'),

    -- Fees earned by the platform.
    ('01920000-0000-7000-8000-000000000003', 'fee-revenue-inr',
     'REVENUE', 'CREDIT', 'INR', NULL, FALSE, 'ACTIVE'),

    -- Where a payout's funds wait between leaving a customer's wallet and
    -- reaching the merchant. Distinct from gateway-suspense-inr above, which
    -- is the INBOUND side, and unlike it this one must NOT allow negative: a
    -- settlement may only pay out what a reserve actually put in, and an
    -- overdrawable payout suspense would let a bug pay a merchant from
    -- nowhere. The saga's semantic lock is this account holding the money.
    ('01920000-0000-7000-8000-000000000005', 'payout-suspense-inr',
     'LIABILITY', 'CREDIT', 'INR', NULL, FALSE, 'ACTIVE'),

    -- Sub-unit residue from currency conversion. Small, and it must land
    -- somewhere or FX transactions cannot balance to the paisa.
    ('01920000-0000-7000-8000-000000000004', 'fx-rounding-inr',
     'EXPENSE', 'DEBIT', 'INR', NULL, TRUE, 'ACTIVE'),

    -- User wallets. A wallet is money the platform owes the user, so it is a
    -- LIABILITY with a CREDIT normal balance -- not an asset, however much the
    -- product surface calls it a balance.
    ('01920000-0000-7000-8000-000000000011', 'wallet-user-1001-inr',
     'LIABILITY', 'CREDIT', 'INR', 'user-1001', FALSE, 'ACTIVE'),
    ('01920000-0000-7000-8000-000000000012', 'wallet-user-1002-inr',
     'LIABILITY', 'CREDIT', 'INR', 'user-1002', FALSE, 'ACTIVE'),
    ('01920000-0000-7000-8000-000000000013', 'wallet-user-1003-inr',
     'LIABILITY', 'CREDIT', 'INR', 'user-1003', FALSE, 'ACTIVE'),

    -- Merchant payables: settled sales not yet paid out.
    ('01920000-0000-7000-8000-000000000021', 'payable-merchant-2001-inr',
     'LIABILITY', 'CREDIT', 'INR', 'merchant-2001', FALSE, 'ACTIVE'),
    ('01920000-0000-7000-8000-000000000022', 'payable-merchant-2002-inr',
     'LIABILITY', 'CREDIT', 'INR', 'merchant-2002', FALSE, 'ACTIVE'),

    -- A second currency, so multi-currency posting is exercised locally rather
    -- than discovered in production. Each currency leg of an FX transaction
    -- must balance on its own; these accounts are what makes that testable.
    ('01920000-0000-7000-8000-000000000101', 'platform-bank-usd',
     'ASSET', 'DEBIT', 'USD', NULL, FALSE, 'ACTIVE'),
    ('01920000-0000-7000-8000-000000000111', 'wallet-user-1001-usd',
     'LIABILITY', 'CREDIT', 'USD', 'user-1001', FALSE, 'ACTIVE')
ON CONFLICT (id) DO NOTHING;

-- Balance rows are derived from accounts rather than listed separately, so the
-- denormalised allow_negative flag cannot be seeded out of step with its
-- source. Every account gets a row at zero: a missing balance row and a zero
-- balance are different bugs, and only one of them is visible.
INSERT INTO account_balances (account_id, available_minor, pending_minor, allow_negative)
SELECT a.id, 0, 0, a.allow_negative
  FROM accounts a
ON CONFLICT (account_id) DO NOTHING;

COMMIT;

\echo 'Seeded chart of accounts:'
SELECT a.external_ref, a.account_type, a.normal_balance, a.currency,
       a.allow_negative, b.available_minor
  FROM accounts a
  JOIN account_balances b ON b.account_id = a.id
 ORDER BY a.currency, a.account_type, a.external_ref;
