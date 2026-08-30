DROP TRIGGER IF EXISTS accounts_create_balance ON accounts;

DROP FUNCTION IF EXISTS ledger_create_account_balance();

-- The backfilled rows are deliberately left in place. This migration adds a
-- trigger; reversing it removes the trigger. Deleting balance rows on the way
-- down would discard live balances for every account created while it was
-- installed, which is data loss dressed up as a rollback.
