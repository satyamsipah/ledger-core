DROP TRIGGER IF EXISTS accounts_sync_allow_negative ON accounts;
DROP TRIGGER IF EXISTS account_balances_set_updated_at ON account_balances;

DROP FUNCTION IF EXISTS ledger_sync_allow_negative();

DROP TABLE IF EXISTS account_balances;
