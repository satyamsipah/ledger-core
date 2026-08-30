-- Every account gets its balance row at creation, from a trigger rather than
-- from whichever code path happened to insert the account.
--
-- WHY THIS IS A CORRECTNESS FIX AND NOT A CONVENIENCE
--
-- The posting path serialises concurrent writers by taking
-- `SELECT ... FROM account_balances WHERE account_id = ANY($1) FOR UPDATE`.
-- A row lock on a row that does not exist locks nothing, and -- this is the
-- dangerous part -- reports no error while doing so. Two concurrent transfers
-- against an account whose balance row was never inserted would both sail past
-- the lock, both read nothing, and neither would block the other: the
-- serialisation point silently disappears at exactly the moment it matters.
--
-- Making the row's existence a property of the schema means the posting path
-- can treat "no row returned" as a missing account, which is a real error,
-- rather than as an empty balance, which is a plausible-looking lie.
--
-- ON CONFLICT DO NOTHING so that callers which still insert the balance row
-- themselves -- deploy/seed/seed.sql does, deliberately, to keep the
-- denormalised allow_negative flag derived from one place -- are unaffected
-- rather than newly broken.
CREATE OR REPLACE FUNCTION ledger_create_account_balance() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    INSERT INTO account_balances (account_id, available_minor, pending_minor, allow_negative)
    VALUES (NEW.id, 0, 0, NEW.allow_negative)
    ON CONFLICT (account_id) DO NOTHING;
    RETURN NULL;
END;
$$;

CREATE TRIGGER accounts_create_balance
    AFTER INSERT ON accounts
    FOR EACH ROW EXECUTE FUNCTION ledger_create_account_balance();

-- Backfill: the trigger only covers accounts inserted from here on, and this
-- migration may run against a database that already carries accounts whose
-- balance row was never written. Cheap on any realistic accounts table, which
-- is measured in thousands.
INSERT INTO account_balances (account_id, available_minor, pending_minor, allow_negative)
SELECT a.id, 0, 0, a.allow_negative
  FROM accounts a
ON CONFLICT (account_id) DO NOTHING;
