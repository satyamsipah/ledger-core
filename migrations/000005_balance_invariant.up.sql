-- ---------------------------------------------------------------------------
-- Invariant 2: journal_entries is append-only.
-- ---------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION ledger_reject_journal_mutation() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION 'journal_entries is append-only; % rejected (entry %)',
        TG_OP, COALESCE(OLD.id::text, '<statement>')
        USING ERRCODE = 'restrict_violation',
              HINT = 'Corrections are made by posting a reversing transaction.';
END;
$$;

CREATE TRIGGER journal_entries_no_mutation
    BEFORE UPDATE OR DELETE ON journal_entries
    FOR EACH ROW EXECUTE FUNCTION ledger_reject_journal_mutation();

-- TRUNCATE does not fire row triggers. Without this second, statement-level
-- guard a single TRUNCATE erases the entire ledger with the trigger above still
-- sitting there looking effective.
CREATE TRIGGER journal_entries_no_truncate
    BEFORE TRUNCATE ON journal_entries
    FOR EACH STATEMENT EXECUTE FUNCTION ledger_reject_journal_mutation();


-- ---------------------------------------------------------------------------
-- Invariant 1: every transaction balances, per currency.
--
-- WHY THIS IS DEFERRED AND NOT AN ORDINARY TRIGGER
--
-- Entries are inserted one row at a time. After the first INSERT -- a
-- 1000-paise DEBIT whose matching CREDIT has not been written yet -- the
-- transaction is unbalanced, and it has to be: there is no way to write both
-- legs in a single statement of record. An ordinary AFTER ... FOR EACH ROW
-- trigger fires at statement time and would reject that first row every single
-- time, making the invariant not merely awkward to satisfy but unenforceable.
--
-- The invariant is not a property of a row, nor of a statement. It is a
-- property of the database transaction, so it is checked at the only instant
-- where it is meaningful: COMMIT. DEFERRABLE INITIALLY DEFERRED moves it there
-- -- every leg is visible, the aggregate means something, and an unbalanced
-- write fails the COMMIT itself instead of landing quietly. Only CONSTRAINT
-- triggers can be deferred, which is why this is one.
--
-- KNOWN COST: PostgreSQL does not allow deferred statement-level triggers, so a
-- constraint trigger is necessarily FOR EACH ROW, and a two-leg transaction
-- therefore runs this check twice at commit. The aggregate is an index-only
-- scan over a handful of rows on journal_entries_transaction_id_entry_seq_key,
-- so the duplicated work is real but small. Do NOT "optimise" this into a
-- non-deferred trigger -- see above for why that cannot work at all.
--
-- FREE CONSEQUENCE: because amount_minor > 0, a single entry can never sum to
-- zero, so this also enforces "at least two legs per currency" without needing
-- a separate constraint.
-- ---------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION ledger_assert_transaction_balanced() RETURNS trigger
LANGUAGE plpgsql AS $$
DECLARE
    offending RECORD;
BEGIN
    SELECT je.transaction_id,
           je.currency,
           SUM(CASE WHEN je.direction = 'DEBIT'
                    THEN je.amount_minor
                    ELSE -je.amount_minor END) AS imbalance_minor
      INTO offending
      FROM journal_entries je
     WHERE je.transaction_id = NEW.transaction_id
     GROUP BY je.transaction_id, je.currency
    HAVING SUM(CASE WHEN je.direction = 'DEBIT'
                    THEN je.amount_minor
                    ELSE -je.amount_minor END) <> 0
     LIMIT 1;

    IF FOUND THEN
        RAISE EXCEPTION
            'transaction % does not balance in %: signed sum is % minor units, expected 0',
            offending.transaction_id, offending.currency, offending.imbalance_minor
            USING ERRCODE = 'check_violation';
    END IF;

    RETURN NULL;
END;
$$;

-- INSERT only: UPDATE and DELETE are already impossible (see above), so there
-- is no other way to unbalance a transaction once it has committed.
CREATE CONSTRAINT TRIGGER journal_entries_balanced
    AFTER INSERT ON journal_entries
    DEFERRABLE INITIALLY DEFERRED
    FOR EACH ROW EXECUTE FUNCTION ledger_assert_transaction_balanced();
