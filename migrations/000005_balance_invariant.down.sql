DROP TRIGGER IF EXISTS journal_entries_balanced ON journal_entries;
DROP TRIGGER IF EXISTS journal_entries_no_truncate ON journal_entries;
DROP TRIGGER IF EXISTS journal_entries_no_mutation ON journal_entries;

DROP FUNCTION IF EXISTS ledger_assert_transaction_balanced();
DROP FUNCTION IF EXISTS ledger_reject_journal_mutation();
