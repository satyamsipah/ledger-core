-- Reverses 000017_reconciliation.up.sql. Exceptions first: they reference
-- runs, and dropping the referenced table first would fail the constraint.
DROP TABLE IF EXISTS reconciliation_exceptions;
DROP TABLE IF EXISTS reconciliation_runs;
