-- Shared trigger function for any table carrying an updated_at column.
--
-- It lives in its own migration so the tables that depend on it can be added
-- and rolled back independently: a down-migration for accounts must not take
-- the function out from under account_balances.
CREATE OR REPLACE FUNCTION set_updated_at() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    NEW.updated_at = now();
    RETURN NEW;
END;
$$;
