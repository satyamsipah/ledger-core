-- Phase 3 turns idempotency_keys from a schema into a live mechanism.
--
-- THE LEASE, AND WHY IT IS NOT expires_at
--
-- expires_at is the 24-hour TTL on the *replay record*: how long a client may
-- come back and be handed the original response. lease_expires_at is how long
-- one in-flight request may hold the key before another request is entitled to
-- conclude it died and take over. Seconds against a day. Conflating them means
-- choosing between a crashed request blocking its own retry for 24 hours, and a
-- replay record vanishing while the request that owns it is still running.
--
-- WHY RECLAIMING AN EXPIRED LEASE IS SAFE, stated where the column is defined
-- rather than only in Go: a row sitting in IN_PROGRESS is *proof* that no
-- transaction committed under this key, because the move to COMPLETED happens
-- in the same database transaction as the journal entries it describes. There
-- is therefore no state in which the money moved and the lease still looks
-- abandoned. That single fact is what lets a stale lease be reclaimed with no
-- fencing token, no lock service and no distributed coordination of any kind.
-- See docs/DECISIONS.md D20.

-- Added nullable, backfilled, then constrained, per .claude/rules/sql-migrations.md.
-- The table is empty today -- Phase 2 shipped the schema but no writer -- so the
-- backfill is a no-op and a single ADD COLUMN NOT NULL DEFAULT would have been
-- equivalent. The three-step form is kept because it is the shape that stays
-- correct when this runs against a table that is no longer empty, and a
-- migration that only works on an empty table teaches the wrong pattern to the
-- next person who copies it.
ALTER TABLE idempotency_keys ADD COLUMN lease_expires_at TIMESTAMPTZ;

UPDATE idempotency_keys
   SET lease_expires_at = created_at
 WHERE lease_expires_at IS NULL;

ALTER TABLE idempotency_keys ALTER COLUMN lease_expires_at SET NOT NULL;

-- The lease is a short sub-interval of the replay window, never longer than it.
-- A lease outliving expires_at would let the sweeper delete a record whose owner
-- still believes it holds the key.
ALTER TABLE idempotency_keys
    ADD CONSTRAINT idempotency_keys_lease_within_ttl_check
        CHECK (lease_expires_at <= expires_at);

-- Diagnostics, not correctness. The fingerprint already covers the method and
-- the route (see internal/idempotency/fingerprint.go), so reusing one key across
-- two endpoints is rejected whether or not these columns exist. They exist so
-- the 422 can say *which* endpoint the key belongs to, which is the difference
-- between a client fixing the bug and a client opening a ticket.
ALTER TABLE idempotency_keys ADD COLUMN request_method TEXT;
ALTER TABLE idempotency_keys ADD COLUMN request_route  TEXT;

-- Every duplicate request under an active lease reads this table, and every
-- completion updates a row in place without touching an indexed column -- key
-- and expires_at never change after insert. That makes the completion eligible
-- to be a heap-only tuple, which skips both index writes, but only if the page
-- has room. Same reasoning as account_balances in migration 000006.
ALTER TABLE idempotency_keys SET (fillfactor = 70);
