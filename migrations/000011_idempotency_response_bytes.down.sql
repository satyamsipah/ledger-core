-- Reversing this reintroduces the normalisation described in the up migration:
-- any response already stored will come back with its keys reordered, so
-- replays stop being byte-exact. That is inherent to JSONB rather than a flaw
-- in this statement, and it is the reason the forward migration exists.
--
-- The cast will fail outright on any stored body that is not valid JSON. That
-- is the correct behaviour for a down migration -- refusing loudly beats
-- silently discarding a stored response that a client may still retry for --
-- and today nothing writes a non-JSON body, so it cannot trigger.

ALTER TABLE idempotency_keys
    ALTER COLUMN response_body TYPE JSONB
    USING convert_from(response_body, 'UTF8')::jsonb;
