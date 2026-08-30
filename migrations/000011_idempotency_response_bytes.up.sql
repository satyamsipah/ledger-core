-- response_body becomes BYTEA, and the reason is a promise the API makes.
--
-- A replay returns the response that was stored when the transaction committed
-- -- byte for byte, not merely an equivalent document. JSONB cannot keep that
-- promise, because it is a PARSED representation rather than a stored one: it
-- reorders object keys into its own internal order, discards insignificant
-- whitespace, drops duplicate keys, and normalises numbers. Round-tripping a
-- response through it therefore returns a different sequence of bytes than the
-- one the first caller received. The document is semantically identical, which
-- is exactly why the bug is easy to miss and unpleasant to find: Content-Length
-- changes, any signature or ETag over the body changes, and two callers holding
-- what should be one answer can no longer compare them.
--
-- BYTEA is the honest type for an opaque payload this database never looks
-- inside. Nothing queries into response_body -- migration 000007 already
-- declined to index it -- so JSONB's one advantage was never claimed, while its
-- normalisation actively broke the guarantee.
--
-- Caught by TestAPI_ReplayReturnsTheStoredResponseByteForByte, which compares
-- the executed response with the replayed one rather than unmarshalling both
-- and comparing the objects. A test written the looser way would still pass
-- against JSONB, and the defect would have shipped.

ALTER TABLE idempotency_keys
    ALTER COLUMN response_body TYPE BYTEA
    USING convert_to(response_body::text, 'UTF8');
