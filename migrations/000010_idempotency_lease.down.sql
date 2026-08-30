-- Reversing the lease columns is a genuine loss of in-flight state: any key
-- currently IN_PROGRESS loses the deadline that says when it may be reclaimed.
-- That is acceptable only because the rows themselves survive, and a row in
-- IN_PROGRESS still proves no transaction committed under it -- so the worst
-- outcome of rolling back mid-flight is a key that stays blocked until its
-- 24-hour expires_at reaps it, rather than one that permits a double post.

ALTER TABLE idempotency_keys RESET (fillfactor);

ALTER TABLE idempotency_keys DROP COLUMN request_route;
ALTER TABLE idempotency_keys DROP COLUMN request_method;

ALTER TABLE idempotency_keys
    DROP CONSTRAINT idempotency_keys_lease_within_ttl_check;

ALTER TABLE idempotency_keys DROP COLUMN lease_expires_at;
