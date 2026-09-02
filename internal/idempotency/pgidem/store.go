// Package pgidem implements the idempotency store against PostgreSQL.
//
// Every statement touching idempotency_keys lives here, including Complete,
// which runs inside somebody else's transaction. Gathering them in one place is
// deliberate: the guarantee this table provides is a property of *which
// transaction* each statement runs in, and that is far easier to audit when the
// statements sit next to each other than when the completion is buried in the
// ledger repository beside the journal inserts.
package pgidem

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/satyamsipah/ledger-core/internal/idempotency"
)

// Compile-time proof that this package satisfies the port it claims to.
var _ idempotency.Store = (*Store)(nil)

// Store is the PostgreSQL-backed idempotency store.
type Store struct {
	pool    *pgxpool.Pool
	timeout time.Duration
}

// New builds a store over an existing pool.
//
// The timeout is per statement here rather than per unit of work, unlike the
// ledger repository: none of these statements holds a lock anybody waits on for
// longer than its own duration, so there is no compound operation to bound.
func New(pool *pgxpool.Pool, timeout time.Duration) *Store {
	return &Store{pool: pool, timeout: timeout}
}

// Reserve inserts the IN_PROGRESS row and reports whether this caller won.
//
// ON CONFLICT DO NOTHING rather than DO UPDATE. DO UPDATE would let the loser
// read the winner's row in the same round trip -- it blocks on the conflicting
// insert and then returns the committed version -- which looks like the tidier
// implementation. It was rejected because it writes a dead tuple for every
// duplicate request: a hundred retries of one key would produce a hundred row
// versions of a row nobody changed, and the duplicate path is exactly the path
// that gets hammered during an incident. The loser reads the row in a second
// statement instead, and the single-fsync window where that read comes back
// empty is answered honestly with a 409.
//
// This runs in its own implicit transaction and commits before any ledger work
// begins. That is the one place this design deviates from "the idempotency row
// is written in the same transaction as the journal entries", and it is what
// makes IN_PROGRESS observable at all -- a row written in the ledger's own
// transaction is invisible until it commits, by which time its status is
// already COMPLETED. What stays in the ledger transaction is the part that
// carries consequence: see Complete.
func (s *Store) Reserve(ctx context.Context, r idempotency.Reservation) (bool, error) {
	ctx, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()

	tag, err := s.pool.Exec(ctx, `
		INSERT INTO idempotency_keys
		    (principal_id, key, request_fingerprint, status, request_method,
		     request_route, expires_at, lease_expires_at)
		VALUES ($1, $2, $3, 'IN_PROGRESS', $4, $5,
		        now() + make_interval(secs => $6),
		        now() + make_interval(secs => $7))
		ON CONFLICT (principal_id, key) DO NOTHING`,
		r.PrincipalID, r.Key, r.Fingerprint.Bytes(), r.Method, r.Route,
		r.TTL.Seconds(), r.Lease.Seconds())
	if err != nil {
		return false, fmt.Errorf("insert idempotency key %s: %w", r.Key, err)
	}

	return tag.RowsAffected() == 1, nil
}

// Lookup reads a record, expired or not.
//
// The expires_at filter belongs to the caller, not here: "no such key" and "that
// key's response has aged out" lead to opposite outcomes -- executing afresh
// against refusing to -- and a query that folded them together would make the
// difference unrecoverable.
func (s *Store) Lookup(ctx context.Context, principalID, key string) (*idempotency.Record, error) {
	ctx, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()

	var (
		record         idempotency.Record
		fingerprint    []byte
		responseStatus *int32
		responseBody   []byte
		method         *string
		route          *string
	)
	err := s.pool.QueryRow(ctx, `
		SELECT principal_id, key, request_fingerprint, status, response_status,
		       response_body, transaction_id, request_method, request_route,
		       created_at, expires_at, lease_expires_at
		  FROM idempotency_keys
		 WHERE principal_id = $1 AND key = $2`, principalID, key).
		Scan(&record.PrincipalID, &record.Key, &fingerprint, &record.Status, &responseStatus, &responseBody,
			&record.TransactionID, &method, &route,
			&record.CreatedAt, &record.ExpiresAt, &record.LeaseExpiresAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("read idempotency key %s: %w", key, err)
	}

	record.Fingerprint, err = idempotency.FingerprintFromBytes(fingerprint)
	if err != nil {
		return nil, fmt.Errorf("idempotency key %s: %w", key, err)
	}
	if responseStatus != nil {
		record.ResponseStatus = int(*responseStatus)
	}
	record.ResponseBody = responseBody
	if method != nil {
		record.Method = *method
	}
	if route != nil {
		record.Route = *route
	}

	return &record, nil
}

// Reclaim extends a dead lease, reporting whether this caller won it.
//
// Three guards, and each rules out a different way of being wrong. `status =
// 'IN_PROGRESS'` is the one that matters most: it is proof that the previous
// holder never committed, because a commit would have moved the row to
// COMPLETED in the same transaction as the journal entries. `lease_expires_at
// <= now()` is what makes this a takeover rather than a theft. And under READ
// COMMITTED a second reclaimer blocked on the row re-evaluates its WHERE clause
// against the winner's committed version, finds a lease that is now in the
// future, and matches nothing -- the same mechanism that stops a double
// reversal in MarkReversed.
//
// LEAST(..., expires_at) keeps idempotency_keys_lease_within_ttl_check
// satisfied: a record two seconds from aging out cannot be handed a
// thirty-second lease.
func (s *Store) Reclaim(ctx context.Context, principalID, key string, lease time.Duration) (bool, error) {
	ctx, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()

	tag, err := s.pool.Exec(ctx, `
		UPDATE idempotency_keys
		   SET lease_expires_at = LEAST(now() + make_interval(secs => $3), expires_at)
		 WHERE principal_id = $1 AND key = $2
		   AND status = 'IN_PROGRESS'
		   AND lease_expires_at <= now()`, principalID, key, lease.Seconds())
	if err != nil {
		return false, fmt.Errorf("reclaim idempotency key %s: %w", key, err)
	}

	return tag.RowsAffected() == 1, nil
}

// Fail records a deterministic rejection.
//
// Its own transaction, and correctly so: the ledger transaction has already
// rolled back by the time this runs, so there is nothing left to be atomic
// with. What is being made durable is the rejection itself, which describes work
// that deliberately did not happen.
func (s *Store) Fail(ctx context.Context, principalID, key string, responseStatus int, responseBody []byte) error {
	ctx, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()

	tag, err := s.pool.Exec(ctx, `
		UPDATE idempotency_keys
		   SET status = 'FAILED', response_status = $3, response_body = $4
		 WHERE principal_id = $1 AND key = $2 AND status = 'IN_PROGRESS'`,
		principalID, key, responseStatus, responseBody)
	if err != nil {
		return fmt.Errorf("mark idempotency key %s failed: %w", key, err)
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("mark idempotency key %s failed: %w", key, idempotency.ErrLeaseLost)
	}
	return nil
}

// Release deletes this request's reservation.
//
// The `status = 'IN_PROGRESS'` guard is the entire safety of this statement, and
// it is worth being explicit about the case it catches. If the ledger
// transaction committed and the process then failed on its way to writing the
// response, the row is already COMPLETED -- and an unguarded DELETE here would
// erase the record of a transaction that really did post, freeing the key to
// post a second one. The guard turns that from a double spend into a no-op.
func (s *Store) Release(ctx context.Context, principalID, key string) error {
	ctx, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()

	_, err := s.pool.Exec(ctx, `
		DELETE FROM idempotency_keys
		 WHERE principal_id = $1 AND key = $2 AND status = 'IN_PROGRESS'`, principalID, key)
	if err != nil {
		return fmt.Errorf("release idempotency key %s: %w", key, err)
	}
	return nil
}

// Sweep deletes a batch of expired records, returning how many it removed.
//
// Batched through a subquery rather than issued as one unbounded DELETE. A
// single statement clearing a day's keys would hold row locks and bloat the
// table for as long as it ran, and would roll the whole thing back if it hit
// the statement timeout -- so the table would grow forever while the sweeper
// looked like it was working. FOR UPDATE SKIP LOCKED lets two replicas sweep
// concurrently without either waiting on the other, which matters during a
// rolling deploy when both the old and new pods are running.
func (s *Store) Sweep(ctx context.Context, batch int) (int64, error) {
	ctx, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()

	tag, err := s.pool.Exec(ctx, `
		DELETE FROM idempotency_keys
		 WHERE key IN (
		     SELECT key
		       FROM idempotency_keys
		      WHERE expires_at < now()
		      ORDER BY expires_at
		      LIMIT $1
		        FOR UPDATE SKIP LOCKED
		 )`, batch)
	if err != nil {
		return 0, fmt.Errorf("sweep expired idempotency keys: %w", err)
	}
	return tag.RowsAffected(), nil
}

// Complete moves a key to COMPLETED inside the caller's transaction.
//
// THIS IS THE WHOLE MECHANISM. It takes a pgx.Tx rather than a pool for the same
// reason outbox.Append does: the guarantee is not that this row gets written,
// it is that this row gets written *with* the journal entries it describes. A
// signature accepting a pool would make the broken version expressible, so it
// does not accept one.
//
// What a separate transaction would cost, concretely: the ledger transaction
// commits, the process dies before the completion commits, and the key is left
// IN_PROGRESS over a transaction that really posted. The retry finds a stale
// lease, correctly concludes that a stale lease means no commit -- and is wrong
// for the first time, because that inference is only sound while COMPLETED and
// the journal are atomic. It posts the money a second time. A Redis-only record
// fails the same way and faster, since a cache eviction is not even a crash.
//
// The `status = 'IN_PROGRESS'` guard handles the one race the lease permits: if
// this request's lease expired and another request reclaimed the key and
// finished first, this UPDATE matches nothing and returns ErrLeaseLost, which
// aborts the caller's transaction and takes these journal entries with it. The
// loser's work is discarded rather than committed alongside the winner's, which
// is what makes reclaiming a lease safe even when the original owner is still
// running.
func Complete(ctx context.Context, tx pgx.Tx, c idempotency.Completion) error {
	tag, err := tx.Exec(ctx, `
		UPDATE idempotency_keys
		   SET status          = 'COMPLETED',
		       response_status = $3,
		       response_body   = $4,
		       transaction_id  = $5
		 WHERE principal_id = $1 AND key = $2 AND status = 'IN_PROGRESS'`,
		c.PrincipalID, c.Key, c.ResponseStatus, c.ResponseBody, c.TransactionID)
	if err != nil {
		return fmt.Errorf("complete idempotency key %s: %w", c.Key, err)
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("complete idempotency key %s: %w", c.Key, idempotency.ErrLeaseLost)
	}
	return nil
}
