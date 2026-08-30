package idempotency

import "errors"

var (
	// ErrIdempotencyConflict means the key was already used with a different
	// request body. This is a client bug -- reusing a key for different content
	// -- and must be a 422, never a silent replay of the earlier response.
	ErrIdempotencyConflict = errors.New("idempotency: key reused with a different request body")

	// ErrRequestInProgress means another request holds this key and has not
	// finished. Callers should return 409 with Retry-After rather than block:
	// holding an HTTP connection open behind an in-flight write is how one slow
	// transaction becomes an exhausted connection pool.
	ErrRequestInProgress = errors.New("idempotency: a request with this key is already in progress")

	// ErrKeyExpired means the key exists but is past its TTL, so the original
	// response is no longer available to replay.
	//
	// This is deliberately NOT the same as "the key is free again". The replay
	// record expires after 24 hours; the key itself stays reserved forever by
	// transactions_idempotency_key_key, so a retry arriving after the TTL is
	// refused rather than executed a second time. The TTL bounds how much this
	// table stores, never whether the ledger can be double-posted.
	ErrKeyExpired = errors.New("idempotency: key has expired")

	// ErrMissingKey means the Idempotency-Key header was absent on a route that
	// requires one. Required rather than optional on every write path: an
	// optional idempotency key is one a client forgets under exactly the
	// conditions -- timeouts, retries, partial outages -- that make it matter.
	ErrMissingKey = errors.New("idempotency: the Idempotency-Key header is required")

	// ErrInvalidKey means the header was present but is not a UUID.
	ErrInvalidKey = errors.New("idempotency: the Idempotency-Key header must be a UUID")

	// ErrMalformedBody means the request body is not a single well-formed JSON
	// document, or contains duplicate object keys. Duplicates are rejected
	// rather than resolved because parsers disagree about which one wins, so
	// such a document has no single meaning to fingerprint.
	ErrMalformedBody = errors.New("idempotency: request body is not canonicalizable JSON")

	// ErrLeaseLost means the completing UPDATE matched no IN_PROGRESS row: while
	// this request was working, its lease expired and another request reclaimed
	// the key and finished first.
	//
	// This must abort the caller's database transaction, and it does -- it is
	// returned from inside the ledger transaction, so the journal entries roll
	// back with it. That is the point. Two executions racing under one key is
	// survivable precisely because the loser's work is discarded rather than
	// committed alongside the winner's.
	ErrLeaseLost = errors.New("idempotency: lease was reclaimed by another request")

	// ErrCorruptRecord means a stored record cannot be interpreted -- a
	// fingerprint of the wrong length, or a COMPLETED row with no response.
	// The schema forbids both, so reaching this means something wrote to the
	// table without going through this package.
	ErrCorruptRecord = errors.New("idempotency: stored record is not interpretable")
)
