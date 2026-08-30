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
	ErrKeyExpired = errors.New("idempotency: key has expired")
)
