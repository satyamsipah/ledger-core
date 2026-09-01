package idempotency

import (
	"fmt"
	"time"

	"github.com/google/uuid"
)

// Status is a key's position in the state machine documented on Store.
type Status string

// The three persisted states, matching idempotency_keys_status_check in
// migration 000007. ABSENT and EXPIRED are also states of the machine, but both
// are the absence of a row rather than a value of this column.
const (
	// StatusInProgress means a request holds the key and has not committed.
	//
	// Load-bearing property: this value is PROOF that no transaction committed
	// under the key, because the move to COMPLETED happens in the same database
	// transaction as the journal entries. Every reclaim decision rests on it.
	StatusInProgress Status = "IN_PROGRESS"

	// StatusCompleted means the work committed and the response is replayable.
	StatusCompleted Status = "COMPLETED"

	// StatusFailed means the request was rejected for a reason that retrying
	// cannot change -- a validation error, an unbalanced transaction -- so the
	// rejection itself is cached and replayed. Rejections that a retry COULD
	// resolve, such as insufficient funds, release the key instead of landing
	// here; see docs/DECISIONS.md D21.
	StatusFailed Status = "FAILED"
)

// Terminal reports whether the status is final, and therefore whether the
// record may be cached. Only terminal records are safe in a cache: they never
// change again, so a stale read is impossible by construction. An IN_PROGRESS
// record cached for even a second would 409 a request that should have
// replayed.
func (s Status) Terminal() bool {
	return s == StatusCompleted || s == StatusFailed
}

// Record is one row of idempotency_keys.
type Record struct {
	Key            string
	Fingerprint    Fingerprint
	Status         Status
	ResponseStatus int
	ResponseBody   []byte
	TransactionID  *uuid.UUID
	Method         string
	Route          string
	CreatedAt      time.Time
	ExpiresAt      time.Time
	LeaseExpiresAt time.Time
}

// Reservation is the request to take a key, as issued before any work starts.
type Reservation struct {
	Key         string
	Fingerprint Fingerprint
	Method      string
	Route       string

	// TTL is how long the replay record survives; Lease is how long this
	// request may hold the key before another may reclaim it. See migration
	// 000010 for why the two are separate.
	TTL   time.Duration
	Lease time.Duration
}

// Completion is the terminal state written from inside the caller's database
// transaction.
//
// It carries the rendered response rather than a reference to it, because the
// response has to be durable at the same instant the journal entries are. A
// completion that stored only the transaction id and re-rendered on replay
// would be smaller and would be wrong the day the rendering changes: a client
// retrying across a deploy would get a different body for the same key, which
// is the one thing an idempotent endpoint promises not to do.
type Completion struct {
	Key            string
	ResponseStatus int
	ResponseBody   []byte
	TransactionID  uuid.UUID
}

// Disposition is what the caller should do with a key it just tried to acquire.
type Disposition int

const (
	// Proceed means the key is held by this request: execute the work.
	Proceed Disposition = iota

	// Replay means a terminal record already exists: return it verbatim, with
	// the Idempotent-Replay header set.
	Replay
)

// InProgressError reports that the key is held by a live lease, and carries how
// long the caller should wait before retrying.
//
// A struct rather than a bare sentinel because Retry-After needs a real number:
// telling every caller to retry after a fixed interval either stalls the fast
// case or stampedes the slow one. errors.Is still matches ErrRequestInProgress
// through Unwrap, so call sites branch on the sentinel as they do everywhere
// else in this codebase.
type InProgressError struct {
	Key        string
	RetryAfter time.Duration
}

func (e *InProgressError) Error() string {
	return fmt.Sprintf("idempotency: key %s is in progress, retry after %s", e.Key, e.RetryAfter)
}

func (e *InProgressError) Unwrap() error { return ErrRequestInProgress }

// ConflictError reports that the key exists with a different fingerprint, and
// names the endpoint it was originally used against.
//
// The endpoint is included because the commonest cause of this error is a
// client reusing one key across two routes, and "you used this key on
// POST /v1/transactions" turns a puzzling 422 into an obvious one.
type ConflictError struct {
	Key    string
	Method string
	Route  string
}

func (e *ConflictError) Error() string {
	return fmt.Sprintf("idempotency: key %s was already used for %s %s with a different body",
		e.Key, e.Method, e.Route)
}

func (e *ConflictError) Unwrap() error { return ErrIdempotencyConflict }

// validate checks a record read back from the database against the guarantees
// the schema is supposed to provide.
//
// It duplicates idempotency_keys_completed_check on purpose. That constraint is
// what makes the property true; this is what turns a violation into
// ErrCorruptRecord at the point of use rather than a nil-pointer dereference
// three frames later in the replay path.
func (r *Record) validate() error {
	if r.Status.Terminal() && (r.ResponseStatus == 0 || len(r.ResponseBody) == 0) {
		return fmt.Errorf("key %s is %s with no stored response: %w", r.Key, r.Status, ErrCorruptRecord)
	}
	return nil
}
