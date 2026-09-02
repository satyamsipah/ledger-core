package idempotency

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/satyamsipah/ledger-core/internal/observability"
)

// Default lifetimes. The TTL is the 24 hours CLAUDE.md asks for; the lease is
// sized to comfortably exceed a posting transaction's budget
// (LEDGER_POSTGRES_QUERY_TIMEOUT, 3s by default) so that a live request is
// never reclaimed out from under itself in normal operation.
const (
	DefaultTTL   = 24 * time.Hour
	DefaultLease = 30 * time.Second
)

// Store is the persistence port for idempotency records.
//
// Every method here is a state transition in the machine below, and the machine
// has exactly one non-obvious property: IN_PROGRESS means no transaction
// committed. That holds because Complete (see the ledger's Tx port) runs in the
// same database transaction as the journal entries, so COMPLETED and the money
// become durable together or not at all.
//
//	ABSENT ──Reserve──▶ IN_PROGRESS ──(ledger txn)──▶ COMPLETED ──sweep──▶ ABSENT
//	   ▲                  │  │  │                          ▲
//	   │                  │  │  └──Fail──▶ FAILED ─────────┘
//	   └────Release───────┘  └──Reclaim──▶ IN_PROGRESS (new lease)
//
// Reserve and Reclaim commit on their own. Everything else about the design
// exists to make that safe: because a reservation carries no consequence beyond
// "somebody is trying", losing one, duplicating one, or abandoning one costs
// availability and never correctness.
type Store interface {
	// Reserve attempts to insert an IN_PROGRESS row, returning whether this
	// caller won the key. It commits immediately and on its own.
	Reserve(ctx context.Context, r Reservation) (won bool, err error)

	// Lookup reads a record by (principal, key), INCLUDING one past its
	// expires_at. The caller decides what expiry means; a store that filtered
	// expired rows out would make "expired" and "absent" indistinguishable,
	// and those two demand different answers -- 409 against executing afresh.
	//
	// Scoped by principal, not merely by key: a lookup under one principal for
	// another's key must return exactly what an absent key returns -- nil,
	// nothing -- so that a cross-tenant probe cannot even observe that a key
	// exists. See docs/DECISIONS.md D24.
	Lookup(ctx context.Context, principalID, key string) (*Record, error)

	// Reclaim takes over a lease that has run out, returning whether this
	// caller won it. Guarded so that only one of several simultaneous
	// reclaimers proceeds.
	Reclaim(ctx context.Context, principalID, key string, lease time.Duration) (won bool, err error)

	// Fail records a deterministic rejection so it can be replayed.
	Fail(ctx context.Context, principalID, key string, responseStatus int, responseBody []byte) error

	// Release deletes this request's reservation, guarded on IN_PROGRESS so it
	// can never remove a completed record.
	Release(ctx context.Context, principalID, key string) error

	// Sweep deletes up to batch expired records, returning how many went.
	// Unscoped by principal on purpose: expiry is a property of time, not of
	// who owns the record, so one sweep clears every principal's stale keys.
	Sweep(ctx context.Context, batch int) (int64, error)
}

// Cache is the read-through fast path in front of Store.
//
// It may only ever hold TERMINAL records. That is not a performance guideline,
// it is what makes the cache unable to cause a wrong answer: a COMPLETED or
// FAILED record never changes again, so a stale read of one is indistinguishable
// from a fresh read. Caching IN_PROGRESS would let a request that has since
// finished keep answering 409 until the entry aged out.
//
// Every method is best-effort by contract. An implementation that fails, times
// out, or is simply absent must not change any outcome -- Postgres is consulted
// on every miss and is the only source of truth. NoopCache is the default for
// exactly that reason.
type Cache interface {
	// Get returns a cached terminal record, or false on any miss, error or
	// timeout. It does not return an error: there is no cache failure a caller
	// could usefully act on, since the answer is always "ask Postgres". key is
	// always produced by cacheKey, composing (principal, key) into one string.
	Get(ctx context.Context, key string) (*Record, bool)

	// Put stores a terminal record under key (see cacheKey). Callers ignore
	// failures.
	Put(ctx context.Context, key string, record *Record, ttl time.Duration)
}

// NoopCache is a Cache that stores nothing.
//
// It is the default, and the service is correct with it in place -- which is
// the property that makes Redis optional rather than load-bearing. Wiring the
// real client in is a constructor change and nothing else, and the cache
// hit-rate counter is what will say whether that dependency has earned itself.
// See docs/DECISIONS.md D22.
type NoopCache struct{}

// Get always misses.
func (NoopCache) Get(context.Context, string) (*Record, bool) { return nil, false }

// Put discards the record.
func (NoopCache) Put(context.Context, string, *Record, time.Duration) {}

// Manager runs the idempotency state machine over a Store and a Cache.
//
// It holds no per-key state. Two replicas racing on one key are resolved by the
// primary key of idempotency_keys, not by anything this struct could coordinate,
// which is what lets the service scale horizontally without a lock service.
type Manager struct {
	store   Store
	cache   Cache
	metrics *observability.Metrics
	ttl     time.Duration
	lease   time.Duration
}

// NewManager wires a manager to its store. Passing NoopCache{} is a fully
// supported configuration, not a degraded one.
func NewManager(store Store, cache Cache, metrics *observability.Metrics, ttl, lease time.Duration) *Manager {
	if cache == nil {
		cache = NoopCache{}
	}
	if ttl <= 0 {
		ttl = DefaultTTL
	}
	if lease <= 0 {
		lease = DefaultLease
	}
	return &Manager{store: store, cache: cache, metrics: metrics, ttl: ttl, lease: lease}
}

// Lease returns the configured lease duration, for callers sizing their own
// deadlines against it.
func (m *Manager) Lease() time.Duration { return m.lease }

// Acquire runs the state machine for one inbound request and says what to do.
//
// The four answers, in the order they are decided:
//
//   - Proceed: the key is ours. Execute, and complete the record inside the
//     same database transaction as the work.
//   - Replay + a record: a terminal result exists. Return it verbatim.
//   - ConflictError (422): the key exists with a different body. Checked FIRST,
//     before any state branch, because reusing a key for different content is a
//     client bug in every state and must never be answered with somebody else's
//     response.
//   - InProgressError (409): a live lease holds the key. The caller returns
//     Retry-After and does not block -- a queue of HTTP connections waiting on
//     an in-flight write is how one slow transaction exhausts the pool.
func (m *Manager) Acquire(ctx context.Context, r Reservation) (*Record, Disposition, error) {
	if r.TTL <= 0 {
		r.TTL = m.ttl
	}
	if r.Lease <= 0 {
		r.Lease = m.lease
	}

	// The cache is consulted before Postgres and can only ever hold terminal
	// records, so a hit is either a replay or a conflict -- never a decision
	// that depends on anything still moving.
	if cached, ok := m.cache.Get(ctx, cacheKey(r.PrincipalID, r.Key)); ok {
		m.count("cache_hit")
		return m.resolveExisting(ctx, cached, r)
	}
	m.count("cache_miss")

	won, err := m.store.Reserve(ctx, r)
	if err != nil {
		return nil, Proceed, fmt.Errorf("reserve idempotency key %s: %w", r.Key, err)
	}
	if won {
		m.count("acquired")
		return nil, Proceed, nil
	}

	existing, err := m.store.Lookup(ctx, r.PrincipalID, r.Key)
	if err != nil {
		return nil, Proceed, fmt.Errorf("look up idempotency key %s: %w", r.Key, err)
	}
	if existing == nil {
		// The insert conflicted, so a row certainly exists, yet the read cannot
		// see it: the winner's reservation is between its INSERT and its COMMIT.
		// That window is a single fsync wide. "A request with this key is
		// already in progress" is not a workaround here, it is the literal
		// truth, so answer it rather than spinning on a re-read.
		m.count("in_progress")
		return nil, Proceed, &InProgressError{Key: r.Key, RetryAfter: time.Second}
	}

	return m.resolveExisting(ctx, existing, r)
}

// resolveExisting decides what an already-present record means for this request.
func (m *Manager) resolveExisting(ctx context.Context, existing *Record, r Reservation) (*Record, Disposition, error) {
	if !existing.Fingerprint.Equal(r.Fingerprint) {
		m.count("conflict")
		return nil, Proceed, &ConflictError{
			Key:    r.Key,
			Method: existing.Method,
			Route:  existing.Route,
		}
	}

	if err := existing.validate(); err != nil {
		return nil, Proceed, err
	}

	now := time.Now()

	// Expiry is checked after the fingerprint and before the status, because an
	// expired record's stored response is gone even though its key is not: the
	// key stays reserved permanently by transactions_idempotency_key_key, so the
	// honest answer is "this happened, and I can no longer show you what it
	// returned" rather than executing it again.
	if !existing.ExpiresAt.After(now) {
		m.count("expired")
		return nil, Proceed, fmt.Errorf("key %s expired at %s: %w",
			existing.Key, existing.ExpiresAt, ErrKeyExpired)
	}

	if existing.Status.Terminal() {
		m.cache.Put(ctx, cacheKey(existing.PrincipalID, existing.Key), existing, time.Until(existing.ExpiresAt))
		m.count("replayed")
		return existing, Replay, nil
	}

	// IN_PROGRESS from here down.
	if existing.LeaseExpiresAt.After(now) {
		m.count("in_progress")
		return nil, Proceed, &InProgressError{
			Key:        existing.Key,
			RetryAfter: retryAfter(existing.LeaseExpiresAt, now),
		}
	}

	// The lease is dead, so its owner never committed -- IN_PROGRESS proves it.
	// Taking over is therefore safe without any coordination beyond the guarded
	// UPDATE, which is also what stops two simultaneous reclaimers both winning.
	won, err := m.store.Reclaim(ctx, existing.PrincipalID, existing.Key, r.Lease)
	if err != nil {
		return nil, Proceed, fmt.Errorf("reclaim idempotency key %s: %w", existing.Key, err)
	}
	if !won {
		m.count("in_progress")
		return nil, Proceed, &InProgressError{Key: existing.Key, RetryAfter: retryAfter(now.Add(r.Lease), now)}
	}

	m.count("reclaimed")
	return nil, Proceed, nil
}

// Fail caches a deterministic rejection so a retry gets the same answer without
// re-running work whose outcome cannot change.
func (m *Manager) Fail(ctx context.Context, principalID, key string, responseStatus int, responseBody []byte) error {
	if err := m.store.Fail(ctx, principalID, key, responseStatus, responseBody); err != nil {
		return fmt.Errorf("record failure for idempotency key %s: %w", key, err)
	}
	m.count("failed")
	return nil
}

// Release hands a key back after a rejection a retry could resolve.
//
// Best-effort by design: the guarded DELETE cannot remove a completed record,
// and a release that never runs -- because the process died, or Postgres was
// unreachable -- leaves a lease that expires on its own. Both failure modes cost
// a delay, neither costs correctness, which is why this returns an error for
// logging rather than for handling.
func (m *Manager) Release(ctx context.Context, principalID, key string) error {
	if err := m.store.Release(ctx, principalID, key); err != nil {
		return fmt.Errorf("release idempotency key %s: %w", key, err)
	}
	m.count("released")
	return nil
}

// CacheCompleted populates the read-through cache after the ledger transaction
// has committed.
//
// After, never before. The cache is allowed to lag Postgres; it is never allowed
// to lead it. An entry written before the commit would answer a replay for a
// transaction that then rolled back.
func (m *Manager) CacheCompleted(ctx context.Context, record *Record) {
	if record == nil || !record.Status.Terminal() {
		return
	}
	m.cache.Put(ctx, cacheKey(record.PrincipalID, record.Key), record, time.Until(record.ExpiresAt))
}

// cacheKey composes the cache's lookup string from a principal and a key.
//
// Unambiguous despite being plain concatenation: key is always a canonical
// UUID string, fixed at 36 characters (idempotency.ParseKey guarantees this),
// so two different (principal, key) pairs can never produce the same composed
// string -- the fixed-width suffix pins where the delimiter falls regardless
// of what characters principalID itself contains.
func cacheKey(principalID, key string) string {
	return principalID + ":" + key
}

func (m *Manager) count(outcome string) {
	if m.metrics == nil {
		return
	}
	m.metrics.IdempotencyOutcomes.WithLabelValues(outcome).Inc()
}

// retryAfter converts a lease deadline into the whole seconds Retry-After
// carries, never returning less than one: a Retry-After of 0 invites an
// immediate retry, which is how a 409 turns into a hot loop.
func retryAfter(deadline, now time.Time) time.Duration {
	remaining := deadline.Sub(now)
	if remaining < time.Second {
		return time.Second
	}
	return remaining.Round(time.Second)
}

// IsConflict reports whether err is a fingerprint conflict, for handlers
// mapping to 422.
func IsConflict(err error) bool { return errors.Is(err, ErrIdempotencyConflict) }

// IsInProgress reports whether err is a live-lease collision, for handlers
// mapping to 409.
func IsInProgress(err error) bool { return errors.Is(err, ErrRequestInProgress) }
