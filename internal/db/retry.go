package db

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"time"

	"github.com/jackc/pgerrcode"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/satyamsipah/ledger-core/internal/observability"
)

// Retry defaults. Five attempts is the cap CLAUDE.md asks for; the backoff
// window is deliberately short because the thing being waited out is another
// transaction finishing, which takes milliseconds, not a remote service
// recovering, which takes seconds.
const (
	DefaultMaxAttempts = 5
	DefaultBaseBackoff = 5 * time.Millisecond
	DefaultMaxBackoff  = 250 * time.Millisecond
)

// Retrier re-runs a database transaction that PostgreSQL aborted for a reason
// that retrying can actually fix.
//
// # THE ONLY TWO ERRORS THIS RETRIES, AND WHY THE LIST IS SO SHORT
//
// 40001 (serialization_failure) and 40P01 (deadlock_detected) share one
// property that nothing else on the error list has: PostgreSQL is telling us,
// definitively, that the transaction was rolled back. Nothing committed. There
// is no ambiguity to resolve, so running it again is not merely safe -- it is
// the only correct response, since the work was thrown away through no fault of
// the request.
//
// Everything else is excluded, and the exclusions are the important part:
//
//   - context.DeadlineExceeded, connection resets, and any error surfacing from
//     COMMIT itself are AMBIGUOUS. The transaction may have committed on the
//     server while the answer was lost on the way back. Retrying one of those in
//     a ledger is how money moves twice, and it is exactly the class of bug that
//     the idempotency key exists to catch when it happens at a higher layer.
//     This retrier must not create it at a lower one.
//   - Domain errors -- ErrInsufficientFunds, ErrIdempotencyConflict,
//     ErrBalanceVersionConflict -- are deterministic. Retrying cannot change
//     the answer, only the number of times the caller waits for it.
//     ErrBalanceVersionConflict is the sharpest case: its doc comment says a
//     retry would re-read the state a lock was supposed to have protected.
//
// The classification is therefore made on SQLSTATE alone, never on "an error
// occurred", and the switch below is the whole of it.
//
// # WHY THIS EXISTS AT ALL, GIVEN D10
//
// D10 records that the posting path has no retry loop, because READ COMMITTED
// plus ordered row locks converts contention into queueing rather than into
// aborts. That reasoning still holds, and this does not weaken it. Deadlocks
// remain constructible for reasons outside the ordering the ledger controls --
// a future write path taking locks in another order, the optional advisory
// locks, index or foreign-key level cycles -- and the honest position is that
// "cannot happen" is a claim worth measuring rather than asserting. The
// ledger_db_tx_retries_total{sqlstate="40P01"} series is that measurement, and
// its staying at zero is a continuous proof of D11 that no single test can give.
type Retrier struct {
	logger      *slog.Logger
	metrics     *observability.Metrics
	maxAttempts int
	baseBackoff time.Duration
	maxBackoff  time.Duration
}

// NewRetrier builds a retrier. Zero values fall back to the defaults above, so
// a caller that only wants metrics does not have to restate the timing.
func NewRetrier(logger *slog.Logger, metrics *observability.Metrics, maxAttempts int, base, max time.Duration) *Retrier {
	if maxAttempts <= 0 {
		maxAttempts = DefaultMaxAttempts
	}
	if base <= 0 {
		base = DefaultBaseBackoff
	}
	if max <= 0 {
		max = DefaultMaxBackoff
	}
	return &Retrier{
		logger:      logger,
		metrics:     metrics,
		maxAttempts: maxAttempts,
		baseBackoff: base,
		maxBackoff:  max,
	}
}

// Do runs fn, retrying it while PostgreSQL reports a retryable abort.
//
// fn must be safe to run from the top more than once, which for this codebase
// means it must not depend on state mutated by a previous attempt. It is, since
// an aborted transaction leaves nothing behind -- but note that any identifier
// the caller wants stable across attempts has to be generated OUTSIDE this
// call. PostTransaction does exactly that with its transaction id, so a leaked
// partial commit would collide on the primary key rather than post twice.
//
// The parent context bounds the whole sequence, not each attempt. A caller that
// gives this a three-second budget gets three seconds in total, so a retried
// transaction cannot quietly consume five times the deadline the HTTP layer
// thinks it granted.
func (r *Retrier) Do(ctx context.Context, operation string, fn func(context.Context) error) error {
	var lastErr error

	for attempt := 1; attempt <= r.maxAttempts; attempt++ {
		err := fn(ctx)
		if err == nil {
			r.observeAttempts(operation, attempt)
			return nil
		}

		sqlstate, retryable := retryableSQLState(err)
		if !retryable {
			// Not a retry, so not counted as one. Recording attempts here would
			// make a wall of insufficient-funds rejections look like contention.
			return err
		}

		lastErr = err
		if r.metrics != nil {
			r.metrics.TxRetries.WithLabelValues(operation, sqlstate).Inc()
		}

		// Checked before sleeping rather than after: if the budget is already
		// spent there is no point waiting to discover it, and the caller gets
		// the database's own error instead of a context deadline that hides it.
		//
		// Both errors are wrapped, not just the database one. A caller has to be
		// able to tell "the budget ran out mid-contention" from "this kept
		// aborting through every attempt": the first is a capacity problem and
		// maps to a 503, the second is a correctness problem and maps to a 500.
		// Reporting only the SQLSTATE would leave the HTTP layer guessing from
		// an error string.
		if ctx.Err() != nil {
			return fmt.Errorf("%s aborted with %s and the deadline expired before retrying: %w: %w",
				operation, sqlstate, ctx.Err(), lastErr)
		}
		if attempt == r.maxAttempts {
			break
		}

		backoff := r.backoff(attempt)
		if r.logger != nil {
			r.logger.WarnContext(ctx, "retrying aborted database transaction",
				slog.String("operation", operation),
				slog.String("sqlstate", sqlstate),
				slog.Int("attempt", attempt),
				slog.Duration("backoff", backoff))
		}

		timer := time.NewTimer(backoff)
		select {
		case <-ctx.Done():
			timer.Stop()
			return fmt.Errorf("%s aborted with %s and the deadline expired while backing off: %w: %w",
				operation, sqlstate, ctx.Err(), lastErr)
		case <-timer.C:
		}
	}

	r.observeAttempts(operation, r.maxAttempts)
	return fmt.Errorf("%s still aborting after %d attempts: %w", operation, r.maxAttempts, lastErr)
}

// backoff returns a FULL JITTER delay: uniformly random in [0, window), where
// the window doubles per attempt up to the cap.
//
// Full jitter rather than exponential-with-a-little-jitter, and the difference
// matters most in the case this exists for. When N writers deadlock on a hot
// account, a deterministic backoff has them all wake at the same instant and
// collide again -- the retries reproduce the contention that caused them, which
// is a thundering herd the system built for itself. Spreading each retry across
// the whole window instead makes the second collision far less likely than the
// first, so the queue drains rather than oscillating.
//
// The floor is deliberately zero. A minimum delay would resynchronise exactly
// the writers that jitter is meant to separate.
func (r *Retrier) backoff(attempt int) time.Duration {
	window := r.baseBackoff << (attempt - 1)
	if window > r.maxBackoff || window <= 0 { // <= 0 catches the shift overflowing
		window = r.maxBackoff
	}
	// math/rand rather than crypto/rand, deliberately. This value decides when
	// to retry, not what a secret is: an attacker who predicts it learns the
	// millisecond a transaction will be retried at, which is worth nothing.
	// crypto/rand would cost a syscall per retry on the contention path and can
	// fail, which would turn a backoff into an error.
	//nolint:gosec // G404: jitter is not a security primitive; see above.
	return time.Duration(rand.Int64N(int64(window) + 1))
}

func (r *Retrier) observeAttempts(operation string, attempts int) {
	if r.metrics != nil {
		r.metrics.TxAttempts.WithLabelValues(operation).Observe(float64(attempts))
	}
}

// retryableSQLState reports whether err is one of the two aborts PostgreSQL
// guarantees rolled back, returning the SQLSTATE for labelling.
//
// errors.As rather than a type assertion, so an error wrapped on its way up
// through the repository is still recognised. That matters: every statement in
// pgledger wraps its error with context before returning it.
func retryableSQLState(err error) (string, bool) {
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) {
		return "", false
	}

	switch pgErr.Code {
	case pgerrcode.SerializationFailure, pgerrcode.DeadlockDetected:
		return pgErr.Code, true
	default:
		return "", false
	}
}
