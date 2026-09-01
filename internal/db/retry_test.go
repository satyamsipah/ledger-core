package db

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/jackc/pgerrcode"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func testRetrier(maxAttempts int) *Retrier {
	return NewRetrier(
		slog.New(slog.NewTextHandler(io.Discard, nil)),
		nil,
		maxAttempts,
		time.Microsecond,
		time.Microsecond,
	)
}

func pgErr(code string) error {
	return &pgconn.PgError{Code: code, Message: "synthetic " + code}
}

// TestRetrier_RetriesOnlyDefiniteAborts is the whole safety argument for this
// package, stated as a table.
//
// 40001 and 40P01 are the only errors PostgreSQL guarantees rolled back
// nothing. Everything else is either deterministic -- so a retry only wastes
// the caller's time -- or AMBIGUOUS about whether the transaction committed,
// and retrying an ambiguous write in a ledger is how money moves twice.
func TestRetrier_RetriesOnlyDefiniteAborts(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		err          error
		wantAttempts int
	}{
		{
			name:         "should retry when the transaction hit a serialization failure",
			err:          pgErr(pgerrcode.SerializationFailure),
			wantAttempts: 5,
		},
		{
			name:         "should retry when the transaction was chosen as a deadlock victim",
			err:          pgErr(pgerrcode.DeadlockDetected),
			wantAttempts: 5,
		},
		{
			// A unique violation is a fact about the data, and the second
			// attempt would find the same row.
			name:         "should not retry when a unique constraint was violated",
			err:          pgErr(pgerrcode.UniqueViolation),
			wantAttempts: 1,
		},
		{
			name:         "should not retry when a check constraint was violated",
			err:          pgErr(pgerrcode.CheckViolation),
			wantAttempts: 1,
		},
		{
			// THE IMPORTANT ONE. A deadline that expired mid-COMMIT leaves us
			// unable to say whether the transaction committed. Retrying it is
			// the double-spend.
			name:         "should not retry when the deadline expired, because the outcome is ambiguous",
			err:          context.DeadlineExceeded,
			wantAttempts: 1,
		},
		{
			name:         "should not retry when the connection was reset, for the same reason",
			err:          io.ErrUnexpectedEOF,
			wantAttempts: 1,
		},
		{
			name:         "should not retry a plain domain error",
			err:          errors.New("ledger: insufficient funds"),
			wantAttempts: 1,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			attempts := 0
			err := testRetrier(5).Do(context.Background(), "test", func(context.Context) error {
				attempts++
				return tc.err
			})

			require.Error(t, err)
			assert.Equal(t, tc.wantAttempts, attempts)
			assert.ErrorIs(t, err, tc.err, "the caller must still see the underlying error")
		})
	}
}

// TestRetrier_RecognisesAWrappedError guards a mistake that would silently
// disable the whole mechanism: every statement in pgledger wraps its error with
// context before returning it, so a classifier using a type assertion rather
// than errors.As would never match anything in production while passing every
// test written against a bare error.
func TestRetrier_RecognisesAWrappedError(t *testing.T) {
	t.Parallel()

	wrapped := fmt.Errorf("apply balance delta: %w",
		fmt.Errorf("lock accounts: %w", pgErr(pgerrcode.DeadlockDetected)))

	attempts := 0
	err := testRetrier(3).Do(context.Background(), "test", func(context.Context) error {
		attempts++
		return wrapped
	})

	require.Error(t, err)
	assert.Equal(t, 3, attempts, "a wrapped 40P01 is still a 40P01")
}

func TestRetrier_StopsAsSoonAsTheTransactionSucceeds(t *testing.T) {
	t.Parallel()

	attempts := 0
	err := testRetrier(5).Do(context.Background(), "test", func(context.Context) error {
		attempts++
		if attempts < 3 {
			return pgErr(pgerrcode.SerializationFailure)
		}
		return nil
	})

	require.NoError(t, err)
	assert.Equal(t, 3, attempts)
}

// TestRetrier_StopsWhenTheDeadlineExpires pins the property that makes the
// per-attempt budget honest: the parent context bounds the whole sequence, so a
// five-attempt retry cannot consume five times the deadline the HTTP layer
// believes it granted.
func TestRetrier_StopsWhenTheDeadlineExpires(t *testing.T) {
	t.Parallel()

	retrier := NewRetrier(
		slog.New(slog.NewTextHandler(io.Discard, nil)),
		nil, 5, 50*time.Millisecond, 50*time.Millisecond,
	)

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()

	attempts := 0
	err := retrier.Do(ctx, "test", func(context.Context) error {
		attempts++
		return pgErr(pgerrcode.SerializationFailure)
	})

	require.Error(t, err)
	assert.Less(t, attempts, 5, "the deadline must cut the sequence short of the attempt cap")
	assert.ErrorIs(t, err, context.DeadlineExceeded, "the caller must be able to see why it stopped")
}

// TestRetrier_BackoffIsFullJitter checks the distribution, not just the bound.
//
// Full jitter means uniform over [0, window), and the reason it matters is the
// case this exists for: N writers aborted by the same deadlock waking at the
// same instant would collide again, reproducing the contention that caused
// them. A "capped exponential with a bit of jitter" would pass a bounds check
// while still clustering, so the test also asserts that the values actually
// spread -- including some near zero, which a scheme with a minimum delay could
// not produce.
func TestRetrier_BackoffIsFullJitter(t *testing.T) {
	t.Parallel()

	const (
		base    = 10 * time.Millisecond
		maximum = 80 * time.Millisecond
		samples = 2000
	)
	retrier := NewRetrier(nil, nil, 5, base, maximum)

	t.Run("should stay within the doubling window for each attempt", func(t *testing.T) {
		for attempt := 1; attempt <= 5; attempt++ {
			window := base << (attempt - 1)
			if window > maximum {
				window = maximum
			}
			for range 200 {
				got := retrier.backoff(attempt)
				assert.GreaterOrEqual(t, got, time.Duration(0))
				assert.LessOrEqual(t, got, window, "attempt %d must not exceed its window", attempt)
			}
		}
	})

	t.Run("should spread across the whole window rather than cluster", func(t *testing.T) {
		const window = 4 * base // attempt 3
		var low, high int
		for range samples {
			if retrier.backoff(3) < window/2 {
				low++
			} else {
				high++
			}
		}

		// Uniform means roughly half each way. The bound is loose enough not to
		// flake and tight enough to fail a scheme that only jitters a little
		// around a fixed exponential delay.
		assert.Greater(t, low, samples/4, "a full-jitter backoff must produce short delays too")
		assert.Greater(t, high, samples/4, "a full-jitter backoff must produce long delays too")
	})

	t.Run("should cap the window rather than overflow it", func(t *testing.T) {
		for _, attempt := range []int{5, 20, 62, 63, 64} {
			got := retrier.backoff(attempt)
			assert.GreaterOrEqual(t, got, time.Duration(0),
				"attempt %d must not produce a negative delay from a shift overflow", attempt)
			assert.LessOrEqual(t, got, maximum, "attempt %d must respect the cap", attempt)
		}
	})
}
