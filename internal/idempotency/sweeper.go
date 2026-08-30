package idempotency

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/satyamsipah/ledger-core/internal/observability"
)

// Sweeper defaults. The interval is far shorter than the 24-hour TTL because
// the cost of sweeping nothing is one indexed range scan that returns no rows,
// and the cost of sweeping too rarely is a table that spends most of its life
// holding records nobody will ever ask for.
const (
	DefaultSweepInterval = 5 * time.Minute
	DefaultSweepBatch    = 1000
)

// Sweeper deletes expired idempotency records on a schedule.
//
// Without it this table grows without bound, and its primary key -- which is
// the concurrency mechanism behind invariant 5, not merely a lookup index --
// degrades along with it. The reaper is therefore load-bearing for write
// throughput, not housekeeping.
//
// WHAT IT DOES NOT DO: reclaim abandoned leases. A dead lease is reclaimed
// lazily, by the retry that runs into it, because that request is the one
// holding the fingerprint needed to decide whether taking over is even legal.
// A sweeper doing it in the background would have to either skip that check or
// duplicate it, and skipping it would let an unrelated request inherit a key.
type Sweeper struct {
	store    Store
	logger   *slog.Logger
	metrics  *observability.Metrics
	interval time.Duration
	batch    int
}

// NewSweeper wires a sweeper to its store.
func NewSweeper(store Store, logger *slog.Logger, metrics *observability.Metrics, interval time.Duration, batch int) *Sweeper {
	if interval <= 0 {
		interval = DefaultSweepInterval
	}
	if batch <= 0 {
		batch = DefaultSweepBatch
	}
	return &Sweeper{store: store, logger: logger, metrics: metrics, interval: interval, batch: batch}
}

// Run sweeps until ctx is cancelled.
//
// No leader election and no advisory lock around the sweep. Several replicas
// running this concurrently is fine and is in fact the point of the SKIP LOCKED
// in the delete: they divide the work instead of queueing behind each other,
// and a rolling deploy never has a window with no sweeper running. Electing a
// leader would add a failure mode -- the leader wedges, nobody sweeps, and the
// table grows silently -- in exchange for avoiding contention that SKIP LOCKED
// has already removed.
func (s *Sweeper) Run(ctx context.Context) error {
	ticker := time.NewTicker(s.interval)
	defer ticker.Stop()

	s.logger.Info("idempotency sweeper started",
		slog.Duration("interval", s.interval),
		slog.Int("batch", s.batch))

	for {
		select {
		case <-ctx.Done():
			s.logger.Info("idempotency sweeper stopped")
			// Cancellation is how this loop is meant to end, so it is not an
			// error to report upward -- returning ctx.Err() here would make a
			// clean shutdown look like a failure in the errgroup that runs it.
			return nil
		case <-ticker.C:
			s.sweepOnce(ctx)
		}
	}
}

// sweepOnce drains expired records in batches until a batch comes back short.
//
// Draining rather than deleting one batch per tick, because a burst of traffic
// a day ago produces a burst of expiries today: at one batch every five minutes
// a spike of a million keys would take three days to clear, and the backlog
// would still be growing.
func (s *Sweeper) sweepOnce(ctx context.Context) {
	var total int64

	for {
		deleted, err := s.store.Sweep(ctx, s.batch)
		if err != nil {
			// A cancelled context during shutdown is not worth an error line;
			// anything else is, because a sweeper that has silently stopped
			// working looks exactly like one with nothing to do.
			if !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
				s.logger.ErrorContext(ctx, "sweep expired idempotency keys",
					slog.String("error", err.Error()),
					slog.Int64("swept_before_failure", total))
			}
			return
		}

		total += deleted
		if s.metrics != nil {
			s.metrics.IdempotencySwept.Add(float64(deleted))
		}

		if deleted < int64(s.batch) {
			break
		}

		// Yield between batches so a large drain cannot monopolise a pool
		// connection that inbound requests are waiting for.
		select {
		case <-ctx.Done():
			return
		default:
		}
	}

	if total > 0 {
		s.logger.Info("swept expired idempotency keys", slog.Int64("deleted", total))
	}
}
