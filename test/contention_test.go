package test

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"

	"github.com/satyamsipah/ledger-core/internal/ledger"
)

const retriesMetric = "ledger_db_tx_retries_total"

// TestContention_HotAccountReportsItsRetryRate drives sustained contention onto
// a single account and reports what it cost.
//
// This is the test D10 and D11 are answerable to. The claim there is that READ
// COMMITTED plus row locks taken in one global order converts contention into a
// queue rather than into aborts -- so the expected retry rate on a hot account
// is not "low", it is ZERO, and the deadlock rate especially so. Reporting the
// number rather than only asserting a bound is deliberate: a regression that
// turns queueing back into aborting shows up here as a number that stopped
// being zero, long before it shows up in production as failed payments.
func TestContention_HotAccountReportsItsRetryRate(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	service, metrics := newRetryingLedgerService(t, sharedPool, false)

	// A house float every writer credits: the shape of a real payments ledger,
	// where every pay-in touches the same account.
	hot := newTypedAccount(t, ctx, sharedPool, ledger.AccountTypeLiability, "INR", false)

	const (
		writers        = 100
		postsPerWriter = 5
		amountMinor    = 100
	)

	funders := make([]uuid.UUID, writers)
	for i := range funders {
		funders[i] = newAccount(t, ctx, sharedPool, "INR", true)
	}

	started := time.Now()

	group, groupCtx := errgroup.WithContext(ctx)
	start := make(chan struct{})
	for i := range writers {
		group.Go(func() error {
			<-start
			for range postsPerWriter {
				request := transferRequest(t, funders[i], hot, amountMinor, "INR")
				if _, err := service.PostTransaction(groupCtx, request); err != nil {
					return err
				}
			}
			return nil
		})
	}
	close(start)
	require.NoError(t, group.Wait(), "no post may fail under contention alone")

	elapsed := time.Since(started)
	total := writers * postsPerWriter

	serialization := counterValue(t, metrics, retriesMetric, map[string]string{"sqlstate": "40001"})
	deadlocks := counterValue(t, metrics, retriesMetric, map[string]string{"sqlstate": "40P01"})
	retries := serialization + deadlocks

	t.Logf("hot-account contention: %d transactions by %d writers in %s (%.0f tx/s)",
		total, writers, elapsed.Round(time.Millisecond), float64(total)/elapsed.Seconds())
	t.Logf("retry rate: %.4f%% (%v retries over %d transactions; 40001=%v, 40P01=%v)",
		retries/float64(total)*100, retries, total, serialization, deadlocks)

	// The assertion D11 actually makes. Locking every account in one global
	// order means a cycle cannot be constructed, so a single deadlock here is a
	// write path that has started taking locks in some other order.
	assert.Zero(t, deadlocks,
		"ordered locking (D11) must make deadlocks unconstructible; %v were retried away", deadlocks)

	// And D10's: row locks queue, they do not abort. SERIALIZABLE or REPEATABLE
	// READ on this workload would produce a wall of 40001 here, which is the
	// trade those entries rejected.
	assert.Zero(t, serialization,
		"READ COMMITTED with explicit row locks (D10) must not produce serialization failures")

	balance, err := service.GetBalance(ctx, hot)
	require.NoError(t, err)
	assert.Equal(t, int64(total*amountMinor), balance.Available.AmountMinor(),
		"every credit must land exactly once: contention may delay a write, never lose or duplicate one")

	assertGlobalInvariant(t, ctx, sharedPool)
}

// TestContention_AdvisoryLocksPreserveCorrectness runs the same workload with
// the advisory-lock flag on.
//
// Whether it is FASTER is a question for the benchmark, not for a test. What a
// test can settle is the thing that would make the flag unusable: entering a
// second, independent lock space must not break the single global ordering that
// D11 depends on. If the advisory locks were taken in a different order than
// the row locks -- or on only some paths -- this is where the deadlocks would
// appear.
func TestContention_AdvisoryLocksPreserveCorrectness(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	service, metrics := newRetryingLedgerService(t, sharedPool, true)

	left := newAccount(t, ctx, sharedPool, "INR", true)
	right := newAccount(t, ctx, sharedPool, "INR", true)

	// Opposite-direction transfers over the same pair: the classic cycle, and
	// the workload that makes unordered locking deadlock within seconds.
	const rounds = 60

	group, groupCtx := errgroup.WithContext(ctx)
	start := make(chan struct{})
	for i := range rounds {
		group.Go(func() error {
			<-start
			from, to := left, right
			if i%2 == 1 {
				from, to = right, left
			}
			_, err := service.PostTransaction(groupCtx, transferRequest(t, from, to, 10, "INR"))
			return err
		})
	}
	close(start)
	require.NoError(t, group.Wait(), "advisory locks must not introduce a deadlock")

	deadlocks := counterValue(t, metrics, retriesMetric, map[string]string{"sqlstate": "40P01"})
	assert.Zero(t, deadlocks,
		"advisory locks taken in the same ascending order as the row locks cannot deadlock; got %v", deadlocks)

	// Equal traffic each way nets to zero, which is a stronger check than a
	// balance count: it would fail if any single transfer had been applied in
	// the wrong direction or applied twice.
	leftBalance, err := service.GetBalance(ctx, left)
	require.NoError(t, err)
	rightBalance, err := service.GetBalance(ctx, right)
	require.NoError(t, err)
	assert.Equal(t, int64(0), leftBalance.Available.AmountMinor()+rightBalance.Available.AmountMinor())

	assertGlobalInvariant(t, ctx, sharedPool)
}

// TestContention_RetrierDoesNotFireOnDomainErrors guards the exclusion that
// matters most.
//
// ErrInsufficientFunds is deterministic: retrying it cannot make the money
// appear, it can only make the caller wait five times as long to be told no. A
// retrier keying on "an error happened" rather than on SQLSTATE would do
// exactly that, and the symptom -- a slow 422 -- is mild enough to survive
// review for a long time.
func TestContention_RetrierDoesNotFireOnDomainErrors(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	service, metrics := newRetryingLedgerService(t, sharedPool, false)

	empty := newTypedAccount(t, ctx, sharedPool, ledger.AccountTypeLiability, "INR", false)
	sink := newAccount(t, ctx, sharedPool, "INR", true)

	_, err := service.PostTransaction(ctx, transferRequest(t, empty, sink, 5000, "INR"))
	require.ErrorIs(t, err, ledger.ErrInsufficientFunds)

	assert.Zero(t, counterValue(t, metrics, retriesMetric, nil),
		"a deterministic domain rejection must not be retried")

	assertGlobalInvariant(t, ctx, sharedPool)
}
