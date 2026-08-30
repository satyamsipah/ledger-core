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

// TestSharding_LogicalBalanceIsTheSumOverShards is the basic contract: an
// account that was split stays one account to every reader.
func TestSharding_LogicalBalanceIsTheSumOverShards(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	service := newLedgerService(sharedPool)

	float := newTypedAccount(t, ctx, sharedPool, ledger.AccountTypeLiability, "INR", false)
	shards := shardAccount(t, ctx, sharedPool, float, 16)
	require.Len(t, shards, 16)

	const (
		posts  = 64
		amount = 125
	)
	for range posts {
		funder := newAccount(t, ctx, sharedPool, "INR", true)
		_, err := service.PostTransaction(ctx, transferRequest(t, funder, float, amount, "INR"))
		require.NoError(t, err)
	}

	balance, err := service.GetBalance(ctx, float)
	require.NoError(t, err)
	assert.Equal(t, int64(posts*amount), balance.Available.AmountMinor(),
		"the logical balance is the sum over shards")

	// Not one shard. A router that always picked shard 0 would satisfy the
	// balance assertion above and deliver none of the throughput sharding
	// exists for, so the spread is asserted rather than assumed.
	touched := shardsTouched(t, ctx, sharedPool, float)
	assert.Greater(t, touched, 1, "writes must spread across shards, not pile onto one")
	t.Logf("%d posts spread across %d of %d shards", posts, touched, len(shards))

	// The parent itself holds nothing: ledger_shard_account moves no balance,
	// and routing sends new writes to children only.
	var parentBalance int64
	require.NoError(t, sharedPool.QueryRow(ctx,
		`SELECT available_minor FROM account_balances WHERE account_id = $1`, float).Scan(&parentBalance))
	assert.Equal(t, int64(0), parentBalance)

	assertGlobalInvariant(t, ctx, sharedPool)
}

// TestSharding_PreservesTheOverdraftInvariant is the safety half of the
// trade-off, and the reason sharding does not violate invariant 4.
//
// Every shard carries account_balances_no_overdraft_check individually, so
// every shard is non-negative, so their sum is non-negative. You cannot
// overdraw the logical account without overdrawing some shard first, and the
// constraint stops that.
func TestSharding_PreservesTheOverdraftInvariant(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	service := newLedgerService(sharedPool)

	float := newTypedAccount(t, ctx, sharedPool, ledger.AccountTypeLiability, "INR", false)
	shards := shardAccount(t, ctx, sharedPool, float, 4)

	funder := newAccount(t, ctx, sharedPool, "INR", true)
	_, err := service.PostTransaction(ctx, transferRequest(t, funder, float, 400, "INR"))
	require.NoError(t, err)

	// Drain hard, concurrently. Whatever succeeds, no shard may end negative.
	sink := newAccount(t, ctx, sharedPool, "INR", true)
	group := &errgroup.Group{}
	for range 40 {
		group.Go(func() error {
			// Rejections are the expected outcome for most of these, so the
			// error is deliberately swallowed; what matters is the state left
			// behind, asserted below.
			_, _ = service.PostTransaction(ctx, transferRequest(t, float, sink, 100, "INR"))
			return nil
		})
	}
	require.NoError(t, group.Wait())

	var negativeShards int
	require.NoError(t, sharedPool.QueryRow(ctx, `
		SELECT count(*)
		  FROM account_balances ab
		  JOIN accounts a ON a.id = ab.account_id
		 WHERE a.parent_account_id = $1 AND ab.available_minor < 0`, float).Scan(&negativeShards))
	assert.Zero(t, negativeShards, "invariant 4 holds per shard, so it holds for their sum")

	balance, err := service.GetBalance(ctx, float)
	require.NoError(t, err)
	assert.GreaterOrEqual(t, balance.Available.AmountMinor(), int64(0),
		"the logical balance is a sum of non-negatives and cannot be negative")

	t.Logf("logical balance after the drain: %s across %d shards",
		balance.Available, len(shards))

	assertGlobalInvariant(t, ctx, sharedPool)
}

// TestSharding_CanRefuseADebitTheLogicalAccountCouldAfford asserts the LIVENESS
// cost, on purpose.
//
// This is the documented price of sharding (D24), and a test that pins it is
// worth more than a comment: it makes the weakness a known, reproducible
// property rather than something a future reader has to rediscover from a
// support ticket. Value spread thinly across shards can refuse a debit the
// account plainly holds, because the overdraft check is per row.
//
// It is also why sharding is restricted to accounts whose traffic is
// effectively one-directional, and why a sibling-to-sibling rebalancer is the
// fix rather than a weaker constraint.
func TestSharding_CanRefuseADebitTheLogicalAccountCouldAfford(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	service := newLedgerService(sharedPool)

	float := newTypedAccount(t, ctx, sharedPool, ledger.AccountTypeLiability, "INR", false)
	shards := shardAccount(t, ctx, sharedPool, float, 8)

	// Put exactly 100 on every shard: 800 in total, 100 anywhere in particular.
	for _, shard := range shards {
		funder := newAccount(t, ctx, sharedPool, "INR", true)
		_, err := service.PostTransaction(ctx, transferRequest(t, funder, shard, 100, "INR"))
		require.NoError(t, err)
	}

	balance, err := service.GetBalance(ctx, float)
	require.NoError(t, err)
	require.Equal(t, int64(800), balance.Available.AmountMinor())

	// The account holds 800. This debit of 500 is affordable logically and
	// unaffordable on every individual shard, so it must fail -- conservatively,
	// never by going negative.
	sink := newAccount(t, ctx, sharedPool, "INR", true)
	_, err = service.PostTransaction(ctx, transferRequest(t, float, sink, 500, "INR"))
	require.ErrorIs(t, err, ledger.ErrInsufficientFunds,
		"a per-shard check refuses what the logical account could afford: the documented cost of sharding")

	after, err := service.GetBalance(ctx, float)
	require.NoError(t, err)
	assert.Equal(t, int64(800), after.Available.AmountMinor(), "the refusal moved nothing")

	assertGlobalInvariant(t, ctx, sharedPool)
}

// TestSharding_ReversalReturnsToTheSameShard covers the one place routing must
// NOT run.
//
// A reversal mirrors the original's entries, which already name the shards the
// money went to. Routing it afresh would pick shards at random and could drive
// one negative while a sibling held the funds, turning a correction into an
// insufficient-funds failure on an account that plainly has the money.
func TestSharding_ReversalReturnsToTheSameShard(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	service := newLedgerService(sharedPool)

	float := newTypedAccount(t, ctx, sharedPool, ledger.AccountTypeLiability, "INR", false)
	shardAccount(t, ctx, sharedPool, float, 8)

	funder := newAccount(t, ctx, sharedPool, "INR", true)
	posted, err := service.PostTransaction(ctx, transferRequest(t, funder, float, 700, "INR"))
	require.NoError(t, err)

	// Which shard did it land on?
	var creditedShard uuid.UUID
	for _, e := range posted.Entries {
		if e.Direction == ledger.DirectionCredit {
			creditedShard = e.AccountID
		}
	}
	require.NotEqual(t, uuid.Nil, creditedShard)
	require.NotEqual(t, float, creditedShard, "the credit must have landed on a shard, not the parent")

	reversal, err := service.ReverseTransaction(ctx, posted.ID, "test reversal")
	require.NoError(t, err)

	var debitedShard uuid.UUID
	for _, e := range reversal.Entries {
		if e.Direction == ledger.DirectionDebit {
			debitedShard = e.AccountID
		}
	}
	assert.Equal(t, creditedShard, debitedShard,
		"a reversal must take the money out of the shard it went into")

	balance, err := service.GetBalance(ctx, float)
	require.NoError(t, err)
	assert.Equal(t, int64(0), balance.Available.AmountMinor())

	assertGlobalInvariant(t, ctx, sharedPool)
}

// TestSharding_StatementAndTemporalBalanceSpanShards guards the trap sharding
// would otherwise set for the read paths.
//
// A customer's statement must read as one account. Had the queries kept
// matching on je.account_id = $1, a sharded account's statement would have come
// back empty -- a plausible-looking answer that is completely wrong, which is
// the worst kind.
func TestSharding_StatementAndTemporalBalanceSpanShards(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	service := newLedgerService(sharedPool)

	float := newTypedAccount(t, ctx, sharedPool, ledger.AccountTypeLiability, "INR", false)
	shardAccount(t, ctx, sharedPool, float, 8)

	const posts = 12
	for range posts {
		funder := newAccount(t, ctx, sharedPool, "INR", true)
		_, err := service.PostTransaction(ctx, transferRequest(t, funder, float, 50, "INR"))
		require.NoError(t, err)
	}

	statement, err := service.GetStatement(ctx, ledger.StatementQuery{
		AccountID: float,
		From:      time.Now().Add(-time.Hour),
		To:        time.Now().Add(time.Hour),
		Limit:     100,
	})
	require.NoError(t, err)
	assert.Len(t, statement.Lines, posts, "every shard's entries belong to the logical account's statement")
	assert.Equal(t, int64(posts*50), statement.Closing.AmountMinor(),
		"the running balance must accumulate across shards")

	asOf, err := service.GetBalanceAsOf(ctx, float, time.Now().Add(time.Hour))
	require.NoError(t, err)
	assert.Equal(t, int64(posts*50), asOf.AmountMinor(),
		"the temporal balance sums the journal across shards too")

	assertGlobalInvariant(t, ctx, sharedPool)
}

// TestSharding_ThroughputSingleVersusSixteen is the measurement D24 rests on.
//
// Identical workload against an unsharded account and a 16-way sharded one:
// N writers each posting M credits into one logical account. The unsharded arm
// is bounded by one row lock, so its ceiling is roughly 1/(lock hold time)
// regardless of hardware; the sharded arm spreads over sixteen locks until
// something else becomes the bottleneck.
//
// The numbers go into docs/DECISIONS.md. They are from a container on a
// developer laptop and are therefore a RATIO worth quoting and an absolute
// figure worth ignoring -- the shape of the result is the finding, not the
// throughput of this machine.
func TestSharding_ThroughputSingleVersusSixteen(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	const (
		writers        = 32
		postsPerWriter = 8
		amount         = 10
		shardCount     = 16
	)
	total := writers * postsPerWriter

	run := func(t *testing.T, shards int) time.Duration {
		t.Helper()

		service, _ := newRetryingLedgerService(t, sharedPool, false)

		target := newTypedAccount(t, ctx, sharedPool, ledger.AccountTypeLiability, "INR", false)
		if shards > 1 {
			shardAccount(t, ctx, sharedPool, target, shards)
		}

		// Funders are created up front so account creation is not timed, and
		// one per writer so the only contended row is the target's.
		funders := make([]uuid.UUID, writers)
		for i := range funders {
			funders[i] = newAccount(t, ctx, sharedPool, "INR", true)
		}

		group, groupCtx := errgroup.WithContext(ctx)
		start := make(chan struct{})
		for i := range writers {
			group.Go(func() error {
				<-start
				for range postsPerWriter {
					if _, err := service.PostTransaction(groupCtx,
						transferRequest(t, funders[i], target, amount, "INR")); err != nil {
						return err
					}
				}
				return nil
			})
		}

		started := time.Now()
		close(start)
		require.NoError(t, group.Wait())
		elapsed := time.Since(started)

		balance, err := service.GetBalance(ctx, target)
		require.NoError(t, err)
		require.Equal(t, int64(total*amount), balance.Available.AmountMinor(),
			"the benchmark must post every transaction exactly once to mean anything")

		return elapsed
	}

	single := run(t, 1)
	sharded := run(t, shardCount)

	singleRate := float64(total) / single.Seconds()
	shardedRate := float64(total) / sharded.Seconds()

	t.Logf("single account : %d tx in %-12s %7.0f tx/s",
		total, single.Round(time.Millisecond), singleRate)
	t.Logf("%2d shards      : %d tx in %-12s %7.0f tx/s  (%.2fx)",
		shardCount, total, sharded.Round(time.Millisecond), shardedRate, shardedRate/singleRate)

	// No assertion on the ratio. On a laptop container the bottleneck is as
	// likely to be fsync or the connection pool as the row lock, so a threshold
	// here would be a flaky test asserting a property of the machine rather
	// than of the design. The numbers are the deliverable; D24 records them.
	assertGlobalInvariant(t, ctx, sharedPool)
}
