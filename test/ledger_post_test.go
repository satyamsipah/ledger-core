package test

import (
	"context"
	"encoding/json"
	"errors"
	"math/rand/v2"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgerrcode"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"

	"github.com/satyamsipah/ledger-core/internal/ledger"
	"github.com/satyamsipah/ledger-core/internal/outbox"
)

// TestPostTransaction_PostsBalancedTransfer walks the whole write path once and
// checks everything it is supposed to have touched.
//
// The accounts are LIABILITY wallets, not assets, on purpose: a wallet is
// CREDIT-normal, so the transaction sign convention and the account sign
// convention disagree on it. A sign bug that an asset-only test would sail past
// shows up here as a balance of the wrong sign.
func TestPostTransaction_PostsBalancedTransfer(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	svc := newLedgerService(sharedPool)

	// The funding account may go negative: it stands in for the platform's own
	// float, which is where the money in a wallet comes from.
	source := newTypedAccount(t, ctx, sharedPool, ledger.AccountTypeLiability, "INR", true)
	target := newTypedAccount(t, ctx, sharedPool, ledger.AccountTypeLiability, "INR", false)

	posted, err := svc.PostTransaction(ctx, ledger.TransactionRequest{
		Type:        ledger.TransactionTypeTransfer,
		ExternalRef: strPtr("gw-" + mustUUIDv7(t).String()),
		Metadata:    map[string]any{"note": "first transfer"},
		Entries: []ledger.EntryRequest{
			{AccountID: source, Direction: ledger.DirectionDebit, Amount: ledger.MustNewMoney(500_00, "INR")},
			{AccountID: target, Direction: ledger.DirectionCredit, Amount: ledger.MustNewMoney(500_00, "INR")},
		},
	})
	require.NoError(t, err)

	t.Run("should return a posted transaction", func(t *testing.T) {
		assert.Equal(t, ledger.TransactionStatusPosted, posted.Status)
		require.NotNil(t, posted.PostedAt, "a POSTED transaction must carry posted_at")
		assert.False(t, posted.CreatedAt.IsZero())
		require.Len(t, posted.Entries, 2)
		assert.Equal(t, 0, posted.Entries[0].EntrySeq)
		assert.Equal(t, 1, posted.Entries[1].EntrySeq)
	})

	t.Run("should persist the transaction and its entries", func(t *testing.T) {
		var (
			status   string
			postedAt *string
			entries  int
		)
		require.NoError(t, sharedPool.QueryRow(ctx,
			`SELECT status, posted_at::text FROM transactions WHERE id = $1`, posted.ID).
			Scan(&status, &postedAt))
		assert.Equal(t, "POSTED", status)
		assert.NotNil(t, postedAt)

		require.NoError(t, sharedPool.QueryRow(ctx,
			`SELECT count(*) FROM journal_entries WHERE transaction_id = $1`, posted.ID).Scan(&entries))
		assert.Equal(t, 2, entries)
	})

	t.Run("should sign the balances by each account's normal balance", func(t *testing.T) {
		// Both accounts are CREDIT-normal. The DEBIT leg therefore decreases the
		// source and the CREDIT leg increases the target -- the opposite of what
		// the transaction-level sign convention would give.
		sourceBalance, err := svc.GetBalance(ctx, source)
		require.NoError(t, err)
		assert.Equal(t, int64(-500_00), sourceBalance.Available.AmountMinor())
		assert.Equal(t, "INR", sourceBalance.Available.Currency())

		targetBalance, err := svc.GetBalance(ctx, target)
		require.NoError(t, err)
		assert.Equal(t, int64(500_00), targetBalance.Available.AmountMinor())
	})

	t.Run("should bump each balance version exactly once", func(t *testing.T) {
		for _, id := range []uuid.UUID{source, target} {
			balance, err := svc.GetBalance(ctx, id)
			require.NoError(t, err)
			assert.Equal(t, int64(1), balance.Version,
				"one transaction must produce exactly one version bump")
		}
	})

	t.Run("should append one outbox event carrying the resulting balances", func(t *testing.T) {
		var (
			aggregateType string
			eventType     string
			envelopeBytes []byte
		)
		require.NoError(t, sharedPool.QueryRow(ctx, `
			SELECT aggregate_type, event_type, payload
			  FROM outbox WHERE aggregate_id = $1 AND aggregate_type = 'transaction'`, posted.ID.String()).
			Scan(&aggregateType, &eventType, &envelopeBytes))

		assert.Equal(t, "transaction", aggregateType)
		assert.Equal(t, ledger.EventTypeTransactionPosted, eventType)

		// The stored payload is the full wire envelope (event_id, event_type,
		// event_version, occurred_at, trace_id) wrapping the transaction-
		// specific fields, per docs/DECISIONS.md D31/D32 -- both publishers
		// relay this column verbatim, so it has to be self-contained.
		var envelope outbox.Envelope
		require.NoError(t, json.Unmarshal(envelopeBytes, &envelope))
		assert.Equal(t, ledger.EventTypeTransactionPosted, envelope.EventType)
		assert.Equal(t, posted.ID.String(), envelope.AggregateID)

		var event struct {
			TransactionID uuid.UUID `json:"transaction_id"`
			Currency      string    `json:"currency"`
			Entries       []struct {
				AccountID uuid.UUID `json:"account_id"`
				Direction string    `json:"direction"`
				Amount    struct {
					Amount   string `json:"amount"`
					Currency string `json:"currency"`
					Scale    int    `json:"scale"`
				} `json:"amount"`
			} `json:"entries"`
			Balances []struct {
				AccountID uuid.UUID `json:"account_id"`
				Version   int64     `json:"version"`
			} `json:"balances"`
		}
		require.NoError(t, json.Unmarshal(envelope.Payload, &event))

		assert.Equal(t, posted.ID, event.TransactionID)
		assert.Equal(t, "INR", event.Currency)
		require.Len(t, event.Entries, 2)
		require.Len(t, event.Balances, 2,
			"the projector needs every touched balance and its version to dedupe redeliveries")

		// Amounts must cross the wire as strings; a JSON number would lose
		// precision in the TypeScript dashboard.
		assert.Equal(t, "50000", event.Entries[0].Amount.Amount)
		assert.Equal(t, 2, event.Entries[0].Amount.Scale)
	})

	t.Run("should append one BalanceUpdated event per account touched", func(t *testing.T) {
		var count int
		require.NoError(t, sharedPool.QueryRow(ctx, `
			SELECT count(*) FROM outbox
			 WHERE aggregate_type = 'account' AND event_type = $1
			   AND aggregate_id IN ($2, $3)`,
			ledger.EventTypeBalanceUpdated, source.String(), target.String()).Scan(&count))
		assert.Equal(t, 2, count,
			"one BalanceUpdated per touched account is what makes partition key = account_id meaningful; see D32")
	})

	assertGlobalInvariant(t, ctx, sharedPool)
}

// TestPostTransaction_RejectsBadRequests covers the failures that can only be
// detected once account state has been read under a lock. Each case also
// asserts that nothing at all was written, because a partial post is worse than
// a rejected one.
// TestPostTransaction_RecordsMetrics proves ledger_transactions_posted_total
// and ledger_transaction_duration_seconds are wired all the way through --
// not merely that the counters exist, but that a real post and a real
// rejection each land in the label set an operator's dashboard would filter
// on.
func TestPostTransaction_RecordsMetrics(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	svc, metrics := newRetryingLedgerService(t, sharedPool, false)

	source := newTypedAccount(t, ctx, sharedPool, ledger.AccountTypeLiability, "INR", true)
	target := newTypedAccount(t, ctx, sharedPool, ledger.AccountTypeLiability, "INR", false)

	before := counterValue(t, metrics, "ledger_transactions_posted_total",
		map[string]string{"type": "TRANSFER", "status": "success"})

	posted, err := svc.PostTransaction(ctx, ledger.TransactionRequest{
		Type: ledger.TransactionTypeTransfer,
		Entries: []ledger.EntryRequest{
			{AccountID: source, Direction: ledger.DirectionDebit, Amount: ledger.MustNewMoney(100_00, "INR")},
			{AccountID: target, Direction: ledger.DirectionCredit, Amount: ledger.MustNewMoney(100_00, "INR")},
		},
	})
	require.NoError(t, err)

	t.Run("should count a successful post by type and outcome", func(t *testing.T) {
		after := counterValue(t, metrics, "ledger_transactions_posted_total",
			map[string]string{"type": "TRANSFER", "status": "success"})
		assert.Equal(t, before+1, after)
	})

	t.Run("should count a rejected post separately from a successful one", func(t *testing.T) {
		before := counterValue(t, metrics, "ledger_transactions_posted_total",
			map[string]string{"type": "TRANSFER", "status": "error"})

		_, err := svc.PostTransaction(ctx, ledger.TransactionRequest{
			Type: ledger.TransactionTypeTransfer,
			Entries: []ledger.EntryRequest{
				{AccountID: source, Direction: ledger.DirectionDebit, Amount: ledger.MustNewMoney(100_00, "INR")},
				// Missing the credit leg: unbalanced, rejected before any lock
				// is taken.
			},
		})
		require.Error(t, err)

		after := counterValue(t, metrics, "ledger_transactions_posted_total",
			map[string]string{"type": "TRANSFER", "status": "error"})
		assert.Equal(t, before+1, after)
	})

	t.Run("should count a reversal as its own type, not as the original transfer", func(t *testing.T) {
		before := counterValue(t, metrics, "ledger_transactions_posted_total",
			map[string]string{"type": "REVERSAL", "status": "success"})

		_, err := svc.ReverseTransaction(ctx, posted.ID, "test reversal")
		require.NoError(t, err)

		after := counterValue(t, metrics, "ledger_transactions_posted_total",
			map[string]string{"type": "REVERSAL", "status": "success"})
		assert.Equal(t, before+1, after)
	})

	t.Run("should record duration observations for the transfer type", func(t *testing.T) {
		families, err := metrics.Registry().Gather()
		require.NoError(t, err)

		var sampleCount uint64
		for _, family := range families {
			if family.GetName() != "ledger_transaction_duration_seconds" {
				continue
			}
			for _, m := range family.GetMetric() {
				for _, pair := range m.GetLabel() {
					if pair.GetName() == "type" && pair.GetValue() == "TRANSFER" {
						sampleCount += m.GetHistogram().GetSampleCount()
					}
				}
			}
		}
		assert.Positive(t, sampleCount, "expected at least one TRANSFER duration observation")
	})
}

func TestPostTransaction_RejectsBadRequests(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	svc := newLedgerService(sharedPool)

	funded := newAccount(t, ctx, sharedPool, "INR", true)
	restricted := newAccount(t, ctx, sharedPool, "INR", false)
	usd := newAccount(t, ctx, sharedPool, "USD", true)
	frozen := newAccount(t, ctx, sharedPool, "INR", true)
	closed := newAccount(t, ctx, sharedPool, "INR", true)
	missing := mustUUIDv7(t)

	setStatus(t, ctx, frozen, "FROZEN")
	setStatus(t, ctx, closed, "CLOSED")

	entry := func(account uuid.UUID, direction ledger.Direction, amount int64, currency string) ledger.EntryRequest {
		return ledger.EntryRequest{
			AccountID: account,
			Direction: direction,
			Amount:    ledger.MustNewMoney(amount, currency),
		}
	}

	tests := []struct {
		name    string
		entries []ledger.EntryRequest
		wantErr error
	}{
		{
			name: "should reject the post when an account does not exist",
			entries: []ledger.EntryRequest{
				entry(funded, ledger.DirectionDebit, 100_00, "INR"),
				entry(missing, ledger.DirectionCredit, 100_00, "INR"),
			},
			wantErr: ledger.ErrAccountNotFound,
		},
		{
			name: "should reject the post when an account is frozen",
			entries: []ledger.EntryRequest{
				entry(funded, ledger.DirectionDebit, 100_00, "INR"),
				entry(frozen, ledger.DirectionCredit, 100_00, "INR"),
			},
			wantErr: ledger.ErrAccountNotPostable,
		},
		{
			name: "should reject the post when an account is closed",
			entries: []ledger.EntryRequest{
				entry(funded, ledger.DirectionDebit, 100_00, "INR"),
				entry(closed, ledger.DirectionCredit, 100_00, "INR"),
			},
			wantErr: ledger.ErrAccountNotPostable,
		},
		{
			name: "should reject the post when a leg's currency differs from its account",
			entries: []ledger.EntryRequest{
				entry(funded, ledger.DirectionDebit, 100_00, "INR"),
				entry(usd, ledger.DirectionCredit, 100_00, "INR"),
			},
			wantErr: ledger.ErrCurrencyMismatch,
		},
		{
			name: "should reject the post when a restricted account would go negative",
			entries: []ledger.EntryRequest{
				entry(restricted, ledger.DirectionCredit, 100_00, "INR"),
				entry(funded, ledger.DirectionDebit, 100_00, "INR"),
			},
			wantErr: ledger.ErrInsufficientFunds,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			before := countEntries(t, ctx, tc.entries)

			_, err := svc.PostTransaction(ctx, ledger.TransactionRequest{
				Type:    ledger.TransactionTypeTransfer,
				Entries: tc.entries,
			})
			require.ErrorIs(t, err, tc.wantErr)

			assert.Equal(t, before, countEntries(t, ctx, tc.entries),
				"a rejected post must leave no journal entries behind")
		})
	}

	t.Cleanup(func() { assertGlobalInvariant(t, ctx, sharedPool) })
}

// TestPostTransaction_OverdraftUnderConcurrency is invariant 4 under contention.
//
// Ten goroutines each try to withdraw a quarter of the balance from an account
// that can only fund four of them. Exactly four must succeed. Anything else
// means the overdraft check read a balance that another transaction was already
// spending, which is the precise failure that row locking exists to prevent.
func TestPostTransaction_OverdraftUnderConcurrency(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	svc := newLedgerService(sharedPool)

	const (
		withdrawers = 10
		affordable  = 4
		amount      = int64(250_00)
	)

	float := newAccount(t, ctx, sharedPool, "INR", true)
	wallet := newTypedAccount(t, ctx, sharedPool, ledger.AccountTypeLiability, "INR", false)
	sink := newAccount(t, ctx, sharedPool, "INR", true)

	fund(t, ctx, svc, float, wallet, amount*affordable)

	var succeeded, insufficient atomic.Int64

	var wg sync.WaitGroup
	wg.Add(withdrawers)
	for range withdrawers {
		go func() {
			defer wg.Done()

			_, err := svc.PostTransaction(ctx, ledger.TransactionRequest{
				Type: ledger.TransactionTypeTransfer,
				Entries: []ledger.EntryRequest{
					{AccountID: wallet, Direction: ledger.DirectionDebit, Amount: ledger.MustNewMoney(amount, "INR")},
					{AccountID: sink, Direction: ledger.DirectionCredit, Amount: ledger.MustNewMoney(amount, "INR")},
				},
			})
			switch {
			case err == nil:
				succeeded.Add(1)
			case errors.Is(err, ledger.ErrInsufficientFunds):
				insufficient.Add(1)
			default:
				assert.NoError(t, err, "unexpected failure mode")
			}
		}()
	}
	wg.Wait()

	assert.Equal(t, int64(affordable), succeeded.Load(),
		"exactly the affordable number of withdrawals may succeed")
	assert.Equal(t, int64(withdrawers-affordable), insufficient.Load())

	balance, err := svc.GetBalance(ctx, wallet)
	require.NoError(t, err)
	assert.Zero(t, balance.Available.AmountMinor(), "the wallet must land exactly on zero")
	assert.GreaterOrEqual(t, balance.Available.AmountMinor(), int64(0),
		"a restricted account must never be observed negative")

	assertGlobalInvariant(t, ctx, sharedPool)
}

// TestPostTransaction_UnderConcurrency is the headline concurrency test: 200
// goroutines moving money between the same five accounts.
//
// The assertion is not "it did not crash". It is that the final balances equal,
// to the paisa, the sum of what the successful transfers moved, that they equal
// what the journal independently says, and that the five accounts together hold
// exactly what they started with. Money is neither created nor destroyed.
func TestPostTransaction_UnderConcurrency(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	svc := newLedgerService(sharedPool)

	const (
		writers  = 200
		accounts = 5
		opening  = int64(1_000_00)
	)

	float := newAccount(t, ctx, sharedPool, "INR", true)

	// Wallets are CREDIT-normal and allow_negative, so every transfer succeeds
	// and the arithmetic stays checkable: this test is about lost updates, not
	// about the overdraft path, which has its own test above.
	wallets := make([]uuid.UUID, accounts)
	for i := range wallets {
		wallets[i] = newTypedAccount(t, ctx, sharedPool, ledger.AccountTypeLiability, "INR", true)
		fund(t, ctx, svc, float, wallets[i], opening)
	}

	// Expected movement per account, accumulated independently of the database.
	expected := make([]atomic.Int64, accounts)
	for i := range expected {
		expected[i].Store(opening)
	}

	var wg sync.WaitGroup
	wg.Add(writers)
	for w := range writers {
		go func(seed uint64) {
			defer wg.Done()

			rng := rand.New(rand.NewPCG(seed, 0x5EED))

			from := rng.IntN(accounts)
			to := (from + 1 + rng.IntN(accounts-1)) % accounts
			amount := int64(rng.IntN(500)+1) * 100

			_, err := svc.PostTransaction(ctx, ledger.TransactionRequest{
				Type: ledger.TransactionTypeTransfer,
				Entries: []ledger.EntryRequest{
					{AccountID: wallets[from], Direction: ledger.DirectionDebit, Amount: ledger.MustNewMoney(amount, "INR")},
					{AccountID: wallets[to], Direction: ledger.DirectionCredit, Amount: ledger.MustNewMoney(amount, "INR")},
				},
			})
			if !assert.NoError(t, err) {
				return
			}

			// Only recorded after the commit succeeded, so the expectation
			// tracks what actually landed.
			expected[from].Add(-amount)
			expected[to].Add(amount)
		}(uint64(w) + 1)
	}
	wg.Wait()

	var total int64
	for i, wallet := range wallets {
		want := expected[i].Load()

		balance, err := svc.GetBalance(ctx, wallet)
		require.NoError(t, err)
		assert.Equal(t, want, balance.Available.AmountMinor(),
			"account %d: stored balance must match the sum of committed transfers", i)

		// The journal is derived independently of account_balances. If the two
		// disagree, a balance update was lost or double-applied.
		assert.Equal(t, want, journalBalance(t, ctx, wallet),
			"account %d: stored balance must match the journal", i)

		total += balance.Available.AmountMinor()
	}

	assert.Equal(t, opening*accounts, total,
		"the five accounts must together hold exactly what they were funded with")

	assertGlobalInvariant(t, ctx, sharedPool)
}

// TestPostTransaction_ConcurrentOppositeTransfersDoNotDeadlock is the empirical
// proof behind ledger.sortedAccountIDs.
//
// Half the writers move A to B and half move B to A. Without the sort, this is
// the textbook deadlock: two transactions acquiring the same pair of locks in
// opposite orders. PostgreSQL would break the cycle by killing one of them with
// SQLSTATE 40P01, so a single deadlock error here is a failure.
//
// It also guards an assumption the sort alone does not cover: that PostgreSQL
// applies ORDER BY before the row locks in LockAccounts. That is a property of
// the plan shape, not something the SQL standard promises, so it is asserted by
// experiment rather than by reading.
func TestPostTransaction_ConcurrentOppositeTransfersDoNotDeadlock(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	svc := newLedgerService(sharedPool)

	const perDirection = 60

	a := newAccount(t, ctx, sharedPool, "INR", true)
	b := newAccount(t, ctx, sharedPool, "INR", true)

	var deadlocks atomic.Int64

	var wg sync.WaitGroup
	wg.Add(perDirection * 2)

	transfer := func(from, to uuid.UUID) {
		defer wg.Done()

		_, err := svc.PostTransaction(ctx, ledger.TransactionRequest{
			Type: ledger.TransactionTypeTransfer,
			Entries: []ledger.EntryRequest{
				{AccountID: from, Direction: ledger.DirectionCredit, Amount: ledger.MustNewMoney(10_00, "INR")},
				{AccountID: to, Direction: ledger.DirectionDebit, Amount: ledger.MustNewMoney(10_00, "INR")},
			},
		})
		if err != nil {
			var pgErr *pgconn.PgError
			if errors.As(err, &pgErr) && pgErr.Code == pgerrcode.DeadlockDetected {
				deadlocks.Add(1)
				return
			}
			assert.NoError(t, err)
		}
	}

	for range perDirection {
		go transfer(a, b)
		go transfer(b, a)
	}
	wg.Wait()

	assert.Zero(t, deadlocks.Load(),
		"deterministic lock ordering must make deadlock unreachable, not merely rare")

	// Equal traffic in both directions, so both accounts land back on zero.
	for _, id := range []uuid.UUID{a, b} {
		balance, err := svc.GetBalance(ctx, id)
		require.NoError(t, err)
		assert.Zero(t, balance.Available.AmountMinor())
		assert.Equal(t, journalBalance(t, ctx, id), balance.Available.AmountMinor())
	}

	assertGlobalInvariant(t, ctx, sharedPool)
}

// TestPostTransaction_RandomTransactionsStayBalanced is the property test:
// ten thousand randomly shaped, individually valid transactions, after which
// the global signed sum of every journal entry must still be exactly zero.
//
// Random shape, not random validity -- the point is not to check that invalid
// input is rejected, which the tables above already do, but that no combination
// of leg counts, amounts and account choices can produce a committed
// transaction that fails to balance.
func TestPostTransaction_RandomTransactionsStayBalanced(t *testing.T) {
	if testing.Short() {
		t.Skip("10,000 posted transactions is minutes of round trips; run without -short")
	}
	t.Parallel()
	ctx := context.Background()

	svc := newLedgerService(sharedPool)

	const (
		transactions = 10_000
		accountCount = 8
		workers      = 16
	)

	types := []ledger.AccountType{
		ledger.AccountTypeAsset,
		ledger.AccountTypeLiability,
		ledger.AccountTypeRevenue,
		ledger.AccountTypeExpense,
	}

	accounts := make([]uuid.UUID, accountCount)
	for i := range accounts {
		accounts[i] = newTypedAccount(t, ctx, sharedPool, types[i%len(types)], "INR", true)
	}

	work := make(chan uint64)
	group, groupCtx := errgroup.WithContext(ctx)

	for w := range workers {
		group.Go(func() error {
			rng := rand.New(rand.NewPCG(uint64(w)+1, 0xC0FFEE))

			for range work {
				if _, err := svc.PostTransaction(groupCtx, randomTransaction(rng, accounts)); err != nil {
					return err
				}
			}
			return nil
		})
	}

	for i := range uint64(transactions) {
		work <- i
	}
	close(work)
	require.NoError(t, group.Wait())

	// The invariant, asserted across the entire journal rather than only the
	// rows this test wrote.
	assertGlobalInvariant(t, ctx, sharedPool)

	var posted int
	require.NoError(t, sharedPool.QueryRow(ctx, `
		SELECT count(*) FROM journal_entries WHERE account_id = ANY($1::uuid[])`,
		accounts).Scan(&posted))
	assert.GreaterOrEqual(t, posted, transactions*2, "every transaction writes at least two legs")

	// Every account's stored balance must still agree with the journal it was
	// derived from, after ten thousand concurrent updates.
	for i, account := range accounts {
		balance, err := svc.GetBalance(ctx, account)
		require.NoError(t, err)
		assert.Equal(t, journalBalance(t, ctx, account), balance.Available.AmountMinor(),
			"account %d: balance drifted from the journal", i)
	}
}

// randomTransaction builds a valid transaction: two to four legs, random
// amounts, and a final leg sized to make the whole thing balance.
func randomTransaction(rng *rand.Rand, accounts []uuid.UUID) ledger.TransactionRequest {
	legs := 2 + rng.IntN(3)

	entries := make([]ledger.EntryRequest, 0, legs+1)
	var total int64

	for range legs - 1 {
		amount := int64(rng.IntN(10_000) + 1)
		total += amount
		entries = append(entries, ledger.EntryRequest{
			AccountID: accounts[rng.IntN(len(accounts))],
			Direction: ledger.DirectionDebit,
			Amount:    ledger.MustNewMoney(amount, "INR"),
		})
	}

	// One credit closing the transaction out. Balanced by construction, so any
	// imbalance found afterwards came from the system, not from the generator.
	entries = append(entries, ledger.EntryRequest{
		AccountID: accounts[rng.IntN(len(accounts))],
		Direction: ledger.DirectionCredit,
		Amount:    ledger.MustNewMoney(total, "INR"),
	})

	return ledger.TransactionRequest{
		Type:    ledger.TransactionTypeAdjustment,
		Entries: entries,
	}
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

// fund moves an opening balance into an account through the real posting path,
// so that no test sets up its fixtures by a route the system does not use.
func fund(t *testing.T, ctx context.Context, svc *ledger.Service, from, to uuid.UUID, amount int64) {
	t.Helper()

	fromDirection, toDirection := ledger.DirectionCredit, ledger.DirectionDebit
	if isCreditNormal(t, ctx, to) {
		fromDirection, toDirection = ledger.DirectionDebit, ledger.DirectionCredit
	}

	_, err := svc.PostTransaction(ctx, ledger.TransactionRequest{
		Type: ledger.TransactionTypePayin,
		Entries: []ledger.EntryRequest{
			{AccountID: from, Direction: fromDirection, Amount: ledger.MustNewMoney(amount, "INR")},
			{AccountID: to, Direction: toDirection, Amount: ledger.MustNewMoney(amount, "INR")},
		},
	})
	require.NoError(t, err, "fund account %s", to)
}

func isCreditNormal(t *testing.T, ctx context.Context, account uuid.UUID) bool {
	t.Helper()

	var normalBalance string
	require.NoError(t, sharedPool.QueryRow(ctx,
		`SELECT normal_balance FROM accounts WHERE id = $1`, account).Scan(&normalBalance))
	return normalBalance == "CREDIT"
}

// journalBalance recomputes an account's balance straight from the journal,
// using the account's own normal balance for the sign. It is deliberately a
// separate implementation from the one in internal/ledger: a test that reused
// the production signing would agree with it even when both were wrong.
func journalBalance(t *testing.T, ctx context.Context, account uuid.UUID) int64 {
	t.Helper()

	var balance int64
	require.NoError(t, sharedPool.QueryRow(ctx, `
		SELECT COALESCE(SUM(CASE WHEN je.direction = a.normal_balance
		                         THEN je.amount_minor ELSE -je.amount_minor END), 0)
		  FROM accounts a
		  LEFT JOIN journal_entries je ON je.account_id = a.id
		 WHERE a.id = $1`, account).Scan(&balance))
	return balance
}

func setStatus(t *testing.T, ctx context.Context, account uuid.UUID, status string) {
	t.Helper()

	_, err := sharedPool.Exec(ctx,
		`UPDATE accounts SET status = $2 WHERE id = $1`, account, status)
	require.NoError(t, err, "set account %s status to %s", account, status)
}

func countEntries(t *testing.T, ctx context.Context, entries []ledger.EntryRequest) int {
	t.Helper()

	ids := make([]uuid.UUID, 0, len(entries))
	for _, e := range entries {
		ids = append(ids, e.AccountID)
	}

	var count int
	require.NoError(t, sharedPool.QueryRow(ctx,
		`SELECT count(*) FROM journal_entries WHERE account_id = ANY($1::uuid[])`, ids).Scan(&count))
	return count
}

func strPtr(s string) *string { return &s }
