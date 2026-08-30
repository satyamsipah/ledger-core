package test

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/satyamsipah/ledger-core/internal/ledger"
)

// TestReverseTransaction_MirrorsWithoutMutatingHistory is invariant 2 expressed
// as behaviour rather than as a trigger: the correction is a new fact, and the
// mistake is still on the record next to it.
func TestReverseTransaction_MirrorsWithoutMutatingHistory(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	svc := newLedgerService(sharedPool)

	float := newAccount(t, ctx, sharedPool, "INR", true)
	wallet := newTypedAccount(t, ctx, sharedPool, ledger.AccountTypeLiability, "INR", true)

	original, err := svc.PostTransaction(ctx, ledger.TransactionRequest{
		Type: ledger.TransactionTypeTransfer,
		Entries: []ledger.EntryRequest{
			{AccountID: float, Direction: ledger.DirectionDebit, Amount: ledger.MustNewMoney(750_00, "INR")},
			{AccountID: wallet, Direction: ledger.DirectionCredit, Amount: ledger.MustNewMoney(750_00, "INR")},
		},
	})
	require.NoError(t, err)

	beforeFloat := journalBalance(t, ctx, float)
	beforeWallet := journalBalance(t, ctx, wallet)

	reversal, err := svc.ReverseTransaction(ctx, original.ID, "duplicate settlement file")
	require.NoError(t, err)

	t.Run("should create a new reversal transaction rather than editing the original", func(t *testing.T) {
		assert.NotEqual(t, original.ID, reversal.ID)
		assert.Equal(t, ledger.TransactionTypeReversal, reversal.Type)
		assert.Equal(t, ledger.TransactionStatusPosted, reversal.Status)
		assert.Equal(t, original.ID.String(), reversal.Metadata[ledger.MetadataKeyReverses])
		assert.Equal(t, "duplicate settlement file", reversal.Metadata[ledger.MetadataKeyReason])
	})

	t.Run("should mirror every leg", func(t *testing.T) {
		require.Len(t, reversal.Entries, len(original.Entries))
		for i, mirrored := range reversal.Entries {
			source := original.Entries[i]
			assert.Equal(t, source.AccountID, mirrored.AccountID)
			assert.True(t, source.Amount.Equal(mirrored.Amount), "amounts must be identical")
			assert.Equal(t, source.Direction.Opposite(), mirrored.Direction)
		}
	})

	t.Run("should mark the original REVERSED and change nothing else about it", func(t *testing.T) {
		var (
			status      string
			entryCount  int
			amountTotal int64
		)
		require.NoError(t, sharedPool.QueryRow(ctx,
			`SELECT status FROM transactions WHERE id = $1`, original.ID).Scan(&status))
		assert.Equal(t, "REVERSED", status)

		require.NoError(t, sharedPool.QueryRow(ctx, `
			SELECT count(*), COALESCE(SUM(amount_minor), 0)
			  FROM journal_entries WHERE transaction_id = $1`, original.ID).
			Scan(&entryCount, &amountTotal))
		assert.Equal(t, 2, entryCount, "the original's entries must survive untouched")
		assert.Equal(t, int64(750_00*2), amountTotal)
	})

	t.Run("should return both balances to where they started", func(t *testing.T) {
		assert.Equal(t, beforeFloat-750_00, journalBalance(t, ctx, float))
		assert.Equal(t, beforeWallet-750_00, journalBalance(t, ctx, wallet))

		floatBalance, err := svc.GetBalance(ctx, float)
		require.NoError(t, err)
		assert.Equal(t, journalBalance(t, ctx, float), floatBalance.Available.AmountMinor())

		walletBalance, err := svc.GetBalance(ctx, wallet)
		require.NoError(t, err)
		assert.Zero(t, walletBalance.Available.AmountMinor(),
			"reversing the only credit must leave the wallet empty")
	})

	t.Run("should emit a reversal event naming the transaction it undoes", func(t *testing.T) {
		var (
			eventType string
			payload   []byte
		)
		require.NoError(t, sharedPool.QueryRow(ctx, `
			SELECT event_type, payload FROM outbox WHERE aggregate_id = $1`, reversal.ID.String()).
			Scan(&eventType, &payload))
		assert.Equal(t, ledger.EventTypeTransactionReversed, eventType)

		var event struct {
			Reverses *uuid.UUID `json:"reverses_transaction_id"`
			Reason   string     `json:"reason"`
		}
		require.NoError(t, json.Unmarshal(payload, &event))
		require.NotNil(t, event.Reverses)
		assert.Equal(t, original.ID, *event.Reverses)
		assert.Equal(t, "duplicate settlement file", event.Reason)
	})

	assertGlobalInvariant(t, ctx, sharedPool)
}

// TestReverseTransaction_RejectsBadRequests covers every way a reversal can be
// refused.
func TestReverseTransaction_RejectsBadRequests(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	svc := newLedgerService(sharedPool)

	float := newAccount(t, ctx, sharedPool, "INR", true)
	other := newAccount(t, ctx, sharedPool, "INR", true)

	posted, err := svc.PostTransaction(ctx, ledger.TransactionRequest{
		Type: ledger.TransactionTypeTransfer,
		Entries: []ledger.EntryRequest{
			{AccountID: float, Direction: ledger.DirectionDebit, Amount: ledger.MustNewMoney(100_00, "INR")},
			{AccountID: other, Direction: ledger.DirectionCredit, Amount: ledger.MustNewMoney(100_00, "INR")},
		},
	})
	require.NoError(t, err)

	// A PENDING header written directly: the saga writes one of these before its
	// legs exist, and reversing it is a cancellation, not a correction.
	pending := mustUUIDv7(t)
	_, err = sharedPool.Exec(ctx,
		`INSERT INTO transactions (id, transaction_type, status) VALUES ($1, 'TRANSFER', 'PENDING')`,
		pending)
	require.NoError(t, err)

	t.Run("should reject the reversal when no reason is given", func(t *testing.T) {
		t.Parallel()

		_, err := svc.ReverseTransaction(ctx, posted.ID, "")
		require.ErrorIs(t, err, ledger.ErrReversalReasonRequired)
	})

	t.Run("should reject the reversal when the transaction does not exist", func(t *testing.T) {
		t.Parallel()

		_, err := svc.ReverseTransaction(ctx, mustUUIDv7(t), "no such transaction")
		require.ErrorIs(t, err, ledger.ErrTransactionNotFound)
	})

	t.Run("should reject the reversal when the transaction has not posted", func(t *testing.T) {
		t.Parallel()

		_, err := svc.ReverseTransaction(ctx, pending, "still pending")
		require.ErrorIs(t, err, ledger.ErrTransactionNotPosted)
	})

	t.Run("should reject the second reversal of the same transaction", func(t *testing.T) {
		reversible, err := svc.PostTransaction(ctx, ledger.TransactionRequest{
			Type: ledger.TransactionTypeTransfer,
			Entries: []ledger.EntryRequest{
				{AccountID: float, Direction: ledger.DirectionDebit, Amount: ledger.MustNewMoney(50_00, "INR")},
				{AccountID: other, Direction: ledger.DirectionCredit, Amount: ledger.MustNewMoney(50_00, "INR")},
			},
		})
		require.NoError(t, err)

		_, err = svc.ReverseTransaction(ctx, reversible.ID, "first reversal")
		require.NoError(t, err)

		_, err = svc.ReverseTransaction(ctx, reversible.ID, "second reversal")
		require.ErrorIs(t, err, ledger.ErrAlreadyReversed)
	})

	t.Run("should reject the reversal when it would overdraw a restricted account", func(t *testing.T) {
		// A wallet is funded, then spends everything. Reversing the funding now
		// needs money the wallet no longer has.
		wallet := newTypedAccount(t, ctx, sharedPool, ledger.AccountTypeLiability, "INR", false)
		sink := newAccount(t, ctx, sharedPool, "INR", true)

		funding, err := svc.PostTransaction(ctx, ledger.TransactionRequest{
			Type: ledger.TransactionTypePayin,
			Entries: []ledger.EntryRequest{
				{AccountID: float, Direction: ledger.DirectionDebit, Amount: ledger.MustNewMoney(400_00, "INR")},
				{AccountID: wallet, Direction: ledger.DirectionCredit, Amount: ledger.MustNewMoney(400_00, "INR")},
			},
		})
		require.NoError(t, err)

		_, err = svc.PostTransaction(ctx, ledger.TransactionRequest{
			Type: ledger.TransactionTypePayout,
			Entries: []ledger.EntryRequest{
				{AccountID: wallet, Direction: ledger.DirectionDebit, Amount: ledger.MustNewMoney(400_00, "INR")},
				{AccountID: sink, Direction: ledger.DirectionCredit, Amount: ledger.MustNewMoney(400_00, "INR")},
			},
		})
		require.NoError(t, err)

		_, err = svc.ReverseTransaction(ctx, funding.ID, "the money has already been spent")
		require.ErrorIs(t, err, ledger.ErrInsufficientFunds)

		// The refusal must be total: the original stays POSTED and no reversal
		// legs were written.
		var status string
		require.NoError(t, sharedPool.QueryRow(ctx,
			`SELECT status FROM transactions WHERE id = $1`, funding.ID).Scan(&status))
		assert.Equal(t, "POSTED", status,
			"a failed reversal must not leave the original marked REVERSED")
	})

	t.Cleanup(func() { assertGlobalInvariant(t, ctx, sharedPool) })
}

// TestReverseTransaction_ConcurrentReversals proves the status transition is the
// mechanism preventing a double reversal, not merely a check that usually wins.
//
// Twenty goroutines reverse the same transaction at once. Exactly one may
// succeed: two would refund the money twice, which the balance invariant would
// happily allow because both reversals balance perfectly on their own.
func TestReverseTransaction_ConcurrentReversals(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	svc := newLedgerService(sharedPool)

	const attempts = 20

	float := newAccount(t, ctx, sharedPool, "INR", true)
	wallet := newTypedAccount(t, ctx, sharedPool, ledger.AccountTypeLiability, "INR", true)

	posted, err := svc.PostTransaction(ctx, ledger.TransactionRequest{
		Type: ledger.TransactionTypeTransfer,
		Entries: []ledger.EntryRequest{
			{AccountID: float, Direction: ledger.DirectionDebit, Amount: ledger.MustNewMoney(300_00, "INR")},
			{AccountID: wallet, Direction: ledger.DirectionCredit, Amount: ledger.MustNewMoney(300_00, "INR")},
		},
	})
	require.NoError(t, err)

	balanceBefore, err := svc.GetBalance(ctx, wallet)
	require.NoError(t, err)

	var succeeded, rejected atomic.Int64

	var wg sync.WaitGroup
	wg.Add(attempts)
	for range attempts {
		go func() {
			defer wg.Done()

			_, err := svc.ReverseTransaction(ctx, posted.ID, "concurrent reversal")
			switch {
			case err == nil:
				succeeded.Add(1)
			case errors.Is(err, ledger.ErrAlreadyReversed):
				rejected.Add(1)
			default:
				assert.NoError(t, err, "unexpected failure mode")
			}
		}()
	}
	wg.Wait()

	assert.Equal(t, int64(1), succeeded.Load(), "exactly one reversal may commit")
	assert.Equal(t, int64(attempts-1), rejected.Load())

	balanceAfter, err := svc.GetBalance(ctx, wallet)
	require.NoError(t, err)
	assert.Equal(t, balanceBefore.Available.AmountMinor()-300_00, balanceAfter.Available.AmountMinor(),
		"the money must be refunded exactly once")

	var reversals int
	require.NoError(t, sharedPool.QueryRow(ctx, `
		SELECT count(*) FROM transactions
		 WHERE transaction_type = 'REVERSAL'
		   AND metadata ->> 'reverses_transaction_id' = $1`, posted.ID.String()).Scan(&reversals))
	assert.Equal(t, 1, reversals, "only one reversal transaction may exist")

	assertGlobalInvariant(t, ctx, sharedPool)
}
