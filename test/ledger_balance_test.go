package test

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/satyamsipah/ledger-core/internal/ledger"
)

// TestGetBalanceAsOf_ReplaysTheJournal covers the temporal query. Each instant
// must report what the account held then, not what it holds now.
func TestGetBalanceAsOf_ReplaysTheJournal(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	svc := newLedgerService(sharedPool)

	float := newAccount(t, ctx, sharedPool, "INR", true)
	wallet := newTypedAccount(t, ctx, sharedPool, ledger.AccountTypeLiability, "INR", true)

	// Posted one at a time so each transaction gets its own created_at; the
	// assertions below turn on being able to name an instant between them.
	first := transfer(t, ctx, svc, float, wallet, 100_00)
	second := transfer(t, ctx, svc, float, wallet, 250_00)
	third := transfer(t, ctx, svc, wallet, float, 50_00)

	tests := []struct {
		name string
		at   time.Time
		want int64
	}{
		{
			name: "should report nothing when the instant precedes every entry",
			at:   first.CreatedAt.Add(-time.Millisecond), want: 0,
		},
		{
			name: "should include an entry stamped exactly at the instant",
			at:   first.CreatedAt, want: 100_00,
		},
		{
			name: "should report the running total when the instant falls between transactions",
			at:   second.CreatedAt.Add(-time.Microsecond), want: 100_00,
		},
		{
			name: "should accumulate every credit up to the instant",
			at:   second.CreatedAt, want: 350_00,
		},
		{
			name: "should subtract a debit from a credit-normal account",
			at:   third.CreatedAt, want: 300_00,
		},
		{
			name: "should report the current balance when the instant is in the future",
			at:   third.CreatedAt.Add(time.Hour), want: 300_00,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			got, err := svc.GetBalanceAsOf(ctx, wallet, tc.at)
			require.NoError(t, err)
			assert.Equal(t, tc.want, got.AmountMinor())
			assert.Equal(t, "INR", got.Currency())
		})
	}

	t.Run("should agree with the synchronous balance at the present instant", func(t *testing.T) {
		t.Parallel()

		// The two are derived by completely different routes -- one accumulated
		// under a row lock as each transaction posted, the other summed from the
		// journal afterwards. They agreeing is the property the reconciliation
		// engine will eventually be built to watch.
		stored, err := svc.GetBalance(ctx, wallet)
		require.NoError(t, err)

		replayed, err := svc.GetBalanceAsOf(ctx, wallet, time.Now().Add(time.Hour))
		require.NoError(t, err)

		assert.Equal(t, stored.Available.AmountMinor(), replayed.AmountMinor())
	})

	t.Run("should report zero when the account has never been posted to", func(t *testing.T) {
		t.Parallel()

		empty := newAccount(t, ctx, sharedPool, "USD", false)

		got, err := svc.GetBalanceAsOf(ctx, empty, time.Now().Add(time.Hour))
		require.NoError(t, err)
		assert.Zero(t, got.AmountMinor())
		assert.Equal(t, "USD", got.Currency(),
			"an empty balance still has to know its currency")
	})

	assertGlobalInvariant(t, ctx, sharedPool)
}

// TestGetStatement_RunsTheBalanceForward covers pagination and the running
// balance together, because the interesting failure is at the seam between two
// pages rather than inside either one.
func TestGetStatement_RunsTheBalanceForward(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	svc := newLedgerService(sharedPool)

	float := newAccount(t, ctx, sharedPool, "INR", true)
	wallet := newTypedAccount(t, ctx, sharedPool, ledger.AccountTypeLiability, "INR", true)

	amounts := []int64{100_00, 250_00, 75_00, 400_00, 25_00}
	posted := make([]*ledger.Transaction, 0, len(amounts))
	for i, amount := range amounts {
		// Alternating direction, so the running balance moves both ways and a
		// sign error cannot hide behind a monotonically rising total.
		if i%2 == 0 {
			posted = append(posted, transfer(t, ctx, svc, float, wallet, amount))
		} else {
			posted = append(posted, transfer(t, ctx, svc, wallet, float, amount))
		}
	}

	from := posted[0].CreatedAt.Add(-time.Second)
	to := posted[len(posted)-1].CreatedAt.Add(time.Second)

	t.Run("should return every line in one page when the limit allows", func(t *testing.T) {
		t.Parallel()

		statement, err := svc.GetStatement(ctx, ledger.StatementQuery{
			AccountID: wallet, From: from, To: to, Limit: 50,
		})
		require.NoError(t, err)

		require.Len(t, statement.Lines, len(amounts))
		assert.Equal(t, "INR", statement.Currency)
		assert.Zero(t, statement.Opening.AmountMinor(), "nothing preceded the period")
		assert.Nil(t, statement.NextCursor, "a complete page has nothing after it")

		running := statement.Opening.AmountMinor()
		for i, line := range statement.Lines {
			running += line.Signed.AmountMinor()
			assert.Equal(t, running, line.RunningBalance.AmountMinor(),
				"line %d: running balance must be the opening balance plus every signed amount so far", i)
			assert.Equal(t, wallet, line.Entry.AccountID)
		}

		assert.Equal(t, running, statement.Closing.AmountMinor())

		stored, err := svc.GetBalance(ctx, wallet)
		require.NoError(t, err)
		assert.Equal(t, stored.Available.AmountMinor(), statement.Closing.AmountMinor(),
			"a full statement must close on the account's actual balance")
	})

	t.Run("should stay continuous when paged two lines at a time", func(t *testing.T) {
		t.Parallel()

		var (
			seen   []ledger.StatementLine
			cursor *ledger.StatementCursor
			pages  int
		)
		for {
			page, err := svc.GetStatement(ctx, ledger.StatementQuery{
				AccountID: wallet, From: from, To: to, Limit: 2, After: cursor,
			})
			require.NoError(t, err)
			pages++
			require.LessOrEqual(t, pages, len(amounts)+1, "pagination must terminate")

			if len(seen) > 0 {
				assert.Equal(t, seen[len(seen)-1].RunningBalance.AmountMinor(),
					page.Opening.AmountMinor(),
					"each page must open where the previous one closed")
			}

			// The running balance is continuous across pages, not restarted.
			running := page.Opening.AmountMinor()
			for _, line := range page.Lines {
				running += line.Signed.AmountMinor()
				assert.Equal(t, running, line.RunningBalance.AmountMinor())
			}

			seen = append(seen, page.Lines...)

			if page.NextCursor == nil {
				assert.Equal(t, running, page.Closing.AmountMinor())
				break
			}
			cursor = page.NextCursor
		}

		require.Len(t, seen, len(amounts), "paging must return every line exactly once")

		// No repeats and no gaps: entry ids are unique, so a duplicate would
		// show up here as a shorter set.
		ids := make(map[string]struct{}, len(seen))
		for _, line := range seen {
			ids[line.Entry.ID.String()] = struct{}{}
		}
		assert.Len(t, ids, len(amounts))

		stored, err := svc.GetBalance(ctx, wallet)
		require.NoError(t, err)
		assert.Equal(t, stored.Available.AmountMinor(),
			seen[len(seen)-1].RunningBalance.AmountMinor())
	})

	t.Run("should narrow to the requested period", func(t *testing.T) {
		t.Parallel()

		// From the second transaction onwards. The opening balance must then be
		// the first transaction's effect rather than zero.
		statement, err := svc.GetStatement(ctx, ledger.StatementQuery{
			AccountID: wallet,
			From:      posted[1].CreatedAt,
			To:        to,
			Limit:     50,
		})
		require.NoError(t, err)

		assert.Len(t, statement.Lines, len(amounts)-1)
		assert.Equal(t, amounts[0], statement.Opening.AmountMinor(),
			"the opening balance carries everything before the period")
	})

	t.Run("should return an empty page with a balance when the period holds nothing", func(t *testing.T) {
		t.Parallel()

		statement, err := svc.GetStatement(ctx, ledger.StatementQuery{
			AccountID: wallet,
			From:      to.Add(time.Hour),
			To:        to.Add(2 * time.Hour),
			Limit:     50,
		})
		require.NoError(t, err)

		assert.Empty(t, statement.Lines)
		assert.Nil(t, statement.NextCursor)
		assert.Equal(t, "INR", statement.Currency, "an empty statement still knows its currency")

		stored, err := svc.GetBalance(ctx, wallet)
		require.NoError(t, err)
		assert.Equal(t, stored.Available.AmountMinor(), statement.Opening.AmountMinor(),
			"a period after all activity opens on the current balance")
		assert.Equal(t, statement.Opening.AmountMinor(), statement.Closing.AmountMinor())
	})

	assertGlobalInvariant(t, ctx, sharedPool)
}

// TestBalanceQueries_RejectUnknownAccounts keeps a missing account from reading
// as an empty one. The two are different bugs and only one of them is visible.
func TestBalanceQueries_RejectUnknownAccounts(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	svc := newLedgerService(sharedPool)
	missing := mustUUIDv7(t)

	t.Run("should reject the read when GetBalance names an unknown account", func(t *testing.T) {
		t.Parallel()

		_, err := svc.GetBalance(ctx, missing)
		require.ErrorIs(t, err, ledger.ErrAccountNotFound)
	})

	t.Run("should reject the read when GetBalanceAsOf names an unknown account", func(t *testing.T) {
		t.Parallel()

		_, err := svc.GetBalanceAsOf(ctx, missing, time.Now())
		require.ErrorIs(t, err, ledger.ErrAccountNotFound)
	})

	t.Run("should reject the read when GetStatement names an unknown account", func(t *testing.T) {
		t.Parallel()

		_, err := svc.GetStatement(ctx, ledger.StatementQuery{
			AccountID: missing,
			From:      time.Now().Add(-time.Hour),
			To:        time.Now(),
		})
		require.ErrorIs(t, err, ledger.ErrAccountNotFound)
	})

	t.Run("should reject the read when the statement period runs backwards", func(t *testing.T) {
		t.Parallel()

		now := time.Now()
		_, err := svc.GetStatement(ctx, ledger.StatementQuery{
			AccountID: newAccount(t, ctx, sharedPool, "INR", false),
			From:      now,
			To:        now.Add(-time.Hour),
		})
		require.ErrorIs(t, err, ledger.ErrInvalidEntry)
	})
}

// transfer posts a two-leg transaction and returns it, so callers can use the
// database-assigned created_at as the instant to query at. Naming the accounts
// by the side they take, rather than as "from" and "to", is what keeps the
// expected sign of each balance readable at the call site: which account rises
// depends on its normal balance, not on the direction alone.
func transfer(
	t *testing.T,
	ctx context.Context,
	svc *ledger.Service,
	debitAccount, creditAccount uuid.UUID,
	amount int64,
) *ledger.Transaction {
	t.Helper()

	posted, err := svc.PostTransaction(ctx, ledger.TransactionRequest{
		Type: ledger.TransactionTypeTransfer,
		Entries: []ledger.EntryRequest{
			{AccountID: debitAccount, Direction: ledger.DirectionDebit, Amount: ledger.MustNewMoney(amount, "INR")},
			{AccountID: creditAccount, Direction: ledger.DirectionCredit, Amount: ledger.MustNewMoney(amount, "INR")},
		},
	})
	require.NoError(t, err, "transfer %d from %s to %s", amount, debitAccount, creditAccount)
	return posted
}
