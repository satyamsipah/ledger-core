package ledger

import (
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestSignedAmount pins the account sign convention.
//
// The four direction/normal-balance combinations are the whole of it, and two
// of them are the ones that matter: a CREDIT to a CREDIT-normal account
// increases it, and a DEBIT to a CREDIT-normal account decreases it. Get those
// backwards and every customer wallet in the system reports the negative of its
// true balance while every asset account still looks correct -- which is why
// this table covers liabilities at all.
func TestSignedAmount(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		direction     Direction
		normalBalance Direction
		amount        int64
		want          int64
		wantErr       error
	}{
		{
			name:      "should increase the balance when a debit lands on a debit-normal account",
			direction: DirectionDebit, normalBalance: DirectionDebit, amount: 1250, want: 1250,
		},
		{
			name:      "should decrease the balance when a credit lands on a debit-normal account",
			direction: DirectionCredit, normalBalance: DirectionDebit, amount: 1250, want: -1250,
		},
		{
			name:      "should increase the balance when a credit lands on a credit-normal account",
			direction: DirectionCredit, normalBalance: DirectionCredit, amount: 1250, want: 1250,
		},
		{
			name:      "should decrease the balance when a debit lands on a credit-normal account",
			direction: DirectionDebit, normalBalance: DirectionCredit, amount: 1250, want: -1250,
		},
		{
			name:      "should reject the entry when the amount is zero",
			direction: DirectionDebit, normalBalance: DirectionDebit, amount: 0, wantErr: ErrInvalidEntry,
		},
		{
			name:      "should reject the entry when the amount is negative",
			direction: DirectionDebit, normalBalance: DirectionDebit, amount: -1, wantErr: ErrInvalidEntry,
		},
		{
			name:      "should reject the entry when the direction is unknown",
			direction: Direction("SIDEWAYS"), normalBalance: DirectionDebit, amount: 1, wantErr: ErrInvalidEntry,
		},
		{
			name:      "should reject the entry when the normal balance is unknown",
			direction: DirectionDebit, normalBalance: Direction(""), amount: 1, wantErr: ErrInvalidEntry,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			got, err := signedAmount(tc.direction, tc.normalBalance, tc.amount)
			if tc.wantErr != nil {
				require.ErrorIs(t, err, tc.wantErr)
				return
			}

			require.NoError(t, err)
			assert.Equal(t, tc.want, got)
		})
	}
}

// TestSignedAmountSumsToZeroAcrossATransfer is the property that makes the
// convention safe: a transfer between two accounts of the SAME normal balance
// nets to zero across them, so no money is created by the signing itself.
//
// Between accounts of DIFFERENT normal balances it deliberately does not net to
// zero -- funding a wallet from a bank account increases both, because the
// platform gains an asset and simultaneously owes the user. That is the
// accounting equation working, not a bug, and the second case below records it
// so nobody later "fixes" it.
func TestSignedAmountSumsToZeroAcrossATransfer(t *testing.T) {
	t.Parallel()

	const amount = 500_00

	t.Run("should net to zero when both accounts are credit-normal", func(t *testing.T) {
		t.Parallel()

		from, err := signedAmount(DirectionDebit, DirectionCredit, amount)
		require.NoError(t, err)
		to, err := signedAmount(DirectionCredit, DirectionCredit, amount)
		require.NoError(t, err)

		assert.Zero(t, from+to, "a wallet-to-wallet transfer must conserve value")
	})

	t.Run("should net to zero when both accounts are debit-normal", func(t *testing.T) {
		t.Parallel()

		from, err := signedAmount(DirectionCredit, DirectionDebit, amount)
		require.NoError(t, err)
		to, err := signedAmount(DirectionDebit, DirectionDebit, amount)
		require.NoError(t, err)

		assert.Zero(t, from+to)
	})

	t.Run("should increase both when an asset funds a liability", func(t *testing.T) {
		t.Parallel()

		bank, err := signedAmount(DirectionDebit, DirectionDebit, amount)
		require.NoError(t, err)
		wallet, err := signedAmount(DirectionCredit, DirectionCredit, amount)
		require.NoError(t, err)

		assert.Equal(t, int64(amount), bank)
		assert.Equal(t, int64(amount), wallet)
	})
}

// TestAccountTypeNormalBalance mirrors accounts_normal_balance_matches_type_check.
// If this table and that constraint ever disagree, one of them is silently
// reporting the opposite of the truth.
func TestAccountTypeNormalBalance(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		accountType AccountType
		want        Direction
	}{
		{name: "should be debit-normal when the account is an asset", accountType: AccountTypeAsset, want: DirectionDebit},
		{name: "should be debit-normal when the account is an expense", accountType: AccountTypeExpense, want: DirectionDebit},
		{name: "should be credit-normal when the account is a liability", accountType: AccountTypeLiability, want: DirectionCredit},
		{name: "should be credit-normal when the account is equity", accountType: AccountTypeEquity, want: DirectionCredit},
		{name: "should be credit-normal when the account is revenue", accountType: AccountTypeRevenue, want: DirectionCredit},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			require.True(t, tc.accountType.Valid())
			assert.Equal(t, tc.want, tc.accountType.NormalBalance())
		})
	}

	assert.False(t, AccountType("CONTRA_ASSET").Valid(),
		"contra accounts are deliberately unrepresentable; see DECISIONS.md D8")
}

func TestDirectionOpposite(t *testing.T) {
	t.Parallel()

	assert.Equal(t, DirectionCredit, DirectionDebit.Opposite())
	assert.Equal(t, DirectionDebit, DirectionCredit.Opposite())
	assert.Equal(t, DirectionDebit, DirectionDebit.Opposite().Opposite(),
		"mirroring twice must return the original")
}

func TestAccountStatusPostable(t *testing.T) {
	t.Parallel()

	assert.True(t, AccountStatusActive.Postable())
	assert.False(t, AccountStatusFrozen.Postable())
	assert.False(t, AccountStatusClosed.Postable())
}

// TestTransactionRequestValidate covers everything the service rejects before
// it takes a single row lock.
func TestTransactionRequestValidate(t *testing.T) {
	t.Parallel()

	accountA := uuid.MustParse("01920000-0000-7000-8000-0000000000a1")
	accountB := uuid.MustParse("01920000-0000-7000-8000-0000000000b2")

	entry := func(account uuid.UUID, direction Direction, amount int64, currency string) EntryRequest {
		return EntryRequest{AccountID: account, Direction: direction, Amount: MustNewMoney(amount, currency)}
	}

	tests := []struct {
		name    string
		request TransactionRequest
		wantErr error
	}{
		{
			name: "should accept the request when two legs balance",
			request: TransactionRequest{
				Type: TransactionTypeTransfer,
				Entries: []EntryRequest{
					entry(accountA, DirectionDebit, 500_00, "INR"),
					entry(accountB, DirectionCredit, 500_00, "INR"),
				},
			},
		},
		{
			name: "should accept the request when many legs balance",
			request: TransactionRequest{
				Type: TransactionTypePayin,
				Entries: []EntryRequest{
					entry(accountA, DirectionDebit, 500_00, "INR"),
					entry(accountB, DirectionCredit, 300_00, "INR"),
					entry(accountB, DirectionCredit, 200_00, "INR"),
				},
			},
		},
		{
			name: "should reject the request when it carries a single leg",
			request: TransactionRequest{
				Type:    TransactionTypeTransfer,
				Entries: []EntryRequest{entry(accountA, DirectionDebit, 500_00, "INR")},
			},
			wantErr: ErrTooFewEntries,
		},
		{
			name:    "should reject the request when it carries no legs",
			request: TransactionRequest{Type: TransactionTypeTransfer},
			wantErr: ErrTooFewEntries,
		},
		{
			name: "should reject the request when debits exceed credits",
			request: TransactionRequest{
				Type: TransactionTypeTransfer,
				Entries: []EntryRequest{
					entry(accountA, DirectionDebit, 500_01, "INR"),
					entry(accountB, DirectionCredit, 500_00, "INR"),
				},
			},
			wantErr: ErrUnbalancedTransaction,
		},
		{
			name: "should reject the request when credits exceed debits",
			request: TransactionRequest{
				Type: TransactionTypeTransfer,
				Entries: []EntryRequest{
					entry(accountA, DirectionDebit, 500_00, "INR"),
					entry(accountB, DirectionCredit, 500_01, "INR"),
				},
			},
			wantErr: ErrUnbalancedTransaction,
		},
		{
			name: "should reject the request when the legs span two currencies",
			request: TransactionRequest{
				Type: TransactionTypeFX,
				Entries: []EntryRequest{
					entry(accountA, DirectionDebit, 500_00, "INR"),
					entry(accountB, DirectionCredit, 500_00, "USD"),
				},
			},
			wantErr: ErrMixedCurrency,
		},
		{
			name: "should reject the request when an amount is zero",
			request: TransactionRequest{
				Type: TransactionTypeTransfer,
				Entries: []EntryRequest{
					entry(accountA, DirectionDebit, 0, "INR"),
					entry(accountB, DirectionCredit, 0, "INR"),
				},
			},
			wantErr: ErrInvalidEntry,
		},
		{
			name: "should reject the request when an amount is negative",
			request: TransactionRequest{
				Type: TransactionTypeTransfer,
				Entries: []EntryRequest{
					entry(accountA, DirectionDebit, -500_00, "INR"),
					entry(accountB, DirectionCredit, -500_00, "INR"),
				},
			},
			wantErr: ErrInvalidEntry,
		},
		{
			name: "should reject the request when a direction is unknown",
			request: TransactionRequest{
				Type: TransactionTypeTransfer,
				Entries: []EntryRequest{
					{AccountID: accountA, Direction: Direction("SIDEWAYS"), Amount: MustNewMoney(500_00, "INR")},
					entry(accountB, DirectionCredit, 500_00, "INR"),
				},
			},
			wantErr: ErrInvalidEntry,
		},
		{
			name: "should reject the request when an account id is missing",
			request: TransactionRequest{
				Type: TransactionTypeTransfer,
				Entries: []EntryRequest{
					entry(uuid.Nil, DirectionDebit, 500_00, "INR"),
					entry(accountB, DirectionCredit, 500_00, "INR"),
				},
			},
			wantErr: ErrInvalidEntry,
		},
		{
			name: "should reject the request when the transaction type is unknown",
			request: TransactionRequest{
				Type: TransactionType("GIFT"),
				Entries: []EntryRequest{
					entry(accountA, DirectionDebit, 500_00, "INR"),
					entry(accountB, DirectionCredit, 500_00, "INR"),
				},
			},
			wantErr: ErrInvalidTransactionType,
		},
		{
			name: "should reject the request when a leg carries no currency",
			request: TransactionRequest{
				Type: TransactionTypeTransfer,
				Entries: []EntryRequest{
					{AccountID: accountA, Direction: DirectionDebit},
					{AccountID: accountB, Direction: DirectionCredit},
				},
			},
			wantErr: ErrInvalidCurrency,
		},
		{
			name: "should reject the request when the debit total overflows int64",
			request: TransactionRequest{
				Type: TransactionTypeTransfer,
				Entries: []EntryRequest{
					entry(accountA, DirectionDebit, 1<<62, "INR"),
					entry(accountA, DirectionDebit, 1<<62, "INR"),
					entry(accountB, DirectionCredit, 1<<62, "INR"),
					entry(accountB, DirectionCredit, 1<<62, "INR"),
				},
			},
			wantErr: ErrMoneyOverflow,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			err := tc.request.validate()
			if tc.wantErr != nil {
				require.ErrorIs(t, err, tc.wantErr)
				return
			}
			require.NoError(t, err)
		})
	}
}

// TestSortedAccountIDs covers the deadlock prevention: whatever order a client
// lists accounts in, the lock order is the same.
func TestSortedAccountIDs(t *testing.T) {
	t.Parallel()

	a := uuid.MustParse("01920000-0000-7000-8000-00000000000a")
	b := uuid.MustParse("01920000-0000-7000-8000-00000000000b")
	c := uuid.MustParse("01920000-0000-7000-8000-00000000000c")

	forward := sortedAccountIDs([]EntryRequest{
		{AccountID: a}, {AccountID: b}, {AccountID: c},
	})
	reverse := sortedAccountIDs([]EntryRequest{
		{AccountID: c}, {AccountID: b}, {AccountID: a},
	})

	assert.Equal(t, []uuid.UUID{a, b, c}, forward)
	assert.Equal(t, forward, reverse,
		"the lock order must not depend on the order the client listed accounts in")

	t.Run("should list each account once when a transaction touches it twice", func(t *testing.T) {
		t.Parallel()

		got := sortedAccountIDs([]EntryRequest{
			{AccountID: b}, {AccountID: a}, {AccountID: b}, {AccountID: a},
		})
		assert.Equal(t, []uuid.UUID{a, b}, got)
	})
}
