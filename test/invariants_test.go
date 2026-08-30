package test

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgerrcode"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestBalanceInvariant_DeferredUntilCommit is the test the schema exists for.
//
// Each case posts a set of legs inside one transaction. Every INSERT is
// expected to succeed regardless of whether the set balances -- that is what
// "deferred" means -- and the verdict is delivered by COMMIT.
func TestBalanceInvariant_DeferredUntilCommit(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	inrA := newAccount(t, ctx, sharedPool, "INR", false)
	inrB := newAccount(t, ctx, sharedPool, "INR", false)
	usdA := newAccount(t, ctx, sharedPool, "USD", false)
	usdB := newAccount(t, ctx, sharedPool, "USD", false)

	tests := []struct {
		name      string
		legs      []leg
		wantError bool
	}{
		{
			name: "should accept the commit when debits equal credits",
			legs: []leg{
				{inrA, "DEBIT", 150_00, "INR"},
				{inrB, "CREDIT", 150_00, "INR"},
			},
		},
		{
			name: "should accept the commit when many legs sum to zero",
			legs: []leg{
				{inrA, "DEBIT", 100_00, "INR"},
				{inrB, "CREDIT", 60_00, "INR"},
				{inrB, "CREDIT", 40_00, "INR"},
			},
		},
		{
			name: "should reject the commit when a transaction has a single leg",
			legs: []leg{
				{inrA, "DEBIT", 100_00, "INR"},
			},
			wantError: true,
		},
		{
			name: "should reject the commit when debits exceed credits",
			legs: []leg{
				{inrA, "DEBIT", 100_01, "INR"},
				{inrB, "CREDIT", 100_00, "INR"},
			},
			wantError: true,
		},
		{
			name: "should reject the commit when credits exceed debits",
			legs: []leg{
				{inrA, "DEBIT", 100_00, "INR"},
				{inrB, "CREDIT", 100_01, "INR"},
			},
			wantError: true,
		},
		{
			name: "should accept the commit when every currency leg balances independently",
			legs: []leg{
				{inrA, "DEBIT", 830_00, "INR"},
				{inrB, "CREDIT", 830_00, "INR"},
				{usdA, "CREDIT", 10_00, "USD"},
				{usdB, "DEBIT", 10_00, "USD"},
			},
		},
		{
			name: "should reject the commit when one currency of a multi-currency transaction is unbalanced",
			legs: []leg{
				{inrA, "DEBIT", 830_00, "INR"},
				{inrB, "CREDIT", 830_00, "INR"},
				{usdA, "CREDIT", 10_00, "USD"},
				{usdB, "DEBIT", 9_99, "USD"},
			},
			wantError: true,
		},
		{
			name: "should reject the commit when currencies offset each other instead of balancing",
			legs: []leg{
				{inrA, "DEBIT", 100_00, "INR"},
				{usdA, "CREDIT", 100_00, "USD"},
			},
			wantError: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			tx, err := sharedPool.Begin(ctx)
			require.NoError(t, err)
			defer func() { _ = tx.Rollback(ctx) }()

			txID := newTransaction(t, ctx, tx)

			// Every insert must succeed even for the unbalanced cases. If one
			// of these fails, the trigger is firing per-statement and the
			// deferral has been lost.
			require.NoError(t, postLegs(ctx, tx, txID, tc.legs...),
				"inserts must succeed regardless of balance; the check is deferred to COMMIT")

			err = tx.Commit(ctx)
			if !tc.wantError {
				require.NoError(t, err, "balanced transaction must commit")
				return
			}

			require.Error(t, err, "unbalanced transaction must fail at COMMIT")

			var pgErr *pgconn.PgError
			require.True(t, errors.As(err, &pgErr), "expected a PgError, got %T: %v", err, err)
			assert.Equal(t, pgerrcode.CheckViolation, pgErr.Code)
			assert.Contains(t, pgErr.Message, txID.String(),
				"the error must name the offending transaction")

			// The whole transaction rolls back, not just the failing check.
			var entries int
			require.NoError(t, sharedPool.QueryRow(ctx,
				`SELECT count(*) FROM journal_entries WHERE transaction_id = $1`, txID).
				Scan(&entries))
			assert.Zero(t, entries, "a rejected commit must leave no entries behind")
		})
	}

	t.Cleanup(func() { assertGlobalInvariant(t, ctx, sharedPool) })
}

// TestBalanceInvariant_UnderConcurrency runs balanced and unbalanced writers
// against the same accounts simultaneously. The point is not throughput: it is
// that a deferred constraint trigger evaluated at COMMIT still sees a coherent
// view when many transactions commit at once.
func TestBalanceInvariant_UnderConcurrency(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	const (
		balancedWriters   = 60
		unbalancedWriters = 60
	)

	source := newAccount(t, ctx, sharedPool, "INR", true)
	sink := newAccount(t, ctx, sharedPool, "INR", true)

	type result struct {
		txID     uuid.UUID
		balanced bool
		err      error
	}

	var (
		mu      sync.Mutex
		results []result
		wg      sync.WaitGroup
	)

	post := func(balanced bool) {
		defer wg.Done()

		tx, err := sharedPool.Begin(ctx)
		if err != nil {
			mu.Lock()
			results = append(results, result{balanced: balanced, err: err})
			mu.Unlock()
			return
		}
		defer func() { _ = tx.Rollback(ctx) }()

		txID, err := uuid.NewV7()
		if err != nil {
			mu.Lock()
			results = append(results, result{balanced: balanced, err: err})
			mu.Unlock()
			return
		}

		credit := int64(500_00)
		if !balanced {
			// One paisa out. Nothing about the shape of the transaction looks
			// wrong; only the sum does.
			credit--
		}

		err = func() error {
			if _, err := tx.Exec(ctx,
				`INSERT INTO transactions (id, transaction_type, status) VALUES ($1, 'TRANSFER', 'PENDING')`,
				txID); err != nil {
				return err
			}
			if err := postLegs(ctx, tx, txID,
				leg{source, "DEBIT", 500_00, "INR"},
				leg{sink, "CREDIT", credit, "INR"},
			); err != nil {
				return err
			}
			return tx.Commit(ctx)
		}()

		mu.Lock()
		results = append(results, result{txID: txID, balanced: balanced, err: err})
		mu.Unlock()
	}

	wg.Add(balancedWriters + unbalancedWriters)
	for i := 0; i < balancedWriters; i++ {
		go post(true)
	}
	for i := 0; i < unbalancedWriters; i++ {
		go post(false)
	}
	wg.Wait()

	require.Len(t, results, balancedWriters+unbalancedWriters)

	var committed, rejected int
	for _, r := range results {
		if r.balanced {
			require.NoError(t, r.err, "balanced writer %s must commit under contention", r.txID)
			committed++
			continue
		}

		require.Error(t, r.err, "unbalanced writer %s must be rejected", r.txID)
		var pgErr *pgconn.PgError
		require.True(t, errors.As(r.err, &pgErr), "expected PgError, got %T", r.err)
		assert.Equal(t, pgerrcode.CheckViolation, pgErr.Code)
		rejected++

		var entries int
		require.NoError(t, sharedPool.QueryRow(ctx,
			`SELECT count(*) FROM journal_entries WHERE transaction_id = $1`, r.txID).Scan(&entries))
		assert.Zero(t, entries, "rejected transaction %s must leave no entries", r.txID)
	}

	assert.Equal(t, balancedWriters, committed)
	assert.Equal(t, unbalancedWriters, rejected)

	// The 60 accepted transfers must be the only thing that landed.
	var totalEntries int
	require.NoError(t, sharedPool.QueryRow(ctx,
		`SELECT count(*) FROM journal_entries WHERE account_id IN ($1, $2)`,
		source, sink).Scan(&totalEntries))
	assert.Equal(t, balancedWriters*2, totalEntries)

	assertGlobalInvariant(t, ctx, sharedPool)
}

// TestJournalEntries_AppendOnly covers invariant 2. Corrections happen through
// reversing entries; nothing may edit history.
func TestJournalEntries_AppendOnly(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	from := newAccount(t, ctx, sharedPool, "INR", false)
	to := newAccount(t, ctx, sharedPool, "INR", false)

	tx, err := sharedPool.Begin(ctx)
	require.NoError(t, err)
	txID := newTransaction(t, ctx, tx)
	require.NoError(t, postLegs(ctx, tx, txID,
		leg{from, "DEBIT", 42_00, "INR"},
		leg{to, "CREDIT", 42_00, "INR"},
	))
	require.NoError(t, tx.Commit(ctx))

	tests := []struct {
		name string
		stmt string
		args []any
	}{
		{
			name: "should reject the write when a committed entry is updated",
			stmt: `UPDATE journal_entries SET amount_minor = 1 WHERE transaction_id = $1`,
			args: []any{txID},
		},
		{
			name: "should reject the write when a committed entry is deleted",
			stmt: `DELETE FROM journal_entries WHERE transaction_id = $1`,
			args: []any{txID},
		},
		{
			name: "should reject the write when the journal is truncated",
			stmt: `TRUNCATE journal_entries`,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := sharedPool.Exec(ctx, tc.stmt, tc.args...)
			require.Error(t, err)

			var pgErr *pgconn.PgError
			require.True(t, errors.As(err, &pgErr), "expected PgError, got %T", err)
			assert.Equal(t, pgerrcode.RestrictViolation, pgErr.Code)
			assert.Contains(t, pgErr.Message, "append-only")
		})
	}

	// The entries survived every attempt.
	var entries int
	require.NoError(t, sharedPool.QueryRow(ctx,
		`SELECT count(*) FROM journal_entries WHERE transaction_id = $1`, txID).Scan(&entries))
	assert.Equal(t, 2, entries)

	assertGlobalInvariant(t, ctx, sharedPool)
}

// TestAccountBalances_OverdraftConstraint covers invariant 4: the CHECK that
// stops an account going negative unless it is explicitly allowed to.
func TestAccountBalances_OverdraftConstraint(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	tests := []struct {
		name          string
		allowNegative bool
		available     int64
		wantError     bool
	}{
		{
			name:          "should reject the write when a debit takes a restricted account below zero",
			allowNegative: false,
			available:     -1,
			wantError:     true,
		},
		{
			name:          "should accept the write when a restricted account lands exactly on zero",
			allowNegative: false,
			available:     0,
		},
		{
			name:          "should accept the write when the account allows a negative balance",
			allowNegative: true,
			available:     -250_00,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			account := newAccount(t, ctx, sharedPool, "INR", tc.allowNegative)

			_, err := sharedPool.Exec(ctx,
				`UPDATE account_balances SET available_minor = $2, version = version + 1
				  WHERE account_id = $1`, account, tc.available)

			if !tc.wantError {
				require.NoError(t, err)
				return
			}

			require.Error(t, err)
			var pgErr *pgconn.PgError
			require.True(t, errors.As(err, &pgErr), "expected PgError, got %T", err)
			assert.Equal(t, pgerrcode.CheckViolation, pgErr.Code)
			assert.Equal(t, "account_balances_no_overdraft_check", pgErr.ConstraintName)
		})
	}
}

// TestJournalEntries_CurrencyMustMatchAccount covers the composite foreign key.
// It is the constraint that makes "post USD into an INR wallet" unrepresentable
// rather than merely discouraged.
func TestJournalEntries_CurrencyMustMatchAccount(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	inr := newAccount(t, ctx, sharedPool, "INR", false)
	usd := newAccount(t, ctx, sharedPool, "USD", false)

	tx, err := sharedPool.Begin(ctx)
	require.NoError(t, err)
	defer func() { _ = tx.Rollback(ctx) }()

	txID := newTransaction(t, ctx, tx)

	// Balanced in USD, but one leg points at an INR account. The FK rejects it
	// immediately -- this one is not deferred, and does not need to be.
	err = postLegs(ctx, tx, txID,
		leg{usd, "DEBIT", 10_00, "USD"},
		leg{inr, "CREDIT", 10_00, "USD"},
	)
	require.Error(t, err)

	var pgErr *pgconn.PgError
	require.True(t, errors.As(err, &pgErr), "expected PgError, got %T", err)
	assert.Equal(t, pgerrcode.ForeignKeyViolation, pgErr.Code)
	assert.Equal(t, "journal_entries_account_currency_fkey", pgErr.ConstraintName)
}

// TestTransactions_IdempotencyKeyIsUnique covers the database half of
// invariant 5: even with the idempotency service bypassed entirely, two
// transactions cannot share a client key.
func TestTransactions_IdempotencyKeyIsUnique(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	key := "idem-" + mustUUIDv7(t).String()

	insert := func(ctx context.Context) error {
		id, err := uuid.NewV7()
		if err != nil {
			return err
		}
		_, err = sharedPool.Exec(ctx,
			`INSERT INTO transactions (id, idempotency_key, transaction_type, status)
			 VALUES ($1, $2, 'PAYIN', 'PENDING')`, id, key)
		return err
	}

	require.NoError(t, insert(ctx), "first use of the key must succeed")

	err := insert(ctx)
	require.Error(t, err, "second use of the same key must be rejected")

	var pgErr *pgconn.PgError
	require.True(t, errors.As(err, &pgErr), "expected PgError, got %T", err)
	assert.Equal(t, pgerrcode.UniqueViolation, pgErr.Code)

	// NULL keys must not collide with each other: reversals and internal
	// adjustments carry none, and a non-partial index would let the first one
	// block every subsequent.
	for i := 0; i < 3; i++ {
		id := mustUUIDv7(t)
		_, err := sharedPool.Exec(ctx,
			`INSERT INTO transactions (id, idempotency_key, transaction_type, status)
			 VALUES ($1, NULL, 'REVERSAL', 'PENDING')`, id)
		require.NoError(t, err, "transactions without an idempotency key must not collide")
	}
}

// TestTransactions_PostedAtTracksStatus covers the constraint that keeps status
// and posted_at from drifting apart.
func TestTransactions_PostedAtTracksStatus(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	tests := []struct {
		name      string
		status    string
		postedAt  any
		wantError bool
	}{
		{name: "should accept the write when a pending transaction has no posted_at", status: "PENDING", postedAt: nil},
		{name: "should accept the write when a posted transaction has a posted_at", status: "POSTED", postedAt: "now()"},
		{name: "should reject the write when a pending transaction carries a posted_at", status: "PENDING", postedAt: "now()", wantError: true},
		{name: "should reject the write when a posted transaction has no posted_at", status: "POSTED", postedAt: nil, wantError: true},
		{name: "should reject the write when a reversed transaction has no posted_at", status: "REVERSED", postedAt: nil, wantError: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			id := mustUUIDv7(t)
			stmt := `INSERT INTO transactions (id, transaction_type, status, posted_at)
			         VALUES ($1, 'TRANSFER', $2, NULL)`
			if tc.postedAt != nil {
				stmt = `INSERT INTO transactions (id, transaction_type, status, posted_at)
				        VALUES ($1, 'TRANSFER', $2, now())`
			}

			_, err := sharedPool.Exec(ctx, stmt, id, tc.status)
			if !tc.wantError {
				require.NoError(t, err)
				return
			}

			require.Error(t, err)
			var pgErr *pgconn.PgError
			require.True(t, errors.As(err, &pgErr), "expected PgError, got %T", err)
			assert.Equal(t, "transactions_posted_at_check", pgErr.ConstraintName)
		})
	}
}

// TestAccounts_NormalBalanceMatchesType pins the accounting rule that an
// account's normal balance follows from its type.
func TestAccounts_NormalBalanceMatchesType(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	tests := []struct {
		name          string
		accountType   string
		normalBalance string
		wantError     bool
	}{
		{name: "should accept the write when an asset account is debit-normal", accountType: "ASSET", normalBalance: "DEBIT"},
		{name: "should accept the write when a liability account is credit-normal", accountType: "LIABILITY", normalBalance: "CREDIT"},
		{name: "should accept the write when a revenue account is credit-normal", accountType: "REVENUE", normalBalance: "CREDIT"},
		{name: "should accept the write when an expense account is debit-normal", accountType: "EXPENSE", normalBalance: "DEBIT"},
		{name: "should reject the write when an asset account is credit-normal", accountType: "ASSET", normalBalance: "CREDIT", wantError: true},
		{name: "should reject the write when a liability account is debit-normal", accountType: "LIABILITY", normalBalance: "DEBIT", wantError: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			id := mustUUIDv7(t)
			_, err := sharedPool.Exec(ctx, `
				INSERT INTO accounts (id, external_ref, account_type, normal_balance, currency, status)
				VALUES ($1, $2, $3, $4, 'INR', 'ACTIVE')`,
				id, "nb-"+id.String(), tc.accountType, tc.normalBalance)

			if !tc.wantError {
				require.NoError(t, err)
				return
			}

			require.Error(t, err)
			var pgErr *pgconn.PgError
			require.True(t, errors.As(err, &pgErr), "expected PgError, got %T", err)
			assert.Equal(t, "accounts_normal_balance_matches_type_check", pgErr.ConstraintName)
		})
	}
}

// TestJournalEntries_AmountMustBePositive covers invariant 3: sign lives in
// direction, never in the amount.
func TestJournalEntries_AmountMustBePositive(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	account := newAccount(t, ctx, sharedPool, "INR", false)

	for _, amount := range []int64{0, -1, -100_00} {
		tx, err := sharedPool.Begin(ctx)
		require.NoError(t, err)

		txID := newTransaction(t, ctx, tx)
		err = postLegs(ctx, tx, txID, leg{account, "DEBIT", amount, "INR"})

		require.Error(t, err, "amount_minor = %d must be rejected", amount)
		var pgErr *pgconn.PgError
		require.True(t, errors.As(err, &pgErr), "expected PgError, got %T", err)
		assert.Equal(t, "journal_entries_amount_check", pgErr.ConstraintName)

		require.NoError(t, tx.Rollback(ctx))
	}
}
