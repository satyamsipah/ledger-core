package test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/satyamsipah/ledger-core/internal/consistency"
	"github.com/satyamsipah/ledger-core/internal/ledger"
)

// TestConsistency_Checks drives all three internal/consistency checks against
// a database this test fully owns, rather than the suite's sharedPool.
//
// Proving a check actually DETECTS a violation, not merely that it reports OK
// against healthy data, needs a genuinely unbalanced journal or a genuinely
// drifted balance sitting on disk -- and the only way to produce either is to
// write around the very safeguards (principally the deferred balance trigger)
// that make them otherwise unreachable through any real write path. Doing
// that against sharedPool would corrupt every other test in this package
// running concurrently against it. TestMigrations_RoundTrip already
// established the precedent of spinning a private container for exactly this
// class of reason ("it tears the schema down"); this test tears the SAFETY
// down instead, which is no less disqualifying for a database other tests
// depend on staying clean.
func TestConsistency_Checks(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	dsn, stop, err := startPostgres(ctx)
	require.NoError(t, err)
	t.Cleanup(func() { _ = stop(context.Background()) })

	pool, err := newPool(ctx, dsn)
	require.NoError(t, err)
	t.Cleanup(pool.Close)

	migrator, err := newMigrator(dsn)
	require.NoError(t, err)
	require.NoError(t, migrator.Up())
	t.Cleanup(func() { _, _ = migrator.Close() })

	svc := newLedgerService(pool)
	a := newAccount(t, ctx, pool, "INR", true)
	b := newAccount(t, ctx, pool, "INR", true)

	// A healthy baseline: one real, balanced transfer through the real write
	// path, so "everything reports OK" is proven against genuine data rather
	// than an empty database that has nothing to be wrong about.
	_, err = svc.PostTransaction(ctx, ledger.TransactionRequest{
		Type: ledger.TransactionTypeTransfer,
		Entries: []ledger.EntryRequest{
			{AccountID: a, Direction: ledger.DirectionDebit, Amount: ledger.MustNewMoney(1000, "INR")},
			{AccountID: b, Direction: ledger.DirectionCredit, Amount: ledger.MustNewMoney(1000, "INR")},
		},
	})
	require.NoError(t, err)

	t.Run("should report every check healthy against a database nothing has corrupted", func(t *testing.T) {
		invariant, err := consistency.CheckGlobalInvariant(ctx, pool)
		require.NoError(t, err)
		assert.True(t, invariant.OK(), "violations: %+v", invariant.Violations)

		drift, err := consistency.CheckProjectionDrift(ctx, pool)
		require.NoError(t, err)
		assert.True(t, drift.OK(), "drifted: %+v", drift.Drifted)

		orphans, err := consistency.CheckOrphans(ctx, pool)
		require.NoError(t, err)
		assert.True(t, orphans.OK())
	})

	t.Run("should detect a currency whose journal does not sum to zero", func(t *testing.T) {
		// The deferred trigger enforces invariant 1 at COMMIT for every
		// ordinary write, so proving this check catches a violation means
		// writing one the trigger never sees -- disabled here, for exactly
		// one insert, on a database this test owns alone.
		// journal_entries_no_mutation (invariant 2, append-only) is disabled
		// too, purely so this subtest can clean its own bad row up afterward
		// with a DELETE rather than leaving it to corrupt every later
		// subtest's rebuilt totals.
		_, err := pool.Exec(ctx, `ALTER TABLE journal_entries DISABLE TRIGGER journal_entries_balanced`)
		require.NoError(t, err)
		_, err = pool.Exec(ctx, `ALTER TABLE journal_entries DISABLE TRIGGER journal_entries_no_mutation`)
		require.NoError(t, err)
		t.Cleanup(func() {
			_, _ = pool.Exec(context.Background(), `ALTER TABLE journal_entries ENABLE TRIGGER journal_entries_balanced`)
			_, _ = pool.Exec(context.Background(), `ALTER TABLE journal_entries ENABLE TRIGGER journal_entries_no_mutation`)
		})

		txID := mustUUIDv7(t)
		_, err = pool.Exec(ctx, `
			INSERT INTO transactions (id, transaction_type, status, posted_at)
			VALUES ($1, 'ADJUSTMENT', 'POSTED', now())`, txID)
		require.NoError(t, err)

		entryID := mustUUIDv7(t)
		_, err = pool.Exec(ctx, `
			INSERT INTO journal_entries (id, transaction_id, account_id, direction, amount_minor, currency, entry_seq)
			VALUES ($1, $2, $3, 'DEBIT', 500, 'INR', 0)`, entryID, txID, a)
		require.NoError(t, err)

		result, err := consistency.CheckGlobalInvariant(ctx, pool)
		require.NoError(t, err)
		require.False(t, result.OK())
		require.Len(t, result.Violations, 1)
		assert.Equal(t, "INR", result.Violations[0].Currency)
		assert.Equal(t, int64(500), result.Violations[0].SignedTotal)

		// Clean up so later subtests' healthy-baseline assumptions hold: this
		// suite reuses the same database for the rest of the checks.
		_, err = pool.Exec(ctx, `DELETE FROM journal_entries WHERE id = $1`, entryID)
		require.NoError(t, err)
		_, err = pool.Exec(ctx, `DELETE FROM transactions WHERE id = $1`, txID)
		require.NoError(t, err)
		_, err = pool.Exec(ctx, `ALTER TABLE journal_entries ENABLE TRIGGER journal_entries_balanced`)
		require.NoError(t, err)
	})

	t.Run("should detect an account where account_balances disagrees with the journal", func(t *testing.T) {
		var before int64
		require.NoError(t, pool.QueryRow(ctx,
			`SELECT available_minor FROM account_balances WHERE account_id = $1`, a).Scan(&before))

		_, err := pool.Exec(ctx,
			`UPDATE account_balances SET available_minor = available_minor + 777 WHERE account_id = $1`, a)
		require.NoError(t, err)
		t.Cleanup(func() {
			_, _ = pool.Exec(context.Background(),
				`UPDATE account_balances SET available_minor = $2 WHERE account_id = $1`, a, before)
		})

		result, err := consistency.CheckProjectionDrift(ctx, pool)
		require.NoError(t, err)
		require.False(t, result.OK())

		var found *consistency.BalanceDrift
		for i := range result.Drifted {
			if result.Drifted[i].AccountID == a {
				found = &result.Drifted[i]
			}
		}
		require.NotNil(t, found, "account %s should be reported drifted", a)
		assert.Equal(t, before, found.RebuiltAvailable, "the journal itself was never touched")
		assert.Equal(t, before+777, found.LiveAvailable)
	})

	t.Run("should detect a POSTED transaction with fewer than two entries", func(t *testing.T) {
		txID := mustUUIDv7(t)
		_, err := pool.Exec(ctx, `
			INSERT INTO transactions (id, transaction_type, status, posted_at)
			VALUES ($1, 'ADJUSTMENT', 'POSTED', now())`, txID)
		require.NoError(t, err)

		result, err := consistency.CheckOrphans(ctx, pool)
		require.NoError(t, err)
		require.False(t, result.OK())
		assert.Contains(t, result.FewEntryTransactions, txID)
		assert.Empty(t, result.OrphanEntries, "no journal entry was touched, so this half must stay clean")
	})

	t.Run("should not flag a PENDING transaction with zero entries", func(t *testing.T) {
		// A PENDING header with no legs yet is a legitimate transient state --
		// see docs/DECISIONS.md's Phase 1 notes -- not a defect, so the
		// few-entries check must exclude it.
		txID := mustUUIDv7(t)
		_, err := pool.Exec(ctx, `
			INSERT INTO transactions (id, transaction_type, status) VALUES ($1, 'ADJUSTMENT', 'PENDING')`, txID)
		require.NoError(t, err)

		result, err := consistency.CheckOrphans(ctx, pool)
		require.NoError(t, err)
		assert.NotContains(t, result.FewEntryTransactions, txID)
	})
}
