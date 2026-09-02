package test

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/satyamsipah/ledger-core/internal/ledger"
	"github.com/satyamsipah/ledger-core/internal/reconciliation"
	"github.com/satyamsipah/ledger-core/internal/reconciliation/pgreconciliation"
)

// newReconciliationEngine builds an engine over the shared database, with a
// one-hour auto-resolve window so the timing-difference cases below have a
// gap comfortably on either side of it.
func newReconciliationEngine(t *testing.T) *reconciliation.Engine {
	t.Helper()
	store := pgreconciliation.New(sharedPool, 30*time.Second)
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	return reconciliation.NewEngine(store, logger, nil, time.Hour, 7*24*time.Hour)
}

// reconciliationTxn posts a real, balanced transfer carrying externalRef, the
// same write path every other test in this package uses -- there is no
// shortcut here, because what this test is actually proving is that Match
// reads back what PostTransaction really wrote.
func reconciliationTxn(t *testing.T, ctx context.Context, svc *ledger.Service, externalRef string, amountMinor int64, from, to uuid.UUID) *ledger.Transaction {
	t.Helper()
	ref := externalRef
	tx, err := svc.PostTransaction(ctx, ledger.TransactionRequest{
		Type:        ledger.TransactionTypeTransfer,
		ExternalRef: &ref,
		Entries: []ledger.EntryRequest{
			{AccountID: from, Direction: ledger.DirectionDebit, Amount: ledger.MustNewMoney(amountMinor, "INR")},
			{AccountID: to, Direction: ledger.DirectionCredit, Amount: ledger.MustNewMoney(amountMinor, "INR")},
		},
	})
	require.NoError(t, err, "post reconciliation test transaction")
	return tx
}

// reconciliationSaga inserts a completed saga carrying externalRef in its
// payload, directly -- there is no saga.Store method for "insert an arbitrary
// finished saga", and this test only needs the row to exist for Match's own
// join, not to exercise the orchestrator.
func reconciliationSaga(t *testing.T, ctx context.Context, externalRef string) uuid.UUID {
	t.Helper()
	id := mustUUIDv7(t)
	payload := fmt.Sprintf(`{"external_ref":%q}`, externalRef)
	_, err := sharedPool.Exec(ctx, `
		INSERT INTO saga_instances (id, saga_type, current_step, status, payload, step_deadline_at)
		VALUES ($1, 'MARKETPLACE_PAYOUT', 'DONE', 'COMPLETED', $2::jsonb, now())`,
		id, payload)
	require.NoError(t, err, "insert reconciliation test saga")
	return id
}

// exceptionsByRef indexes a run's exceptions by external_ref, since the
// shared database carries every other test's external-ref'd transactions
// too -- Match's MISSING_IN_PSP side scans the whole lookback window on
// purpose (see Store.Match's doc comment), so a run in this suite always
// returns more than this test's own rows. Asserting on the map entry for a
// ref this test generated (a fresh UUID every time) is what makes the
// assertions correct regardless of what else is in the table.
func exceptionsByRef(exceptions []reconciliation.Exception) map[string]reconciliation.Exception {
	byRef := make(map[string]reconciliation.Exception, len(exceptions))
	for _, exc := range exceptions {
		byRef[exc.ExternalRef] = exc
	}
	return byRef
}

// TestReconciliation_ThreeWayMatch drives every category the phase requires
// through one run, each keyed on its own fresh external_ref so the scenarios
// cannot interfere with each other or with the rest of the suite.
func TestReconciliation_ThreeWayMatch(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	svc := newLedgerService(sharedPool)
	a := newAccount(t, ctx, sharedPool, "INR", true)
	b := newAccount(t, ctx, sharedPool, "INR", true)

	ref := func(name string) string { return "recon-" + name + "-" + uuid.NewString() }

	// Clean match: amount, status and settlement instant all agree exactly.
	cleanRef := ref("clean")
	cleanTx := reconciliationTxn(t, ctx, svc, cleanRef, 1000, a, b)
	require.NotNil(t, cleanTx.PostedAt)

	// Amount mismatch.
	amountRef := ref("amount")
	reconciliationTxn(t, ctx, svc, amountRef, 1000, a, b)

	// Status mismatch: posted in the ledger, FAILED at the PSP.
	statusRef := ref("status")
	reconciliationTxn(t, ctx, svc, statusRef, 1000, a, b)

	// Missing in PSP: the ledger has it, the statement never mentions it.
	missingPSPRef := ref("missing-psp")
	reconciliationTxn(t, ctx, svc, missingPSPRef, 1000, a, b)

	// Missing in ledger: the statement mentions it, no transaction carries it.
	missingLedgerRef := ref("missing-ledger")

	// Timing difference, inside the one-hour window: auto-resolved.
	timingAutoRef := ref("timing-auto")
	timingAutoTx := reconciliationTxn(t, ctx, svc, timingAutoRef, 1000, a, b)
	require.NotNil(t, timingAutoTx.PostedAt)

	// Timing difference, outside the window: left open.
	timingOpenRef := ref("timing-open")
	timingOpenTx := reconciliationTxn(t, ctx, svc, timingOpenRef, 1000, a, b)
	require.NotNil(t, timingOpenTx.PostedAt)

	// Duplicate: the statement lists this reference twice.
	duplicateRef := ref("duplicate")

	// A saga-linked reference with a genuine mismatch, so SagaID surfaces on
	// the resulting exception.
	sagaRef := ref("saga")
	reconciliationSaga(t, ctx, sagaRef)
	reconciliationTxn(t, ctx, svc, sagaRef, 1000, a, b)

	psp := []reconciliation.PSPRecord{
		{ExternalRef: cleanRef, AmountMinor: 1000, Currency: "INR", Status: "SETTLED", SettledAt: *cleanTx.PostedAt},
		{ExternalRef: amountRef, AmountMinor: 999, Currency: "INR", Status: "SETTLED", SettledAt: time.Now()},
		{ExternalRef: statusRef, AmountMinor: 1000, Currency: "INR", Status: "FAILED", SettledAt: time.Now()},
		{ExternalRef: missingLedgerRef, AmountMinor: 1000, Currency: "INR", Status: "SETTLED", SettledAt: time.Now()},
		{ExternalRef: timingAutoRef, AmountMinor: 1000, Currency: "INR", Status: "SETTLED", SettledAt: timingAutoTx.PostedAt.Add(30 * time.Minute)},
		{ExternalRef: timingOpenRef, AmountMinor: 1000, Currency: "INR", Status: "SETTLED", SettledAt: timingOpenTx.PostedAt.Add(3 * time.Hour)},
		{ExternalRef: duplicateRef, AmountMinor: 500, Currency: "INR", Status: "SETTLED", SettledAt: time.Now()},
		{ExternalRef: duplicateRef, AmountMinor: 600, Currency: "INR", Status: "SETTLED", SettledAt: time.Now().Add(time.Minute)},
		{ExternalRef: sagaRef, AmountMinor: 1, Currency: "INR", Status: "SETTLED", SettledAt: time.Now()},
	}
	// missingPSPRef is deliberately absent from the statement.

	engine := newReconciliationEngine(t)
	run, err := engine.Run(ctx, "test-statement.csv", psp)
	require.NoError(t, err)

	assert.Equal(t, reconciliation.RunStatusCompleted, run.Status)
	assert.Equal(t, len(psp), run.PSPRowCount)
	assert.Equal(t, "test-statement.csv", run.Source)

	// The run report round-trips through the store, not only the in-memory
	// value Run returned -- this is what the HTTP report endpoint actually
	// reads.
	stored, err := pgreconciliation.New(sharedPool, 30*time.Second).GetRun(ctx, run.ID)
	require.NoError(t, err)
	assert.Equal(t, run.ExceptionCount, stored.ExceptionCount)
	assert.Equal(t, run.MatchedCount, stored.MatchedCount)
	assert.Equal(t, run.AutoResolvedCount, stored.AutoResolvedCount)

	exceptions, err := pgreconciliation.New(sharedPool, 30*time.Second).ListExceptions(ctx, run.ID)
	require.NoError(t, err)
	byRef := exceptionsByRef(exceptions)

	_, cleanHasException := byRef[cleanRef]
	assert.False(t, cleanHasException, "a clean match must not raise an exception")

	require.Contains(t, byRef, amountRef)
	assert.Equal(t, reconciliation.CategoryAmountMismatch, byRef[amountRef].Category)
	assert.Equal(t, reconciliation.ExceptionStatusOpen, byRef[amountRef].Status)

	require.Contains(t, byRef, statusRef)
	assert.Equal(t, reconciliation.CategoryStatusMismatch, byRef[statusRef].Category)

	require.Contains(t, byRef, missingPSPRef)
	assert.Equal(t, reconciliation.CategoryMissingInPSP, byRef[missingPSPRef].Category)

	require.Contains(t, byRef, missingLedgerRef)
	assert.Equal(t, reconciliation.CategoryMissingInLedger, byRef[missingLedgerRef].Category)

	require.Contains(t, byRef, timingAutoRef)
	assert.Equal(t, reconciliation.CategoryTimingDifference, byRef[timingAutoRef].Category)
	assert.Equal(t, reconciliation.ExceptionStatusAutoResolved, byRef[timingAutoRef].Status)
	assert.NotNil(t, byRef[timingAutoRef].ResolvedAt)

	require.Contains(t, byRef, timingOpenRef)
	assert.Equal(t, reconciliation.CategoryTimingDifference, byRef[timingOpenRef].Category)
	assert.Equal(t, reconciliation.ExceptionStatusOpen, byRef[timingOpenRef].Status)

	require.Contains(t, byRef, duplicateRef)
	assert.Equal(t, reconciliation.CategoryDuplicate, byRef[duplicateRef].Category)
	assert.Equal(t, float64(2), byRef[duplicateRef].Details["psp_row_count"])

	require.Contains(t, byRef, sagaRef)
	require.NotNil(t, byRef[sagaRef].SagaID)
	assert.Equal(t, reconciliation.CategoryAmountMismatch, byRef[sagaRef].Category)
}

// TestReconciliation_EmptyStatementIsRejected asserts that Run refuses to
// silently report "no discrepancies" for a statement that never actually
// arrived.
func TestReconciliation_EmptyStatementIsRejected(t *testing.T) {
	t.Parallel()

	engine := newReconciliationEngine(t)
	_, err := engine.Run(context.Background(), "empty.csv", nil)
	assert.ErrorIs(t, err, reconciliation.ErrEmptyStatement)
}

// TestReconciliation_GetRunReturnsNotFoundForAnUnknownID pins the HTTP-facing
// error path: a run id nobody ever created must not be indistinguishable from
// a database error.
func TestReconciliation_GetRunReturnsNotFoundForAnUnknownID(t *testing.T) {
	t.Parallel()

	store := pgreconciliation.New(sharedPool, 30*time.Second)
	_, err := store.GetRun(context.Background(), uuid.New())
	assert.ErrorIs(t, err, reconciliation.ErrRunNotFound)
}
