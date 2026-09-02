package reconciliation

import (
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestClassify pins every classification rule against synthetic MatchedRecords
// -- no database involved, since classify's whole job is deciding a category
// from data it is handed, not fetching that data. The database-backed match
// itself is covered by test/reconciliation_test.go, against a real ledger and
// real saga rows.
func TestClassify(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 1, 15, 12, 0, 0, 0, time.UTC)
	txID := uuid.New()
	sagaID := uuid.New()

	ledgerAt := func(status string, postedAt *time.Time, amount int64) *LedgerSide {
		return &LedgerSide{TransactionID: txID, AmountMinor: amount, Currency: "INR", Status: status, PostedAt: postedAt, CreatedAt: now}
	}
	psp := func(status string, amount int64, settledAt time.Time) *PSPAggregate {
		return NewPSPAggregate(PSPRecord{ExternalRef: "REF", AmountMinor: amount, Currency: "INR", Status: status, SettledAt: settledAt}, 1)
	}

	tests := []struct {
		name         string
		record       MatchedRecord
		window       time.Duration
		wantClean    bool
		wantCategory Category
		wantStatus   ExceptionStatus
	}{
		{
			name: "should report a clean match when amount, status and timestamp all agree",
			record: MatchedRecord{ExternalRef: "REF",
				Ledger: ledgerAt("POSTED", &now, 1000), PSP: psp("SETTLED", 1000, now)},
			wantClean: true,
		},
		{
			name: "should report a clean match when the transaction has not posted and neither has the PSP",
			record: MatchedRecord{ExternalRef: "REF",
				Ledger: ledgerAt("PENDING", nil, 1000), PSP: psp("PENDING", 1000, now)},
			wantClean: true,
		},
		{
			name: "should report a duplicate when the PSP statement lists one reference more than once, ahead of any other finding",
			record: MatchedRecord{ExternalRef: "REF",
				Ledger: ledgerAt("POSTED", &now, 1000),
				PSP:    NewPSPAggregate(PSPRecord{ExternalRef: "REF", AmountMinor: 1, Currency: "INR", Status: "SETTLED", SettledAt: now}, 2)},
			wantCategory: CategoryDuplicate,
			wantStatus:   ExceptionStatusOpen,
		},
		{
			name:         "should report missing in PSP when the ledger has a transaction the statement never mentions",
			record:       MatchedRecord{ExternalRef: "REF", Ledger: ledgerAt("POSTED", &now, 1000)},
			wantCategory: CategoryMissingInPSP,
			wantStatus:   ExceptionStatusOpen,
		},
		{
			name:         "should report missing in ledger when the statement names a reference no transaction carries",
			record:       MatchedRecord{ExternalRef: "REF", PSP: psp("SETTLED", 1000, now)},
			wantCategory: CategoryMissingInLedger,
			wantStatus:   ExceptionStatusOpen,
		},
		{
			name: "should report an amount mismatch when the ledger and the PSP disagree on the amount",
			record: MatchedRecord{ExternalRef: "REF",
				Ledger: ledgerAt("POSTED", &now, 1000), PSP: psp("SETTLED", 999, now)},
			wantCategory: CategoryAmountMismatch,
			wantStatus:   ExceptionStatusOpen,
		},
		{
			name: "should report a status mismatch when a posted transaction has no settled counterpart",
			record: MatchedRecord{ExternalRef: "REF",
				Ledger: ledgerAt("POSTED", &now, 1000), PSP: psp("FAILED", 1000, now)},
			wantCategory: CategoryStatusMismatch,
			wantStatus:   ExceptionStatusOpen,
		},
		{
			name: "should accept a reversed transaction against either a refunded or a failed PSP status",
			record: MatchedRecord{ExternalRef: "REF",
				Ledger: ledgerAt("REVERSED", &now, 1000), PSP: psp("REFUNDED", 1000, now)},
			wantClean: true,
		},
		{
			name: "should auto-resolve a timing difference inside the configured window",
			record: MatchedRecord{ExternalRef: "REF",
				Ledger: ledgerAt("POSTED", &now, 1000), PSP: psp("SETTLED", 1000, now.Add(30*time.Minute))},
			window:       2 * time.Hour,
			wantCategory: CategoryTimingDifference,
			wantStatus:   ExceptionStatusAutoResolved,
		},
		{
			name: "should leave a timing difference open when the gap exceeds the configured window",
			record: MatchedRecord{ExternalRef: "REF",
				Ledger: ledgerAt("POSTED", &now, 1000), PSP: psp("SETTLED", 1000, now.Add(3*time.Hour))},
			window:       2 * time.Hour,
			wantCategory: CategoryTimingDifference,
			wantStatus:   ExceptionStatusOpen,
		},
		{
			name: "should treat the timing gap as symmetric regardless of which side is later",
			record: MatchedRecord{ExternalRef: "REF",
				Ledger: ledgerAt("POSTED", &now, 1000), PSP: psp("SETTLED", 1000, now.Add(-3*time.Hour))},
			window:       2 * time.Hour,
			wantCategory: CategoryTimingDifference,
			wantStatus:   ExceptionStatusOpen,
		},
		{
			name: "should carry the saga id through onto the exception when a saga matched the reference",
			record: MatchedRecord{ExternalRef: "REF",
				Ledger: ledgerAt("POSTED", &now, 1000),
				Saga:   &SagaSide{SagaID: sagaID, Status: "COMPLETED"},
				PSP:    psp("SETTLED", 999, now)},
			wantCategory: CategoryAmountMismatch,
			wantStatus:   ExceptionStatusOpen,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			exc, clean := classify(tt.record, tt.window, now)
			if tt.wantClean {
				assert.True(t, clean, "expected a clean match")
				assert.Nil(t, exc)
				return
			}

			require.False(t, clean)
			require.NotNil(t, exc)
			assert.Equal(t, tt.wantCategory, exc.Category)
			assert.Equal(t, tt.wantStatus, exc.Status)
			assert.Equal(t, tt.wantStatus == ExceptionStatusAutoResolved, exc.ResolvedAt != nil)

			if tt.record.Saga != nil {
				require.NotNil(t, exc.SagaID)
				assert.Equal(t, tt.record.Saga.SagaID, *exc.SagaID)
			}
		})
	}
}

// TestClassify_DuplicateTakesPriorityEvenWithNoLedgerSide pins that a
// duplicate PSP row is reported as DUPLICATE even when the ledger has nothing
// at all to say about the reference -- a statement's own internal
// inconsistency does not need a ledger transaction to be worth flagging.
func TestClassify_DuplicateTakesPriorityEvenWithNoLedgerSide(t *testing.T) {
	t.Parallel()

	now := time.Now()
	record := MatchedRecord{
		ExternalRef: "REF",
		PSP:         NewPSPAggregate(PSPRecord{ExternalRef: "REF", AmountMinor: 500, Currency: "INR", Status: "SETTLED", SettledAt: now}, 3),
	}

	exc, clean := classify(record, time.Hour, now)
	require.False(t, clean)
	require.NotNil(t, exc)
	assert.Equal(t, CategoryDuplicate, exc.Category)
	assert.Equal(t, 3, exc.Details["psp_row_count"])
}
