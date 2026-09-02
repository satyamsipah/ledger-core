package reconciliation

import (
	"fmt"
	"time"
)

// classify decides what, if anything, is wrong with one matched external_ref.
//
// Returns (nil, true) for a clean match: nothing worth a row in
// reconciliation_exceptions. Returns (exc, false) otherwise, with exc.Status
// already set to OPEN or AUTO_RESOLVED -- classify is the only place that
// decides auto-resolution, so a caller never has to re-derive "was this within
// the window" from a stored exception later.
//
// The categories are checked in a fixed priority order, because more than one
// could describe the same reference and the caller needs exactly one answer.
// DUPLICATE first: a PSP statement listing one reference twice is a data
// problem with the STATEMENT, independent of whatever the ledger says, and
// reporting it as an amount or status mismatch instead would send an operator
// investigating the wrong system. MISSING_* next, because nothing else is
// computable without both sides present. Then AMOUNT_MISMATCH before
// STATUS_MISMATCH: an amount that is simply wrong is the more actionable
// defect, and a status disagreement on a transaction whose amount is also
// wrong is very likely a symptom of the same underlying problem, not a second
// one. TIMING_DIFFERENCE last, because it is the only category this function
// ever resolves on the spot.
func classify(m MatchedRecord, timingWindow time.Duration, now time.Time) (*Exception, bool) {
	exc := &Exception{
		ExternalRef: m.ExternalRef,
		Status:      ExceptionStatusOpen,
		Details:     map[string]any{},
	}
	if m.Ledger != nil {
		id := m.Ledger.TransactionID
		exc.LedgerTransactionID = &id
		exc.LedgerAmountMinor = &m.Ledger.AmountMinor
		exc.LedgerStatus = m.Ledger.Status
		exc.Currency = m.Ledger.Currency
	}
	if m.Saga != nil {
		id := m.Saga.SagaID
		exc.SagaID = &id
	}
	if m.PSP != nil {
		amount := m.PSP.AmountMinor
		exc.PSPAmountMinor = &amount
		exc.PSPStatus = m.PSP.Status
		if exc.Currency == "" {
			exc.Currency = m.PSP.Currency
		}
	}

	switch {
	case m.PSP != nil && m.PSP.RowCount > 1:
		exc.Category = CategoryDuplicate
		exc.Details["psp_row_count"] = m.PSP.RowCount

	case m.PSP == nil:
		// Ledger present (or classify would never have been called for this
		// external_ref -- see engine.go), PSP absent.
		exc.Category = CategoryMissingInPSP

	case m.Ledger == nil:
		exc.Category = CategoryMissingInLedger

	case m.Ledger.AmountMinor != m.PSP.AmountMinor || m.Ledger.Currency != m.PSP.Currency:
		exc.Category = CategoryAmountMismatch
		exc.Details["ledger_amount"] = fmt.Sprintf("%d %s", m.Ledger.AmountMinor, m.Ledger.Currency)
		exc.Details["psp_amount"] = fmt.Sprintf("%d %s", m.PSP.AmountMinor, m.PSP.Currency)

	case !statusesAgree(m.Ledger.Status, m.PSP.Status):
		exc.Category = CategoryStatusMismatch

	case m.Ledger.PostedAt != nil:
		gap := m.Ledger.PostedAt.Sub(m.PSP.SettledAt)
		if gap < 0 {
			gap = -gap
		}
		if gap == 0 {
			return nil, true
		}
		exc.Category = CategoryTimingDifference
		exc.Details["gap"] = gap.String()
		if gap <= timingWindow {
			exc.Status = ExceptionStatusAutoResolved
			resolvedAt := now
			exc.ResolvedAt = &resolvedAt
		}

	default:
		// Amount and status agree, and there is no posted_at to compare a
		// settlement time against (the transaction is PENDING). Nothing left
		// to disagree about.
		return nil, true
	}

	return exc, false
}

// statusesAgree maps the ledger's TransactionStatus vocabulary onto the PSP's
// own, because the two sides were never going to use the same words for the
// same fact.
//
// REVERSED accepts both REFUNDED and FAILED on the PSP side deliberately: a
// reversal in this ledger can correct either a settlement that later had to be
// given back (REFUNDED) or one that the PSP itself never actually completed
// (FAILED) -- this package cannot tell which from the ledger alone, and both
// are a legitimate reason for POSTED money to have been reversed.
func statusesAgree(ledgerStatus, pspStatus string) bool {
	switch ledgerStatus {
	case "POSTED":
		return pspStatus == "SETTLED"
	case "REVERSED":
		return pspStatus == "REFUNDED" || pspStatus == "FAILED"
	case "PENDING":
		return pspStatus == "PENDING"
	default:
		return false
	}
}
