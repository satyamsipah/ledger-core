package consistency

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
)

// CurrencyViolation is one currency whose journal_entries do not sum to zero.
type CurrencyViolation struct {
	Currency string

	// SignedTotal uses invariant 1's own transaction-sign convention --
	// DEBIT=+, CREDIT=- (see internal/ledger/types.go's block comment on the
	// two sign conventions this codebase carries). Zero is healthy; this
	// field is only ever populated on a violation.
	SignedTotal int64
}

// GlobalInvariantResult is the outcome of one CheckGlobalInvariant pass.
type GlobalInvariantResult struct {
	Violations []CurrencyViolation
}

// OK reports whether every currency's journal balanced.
func (r GlobalInvariantResult) OK() bool { return len(r.Violations) == 0 }

// CheckGlobalInvariant sums every journal entry ever written, signed by
// invariant 1's own convention, grouped by currency.
//
// Every individual transaction already balances to zero at COMMIT -- the
// deferred constraint trigger in migration 000005 enforces that
// unconditionally, for every transaction, unconditionally. This check does
// not re-derive that. It proves something the trigger cannot: that the SUM
// across every transaction that has ever committed is STILL zero, which can
// only fail if some row was written, corrupted or removed in a way the
// trigger never saw -- raw SQL run outside the application, a future
// migration that grants UPDATE or DELETE on journal_entries, or a defect in
// the trigger itself. None of those would be caught by a check that only
// ever looks at one transaction at a time, which is what makes this a
// genuinely different proof rather than a restatement of the same one.
//
// Grouped by currency rather than summed globally: invariant 1 balances per
// (transaction_id, currency), so a bug that is simultaneously wrong by +100 in
// one currency and -100 in another would sum to zero across the whole ledger
// and hide behind a single ungrouped total. Two real, unrelated defects must
// not be able to cancel each other out of this check.
func CheckGlobalInvariant(ctx context.Context, pool *pgxpool.Pool) (GlobalInvariantResult, error) {
	rows, err := pool.Query(ctx, `
		SELECT currency,
		       SUM(CASE WHEN direction = 'DEBIT' THEN amount_minor ELSE -amount_minor END) AS signed_total
		  FROM journal_entries
		 GROUP BY currency
		HAVING SUM(CASE WHEN direction = 'DEBIT' THEN amount_minor ELSE -amount_minor END) <> 0
		 ORDER BY currency`)
	if err != nil {
		return GlobalInvariantResult{}, fmt.Errorf("check global invariant: %w", err)
	}
	defer rows.Close()

	var result GlobalInvariantResult
	for rows.Next() {
		var v CurrencyViolation
		if err := rows.Scan(&v.Currency, &v.SignedTotal); err != nil {
			return GlobalInvariantResult{}, fmt.Errorf("scan global invariant violation: %w", err)
		}
		result.Violations = append(result.Violations, v)
	}
	if err := rows.Err(); err != nil {
		return GlobalInvariantResult{}, fmt.Errorf("check global invariant: %w", err)
	}
	return result, nil
}
