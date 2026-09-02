package consistency

import (
	"context"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

// maxReported bounds how many offending rows any single check returns. A
// check that found thousands of violations has a much bigger problem than
// this process's memory or a log line can usefully describe -- TotalFound
// (on each result type) still reports the true count, so the difference
// between "3 accounts drifted" and "50,000 accounts drifted" is never lost,
// even when only the first maxReported are named.
const maxReported = 200

// BalanceDrift is one account where the synchronous balance disagrees with
// the sum of its own journal entries.
type BalanceDrift struct {
	AccountID        uuid.UUID
	Currency         string
	RebuiltAvailable int64
	LiveAvailable    int64
}

// DriftResult is the outcome of one CheckProjectionDrift pass.
type DriftResult struct {
	AccountsCompared int
	Drifted          []BalanceDrift

	// TotalDrifted is the true count of drifted accounts, which may exceed
	// len(Drifted) once maxReported is hit.
	TotalDrifted int
}

// OK reports whether every compared account's synchronous balance matched
// the journal.
func (r DriftResult) OK() bool { return r.TotalDrifted == 0 }

// CheckProjectionDrift recomputes every account's balance directly from
// journal_entries -- signed by the account's own normal balance, exactly as
// account_balances.available_minor itself is (docs/DECISIONS.md D13) -- and
// diffs it against the live account_balances row the write path maintains
// under lock.
//
// This is deliberately NOT the same comparison internal/projector.Rebuild
// already makes: that one diffs the journal against balance_projections, the
// Kafka-DRIVEN read model, to prove the async pipeline (outbox, publish,
// consume, apply) agrees with the ledger. This check diffs the journal
// against account_balances, the SYNCHRONOUS balance updated inside the same
// transaction as the journal entries themselves (D1). The two comparisons
// catch different bugs -- an async pipeline defect versus a write-path defect
// in the synchronous update or its locking -- and D1 names exactly this
// three-balance triangle (synchronous, event-sourced, journal) so that any
// two agreeing while a third dissents localises which one is wrong.
//
// A missing account_balances row is treated as an available_minor of 0 for
// the comparison, not as a drift finding on its own: migration 000009's
// trigger makes that row's existence a write-path invariant already, and an
// account with genuinely nothing posted to it agrees with a rebuilt balance
// of zero regardless. What this reports is a NONZERO rebuilt balance against
// a missing or disagreeing row, exactly like any other drift.
func CheckProjectionDrift(ctx context.Context, pool *pgxpool.Pool) (DriftResult, error) {
	var result DriftResult

	if err := pool.QueryRow(ctx, `SELECT count(*) FROM accounts`).Scan(&result.AccountsCompared); err != nil {
		return DriftResult{}, fmt.Errorf("count accounts for projection drift check: %w", err)
	}

	rows, err := pool.Query(ctx, `
		WITH rebuilt AS (
		    SELECT a.id AS account_id, a.currency,
		           COALESCE(SUM(CASE WHEN je.direction = a.normal_balance
		                             THEN je.amount_minor ELSE -je.amount_minor END), 0) AS available_minor
		      FROM accounts a
		      LEFT JOIN journal_entries je ON je.account_id = a.id
		     GROUP BY a.id, a.currency
		),
		compared AS (
		    SELECT r.account_id, r.currency,
		           r.available_minor AS rebuilt_available,
		           COALESCE(ab.available_minor, 0) AS live_available
		      FROM rebuilt r
		      LEFT JOIN account_balances ab ON ab.account_id = r.account_id
		     WHERE r.available_minor <> COALESCE(ab.available_minor, 0)
		)
		SELECT account_id, currency, rebuilt_available, live_available, count(*) OVER ()
		  FROM compared
		 ORDER BY account_id
		 LIMIT $1`, maxReported)
	if err != nil {
		return DriftResult{}, fmt.Errorf("check projection drift: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var d BalanceDrift
		if err := rows.Scan(&d.AccountID, &d.Currency, &d.RebuiltAvailable, &d.LiveAvailable, &result.TotalDrifted); err != nil {
			return DriftResult{}, fmt.Errorf("scan projection drift row: %w", err)
		}
		result.Drifted = append(result.Drifted, d)
	}
	if err := rows.Err(); err != nil {
		return DriftResult{}, fmt.Errorf("check projection drift: %w", err)
	}
	return result, nil
}
