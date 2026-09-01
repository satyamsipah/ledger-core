package projector

import (
	"context"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Mismatch is one account where the live (Kafka-driven) projection disagrees
// with what journal_entries alone says it should be.
type Mismatch struct {
	AccountID        uuid.UUID
	Currency         string
	RebuiltAvailable int64
	LiveAvailable    int64
	LiveVersion      int64
}

// RebuildResult summarises one rebuild-and-verify pass.
type RebuildResult struct {
	AccountsCompared int
	Mismatches       []Mismatch
}

// OK reports whether the rebuild found the live projection identical to the
// journal, account for account.
func (r RebuildResult) OK() bool { return len(r.Mismatches) == 0 }

// Rebuild recomputes every account's balance directly from journal_entries --
// bypassing Kafka, the outbox and every publisher entirely -- and compares it,
// account by account, against the live balance_projections row Consumer has
// been maintaining from the event stream.
//
// accountIDs scopes the comparison. Empty means every account in the
// database -- the whole-journal check cmd/projector -rebuild runs by
// default, and the one that actually answers "does the pipeline agree with
// the ledger, everywhere." A non-empty list restricts it to exactly those
// accounts, which is a real operational need on its own (an investigation
// into one customer's balance does not want a report on every other
// account in the system) and separately is what makes this function usable
// against a database shared with other tests: TestProjector_RebuildMatchesLive
// passes only the accounts it created, so unrelated concurrent tests' rows --
// which legitimately have no projection, because no consumer is running
// against them -- are not reported as mismatches that have nothing to do with
// what the test is checking.
//
// This is deliberately the same aggregation GetBalanceAsOf performs in
// pgledger/repository.go (sum of signed entries by the account's own normal
// balance), unbounded rather than as-of an instant, because the source of
// truth this compares against is journal_entries itself -- not
// account_balances, and not any other derived view. Comparing the projection
// against journal_entries directly is what makes this a genuine end-to-end
// check of the whole pipeline (outbox write, publish, consume, apply) rather
// than a check that the projection agrees with some other cache.
//
// Every account is compared at the PHYSICAL level -- shards included, treated
// as the ordinary accounts they are (D25) -- because that is the level
// balance_projections itself is keyed at: routeToShards rewrites a
// transaction's entries to name a shard before the balance event is emitted,
// so a shard's BalanceUpdated carries the shard's own account_id, never its
// parent's. Comparing at the logical (summed-over-shards) level would hide a
// bug in exactly the accounts sharding exists to relieve.
//
// A missing projection row is treated as available_minor 0 for the
// comparison, not as a mismatch on its own: an account nothing has ever
// posted to has correctly never had a BalanceUpdated event to build a
// projection from, and journal_entries agrees -- it also sums to zero. The
// mismatch that matters is a NONZERO rebuilt balance with no projection row
// at all, which this reports exactly like any other disagreement.
func Rebuild(ctx context.Context, pool *pgxpool.Pool, accountIDs ...uuid.UUID) (RebuildResult, error) {
	// nil, not a zero-length non-nil slice: ANY(NULL) matches nothing, so the
	// filter has to be skipped outright with a boolean rather than passed
	// through as an always-empty array when the caller wants "everything."
	var filter []uuid.UUID
	if len(accountIDs) > 0 {
		filter = accountIDs
	}

	rows, err := pool.Query(ctx, `
		WITH rebuilt AS (
		    SELECT a.id AS account_id,
		           a.currency,
		           COALESCE(SUM(CASE WHEN je.direction = a.normal_balance
		                             THEN je.amount_minor ELSE -je.amount_minor END), 0) AS available_minor
		      FROM accounts a
		      LEFT JOIN journal_entries je ON je.account_id = a.id
		     WHERE $1::uuid[] IS NULL OR a.id = ANY($1)
		     GROUP BY a.id, a.currency
		)
		SELECT r.account_id, r.currency, r.available_minor,
		       COALESCE(bp.available_minor, 0), COALESCE(bp.version, 0)
		  FROM rebuilt r
		  LEFT JOIN balance_projections bp ON bp.account_id = r.account_id
		 ORDER BY r.account_id`, filter)
	if err != nil {
		return RebuildResult{}, fmt.Errorf("rebuild balances from journal: %w", err)
	}
	defer rows.Close()

	var result RebuildResult
	for rows.Next() {
		var m Mismatch
		var rebuiltAvailable, liveAvailable int64
		if err := rows.Scan(&m.AccountID, &m.Currency, &rebuiltAvailable, &liveAvailable, &m.LiveVersion); err != nil {
			return RebuildResult{}, fmt.Errorf("scan rebuilt balance row: %w", err)
		}
		result.AccountsCompared++

		if rebuiltAvailable != liveAvailable {
			m.RebuiltAvailable = rebuiltAvailable
			m.LiveAvailable = liveAvailable
			result.Mismatches = append(result.Mismatches, m)
		}
	}
	if err := rows.Err(); err != nil {
		return RebuildResult{}, fmt.Errorf("rebuild balances from journal: %w", err)
	}

	return result, nil
}
