// Package pgreconciliation implements reconciliation.Store against
// PostgreSQL.
//
// Match is the interesting statement here, and it is the reason this
// implementation cannot be swapped for one backed by anything else: it joins
// the caller's PSP records against this same database's own transactions,
// journal_entries and saga_instances tables in one round trip, which is only
// possible because all four live in one Postgres instance.
package pgreconciliation

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/satyamsipah/ledger-core/internal/reconciliation"
)

// Compile-time proof this package satisfies the port it claims to.
var _ reconciliation.Store = (*Repository)(nil)

// matchTimeoutMultiple scales the ordinary per-statement timeout for Match
// alone. It is a three-way join over a lookback window rather than a
// point lookup, and it is a daily batch job, not a request on the write
// path -- both point at a more generous budget than every other statement
// this store issues.
const matchTimeoutMultiple = 10

// Repository is the PostgreSQL-backed reconciliation store.
type Repository struct {
	pool    *pgxpool.Pool
	timeout time.Duration
}

// New builds a store over an existing pool.
func New(pool *pgxpool.Pool, timeout time.Duration) *Repository {
	return &Repository{pool: pool, timeout: timeout}
}

// Match assembles the three-way join. See doc.go and store.go for what
// "latest" and "since" mean here; this method is just the SQL that
// implements them.
func (r *Repository) Match(ctx context.Context, psp []reconciliation.PSPRecord, since time.Time) ([]reconciliation.MatchedRecord, error) {
	ctx, cancel := context.WithTimeout(ctx, r.timeout*matchTimeoutMultiple)
	defer cancel()

	refs := make([]string, len(psp))
	amounts := make([]int64, len(psp))
	currencies := make([]string, len(psp))
	statuses := make([]string, len(psp))
	settledAts := make([]time.Time, len(psp))
	for i, rec := range psp {
		refs[i] = rec.ExternalRef
		amounts[i] = rec.AmountMinor
		currencies[i] = rec.Currency
		statuses[i] = rec.Status
		settledAts[i] = rec.SettledAt
	}

	rows, err := r.pool.Query(ctx, `
		WITH psp_input AS (
		    SELECT * FROM unnest($1::text[], $2::bigint[], $3::text[], $4::text[], $5::timestamptz[])
		        AS t(external_ref, amount_minor, currency, status, settled_at)
		),
		-- One row per external_ref the statement mentions. The (array_agg ...
		-- ORDER BY settled_at DESC)[1] idiom picks "the most recent row" per
		-- column without a second join back to psp_input -- row_count is what
		-- turns into a DUPLICATE finding in classify.go.
		psp_agg AS (
		    SELECT external_ref,
		           (array_agg(amount_minor ORDER BY settled_at DESC))[1] AS amount_minor,
		           (array_agg(currency ORDER BY settled_at DESC))[1]     AS currency,
		           (array_agg(status ORDER BY settled_at DESC))[1]       AS status,
		           (array_agg(settled_at ORDER BY settled_at DESC))[1]   AS settled_at,
		           count(*)                                              AS row_count
		      FROM psp_input
		     GROUP BY external_ref
		),
		-- The latest transaction per external_ref, with its amount computed
		-- as the sum of its DEBIT-side entries -- see doc.go for why that is
		-- a safe definition of "amount" for any balanced transaction, not
		-- only a payout's.
		ledger_latest AS (
		    SELECT DISTINCT ON (t.external_ref)
		           t.external_ref, t.id AS transaction_id, t.status,
		           t.posted_at, t.created_at, e.amount_minor, e.currency
		      FROM transactions t
		      JOIN LATERAL (
		             SELECT sum(je.amount_minor) AS amount_minor,
		                    (array_agg(je.currency))[1] AS currency
		               FROM journal_entries je
		              WHERE je.transaction_id = t.id AND je.direction = 'DEBIT'
		           ) e ON true
		     WHERE t.external_ref IS NOT NULL AND t.created_at >= $6
		     ORDER BY t.external_ref, t.created_at DESC
		),
		-- The latest saga per external_ref. payload->>'external_ref' rather
		-- than a column: only payout sagas carry one today (see
		-- internal/saga/payout.Payload), and a saga type with no such key
		-- simply never appears here, which is correct rather than an error.
		saga_latest AS (
		    SELECT DISTINCT ON (s.payload ->> 'external_ref')
		           s.payload ->> 'external_ref' AS external_ref, s.id AS saga_id, s.status
		      FROM saga_instances s
		     WHERE s.payload ->> 'external_ref' IS NOT NULL
		     ORDER BY s.payload ->> 'external_ref', s.created_at DESC
		),
		refs AS (
		    SELECT external_ref FROM psp_agg
		    UNION
		    SELECT external_ref FROM ledger_latest
		)
		SELECT r.external_ref,
		       l.transaction_id, l.amount_minor, l.currency, l.status, l.posted_at, l.created_at,
		       sg.saga_id, sg.status,
		       p.amount_minor, p.currency, p.status, p.settled_at, p.row_count
		  FROM refs r
		  LEFT JOIN ledger_latest l ON l.external_ref = r.external_ref
		  LEFT JOIN saga_latest sg ON sg.external_ref = r.external_ref
		  LEFT JOIN psp_agg p ON p.external_ref = r.external_ref
		 ORDER BY r.external_ref`,
		refs, amounts, currencies, statuses, settledAts, since)
	if err != nil {
		return nil, fmt.Errorf("match reconciliation records: %w", err)
	}
	defer rows.Close()

	var matches []reconciliation.MatchedRecord
	for rows.Next() {
		var (
			externalRef string

			transactionID  *uuid.UUID
			ledgerAmount   *int64
			ledgerCurrency *string
			ledgerStatus   *string
			postedAt       *time.Time
			createdAt      *time.Time

			sagaID     *uuid.UUID
			sagaStatus *string

			pspAmount   *int64
			pspCurrency *string
			pspStatus   *string
			settledAt   *time.Time
			rowCount    *int
		)
		if err := rows.Scan(
			&externalRef,
			&transactionID, &ledgerAmount, &ledgerCurrency, &ledgerStatus, &postedAt, &createdAt,
			&sagaID, &sagaStatus,
			&pspAmount, &pspCurrency, &pspStatus, &settledAt, &rowCount,
		); err != nil {
			return nil, fmt.Errorf("scan matched reconciliation record: %w", err)
		}

		m := reconciliation.MatchedRecord{ExternalRef: externalRef}
		if transactionID != nil {
			// ledgerAmount/ledgerCurrency can still be NULL here: the LATERAL
			// aggregate in the query below returns one row even over zero
			// journal_entries, with a NULL sum. Every write path today closes
			// that gap (see docs/DECISIONS.md D41), so this is defensive
			// against stale or manually-inserted data, not a case this
			// service's own paths produce -- and defaulting to zero here
			// means such a transaction surfaces as an AMOUNT_MISMATCH for a
			// human to look at, rather than crashing the whole run.
			var amount int64
			if ledgerAmount != nil {
				amount = *ledgerAmount
			}
			var currency string
			if ledgerCurrency != nil {
				currency = *ledgerCurrency
			}
			m.Ledger = &reconciliation.LedgerSide{
				TransactionID: *transactionID,
				AmountMinor:   amount,
				Currency:      currency,
				Status:        *ledgerStatus,
				PostedAt:      postedAt,
				CreatedAt:     *createdAt,
			}
		}
		if sagaID != nil {
			m.Saga = &reconciliation.SagaSide{SagaID: *sagaID, Status: *sagaStatus}
		}
		if pspAmount != nil {
			m.PSP = reconciliation.NewPSPAggregate(reconciliation.PSPRecord{
				ExternalRef: externalRef,
				AmountMinor: *pspAmount,
				Currency:    *pspCurrency,
				Status:      *pspStatus,
				SettledAt:   *settledAt,
			}, *rowCount)
		}
		matches = append(matches, m)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("match reconciliation records: %w", err)
	}
	return matches, nil
}

// CreateRun inserts a new RUNNING run.
func (r *Repository) CreateRun(ctx context.Context, run reconciliation.Run) error {
	ctx, cancel := context.WithTimeout(ctx, r.timeout)
	defer cancel()

	_, err := r.pool.Exec(ctx, `
		INSERT INTO reconciliation_runs (id, source, started_at, status, psp_row_count)
		VALUES ($1, $2, $3, $4, $5)`,
		run.ID, run.Source, run.StartedAt, run.Status, run.PSPRowCount)
	if err != nil {
		return fmt.Errorf("insert reconciliation run %s: %w", run.ID, err)
	}
	return nil
}

// FinishRun records a run's outcome.
func (r *Repository) FinishRun(ctx context.Context, id uuid.UUID, status reconciliation.RunStatus, errMsg string, matched, autoResolved, exceptionCount int) error {
	ctx, cancel := context.WithTimeout(ctx, r.timeout)
	defer cancel()

	_, err := r.pool.Exec(ctx, `
		UPDATE reconciliation_runs
		   SET status = $2, finished_at = now(), error = NULLIF($3, ''),
		       matched_count = $4, auto_resolved_count = $5, exception_count = $6
		 WHERE id = $1`,
		id, status, errMsg, matched, autoResolved, exceptionCount)
	if err != nil {
		return fmt.Errorf("finish reconciliation run %s: %w", id, err)
	}
	return nil
}

// SaveExceptions inserts every exception in one round trip via a pipelined
// batch -- not a single multi-row INSERT, because details is JSONB and
// mixing a jsonb[] unnest column with the plain scalar ones is more
// machinery than a batch of N single-row inserts buys back at this table's
// size (a run's worth of exceptions, not the whole ledger).
func (r *Repository) SaveExceptions(ctx context.Context, exceptions []reconciliation.Exception) error {
	if len(exceptions) == 0 {
		return nil
	}

	ctx, cancel := context.WithTimeout(ctx, r.timeout)
	defer cancel()

	batch := &pgx.Batch{}
	for _, exc := range exceptions {
		details, err := json.Marshal(exc.Details)
		if err != nil {
			return fmt.Errorf("marshal details for exception on %s: %w", exc.ExternalRef, err)
		}
		batch.Queue(`
			INSERT INTO reconciliation_exceptions
			    (id, run_id, external_ref, category, status, ledger_transaction_id,
			     saga_id, ledger_amount_minor, psp_amount_minor, currency,
			     ledger_status, psp_status, details, created_at, resolved_at)
			VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15)`,
			exc.ID, exc.RunID, exc.ExternalRef, exc.Category, exc.Status, exc.LedgerTransactionID,
			exc.SagaID, exc.LedgerAmountMinor, exc.PSPAmountMinor, exc.Currency,
			nullIfEmpty(exc.LedgerStatus), nullIfEmpty(exc.PSPStatus), details, exc.CreatedAt, exc.ResolvedAt)
	}

	results := r.pool.SendBatch(ctx, batch)
	defer results.Close()

	for range exceptions {
		if _, err := results.Exec(); err != nil {
			return fmt.Errorf("insert reconciliation exceptions: %w", err)
		}
	}
	return nil
}

// GetRun reads one run and its category breakdown.
func (r *Repository) GetRun(ctx context.Context, id uuid.UUID) (*reconciliation.Run, error) {
	ctx, cancel := context.WithTimeout(ctx, r.timeout)
	defer cancel()

	var run reconciliation.Run
	var errMsg string
	err := r.pool.QueryRow(ctx, `
		SELECT id, source, started_at, finished_at, status,
		       psp_row_count, matched_count, auto_resolved_count, exception_count,
		       COALESCE(error, '')
		  FROM reconciliation_runs
		 WHERE id = $1`, id).
		Scan(&run.ID, &run.Source, &run.StartedAt, &run.FinishedAt, &run.Status,
			&run.PSPRowCount, &run.MatchedCount, &run.AutoResolvedCount, &run.ExceptionCount, &errMsg)
	if err != nil {
		if err == pgx.ErrNoRows {
			return nil, fmt.Errorf("run %s: %w", id, reconciliation.ErrRunNotFound)
		}
		return nil, fmt.Errorf("read reconciliation run %s: %w", id, err)
	}
	run.Error = errMsg

	breakdown, err := r.categoryBreakdown(ctx, id)
	if err != nil {
		return nil, err
	}
	run.ByCategory = breakdown

	return &run, nil
}

func (r *Repository) categoryBreakdown(ctx context.Context, runID uuid.UUID) (map[reconciliation.Category]int, error) {
	rows, err := r.pool.Query(ctx, `
		SELECT category, count(*) FROM reconciliation_exceptions
		 WHERE run_id = $1 GROUP BY category`, runID)
	if err != nil {
		return nil, fmt.Errorf("category breakdown for run %s: %w", runID, err)
	}
	defer rows.Close()

	breakdown := map[reconciliation.Category]int{}
	for rows.Next() {
		var category reconciliation.Category
		var count int
		if err := rows.Scan(&category, &count); err != nil {
			return nil, fmt.Errorf("scan category breakdown for run %s: %w", runID, err)
		}
		breakdown[category] = count
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("category breakdown for run %s: %w", runID, err)
	}
	return breakdown, nil
}

// ListRuns returns the most recent runs, newest first. ByCategory is left
// empty on every entry -- a list view summarises run-level counts, which the
// run itself already carries; a per-category breakdown is what GetRun is for.
func (r *Repository) ListRuns(ctx context.Context, limit int) ([]reconciliation.Run, error) {
	ctx, cancel := context.WithTimeout(ctx, r.timeout)
	defer cancel()

	rows, err := r.pool.Query(ctx, `
		SELECT id, source, started_at, finished_at, status,
		       psp_row_count, matched_count, auto_resolved_count, exception_count,
		       COALESCE(error, '')
		  FROM reconciliation_runs
		 ORDER BY started_at DESC
		 LIMIT $1`, limit)
	if err != nil {
		return nil, fmt.Errorf("list reconciliation runs: %w", err)
	}
	defer rows.Close()

	var runs []reconciliation.Run
	for rows.Next() {
		var run reconciliation.Run
		var errMsg string
		if err := rows.Scan(&run.ID, &run.Source, &run.StartedAt, &run.FinishedAt, &run.Status,
			&run.PSPRowCount, &run.MatchedCount, &run.AutoResolvedCount, &run.ExceptionCount, &errMsg); err != nil {
			return nil, fmt.Errorf("scan reconciliation run: %w", err)
		}
		run.Error = errMsg
		runs = append(runs, run)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("list reconciliation runs: %w", err)
	}
	return runs, nil
}

// ListExceptions returns one run's exceptions, oldest first -- the order they
// were classified in, which is the order a triage pass would want to see
// them.
func (r *Repository) ListExceptions(ctx context.Context, runID uuid.UUID) ([]reconciliation.Exception, error) {
	ctx, cancel := context.WithTimeout(ctx, r.timeout)
	defer cancel()

	rows, err := r.pool.Query(ctx, `
		SELECT id, run_id, external_ref, category, status, ledger_transaction_id,
		       saga_id, ledger_amount_minor, psp_amount_minor, COALESCE(currency, ''),
		       COALESCE(ledger_status, ''), COALESCE(psp_status, ''), details,
		       created_at, resolved_at
		  FROM reconciliation_exceptions
		 WHERE run_id = $1
		 ORDER BY created_at`, runID)
	if err != nil {
		return nil, fmt.Errorf("list exceptions for run %s: %w", runID, err)
	}
	defer rows.Close()

	var exceptions []reconciliation.Exception
	for rows.Next() {
		var exc reconciliation.Exception
		var details []byte
		if err := rows.Scan(&exc.ID, &exc.RunID, &exc.ExternalRef, &exc.Category, &exc.Status,
			&exc.LedgerTransactionID, &exc.SagaID, &exc.LedgerAmountMinor, &exc.PSPAmountMinor, &exc.Currency,
			&exc.LedgerStatus, &exc.PSPStatus, &details, &exc.CreatedAt, &exc.ResolvedAt); err != nil {
			return nil, fmt.Errorf("scan exception for run %s: %w", runID, err)
		}
		if len(details) > 0 {
			if err := json.Unmarshal(details, &exc.Details); err != nil {
				return nil, fmt.Errorf("decode details for exception %s: %w", exc.ID, err)
			}
		}
		exceptions = append(exceptions, exc)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("list exceptions for run %s: %w", runID, err)
	}
	return exceptions, nil
}

// nullIfEmpty turns an empty string into a nil driver value, so an unset
// ledger_status/psp_status (a category that never populated one, like
// MISSING_IN_LEDGER) is stored as SQL NULL rather than the empty string --
// matching how COALESCE(..., ”) reads it back on the way out.
func nullIfEmpty(s string) any {
	if s == "" {
		return nil
	}
	return s
}
