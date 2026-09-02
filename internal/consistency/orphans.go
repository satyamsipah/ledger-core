package consistency

import (
	"context"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

// OrphanResult is the outcome of one CheckOrphans pass: two independent
// structural findings, reported together because both answer the same
// question -- "does every transaction have the shape invariant 2 promises?"
type OrphanResult struct {
	// FewEntryTransactions are POSTED or REVERSED transactions with fewer
	// than two journal entries. A real bug: no balanced transaction can have
	// fewer than two legs, so reaching either of these statuses with fewer
	// means something posted around the normal write path.
	FewEntryTransactions []uuid.UUID
	TotalFewEntry        int

	// OrphanEntries are journal_entries rows with no parent transactions row.
	// Structurally impossible given the foreign key journal_entries carries
	// -- see the doc comment on CheckOrphans for why this is checked anyway.
	OrphanEntries      []uuid.UUID
	TotalOrphanEntries int
}

// OK reports whether both structural checks found nothing.
func (r OrphanResult) OK() bool { return r.TotalFewEntry == 0 && r.TotalOrphanEntries == 0 }

// CheckOrphans looks for two shapes of structural corruption journal_entries
// and transactions should never have between them.
//
// PENDING transactions are excluded from the few-entries check on purpose.
// docs/DECISIONS.md's Phase 1 notes carry this as a known, legitimate gap:
// the saga writes a transaction's header before its legs in some paths, so a
// PENDING transaction with zero entries is a normal transient state, not a
// defect. Every path that reaches POSTED or REVERSED, by contrast, is
// required to have written its entries in the same transaction as the status
// -- PostTransaction always does, and D41 closed the same gap for the saga by
// having it never write a PENDING header at all -- so fewer than two entries
// on a POSTED or REVERSED row is unreachable through any code path this
// codebase has, and finding one means exactly that: something wrote around
// the normal path.
//
// The orphan-entries half is checked even though
// journal_entries_transaction_id_fkey already makes it unconstructible by the
// database. It costs one query, and it is what would catch a future migration
// that weakened or dropped that constraint by mistake -- a runtime check for
// a schema-level promise, kept honest rather than merely assumed.
func CheckOrphans(ctx context.Context, pool *pgxpool.Pool) (OrphanResult, error) {
	var result OrphanResult

	fewEntryRows, err := pool.Query(ctx, `
		WITH counted AS (
		    SELECT t.id, count(je.id) AS entry_count
		      FROM transactions t
		      LEFT JOIN journal_entries je ON je.transaction_id = t.id
		     WHERE t.status IN ('POSTED', 'REVERSED')
		     GROUP BY t.id
		    HAVING count(je.id) < 2
		)
		SELECT id, count(*) OVER () FROM counted ORDER BY id LIMIT $1`, maxReported)
	if err != nil {
		return OrphanResult{}, fmt.Errorf("check for transactions with too few entries: %w", err)
	}
	for fewEntryRows.Next() {
		var id uuid.UUID
		if err := fewEntryRows.Scan(&id, &result.TotalFewEntry); err != nil {
			fewEntryRows.Close()
			return OrphanResult{}, fmt.Errorf("scan few-entry transaction: %w", err)
		}
		result.FewEntryTransactions = append(result.FewEntryTransactions, id)
	}
	if err := fewEntryRows.Err(); err != nil {
		fewEntryRows.Close()
		return OrphanResult{}, fmt.Errorf("check for transactions with too few entries: %w", err)
	}
	fewEntryRows.Close()

	orphanRows, err := pool.Query(ctx, `
		SELECT je.id, count(*) OVER ()
		  FROM journal_entries je
		  LEFT JOIN transactions t ON t.id = je.transaction_id
		 WHERE t.id IS NULL
		 ORDER BY je.id
		 LIMIT $1`, maxReported)
	if err != nil {
		return OrphanResult{}, fmt.Errorf("check for orphaned journal entries: %w", err)
	}
	defer orphanRows.Close()
	for orphanRows.Next() {
		var id uuid.UUID
		if err := orphanRows.Scan(&id, &result.TotalOrphanEntries); err != nil {
			return OrphanResult{}, fmt.Errorf("scan orphaned journal entry: %w", err)
		}
		result.OrphanEntries = append(result.OrphanEntries, id)
	}
	if err := orphanRows.Err(); err != nil {
		return OrphanResult{}, fmt.Errorf("check for orphaned journal entries: %w", err)
	}

	return result, nil
}
