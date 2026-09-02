package reconciliation

import (
	"context"
	"time"

	"github.com/google/uuid"
)

// Store is the persistence port for reconciliation runs and exceptions.
//
// Match is the one method that is not a straightforward CRUD operation: it is
// the three-way join itself, and it lives behind this interface rather than
// in Engine because it is the one place a real implementation needs SQL --
// everything else here a Store implementation could in principle build from
// Match's own output.
type Store interface {
	// Match runs the three-way join for a batch of PSP records and returns one
	// MatchedRecord per external_ref seen by either side.
	//
	// since bounds how far back ledger transactions are considered when
	// looking for a reference the PSP statement never mentions
	// (MISSING_IN_PSP): without a bound, every run would rescan the entire
	// journal's external_ref history looking for statement gaps that a
	// week-old run has already reported. It does not bound the PSP side --
	// every row the caller hands in is matched, regardless of its own
	// settled_at.
	Match(ctx context.Context, psp []PSPRecord, since time.Time) ([]MatchedRecord, error)

	// CreateRun inserts a new run in RUNNING status.
	CreateRun(ctx context.Context, run Run) error

	// FinishRun records a run's outcome. Called exactly once per run, whether
	// it succeeded or failed -- see the RunStatus check constraint in
	// migration 000017, which makes a RUNNING row with a finished_at (or vice
	// versa) unrepresentable.
	FinishRun(ctx context.Context, id uuid.UUID, status RunStatus, errMsg string, matched, autoResolved, exceptionCount int) error

	// SaveExceptions persists every exception one run raised, in one batch.
	// A no-op on an empty slice: a clean run raises nothing, and that is not
	// an error.
	SaveExceptions(ctx context.Context, exceptions []Exception) error

	// GetRun reads one run -- including its category breakdown -- by id,
	// returning ErrRunNotFound if absent.
	GetRun(ctx context.Context, id uuid.UUID) (*Run, error)

	// ListRuns returns the most recent runs, newest first, for the report
	// listing endpoint. ByCategory is left empty on every entry: a list view
	// summarises run-level counts, which Run already carries.
	ListRuns(ctx context.Context, limit int) ([]Run, error)

	// ListExceptions returns every exception one run raised, oldest first,
	// for that run's detail view.
	ListExceptions(ctx context.Context, runID uuid.UUID) ([]Exception, error)
}
