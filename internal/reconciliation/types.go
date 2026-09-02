package reconciliation

import (
	"time"

	"github.com/google/uuid"
)

// Category classifies why an external_ref could not be silently reconciled.
type Category string

// The six categories this phase requires. A CHECK constraint in migration
// 000017 mirrors this set exactly, the same discipline every other enum in
// this schema follows (see docs/DECISIONS.md D2): the Go type and the
// database constraint are two independent statements of the same rule, and
// disagreeing between them is a bug either side could catch first.
const (
	CategoryMissingInLedger  Category = "MISSING_IN_LEDGER"
	CategoryMissingInPSP     Category = "MISSING_IN_PSP"
	CategoryAmountMismatch   Category = "AMOUNT_MISMATCH"
	CategoryStatusMismatch   Category = "STATUS_MISMATCH"
	CategoryTimingDifference Category = "TIMING_DIFFERENCE"
	CategoryDuplicate        Category = "DUPLICATE"
)

// ExceptionStatus is where one exception stands in its own lifecycle.
type ExceptionStatus string

const (
	// ExceptionStatusOpen means a human has not looked at this yet.
	ExceptionStatusOpen ExceptionStatus = "OPEN"

	// ExceptionStatusAutoResolved means classify closed it itself, which is
	// only ever legal for a TIMING_DIFFERENCE inside the configured window.
	// Recorded rather than simply not raised, so the report can show what was
	// auto-closed and why, not just what still needs a person.
	ExceptionStatusAutoResolved ExceptionStatus = "AUTO_RESOLVED"

	// ExceptionStatusResolved means a human closed it. Nothing in this
	// package sets this today; it exists so the schema does not need to
	// change the day an admin endpoint does.
	ExceptionStatusResolved ExceptionStatus = "RESOLVED"
)

// RunStatus is where one reconciliation run stands.
type RunStatus string

// The three statuses a run passes through, matching
// reconciliation_runs_status_check.
const (
	RunStatusRunning   RunStatus = "RUNNING"
	RunStatusCompleted RunStatus = "COMPLETED"
	RunStatusFailed    RunStatus = "FAILED"
)

// PSPRecord is one row of the PSP settlement file, after parsing.
type PSPRecord struct {
	ExternalRef string
	AmountMinor int64
	Currency    string
	Status      string
	SettledAt   time.Time
}

// PSPAggregate is what the settlement file says about one external_ref, after
// collapsing every row sharing that reference into one representative record
// plus a count.
//
// The representative is the most recently settled row for that reference --
// the same "latest wins" rule Match applies to the ledger side -- and
// RowCount is what turns into a DUPLICATE finding in classify.go when it is
// more than one. A single struct rather than a slice of PSPRecord: nothing
// downstream of Match ever needs the individual duplicate rows, only that
// there were some.
type PSPAggregate struct {
	AmountMinor int64
	Currency    string
	Status      string
	SettledAt   time.Time
	RowCount    int
}

// NewPSPAggregate builds an aggregate from one representative record and the
// number of rows it stood in for.
func NewPSPAggregate(rec PSPRecord, rowCount int) *PSPAggregate {
	return &PSPAggregate{
		AmountMinor: rec.AmountMinor,
		Currency:    rec.Currency,
		Status:      rec.Status,
		SettledAt:   rec.SettledAt,
		RowCount:    rowCount,
	}
}

// LedgerSide is the ledger's current position for one external_ref: the most
// recently created transaction that carries it, with its amount computed as
// the sum of its DEBIT-side entries -- see doc.go for why that is a safe,
// invariant-derived definition of "amount" for any balanced transaction.
type LedgerSide struct {
	TransactionID uuid.UUID
	AmountMinor   int64
	Currency      string
	Status        string
	PostedAt      *time.Time
	CreatedAt     time.Time
}

// SagaSide is the saga (if any) whose payload names this external_ref. Only
// the payout saga does today; the lookup is generic so a second saga type
// that adopts the same payload convention is picked up for free.
type SagaSide struct {
	SagaID uuid.UUID
	Status string
}

// MatchedRecord is one external_ref with whichever of the three sides
// mentioned it -- the direct output of Store.Match, before classification.
type MatchedRecord struct {
	ExternalRef string
	Ledger      *LedgerSide
	Saga        *SagaSide
	PSP         *PSPAggregate
}

// Run is one pass of the three-way match against one PSP statement.
type Run struct {
	ID         uuid.UUID
	Source     string
	StartedAt  time.Time
	FinishedAt *time.Time
	Status     RunStatus

	PSPRowCount       int
	MatchedCount      int
	AutoResolvedCount int
	ExceptionCount    int
	Error             string

	// ByCategory is the report's breakdown. reconciliation_exceptions.category
	// is the source of truth; this is a read-time aggregate over it, never
	// persisted as columns of its own, so it cannot disagree with the rows it
	// summarises.
	ByCategory map[Category]int
}

// Exception is one row this service could not silently reconcile.
type Exception struct {
	ID          uuid.UUID
	RunID       uuid.UUID
	ExternalRef string
	Category    Category
	Status      ExceptionStatus

	LedgerTransactionID *uuid.UUID
	SagaID              *uuid.UUID

	LedgerAmountMinor *int64
	PSPAmountMinor    *int64
	Currency          string
	LedgerStatus      string
	PSPStatus         string

	// Details carries whatever a category needs beyond the columns above --
	// the PSP row count behind a DUPLICATE, the computed gap behind a
	// TIMING_DIFFERENCE. Read by a human triaging one exception, never
	// filtered on.
	Details map[string]any

	CreatedAt  time.Time
	ResolvedAt *time.Time
}
