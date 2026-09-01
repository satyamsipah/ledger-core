package saga

import (
	"encoding/json"
	"time"

	"github.com/google/uuid"
)

// Event types emitted onto ledger.events.saga.
//
// SagaStepCompleted was declared in Phase 4 (docs/DECISIONS.md D32) against an
// orchestrator that did not exist yet; this is where it finally gets emitted.
const (
	EventTypeSagaStepCompleted     = "SagaStepCompleted"
	EventTypeSagaNeedsManualReview = "SagaNeedsManualReview"
)

// EventVersion is the envelope version for every event this package emits,
// matching the int16 scheme D32 settled on.
const EventVersion int16 = 1

// Instance is one row of saga_instances: a single in-flight or finished saga.
type Instance struct {
	ID             uuid.UUID
	SagaType       string
	CurrentStep    Step
	Status         Status
	Payload        json.RawMessage
	RetryCount     int
	LastError      string
	IdempotencyKey *string
	LeaseOwner     *string
	LeaseExpiresAt *time.Time
	StepDeadlineAt time.Time
	CreatedAt      time.Time
	UpdatedAt      time.Time
}

// Transition is one move of the state machine, expressed as a compare-and-set.
//
// From is not decoration. The UPDATE is guarded on it, so an orchestrator whose
// lease expired while it was working cannot advance a saga that another replica
// has since moved on -- the guarded UPDATE matches nothing and the stale writer
// finds out. This is the same mechanism D15 uses to make double reversal
// impossible, and it is what makes a transition safe to attempt from a process
// that may not still own the saga.
type Transition struct {
	SagaID      uuid.UUID
	From        Status
	To          Status
	CurrentStep Step

	// StepTimeout is how long the NEXT step may take before the sweeper
	// considers it stuck. A duration rather than an instant on purpose: the
	// deadline is computed as now() + this by PostgreSQL, so several
	// orchestrator replicas and the sweeper all read one clock. Deadlines
	// computed from a Go process's wall clock and compared against the
	// database's would drift, and the symptom -- sagas swept early or late on
	// one node only -- is miserable to diagnose.
	StepTimeout time.Duration

	RetryCount int
	LastError  string

	// ReleaseLease clears lease_owner and lease_expires_at, handing the saga
	// back. Set on every terminal transition, and whenever the orchestrator is
	// done with a saga for now but expects to return to it.
	ReleaseLease bool
}

// Attempt is one row of saga_steps: a single try at one step, in one direction.
type Attempt struct {
	ID            uuid.UUID
	SagaID        uuid.UUID
	Step          Step
	Number        int
	Direction     Direction
	Status        StepStatus
	TransactionID *uuid.UUID
	GatewayKey    string
	Error         string
	StartedAt     time.Time
	FinishedAt    *time.Time
}

// StartAttempt is the write-ahead intent record, committed BEFORE the work it
// describes.
//
// For a ledger step this is merely history. For the gateway step it is the
// mechanism: it makes "a payment may exist under key K" survive a crash that
// happens between the call going out and the answer coming back. Recording the
// attempt afterwards would be simpler and would lose the key in exactly the
// case the key is needed, leaving a payment nobody can name and therefore
// nobody can reconcile.
type StartAttempt struct {
	ID         uuid.UUID
	SagaID     uuid.UUID
	Step       Step
	Number     int
	Direction  Direction
	GatewayKey string
}

// FinishAttempt records how an attempt turned out, filling in the row
// StartAttempt inserted. It never overwrites a different attempt: a retry is a
// new row with a higher Number, because how many times a step was tried and
// what went wrong each time is the question an operator actually has.
type FinishAttempt struct {
	ID            uuid.UUID
	Status        StepStatus
	TransactionID *uuid.UUID
	Error         string
}

// StepCompletedEvent is the payload of SagaStepCompleted.
type StepCompletedEvent struct {
	SagaID        uuid.UUID  `json:"saga_id"`
	SagaType      string     `json:"saga_type"`
	Step          Step       `json:"step"`
	Direction     Direction  `json:"direction"`
	Attempt       int        `json:"attempt"`
	Status        Status     `json:"saga_status"`
	TransactionID *uuid.UUID `json:"transaction_id,omitempty"`
	OccurredAt    time.Time  `json:"occurred_at"`
}

// ManualReviewEvent is the payload of SagaNeedsManualReview: the alert.
//
// It carries the reason and the attempt count rather than only the saga id,
// because the consumer of this event is a human being paged, and "compensation
// failed 8 times: account is not open for posting" is actionable where "saga
// 0192... needs review" is a lookup task.
type ManualReviewEvent struct {
	SagaID     uuid.UUID `json:"saga_id"`
	SagaType   string    `json:"saga_type"`
	Step       Step      `json:"step"`
	Reason     string    `json:"reason"`
	LastError  string    `json:"last_error"`
	Attempts   int       `json:"attempts"`
	OccurredAt time.Time `json:"occurred_at"`
}

// StepCommit is a completed attempt and the transition it causes, applied
// together inside the ledger's own transaction.
//
// The pairing is the point. A saga step that moved money and a saga row that
// still says it did not are the two halves of the bug this phase exists to
// remove, and the only way they cannot diverge is if one COMMIT covers both.
type StepCommit struct {
	Attempt    Attempt
	Transition Transition
}
