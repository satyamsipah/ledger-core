package saga

import (
	"context"
	"time"

	"github.com/google/uuid"

	"github.com/satyamsipah/ledger-core/internal/outbox"
)

// Store is the persistence port for saga state.
//
// The state machine it backs has one property worth stating outright, because
// every recovery decision rests on it:
//
//	A saga's status and the money that status describes commit together.
//
// That holds because Advance is also reachable from inside the ledger's own
// transaction (see the AdvanceSaga method on internal/ledger's Tx port), so a
// forward step's journal entries and its transition to the next status are one
// COMMIT. A crash therefore leaves a saga in a status whose ledger effects have
// definitely happened, or in the previous status with definitely none of them
// -- never in between.
//
//	PENDING ──reserve──▶ RESERVED ──intent──▶ GATEWAY_PENDING
//	   │                    │                    │      │
//	   ▼                    │              probe │      │ probe
//	 FAILED                 │                    ▼      ▼
//	                        │       GATEWAY_SUCCEEDED  GATEWAY_FAILED
//	                        │                    │      │
//	                        │             settle │      ▼
//	                        │                    ▼   COMPENSATING
//	                        │              COMPLETED     │
//	                        └───────────────────────▶ COMPENSATED
//
// The methods here are the transitions that happen OUTSIDE a ledger
// transaction: creating a saga, claiming one, and the gateway step, which moves
// no money and so has no ledger transaction to ride along with.
type Store interface {
	// Create inserts a new saga. A duplicate idempotency key returns the
	// existing instance rather than an error, so POST /v1/payouts is idempotent
	// at the saga level in the same way the write path is at the ledger level.
	Create(ctx context.Context, in Instance) (*Instance, error)

	// Get reads one saga by id, returning ErrSagaNotFound if absent.
	Get(ctx context.Context, id uuid.UUID) (*Instance, error)

	// ClaimRunnable takes up to batch sagas of one type that can be acted on
	// right now -- non-terminal, lease free, and in a status the orchestrator
	// can drive without waiting for anything.
	//
	// Scoped by saga type so an orchestrator never claims, leases and then
	// discards work belonging to a different definition. Without the scope, two
	// orchestrators deployed side by side would each spend their claim budget
	// taking the other's sagas hostage for a lease at a time.
	//
	// GATEWAY_PENDING is excluded on purpose. A saga waiting on a gateway
	// response has nothing to do until its deadline passes, and claiming it
	// would either spin or force a premature probe. ClaimExpired picks it up
	// once its deadline actually blows.
	ClaimRunnable(ctx context.Context, sagaType, owner string, lease time.Duration, batch int) ([]Instance, error)

	// ClaimExpired takes up to batch sagas whose step deadline has passed,
	// whatever their status. This is the sweeper's query, and it is the only
	// path by which a GATEWAY_PENDING saga is ever looked at again.
	//
	// It uses FOR UPDATE SKIP LOCKED for the same reason the polling publisher
	// does (D31): several orchestrator replicas partition the backlog with no
	// leader election, and a replica that dies mid-batch simply stops holding
	// its rows.
	ClaimExpired(ctx context.Context, sagaType, owner string, lease time.Duration, batch int) ([]Instance, error)

	// Advance applies a guarded transition outside any ledger transaction, for
	// the gateway step and for escalations to NEEDS_MANUAL_REVIEW. It returns
	// ErrStaleTransition if the saga was not in Transition.From.
	Advance(ctx context.Context, t Transition) error

	// RenewLease extends this orchestrator's hold on a saga it is still
	// working, returning ErrLeaseLost if another replica has taken it.
	RenewLease(ctx context.Context, id uuid.UUID, owner string, lease time.Duration) error

	// BeginStep records the write-ahead intent row and applies the transition
	// that accompanies it, in ONE transaction, before work that cannot be
	// rolled back begins.
	//
	// This is what the gateway step commits before calling out. Both halves
	// together, because either alone is a lie: an intent row without the
	// GATEWAY_PENDING status leaves a saga that will start a second attempt,
	// and the status without the row leaves an audit log that never records
	// the call was made.
	BeginStep(ctx context.Context, a StartAttempt, t Transition) error

	// RecordAttempt inserts one already-finished attempt outside any ledger
	// transaction, for a step that failed and therefore rolled its own audit
	// row back along with everything else it did.
	RecordAttempt(ctx context.Context, a Attempt) error

	// FinishAttempt records an ATTEMPTED row's outcome.
	FinishAttempt(ctx context.Context, f FinishAttempt) error

	// Escalate moves a saga to NEEDS_MANUAL_REVIEW and appends the alert event
	// in one transaction.
	//
	// Together, because an escalation nobody is told about is indistinguishable
	// from a saga that quietly stopped -- and "never silently drop it" is the
	// entire requirement this state exists to satisfy.
	Escalate(ctx context.Context, t Transition, alert outbox.Event) error

	// UnresolvedAttempt returns the newest ATTEMPTED row for a step, or nil.
	//
	// This is how a restarted orchestrator discovers that a gateway call may
	// have gone out under a key it no longer holds in memory, which is the
	// question the whole write-ahead ordering exists to make answerable.
	UnresolvedAttempt(ctx context.Context, sagaID uuid.UUID, step Step) (*Attempt, error)

	// Attempts returns a saga's full history, newest last, for the dashboard.
	Attempts(ctx context.Context, sagaID uuid.UUID) ([]Attempt, error)

	// NextAttemptNumber returns the number the next attempt at a step should
	// carry, so the unique constraint on (saga, step, direction, attempt) is a
	// real duplicate guard rather than a source of spurious collisions.
	NextAttemptNumber(ctx context.Context, sagaID uuid.UUID, step Step, direction Direction) (int, error)

	// ListByStatus powers the dashboard's stuck-saga view.
	ListByStatus(ctx context.Context, status Status, limit int) ([]Instance, error)

	// CountByStatus feeds the ledger_saga_instances gauge. One query rather
	// than one per status, because the gauge is refreshed on a ticker and a
	// ten-query refresh is ten chances to disagree with itself.
	CountByStatus(ctx context.Context) (map[Status]int, error)

	// OldestOverdueSeconds returns how long the most-overdue non-terminal saga
	// has been past its own step_deadline_at, or zero when nothing is overdue.
	//
	// Feeds the "saga stuck" alert. NEEDS_MANUAL_REVIEW is excluded, along with
	// every other terminal status, by the same partial index the sweeper's own
	// claim query uses: a saga already escalated is alerted on separately
	// (ledger_saga_manual_review_total), and including it here would let one
	// old escalation that nobody has resolved yet permanently dominate this
	// gauge and hide a second, freshly-stuck saga behind it.
	OldestOverdueSeconds(ctx context.Context) (float64, error)
}
