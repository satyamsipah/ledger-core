package saga

import "errors"

var (
	// ErrSagaNotFound means no saga_instances row carries the requested id.
	ErrSagaNotFound = errors.New("saga: instance not found")

	// ErrStaleTransition means a guarded transition matched no row: the saga
	// was not in the status this caller believed it to be.
	//
	// This is a lost race, not a corruption. It happens when a lease expired
	// and another replica moved the saga on while this one was still working.
	// The correct response is to abandon the attempt, never to re-read and
	// retry -- the other replica is now the authority, and a second writer
	// forcing its view is how a compensated saga gets settled anyway.
	ErrStaleTransition = errors.New("saga: transition did not match the expected status")

	// ErrLeaseLost means the orchestrator no longer owns the saga it is
	// driving. Returned from inside the ledger transaction so the money rolls
	// back with it, exactly as idempotency.ErrLeaseLost does on the write path.
	ErrLeaseLost = errors.New("saga: lease was claimed by another orchestrator")

	// ErrAmbiguousOutcome means the external gateway's result is unknown and
	// could not be resolved by probing.
	//
	// It is deliberately NOT a failure. Treating an unknown outcome as a
	// failure compensates payments that really went out; treating it as a
	// success pays merchants for payments that never happened. This error
	// exists so that neither can be written by accident: the only paths out of
	// it are a conclusive probe or NEEDS_MANUAL_REVIEW.
	ErrAmbiguousOutcome = errors.New("saga: gateway outcome is unknown")

	// ErrCompensationExhausted means a compensation failed more times than it
	// is allowed to. It escalates to NEEDS_MANUAL_REVIEW and must never be
	// resolved by writing a balancing adjustment automatically; see
	// docs/DECISIONS.md.
	ErrCompensationExhausted = errors.New("saga: compensation exhausted its retries")

	// ErrUnknownSagaType means the orchestrator claimed a saga it has no
	// definition for -- almost always a saga written by a newer deployment.
	// Logged and left alone rather than failed, so a rolling deploy does not
	// mark the new version's sagas dead.
	ErrUnknownSagaType = errors.New("saga: no definition registered for this saga type")
)
