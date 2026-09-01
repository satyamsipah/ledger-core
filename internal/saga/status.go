package saga

// Status is a saga instance's position in the state machine.
//
// Every value here is a SETTLED state. There is deliberately no RESERVING or
// SETTLING: a forward step commits its ledger entries and its transition to the
// next status in one database transaction, so no crash can leave an instance
// describing itself as halfway through something. Whether a step is currently
// being attempted is carried by the lease, not by this column.
type Status string

// The ten persisted states, matching saga_instances_status_check in migration
// 000015.
const (
	// StatusPending is a saga that has been created and has moved no money.
	StatusPending Status = "PENDING"

	// StatusReserved means the customer wallet has been debited and the funds
	// are sitting in the platform suspense account. This is the semantic lock:
	// the money is out of the wallet, so a concurrent payout cannot spend it,
	// enforced by the ordinary overdraft CHECK rather than by anything this
	// package does.
	StatusReserved Status = "RESERVED"

	// StatusGatewayPending means an attempt was durably recorded and the
	// outcome is UNKNOWN.
	//
	// It covers three situations that are indistinguishable from here, which is
	// exactly why they share a state: the call is still in flight, the call
	// timed out, or the orchestrator died mid-call. All three are resolved the
	// same way -- by asking the gateway what happened, using the key recorded
	// before the call went out. None of them may be resolved by assuming.
	StatusGatewayPending Status = "GATEWAY_PENDING"

	// StatusGatewaySucceeded means the gateway confirmed the payment, either in
	// its original response or in answer to a later probe.
	StatusGatewaySucceeded Status = "GATEWAY_SUCCEEDED"

	// StatusGatewayFailed means the gateway confirmed the payment did not
	// happen. Confirmed, not inferred: a timeout is StatusGatewayPending.
	StatusGatewayFailed Status = "GATEWAY_FAILED"

	// StatusCompensating means the saga is running backwards.
	StatusCompensating Status = "COMPENSATING"

	// StatusCompleted is the successful terminal state: settled to the merchant
	// and to fee revenue.
	StatusCompleted Status = "COMPLETED"

	// StatusCompensated is the clean-failure terminal state. Every account
	// involved is back to the balance it held before the saga started.
	StatusCompensated Status = "COMPENSATED"

	// StatusFailed is the terminal state for a saga that failed before moving
	// anything -- an unfunded wallet, a frozen account. There is nothing to
	// compensate, which is why this is distinct from StatusCompensated.
	StatusFailed Status = "FAILED"

	// StatusNeedsManualReview means automation has stopped, on purpose.
	//
	// Reached when a compensation exhausts its retries or the gateway's outcome
	// stays unknown after every probe. The money is parked in the suspense
	// account -- taken from the customer, not given to the merchant -- which is
	// a wrong state but a KNOWN and NAMED one. See docs/DECISIONS.md for why
	// resolving this automatically is more dangerous than leaving it stuck.
	StatusNeedsManualReview Status = "NEEDS_MANUAL_REVIEW"
)

// Terminal reports whether the orchestrator is finished with this saga.
//
// NEEDS_MANUAL_REVIEW counts as terminal even though the saga is not resolved,
// because the sweeper must stop picking it up. Continuing to retry a saga a
// human has been paged about turns one alert into a stream of them, and buries
// the entry an operator is trying to read.
func (s Status) Terminal() bool {
	switch s {
	case StatusCompleted, StatusCompensated, StatusFailed, StatusNeedsManualReview:
		return true
	default:
		return false
	}
}

// Compensating reports whether the saga is running backwards, which is what
// decides whether the sweeper re-drives a forward step or retries a
// compensation.
func (s Status) Compensating() bool { return s == StatusCompensating }

// Step names one stage of the payout saga.
//
// There are three, not the five a reading of the business description suggests.
// "Debit the wallet" and "credit suspense" are the two legs of ONE transaction
// and cannot be separate steps: a transaction carrying only a debit sums to a
// non-zero value and the deferred balance trigger rejects it at COMMIT.
// Likewise settlement's two credits need the balancing suspense debit.
type Step string

// The steps, matching saga_steps_step_check and saga_instances_current_step_check.
const (
	// StepReserve debits the customer wallet and credits platform suspense.
	StepReserve Step = "RESERVE"

	// StepGateway calls the external payment gateway. It moves no money in this
	// ledger, and it is the only step that cannot commit atomically with its
	// own state transition.
	StepGateway Step = "GATEWAY"

	// StepSettle debits platform suspense and credits the merchant payable and
	// fee revenue accounts.
	StepSettle Step = "SETTLE"

	// StepDone is the resting value of saga_instances.current_step once no
	// further step will be attempted. It is not a stage and never appears in
	// saga_steps.
	StepDone Step = "DONE"
)

// Direction distinguishes a forward attempt from a compensating one, so that
// the audit log shows a step and its undo as two rows rather than one confusing
// one.
type Direction string

// The two directions, matching saga_steps_direction_check.
const (
	DirectionForward      Direction = "FORWARD"
	DirectionCompensation Direction = "COMPENSATION"
)

// StepStatus is the outcome of a single attempt.
type StepStatus string

// The three attempt outcomes, matching saga_steps_status_check.
const (
	// StepAttempted is written BEFORE the work, and is the state an attempt is
	// left in by a crash. For the gateway step it is the durable evidence that
	// a payment may exist under a known key.
	StepAttempted StepStatus = "ATTEMPTED"

	StepSucceeded StepStatus = "SUCCEEDED"
	StepFailed    StepStatus = "FAILED"
)
