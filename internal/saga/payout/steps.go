package payout

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/google/uuid"

	"github.com/satyamsipah/ledger-core/internal/gateway"
	"github.com/satyamsipah/ledger-core/internal/ledger"
	"github.com/satyamsipah/ledger-core/internal/outbox"
	"github.com/satyamsipah/ledger-core/internal/saga"
)

// reserve debits the customer wallet into platform suspense.
//
// One transaction, two legs, and it is the saga's semantic lock: after it
// commits the money is out of the wallet, so a concurrent payout cannot spend
// it. The guard is account_balances_no_overdraft_check under the row lock
// LockAccounts already takes -- the ordinary write path's protection, not
// anything this package invents. pending_minor on the SUSPENSE account records
// that the value now sitting there is spoken for; it makes the intermediate
// state self-describing but it is not the guard, and must never be mistaken for
// one.
func (o *Orchestrator) reserve(ctx context.Context, in *saga.Instance, p Payload) error {
	attemptID, number, err := o.nextAttempt(ctx, in.ID, saga.StepReserve, saga.DirectionForward)
	if err != nil {
		return err
	}

	key := stepKey(in.ID, saga.StepReserve, saga.DirectionForward)
	amount := ledger.MustNewMoney(p.AmountMinor, p.Currency)

	_, err = o.ledger.PostTransaction(ctx, ledger.TransactionRequest{
		PrincipalID:    in.PrincipalID,
		Type:           ledger.TransactionTypePayout,
		ExternalRef:    externalRef(p),
		IdempotencyKey: &key,
		Metadata: map[string]any{
			MetadataKeySagaID: in.ID.String(),
			MetadataKeyStep:   string(saga.StepReserve),
		},
		Entries: []ledger.EntryRequest{
			{AccountID: p.CustomerWalletID, Direction: ledger.DirectionDebit, Amount: amount},
			{AccountID: p.SuspenseID, Direction: ledger.DirectionCredit, Amount: amount},
		},
		Record: func(ctx context.Context, tx ledger.Tx, posted *ledger.Transaction) error {
			if err := tx.ApplyPendingDelta(ctx, ledger.PendingDelta{
				AccountID:  p.SuspenseID,
				DeltaMinor: p.AmountMinor,
			}); err != nil {
				return err
			}
			return o.commitStep(ctx, tx, in, commit{
				attemptID:     attemptID,
				number:        number,
				step:          saga.StepReserve,
				direction:     saga.DirectionForward,
				transactionID: &posted.ID,
				from:          saga.StatusPending,
				to:            saga.StatusReserved,
				nextStep:      saga.StepGateway,
			})
		},
	})
	if err != nil {
		return o.failForwardStep(ctx, in, saga.StepReserve, attemptID, number, err)
	}

	o.count(saga.StepReserve, saga.DirectionForward, "succeeded")
	return nil
}

// callGateway submits the payment.
//
// The ordering here is the entire ambiguity strategy, so it is worth reading as
// a sequence rather than as code:
//
//  1. Commit an ATTEMPTED row and the move to GATEWAY_PENDING, together.
//  2. Only then make the call.
//
// Everything after step 1 is recoverable, because whatever happens next -- a
// timeout, a killed process, a severed network -- leaves durable evidence that
// a payment MAY exist under a key that can be recomputed from the saga's own
// id. Reversing the order would leave a payment nobody can name.
func (o *Orchestrator) callGateway(ctx context.Context, in *saga.Instance, p Payload) error {
	attemptID, number, err := o.nextAttempt(ctx, in.ID, saga.StepGateway, saga.DirectionForward)
	if err != nil {
		return err
	}
	key := gatewayKey(in.ID)

	if err := o.store.BeginStep(ctx,
		saga.StartAttempt{
			ID:         attemptID,
			SagaID:     in.ID,
			Step:       saga.StepGateway,
			Number:     number,
			Direction:  saga.DirectionForward,
			GatewayKey: key,
		},
		saga.Transition{
			SagaID:      in.ID,
			From:        saga.StatusReserved,
			To:          saga.StatusGatewayPending,
			CurrentStep: saga.StepGateway,
			StepTimeout: o.cfg.StepTimeout,
			RetryCount:  in.RetryCount,
		}); err != nil {
		return err
	}

	payment, err := o.gateway.Pay(ctx, gateway.PaymentRequest{
		IdempotencyKey: key,
		AmountMinor:    p.AmountMinor,
		Currency:       p.Currency,
		Reference:      p.ExternalRef,
	})

	return o.settleGatewayOutcome(ctx, in, attemptID, payment, err, "call")
}

// resolveGateway asks what became of a payment whose outcome is unknown.
//
// This is the ONLY way out of GATEWAY_PENDING other than the original response
// arriving, and it is a query rather than a re-submission on purpose: a GET
// cannot create a payment, so it is safe to repeat against a gateway whose
// state is unknown, which is exactly the situation. Re-POSTing would be safe
// too -- the key is stable and the gateway is idempotent -- but only because of
// a property of the OTHER system, and a design that stakes a customer's money
// on someone else's correctness when it does not have to is a bad trade.
func (o *Orchestrator) resolveGateway(ctx context.Context, in *saga.Instance) error {
	key := gatewayKey(in.ID)

	unresolved, err := o.store.UnresolvedAttempt(ctx, in.ID, saga.StepGateway)
	if err != nil {
		return err
	}
	var attemptID uuid.UUID
	if unresolved != nil {
		attemptID = unresolved.ID
	}

	payment, probeErr := o.gateway.Probe(ctx, key)
	return o.settleGatewayOutcome(ctx, in, attemptID, payment, probeErr, "probe")
}

// settleGatewayOutcome maps a gateway answer onto the state machine.
//
// Three outcomes, and the third is the one the design exists for:
//
//   - confirmed success -> GATEWAY_SUCCEEDED, settle
//   - confirmed failure -> GATEWAY_FAILED, compensate
//   - UNKNOWN           -> stay in GATEWAY_PENDING and probe again later
//
// The third case does nothing, deliberately. Every instinct here is to pick a
// side, and both sides are wrong: assuming failure refunds a customer whose
// money really left, assuming success pays a merchant for a payment that never
// happened. Doing nothing is the only action that cannot create a discrepancy,
// and the money is safe in suspense while it waits.
func (o *Orchestrator) settleGatewayOutcome(
	ctx context.Context,
	in *saga.Instance,
	attemptID uuid.UUID,
	payment *gateway.Payment,
	callErr error,
	source string,
) error {
	if source == "probe" {
		o.countProbe(callErr)
	}

	switch {
	case callErr == nil && payment.Succeeded():
		if err := o.finish(ctx, attemptID, saga.StepSucceeded, nil, ""); err != nil {
			return err
		}
		o.count(saga.StepGateway, saga.DirectionForward, "succeeded")
		return o.store.Advance(ctx, saga.Transition{
			SagaID:       in.ID,
			From:         saga.StatusGatewayPending,
			To:           saga.StatusGatewaySucceeded,
			CurrentStep:  saga.StepSettle,
			StepTimeout:  o.cfg.StepTimeout,
			ReleaseLease: true,
		})

	case gateway.Conclusive(callErr):
		// A conclusive refusal, including "no payment exists under this key".
		// See gateway.ErrPaymentNotFound for the durability assumption that
		// makes the latter safe to act on.
		if err := o.finish(ctx, attemptID, saga.StepFailed, nil, callErr.Error()); err != nil {
			return err
		}
		o.count(saga.StepGateway, saga.DirectionForward, "failed")
		o.logger.InfoContext(ctx, "gateway declined the payment",
			slog.String("saga_id", in.ID.String()),
			slog.String("source", source),
			slog.String("error", callErr.Error()))
		return o.store.Advance(ctx, saga.Transition{
			SagaID:       in.ID,
			From:         saga.StatusGatewayPending,
			To:           saga.StatusGatewayFailed,
			CurrentStep:  saga.StepGateway,
			StepTimeout:  o.cfg.StepTimeout,
			LastError:    callErr.Error(),
			ReleaseLease: true,
		})

	default:
		return o.deferAmbiguity(ctx, in, callErr)
	}
}

// deferAmbiguity leaves the saga unresolved for another probe, or escalates.
//
// The probe budget is what stops "we do not know" from being an infinite loop.
// Exhausting it does NOT resolve the ambiguity -- nothing here can -- it hands
// the saga to a human with the money still parked in suspense, which is the
// honest end state.
func (o *Orchestrator) deferAmbiguity(ctx context.Context, in *saga.Instance, callErr error) error {
	attempts := in.RetryCount + 1
	detail := "gateway did not respond"
	if callErr != nil {
		detail = callErr.Error()
	}

	if attempts >= o.cfg.MaxProbes {
		return o.escalate(ctx, in, escalation{
			from:     saga.StatusGatewayPending,
			step:     saga.StepGateway,
			reason:   "gateway_outcome_unknown",
			lastErr:  detail,
			attempts: attempts,
		})
	}

	o.count(saga.StepGateway, saga.DirectionForward, "unknown")
	o.logger.WarnContext(ctx, "gateway outcome is unknown; will probe again",
		slog.String("saga_id", in.ID.String()),
		slog.Int("probe", attempts),
		slog.Int("max_probes", o.cfg.MaxProbes),
		slog.String("error", detail))

	// Same status in and out. The guarded UPDATE still applies -- it refuses if
	// another replica has since resolved the saga -- and the deadline it pushes
	// forward is what schedules the next probe.
	return o.store.Advance(ctx, saga.Transition{
		SagaID:       in.ID,
		From:         saga.StatusGatewayPending,
		To:           saga.StatusGatewayPending,
		CurrentStep:  saga.StepGateway,
		StepTimeout:  o.cfg.StepTimeout,
		RetryCount:   attempts,
		LastError:    detail,
		ReleaseLease: true,
	})
}

// settle drains suspense to the merchant and to fee revenue.
func (o *Orchestrator) settle(ctx context.Context, in *saga.Instance, p Payload) error {
	attemptID, number, err := o.nextAttempt(ctx, in.ID, saga.StepSettle, saga.DirectionForward)
	if err != nil {
		return err
	}

	key := stepKey(in.ID, saga.StepSettle, saga.DirectionForward)

	entries := []ledger.EntryRequest{
		{
			AccountID: p.SuspenseID,
			Direction: ledger.DirectionDebit,
			Amount:    ledger.MustNewMoney(p.AmountMinor, p.Currency),
		},
		{
			AccountID: p.MerchantPayableID,
			Direction: ledger.DirectionCredit,
			Amount:    ledger.MustNewMoney(p.merchantMinor(), p.Currency),
		},
	}
	// A zero fee gets no leg at all: journal_entries_amount_check requires a
	// positive amount, so a zero-amount entry is not merely pointless, it is
	// rejected.
	if p.FeeMinor > 0 {
		entries = append(entries, ledger.EntryRequest{
			AccountID: p.FeeRevenueID,
			Direction: ledger.DirectionCredit,
			Amount:    ledger.MustNewMoney(p.FeeMinor, p.Currency),
		})
	}

	_, err = o.ledger.PostTransaction(ctx, ledger.TransactionRequest{
		PrincipalID:    in.PrincipalID,
		Type:           ledger.TransactionTypePayout,
		ExternalRef:    externalRef(p),
		IdempotencyKey: &key,
		Metadata: map[string]any{
			MetadataKeySagaID: in.ID.String(),
			MetadataKeyStep:   string(saga.StepSettle),
		},
		Entries: entries,
		Record: func(ctx context.Context, tx ledger.Tx, posted *ledger.Transaction) error {
			if err := tx.ApplyPendingDelta(ctx, ledger.PendingDelta{
				AccountID:  p.SuspenseID,
				DeltaMinor: -p.AmountMinor,
			}); err != nil {
				return err
			}

			// The crash window: journal entries are inserted, balances are
			// moved, and the saga still says GATEWAY_SUCCEEDED. A process that
			// dies here must leave NONE of it behind.
			if o.beforeSettleCommit != nil {
				o.beforeSettleCommit()
			}

			return o.commitStep(ctx, tx, in, commit{
				attemptID:     attemptID,
				number:        number,
				step:          saga.StepSettle,
				direction:     saga.DirectionForward,
				transactionID: &posted.ID,
				from:          saga.StatusGatewaySucceeded,
				to:            saga.StatusCompleted,
				nextStep:      saga.StepDone,
			})
		},
	})
	if err != nil {
		return o.failForwardStep(ctx, in, saga.StepSettle, attemptID, number, err)
	}

	o.count(saga.StepSettle, saga.DirectionForward, "succeeded")
	o.logger.InfoContext(ctx, "payout saga completed", slog.String("saga_id", in.ID.String()))
	return nil
}

// compensate reverses the reserve transaction, returning the money to the
// customer's wallet.
//
// IDEMPOTENT, and the mechanism is inherited rather than invented. A reversal
// is guarded by `UPDATE transactions SET status = 'REVERSED' WHERE id = $1 AND
// status = 'POSTED'` (D15), so a second compensation matches nothing and gets
// ErrAlreadyReversed. The saga transition is guarded on COMPENSATING for the
// same reason. Two independent guards, either of which alone would prevent the
// money being returned twice -- which matters, because two reversals would each
// balance perfectly on their own and the balance invariant would not notice.
func (o *Orchestrator) compensate(ctx context.Context, in *saga.Instance, p Payload) error {
	reserveTxID, err := o.reserveTransactionID(ctx, in.ID)
	if err != nil {
		return err
	}
	if reserveTxID == uuid.Nil {
		// Nothing was ever posted, so there is nothing to reverse. Reaching
		// COMPENSATING without a reserve transaction means the saga failed
		// before it moved anything, and the honest terminal state is
		// COMPENSATED with an empty compensation.
		return o.store.Advance(ctx, saga.Transition{
			SagaID:       in.ID,
			From:         saga.StatusCompensating,
			To:           saga.StatusCompensated,
			CurrentStep:  saga.StepDone,
			StepTimeout:  o.cfg.StepTimeout,
			ReleaseLease: true,
		})
	}

	attemptID, number, err := o.nextAttempt(ctx, in.ID, saga.StepReserve, saga.DirectionCompensation)
	if err != nil {
		return err
	}

	reason := fmt.Sprintf("saga %s compensating: %s", in.ID, truncate(in.LastError, 200))

	_, err = o.ledger.ReverseTransactionRecorded(ctx, reserveTxID, reason,
		func(ctx context.Context, tx ledger.Tx, posted *ledger.Transaction) error {
			if err := tx.ApplyPendingDelta(ctx, ledger.PendingDelta{
				AccountID:  p.SuspenseID,
				DeltaMinor: -p.AmountMinor,
			}); err != nil {
				return err
			}
			return o.commitStep(ctx, tx, in, commit{
				attemptID:     attemptID,
				number:        number,
				step:          saga.StepReserve,
				direction:     saga.DirectionCompensation,
				transactionID: &posted.ID,
				from:          saga.StatusCompensating,
				to:            saga.StatusCompensated,
				nextStep:      saga.StepDone,
			})
		})

	switch {
	case err == nil:
		o.count(saga.StepReserve, saga.DirectionCompensation, "succeeded")
		o.logger.InfoContext(ctx, "payout saga compensated", slog.String("saga_id", in.ID.String()))
		return nil

	case errors.Is(err, ledger.ErrAlreadyReversed), errors.Is(err, saga.ErrStaleTransition):
		// Two replicas compensated at once and this one lost. Both guards did
		// their job; the money went back exactly once. Confirm rather than
		// assume: if the saga really is compensated, this is a benign race.
		return o.confirmCompensated(ctx, in, err)

	default:
		return o.failCompensation(ctx, in, attemptID, number, err)
	}
}

// confirmCompensated distinguishes a lost race from a genuine anomaly.
//
// Losing a race to another replica is fine and expected. Finding the reserve
// transaction reversed while this saga is still COMPENSATING is not: it means
// something outside the saga reversed it, and an automatic response to that
// would be guessing about money somebody else moved.
func (o *Orchestrator) confirmCompensated(ctx context.Context, in *saga.Instance, cause error) error {
	current, err := o.store.Get(ctx, in.ID)
	if err != nil {
		return err
	}
	if current.Status == saga.StatusCompensated {
		o.logger.InfoContext(ctx, "compensation already applied by another orchestrator",
			slog.String("saga_id", in.ID.String()))
		return nil
	}

	return o.escalate(ctx, in, escalation{
		from:     current.Status,
		step:     saga.StepReserve,
		reason:   "reserve_reversed_outside_saga",
		lastErr:  cause.Error(),
		attempts: in.RetryCount,
	})
}

// failForwardStep records a failed forward attempt and decides what happens
// next.
//
// The split between terminal and retryable is where a saga either fails fast or
// hammers a database that is already unhappy. A rejection that is a property of
// the REQUEST -- insufficient funds for this amount, a frozen account, a
// malformed entry -- will not become true by being retried, so the saga fails
// now. A rejection that is a property of the WORLD gets the attempt budget.
func (o *Orchestrator) failForwardStep(
	ctx context.Context,
	in *saga.Instance,
	step saga.Step,
	attemptID uuid.UUID,
	number int,
	cause error,
) error {
	if recordErr := o.store.RecordAttempt(ctx, saga.Attempt{
		ID:        attemptID,
		SagaID:    in.ID,
		Step:      step,
		Number:    number,
		Direction: saga.DirectionForward,
		Status:    saga.StepFailed,
		Error:     truncate(cause.Error(), 1000),
	}); recordErr != nil {
		o.logger.ErrorContext(ctx, "record failed saga attempt",
			slog.String("saga_id", in.ID.String()), slog.String("error", recordErr.Error()))
	}

	attempts := in.RetryCount + 1
	terminal := terminalLedgerError(cause) || attempts >= o.cfg.MaxStepAttempts

	if !terminal {
		o.count(step, saga.DirectionForward, "retry")
		return o.store.Advance(ctx, saga.Transition{
			SagaID:       in.ID,
			From:         in.Status,
			To:           in.Status,
			CurrentStep:  step,
			StepTimeout:  o.cfg.StepTimeout,
			RetryCount:   attempts,
			LastError:    truncate(cause.Error(), 1000),
			ReleaseLease: true,
		})
	}

	o.count(step, saga.DirectionForward, "failed")
	o.logger.WarnContext(ctx, "payout saga step failed",
		slog.String("saga_id", in.ID.String()),
		slog.String("step", string(step)),
		slog.Int("attempts", attempts),
		slog.String("error", cause.Error()))

	// RESERVE moved nothing, so there is nothing to compensate: FAILED, not
	// COMPENSATED. Distinguishing them matters because "we never touched this
	// customer's money" and "we took it and gave it back" are different facts
	// and support will be asked about both.
	if step == saga.StepReserve {
		return o.store.Advance(ctx, saga.Transition{
			SagaID:       in.ID,
			From:         in.Status,
			To:           saga.StatusFailed,
			CurrentStep:  saga.StepDone,
			StepTimeout:  o.cfg.StepTimeout,
			RetryCount:   attempts,
			LastError:    truncate(cause.Error(), 1000),
			ReleaseLease: true,
		})
	}

	return o.store.Advance(ctx, saga.Transition{
		SagaID:       in.ID,
		From:         in.Status,
		To:           saga.StatusCompensating,
		CurrentStep:  saga.StepReserve,
		StepTimeout:  o.cfg.StepTimeout,
		RetryCount:   0,
		LastError:    truncate(cause.Error(), 1000),
		ReleaseLease: true,
	})
}

// failCompensation records a failed compensation and escalates once its budget
// is gone.
//
// The budget is larger than a forward step's, and the asymmetry is deliberate:
// giving up on a forward step costs nothing, while giving up on a compensation
// strands a customer's money in a suspense account.
func (o *Orchestrator) failCompensation(
	ctx context.Context,
	in *saga.Instance,
	attemptID uuid.UUID,
	number int,
	cause error,
) error {
	if recordErr := o.store.RecordAttempt(ctx, saga.Attempt{
		ID:        attemptID,
		SagaID:    in.ID,
		Step:      saga.StepReserve,
		Number:    number,
		Direction: saga.DirectionCompensation,
		Status:    saga.StepFailed,
		Error:     truncate(cause.Error(), 1000),
	}); recordErr != nil {
		o.logger.ErrorContext(ctx, "record failed compensation attempt",
			slog.String("saga_id", in.ID.String()), slog.String("error", recordErr.Error()))
	}

	attempts := in.RetryCount + 1
	if attempts >= o.cfg.MaxCompensationAttempts {
		o.count(saga.StepReserve, saga.DirectionCompensation, "failed")
		return o.escalate(ctx, in, escalation{
			from:     saga.StatusCompensating,
			step:     saga.StepReserve,
			reason:   "compensation_exhausted",
			lastErr:  cause.Error(),
			attempts: attempts,
		})
	}

	o.count(saga.StepReserve, saga.DirectionCompensation, "retry")
	o.logger.WarnContext(ctx, "compensation failed; will retry",
		slog.String("saga_id", in.ID.String()),
		slog.Int("attempt", attempts),
		slog.Int("max_attempts", o.cfg.MaxCompensationAttempts),
		slog.String("error", cause.Error()))

	return o.store.Advance(ctx, saga.Transition{
		SagaID:       in.ID,
		From:         saga.StatusCompensating,
		To:           saga.StatusCompensating,
		CurrentStep:  saga.StepReserve,
		StepTimeout:  o.cfg.StepTimeout,
		RetryCount:   attempts,
		LastError:    truncate(cause.Error(), 1000),
		ReleaseLease: true,
	})
}

// escalation is the reason a saga is being handed to a human.
type escalation struct {
	from     saga.Status
	step     saga.Step
	reason   string
	lastErr  string
	attempts int
}

// escalate stops automation and raises the alert.
//
// WHY AUTOMATIC RESOLUTION IS DANGEROUS HERE, since this is the function that
// would have to implement it. A compensation that has failed its whole budget
// failed for a reason this process does not understand. Either it was transient
// -- and the retries already covered that -- or it is semantically impossible:
// the account is frozen, the suspense funds moved by another path, the reversal
// would drive an account negative. In that second case no number of retries
// helps, and the only "automatic resolutions" available are force-posting with
// allow_negative or writing a balancing ADJUSTMENT. Both mint money that no
// business event justifies, and a ledger that can silently self-heal is a
// ledger whose balances are not evidence of anything.
//
// So this stops. The money stays in the suspense account -- a wrong state, but
// a named and audited one -- the event goes out, the counter moves, and a
// person decides. That is worse for the dashboard and better for the ledger.
func (o *Orchestrator) escalate(ctx context.Context, in *saga.Instance, e escalation) error {
	payload, err := json.Marshal(saga.ManualReviewEvent{
		SagaID:     in.ID,
		SagaType:   in.SagaType,
		Step:       e.step,
		Reason:     e.reason,
		LastError:  truncate(e.lastErr, 1000),
		Attempts:   e.attempts,
		OccurredAt: time.Now().UTC(),
	})
	if err != nil {
		return fmt.Errorf("encode manual review alert for saga %s: %w", in.ID, err)
	}

	alert := outbox.Event{
		AggregateType: outbox.AggregateSaga,
		AggregateID:   in.ID.String(),
		EventType:     saga.EventTypeSagaNeedsManualReview,
		EventVersion:  saga.EventVersion,
		OccurredAt:    time.Now().UTC(),
		Payload:       payload,
	}

	if err := o.store.Escalate(ctx, saga.Transition{
		SagaID:       in.ID,
		From:         e.from,
		To:           saga.StatusNeedsManualReview,
		CurrentStep:  e.step,
		StepTimeout:  o.cfg.StepTimeout,
		RetryCount:   e.attempts,
		LastError:    truncate(e.lastErr, 1000),
		ReleaseLease: true,
	}, alert); err != nil {
		return err
	}

	if o.metrics != nil {
		o.metrics.SagaManualReview.WithLabelValues(in.SagaType, e.reason).Inc()
	}

	// ERROR rather than WARN: this is the line that should page somebody, and
	// it carries everything needed to act without opening a database.
	o.logger.ErrorContext(ctx, "saga needs manual review",
		slog.String("saga_id", in.ID.String()),
		slog.String("saga_type", in.SagaType),
		slog.String("step", string(e.step)),
		slog.String("reason", e.reason),
		slog.Int("attempts", e.attempts),
		slog.String("last_error", e.lastErr))

	return nil
}
