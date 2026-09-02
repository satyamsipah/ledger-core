package payout

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/google/uuid"
	"golang.org/x/sync/errgroup"

	"github.com/satyamsipah/ledger-core/internal/gateway"
	"github.com/satyamsipah/ledger-core/internal/ledger"
	"github.com/satyamsipah/ledger-core/internal/outbox"
	"github.com/satyamsipah/ledger-core/internal/saga"
)

// driveConcurrency bounds how many sagas one replica drives at once.
//
// Sized well below LEDGER_POSTGRES_MAX_CONNS: each driven saga holds a pool
// connection for the length of a posting transaction, and an orchestrator that
// could consume the whole pool would starve the API sharing it.
const driveConcurrency = 8

// Run is the claim loop: the orchestrator's main body.
//
// It drives itself from Postgres rather than from Kafka, which is a deliberate
// deviation from the architecture sketch in CLAUDE.md and is recorded as such
// in docs/DECISIONS.md. The short reason: an orchestrator whose state machine
// is advanced by consuming events has its state split across two systems and
// cannot answer "what step is this saga on" without a replay -- and it is
// halfway to the choreography this phase exists to reject.
//
// No leader election, for the reason D31 gives for the polling publisher: the
// claim query uses FOR UPDATE SKIP LOCKED, so N replicas partition the backlog
// with no coordination and a replica that dies mid-batch simply stops holding
// its rows. Electing a leader would add a failure mode and remove throughput.
func (o *Orchestrator) Run(ctx context.Context) error {
	ticker := time.NewTicker(o.cfg.ClaimInterval)
	defer ticker.Stop()

	o.logger.Info("payout orchestrator started",
		slog.String("worker_id", o.cfg.WorkerID),
		slog.Duration("claim_interval", o.cfg.ClaimInterval),
		slog.Int("claim_batch", o.cfg.ClaimBatch))

	for {
		select {
		case <-ctx.Done():
			o.logger.Info("payout orchestrator stopped")
			// Cancellation is how this loop is meant to end, so it is not an
			// error to report upward -- returning ctx.Err() would make a clean
			// shutdown look like a failure in the errgroup that runs it.
			return nil
		case <-ticker.C:
			o.drainRunnable(ctx)
		}
	}
}

// Sweep is the timeout sweeper: it finds sagas whose step deadline has passed
// and either retries them, probes them, or compensates.
//
// It is the only path back to a GATEWAY_PENDING saga, and therefore the only
// thing standing between an unanswered gateway call and a customer's money
// sitting in suspense forever. It also reclaims any saga abandoned by a replica
// that died mid-step, because a dead replica's lease lapses and its saga
// becomes visible here.
//
// One sweep runs immediately at startup rather than waiting a full interval: a
// process that has just restarted is exactly the process most likely to have
// left something stuck.
func (o *Orchestrator) Sweep(ctx context.Context) error {
	ticker := time.NewTicker(o.cfg.SweepInterval)
	defer ticker.Stop()

	o.logger.Info("payout saga sweeper started",
		slog.Duration("interval", o.cfg.SweepInterval),
		slog.Duration("step_timeout", o.cfg.StepTimeout))

	o.SweepOnce(ctx)

	for {
		select {
		case <-ctx.Done():
			o.logger.Info("payout saga sweeper stopped")
			return nil
		case <-ticker.C:
			o.SweepOnce(ctx)
		}
	}
}

// SweepOnce runs a single sweep, exported so a test can drive the recovery path
// deterministically instead of waiting on a ticker.
func (o *Orchestrator) SweepOnce(ctx context.Context) {
	o.refreshGauges(ctx)
	o.drain(ctx, o.store.ClaimExpired, "sweep")
}

// DriveOnce runs a single claim-and-drive pass, exported for the same reason as
// SweepOnce.
func (o *Orchestrator) DriveOnce(ctx context.Context) { o.drainRunnable(ctx) }

func (o *Orchestrator) drainRunnable(ctx context.Context) {
	o.drain(ctx, o.store.ClaimRunnable, "claim")
}

type claimFunc func(ctx context.Context, sagaType, owner string, lease time.Duration, batch int) ([]saga.Instance, error)

// drain claims and drives batches until a short one comes back.
//
// Draining rather than one batch per tick keeps a backlog from being metered
// out at the ticker's rate: a hundred sagas arriving at once should be worked
// through as fast as the database allows, not over the next twenty-five
// seconds. A short batch means the backlog is gone and the ticker takes over.
func (o *Orchestrator) drain(ctx context.Context, claim claimFunc, mode string) {
	for {
		claimed, err := claim(ctx, o.cfg.SagaType, o.cfg.WorkerID, o.cfg.Lease, o.cfg.ClaimBatch)
		if err != nil {
			if !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
				o.logger.ErrorContext(ctx, "claim sagas",
					slog.String("mode", mode), slog.String("error", err.Error()))
			}
			return
		}
		if len(claimed) == 0 {
			return
		}

		group, groupCtx := errgroup.WithContext(ctx)
		group.SetLimit(driveConcurrency)
		for i := range claimed {
			group.Go(func() error {
				o.driveOne(groupCtx, &claimed[i])
				// Never propagated: one saga's failure must not cancel the
				// batch, and every failure is already recorded on the saga
				// itself. Returning it here would abandon its siblings'
				// leases for no benefit.
				return nil
			})
		}
		_ = group.Wait()

		if len(claimed) < o.cfg.ClaimBatch {
			return
		}
		select {
		case <-ctx.Done():
			return
		default:
		}
	}
}

// driveOne advances a single saga by exactly one step.
//
// One step per claim, not a loop to completion. A saga that ran to completion
// inside one claim would hold its lease across several posting transactions and
// a network call, which is both a long-held lease and a long-running unit of
// work that a crash makes maximally ambiguous. Stepping once and releasing
// means every crash lands on a boundary that the persisted status already
// describes.
func (o *Orchestrator) driveOne(ctx context.Context, in *saga.Instance) {
	if in.SagaType != o.cfg.SagaType {
		// Almost always a saga written by a newer deployment. Logged and left
		// alone rather than failed: a rolling deploy must not mark the new
		// version's sagas dead, and the lease it holds lapses on its own.
		o.logger.WarnContext(ctx, "skipping saga of unknown type",
			slog.String("saga_id", in.ID.String()),
			slog.String("saga_type", in.SagaType),
			slog.String("error", saga.ErrUnknownSagaType.Error()))
		return
	}

	p, err := unmarshalPayload(in)
	if err != nil {
		o.logger.ErrorContext(ctx, "decode saga payload",
			slog.String("saga_id", in.ID.String()), slog.String("error", err.Error()))
		return
	}

	switch in.Status {
	case saga.StatusPending:
		err = o.reserve(ctx, in, p)
	case saga.StatusReserved:
		err = o.callGateway(ctx, in, p)
	case saga.StatusGatewayPending:
		err = o.resolveGateway(ctx, in)
	case saga.StatusGatewaySucceeded:
		err = o.settle(ctx, in, p)
	case saga.StatusGatewayFailed:
		err = o.beginCompensation(ctx, in)
	case saga.StatusCompensating:
		err = o.compensate(ctx, in, p)
	default:
		// Terminal. It was claimed only because a deadline was left in the
		// past, which is harmless; the lease lapses.
		return
	}

	switch {
	case err == nil:
	case errors.Is(err, saga.ErrStaleTransition), errors.Is(err, saga.ErrLeaseLost):
		// Another replica got there first. Both guards exist precisely so this
		// is a log line rather than a corruption.
		o.logger.InfoContext(ctx, "saga was advanced by another orchestrator",
			slog.String("saga_id", in.ID.String()),
			slog.String("status", string(in.Status)))
	case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
		// Shutdown, or a step that outran its budget. The lease lapses and the
		// sweeper picks the saga up.
	default:
		o.logger.ErrorContext(ctx, "drive saga",
			slog.String("saga_id", in.ID.String()),
			slog.String("status", string(in.Status)),
			slog.String("error", err.Error()))
	}
}

// beginCompensation turns a confirmed gateway failure into the decision to run
// backwards.
//
// A separate transition rather than compensating directly, so that "we decided
// to compensate" and "we compensated" are distinguishable states. The retry
// counter is reset here because the compensation gets its own, larger budget.
func (o *Orchestrator) beginCompensation(ctx context.Context, in *saga.Instance) error {
	return o.store.Advance(ctx, saga.Transition{
		SagaID:       in.ID,
		From:         saga.StatusGatewayFailed,
		To:           saga.StatusCompensating,
		CurrentStep:  saga.StepReserve,
		StepTimeout:  o.cfg.StepTimeout,
		RetryCount:   0,
		LastError:    in.LastError,
		ReleaseLease: true,
	})
}

// commit is the shape of one successful step's bookkeeping.
type commit struct {
	attemptID     uuid.UUID
	number        int
	step          saga.Step
	direction     saga.Direction
	transactionID *uuid.UUID
	from, to      saga.Status
	nextStep      saga.Step
}

// commitStep writes the audit row, the transition and the event, all inside the
// ledger's transaction.
//
// The event goes here rather than after COMMIT for the reason invariant 6
// exists: an event published outside the transaction that produced it is a dual
// write, and the outbox is what makes it not one. SagaStepCompleted was
// declared in D32 against an orchestrator that did not exist; this is where it
// is finally emitted.
func (o *Orchestrator) commitStep(ctx context.Context, tx ledger.Tx, in *saga.Instance, c commit) error {
	if err := tx.CommitSagaStep(ctx, saga.StepCommit{
		Attempt: saga.Attempt{
			ID:            c.attemptID,
			SagaID:        in.ID,
			Step:          c.step,
			Number:        c.number,
			Direction:     c.direction,
			Status:        saga.StepSucceeded,
			TransactionID: c.transactionID,
		},
		Transition: saga.Transition{
			SagaID:      in.ID,
			From:        c.from,
			To:          c.to,
			CurrentStep: c.nextStep,
			StepTimeout: o.cfg.StepTimeout,

			// Every completed step hands the lease back. The orchestrator
			// advances a saga by exactly one step per claim, so holding the
			// lease past the commit would block the next step behind a lease
			// nobody is using -- and would make a replica's death cost a full
			// lease of latency on a saga that is not even in flight.
			ReleaseLease: true,
		},
	}); err != nil {
		return err
	}

	payload, err := json.Marshal(saga.StepCompletedEvent{
		SagaID:        in.ID,
		SagaType:      in.SagaType,
		Step:          c.step,
		Direction:     c.direction,
		Attempt:       c.number,
		Status:        c.to,
		TransactionID: c.transactionID,
		OccurredAt:    time.Now().UTC(),
	})
	if err != nil {
		return fmt.Errorf("encode step event for saga %s: %w", in.ID, err)
	}

	return tx.AppendEvent(ctx, outbox.Event{
		AggregateType: outbox.AggregateSaga,
		AggregateID:   in.ID.String(),
		EventType:     saga.EventTypeSagaStepCompleted,
		EventVersion:  saga.EventVersion,
		OccurredAt:    time.Now().UTC(),
		Payload:       payload,
	})
}

// nextAttempt allocates an attempt id and number.
//
// The id is generated here, outside anything that might retry, for the same
// reason PostTransaction generates its transaction id outside the retrier: a
// fresh id per attempt would let a commit that was reported as failed be
// recorded twice, where a reused one collides on the unique constraint and says
// so.
func (o *Orchestrator) nextAttempt(
	ctx context.Context,
	sagaID uuid.UUID,
	step saga.Step,
	direction saga.Direction,
) (uuid.UUID, int, error) {
	number, err := o.store.NextAttemptNumber(ctx, sagaID, step, direction)
	if err != nil {
		return uuid.Nil, 0, err
	}
	id, err := uuid.NewV7()
	if err != nil {
		return uuid.Nil, 0, fmt.Errorf("generate attempt id for saga %s: %w", sagaID, err)
	}
	return id, number, nil
}

// finish closes an ATTEMPTED row, tolerating the case where there is none.
func (o *Orchestrator) finish(ctx context.Context, attemptID uuid.UUID, status saga.StepStatus, txID *uuid.UUID, failure string) error {
	if attemptID == uuid.Nil {
		return nil
	}
	return o.store.FinishAttempt(ctx, saga.FinishAttempt{
		ID:            attemptID,
		Status:        status,
		TransactionID: txID,
		Error:         truncate(failure, 1000),
	})
}

// reserveTransactionID finds the ledger transaction the reserve step posted,
// which is the transaction a compensation reverses.
//
// Read from saga_steps rather than recomputed, because the compensation must
// reverse the transaction that ACTUALLY posted -- the one whose entries name
// the physical shard accounts the money went to. Re-deriving it would risk
// reversing something else that balances just as well.
func (o *Orchestrator) reserveTransactionID(ctx context.Context, sagaID uuid.UUID) (uuid.UUID, error) {
	attempts, err := o.store.Attempts(ctx, sagaID)
	if err != nil {
		return uuid.Nil, err
	}
	for _, a := range attempts {
		if a.Step == saga.StepReserve && a.Direction == saga.DirectionForward &&
			a.Status == saga.StepSucceeded && a.TransactionID != nil {
			return *a.TransactionID, nil
		}
	}
	return uuid.Nil, nil
}

// refreshGauges republishes the population by status.
func (o *Orchestrator) refreshGauges(ctx context.Context) {
	if o.metrics == nil {
		return
	}
	counts, err := o.store.CountByStatus(ctx)
	if err != nil {
		o.logger.WarnContext(ctx, "refresh saga gauges", slog.String("error", err.Error()))
		return
	}
	// Every status is published, including the zeroes. A gauge that simply
	// stops being reported when its population empties looks identical to a
	// scrape failure, and NEEDS_MANUAL_REVIEW is the one series where "no data"
	// must never be mistaken for "none".
	for _, status := range allStatuses {
		o.metrics.SagaInstances.WithLabelValues(string(status)).Set(float64(counts[status]))
	}

	overdue, err := o.store.OldestOverdueSeconds(ctx)
	if err != nil {
		o.logger.WarnContext(ctx, "refresh saga oldest-overdue gauge", slog.String("error", err.Error()))
		return
	}
	o.metrics.SagaOldestOverdueSeconds.Set(overdue)
}

var allStatuses = []saga.Status{
	saga.StatusPending, saga.StatusReserved, saga.StatusGatewayPending,
	saga.StatusGatewaySucceeded, saga.StatusGatewayFailed, saga.StatusCompensating,
	saga.StatusCompleted, saga.StatusCompensated, saga.StatusFailed,
	saga.StatusNeedsManualReview,
}

func (o *Orchestrator) count(step saga.Step, direction saga.Direction, outcome string) {
	if o.metrics == nil {
		return
	}
	o.metrics.SagaSteps.WithLabelValues(string(step), string(direction), outcome).Inc()
}

func (o *Orchestrator) countProbe(err error) {
	if o.metrics == nil {
		return
	}
	outcome := "conclusive"
	if !gateway.Conclusive(err) {
		outcome = "unknown"
	}
	o.metrics.SagaGatewayProbes.WithLabelValues(outcome).Inc()
}

// terminalLedgerError reports whether a ledger rejection will still be a
// rejection on the next attempt.
//
// The list is the saga's counterpart of the deterministic/transient split the
// idempotency layer makes (D22), and it differs from it in one place worth
// naming: ErrInsufficientFunds is TRANSIENT for an idempotency key, because the
// account may be funded a second later and the client's key should survive that
// wait. It is TERMINAL for a payout saga, because a payout is for a specific
// amount at a specific moment and a saga is not a standing order. Retrying it
// would leave sagas circling for hours against wallets that are simply empty.
func terminalLedgerError(err error) bool {
	switch {
	case errors.Is(err, ledger.ErrInsufficientFunds),
		errors.Is(err, ledger.ErrAccountNotPostable),
		errors.Is(err, ledger.ErrAccountNotFound),
		errors.Is(err, ledger.ErrCurrencyMismatch),
		errors.Is(err, ledger.ErrMixedCurrency),
		errors.Is(err, ledger.ErrInvalidCurrency),
		errors.Is(err, ledger.ErrScaleMismatch),
		errors.Is(err, ledger.ErrInvalidEntry),
		errors.Is(err, ledger.ErrTooFewEntries),
		errors.Is(err, ledger.ErrUnbalancedTransaction),
		errors.Is(err, ledger.ErrMoneyOverflow),
		errors.Is(err, ledger.ErrInvalidTransactionType),
		errors.Is(err, ledger.ErrDuplicateIdempotencyKey),
		errors.Is(err, ledger.ErrAlreadyReversed),
		errors.Is(err, ledger.ErrTransactionNotPosted):
		return true
	default:
		return false
	}
}

func externalRef(p Payload) *string {
	if p.ExternalRef == "" {
		return nil
	}
	ref := p.ExternalRef
	return &ref
}

// truncate bounds a stored error message. last_error is read by humans and
// written by whatever failed, and a driver dumping a query plan into it would
// otherwise become a row nobody can display.
func truncate(s string, max int) string {
	if len(s) <= max {
		return s
	}
	return s[:max] + "…"
}
