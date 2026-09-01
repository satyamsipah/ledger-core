package pgsaga

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/satyamsipah/ledger-core/internal/saga"
)

// execer is the sliver of pgx that both a pool and a transaction satisfy, so
// the guarded transition is written once and cannot drift between the
// standalone path and the in-ledger-transaction path.
type execer interface {
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
}

// CommitStep records a completed attempt and applies the transition it causes,
// inside the caller's transaction.
//
// THIS IS THE FUNCTION THE WHOLE PHASE TURNS ON, so it is worth being explicit
// about what it buys. It is reached from internal/ledger's Tx port, which means
// the journal entries for a step, the balance updates they imply, the
// pending_minor hold, the saga's move to its next status, and the audit row
// recording the attempt are ONE COMMIT. There is no instant at which the money
// has moved and the saga does not know it, and no instant at which the saga
// believes a step succeeded that did not.
//
// The alternative -- post through the ledger, then update the saga in a second
// transaction -- is the shape docs/DECISIONS.md D20 identifies as the bug that
// phase existed to remove: the work commits, the bookkeeping does not, and the
// resumed process re-runs a step that already moved real money. Sagas make that
// worse than idempotency keys did, because the step being re-run is a debit
// against a customer.
//
// It takes a pgx.Tx and not a pool, for the same reason outbox.Append does: a
// signature accepting a pool would make the non-atomic version trivially
// expressible, so it does not accept one.
func CommitStep(ctx context.Context, tx pgx.Tx, c saga.StepCommit) error {
	a := c.Attempt
	_, err := tx.Exec(ctx, `
		INSERT INTO saga_steps
		    (id, saga_id, step, attempt, direction, status, transaction_id,
		     gateway_key, error, finished_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, NULLIF($8, ''), NULLIF($9, ''), now())`,
		a.ID, a.SagaID, a.Step, a.Number, a.Direction, a.Status,
		a.TransactionID, a.GatewayKey, a.Error)
	if err != nil {
		return fmt.Errorf("record attempt %d of %s on saga %s: %w", a.Number, a.Step, a.SagaID, err)
	}

	if err := advance(ctx, tx, c.Transition); err != nil {
		return err
	}
	return nil
}

// Advance applies a guarded transition inside the caller's transaction, for
// steps that move no money of their own but must still not race a competing
// orchestrator.
func Advance(ctx context.Context, tx pgx.Tx, t saga.Transition) error {
	return advance(ctx, tx, t)
}

// advance is the guarded state transition, shared by every caller.
//
// The `status = $2` guard is what makes a transition safe to attempt from an
// orchestrator that may no longer own the saga. A replica whose lease expired
// mid-step can still be running; when it finally tries to advance, another
// replica has already moved the saga on, this UPDATE matches nothing, and the
// stale writer is told so instead of overwriting a decision it did not make.
// Reached from inside the ledger transaction, that error also takes the step's
// journal entries down with it -- the loser's work is discarded rather than
// committed alongside the winner's, exactly as idempotency.ErrLeaseLost does on
// the write path.
func advance(ctx context.Context, db execer, t saga.Transition) error {
	tag, err := db.Exec(ctx, `
		UPDATE saga_instances
		   SET status           = $3,
		       current_step     = $4,
		       step_deadline_at = now() + make_interval(secs => $5),
		       retry_count      = $6,
		       last_error       = NULLIF($7, ''),
		       lease_owner      = CASE WHEN $8 THEN NULL ELSE lease_owner END,
		       lease_expires_at = CASE WHEN $8 THEN NULL ELSE lease_expires_at END
		 WHERE id = $1 AND status = $2`,
		t.SagaID, t.From, t.To, t.CurrentStep, t.StepTimeout.Seconds(),
		t.RetryCount, t.LastError, t.ReleaseLease)
	if err != nil {
		return fmt.Errorf("advance saga %s from %s to %s: %w", t.SagaID, t.From, t.To, err)
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("advance saga %s from %s to %s: %w",
			t.SagaID, t.From, t.To, saga.ErrStaleTransition)
	}
	return nil
}
