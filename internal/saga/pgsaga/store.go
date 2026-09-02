// Package pgsaga implements the saga store against PostgreSQL.
//
// Every statement touching saga_instances and saga_steps lives here, including
// CommitStep, which runs inside somebody else's transaction. Gathering them in
// one place is the same deliberate choice pgidem makes: the guarantee this
// schema provides is a property of WHICH TRANSACTION each statement runs in,
// and that is far easier to audit when the statements sit next to each other
// than when the saga transition is buried in the ledger repository beside the
// journal inserts.
package pgsaga

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/satyamsipah/ledger-core/internal/outbox"
	"github.com/satyamsipah/ledger-core/internal/saga"
)

// Compile-time proof that this package satisfies the port it claims to.
var _ saga.Store = (*Store)(nil)

// instanceColumns is the projection every instance read shares, so a column
// added to the table is added in exactly one place.
const instanceColumns = `id, saga_type, principal_id, current_step, status, payload, retry_count,
	COALESCE(last_error, ''), idempotency_key, lease_owner, lease_expires_at,
	step_deadline_at, created_at, updated_at`

// terminalStatuses is the set the sweeper and both claim queries exclude.
// Written out rather than derived from Status.Terminal() because it has to be
// a SQL value, and the migration's partial indexes hardcode the same four --
// if these ever disagree the index silently stops covering the query.
var terminalStatuses = []string{
	string(saga.StatusCompleted),
	string(saga.StatusCompensated),
	string(saga.StatusFailed),
	string(saga.StatusNeedsManualReview),
}

// runnableStatuses are the states the orchestrator can act on immediately.
//
// GATEWAY_PENDING is absent on purpose: a saga waiting on a gateway response
// has nothing to do until its deadline passes. Including it here would make the
// claim loop spin on sagas it can only wait for, and -- worse -- would force a
// probe the instant the call went out, turning every slow-but-healthy gateway
// response into an ambiguity that did not exist.
var runnableStatuses = []string{
	string(saga.StatusPending),
	string(saga.StatusReserved),
	string(saga.StatusGatewaySucceeded),
	string(saga.StatusGatewayFailed),
	string(saga.StatusCompensating),
}

// Store is the PostgreSQL-backed saga store.
type Store struct {
	pool    *pgxpool.Pool
	timeout time.Duration
}

// New builds a store over an existing pool.
func New(pool *pgxpool.Pool, timeout time.Duration) *Store {
	return &Store{pool: pool, timeout: timeout}
}

// Create inserts a new saga, or returns the existing one when the idempotency
// key has been seen before.
//
// A duplicate returns the original rather than an error because a client
// retrying POST /v1/payouts after a timeout is asking "did my payout start?",
// and the honest answer is the saga it started. Raising a conflict would push
// the caller into inventing a new key for an operation it already has one for,
// which is the precise situation idempotency keys exist to prevent.
func (s *Store) Create(ctx context.Context, in saga.Instance) (*saga.Instance, error) {
	ctx, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()

	rows, err := s.pool.Query(ctx, `
		INSERT INTO saga_instances
		    (id, saga_type, principal_id, current_step, status, payload,
		     idempotency_key, step_deadline_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, now() + make_interval(secs => $8))
		ON CONFLICT (principal_id, idempotency_key) WHERE idempotency_key IS NOT NULL
		DO NOTHING
		RETURNING `+instanceColumns,
		in.ID, in.SagaType, in.PrincipalID, in.CurrentStep, in.Status, in.Payload,
		in.IdempotencyKey, time.Until(in.StepDeadlineAt).Seconds())
	if err != nil {
		return nil, fmt.Errorf("insert saga %s: %w", in.ID, err)
	}

	created, err := collectInstances(rows)
	if err != nil {
		return nil, fmt.Errorf("insert saga %s: %w", in.ID, err)
	}
	if len(created) == 1 {
		return &created[0], nil
	}

	// DO NOTHING returned no row, so the key already names a saga. Read it.
	if in.IdempotencyKey == nil {
		return nil, fmt.Errorf("insert saga %s: no row returned and no idempotency key to look it up by", in.ID)
	}
	return s.getByIdempotencyKey(ctx, in.PrincipalID, *in.IdempotencyKey)
}

// getByIdempotencyKey is scoped by principal so that the fallback read after a
// DO NOTHING never returns a different principal's saga -- the same property
// D24 requires of idempotency_keys, extended to this table's own dedupe.
func (s *Store) getByIdempotencyKey(ctx context.Context, principalID, key string) (*saga.Instance, error) {
	rows, err := s.pool.Query(ctx,
		`SELECT `+instanceColumns+` FROM saga_instances WHERE principal_id = $1 AND idempotency_key = $2`,
		principalID, key)
	if err != nil {
		return nil, fmt.Errorf("read saga by idempotency key %s: %w", key, err)
	}
	found, err := collectInstances(rows)
	if err != nil {
		return nil, fmt.Errorf("read saga by idempotency key %s: %w", key, err)
	}
	if len(found) == 0 {
		return nil, fmt.Errorf("idempotency key %s: %w", key, saga.ErrSagaNotFound)
	}
	return &found[0], nil
}

// Get reads one saga by id.
func (s *Store) Get(ctx context.Context, id uuid.UUID) (*saga.Instance, error) {
	ctx, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()

	rows, err := s.pool.Query(ctx,
		`SELECT `+instanceColumns+` FROM saga_instances WHERE id = $1`, id)
	if err != nil {
		return nil, fmt.Errorf("read saga %s: %w", id, err)
	}
	found, err := collectInstances(rows)
	if err != nil {
		return nil, fmt.Errorf("read saga %s: %w", id, err)
	}
	if len(found) == 0 {
		return nil, fmt.Errorf("saga %s: %w", id, saga.ErrSagaNotFound)
	}
	return &found[0], nil
}

// ClaimRunnable takes sagas that can be driven right now.
func (s *Store) ClaimRunnable(ctx context.Context, sagaType, owner string, lease time.Duration, batch int) ([]saga.Instance, error) {
	return s.claim(ctx, `
		UPDATE saga_instances
		   SET lease_owner = $1, lease_expires_at = now() + make_interval(secs => $2)
		 WHERE id IN (
		     SELECT id FROM saga_instances
		      WHERE saga_type = $5
		        AND status = ANY($3)
		        AND (lease_expires_at IS NULL OR lease_expires_at < now())
		      ORDER BY created_at
		      LIMIT $4
		        FOR UPDATE SKIP LOCKED
		 )
		RETURNING `+instanceColumns,
		owner, lease.Seconds(), runnableStatuses, batch, sagaType)
}

// ClaimExpired takes sagas whose step deadline has passed, whatever their
// status. This is the sweeper's query and the only path back to a
// GATEWAY_PENDING saga.
func (s *Store) ClaimExpired(ctx context.Context, sagaType, owner string, lease time.Duration, batch int) ([]saga.Instance, error) {
	return s.claim(ctx, `
		UPDATE saga_instances
		   SET lease_owner = $1, lease_expires_at = now() + make_interval(secs => $2)
		 WHERE id IN (
		     SELECT id FROM saga_instances
		      WHERE saga_type = $5
		        AND status <> ALL($3)
		        AND step_deadline_at < now()
		        AND (lease_expires_at IS NULL OR lease_expires_at < now())
		      ORDER BY step_deadline_at
		      LIMIT $4
		        FOR UPDATE SKIP LOCKED
		 )
		RETURNING `+instanceColumns,
		owner, lease.Seconds(), terminalStatuses, batch, sagaType)
}

// claim runs a claim statement and materialises the rows it took.
//
// FOR UPDATE SKIP LOCKED rather than FOR UPDATE, for the reason D31 sets out at
// length for the polling publisher: without SKIP LOCKED a second replica BLOCKS
// on rows the first has claimed, so N replicas have the effective concurrency of
// one while costing N. Skipping makes them partition the backlog with no
// coordination and nothing to rebalance when one dies mid-batch.
func (s *Store) claim(ctx context.Context, sql, owner string, leaseSeconds float64, statuses []string, batch int, sagaType string) ([]saga.Instance, error) {
	ctx, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()

	rows, err := s.pool.Query(ctx, sql, owner, leaseSeconds, statuses, batch, sagaType)
	if err != nil {
		return nil, fmt.Errorf("claim sagas for %s: %w", owner, err)
	}
	claimed, err := collectInstances(rows)
	if err != nil {
		return nil, fmt.Errorf("claim sagas for %s: %w", owner, err)
	}
	return claimed, nil
}

// Advance applies a guarded transition outside any ledger transaction.
func (s *Store) Advance(ctx context.Context, t saga.Transition) error {
	ctx, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()

	return advance(ctx, s.pool, t)
}

// RenewLease extends this orchestrator's hold on a saga.
func (s *Store) RenewLease(ctx context.Context, id uuid.UUID, owner string, lease time.Duration) error {
	ctx, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()

	tag, err := s.pool.Exec(ctx, `
		UPDATE saga_instances
		   SET lease_expires_at = now() + make_interval(secs => $3)
		 WHERE id = $1 AND lease_owner = $2`,
		id, owner, lease.Seconds())
	if err != nil {
		return fmt.Errorf("renew lease on saga %s: %w", id, err)
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("renew lease on saga %s: %w", id, saga.ErrLeaseLost)
	}
	return nil
}

// BeginStep records the intent row and applies its transition in one
// transaction, and commits it BEFORE the work it describes begins.
//
// Committing first is the entire contract, and it is what makes an unanswered
// gateway call recoverable rather than merely regrettable. What survives a
// crash is a saga in GATEWAY_PENDING plus an ATTEMPTED row naming the key the
// call was about to use -- enough to ask the gateway what happened. Recording
// afterwards instead loses exactly the record that the crash makes necessary,
// leaving a payment that may exist and cannot be named.
func (s *Store) BeginStep(ctx context.Context, a saga.StartAttempt, t saga.Transition) error {
	ctx, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin step %s on saga %s: %w", a.Step, a.SagaID, err)
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()

	if _, err := tx.Exec(ctx, `
		INSERT INTO saga_steps
		    (id, saga_id, step, attempt, direction, status, gateway_key)
		VALUES ($1, $2, $3, $4, $5, 'ATTEMPTED', NULLIF($6, ''))`,
		a.ID, a.SagaID, a.Step, a.Number, a.Direction, a.GatewayKey); err != nil {
		return fmt.Errorf("start attempt %d of %s on saga %s: %w", a.Number, a.Step, a.SagaID, err)
	}

	if err := advance(ctx, tx, t); err != nil {
		return err
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("begin step %s on saga %s: %w", a.Step, a.SagaID, err)
	}
	return nil
}

// RecordAttempt inserts one already-finished attempt.
//
// Used by a ledger step that FAILED: its audit row was written inside the
// transaction that rolled back, so it has to be written again outside one. A
// failed attempt that leaves no trace is the difference between "this saga was
// tried four times and refused for insufficient funds each time" and "this saga
// did nothing for an hour".
func (s *Store) RecordAttempt(ctx context.Context, a saga.Attempt) error {
	ctx, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()

	_, err := s.pool.Exec(ctx, `
		INSERT INTO saga_steps
		    (id, saga_id, step, attempt, direction, status, transaction_id,
		     gateway_key, error, finished_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, NULLIF($8, ''), NULLIF($9, ''), now())`,
		a.ID, a.SagaID, a.Step, a.Number, a.Direction, a.Status,
		a.TransactionID, a.GatewayKey, a.Error)
	if err != nil {
		return fmt.Errorf("record attempt %d of %s on saga %s: %w", a.Number, a.Step, a.SagaID, err)
	}
	return nil
}

// Escalate moves a saga to NEEDS_MANUAL_REVIEW and appends the alert in one
// transaction.
//
// One transaction because the alert IS the escalation as far as anyone outside
// this process is concerned. Writing the status without the event produces a
// saga that has silently stopped -- which is precisely the outcome the state
// was introduced to prevent -- and writing the event without the status pages
// somebody about a saga that is still being retried.
func (s *Store) Escalate(ctx context.Context, t saga.Transition, alert outbox.Event) error {
	ctx, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("escalate saga %s: %w", t.SagaID, err)
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()

	if err := advance(ctx, tx, t); err != nil {
		return err
	}
	if err := outbox.Append(ctx, tx, alert); err != nil {
		return fmt.Errorf("escalate saga %s: %w", t.SagaID, err)
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("escalate saga %s: %w", t.SagaID, err)
	}
	return nil
}

// FinishAttempt records how an attempt turned out.
//
// Guarded on ATTEMPTED so that finishing an attempt twice -- which a redelivery
// or a resumed orchestrator can genuinely try -- is a no-op rather than an
// overwrite of the first, truthful outcome with a second, guessed one.
func (s *Store) FinishAttempt(ctx context.Context, f saga.FinishAttempt) error {
	ctx, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()

	_, err := s.pool.Exec(ctx, `
		UPDATE saga_steps
		   SET status = $2, transaction_id = $3, error = NULLIF($4, ''),
		       finished_at = now()
		 WHERE id = $1 AND status = 'ATTEMPTED'`,
		f.ID, f.Status, f.TransactionID, f.Error)
	if err != nil {
		return fmt.Errorf("finish attempt %s: %w", f.ID, err)
	}
	return nil
}

// UnresolvedAttempt returns the newest ATTEMPTED row for a step, or nil.
func (s *Store) UnresolvedAttempt(ctx context.Context, sagaID uuid.UUID, step saga.Step) (*saga.Attempt, error) {
	ctx, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()

	rows, err := s.pool.Query(ctx, `
		SELECT id, saga_id, step, attempt, direction, status, transaction_id,
		       COALESCE(gateway_key, ''), COALESCE(error, ''), started_at, finished_at
		  FROM saga_steps
		 WHERE saga_id = $1 AND step = $2 AND status = 'ATTEMPTED'
		 ORDER BY started_at DESC
		 LIMIT 1`, sagaID, step)
	if err != nil {
		return nil, fmt.Errorf("read unresolved %s attempt on saga %s: %w", step, sagaID, err)
	}
	found, err := collectAttempts(rows)
	if err != nil {
		return nil, fmt.Errorf("read unresolved %s attempt on saga %s: %w", step, sagaID, err)
	}
	if len(found) == 0 {
		return nil, nil
	}
	return &found[0], nil
}

// Attempts returns a saga's full history, oldest first.
func (s *Store) Attempts(ctx context.Context, sagaID uuid.UUID) ([]saga.Attempt, error) {
	ctx, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()

	rows, err := s.pool.Query(ctx, `
		SELECT id, saga_id, step, attempt, direction, status, transaction_id,
		       COALESCE(gateway_key, ''), COALESCE(error, ''), started_at, finished_at
		  FROM saga_steps
		 WHERE saga_id = $1
		 ORDER BY started_at, attempt`, sagaID)
	if err != nil {
		return nil, fmt.Errorf("read attempts on saga %s: %w", sagaID, err)
	}
	found, err := collectAttempts(rows)
	if err != nil {
		return nil, fmt.Errorf("read attempts on saga %s: %w", sagaID, err)
	}
	return found, nil
}

// NextAttemptNumber returns the number the next attempt should carry.
func (s *Store) NextAttemptNumber(ctx context.Context, sagaID uuid.UUID, step saga.Step, direction saga.Direction) (int, error) {
	ctx, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()

	var next int
	err := s.pool.QueryRow(ctx, `
		SELECT COALESCE(MAX(attempt), 0) + 1
		  FROM saga_steps
		 WHERE saga_id = $1 AND step = $2 AND direction = $3`,
		sagaID, step, direction).Scan(&next)
	if err != nil {
		return 0, fmt.Errorf("read next attempt number for %s on saga %s: %w", step, sagaID, err)
	}
	return next, nil
}

// ListByStatus powers the dashboard's stuck-saga view.
func (s *Store) ListByStatus(ctx context.Context, status saga.Status, limit int) ([]saga.Instance, error) {
	ctx, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()

	rows, err := s.pool.Query(ctx,
		`SELECT `+instanceColumns+`
		   FROM saga_instances
		  WHERE status = $1
		  ORDER BY created_at DESC
		  LIMIT $2`, status, limit)
	if err != nil {
		return nil, fmt.Errorf("list sagas in %s: %w", status, err)
	}
	found, err := collectInstances(rows)
	if err != nil {
		return nil, fmt.Errorf("list sagas in %s: %w", status, err)
	}
	return found, nil
}

// CountByStatus feeds the ledger_saga_instances gauge.
func (s *Store) CountByStatus(ctx context.Context) (map[saga.Status]int, error) {
	ctx, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()

	rows, err := s.pool.Query(ctx, `SELECT status, count(*) FROM saga_instances GROUP BY status`)
	if err != nil {
		return nil, fmt.Errorf("count sagas by status: %w", err)
	}
	defer rows.Close()

	counts := make(map[saga.Status]int)
	for rows.Next() {
		var (
			status saga.Status
			n      int
		)
		if err := rows.Scan(&status, &n); err != nil {
			return nil, fmt.Errorf("count sagas by status: %w", err)
		}
		counts[status] = n
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("count sagas by status: %w", err)
	}
	return counts, nil
}

func collectInstances(rows pgx.Rows) ([]saga.Instance, error) {
	defer rows.Close()

	var found []saga.Instance
	for rows.Next() {
		var in saga.Instance
		if err := rows.Scan(&in.ID, &in.SagaType, &in.PrincipalID, &in.CurrentStep, &in.Status, &in.Payload,
			&in.RetryCount, &in.LastError, &in.IdempotencyKey, &in.LeaseOwner,
			&in.LeaseExpiresAt, &in.StepDeadlineAt, &in.CreatedAt, &in.UpdatedAt); err != nil {
			return nil, fmt.Errorf("scan saga instance: %w", err)
		}
		found = append(found, in)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read saga instances: %w", err)
	}
	return found, nil
}

func collectAttempts(rows pgx.Rows) ([]saga.Attempt, error) {
	defer rows.Close()

	var found []saga.Attempt
	for rows.Next() {
		var a saga.Attempt
		if err := rows.Scan(&a.ID, &a.SagaID, &a.Step, &a.Number, &a.Direction, &a.Status,
			&a.TransactionID, &a.GatewayKey, &a.Error, &a.StartedAt, &a.FinishedAt); err != nil {
			return nil, fmt.Errorf("scan saga attempt: %w", err)
		}
		found = append(found, a)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read saga attempts: %w", err)
	}
	return found, nil
}
