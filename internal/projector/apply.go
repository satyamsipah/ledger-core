package projector

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/satyamsipah/ledger-core/internal/ledger"
	"github.com/satyamsipah/ledger-core/internal/outbox"
)

// Applier applies decoded events to balance_projections.
type Applier struct {
	pool *pgxpool.Pool
}

// NewApplier builds an Applier over the projection database.
func NewApplier(pool *pgxpool.Pool) *Applier {
	return &Applier{pool: pool}
}

// transactionPayload is the subset of TransactionPosted/TransactionReversed's
// business payload this package reads.
//
// A projector-owned type rather than importing internal/ledger's own
// (unexported) transactionEvent: the wire format -- what Append actually put
// on Kafka -- is the contract between the write path and every consumer, not
// the producer's private Go type. Field names below are chosen to match that
// wire format exactly (see appendTransactionEvent in internal/ledger/service.go),
// and matching it is what this package is actually coupled to, deliberately
// looser than a shared struct would be.
type transactionPayload struct {
	TransactionID uuid.UUID        `json:"transaction_id"`
	Balances      []balancePayload `json:"balances"`
}

type balancePayload struct {
	AccountID uuid.UUID    `json:"account_id"`
	Available ledger.Money `json:"available"`
	Version   int64        `json:"version"`
}

// ErrUnknownEventType means this package has no apply logic for the event's
// type. Routed to the dead-letter topic rather than treated as a crash: a
// producer emitting a new event type this consumer has not been taught yet is
// a deployment-ordering fact, not a poison message, and the DLQ replay
// procedure (docs/DECISIONS.md) is exactly the mechanism for catching up once
// this consumer is.
var ErrUnknownEventType = errors.New("projector: unknown event type")

// Apply decodes one Kafka message and applies it, inside one local database
// transaction.
//
// # WHY processed_events, GIVEN THE VERSION COMPARE-AND-SET ALREADY HANDLES REDELIVERY
//
// The CAS on balance_projections.version makes re-applying an event already
// seen a no-op -- the incoming version is not greater than the stored one, so
// the UPDATE's WHERE clause matches nothing. What it does NOT cover is the
// window between this transaction committing and this consumer's Kafka offset
// commit succeeding. A crash in that window means the next delivery of the
// SAME message re-enters this function with a message the CAS has already
// silently absorbed -- which is fine for the CAS itself, but leaves no record
// that the event was ever definitively handled, and is the seam any future
// side effect of applying an event (a notification, a metric with exactly-once
// intent) would need. processed_events, checked and inserted in the SAME
// transaction as the projection update, is what makes "have I handled
// event_id X" an answerable question rather than an inference from the
// projection's current state -- the identical reasoning idempotency_keys
// applies to the write path in Phase 3, transplanted to the read side.
//
// Returns (applied=false, nil) for a duplicate: the caller commits the Kafka
// offset either way, because a message this function has already durably
// recorded handling does not need handling again.
func (a *Applier) Apply(ctx context.Context, envelope outbox.Envelope) (applied bool, err error) {
	switch envelope.EventType {
	case "TransactionPosted", "TransactionReversed":
	default:
		return false, fmt.Errorf("event %s type %q: %w", envelope.EventID, envelope.EventType, ErrUnknownEventType)
	}

	var payload transactionPayload
	if err := json.Unmarshal(envelope.Payload, &payload); err != nil {
		return false, fmt.Errorf("decode %s payload for event %s: %w", envelope.EventType, envelope.EventID, err)
	}

	tx, err := a.pool.Begin(ctx)
	if err != nil {
		return false, fmt.Errorf("begin apply transaction for event %s: %w", envelope.EventID, err)
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()

	tag, err := tx.Exec(ctx, `
		INSERT INTO processed_events (event_id, event_type)
		VALUES ($1, $2)
		ON CONFLICT (event_id) DO NOTHING`,
		envelope.EventID, envelope.EventType)
	if err != nil {
		return false, fmt.Errorf("record processed event %s: %w", envelope.EventID, err)
	}
	if tag.RowsAffected() == 0 {
		// Already handled. Not an error -- at-least-once delivery guarantees
		// this happens in normal operation, not only during a crash test.
		return false, nil
	}

	for _, b := range payload.Balances {
		if err := applyBalance(ctx, tx, envelope.EventID, b); err != nil {
			return false, err
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return false, fmt.Errorf("commit apply transaction for event %s: %w", envelope.EventID, err)
	}

	return true, nil
}

// applyBalance is the version compare-and-set. The WHERE clause on the DO
// UPDATE branch is what makes this safe against out-of-order arrival: an
// incoming version no greater than what is stored leaves the row untouched
// rather than erroring, because "this event is stale" is an expected outcome
// under at-least-once delivery, not a failure.
func applyBalance(ctx context.Context, tx pgx.Tx, eventID uuid.UUID, b balancePayload) error {
	_, err := tx.Exec(ctx, `
		INSERT INTO balance_projections (account_id, available_minor, currency, version, last_event_id)
		VALUES ($1, $2, $3, $4, $5)
		ON CONFLICT (account_id) DO UPDATE
		   SET available_minor = EXCLUDED.available_minor,
		       version         = EXCLUDED.version,
		       last_event_id   = EXCLUDED.last_event_id
		 WHERE balance_projections.version < EXCLUDED.version`,
		b.AccountID, b.Available.AmountMinor(), b.Available.Currency(), b.Version, eventID)
	if err != nil {
		return fmt.Errorf("apply balance for account %s from event %s: %w", b.AccountID, eventID, err)
	}
	return nil
}
