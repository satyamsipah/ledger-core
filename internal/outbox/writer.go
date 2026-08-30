package outbox

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/jackc/pgx/v5"
)

// Event is one row destined for the outbox table.
//
// It carries no database types so that the domain packages which construct
// events do not have to know how, or whether, they are eventually published.
type Event struct {
	// AggregateType and AggregateID name what the event is about. Debezium's
	// outbox routing turns AggregateType into the Kafka topic and AggregateID
	// into the message key, which is what keeps every event for one transaction
	// on the same partition and therefore in order.
	AggregateType string
	AggregateID   string

	// EventType is versioned in its own name (…​.v1) rather than in a header,
	// because a consumer that cannot parse a payload needs to discover that
	// from the routing key it subscribed to, not after deserialising.
	EventType string

	Payload json.RawMessage
}

// Aggregate type constants, kept here so producers and the Debezium connector
// configuration cannot drift apart silently.
const (
	AggregateTransaction = "transaction"
)

// Append writes one event inside the caller's transaction.
//
// It takes a pgx.Tx rather than a pool on purpose: invariant 6 says the
// database and Kafka are never written in the same logical step, and the way
// this codebase keeps that true is that the event row commits atomically with
// the state it describes. A signature that accepted a pool would make the
// dual-write trivially expressible, so it does not accept one.
func Append(ctx context.Context, tx pgx.Tx, e Event) error {
	if e.AggregateType == "" || e.AggregateID == "" || e.EventType == "" {
		return fmt.Errorf("append outbox event: aggregate type, aggregate id and event type are required")
	}
	if len(e.Payload) == 0 {
		return fmt.Errorf("append outbox event %s: payload is empty", e.EventType)
	}

	_, err := tx.Exec(ctx, `
		INSERT INTO outbox (aggregate_type, aggregate_id, event_type, payload)
		VALUES ($1, $2, $3, $4)`,
		e.AggregateType, e.AggregateID, e.EventType, []byte(e.Payload))
	if err != nil {
		return fmt.Errorf("append outbox event %s for %s: %w", e.EventType, e.AggregateID, err)
	}
	return nil
}
