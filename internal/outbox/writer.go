package outbox

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"go.opentelemetry.io/otel/trace"
)

// Event is one domain fact a write path wants published, before it becomes an
// outbox row.
//
// It carries no database types so that the domain packages which construct
// events do not have to know how, or whether, they are eventually published.
type Event struct {
	// AggregateType and AggregateID name what the event is about, and are the
	// two fields Debezium's EventRouter SMT reads to route it: AggregateType
	// selects the topic (ledger.events.<AggregateType>), AggregateID becomes
	// the Kafka message key. The polling publisher makes the identical
	// decision from the identical columns -- see internal/outbox/publish --
	// which is what makes the two publishers' output indistinguishable to a
	// consumer.
	AggregateType string
	AggregateID   string

	// EventType names what happened -- TransactionPosted, BalanceUpdated, and
	// so on. Stable across schema revisions; EventVersion is what changes.
	EventType string

	// EventVersion is the schema version of Payload. An integer a consumer can
	// compare, rather than a ".v1" suffix baked into EventType: Phase 3 used
	// the suffix, and this phase retires it in favour of a field that does not
	// require parsing a string to branch on.
	EventVersion int16

	// OccurredAt is when the business event happened, sourced from a
	// database-generated timestamp the caller already has -- a transaction's
	// posted_at, an account's created_at -- never from this process's own
	// clock. It is deliberately not the same thing as the outbox row's own
	// created_at column: that one records when the row was appended for
	// publishing, which is administratively useful (publish latency =
	// published_at - created_at) but is not the answer to "when did this
	// happen," and D16/D17 already made the case in Phase 2 for why the
	// distinction between a business timestamp and a bookkeeping one is worth
	// keeping precise rather than collapsing the two into one column.
	OccurredAt time.Time

	// Payload is the event-specific JSON. Append wraps it in the envelope; it
	// is not the whole wire payload itself.
	Payload json.RawMessage
}

// Aggregate type constants. AggregateType selects the Kafka topic
// (ledger.events.<AggregateType>) via the Debezium EventRouter's
// route.by.field, and the polling publisher derives the same topic from the
// same column -- kept here so producers, the connector configuration, and the
// polling publisher's routing table cannot drift apart silently.
const (
	// AggregateTransaction: TransactionPosted, TransactionReversed. Keyed by
	// transaction id, because a transaction inherently touches two or more
	// accounts (double-entry) and cannot be keyed by a single account_id
	// without either an arbitrary choice or fanning one domain fact out into
	// several rows. See docs/DECISIONS.md D32 for the full account_id-keying
	// argument this decision sits inside.
	AggregateTransaction = "transaction"

	// AggregateAccount: AccountCreated, BalanceUpdated. Keyed by account id --
	// this is where "partition key = account_id" actually applies, and every
	// event that ever mentions a given account, across every transaction that
	// touches it, is delivered to one consumer in commit order.
	AggregateAccount = "account"

	// AggregateSaga: SagaStepCompleted. Keyed by saga instance id.
	AggregateSaga = "saga"
)

// Envelope is the complete wire shape: everything a consumer needs, in one
// self-contained JSON document, regardless of which publisher produced it.
//
// This struct is the literal implementation of D31's consequence: because both
// the polling publisher and the Debezium connector do nothing more than put
// the payload column on Kafka verbatim, whatever shape this struct marshals to
// IS the message a consumer receives. There is exactly one place that decides
// the wire format, and it is exported specifically so a consumer decodes with
// this same type rather than a hand-maintained copy of its field tags -- two
// independently-written structs for one wire format is exactly how a producer
// and a consumer drift silently apart.
type Envelope struct {
	EventID      uuid.UUID       `json:"event_id"`
	EventType    string          `json:"event_type"`
	EventVersion int16           `json:"event_version"`
	AggregateID  string          `json:"aggregate_id"`
	OccurredAt   time.Time       `json:"occurred_at"`
	TraceID      string          `json:"trace_id,omitempty"`
	Payload      json.RawMessage `json:"payload"`
}

// Append writes one event inside the caller's transaction.
//
// It takes a pgx.Tx rather than a pool on purpose: invariant 6 says the
// database and Kafka are never written in the same logical step, and the way
// this codebase keeps that true is that the event row commits atomically with
// the state it describes. A signature that accepted a pool would make the
// dual-write trivially expressible, so it does not accept one.
//
// event_id is generated here, as a UUIDv7 -- consistent with D3's convention
// for every other primary identifier in this schema -- rather than left to the
// caller, because doing it in one place is what guarantees an outbox row can
// never exist without the identifier its own dedupe and its own Debezium
// producer-idempotency tracking both depend on.
func Append(ctx context.Context, tx pgx.Tx, e Event) error {
	if e.AggregateType == "" || e.AggregateID == "" || e.EventType == "" {
		return fmt.Errorf("append outbox event: aggregate type, aggregate id and event type are required")
	}
	if e.EventVersion < 1 {
		return fmt.Errorf("append outbox event %s: event version must be at least 1, got %d", e.EventType, e.EventVersion)
	}
	if e.OccurredAt.IsZero() {
		return fmt.Errorf("append outbox event %s: occurred_at is required: %w", e.EventType, ErrMissingOccurredAt)
	}
	if len(e.Payload) == 0 {
		return fmt.Errorf("append outbox event %s: payload is empty", e.EventType)
	}

	eventID, err := uuid.NewV7()
	if err != nil {
		return fmt.Errorf("generate event id for %s: %w", e.EventType, err)
	}

	// Best-effort: a caller running outside a traced context (a backfill
	// script, a migration-adjacent tool) has no span, and that is a legitimate
	// state rather than an error. traceID stays empty and the column stays
	// NULL, which is the honest answer to "what trace produced this," not a
	// default value standing in for one.
	var traceID string
	if sc := trace.SpanContextFromContext(ctx); sc.IsValid() {
		traceID = sc.TraceID().String()
	}

	payload, err := json.Marshal(Envelope{
		EventID:      eventID,
		EventType:    e.EventType,
		EventVersion: e.EventVersion,
		AggregateID:  e.AggregateID,
		OccurredAt:   e.OccurredAt,
		TraceID:      traceID,
		Payload:      e.Payload,
	})
	if err != nil {
		return fmt.Errorf("marshal envelope for %s event %s: %w", e.EventType, eventID, err)
	}

	_, err = tx.Exec(ctx, `
		INSERT INTO outbox (aggregate_type, aggregate_id, event_type, event_id,
		                    event_version, trace_id, payload)
		VALUES ($1, $2, $3, $4, $5, NULLIF($6, ''), $7)`,
		e.AggregateType, e.AggregateID, e.EventType, eventID, e.EventVersion, traceID, []byte(payload))
	if err != nil {
		return fmt.Errorf("append outbox event %s for %s: %w", e.EventType, e.AggregateID, err)
	}
	return nil
}
