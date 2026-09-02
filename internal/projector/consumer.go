package projector

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"

	"github.com/satyamsipah/ledger-core/internal/kafka"
	"github.com/satyamsipah/ledger-core/internal/observability"
	"github.com/satyamsipah/ledger-core/internal/outbox"
)

// tracer is named for the package it instruments, the ordinary OpenTelemetry
// Go convention -- there is no dedicated per-service tracer construction
// anywhere in this codebase (every other span comes from otelhttp's own
// automatic instrumentation at the HTTP boundary), so this is the first
// manually-started span and the name is chosen to match what a future one
// elsewhere would follow.
var tracer = otel.Tracer("github.com/satyamsipah/ledger-core/internal/projector")

// Consumer reads TransactionPosted/TransactionReversed from
// kafka.TopicTransaction and applies each to balance_projections.
//
// One client does both consuming and, for the dead-letter path, producing --
// franz-go's Client is not consumer-only, and a second client for the rare DLQ
// write would be a second connection pool to manage for no benefit.
type Consumer struct {
	client  *kgo.Client
	applier *Applier
	logger  *slog.Logger
	metrics *observability.Metrics
}

// NewConsumer builds a Consumer. group is the consumer group id; two
// processes sharing one group id divide the topic's partitions between them,
// which is how this consumer scales out.
func NewConsumer(brokers []string, group string, applier *Applier, logger *slog.Logger, metrics *observability.Metrics) (*Consumer, error) {
	client, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.ConsumerGroup(group),
		kgo.ConsumeTopics(kafka.TopicTransaction),
		// Manual offset commits, not autocommit: an offset must never be
		// committed ahead of the local database transaction that recorded
		// having handled it, or a crash between the two loses that record
		// while Kafka believes the message was consumed -- silently dropping
		// exactly the event a consumer at at-least-once delivery is supposed
		// never to drop.
		kgo.DisableAutoCommit(),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)
	if err != nil {
		return nil, fmt.Errorf("create kafka consumer client: %w", err)
	}

	return &Consumer{client: client, applier: applier, logger: logger, metrics: metrics}, nil
}

// Close releases the underlying Kafka client.
func (c *Consumer) Close() { c.client.Close() }

// Client exposes the underlying client for the lag reporter, which needs a
// kadm.Client built over the same connection rather than a second one.
func (c *Consumer) Client() *kgo.Client { return c.client }

// Run polls and applies until ctx is cancelled.
func (c *Consumer) Run(ctx context.Context) error {
	c.logger.Info("projector consumer started", slog.String("topic", kafka.TopicTransaction))

	for {
		if ctx.Err() != nil {
			c.logger.Info("projector consumer stopped")
			return nil
		}

		fetches := c.client.PollFetches(ctx)
		if errors.Is(fetches.Err0(), context.Canceled) {
			return nil
		}

		fetches.EachError(func(topic string, partition int32, err error) {
			c.logger.ErrorContext(ctx, "fetch error",
				slog.String("topic", topic), slog.Int("partition", int(partition)),
				slog.String("error", err.Error()))
		})

		c.processBatch(ctx, fetches.Records())
	}
}

// processBatch applies records in order and commits everything successfully
// handled, stopping at the first transient failure.
//
// Stopping rather than skipping matters: committing offset N+1 while N failed
// would mean a restart resumes past a message this consumer never actually
// applied, converting a transient database outage into a silent gap in the
// projection. The unprocessed remainder of this batch is simply re-fetched
// (records are never marked consumed until CommitRecords runs), so nothing is
// lost -- the batch just takes another pass.
func (c *Consumer) processBatch(ctx context.Context, records []*kgo.Record) {
	var toCommit []*kgo.Record

	for _, rec := range records {
		if err := c.handle(ctx, rec); err != nil {
			c.logger.ErrorContext(ctx, "stopping batch on transient error",
				slog.String("topic", rec.Topic), slog.Int64("offset", rec.Offset),
				slog.String("error", err.Error()))
			break
		}
		toCommit = append(toCommit, rec)
	}

	if len(toCommit) == 0 {
		return
	}
	if err := c.client.CommitRecords(ctx, toCommit...); err != nil {
		c.logger.ErrorContext(ctx, "commit offsets", slog.String("error", err.Error()))
	}
}

// handle decodes and applies one record.
//
// A nil return means this record is DONE -- successfully applied, a duplicate
// already recorded, or dead-lettered -- and its offset is safe to commit. A
// non-nil return means a transient condition (the database is unreachable,
// most plausibly) that the next poll should retry rather than skip past.
//
// The span this starts is what makes "one trace spans the whole async flow"
// (the phase's own requirement) actually true rather than a coincidence of
// two processes logging the same trace id next to each other. Extract reads
// the "traceparent" header both outbox publishers set from
// outbox.trace_parent (see internal/outbox/publish/polling and
// deploy/debezium/outbox-connector.json's table.fields.additional.placement);
// when present, tracer.Start below creates a genuine CHILD of the span that
// produced this event, linked by the OpenTelemetry SDK's own machinery
// rather than by two independently-chosen values that merely happen to
// match. When absent -- a row written outside a traced context -- Extract is
// a no-op and this starts a new, unparented trace instead, which is the
// correct, honest fallback rather than an error.
func (c *Consumer) handle(ctx context.Context, rec *kgo.Record) error {
	carrier := propagation.MapCarrier{}
	for _, h := range rec.Headers {
		if h.Key == "traceparent" {
			carrier.Set("traceparent", string(h.Value))
		}
	}
	ctx = otel.GetTextMapPropagator().Extract(ctx, carrier)

	ctx, span := tracer.Start(ctx, "projector.apply_event", trace.WithSpanKind(trace.SpanKindConsumer))
	defer span.End()

	var envelope outbox.Envelope
	if err := json.Unmarshal(rec.Value, &envelope); err != nil {
		wrapped := fmt.Errorf("decode envelope: %w", err)
		span.RecordError(wrapped)
		span.SetStatus(codes.Error, wrapped.Error())
		return c.deadLetter(ctx, rec, wrapped)
	}

	span.SetAttributes(
		attribute.String("ledger.event_id", envelope.EventID.String()),
		attribute.String("ledger.event_type", envelope.EventType),
		attribute.String("ledger.aggregate_id", envelope.AggregateID),
	)

	applied, err := c.applier.Apply(ctx, envelope)
	switch {
	case err == nil:
		if applied {
			c.metrics.ProjectorEventsProcessed.WithLabelValues(envelope.EventType).Inc()
		} else {
			c.metrics.ProjectorDuplicatesSkipped.Inc()
			span.SetAttributes(attribute.Bool("ledger.duplicate", true))
		}
		return nil
	case errors.Is(err, ErrUnknownEventType):
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return c.deadLetter(ctx, rec, err)
	default:
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return fmt.Errorf("apply event %s: %w", envelope.EventID, err)
	}
}

// deadLetter routes a message this consumer cannot apply -- and, having tried,
// knows it will never be able to on its own -- to kafka.TopicDLQ, tagged with
// enough of its origin to replay it once whatever was wrong is fixed.
//
// A nil return here means the DLQ write itself succeeded, so the ORIGINAL
// message's offset is safe to commit: the message has not been lost, it has
// been moved. If the DLQ write fails, that failure is Kafka being unreachable
// -- a transient condition like any other -- so this returns an error rather
// than silently dropping the message, and the original offset stays
// uncommitted for the next poll to retry.
func (c *Consumer) deadLetter(ctx context.Context, rec *kgo.Record, cause error) error {
	c.logger.WarnContext(ctx, "routing message to dead-letter topic",
		slog.String("source_topic", rec.Topic),
		slog.Int("source_partition", int(rec.Partition)),
		slog.Int64("source_offset", rec.Offset),
		slog.String("error", cause.Error()))

	dlqRecord := &kgo.Record{
		Topic: kafka.TopicDLQ,
		Key:   rec.Key,
		Value: rec.Value,
		Headers: []kgo.RecordHeader{
			{Key: "dlq.source_topic", Value: []byte(rec.Topic)},
			{Key: "dlq.source_partition", Value: []byte(strconv.Itoa(int(rec.Partition)))},
			{Key: "dlq.source_offset", Value: []byte(strconv.FormatInt(rec.Offset, 10))},
			{Key: "dlq.error", Value: []byte(cause.Error())},
			{Key: "dlq.timestamp", Value: []byte(time.Now().UTC().Format(time.RFC3339Nano))},
		},
	}

	if err := c.client.ProduceSync(ctx, dlqRecord).FirstErr(); err != nil {
		return fmt.Errorf("produce to dead-letter topic: %w", err)
	}
	c.metrics.ProjectorDLQTotal.Inc()
	return nil
}
