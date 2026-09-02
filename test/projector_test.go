package test

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kgo"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"

	"github.com/satyamsipah/ledger-core/internal/kafka"
	"github.com/satyamsipah/ledger-core/internal/ledger"
	"github.com/satyamsipah/ledger-core/internal/observability"
	"github.com/satyamsipah/ledger-core/internal/outbox"
	"github.com/satyamsipah/ledger-core/internal/outbox/publish/polling"
	"github.com/satyamsipah/ledger-core/internal/projector"
)

// runConsumerFor starts a projector.Consumer against brokers, runs it until
// stopped, and returns the stop function. Each call gets its own consumer
// group so tests never contend over Kafka Connect-style partition assignment
// with each other or with the group cmd/projector would use in the real
// stack.
func runConsumerFor(t *testing.T, brokers []string) (consumer *projector.Consumer, stop func()) {
	t.Helper()

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	metrics := observability.NewMetrics("test")
	applier := projector.NewApplier(sharedPool)

	consumer, err := projector.NewConsumer(brokers, "projector-test-"+uuid.NewString(), applier, logger, metrics)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = consumer.Run(ctx)
	}()

	return consumer, func() {
		cancel()
		<-done
		consumer.Close()
	}
}

// drainToKafka runs the polling publisher until the outbox backlog it created
// is empty, then stops it. A test-scoped helper rather than reusing the
// long-running pattern from the outage test, because these tests want the
// publisher to do exactly one job -- get what exists onto Kafka -- and then
// get out of the way.
func drainToKafka(t *testing.T, ctx context.Context, brokers []string) {
	t.Helper()

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	metrics := observability.NewMetrics("test")
	client := newTestKafkaClient(t, brokers)

	publisher := polling.New(sharedPool, client, logger, metrics, polling.Config{BatchSize: 100})

	require.Eventually(t, func() bool {
		n, err := publisher.PublishOnce(ctx)
		require.NoError(t, err)
		return n == 0
	}, 15*time.Second, 50*time.Millisecond, "polling publisher must drain the outbox backlog")
}

// TestProjector_ConsumesAndAppliesTransactionPosted is the foundational
// correctness check: the projection the consumer builds from Kafka must equal
// what the write path itself believes, for accounts nothing but this test
// touched.
func TestProjector_ConsumesAndAppliesTransactionPosted(t *testing.T) {
	ctx := context.Background()

	_, brokers := startRedpanda(ctx, t)
	provisionTestTopics(ctx, t, brokers)

	service := newLedgerService(sharedPool)
	from := newAccount(t, ctx, sharedPool, "INR", true)
	to := newTypedAccount(t, ctx, sharedPool, ledger.AccountTypeLiability, "INR", false)

	const posts = 6
	for range posts {
		_, err := service.PostTransaction(ctx, transferRequest(t, from, to, 300, "INR"))
		require.NoError(t, err)
	}

	drainToKafka(t, ctx, brokers)

	_, stop := runConsumerFor(t, brokers)
	defer stop()

	authoritative, err := service.GetBalance(ctx, to)
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		var available int64
		var version int64
		err := sharedPool.QueryRow(ctx,
			`SELECT available_minor, version FROM balance_projections WHERE account_id = $1`, to).
			Scan(&available, &version)
		return err == nil && available == authoritative.Available.AmountMinor() && version == authoritative.Version
	}, 15*time.Second, 100*time.Millisecond,
		"the projection must converge to the same balance and version the write path itself holds")
}

// TestProjector_TraceContextPropagatesThroughKafkaHeaders proves the thing
// the phase actually asked for -- "trace context propagated through Kafka
// message headers so one trace spans the whole async flow" -- rather than
// merely that outbox.trace_parent gets populated. Populating a column and
// genuinely linking a span are different claims, and only the second is what
// makes a trace in a real backend show the HTTP request and the projector's
// handling of its event as one connected waterfall instead of two spans that
// happen to carry a matching string.
//
// It installs a REAL tracer provider with an in-memory recorder as the
// process-global one, globally, for its own duration -- safe despite this
// package running many tests in parallel elsewhere, because this test does
// not call t.Parallel(). Go's test runner does not begin executing ANY
// t.Parallel() test's body in this package until every non-parallel test --
// this one included, cleanup and all -- has already returned, so no parallel
// test observes the swapped provider mid-test. See
// TestProjector_ConsumesAndAppliesTransactionPosted and its neighbours for
// the same non-parallel shape, chosen there for the unrelated reason that
// each test spins its own Redpanda container.
//
// The assertion that matters is SpanContext().TraceID() equality between the
// producing span and a span the projector started, found by searching
// recorded spans rather than assumed to be the only one recorded: draining
// the outbox (see drainToKafka) also publishes any backlog OTHER tests left
// behind on the shared pool, and this consumer will apply those too, each
// starting its own "projector.apply_event" span with no relation to this
// test's trace. Searching for the matching trace id, rather than asserting
// there is exactly one span, is what makes this robust to that shared state
// rather than flaky because of it.
func TestProjector_TraceContextPropagatesThroughKafkaHeaders(t *testing.T) {
	ctx := context.Background()

	_, brokers := startRedpanda(ctx, t)
	provisionTestTopics(ctx, t, brokers)

	recorder := tracetest.NewSpanRecorder()
	provider := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder))

	originalProvider := otel.GetTracerProvider()
	originalPropagator := otel.GetTextMapPropagator()
	otel.SetTracerProvider(provider)
	otel.SetTextMapPropagator(propagation.TraceContext{})
	t.Cleanup(func() {
		otel.SetTracerProvider(originalProvider)
		otel.SetTextMapPropagator(originalPropagator)
	})

	service := newLedgerService(sharedPool)
	from := newAccount(t, ctx, sharedPool, "INR", true)
	to := newTypedAccount(t, ctx, sharedPool, ledger.AccountTypeLiability, "INR", false)

	tracedCtx, span := provider.Tracer("test").Start(ctx, "test-request")
	producingTraceID := span.SpanContext().TraceID()
	require.True(t, producingTraceID.IsValid())

	_, err := service.PostTransaction(tracedCtx, transferRequest(t, from, to, 300, "INR"))
	require.NoError(t, err)
	span.End()

	drainToKafka(t, ctx, brokers)

	_, stop := runConsumerFor(t, brokers)
	defer stop()

	var consumerSpan sdktrace.ReadOnlySpan
	require.Eventually(t, func() bool {
		for _, s := range recorder.Ended() {
			if s.Name() == "projector.apply_event" && s.SpanContext().TraceID() == producingTraceID {
				consumerSpan = s
				return true
			}
		}
		return false
	}, 15*time.Second, 100*time.Millisecond,
		"the projector must start a span in the SAME trace as the request that produced the event")

	assert.True(t, consumerSpan.Parent().IsValid(),
		"the consumer's span must carry a real parent, proving Extract linked it rather than starting a fresh root that coincidentally shares a trace id")
	assert.Equal(t, producingTraceID, consumerSpan.Parent().TraceID())
}

// TestProjector_DuplicateDeliveryIsSkipped covers dedupe on the consumer's
// normal path (as opposed to TestOutboxPublish_PollingCrashBetweenPublishAndMark,
// which covers it on the specific crash path that produces a real duplicate on
// the wire): re-delivering an already-committed offset's message must not
// double-apply it.
func TestProjector_DuplicateDeliveryIsSkipped(t *testing.T) {
	ctx := context.Background()

	_, brokers := startRedpanda(ctx, t)
	provisionTestTopics(ctx, t, brokers)

	service := newLedgerService(sharedPool)
	from := newAccount(t, ctx, sharedPool, "INR", true)
	to := newTypedAccount(t, ctx, sharedPool, ledger.AccountTypeLiability, "INR", false)

	posted, err := service.PostTransaction(ctx, transferRequest(t, from, to, 900, "INR"))
	require.NoError(t, err)

	drainToKafka(t, ctx, brokers)

	applier := projector.NewApplier(sharedPool)

	env := requireTransactionEnvelope(t, ctx, brokers, posted.ID.String())

	firstApplied, err := applier.Apply(ctx, env)
	require.NoError(t, err)
	assert.True(t, firstApplied)

	secondApplied, err := applier.Apply(ctx, env)
	require.NoError(t, err)
	assert.False(t, secondApplied, "re-applying the identical event must be a no-op")

	var processedCount int
	require.NoError(t, sharedPool.QueryRow(ctx,
		`SELECT count(*) FROM processed_events WHERE event_id = $1`, env.EventID).Scan(&processedCount))
	assert.Equal(t, 1, processedCount, "processed_events must record the event exactly once regardless of delivery count")
}

// TestProjector_RebuildMatchesLive is the required end-to-end check: post
// through the real write path, publish through the real polling publisher,
// consume and apply through the real Consumer, then verify the result against
// journal_entries directly -- a check of the whole pipeline, not of any one
// component's self-consistency.
func TestProjector_RebuildMatchesLive(t *testing.T) {
	ctx := context.Background()

	_, brokers := startRedpanda(ctx, t)
	provisionTestTopics(ctx, t, brokers)

	service := newLedgerService(sharedPool)

	// A mix deliberately wider than one transfer: several accounts, several
	// transactions each, and a reversal -- so the rebuild is checked against
	// more than the single easy case.
	accountA := newAccount(t, ctx, sharedPool, "INR", true)
	accountB := newTypedAccount(t, ctx, sharedPool, ledger.AccountTypeLiability, "INR", false)
	accountC := newTypedAccount(t, ctx, sharedPool, ledger.AccountTypeLiability, "INR", false)

	for range 4 {
		_, err := service.PostTransaction(ctx, transferRequest(t, accountA, accountB, 400, "INR"))
		require.NoError(t, err)
	}
	for range 3 {
		_, err := service.PostTransaction(ctx, transferRequest(t, accountA, accountC, 250, "INR"))
		require.NoError(t, err)
	}
	reversed, err := service.PostTransaction(ctx, transferRequest(t, accountB, accountC, 100, "INR"))
	require.NoError(t, err)
	_, err = service.ReverseTransaction(ctx, reversed.ID, "test: rebuild coverage needs a reversal too")
	require.NoError(t, err)

	drainToKafka(t, ctx, brokers)

	_, stop := runConsumerFor(t, brokers)

	scope := []uuid.UUID{accountA, accountB, accountC}

	// Wait for the projection to converge before stopping the consumer and
	// checking it -- otherwise this test would be asserting about a race
	// against its own setup rather than about the pipeline's correctness.
	require.Eventually(t, func() bool {
		result, err := projector.Rebuild(ctx, sharedPool, scope...)
		return err == nil && result.OK()
	}, 20*time.Second, 200*time.Millisecond, "the projection must converge to match the journal")

	stop()

	result, err := projector.Rebuild(ctx, sharedPool, scope...)
	require.NoError(t, err)

	if !result.OK() {
		for _, m := range result.Mismatches {
			t.Logf("mismatch: account=%s currency=%s rebuilt=%d live=%d version=%d",
				m.AccountID, m.Currency, m.RebuiltAvailable, m.LiveAvailable, m.LiveVersion)
		}
	}
	assert.True(t, result.OK(), "the rebuilt projection must match the live one exactly, account for account")
	assert.Equal(t, len(scope), result.AccountsCompared)

	assertGlobalInvariant(t, ctx, sharedPool)
}

// TestProjector_UnknownEventTypeIsDeadLettered proves the DLQ path actually
// moves a message it cannot apply, tags it with enough to investigate, and
// still lets its offset commit -- so one message this consumer does not
// understand cannot wedge every message behind it.
func TestProjector_UnknownEventTypeIsDeadLettered(t *testing.T) {
	ctx := context.Background()

	_, brokers := startRedpanda(ctx, t)
	provisionTestTopics(ctx, t, brokers)

	group := "projector-dlq-test-" + uuid.NewString()
	producer := newTestKafkaClient(t, brokers)

	poison := outbox.Envelope{
		EventID:      uuid.Must(uuid.NewV7()),
		EventType:    "SomeFutureEventTypeThisBuildDoesNotKnow",
		EventVersion: 1,
		AggregateID:  uuid.NewString(),
		OccurredAt:   time.Now().UTC(),
		Payload:      []byte(`{"anything":"at all"}`),
	}
	body, err := json.Marshal(poison)
	require.NoError(t, err)

	require.NoError(t, producer.ProduceSync(ctx, &kgo.Record{
		Topic: kafka.TopicTransaction,
		Key:   []byte(poison.AggregateID),
		Value: body,
	}).FirstErr())

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	metrics := observability.NewMetrics("test")
	applier := projector.NewApplier(sharedPool)

	consumer, err := projector.NewConsumer(brokers, group, applier, logger, metrics)
	require.NoError(t, err)
	defer consumer.Close()

	dlqReader := newTestKafkaClient(t, brokers,
		kgo.ConsumerGroup("dlq-reader-"+uuid.NewString()),
		kgo.ConsumeTopics(kafka.TopicDLQ),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()))

	runCtx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = consumer.Run(runCtx)
	}()
	defer func() {
		cancel()
		<-done
	}()

	records := consumeN(ctx, t, dlqReader, 1, 15*time.Second)
	require.Len(t, records, 1)

	headers := map[string]string{}
	for _, h := range records[0].Headers {
		headers[h.Key] = string(h.Value)
	}
	assert.Equal(t, kafka.TopicTransaction, headers["dlq.source_topic"])
	assert.Contains(t, headers["dlq.error"], "unknown event type")
	assert.Equal(t, poison.AggregateID, string(records[0].Key), "the DLQ record keeps the original key for traceability")
}

// requireTransactionEnvelope reads back the TransactionPosted envelope for a
// specific transaction id from kafka.TopicTransaction.
func requireTransactionEnvelope(t *testing.T, ctx context.Context, brokers []string, transactionID string) outbox.Envelope {
	t.Helper()

	client := newTestKafkaClient(t, brokers,
		kgo.ConsumerGroup("envelope-reader-"+uuid.NewString()),
		kgo.ConsumeTopics(kafka.TopicTransaction),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()))

	return requireEnvelope(t, ctx, client, transactionID)
}
