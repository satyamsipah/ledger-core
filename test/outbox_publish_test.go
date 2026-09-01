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
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/satyamsipah/ledger-core/internal/kafka"
	"github.com/satyamsipah/ledger-core/internal/ledger"
	"github.com/satyamsipah/ledger-core/internal/observability"
	"github.com/satyamsipah/ledger-core/internal/outbox"
	"github.com/satyamsipah/ledger-core/internal/outbox/publish/polling"
	"github.com/satyamsipah/ledger-core/internal/projector"
)

// outboxBacklog returns how many outbox rows are still unpublished.
func outboxBacklog(t *testing.T, ctx context.Context) int {
	t.Helper()
	var n int
	require.NoError(t, sharedPool.QueryRow(ctx,
		`SELECT count(*) FROM outbox WHERE published_at IS NULL`).Scan(&n))
	return n
}

func outboxPublishedCount(t *testing.T, ctx context.Context) int {
	t.Helper()
	var n int
	require.NoError(t, sharedPool.QueryRow(ctx,
		`SELECT count(*) FROM outbox WHERE published_at IS NOT NULL`).Scan(&n))
	return n
}

// postSomeTransactions posts n transfers between fresh accounts, each of which
// appends a TransactionPosted event plus one BalanceUpdated event per account
// touched -- real envelopes, produced by the real write path, not a hand-built
// fixture standing in for one.
func postSomeTransactions(t *testing.T, ctx context.Context, service *ledger.Service, n int) {
	t.Helper()
	for range n {
		from := newAccount(t, ctx, sharedPool, "INR", true)
		to := newTypedAccount(t, ctx, sharedPool, ledger.AccountTypeLiability, "INR", false)
		_, err := service.PostTransaction(ctx, transferRequest(t, from, to, 500, "INR"))
		require.NoError(t, err)
	}
}

// TestOutboxPublish_KafkaOutage kills the broker mid-run and proves the
// backlog absorbs the outage rather than losing anything.
//
// This is D30 made concrete: the outbox converts "Kafka is down" from a
// problem that could lose an event into a problem that is merely a growing,
// entirely recoverable number in one table. Nothing here should require any
// intervention beyond Kafka coming back.
func TestOutboxPublish_KafkaOutage(t *testing.T) {
	ctx := context.Background()

	container, brokers := startRedpanda(ctx, t)
	provisionTestTopics(ctx, t, brokers)

	service := newLedgerService(sharedPool)
	// RecordDeliveryTimeout is client-level, not per-call: franz-go's
	// ProduceSync buffers each record via cl.Produce(ctx, ...), but once
	// buffered, delivery is governed entirely by the CLIENT's own retry
	// bookkeeping -- the context passed to ProduceSync itself is not
	// consulted again. Without this option a client whose broker never comes
	// back retries a buffered record forever, and ProduceSync's wg.Wait()
	// blocks forever with it, regardless of any context deadline wrapped
	// around the call. This is exactly what cmd/outbox-publisher's production
	// wiring already sets (at 30s, sized for production); the test uses a
	// short bound so a genuine outage surfaces in seconds, not minutes.
	//
	// DisableIdempotentWrite matters here for a subtler reason, discovered by
	// this test hanging for its full timeout before this line was added:
	// franz-go's idempotent producer (the default) only enforces
	// RecordDeliveryTimeout when it is SAFE to -- a record never sent, or one
	// sent that received a response. A record already in flight when
	// container.Stop() severs the connection is neither: the client cannot
	// tell whether the broker received it, so timing it out could create a
	// gap in the idempotent sequence, and franz-go refuses to guess. It waits
	// forever instead. That is the correct, conservative choice for a
	// producer whose job is exactly-once-per-partition delivery -- but this
	// publisher's job is at-least-once (D30), so idempotent production was
	// never buying anything here, and disabling it is what lets the delivery
	// timeout actually fire on the connection this test severs on purpose.
	client := newTestKafkaClient(t, brokers,
		kgo.RecordDeliveryTimeout(2*time.Second),
		kgo.DisableIdempotentWrite())
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	metrics := observability.NewMetrics("test")

	publisher := polling.New(sharedPool, client, logger, metrics, polling.Config{
		Interval:     100 * time.Millisecond,
		BatchSize:    50,
		BatchTimeout: 5 * time.Second,
	})

	admin := kadm.NewClient(newTestKafkaClient(t, brokers))

	before := outboxPublishedCount(t, ctx)

	// Traffic before the outage, so there is a real backlog to drain once
	// Kafka is back, not merely rows created after the fact.
	const prePosts = 5
	postSomeTransactions(t, ctx, service, prePosts)

	runCtx, cancelRun := context.WithCancel(ctx)
	runDone := make(chan struct{})
	go func() {
		defer close(runDone)
		_ = publisher.Run(runCtx)
	}()
	t.Cleanup(func() {
		cancelRun()
		<-runDone
	})

	// Let the publisher catch up on the pre-outage traffic before pulling the
	// broker out from under it, so the assertions below are about what
	// happens DURING the outage, not a race with startup.
	require.Eventually(t, func() bool {
		return outboxBacklog(t, ctx) == 0
	}, 10*time.Second, 100*time.Millisecond, "publisher must drain the backlog before the outage begins")

	// docker pause, not container.Stop: the redpanda testcontainers module uses
	// a custom entrypoint that waits for its node config to be mounted by a
	// lifecycle hook run only during the original Run() -- container.Start()
	// on an already-created container does not re-run that hook, so a
	// stop/start cycle leaves the entrypoint waiting forever and the broker
	// never actually comes back. Pausing freezes the already-running process
	// via the kernel cgroup freezer instead: every connection to it goes
	// unresponsive exactly as it would during a real outage, and unpausing
	// resumes the SAME process with no startup sequence to re-run at all.
	pauseContainer(t, ctx, container.GetContainerID())

	const duringOutagePosts = 8
	postSomeTransactions(t, ctx, service, duringOutagePosts)

	// The defining assertion of this test: traffic during an outage
	// accumulates in outbox rather than being lost. Given a moment for the
	// publisher's own retries to fail against the dead broker.
	require.Eventually(t, func() bool {
		return outboxBacklog(t, ctx) >= duringOutagePosts
	}, 15*time.Second, 200*time.Millisecond,
		"transactions posted during the outage must accumulate in the outbox, not vanish")

	// And it does not spuriously drain on its own -- if it did, that would
	// mean either data was fabricated or the "outage" was not actually one.
	backlogDuringOutage := outboxBacklog(t, ctx)
	time.Sleep(2 * time.Second)
	assert.Equal(t, backlogDuringOutage, outboxBacklog(t, ctx),
		"the backlog must not move while the broker is down")

	unpauseContainer(t, ctx, container.GetContainerID())

	// Separated from the drain wait below on purpose: redpanda's own restart
	// (re-initialising the Kafka API, reloading topic metadata) takes real,
	// somewhat variable time that has nothing to do with whether this
	// publisher is working. Waiting for the broker to answer ListTopics
	// first means the drain assertion's own timeout budget measures what it
	// claims to -- how long the publisher takes to catch up -- rather than
	// being spent partly on the broker's own recovery.
	require.Eventually(t, func() bool {
		_, err := admin.ListTopics(ctx)
		return err == nil
	}, 60*time.Second, 500*time.Millisecond, "the broker must become reachable again after restarting")

	require.Eventually(t, func() bool {
		return outboxBacklog(t, ctx) == 0
	}, 30*time.Second, 200*time.Millisecond,
		"the backlog must fully drain once the broker returns")

	after := outboxPublishedCount(t, ctx)
	// 3 outbox rows per transfer: one TransactionPosted plus one
	// BalanceUpdated for each of the two accounts a transfer touches (see
	// appendBalanceUpdatedEvents in internal/ledger/service.go).
	const rowsPerTransfer = 3
	assert.Equal(t, (prePosts+duringOutagePosts)*rowsPerTransfer+before, after,
		"every transaction posted, before and during the outage, must eventually publish exactly once -- zero loss")

	assertGlobalInvariant(t, ctx, sharedPool)
}

// TestOutboxPublish_PollingCrashBetweenPublishAndMark drives the exact crash
// window docs/DECISIONS.md D30 and D31 are built around: Kafka has already
// accepted the message, and the connection that would record that fact dies
// before it can. The retry re-publishes -- a genuine duplicate on the wire --
// and this test proves the projector's dedupe is what makes that safe rather
// than merely claimed to be.
func TestOutboxPublish_PollingCrashBetweenPublishAndMark(t *testing.T) {
	ctx := context.Background()

	_, brokers := startRedpanda(ctx, t)
	provisionTestTopics(ctx, t, brokers)

	service := newLedgerService(sharedPool)
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	metrics := observability.NewMetrics("test")

	appName := "outbox-publisher-crash-" + uuid.NewString()[:8]
	victimPool := newNamedPool(ctx, t, appName)

	client := newTestKafkaClient(t, brokers)
	publisher := polling.New(victimPool, client, logger, metrics, polling.Config{BatchSize: 50})

	from := newAccount(t, ctx, sharedPool, "INR", true)
	to := newTypedAccount(t, ctx, sharedPool, ledger.AccountTypeLiability, "INR", false)
	posted, err := service.PostTransaction(ctx, transferRequest(t, from, to, 750, "INR"))
	require.NoError(t, err)

	// The row this test tracks through the crash: TransactionPosted for the
	// transaction just posted. aggregate_id is the transaction id (D32's
	// keying decision), which is what makes it identifiable independent of
	// which attempt eventually marks it published.
	var outboxID int64
	require.NoError(t, sharedPool.QueryRow(ctx, `
		SELECT id FROM outbox
		 WHERE aggregate_type = $1 AND aggregate_id = $2 AND event_type = $3`,
		"transaction", posted.ID.String(), "TransactionPosted").Scan(&outboxID))

	entered := make(chan struct{})
	killed := make(chan struct{})
	publisher.WithCrashHook(func() {
		close(entered)
		<-killed
	})

	var (
		firstErr error
		firstN   int
	)
	done := make(chan struct{})
	go func() {
		defer close(done)
		firstN, firstErr = publisher.PublishOnce(ctx)
	}()

	select {
	case <-entered:
	case <-time.After(20 * time.Second):
		t.Fatal("PublishOnce never reached the crash hook")
	}

	require.True(t, terminateBackend(ctx, t, sharedPool, appName),
		"the publisher's backend must be found and terminated for this test to mean anything")
	close(killed)

	select {
	case <-done:
	case <-time.After(20 * time.Second):
		t.Fatal("PublishOnce never returned after its connection was terminated")
	}
	require.Error(t, firstErr, "a terminated connection must surface as an error")
	assert.Zero(t, firstN)

	// The transaction rolled back: the row is exactly as it was.
	var publishedAt *time.Time
	require.NoError(t, sharedPool.QueryRow(ctx,
		`SELECT published_at FROM outbox WHERE id = $1`, outboxID).Scan(&publishedAt))
	assert.Nil(t, publishedAt, "the row must still look unpublished after the rollback")

	// But Kafka already has it -- ProduceSync succeeded before the hook fired.
	// This is the duplicate materialising for real, not asserted by inference.
	consumer := newTestKafkaClient(t, brokers,
		kgo.ConsumerGroup("crash-test-reader-"+uuid.NewString()),
		kgo.ConsumeTopics(kafka.TopicTransaction),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()))
	first := requireEnvelope(t, ctx, consumer, posted.ID.String())

	// The retry: same row, still unpublished, gets claimed and republished for
	// real this time.
	sharedPublisher := polling.New(sharedPool, client, logger, metrics, polling.Config{BatchSize: 50})
	n, err := sharedPublisher.PublishOnce(ctx)
	require.NoError(t, err)
	assert.GreaterOrEqual(t, n, 1)

	require.NoError(t, sharedPool.QueryRow(ctx,
		`SELECT published_at FROM outbox WHERE id = $1`, outboxID).Scan(&publishedAt))
	require.NotNil(t, publishedAt, "the retry must succeed and mark the row published")

	second := requireEnvelope(t, ctx, consumer, posted.ID.String())

	// Two distinct Kafka messages, but ONE event_id: exactly what "at-least-
	// once, not exactly-once" (D30) predicts, and exactly what a consumer must
	// be able to absorb.
	assert.Equal(t, first.EventID, second.EventID,
		"both deliveries describe the same event; the outage did not fabricate a new one")

	// And this is the part that makes it safe: fed through the real Applier,
	// the second delivery is recognised and skipped.
	applier := projector.NewApplier(sharedPool)
	appliedFirst, err := applier.Apply(ctx, first)
	require.NoError(t, err)
	assert.True(t, appliedFirst, "the first delivery of a new event must apply")

	appliedSecond, err := applier.Apply(ctx, second)
	require.NoError(t, err)
	assert.False(t, appliedSecond, "the duplicate delivery must be recognised and skipped, not re-applied")

	assertGlobalInvariant(t, ctx, sharedPool)
}

// requireEnvelope polls the consumer until it sees a TransactionPosted
// envelope for the given transaction id, decodes it, and returns it.
func requireEnvelope(t *testing.T, ctx context.Context, client *kgo.Client, transactionID string) outbox.Envelope {
	t.Helper()

	ctx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()

	for {
		fetches := client.PollFetches(ctx)
		require.NoError(t, ctx.Err(), "timed out waiting for TransactionPosted envelope for %s", transactionID)
		for _, rec := range fetches.Records() {
			var env outbox.Envelope
			require.NoError(t, json.Unmarshal(rec.Value, &env))
			if env.EventType == "TransactionPosted" && env.AggregateID == transactionID {
				return env
			}
		}
	}
}
