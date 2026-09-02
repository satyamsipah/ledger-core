// Package polling implements the outbox Publisher that reads unpublished rows
// itself and produces them to Kafka, as an alternative to Debezium reading the
// write-ahead log.
//
// See docs/DECISIONS.md D31 for the full comparison; this package exists to
// be the arm of it a test can drive on purpose -- it is a process this
// repository starts and stops, not an offset commit inside Kafka Connect.
package polling

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/satyamsipah/ledger-core/internal/kafka"
	"github.com/satyamsipah/ledger-core/internal/observability"
)

// publisherLabel is this implementation's value for the OutboxPublished{,Errors}
// "publisher" metric label.
const publisherLabel = "polling"

// Config sizes the poll loop.
type Config struct {
	// Interval between polls when the last poll returned fewer than BatchSize
	// rows -- i.e. the backlog looked drained. A full batch is followed
	// immediately by another poll rather than waiting out the interval, so a
	// burst of writes is worked down as fast as the database and Kafka allow
	// rather than throttled to one batch per tick.
	Interval time.Duration

	// BatchSize is the SELECT ... LIMIT. 100 by default, per the task
	// specification: large enough that steady traffic is a handful of round
	// trips a second rather than one per row, small enough that one batch's
	// row locks are held only briefly.
	BatchSize int

	// BatchTimeout bounds one publishBatch attempt end to end: the database
	// transaction AND the Kafka produce inside it. This is what actually
	// enforces the "row locks held across a network call, bounded by a
	// context deadline" design New's doc comment describes -- relying on the
	// caller's kgo.Client to have configured its own delivery timeout would
	// make that bound optional rather than guaranteed, and a dead broker with
	// no client-side timeout configured blocks a batch (and holds its row
	// locks) forever. 10 seconds by default: comfortably above a healthy
	// produce, short enough that Run's next tick retries promptly once the
	// broker is unreachable rather than genuinely slow.
	BatchTimeout time.Duration
}

// row is what one claimed outbox record needs to be produced: the routing
// (aggregate_type -> topic, aggregate_id -> key) and the payload, which is
// already the complete wire envelope (see internal/outbox.Append) and is
// therefore produced verbatim.
type row struct {
	id            int64
	aggregateType string
	aggregateID   string
	payload       []byte

	// traceParent is the W3C value internal/outbox.Append already computed
	// and stored, promoted here onto a Kafka HEADER rather than folded into
	// payload -- the wire body stays byte-for-byte what Append built,
	// regardless of which publisher moves it, and the header is what lets the
	// projector's consumer resume this trace as a child span. Empty for any
	// row written outside a traced context, in which case no header is set at
	// all (see the record-building loop in publishBatch).
	traceParent string
}

// Publisher is the polling implementation of publish.Publisher.
type Publisher struct {
	pool    *pgxpool.Pool
	client  *kgo.Client
	logger  *slog.Logger
	metrics *observability.Metrics
	cfg     Config

	// afterProduceBeforeMark is a test-only hook, nil in production. It runs
	// after Kafka has acknowledged a batch and before the transaction that
	// marks it published is committed -- the exact crash window this design
	// is built around (see docs/DECISIONS.md D30: the gap between "Kafka
	// accepted it" and "this is durably recorded as published" is where a
	// crash produces the at-least-once duplicate every consumer must tolerate).
	// A test sets this to signal a channel and then kills the connection this
	// Publisher is using, deterministically landing in the window rather than
	// guessing at a sleep duration -- the same fault-injection-hook pattern
	// Phase 3 used for the idempotency crash test.
	afterProduceBeforeMark func()
}

// New builds a polling Publisher. pool should be sized for exactly this
// workload if it is not shared -- one held transaction per in-flight batch --
// but sharing the application's own pool is fine at this batch size and
// interval.
func New(pool *pgxpool.Pool, client *kgo.Client, logger *slog.Logger, metrics *observability.Metrics, cfg Config) *Publisher {
	if cfg.Interval <= 0 {
		cfg.Interval = 500 * time.Millisecond
	}
	if cfg.BatchSize <= 0 {
		cfg.BatchSize = 100
	}
	if cfg.BatchTimeout <= 0 {
		cfg.BatchTimeout = 10 * time.Second
	}
	return &Publisher{pool: pool, client: client, logger: logger, metrics: metrics, cfg: cfg}
}

// Run polls until ctx is cancelled.
//
// A publish failure -- Kafka unreachable, a broker rejecting the write -- is
// logged and counted, never fatal: the row locks were never taken outside the
// failed attempt's own rolled-back transaction, so nothing is lost, and the
// next tick simply tries again. This is precisely the behaviour
// TestOutboxPublish_KafkaOutage exercises: stop the broker mid-run and expect
// the backlog to grow, not the process to exit.
func (p *Publisher) Run(ctx context.Context) error {
	ticker := time.NewTicker(p.cfg.Interval)
	defer ticker.Stop()

	p.logger.Info("polling outbox publisher started",
		slog.Duration("interval", p.cfg.Interval),
		slog.Int("batch_size", p.cfg.BatchSize))

	for {
		select {
		case <-ctx.Done():
			p.logger.Info("polling outbox publisher stopped")
			return nil
		case <-ticker.C:
			p.drainOnce(ctx)
		}
	}
}

// drainOnce publishes batches back-to-back until one comes back short of a
// full batch, so a burst of writes is worked down within one tick rather than
// leaking into several.
func (p *Publisher) drainOnce(ctx context.Context) {
	for {
		n, err := p.publishBatch(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) {
				return
			}
			p.logger.ErrorContext(ctx, "publish outbox batch", slog.String("error", err.Error()))
			p.metrics.OutboxPublishErrors.WithLabelValues(publisherLabel).Inc()
			return
		}
		if n > 0 {
			p.metrics.OutboxPublished.WithLabelValues(publisherLabel).Add(float64(n))
		}
		if n < p.cfg.BatchSize {
			return
		}
	}
}

// PublishOnce runs exactly one claim-produce-mark cycle and returns how many
// rows it published.
//
// Exported so a test can drive a single, precisely-timed attempt directly --
// most usefully in combination with a crash hook installed via WithCrashHook
// -- rather than only through Run's ticker, where the exact moment an attempt
// happens is not something a test controls.
func (p *Publisher) PublishOnce(ctx context.Context) (int, error) {
	return p.publishBatch(ctx)
}

// publishBatch is the entire mechanism, in one database transaction: claim
// rows, produce them, mark them published, commit -- or, on any failure,
// implicitly roll back and leave them exactly as they were for the next
// attempt (this Publisher's own retry, or a sibling replica's, via SKIP
// LOCKED -- see docs/DECISIONS.md D31 for why that clause is what makes N
// replicas safe rather than merely convenient).
func (p *Publisher) publishBatch(ctx context.Context) (int, error) {
	ctx, cancel := context.WithTimeout(ctx, p.cfg.BatchTimeout)
	defer cancel()

	tx, err := p.pool.Begin(ctx)
	if err != nil {
		return 0, fmt.Errorf("begin outbox publish transaction: %w", err)
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()

	rows, err := tx.Query(ctx, `
		SELECT id, aggregate_type, aggregate_id, payload, COALESCE(trace_parent, '')
		  FROM outbox
		 WHERE published_at IS NULL
		 ORDER BY id
		   FOR UPDATE SKIP LOCKED
		 LIMIT $1`, p.cfg.BatchSize)
	if err != nil {
		return 0, fmt.Errorf("claim outbox batch: %w", err)
	}

	claimed, err := scanRows(rows)
	if err != nil {
		return 0, err
	}
	if len(claimed) == 0 {
		return 0, nil
	}

	records := make([]*kgo.Record, len(claimed))
	ids := make([]int64, len(claimed))
	for i, r := range claimed {
		rec := &kgo.Record{
			Topic: kafka.TopicForAggregate(r.aggregateType),
			Key:   []byte(r.aggregateID),
			Value: r.payload,
		}
		// Only set when non-empty: a row written outside a traced context
		// (seed data, a backfill) has nothing to propagate, and an empty
		// header is worse than no header at all -- a consumer would have to
		// distinguish "absent" from "present but empty" for no benefit.
		if r.traceParent != "" {
			rec.Headers = []kgo.RecordHeader{{Key: "traceparent", Value: []byte(r.traceParent)}}
		}
		records[i] = rec
		ids[i] = r.id
	}

	// ProduceSync blocks until every record in the batch is acknowledged, or
	// the context (or the client's own configured delivery timeout) ends the
	// attempt. That block is the point: published_at must not be written
	// until Kafka has genuinely accepted the batch, which is what makes
	// holding the row locks across this network call correct rather than
	// merely convenient -- see New's doc comment and D31.
	if err := p.client.ProduceSync(ctx, records...).FirstErr(); err != nil {
		return 0, fmt.Errorf("produce batch of %d: %w", len(records), err)
	}

	if p.afterProduceBeforeMark != nil {
		p.afterProduceBeforeMark()
	}

	if _, err := tx.Exec(ctx, `UPDATE outbox SET published_at = now() WHERE id = ANY($1)`, ids); err != nil {
		return 0, fmt.Errorf("mark %d outbox rows published: %w", len(ids), err)
	}

	if err := tx.Commit(ctx); err != nil {
		return 0, fmt.Errorf("commit outbox publish batch: %w", err)
	}

	return len(claimed), nil
}

func scanRows(rows pgx.Rows) ([]row, error) {
	defer rows.Close()

	var claimed []row
	for rows.Next() {
		var r row
		if err := rows.Scan(&r.id, &r.aggregateType, &r.aggregateID, &r.payload, &r.traceParent); err != nil {
			return nil, fmt.Errorf("scan claimed outbox row: %w", err)
		}
		claimed = append(claimed, r)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("claim outbox batch: %w", err)
	}
	return claimed, nil
}

// WithCrashHook installs the test-only fault-injection hook. Exported and
// separate from Config, rather than a field on it, so it cannot be set by
// accident through ordinary configuration loading -- it is reached for by
// name, from a test, or not at all.
func (p *Publisher) WithCrashHook(hook func()) *Publisher {
	p.afterProduceBeforeMark = hook
	return p
}
