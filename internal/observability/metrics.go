package observability

import (
	nethttp "net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Metrics owns a private registry and the collectors registered against it.
//
// A private registry rather than prometheus.DefaultRegisterer because the
// default one is process-global state: two tests that both register a counter
// named the same thing panic, and a library dependency can silently pollute
// your metric namespace.
type Metrics struct {
	registry *prometheus.Registry

	// HTTPRequests counts requests by route, method and status class. Route
	// rather than raw path, so a UUID in the URL cannot explode cardinality.
	HTTPRequests *prometheus.CounterVec

	// HTTPDuration records latency by route and method.
	HTTPDuration *prometheus.HistogramVec

	// TransactionsPosted counts every ledger.Service.PostTransaction and
	// reverse call, by transaction type and outcome (success/error).
	//
	// Recorded in internal/ledger, not in the HTTP handler, because the HTTP
	// API is not the only caller: the saga orchestrator posts and reverses
	// transactions directly through this same service. Recording only at the
	// HTTP layer would double-count nothing (there is no HTTP request for a
	// saga's own posts) but would silently miss every one of them, which is
	// worse -- a dashboard that looks complete and is not.
	TransactionsPosted *prometheus.CounterVec

	// TransactionDuration records how long one PostTransaction or reverse call
	// took, by type, success or failure alike -- a slow rejection still held
	// row locks for that long, and hiding it behind a success-only histogram
	// would hide exactly the case (lock contention aborting into an error)
	// most worth seeing.
	TransactionDuration *prometheus.HistogramVec

	// TxRetries counts database transactions retried, by SQLSTATE and by the
	// operation that was retrying.
	//
	// This is an assertion about D11, not merely an operational gauge. The
	// ordered locking in pgledger.LockAccounts is supposed to make deadlocks
	// unconstructible, and a 40P01 series that stays flat at zero is the
	// continuous proof of it -- far stronger than a test that exercises one
	// scenario. A 40P01 that starts counting means a write path has begun
	// taking locks in some other order, and it says so before anyone reports a
	// failed payment.
	TxRetries *prometheus.CounterVec

	// TxAttempts records how many attempts each operation needed, so the abort
	// rate is a distribution rather than a total. One transaction retried four
	// times and four retried once produce the same counter and very different
	// systems.
	TxAttempts *prometheus.HistogramVec

	// IdempotencyOutcomes counts what the idempotency state machine decided:
	// acquired, replayed, conflict, in_progress, expired, reclaimed, released,
	// failed, cache_hit, cache_miss.
	//
	// Unlabelled by route on purpose. The interesting questions -- what
	// fraction of traffic is retries, is the cache earning its dependency, are
	// leases being reclaimed (which means requests are dying mid-flight) -- are
	// all answered in aggregate, and a route label would multiply the series
	// count for no extra answer.
	IdempotencyOutcomes *prometheus.CounterVec

	// IdempotencySwept counts expired records deleted by the TTL sweeper. A
	// counter that stops moving while traffic continues means the sweeper has
	// died, which is otherwise invisible until the table is large enough to
	// hurt.
	IdempotencySwept prometheus.Counter

	// OutboxPublished counts outbox rows successfully published, labelled by
	// which implementation did it. Comparable across a config-flag switch
	// between "polling" and "debezium" is the whole point of D31's design --
	// this is the number that answers "did switching actually change
	// anything."
	OutboxPublished *prometheus.CounterVec

	// OutboxPublishErrors counts failed publish attempts. Not fatal by design
	// -- both publishers retry -- but a sustained rate here means Kafka is
	// unreachable or rejecting writes, and is the earliest signal of exactly
	// the "Kafka is down" scenario the failure tests drive on purpose.
	OutboxPublishErrors *prometheus.CounterVec

	// OutboxBacklog is the current count of unpublished outbox rows. Not
	// labelled by publisher: backlog is a property of the outbox table, which
	// only one publisher is ever draining against at a time given the
	// mutually-exclusive config flag, so a second label would only ever have
	// one non-zero value.
	OutboxBacklog prometheus.Gauge

	// OutboxLagSeconds is the age of the OLDEST unpublished outbox row, not
	// merely the count. The two answer different questions: a thousand rows
	// written a second ago is a healthy burst, and five rows sitting for ten
	// minutes is a stalled publisher -- OutboxBacklog alone cannot tell those
	// apart, and this is the alert the phase's Definition of Done actually
	// asks for ("outbox lag > 30s"). Computed and set by whichever process
	// owns the outbox table's monitoring query, regardless of which publisher
	// arm (polling or Debezium) is actually draining it -- see cmd/outbox-publisher.
	OutboxLagSeconds prometheus.Gauge

	// ProjectorConsumerLag is the standard shape -- topic and partition,
	// computed the way any Kafka consumer-lag exporter would (high watermark
	// minus committed offset) rather than derived from client-side fetch
	// counters, which measure a different and less operationally useful
	// thing.
	ProjectorConsumerLag *prometheus.GaugeVec

	// ProjectorEventsProcessed counts events the projector applied, and
	// ProjectorDuplicatesSkipped counts ones processed_events already knew
	// about -- the metric that turns "at-least-once delivery" from a design
	// document's claim into an operationally visible number.
	ProjectorEventsProcessed   *prometheus.CounterVec
	ProjectorDuplicatesSkipped prometheus.Counter

	// ProjectorDLQTotal counts messages the projector could not apply and
	// routed to the dead-letter topic. Anything other than zero here is worth
	// paging on: it means either a poison message or a projection invariant
	// violation, and the replay procedure documented in docs/DECISIONS.md
	// only helps once someone has noticed.
	ProjectorDLQTotal prometheus.Counter

	// SagaSteps counts saga step attempts by step, direction and outcome.
	//
	// Direction is a label rather than a separate metric because the ratio
	// between them is the number that matters: forward steps vastly
	// outnumbering compensations is a healthy system, and the two series
	// converging means something upstream is failing.
	SagaSteps *prometheus.CounterVec

	// SagaGatewayProbes counts resolution probes by what they concluded.
	//
	// The "unknown" series is the one to alert on. A probe that cannot settle
	// the question is the leading indicator of a saga heading for manual
	// review, and it fires while the money is still recoverable rather than
	// after a human has been paged.
	SagaGatewayProbes *prometheus.CounterVec

	// SagaManualReview counts sagas that stopped and require a human, by the
	// reason they stopped.
	//
	// THIS IS THE ALERT. It is a counter rather than a gauge on purpose: a
	// gauge returning to zero because somebody resolved the saga erases the
	// fact that it ever happened, and "how often does this happen" is exactly
	// what decides whether the compensation budget is set correctly.
	SagaManualReview *prometheus.CounterVec

	// SagaInstances is the current population by status, refreshed on a ticker.
	//
	// Labelled by status only, never by saga id: an id label would grow one
	// series per payout and take the scrape down within a day. The stuck sagas
	// themselves are listed through the API, which is the right tool for
	// unbounded identifiers.
	SagaInstances *prometheus.GaugeVec

	// SagaOldestOverdueSeconds is how long the most-overdue non-terminal saga
	// has been past its step deadline, at last check. Zero when nothing is
	// overdue. THIS IS THE "saga stuck" ALERT -- a population count
	// (SagaInstances) cannot distinguish a healthy system with several sagas
	// in flight from one where the oldest of them has been stuck for an
	// hour, and this is the number that can.
	SagaOldestOverdueSeconds prometheus.Gauge

	// ReconciliationRuns counts completed three-way-match runs by outcome
	// (completed/failed). A run is a daily job; this is what answers "did
	// today's reconciliation even happen" without reading logs.
	ReconciliationRuns *prometheus.CounterVec

	// ReconciliationExceptions counts unresolved findings by category. THIS IS
	// THE ALERT the phase asks for: reconciliation_exceptions_total > 0 pages,
	// because every category above TIMING_DIFFERENCE-within-window represents
	// a disagreement between this ledger and money that actually moved
	// somewhere else. A counter, not a gauge, for the same reason
	// SagaManualReview is one -- an exception later marked RESOLVED must not
	// erase the fact that it happened.
	ReconciliationExceptions *prometheus.CounterVec

	// GlobalInvariantViolation is the signed total CheckGlobalInvariant found
	// for one currency, zero when that currency's journal balances. THIS IS
	// AN ALERT: the phase's own framing is "if ever non-zero, page
	// immediately," because a nonzero value here means invariant 1 -- the
	// one CLAUDE.md calls non-negotiable -- no longer holds somewhere in the
	// journal. Reset to zero every check so a currency that stops violating
	// does not leave a stale series behind; see cmd/reconciler's consistency
	// loop for where that reset happens.
	GlobalInvariantViolation *prometheus.GaugeVec

	// ProjectionDriftAccounts is how many accounts CheckProjectionDrift found
	// disagreeing between account_balances and the journal, at last check.
	ProjectionDriftAccounts prometheus.Gauge

	// OrphanTransactions and OrphanEntries are CheckOrphans' two findings, at
	// last check -- POSTED/REVERSED transactions with fewer than two entries,
	// and journal_entries rows with no parent transaction respectively.
	OrphanTransactions prometheus.Gauge
	OrphanEntries      prometheus.Gauge
}

// NewMetrics builds the registry and registers the process collectors.
func NewMetrics(service string) *Metrics {
	registry := prometheus.NewRegistry()
	registry.MustRegister(
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
	)

	m := &Metrics{
		registry: registry,
		HTTPRequests: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace:   "ledger",
			Subsystem:   "http",
			Name:        "requests_total",
			Help:        "Total HTTP requests by route, method and status class.",
			ConstLabels: prometheus.Labels{"service": service},
		}, []string{"route", "method", "status_class"}),
		HTTPDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace:   "ledger",
			Subsystem:   "http",
			Name:        "request_duration_seconds",
			Help:        "HTTP request latency by route and method.",
			ConstLabels: prometheus.Labels{"service": service},
			// Buckets skewed low: this is a ledger write path, where the
			// interesting question is "how many requests exceeded 100ms", not
			// how the multi-second tail is shaped.
			Buckets: []float64{0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5},
		}, []string{"route", "method"}),
		TransactionsPosted: prometheus.NewCounterVec(prometheus.CounterOpts{
			// No Subsystem: this is the core domain event, not a layer within
			// the service the way "http" or "db" are, and "ledger_ledger_"
			// reads as a typo rather than a namespace.
			Namespace:   "ledger",
			Name:        "transactions_posted_total",
			Help:        "PostTransaction and reverse calls, by transaction type and outcome.",
			ConstLabels: prometheus.Labels{"service": service},
		}, []string{"type", "status"}),
		TransactionDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace:   "ledger",
			Name:        "transaction_duration_seconds",
			Help:        "PostTransaction and reverse call latency, by transaction type.",
			ConstLabels: prometheus.Labels{"service": service},
			// Same skewed-low shape as HTTPDuration and for the same reason:
			// this is the write path row locks are held under, and the
			// question that matters is how many exceeded 100ms, not how the
			// multi-second tail looks.
			Buckets: []float64{0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5},
		}, []string{"type"}),
		TxRetries: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace:   "ledger",
			Subsystem:   "db",
			Name:        "tx_retries_total",
			Help:        "Database transactions retried, by SQLSTATE and operation.",
			ConstLabels: prometheus.Labels{"service": service},
		}, []string{"operation", "sqlstate"}),
		TxAttempts: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace:   "ledger",
			Subsystem:   "db",
			Name:        "tx_attempts",
			Help:        "Attempts required per database transaction, by operation.",
			ConstLabels: prometheus.Labels{"service": service},
			// Linear and small: the retry cap is five, so anything past that is
			// impossible and the interesting shape is entirely in the first few.
			Buckets: []float64{1, 2, 3, 4, 5},
		}, []string{"operation"}),
		IdempotencyOutcomes: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace:   "ledger",
			Subsystem:   "idempotency",
			Name:        "outcomes_total",
			Help:        "Idempotency state machine decisions by outcome.",
			ConstLabels: prometheus.Labels{"service": service},
		}, []string{"outcome"}),
		IdempotencySwept: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace:   "ledger",
			Subsystem:   "idempotency",
			Name:        "swept_total",
			Help:        "Expired idempotency records deleted by the TTL sweeper.",
			ConstLabels: prometheus.Labels{"service": service},
		}),
		OutboxPublished: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace:   "ledger",
			Subsystem:   "outbox",
			Name:        "published_total",
			Help:        "Outbox rows successfully published, by publisher implementation.",
			ConstLabels: prometheus.Labels{"service": service},
		}, []string{"publisher"}),
		OutboxPublishErrors: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace:   "ledger",
			Subsystem:   "outbox",
			Name:        "publish_errors_total",
			Help:        "Failed publish attempts, by publisher implementation.",
			ConstLabels: prometheus.Labels{"service": service},
		}, []string{"publisher"}),
		OutboxBacklog: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace:   "ledger",
			Subsystem:   "outbox",
			Name:        "backlog",
			Help:        "Unpublished outbox rows at last check.",
			ConstLabels: prometheus.Labels{"service": service},
		}),
		OutboxLagSeconds: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace:   "ledger",
			Subsystem:   "outbox",
			Name:        "lag_seconds",
			Help:        "Age of the oldest unpublished outbox row, in seconds, at last check. Zero when nothing is unpublished.",
			ConstLabels: prometheus.Labels{"service": service},
		}),
		ProjectorConsumerLag: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace:   "ledger",
			Subsystem:   "projector",
			Name:        "consumer_lag",
			Help:        "Consumer group lag by topic and partition.",
			ConstLabels: prometheus.Labels{"service": service},
		}, []string{"topic", "partition"}),
		ProjectorEventsProcessed: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace:   "ledger",
			Subsystem:   "projector",
			Name:        "events_processed_total",
			Help:        "Events applied to the balance projection, by event type.",
			ConstLabels: prometheus.Labels{"service": service},
		}, []string{"event_type"}),
		ProjectorDuplicatesSkipped: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace:   "ledger",
			Subsystem:   "projector",
			Name:        "duplicates_skipped_total",
			Help:        "Redelivered events discarded by processed_events dedupe.",
			ConstLabels: prometheus.Labels{"service": service},
		}),
		ProjectorDLQTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace:   "ledger",
			Subsystem:   "projector",
			Name:        "dlq_total",
			Help:        "Messages the projector could not apply and routed to the dead-letter topic.",
			ConstLabels: prometheus.Labels{"service": service},
		}),

		SagaSteps: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace:   "ledger",
			Subsystem:   "saga",
			Name:        "steps_total",
			Help:        "Saga step attempts, by step, direction and outcome.",
			ConstLabels: prometheus.Labels{"service": service},
		}, []string{"step", "direction", "outcome"}),

		SagaGatewayProbes: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace:   "ledger",
			Subsystem:   "saga",
			Name:        "gateway_probes_total",
			Help:        "Gateway resolution probes, by what they concluded.",
			ConstLabels: prometheus.Labels{"service": service},
		}, []string{"outcome"}),

		SagaManualReview: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace:   "ledger",
			Subsystem:   "saga",
			Name:        "manual_review_total",
			Help:        "Sagas that stopped and require human resolution, by reason.",
			ConstLabels: prometheus.Labels{"service": service},
		}, []string{"saga_type", "reason"}),

		SagaInstances: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace:   "ledger",
			Subsystem:   "saga",
			Name:        "instances",
			Help:        "Saga instances by status.",
			ConstLabels: prometheus.Labels{"service": service},
		}, []string{"status"}),

		SagaOldestOverdueSeconds: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace:   "ledger",
			Subsystem:   "saga",
			Name:        "oldest_overdue_seconds",
			Help:        "Age in seconds of the most-overdue non-terminal saga, at last check. Zero when nothing is overdue.",
			ConstLabels: prometheus.Labels{"service": service},
		}),

		ReconciliationRuns: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace:   "ledger",
			Subsystem:   "reconciliation",
			Name:        "runs_total",
			Help:        "Three-way-match reconciliation runs, by outcome.",
			ConstLabels: prometheus.Labels{"service": service},
		}, []string{"status"}),

		ReconciliationExceptions: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace:   "ledger",
			Subsystem:   "reconciliation",
			Name:        "exceptions_total",
			Help:        "Reconciliation findings by category. Alert on any category other than an auto-resolved timing difference.",
			ConstLabels: prometheus.Labels{"service": service},
		}, []string{"category"}),

		GlobalInvariantViolation: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace:   "ledger",
			Subsystem:   "consistency",
			Name:        "global_invariant_violation_minor",
			Help:        "Signed sum of journal_entries for one currency; nonzero means invariant 1 is broken. Alert on != 0.",
			ConstLabels: prometheus.Labels{"service": service},
		}, []string{"currency"}),

		ProjectionDriftAccounts: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace:   "ledger",
			Subsystem:   "consistency",
			Name:        "projection_drift_accounts",
			Help:        "Accounts where account_balances disagrees with the journal, at last check.",
			ConstLabels: prometheus.Labels{"service": service},
		}),

		OrphanTransactions: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace:   "ledger",
			Subsystem:   "consistency",
			Name:        "orphan_transactions",
			Help:        "POSTED or REVERSED transactions with fewer than two journal entries, at last check.",
			ConstLabels: prometheus.Labels{"service": service},
		}),

		OrphanEntries: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace:   "ledger",
			Subsystem:   "consistency",
			Name:        "orphan_entries",
			Help:        "journal_entries rows with no parent transaction, at last check. Should never be nonzero given the foreign key.",
			ConstLabels: prometheus.Labels{"service": service},
		}),
	}

	registry.MustRegister(
		m.HTTPRequests, m.HTTPDuration,
		m.TransactionsPosted, m.TransactionDuration,
		m.TxRetries, m.TxAttempts,
		m.IdempotencyOutcomes, m.IdempotencySwept,
		m.OutboxPublished, m.OutboxPublishErrors, m.OutboxBacklog, m.OutboxLagSeconds,
		m.ProjectorConsumerLag, m.ProjectorEventsProcessed,
		m.ProjectorDuplicatesSkipped, m.ProjectorDLQTotal,
		m.SagaSteps, m.SagaGatewayProbes, m.SagaManualReview, m.SagaInstances,
		m.SagaOldestOverdueSeconds,
		m.ReconciliationRuns, m.ReconciliationExceptions,
		m.GlobalInvariantViolation, m.ProjectionDriftAccounts,
		m.OrphanTransactions, m.OrphanEntries,
	)
	return m
}

// Handler returns the scrape endpoint for this registry. It is served on a
// separate listener from the public API so metrics are never internet-exposed
// by accident.
func (m *Metrics) Handler() nethttp.Handler {
	return promhttp.HandlerFor(m.registry, promhttp.HandlerOpts{
		Registry: m.registry,
	})
}

// Registry exposes the underlying registry for tests that need to gather.
func (m *Metrics) Registry() *prometheus.Registry { return m.registry }
