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
	}

	registry.MustRegister(
		m.HTTPRequests, m.HTTPDuration,
		m.TxRetries, m.TxAttempts,
		m.IdempotencyOutcomes, m.IdempotencySwept,
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
