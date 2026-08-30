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
	}

	registry.MustRegister(m.HTTPRequests, m.HTTPDuration)
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
