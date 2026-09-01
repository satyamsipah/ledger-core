// Package http builds the service's HTTP surface: router, middleware, health
// endpoints and the server lifecycle.
//
// The package is named after its directory, which shadows the standard library,
// so net/http is imported as nethttp throughout. Handlers in this package do no
// SQL and hold no business logic; they translate HTTP to a service call and
// back, per the layering rule in CLAUDE.md.
package http

import (
	"context"
	"log/slog"
	nethttp "net/http"

	"github.com/go-chi/chi/v5"
	chimiddleware "github.com/go-chi/chi/v5/middleware"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"

	"github.com/satyamsipah/ledger-core/internal/idempotency"
	"github.com/satyamsipah/ledger-core/internal/observability"
)

// Checker is a dependency that can report whether it is usable right now.
//
// Declared here rather than imported so that internal/http does not depend on
// every package it health-checks; *db.Pool satisfies this structurally.
type Checker interface {
	Name() string
	Check(ctx context.Context) error
}

// Deps are the collaborators the router needs. Passing them in a struct keeps
// the constructor signature stable as the surface grows, and keeps every
// dependency injected rather than reached for.
type Deps struct {
	Service  string
	Logger   *slog.Logger
	Metrics  *observability.Metrics
	Checkers []Checker

	// Ledger and Idempotency are nil in a process that serves only health and
	// metrics. The /v1 routes are registered only when both are present, so a
	// misconfigured process 404s rather than panicking on the first request.
	Ledger      LedgerService
	Idempotency *idempotency.Manager
}

// NewRouter assembles the public API.
//
// Middleware order matters and is not arbitrary: RequestID first so every
// later layer can log it, Recoverer before anything that can panic, and the
// metrics/logging pair innermost so they observe the handler's real status
// rather than one rewritten by an outer layer.
func NewRouter(deps Deps) nethttp.Handler {
	// otelhttp wraps the whole router so the server span covers middleware too;
	// a span that starts inside the handler cannot show you time lost in
	// middleware, which is exactly where surprises live.
	return otelhttp.NewHandler(NewMux(deps), "http.server",
		otelhttp.WithSpanNameFormatter(func(_ string, r *nethttp.Request) string {
			if route := chi.RouteContext(r.Context()); route != nil && route.RoutePattern() != "" {
				return r.Method + " " + route.RoutePattern()
			}
			return r.Method
		}),
	)
}

// NewMux builds the router without the tracing wrapper.
//
// Exported so the OpenAPI conformance test can walk the registered routes with
// chi.Walk and compare them against api/openapi.yaml in both directions. That
// matters more than it looks: a specification nobody checks is a specification
// that drifts, and the drift is discovered by a client integrating against a
// path that no longer exists.
func NewMux(deps Deps) chi.Router {
	r := chi.NewRouter()

	r.Use(chimiddleware.RequestID)
	// RealIP trusts client-controlled headers, so the address it writes into
	// r.RemoteAddr is spoofable until something in front strips them. Suppressed
	// rather than fixed because the correct behaviour depends on the deployment
	// topology -- which proxy, how many hops -- and that is settled by the Phase 3
	// gateway design. Do not treat r.RemoteAddr as trustworthy for rate limiting,
	// fraud signals, or audit until then. See docs/DECISIONS.md D19.
	//nolint:staticcheck // SA1019: deliberate, tracked as a known gap in D19.
	r.Use(chimiddleware.RealIP)
	r.Use(recoverer(deps.Logger))
	r.Use(requestLogger(deps.Logger))
	r.Use(requestMetrics(deps.Metrics))

	r.Get("/healthz", handleLive())
	r.Get("/readyz", handleReady(deps.Checkers, deps.Logger))

	r.Route("/v1", func(r chi.Router) {
		if deps.Ledger == nil || deps.Idempotency == nil {
			return
		}

		r.Route("/transactions", func(r chi.Router) {
			// Idempotency wraps only the write routes, and it is REQUIRED on
			// them rather than optional. An optional idempotency key is one a
			// client forgets under exactly the conditions -- timeouts, retries,
			// a partial outage -- that make it matter, and the first time that
			// happens it is a duplicate payment rather than a lesson.
			r.With(requireIdempotency(deps.Idempotency, deps.Logger)).
				Post("/", handlePostTransaction(deps.Ledger))
			r.With(requireIdempotency(deps.Idempotency, deps.Logger)).
				Post("/{id}/reverse", handleReverseTransaction(deps.Ledger))
		})

		// Reads take no key: they create nothing, so there is nothing for a
		// retry to duplicate.
		r.Route("/accounts", func(r chi.Router) {
			r.Get("/{id}/balance", handleGetBalance(deps.Ledger))
			r.Get("/{id}/statement", handleGetStatement(deps.Ledger))
		})
	})

	return r
}
