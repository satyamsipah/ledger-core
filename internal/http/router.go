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

	// Payout and Sagas are nil in a process that does not expose the saga
	// surface. Registered independently of the ledger routes, because starting
	// a payout and reading saga state are separate capabilities: a read-only
	// dashboard backend gets Sagas without Payout.
	Payout PayoutService
	Sagas  SagaReader

	// Reconciliation is nil in a process that does not expose reconciliation
	// reports. Registered independently of the ledger routes for the same
	// reason Sagas is: cmd/reconciler produces the reports, cmd/api serves
	// them, and neither process needs the other's dependencies to do its job.
	Reconciliation ReconciliationReader

	// TrustedProxyHops configures clientIP. Zero -- the default -- trusts
	// nothing in X-Forwarded-For and always uses the raw socket peer. See D19.
	TrustedProxyHops int

	// Auth authenticates every write route. nil in a process that serves only
	// health and metrics, or that has not been wired for it -- in which case
	// the routes it would gate are not registered at all, matching how Ledger
	// and Idempotency being nil already 404s rather than panics.
	Auth AuthService
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
	// clientIP replaces chi's deprecated RealIP with a bounded trusted-hop
	// parser of X-Forwarded-For. TrustedProxyHops defaults to 0 -- nothing sits
	// in front of this service today -- which means every forwarded-for header
	// is ignored outright and r.RemoteAddr is the raw, unforgeable TCP peer.
	// See docs/DECISIONS.md D19.
	r.Use(clientIP(deps.TrustedProxyHops))
	r.Use(recoverer(deps.Logger))
	r.Use(requestLogger(deps.Logger))
	r.Use(requestMetrics(deps.Metrics))

	r.Get("/healthz", handleLive())
	r.Get("/readyz", handleReady(deps.Checkers, deps.Logger))

	r.Route("/v1", func(r chi.Router) {
		// Starting a payout requires an Idempotency-Key like every other write,
		// but not the idempotency MIDDLEWARE: that completes a key inside the
		// ledger's transaction, and a saga has no ledger transaction to complete
		// it in -- nothing has moved yet. saga_instances.idempotency_key dedupes
		// instead.
		//
		// requireAuth wraps it regardless, and must run BEFORE the handler
		// reads principalFrom(r.Context()) -- see docs/DECISIONS.md D24. Every
		// write route in this function follows the same ordering: auth first,
		// then whatever dedupe mechanism that route uses.
		if deps.Payout != nil && deps.Auth != nil {
			r.With(requireAuth(deps.Auth)).Post("/payouts", handleStartPayout(deps.Payout))
		}
		if deps.Sagas != nil {
			r.Route("/sagas", func(r chi.Router) {
				r.Get("/", handleListSagas(deps.Sagas))
				r.Get("/{id}", handleGetSaga(deps.Sagas))
			})
		}

		// Reads only, and authenticated: a reconciliation exception carries
		// amounts and external references, the same class of information
		// D24 scoped idempotency responses to a principal to protect.
		if deps.Reconciliation != nil && deps.Auth != nil {
			r.Route("/reconciliation/runs", func(r chi.Router) {
				r.With(requireAuth(deps.Auth)).Get("/", handleListReconciliationRuns(deps.Reconciliation))
				r.With(requireAuth(deps.Auth)).Get("/{id}", handleGetReconciliationRun(deps.Reconciliation))
			})
		}

		if deps.Ledger == nil || deps.Idempotency == nil || deps.Auth == nil {
			return
		}

		r.Route("/transactions", func(r chi.Router) {
			// Idempotency wraps only the write routes, and it is REQUIRED on
			// them rather than optional. An optional idempotency key is one a
			// client forgets under exactly the conditions -- timeouts, retries,
			// a partial outage -- that make it matter, and the first time that
			// happens it is a duplicate payment rather than a lesson.
			r.With(requireAuth(deps.Auth), requireIdempotency(deps.Idempotency, deps.Logger)).
				Post("/", handlePostTransaction(deps.Ledger))
			r.With(requireAuth(deps.Auth), requireIdempotency(deps.Idempotency, deps.Logger)).
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
