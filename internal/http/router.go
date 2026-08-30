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
}

// NewRouter assembles the public API.
//
// Middleware order matters and is not arbitrary: RequestID first so every
// later layer can log it, Recoverer before anything that can panic, and the
// metrics/logging pair innermost so they observe the handler's real status
// rather than one rewritten by an outer layer.
func NewRouter(deps Deps) nethttp.Handler {
	r := chi.NewRouter()

	r.Use(chimiddleware.RequestID)
	r.Use(chimiddleware.RealIP)
	r.Use(recoverer(deps.Logger))
	r.Use(requestLogger(deps.Logger))
	r.Use(requestMetrics(deps.Metrics))

	r.Get("/healthz", handleLive())
	r.Get("/readyz", handleReady(deps.Checkers, deps.Logger))

	// Phase 1 exposes no ledger endpoints. /v1 exists so the versioning
	// decision is made now, while it is free, rather than after the first
	// client has hardcoded an unversioned path.
	r.Route("/v1", func(r chi.Router) {})

	// otelhttp wraps the whole router so the server span covers middleware too;
	// a span that starts inside the handler cannot show you time lost in
	// middleware, which is exactly where surprises live.
	return otelhttp.NewHandler(r, "http.server",
		otelhttp.WithSpanNameFormatter(func(_ string, r *nethttp.Request) string {
			if route := chi.RouteContext(r.Context()); route != nil && route.RoutePattern() != "" {
				return r.Method + " " + route.RoutePattern()
			}
			return r.Method
		}),
	)
}
