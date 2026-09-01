package http

import (
	"context"
	"errors"
	nethttp "net/http"
	"strings"

	"github.com/satyamsipah/ledger-core/internal/auth"
)

// AuthService authenticates a presented API key. Narrow on purpose, matching
// LedgerService and PayoutService: this package depends on the one operation
// it calls, not on auth.Store's full surface (which also issues keys, an
// operation no HTTP request path performs).
type AuthService interface {
	Authenticate(ctx context.Context, rawKey string) (principalID string, err error)
}

// principalContextKey is unexported so no other package can write a principal
// into a request's context, deliberately or by colliding on a string key --
// the same defence idempotencyContextKey uses.
type principalContextKey struct{ name string }

var authPrincipalKey = &principalContextKey{name: "principal"}

// requireAuth authenticates every request on the routes it wraps and makes the
// resulting principal available to everything downstream, including
// requireIdempotency -- which is why this middleware must run before it.
//
// WHAT THIS DOES NOT DO. It does not authorize access to any account or
// balance: this schema has no notion of which accounts a principal may read,
// and building that is real multi-tenancy work, not what D24 asked for. What
// it closes is narrower and specific -- an idempotency key is no longer a
// bearer token for someone else's stored response, because the response is
// now looked up by (principal, key) rather than by key alone.
func requireAuth(service AuthService) func(nethttp.Handler) nethttp.Handler {
	return func(next nethttp.Handler) nethttp.Handler {
		return nethttp.HandlerFunc(func(w nethttp.ResponseWriter, r *nethttp.Request) {
			raw := r.Header.Get("Authorization")
			if raw == "" {
				writeProblem(w, r, auth.ErrMissingAPIKey)
				return
			}

			// "Bearer <key>" is required, not merely a convention here: it is
			// what makes the malformed and missing cases both map cleanly to
			// ErrInvalidAPIKey / ErrMissingAPIKey without this layer having to
			// guess whether a bare string was meant to be a key.
			const prefix = "Bearer "
			if !strings.HasPrefix(raw, prefix) || len(raw) == len(prefix) {
				writeProblem(w, r, auth.ErrInvalidAPIKey)
				return
			}

			principalID, err := service.Authenticate(r.Context(), strings.TrimPrefix(raw, prefix))
			if err != nil {
				if errors.Is(err, auth.ErrInvalidAPIKey) {
					writeProblem(w, r, err)
					return
				}
				// An unexpected failure (Postgres unreachable) is not the
				// caller's fault and must not be reported as an invalid key --
				// that would train a legitimate caller to believe its key
				// stopped working.
				writeProblem(w, r, err)
				return
			}

			ctx := context.WithValue(r.Context(), authPrincipalKey, principalID)
			next.ServeHTTP(w, r.WithContext(ctx))
		})
	}
}

// principalFrom returns the authenticated principal for this request.
//
// Panics if none is present, UNLIKE idempotencyFrom's nil-safe equivalent.
// idempotencyFrom is nil-safe because some routes legitimately have no
// idempotency state; there is no route that calls this function without
// requireAuth having run first, so a missing principal here is a routing bug,
// not a state this package needs to tolerate. A silent empty-string fallback
// would turn that bug into a namespace collision -- exactly what this package
// exists to prevent -- so it panics loudly instead of failing that quietly.
func principalFrom(ctx context.Context) string {
	principalID, ok := ctx.Value(authPrincipalKey).(string)
	if !ok {
		panic("http: principalFrom called without requireAuth in the middleware chain")
	}
	return principalID
}
