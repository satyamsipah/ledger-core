package http

import (
	"context"
	"encoding/json"
	"log/slog"
	nethttp "net/http"
	"time"
)

// readinessTimeout bounds the whole readiness probe. Kubernetes will give up on
// its own schedule; returning a fast 503 is more useful than a probe that hangs
// until the kubelet's timeout and tells the operator nothing.
const readinessTimeout = 2 * time.Second

// handleLive answers the liveness probe.
//
// Deliberately checks nothing: liveness asks "is this process wedged?", and
// answering it with a database ping means one slow query restarts every replica
// at once. Dependency health belongs in readiness.
func handleLive() nethttp.HandlerFunc {
	return func(w nethttp.ResponseWriter, _ *nethttp.Request) {
		writeJSON(w, nethttp.StatusOK, map[string]string{"status": "ok"})
	}
}

// handleReady reports whether this instance can serve traffic, checking every
// injected dependency and naming the ones that failed.
//
// It names them because a 503 with no detail turns every readiness failure into
// a shell session on a pod.
func handleReady(checkers []Checker, logger *slog.Logger) nethttp.HandlerFunc {
	return func(w nethttp.ResponseWriter, r *nethttp.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), readinessTimeout)
		defer cancel()

		results := make(map[string]string, len(checkers))
		ready := true
		for _, c := range checkers {
			if err := c.Check(ctx); err != nil {
				ready = false
				results[c.Name()] = err.Error()
				logger.WarnContext(ctx, "readiness check failed",
					slog.String("dependency", c.Name()),
					slog.String("error", err.Error()))
				continue
			}
			results[c.Name()] = "ok"
		}

		status := nethttp.StatusOK
		if !ready {
			status = nethttp.StatusServiceUnavailable
		}
		writeJSON(w, status, map[string]any{
			"status": map[bool]string{true: "ready", false: "not ready"}[ready],
			"checks": results,
		})
	}
}

// writeJSON is the single place a response body is encoded, so content type and
// status ordering cannot drift between handlers.
func writeJSON(w nethttp.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	// The header and status are already committed, so an encoding failure can
	// only be logged by the caller's middleware, not corrected here.
	_ = json.NewEncoder(w).Encode(body)
}
