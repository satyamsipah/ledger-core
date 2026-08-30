package http

import (
	"log/slog"
	nethttp "net/http"
	"runtime/debug"
	"strconv"
	"time"

	"github.com/go-chi/chi/v5"
	chimiddleware "github.com/go-chi/chi/v5/middleware"
	"go.opentelemetry.io/otel/trace"

	"github.com/satyamsipah/ledger-core/internal/observability"
)

// requestLogger emits one structured line per request, carrying the request ID
// and the trace ID.
//
// The trace ID is logged rather than only traced because logs are what you have
// during an incident when the collector is the thing that fell over.
func requestLogger(logger *slog.Logger) func(nethttp.Handler) nethttp.Handler {
	return func(next nethttp.Handler) nethttp.Handler {
		return nethttp.HandlerFunc(func(w nethttp.ResponseWriter, r *nethttp.Request) {
			started := time.Now()
			ww := chimiddleware.NewWrapResponseWriter(w, r.ProtoMajor)

			next.ServeHTTP(ww, r)

			attrs := []any{
				slog.String("method", r.Method),
				slog.String("path", r.URL.Path),
				slog.String("route", routePattern(r)),
				slog.Int("status", ww.Status()),
				slog.Int("bytes", ww.BytesWritten()),
				slog.Duration("duration", time.Since(started)),
				slog.String("request_id", chimiddleware.GetReqID(r.Context())),
			}
			if sc := trace.SpanContextFromContext(r.Context()); sc.IsValid() {
				attrs = append(attrs, slog.String("trace_id", sc.TraceID().String()))
			}

			// 5xx is the service's own fault and deserves an error line; 4xx is
			// the caller's and would otherwise page someone for a typo.
			if ww.Status() >= nethttp.StatusInternalServerError {
				logger.LogAttrs(r.Context(), slog.LevelError, "request failed", toAttrs(attrs)...)
				return
			}
			logger.LogAttrs(r.Context(), slog.LevelInfo, "request", toAttrs(attrs)...)
		})
	}
}

// requestMetrics records count and latency, labelled by route pattern.
//
// Route pattern, never raw path: a ledger URL contains account and transaction
// UUIDs, and labelling by path would mint a new time series per account and
// take the metrics backend down long before the ledger.
func requestMetrics(metrics *observability.Metrics) func(nethttp.Handler) nethttp.Handler {
	return func(next nethttp.Handler) nethttp.Handler {
		return nethttp.HandlerFunc(func(w nethttp.ResponseWriter, r *nethttp.Request) {
			started := time.Now()
			ww := chimiddleware.NewWrapResponseWriter(w, r.ProtoMajor)

			next.ServeHTTP(ww, r)

			route := routePattern(r)
			metrics.HTTPRequests.WithLabelValues(route, r.Method, statusClass(ww.Status())).Inc()
			metrics.HTTPDuration.WithLabelValues(route, r.Method).Observe(time.Since(started).Seconds())
		})
	}
}

// recoverer turns a panic into a 500 and a log line with the stack.
//
// chi ships one, but it writes the stack to stdout unstructured; in a ledger
// the panic is the most important log line of the day and it belongs in the
// same JSON stream as everything else.
func recoverer(logger *slog.Logger) func(nethttp.Handler) nethttp.Handler {
	return func(next nethttp.Handler) nethttp.Handler {
		return nethttp.HandlerFunc(func(w nethttp.ResponseWriter, r *nethttp.Request) {
			defer func() {
				rec := recover()
				if rec == nil {
					return
				}
				// A client disconnecting mid-write is not a panic worth paging
				// on; re-panic so the server handles it as it normally would.
				if err, ok := rec.(error); ok && err == nethttp.ErrAbortHandler {
					panic(rec)
				}
				logger.LogAttrs(r.Context(), slog.LevelError, "panic recovered",
					slog.Any("panic", rec),
					slog.String("method", r.Method),
					slog.String("path", r.URL.Path),
					slog.String("request_id", chimiddleware.GetReqID(r.Context())),
					slog.String("stack", string(debug.Stack())),
				)
				writeJSON(w, nethttp.StatusInternalServerError, map[string]string{
					"error": "internal error",
				})
			}()
			next.ServeHTTP(w, r)
		})
	}
}

// routePattern returns the matched chi pattern, falling back to a constant for
// unmatched requests so 404 floods cannot create unbounded label values.
func routePattern(r *nethttp.Request) string {
	if ctx := chi.RouteContext(r.Context()); ctx != nil {
		if pattern := ctx.RoutePattern(); pattern != "" {
			return pattern
		}
	}
	return "unmatched"
}

// statusClass buckets a status into 2xx/4xx/5xx form, keeping cardinality at
// five series per route instead of one per distinct code.
func statusClass(status int) string {
	if status == 0 {
		status = nethttp.StatusOK
	}
	return strconv.Itoa(status/100) + "xx"
}

func toAttrs(kv []any) []slog.Attr {
	attrs := make([]slog.Attr, 0, len(kv))
	for _, v := range kv {
		if a, ok := v.(slog.Attr); ok {
			attrs = append(attrs, a)
		}
	}
	return attrs
}
