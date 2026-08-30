// Package observability wires the three signals this system is operated
// through: structured logs, Prometheus metrics, and OpenTelemetry traces.
//
// Each constructor returns its component rather than installing it globally, so
// that a test can assert on log output or collected spans without racing every
// other test in the binary.
package observability

import (
	"log/slog"
	"os"

	"github.com/satyamsipah/ledger-core/internal/config"
)

// NewLogger builds the JSON logger every component writes through.
//
// JSON rather than text unconditionally, including locally: a log line that is
// only parseable in production is a log line whose format nobody tests until an
// incident. Service and environment are bound once here so no call site has to
// remember to include them.
func NewLogger(cfg config.Observability, service, env string) *slog.Logger {
	handler := slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: cfg.LogLevel,
	})
	return slog.New(handler).With(
		slog.String("service", service),
		slog.String("env", env),
	)
}
