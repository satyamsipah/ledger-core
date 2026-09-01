// Package publish defines the boundary both outbox publishers implement, so
// cmd/outbox-publisher can select between them with one config value and the
// rest of the process neither knows nor cares which one is running.
//
// See docs/DECISIONS.md D31 for the comparison that decided there should be
// two, and D30 for the dual-write problem the whole outbox exists to convert
// into the at-least-once delivery problem these implementations solve.
package publish

import "context"

// Publisher carries committed outbox rows to Kafka -- or, for the Debezium
// implementation, reports the health of the out-of-band process (Kafka
// Connect) that does.
//
// One method, deliberately: Run owns its own loop and its own retry policy,
// because those differ completely between the two implementations (a poll
// interval and a database transaction versus an HTTP health check against
// Kafka Connect's REST API), and a richer interface would have to either
// impose one implementation's shape on the other or grow methods neither
// truly shares.
type Publisher interface {
	// Run blocks until ctx is cancelled, returning nil on a clean shutdown.
	// Any other return is a reason to restart the process, in keeping with
	// every other long-running Run method in this codebase (see
	// internal/idempotency.Sweeper.Run, internal/http.Server.Run).
	Run(ctx context.Context) error
}
