package kafka

import (
	"context"
	"fmt"

	"github.com/twmb/franz-go/pkg/kgo"
)

// Checker reports whether a Kafka client can still reach the broker seed
// list, satisfying the same readiness contract internal/db.Pool already
// does (Name/Check) so a service holding both wires them into /readyz the
// identical way.
//
// It wraps an existing *kgo.Client rather than opening one of its own: every
// caller (cmd/projector, cmd/outbox-publisher's polling arm) already holds a
// live client for its real work, and a second connection opened only to ping
// would answer "can a NEW connection reach the broker" rather than "is the
// connection this process actually uses still good" -- a distinction that
// matters the moment a broker is reachable for new connections but wedged
// for an existing one.
type Checker struct {
	client *kgo.Client
}

// NewChecker wraps client for readiness reporting.
func NewChecker(client *kgo.Client) *Checker {
	return &Checker{client: client}
}

// Name identifies this dependency in readiness output.
func (c *Checker) Name() string { return "kafka" }

// Check satisfies the readiness Checker contract. Ping is franz-go's own
// lightweight broker-reachability probe -- a metadata request, not a produce
// or consume -- so this adds no load proportional to topic count or
// partition count.
func (c *Checker) Check(ctx context.Context) error {
	if err := c.client.Ping(ctx); err != nil {
		return fmt.Errorf("ping kafka: %w", err)
	}
	return nil
}
