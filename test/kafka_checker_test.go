package test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/satyamsipah/ledger-core/internal/kafka"
)

// TestKafkaChecker_ReportsRealBrokerReachability proves the readiness
// contract cmd/projector and cmd/outbox-publisher's polling arm both wire
// into /readyz: healthy against a real broker, and -- the case that matters
// for a Kubernetes readinessProbe, which exists specifically to catch this --
// unhealthy against one that cannot be reached, rather than reporting ready
// because the client object itself constructed without error.
func TestKafkaChecker_ReportsRealBrokerReachability(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	_, brokers := startRedpanda(ctx, t)

	client, err := kgo.NewClient(kgo.SeedBrokers(brokers...))
	require.NoError(t, err)
	t.Cleanup(client.Close)

	checker := kafka.NewChecker(client)
	assert.Equal(t, "kafka", checker.Name())
	assert.NoError(t, checker.Check(ctx))
}

func TestKafkaChecker_UnreachableBrokerFailsCheck(t *testing.T) {
	t.Parallel()

	// 127.0.0.1:1 is the "unreachable" case done properly: a real address in
	// a range this test does not control the listener on, rather than a DNS
	// name that could resolve differently in a different environment.
	client, err := kgo.NewClient(kgo.SeedBrokers("127.0.0.1:1"))
	require.NoError(t, err, "constructing a client does not itself dial anything")
	t.Cleanup(client.Close)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	checker := kafka.NewChecker(client)
	assert.Error(t, checker.Check(ctx))
}
