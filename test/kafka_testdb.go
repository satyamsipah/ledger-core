package test

import (
	"context"
	"io"
	"log/slog"
	"os/exec"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go/modules/redpanda"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/satyamsipah/ledger-core/internal/kafka"
)

// redpandaImage matches the version pinned in deploy/docker-compose.yml, so a
// test failure cannot be explained away as "works on a different broker
// version than production runs."
const redpandaImage = "docker.redpanda.com/redpandadata/redpanda:v24.2.7"

// startRedpanda brings up a broker for exactly one test.
//
// Unlike sharedPool, this is deliberately NOT shared across the package: the
// Kafka-outage test needs to stop and restart this exact container, and a
// container another test is concurrently relying on being up is not a
// container this test can safely stop. The startup cost is real but small
// next to what a shared, always-up broker would cost in test isolation.
func startRedpanda(ctx context.Context, t *testing.T) (container *redpanda.Container, brokers []string) {
	t.Helper()

	c, err := redpanda.Run(ctx, redpandaImage)
	require.NoError(t, err, "start redpanda container")
	// context.Background(), not ctx: t.Cleanup runs after the test's own
	// context may already be done, and a Terminate issued on an expired
	// context would fail immediately and leak the container instead of
	// removing it. Best-effort otherwise -- a container this test may have
	// already stopped (the outage test) errors harmlessly on a second
	// Terminate.
	//nolint:contextcheck // deliberate: t.Cleanup outlives the test's own ctx; see comment above.
	t.Cleanup(func() {
		_ = c.Terminate(context.Background())
	})

	seed, err := c.KafkaSeedBroker(ctx)
	require.NoError(t, err, "get redpanda seed broker")

	return c, []string{seed}
}

// provisionTestTopics creates the real topic layout against a test broker,
// through the exact same code path cmd/kafka-init runs in every other
// environment -- so a test that passes here is a test that has exercised the
// real provisioning logic, not a simplified stand-in for it.
func provisionTestTopics(ctx context.Context, t *testing.T, brokers []string) {
	t.Helper()

	client, err := kgo.NewClient(kgo.SeedBrokers(brokers...))
	require.NoError(t, err, "create kafka client for provisioning")
	defer client.Close()

	admin := kadm.NewClient(client)
	defer admin.Close()

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	require.NoError(t, kafka.Provision(ctx, admin, logger), "provision test topics")
}

// newTestKafkaClient opens a client for a test to produce or consume with
// directly -- reading back what a publisher produced, or a DLQ topic's
// contents.
func newTestKafkaClient(t *testing.T, brokers []string, opts ...kgo.Opt) *kgo.Client {
	t.Helper()

	client, err := kgo.NewClient(append([]kgo.Opt{kgo.SeedBrokers(brokers...)}, opts...)...)
	require.NoError(t, err, "create test kafka client")
	t.Cleanup(client.Close)

	return client
}

// pauseContainer and unpauseContainer freeze and thaw a container's already-
// running process via the kernel cgroup freezer (docker pause/unpause),
// reached through the Docker CLI rather than testcontainers' own Container
// interface, which exposes no pause/unpause at all.
//
// Deliberately not container.Stop()/Start(): the redpanda module's custom
// entrypoint (mounts/entrypoint-tc.sh) waits for its node config to be copied
// in by a lifecycle hook that testcontainers only runs as part of the
// original Run() call. container.Start() on an already-created container
// restarts the entrypoint from scratch without re-running that hook, so the
// entrypoint waits for a signal that never comes again and the broker never
// actually comes back up -- discovered the slow way, as a test that hung for
// its full timeout instead of failing fast. Pausing never touches the
// container's startup sequence at all: the redpanda process itself is
// already running and stays exactly as it was, frozen, which is what makes
// resuming it trivial and fast.
func pauseContainer(t *testing.T, ctx context.Context, containerID string) {
	t.Helper()
	// containerID is Docker's own generated hex container ID
	// (container.GetContainerID()), never external or user-controlled input,
	// so this is not the injection risk G204 exists to catch -- flagged
	// because the linter cannot see the provenance, only that the argument is
	// not a literal.
	//nolint:gosec // G204: containerID is Docker-generated, not external input.
	cmd := exec.CommandContext(ctx, "docker", "pause", containerID)
	output, err := cmd.CombinedOutput()
	require.NoError(t, err, "docker pause %s: %s", containerID, output)
}

func unpauseContainer(t *testing.T, ctx context.Context, containerID string) {
	t.Helper()
	//nolint:gosec // G204: containerID is Docker-generated, not external input; see pauseContainer.
	cmd := exec.CommandContext(ctx, "docker", "unpause", containerID)
	output, err := cmd.CombinedOutput()
	require.NoError(t, err, "docker unpause %s: %s", containerID, output)
}

// consumeN reads exactly n records from topics within timeout, polling
// repeatedly rather than trusting a single PollFetches to return them all at
// once -- a broker under test-suite load can legitimately return fewer
// records per poll than are actually available.
func consumeN(ctx context.Context, t *testing.T, client *kgo.Client, n int, timeout time.Duration) []*kgo.Record {
	t.Helper()

	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	var records []*kgo.Record
	for len(records) < n {
		fetches := client.PollFetches(ctx)
		if err := fetches.Err0(); err != nil {
			require.NoError(t, ctx.Err(), "timed out waiting for %d records, got %d: %v", n, len(records), err)
			continue
		}
		records = append(records, fetches.Records()...)
	}
	return records
}
