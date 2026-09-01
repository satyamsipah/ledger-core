package kafka

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
)

// Topic names. AggregateType selects which of these a Debezium-routed or
// polling-published event lands on -- see outbox.AggregateTransaction,
// outbox.AggregateAccount, outbox.AggregateSaga, whose values are the suffix
// after "ledger.events." here by construction
// (route.topic.replacement: "ledger.events.${routedByValue}" in the connector
// config, and the polling publisher makes the identical string join).
const (
	TopicTransaction = "ledger.events.transaction"
	TopicAccount     = "ledger.events.account"
	TopicSaga        = "ledger.events.saga"

	// TopicDLQ receives poison messages from two independent sources: Kafka
	// Connect's own errors.deadletterqueue (a message the SMT or converter
	// could not process at all) and the projector's consumer-side DLQ (a
	// message that parsed fine but failed to apply -- an unknown event_type,
	// a projection invariant violation). One shared topic rather than one per
	// source, because the operational question is always the same regardless
	// of origin -- "what's in the DLQ and why" -- and a replay procedure that
	// has to check two places is a replay procedure that gets one of them
	// forgotten during an incident.
	TopicDLQ = "ledger.events.dlq"
)

// TopicForAggregate returns the topic an event with this aggregate_type
// routes to. Matches the Debezium EventRouter's
// route.topic.replacement: "ledger.events.${routedByValue}" exactly, which is
// what lets the polling publisher and Debezium agree on where a given row
// belongs without either configuring the other.
func TopicForAggregate(aggregateType string) string {
	return "ledger.events." + aggregateType
}

// TopicSpec is one topic's explicit configuration.
type TopicSpec struct {
	Name              string
	Partitions        int32
	ReplicationFactor int16
	Configs           map[string]string
}

// Topics is the complete layout. Partition counts are sized to this
// AggregateType's expected relative volume, not to a single default copied
// across every topic:
//
//   - account gets the most (12): BalanceUpdated is emitted once per account
//     touched by every transaction, so it is the highest-volume topic in the
//     system by construction -- a transfer alone produces two of these for
//     one TransactionPosted.
//   - transaction gets a middle value (6): one message per transaction,
//     regardless of how many accounts it touches.
//   - saga gets the least (3): step-completion events are the rarest thing
//     this service emits.
//   - dlq gets the same as saga: it should see traffic only when something is
//     wrong, and if it needs more partitions than that, the alert that fires
//     on ledger_projector_dlq_total is the thing to act on, not the topic's
//     partition count.
//
// These are LOCAL DEVELOPMENT DEFAULTS. Replication factor 1 has no
// redundancy at all, which is correct for a single-broker Redpanda container
// and wrong for anything else; production sizing (partitions for target
// consumer parallelism, replication factor and min.insync.replicas for the
// durability the deployment actually needs) is a capacity-planning exercise
// this repository does not attempt to guess on your behalf.
var Topics = []TopicSpec{
	{
		Name:              TopicTransaction,
		Partitions:        6,
		ReplicationFactor: 1,
		Configs:           standardConfig(),
	},
	{
		Name:              TopicAccount,
		Partitions:        12,
		ReplicationFactor: 1,
		Configs:           standardConfig(),
	},
	{
		Name:              TopicSaga,
		Partitions:        3,
		ReplicationFactor: 1,
		Configs:           standardConfig(),
	},
	{
		Name:              TopicDLQ,
		Partitions:        3,
		ReplicationFactor: 1,
		Configs: map[string]string{
			"cleanup.policy": "delete",
			// Deliberately longer than the business topics: a DLQ message is
			// evidence for an incident that may not be investigated same-day,
			// and losing it to a short retention window before anyone has
			// looked is worse than the extra disk.
			"retention.ms":        "2592000000", // 30 days
			"min.insync.replicas": "1",
		},
	},
}

// standardConfig is the retention and cleanup policy shared by the three
// business-event topics. A week is long enough to cover a consumer being down
// over a weekend deploy freeze without needing to fall back to the rebuild
// path, and short enough that the topic is not standing in for the journal as
// a long-term store -- it is not one; journal_entries is, and always will be,
// the only source of truth these topics are ever rebuilt from (see
// cmd/projector's -rebuild mode).
func standardConfig() map[string]string {
	return map[string]string{
		"cleanup.policy":      "delete",
		"retention.ms":        "604800000", // 7 days
		"min.insync.replicas": "1",
	}
}

// Provision creates every topic in Topics, tolerating one already existing.
//
// Idempotent by design rather than by a "does it exist" check-then-create
// race: CreateTopic is called unconditionally and TopicAlreadyExists is the
// one error this function treats as success. That is what makes it safe to
// run on every deploy -- including a rolling one where two replicas of
// cmd/kafka-init might genuinely race to create the same topic -- without a
// separate existence check that could itself race.
//
// One topic per call rather than kadm.Client.CreateTopics for all of them at
// once: that method applies one partition count and one config map to every
// topic in the call, and Topics deliberately gives each topic its own.
func Provision(ctx context.Context, admin *kadm.Client, logger *slog.Logger) error {
	for _, spec := range Topics {
		configs := make(map[string]*string, len(spec.Configs))
		for k, v := range spec.Configs {
			configs[k] = kadm.StringPtr(v)
		}

		_, err := admin.CreateTopic(ctx, spec.Partitions, spec.ReplicationFactor, configs, spec.Name)
		switch {
		case err == nil:
			logger.Info("created topic",
				slog.String("topic", spec.Name),
				slog.Int("partitions", int(spec.Partitions)))
		case errors.Is(err, kerr.TopicAlreadyExists):
			logger.Info("topic already exists", slog.String("topic", spec.Name))
		default:
			return fmt.Errorf("create topic %s: %w", spec.Name, err)
		}
	}
	return nil
}
