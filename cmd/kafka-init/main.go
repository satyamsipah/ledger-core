// Command kafka-init provisions the Kafka topic layout and exits.
//
// A one-shot binary, run once per deploy before anything tries to produce or
// consume, on the same principle as `migrate` for the schema: the topic
// layout is a deployment artifact with its own explicit configuration
// (internal/kafka), not something left to whatever process happens to touch
// a topic first and gets it created with broker defaults.
package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/satyamsipah/ledger-core/internal/config"
	"github.com/satyamsipah/ledger-core/internal/kafka"
	"github.com/satyamsipah/ledger-core/internal/observability"
)

const serviceName = "kafka-init"

// provisionTimeout bounds the whole run. Creating four topics is a handful of
// broker round trips; anything past this means the broker is unreachable, not
// merely slow, and a one-shot init container should fail fast and let its
// restart policy retry rather than hang.
const provisionTimeout = 30 * time.Second

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "fatal: %v\n", err)
		os.Exit(1)
	}
}

func run() error {
	cfg, err := config.Load(serviceName)
	if err != nil {
		return err
	}

	logger := observability.NewLogger(cfg.Observability, serviceName, cfg.Env)

	ctx, cancel := context.WithTimeout(context.Background(), provisionTimeout)
	defer cancel()

	client, err := kgo.NewClient(kgo.SeedBrokers(cfg.Kafka.Brokers...))
	if err != nil {
		return fmt.Errorf("create kafka client: %w", err)
	}
	defer client.Close()

	admin := kadm.NewClient(client)
	defer admin.Close()

	if err := kafka.Provision(ctx, admin, logger); err != nil {
		return fmt.Errorf("provision topics: %w", err)
	}

	logger.Info("topic layout provisioned", slog.Int("topics", len(kafka.Topics)))
	return nil
}
