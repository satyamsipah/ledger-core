// Package kafka owns the topic layout: names, partition counts, and the
// per-topic configuration this service depends on, all in one place so the
// Debezium connector's routing (deploy/debezium/outbox-connector.json), the
// polling publisher, and every consumer read the same names from the same
// source rather than three copies that can drift.
//
// Topics are provisioned explicitly (Provision, run once at startup by
// cmd/kafka-init) rather than left to broker auto-create. Auto-create exists
// as a safety net -- redpanda.auto_create_topics_enabled stays on in the local
// compose stack -- but a topic that came into existence because something
// happened to produce to it first gets the broker's blanket defaults, not the
// per-topic partition count and retention this package specifies. Relying on
// that would make the topic layout a race between whichever process starts
// first, rather than a decision recorded in code.
package kafka
