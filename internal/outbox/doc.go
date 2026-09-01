// Package outbox implements the transactional outbox that carries ledger events
// to Kafka.
//
// The rule it exists to enforce (invariant 6): the database and Kafka are never
// written in the same logical step. A write path appends an event row inside
// the same database transaction that writes the journal, so the event and the
// state it describes commit or roll back together. Publishing then happens
// separately, by Debezium reading the write-ahead log.
//
// This is a two-phase delivery with at-least-once semantics, not exactly-once:
// a consumer can see the same event twice after a publisher restart, and every
// consumer must therefore be idempotent on the event id. See
// docs/DECISIONS.md D30 for why that gap cannot be closed rather than merely
// has not been yet.
//
// Append assembles the full wire envelope -- event_id, event_type,
// event_version, aggregate_id, occurred_at, trace_id, payload -- and stores it
// as the entirety of the outbox row's payload column. That is what lets two
// different publishers (internal/outbox/publish) produce indistinguishable
// Kafka messages: neither adds anything the other does not, because there is
// nothing left for either to add.
//
// Phase 1 defined the table and its CDC publication. Phase 2 added the writer.
// Phase 4 gave it the envelope and the publishers that carry it to Kafka.
package outbox
