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
// a consumer can see the same event twice after a connector restart, and every
// consumer must therefore be idempotent on the event id.
//
// Phase 1 defines the table and its CDC publication. The writer arrives in
// Phase 2, alongside the first event that needs it.
package outbox
