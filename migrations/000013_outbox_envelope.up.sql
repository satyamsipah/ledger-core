-- The full event envelope -- event_id, event_version, trace_id -- promoted to
-- first-class columns, and folded into outbox.payload as well. See
-- docs/DECISIONS.md D31: the wire format cannot depend on which publisher
-- wrote it, so the envelope is assembled once, in Go, and stored as the
-- ENTIRETY of the payload column. What is added here is what makes that
-- possible plus what makes the envelope operable without parsing JSONB:
--
--   * event_id is what a consumer dedupes on (processed_events.event_id).
--     Promoted to a column, not left inside payload alone, because the
--     Debezium EventRouter SMT's own exactly-once producer tracking wants a
--     literal source column (transforms.outbox.table.field.event.id), and
--     because "which events came from trace X" should not require parsing
--     JSON to answer.
--   * event_version is the payload's schema version. It replaces the ".v1"
--     suffix baked into event_type strings in Phase 3 -- see the constant
--     rename in internal/ledger/service.go -- so a consumer branches on a
--     comparable integer instead of parsing a string suffix.
--   * trace_id is nullable and stays nullable. It is genuinely absent for any
--     write outside a traced request (a migration backfill, an admin script,
--     the seed data), and treating that absence as a defect to paper over
--     would be worse than leaving the column NULL and saying so.
--
-- Added nullable, backfilled, then constrained, per
-- .claude/rules/sql-migrations.md -- even though this table's Phase 1-3
-- writers make it likely to be near-empty on any database this runs against,
-- consistency with every other migration that has touched a populated-capable
-- table (000010, for one) matters more than the few saved lines here.

ALTER TABLE outbox ADD COLUMN event_id UUID;
UPDATE outbox SET event_id = gen_random_uuid() WHERE event_id IS NULL;
ALTER TABLE outbox ALTER COLUMN event_id SET NOT NULL;

ALTER TABLE outbox ADD COLUMN event_version SMALLINT;
UPDATE outbox SET event_version = 1 WHERE event_version IS NULL;
ALTER TABLE outbox ALTER COLUMN event_version SET NOT NULL;
ALTER TABLE outbox ADD CONSTRAINT outbox_event_version_check CHECK (event_version >= 1);

ALTER TABLE outbox ADD COLUMN trace_id TEXT;

-- event_id is what goes out over the wire as the thing a consumer dedupes on,
-- so two outbox rows sharing one is a bug in the writer, not a legitimate
-- state -- caught here rather than downstream in whichever consumer notices
-- first.
CREATE UNIQUE INDEX outbox_event_id_key ON outbox (event_id);
