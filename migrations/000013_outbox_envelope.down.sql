DROP INDEX IF EXISTS outbox_event_id_key;

ALTER TABLE outbox DROP COLUMN trace_id;

ALTER TABLE outbox DROP CONSTRAINT outbox_event_version_check;
ALTER TABLE outbox DROP COLUMN event_version;

ALTER TABLE outbox DROP COLUMN event_id;
