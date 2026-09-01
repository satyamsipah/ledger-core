DROP INDEX IF EXISTS saga_steps_saga_id_started_at_idx;
DROP TABLE IF EXISTS saga_steps;

DROP INDEX IF EXISTS saga_instances_manual_review_idx;
DROP INDEX IF EXISTS saga_instances_deadline_idx;
DROP INDEX IF EXISTS saga_instances_idempotency_key_key;
DROP TRIGGER IF EXISTS saga_instances_set_updated_at ON saga_instances;
DROP TABLE IF EXISTS saga_instances;
