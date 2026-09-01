DROP INDEX IF EXISTS processed_events_processed_at_brin_idx;
DROP TABLE IF EXISTS processed_events;

DROP TRIGGER IF EXISTS balance_projections_set_updated_at ON balance_projections;
DROP TABLE IF EXISTS balance_projections;
