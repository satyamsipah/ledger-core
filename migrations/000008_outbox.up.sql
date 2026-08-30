-- Invariant 6: the database and Kafka are never written in the same logical
-- step. A write path appends to this table inside the same transaction that
-- writes the journal, and Debezium turns committed rows into Kafka records by
-- reading the write-ahead log.
CREATE TABLE outbox (
    id             BIGSERIAL   PRIMARY KEY,
    aggregate_type TEXT        NOT NULL,
    aggregate_id   TEXT        NOT NULL,
    event_type     TEXT        NOT NULL,
    payload        JSONB       NOT NULL,
    created_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    published_at   TIMESTAMPTZ
);

-- The publisher's only query: oldest unpublished rows, in order. Partial, so
-- the index holds just the backlog (normally near zero) rather than the whole
-- table, which also makes "how far behind is the outbox?" a cheap monitoring
-- query.
--
-- Under Debezium nothing writes published_at on the happy path, so today this
-- index serves monitoring and any future polling fallback rather than the hot
-- path. That is a deliberate choice, recorded in docs/DECISIONS.md.
CREATE INDEX outbox_unpublished_idx ON outbox (id) WHERE published_at IS NULL;

-- Retention purge scans by age. BRIN, not btree: the table is append-only and
-- created_at is perfectly correlated with physical order, which is precisely
-- the shape BRIN exists for. Kilobytes where a btree would cost gigabytes.
CREATE INDEX outbox_created_at_brin_idx ON outbox USING BRIN (created_at);

-- The primary key is sufficient identity for CDC because outbox rows are
-- inserted and never updated in place by the write path.
ALTER TABLE outbox REPLICA IDENTITY DEFAULT;

-- The publication lives here rather than in a bootstrap script because this is
-- the only place the table is guaranteed to exist, and because the set of
-- replicated tables is part of the schema contract and should be versioned with
-- it. Scoped to outbox alone: replicating the journal would double the WAL
-- traffic for data no consumer reads.
--
-- Requires CREATE on the database. In production, grant that to the migration
-- role explicitly rather than running migrations as a superuser.
CREATE PUBLICATION ledger_outbox_pub FOR TABLE outbox;
