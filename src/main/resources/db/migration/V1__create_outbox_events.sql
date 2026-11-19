-- EventLib Transactional Outbox schema (PostgreSQL).
-- Consumers may copy this migration or configure Flyway/Liquibase to run it.

CREATE TABLE IF NOT EXISTS outbox_events
(
    id              UUID PRIMARY KEY,
    event_id        UUID,
    event_name      VARCHAR(255) NOT NULL,
    topic           VARCHAR(255) NOT NULL,
    exchange        VARCHAR(255),
    routing_key     VARCHAR(255),
    payload         TEXT NOT NULL,
    created_at      TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    published_at    TIMESTAMP WITH TIME ZONE,
    class_name      VARCHAR(255),
    status          VARCHAR(20) DEFAULT 'PENDING',
    attempts        INTEGER DEFAULT 0,
    next_attempt_at TIMESTAMP WITH TIME ZONE,
    locked_by       VARCHAR(255),
    locked_until    TIMESTAMP WITH TIME ZONE,
    last_error      TEXT
);

CREATE INDEX IF NOT EXISTS idx_outbox_pending_events
    ON outbox_events (created_at ASC NULLS LAST)
    WHERE published_at IS NULL;

CREATE INDEX IF NOT EXISTS idx_outbox_claim
    ON outbox_events (status, next_attempt_at, created_at)
    WHERE published_at IS NULL;
