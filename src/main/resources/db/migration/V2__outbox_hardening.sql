-- EventLib Outbox hardening (PostgreSQL).
-- event_id es un String libre en el modelo Java; UUID en SQL provoca errores de tipo al insertar.
ALTER TABLE outbox_events
    ALTER COLUMN event_id TYPE VARCHAR(256) USING event_id::text;

-- Recuperación de filas abandonadas (lease vencido tras una caída).
CREATE INDEX IF NOT EXISTS idx_outbox_processing_lease
    ON outbox_events (locked_until)
    WHERE status = 'PROCESSING';

CREATE INDEX IF NOT EXISTS idx_outbox_event_id
    ON outbox_events (event_id);
