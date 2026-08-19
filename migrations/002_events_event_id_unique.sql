-- The ingest path relies on INSERT ... ON CONFLICT (event_id) DO NOTHING to
-- make redelivery-safe inserts atomic. That requires a real uniqueness
-- guarantee, not just an index for lookups.
ALTER TABLE events ADD CONSTRAINT events_event_id_key UNIQUE (event_id);

-- Superseded by the unique constraint above (which Postgres backs with its
-- own unique index).
DROP INDEX IF EXISTS idx_events_event_id;
