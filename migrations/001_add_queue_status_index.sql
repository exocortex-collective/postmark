-- Add index on queue table for faster status-based queries
-- This supports GROUP BY queries on status (like queue stats monitoring)
--
-- The existing partial indexes (idx_queue_pending_priority, idx_queue_processing)
-- only cover specific status values. This general index helps queries that need
-- to filter or group by arbitrary status values.
--
-- Run with:
--   psql -U cortex -d cortex -f migrations/001_add_queue_status_index.sql

BEGIN;

-- Index for queries that GROUP BY or filter on status
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_queue_status
    ON queue (status, queue_name);

-- Verify index was created
\di idx_queue_status

COMMIT;
