-- Add index on queue table for faster status-based queries
-- This supports GROUP BY queries on status (like queue stats monitoring)
--
-- The existing partial indexes (idx_queue_pending_priority, idx_queue_processing)
-- only cover specific status values. This general index helps queries that need
-- to filter or group by arbitrary status values.
--
-- IMPORTANT: This migration MUST be run standalone, not as part of schema init.
-- CREATE INDEX CONCURRENTLY cannot run inside a transaction block.
--
-- Run with:
--   psql -U cortex -d cortex -f migrations/001_add_queue_status_index.sql

-- Ensure we stop on errors
\set ON_ERROR_STOP on

-- Index for queries that GROUP BY or filter on status
-- This must run outside a transaction
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_queue_status
    ON queue (status, queue_name);

-- Verify index was created (standard SQL, not psql-specific)
SELECT indexname, tablename
FROM pg_indexes
WHERE indexname = 'idx_queue_status';
