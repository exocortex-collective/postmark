-- Migration: Decouple email mappings from config versioning
-- Description: Remove config_version FK, add history tracking, enable targeted re-enqueue
-- Related: cortex-6xc (Dynamic Config Management)

BEGIN;

-- 1. Create new email mappings table without config_version FK
CREATE TABLE IF NOT EXISTS triage_email_mappings_v2 (
    id BIGSERIAL PRIMARY KEY,

    -- No config_version - mappings are global, not versioned
    mapping_type TEXT NOT NULL CHECK (mapping_type IN ('priority', 'fallback')),
    email_address TEXT NOT NULL,  -- Normalized lowercase

    -- Action configuration
    label TEXT NOT NULL,
    archive BOOLEAN,      -- Three-state: true/false/null
    mark_read BOOLEAN,    -- Three-state: true/false/null

    -- Audit trail
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    created_by TEXT,      -- Who created this mapping
    updated_by TEXT,      -- Who last updated

    -- Soft delete support
    deleted_at TIMESTAMPTZ
);

-- Create partial unique index to enforce uniqueness only for non-deleted rows
-- A partial unique index is used because the table-level UNIQUE constraint syntax does not support a WHERE clause.
DROP INDEX IF EXISTS idx_email_mappings_unique_active;
CREATE UNIQUE INDEX idx_email_mappings_unique_active
    ON triage_email_mappings_v2(mapping_type, email_address)
    WHERE deleted_at IS NULL;

-- Indexes for efficient queries
CREATE INDEX idx_email_mappings_type_v2 ON triage_email_mappings_v2(mapping_type)
    WHERE deleted_at IS NULL;

CREATE INDEX idx_email_mappings_email_v2 ON triage_email_mappings_v2(email_address)
    WHERE deleted_at IS NULL;


-- 2. Create history table for audit trail
CREATE TABLE IF NOT EXISTS triage_email_mappings_history (
    id BIGSERIAL PRIMARY KEY,
    mapping_id BIGINT,  -- References the mapping that was changed

    -- Snapshot of mapping at this point
    mapping_type TEXT NOT NULL,
    email_address TEXT NOT NULL,
    label TEXT NOT NULL,
    archive BOOLEAN,
    mark_read BOOLEAN,

    -- Change metadata
    change_type TEXT NOT NULL CHECK (change_type IN ('created', 'updated', 'deleted')),
    changed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    changed_by TEXT NOT NULL,

    -- Previous values (for update/delete operations)
    previous_label TEXT,
    previous_archive BOOLEAN,
    previous_mark_read BOOLEAN
);

-- Indexes for history queries
CREATE INDEX idx_mapping_history_mapping ON triage_email_mappings_history(mapping_id);
CREATE INDEX idx_mapping_history_email ON triage_email_mappings_history(email_address);
CREATE INDEX idx_mapping_history_changed ON triage_email_mappings_history(changed_at DESC);


-- 3. Create trigger function to populate history
CREATE OR REPLACE FUNCTION log_email_mapping_change()
RETURNS TRIGGER AS $$
BEGIN
    IF TG_OP = 'INSERT' THEN
        INSERT INTO triage_email_mappings_history (
            mapping_id, mapping_type, email_address, label, archive, mark_read,
            change_type, changed_by
        ) VALUES (
            NEW.id, NEW.mapping_type, NEW.email_address, NEW.label, NEW.archive, NEW.mark_read,
            'created', COALESCE(NEW.created_by, 'unknown')
        );
        RETURN NEW;

    ELSIF TG_OP = 'UPDATE' THEN
        -- Only log if something actually changed (not just timestamp updates)
        IF (OLD.label IS DISTINCT FROM NEW.label) OR
           (OLD.archive IS DISTINCT FROM NEW.archive) OR
           (OLD.mark_read IS DISTINCT FROM NEW.mark_read) OR
           (OLD.deleted_at IS DISTINCT FROM NEW.deleted_at) THEN

            INSERT INTO triage_email_mappings_history (
                mapping_id, mapping_type, email_address, label, archive, mark_read,
                change_type, changed_by,
                previous_label, previous_archive, previous_mark_read
            ) VALUES (
                NEW.id, NEW.mapping_type, NEW.email_address, NEW.label, NEW.archive, NEW.mark_read,
                CASE WHEN OLD.deleted_at IS NULL AND NEW.deleted_at IS NOT NULL THEN 'deleted' ELSE 'updated' END,
                COALESCE(NEW.updated_by, NEW.created_by, 'unknown'),
                OLD.label, OLD.archive, OLD.mark_read
            );
        END IF;
        RETURN NEW;

    ELSIF TG_OP = 'DELETE' THEN
        INSERT INTO triage_email_mappings_history (
            mapping_id, mapping_type, email_address, label, archive, mark_read,
            change_type, changed_by
        ) VALUES (
            OLD.id, OLD.mapping_type, OLD.email_address, OLD.label, OLD.archive, OLD.mark_read,
            'deleted', 'unknown (hard delete)'
        );
        RETURN OLD;
    END IF;

    RETURN NULL;
END;
$$ LANGUAGE plpgsql;


-- 4. Migrate data from active config version to new table
DO $$
DECLARE
    active_version INTEGER;
    migrated_count INTEGER;
BEGIN
    -- Get active config version
    SELECT version INTO active_version
    FROM triage_config_versions
    WHERE is_active = TRUE
    LIMIT 1;

    IF active_version IS NOT NULL THEN
        -- Migrate mappings from active version
        INSERT INTO triage_email_mappings_v2 (
            mapping_type, email_address, label, archive, mark_read,
            created_at, updated_at, created_by
        )
        SELECT
            mapping_type,
            email_address,
            label,
            archive,
            mark_read,
            created_at,
            updated_at,
            'migration' as created_by
        FROM triage_email_mappings
        WHERE config_version = active_version;

        GET DIAGNOSTICS migrated_count = ROW_COUNT;

        RAISE NOTICE 'Migrated % mappings from config version %',
            migrated_count,
            active_version;
    ELSE
        RAISE NOTICE 'No active config version found - skipping data migration';
    END IF;
END $$;


-- 5. Rename tables (atomic swap)
ALTER TABLE IF EXISTS triage_email_mappings RENAME TO triage_email_mappings_old;
ALTER TABLE triage_email_mappings_v2 RENAME TO triage_email_mappings;

-- Drop old indexes and constraints from old table (if they exist)
DROP INDEX IF EXISTS idx_email_mappings_type;
ALTER TABLE IF EXISTS triage_email_mappings_old DROP CONSTRAINT IF EXISTS unique_email_mapping;

-- Rename constraints and indexes to match new table name
ALTER INDEX idx_email_mappings_unique_active RENAME TO idx_email_mappings_active;
ALTER INDEX idx_email_mappings_type_v2 RENAME TO idx_email_mappings_type;
ALTER INDEX idx_email_mappings_email_v2 RENAME TO idx_email_mappings_email;


-- 6. Create trigger on new table
DROP TRIGGER IF EXISTS email_mapping_change_log ON triage_email_mappings;
CREATE TRIGGER email_mapping_change_log
    AFTER INSERT OR UPDATE OR DELETE ON triage_email_mappings
    FOR EACH ROW EXECUTE FUNCTION log_email_mapping_change();


-- 7. Update worker_signals to support 'mappings_reload' signal type
-- (Extend existing CHECK constraint if it exists)
DO $$
BEGIN
    -- Try to drop and recreate the constraint with new signal type
    ALTER TABLE worker_signals DROP CONSTRAINT IF EXISTS worker_signals_signal_type_check;
    ALTER TABLE worker_signals ADD CONSTRAINT worker_signals_signal_type_check
        CHECK (signal_type IN ('config_reload', 'mappings_reload', 'shutdown', 'pause', 'resume', 'log_level'));

    RAISE NOTICE 'Updated worker_signals to support mappings_reload signal type';
EXCEPTION
    WHEN undefined_table THEN
        RAISE NOTICE 'worker_signals table does not exist yet - skipping signal type update';
    WHEN undefined_column THEN
        RAISE NOTICE 'worker_signals.signal_type column does not exist yet - skipping signal type update';
END $$;


-- 8. Verification query
DO $$
DECLARE
    new_count INTEGER;
    old_count INTEGER;
    active_version INTEGER;
BEGIN
    SELECT COUNT(*) INTO new_count FROM triage_email_mappings WHERE deleted_at IS NULL;

    SELECT version INTO active_version FROM triage_config_versions WHERE is_active = TRUE LIMIT 1;

    IF active_version IS NOT NULL THEN
        SELECT COUNT(*) INTO old_count
        FROM triage_email_mappings_old
        WHERE config_version = active_version;

        IF new_count = old_count THEN
            RAISE NOTICE '✓ Migration successful: % mappings in new table (matches old table)', new_count;
        ELSE
            RAISE WARNING '✗ Count mismatch: new=%s, old=%s', new_count, old_count;
        END IF;
    ELSE
        RAISE NOTICE 'New table has % mappings (no active version to compare against)', new_count;
    END IF;
END $$;

COMMIT;

-- Manual cleanup step (run after validation):
-- DROP TABLE IF EXISTS triage_email_mappings_old;
