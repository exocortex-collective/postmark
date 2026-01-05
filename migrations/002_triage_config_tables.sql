-- Migration: Triage Config Tables for Dynamic Config Management
-- Ticket: cortex-c2f
-- Description: Database schema for storing triage config with version control,
--              linked list rules, email mappings, and worker management

-- Enable required extensions
CREATE EXTENSION IF NOT EXISTS btree_gist;

-- ============================================================================
-- 1. Config Versions (Full Snapshot Versioning)
-- ============================================================================

CREATE TABLE IF NOT EXISTS triage_config_versions (
    id BIGSERIAL PRIMARY KEY,
    version SERIAL UNIQUE,            -- Auto-assigned via sequence (race-safe)
    config_hash TEXT NOT NULL,        -- SHA256 of YAML content (dedup)
    label_prefix TEXT NOT NULL DEFAULT 'Cortex',

    -- Complex configs stored as JSONB (matches Pydantic models)
    intents JSONB NOT NULL DEFAULT '{}'::jsonb,
    email_categories JSONB NOT NULL DEFAULT '{}'::jsonb,
    prompts JSONB NOT NULL DEFAULT '{}'::jsonb,
    body_extraction_prompts JSONB NOT NULL DEFAULT '{}'::jsonb,

    -- Metadata
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    created_by TEXT NOT NULL,
    notes TEXT,
    is_active BOOLEAN NOT NULL DEFAULT FALSE,
    rolled_back_from INTEGER REFERENCES triage_config_versions(version),

    -- Only one active config at a time (requires btree_gist extension)
    CONSTRAINT only_one_active EXCLUDE USING gist (is_active WITH =) WHERE (is_active = TRUE)
);

CREATE INDEX IF NOT EXISTS idx_config_versions_active ON triage_config_versions(version DESC)
    WHERE is_active = TRUE;

CREATE INDEX IF NOT EXISTS idx_config_versions_hash ON triage_config_versions(config_hash);

-- Trigger: Deactivate old configs when new one activates
CREATE OR REPLACE FUNCTION deactivate_old_configs()
RETURNS TRIGGER AS $$
BEGIN
    IF NEW.is_active = TRUE THEN
        UPDATE triage_config_versions
        SET is_active = FALSE
        WHERE id != NEW.id AND is_active = TRUE;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER deactivate_old_configs_trigger
    BEFORE INSERT OR UPDATE ON triage_config_versions
    FOR EACH ROW
    WHEN (NEW.is_active = TRUE)
    EXECUTE FUNCTION deactivate_old_configs();

-- ============================================================================
-- 2. Email Mappings (Simple Hash Lookups)
-- ============================================================================

CREATE TABLE IF NOT EXISTS triage_email_mappings (
    id BIGSERIAL PRIMARY KEY,
    config_version INTEGER NOT NULL REFERENCES triage_config_versions(version) ON DELETE CASCADE,

    mapping_type TEXT NOT NULL CHECK (mapping_type IN ('priority', 'fallback')),
    email_address TEXT NOT NULL,  -- Normalized lowercase

    -- Action
    label TEXT NOT NULL,
    archive BOOLEAN,       -- Three-state: true/false/null
    mark_read BOOLEAN,     -- Three-state: true/false/null

    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    CONSTRAINT unique_email_mapping UNIQUE (config_version, mapping_type, email_address)
);

CREATE INDEX IF NOT EXISTS idx_email_mappings_config ON triage_email_mappings(config_version);
CREATE INDEX IF NOT EXISTS idx_email_mappings_type ON triage_email_mappings(config_version, mapping_type);
CREATE INDEX IF NOT EXISTS idx_email_mappings_lookup ON triage_email_mappings(config_version, mapping_type, email_address);

-- ============================================================================
-- 3. Chains
-- ============================================================================

CREATE TABLE IF NOT EXISTS triage_chains (
    id BIGSERIAL PRIMARY KEY,
    config_version INTEGER NOT NULL REFERENCES triage_config_versions(version) ON DELETE CASCADE,
    chain_name TEXT NOT NULL,
    display_order INTEGER NOT NULL DEFAULT 0,

    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    CONSTRAINT unique_chain UNIQUE (config_version, chain_name)
);

CREATE INDEX IF NOT EXISTS idx_chains_config ON triage_chains(config_version);
CREATE INDEX IF NOT EXISTS idx_chains_name ON triage_chains(config_version, chain_name);

-- ============================================================================
-- 4. Rules (Linked List Structure)
-- ============================================================================

CREATE TABLE IF NOT EXISTS triage_rules (
    id BIGSERIAL PRIMARY KEY,
    chain_id BIGINT NOT NULL REFERENCES triage_chains(id) ON DELETE CASCADE,
    config_version INTEGER NOT NULL REFERENCES triage_config_versions(version) ON DELETE CASCADE,

    -- Linked list pointers
    prev_rule_id BIGINT REFERENCES triage_rules(id) ON DELETE SET NULL,
    next_rule_id BIGINT REFERENCES triage_rules(id) ON DELETE SET NULL,

    -- Rule content as JSONB (matches Pydantic models)
    match_condition JSONB NOT NULL DEFAULT '{}'::jsonb,
    variables JSONB,
    action JSONB,
    jump_to_chain TEXT,
    return_to_parent BOOLEAN DEFAULT FALSE,
    llm_config JSONB,
    routes JSONB,

    -- Metadata
    rule_name TEXT,
    description TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    row_version INTEGER NOT NULL DEFAULT 1,  -- Optimistic locking

    CONSTRAINT valid_pointers CHECK (
        (prev_rule_id IS NULL OR prev_rule_id != id) AND
        (next_rule_id IS NULL OR next_rule_id != id)
    ),
    CONSTRAINT exactly_one_outcome CHECK (
        (action IS NOT NULL)::int +
        (jump_to_chain IS NOT NULL)::int +
        (return_to_parent = TRUE)::int +
        (llm_config IS NOT NULL)::int = 1
    )
);

CREATE INDEX IF NOT EXISTS idx_rules_chain ON triage_rules(chain_id);
CREATE INDEX IF NOT EXISTS idx_rules_chain_head ON triage_rules(chain_id) WHERE prev_rule_id IS NULL;
CREATE INDEX IF NOT EXISTS idx_rules_config ON triage_rules(config_version);

-- Trigger: Increment row_version on UPDATE (optimistic locking)
CREATE OR REPLACE FUNCTION increment_rule_version()
RETURNS TRIGGER AS $$
BEGIN
    NEW.row_version = OLD.row_version + 1;
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER increment_rule_version_trigger
    BEFORE UPDATE ON triage_rules
    FOR EACH ROW
    EXECUTE FUNCTION increment_rule_version();

-- ============================================================================
-- 5. Worker Signals (Control Message Queue)
-- ============================================================================

CREATE TABLE IF NOT EXISTS worker_signals (
    id BIGSERIAL PRIMARY KEY,

    signal_type TEXT NOT NULL
        CONSTRAINT worker_signals_signal_type_check
        CHECK (signal_type IN ('config_reload', 'shutdown', 'pause', 'resume', 'log_level')),
    target_worker TEXT NOT NULL,  -- 'triage', 'labeling', 'all'
    payload JSONB,

    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    expires_at TIMESTAMPTZ,
    processed_at TIMESTAMPTZ,
    processed_by TEXT,

    status TEXT NOT NULL DEFAULT 'pending' CHECK (status IN ('pending', 'processed', 'expired'))
);

CREATE INDEX IF NOT EXISTS idx_signals_pending ON worker_signals(created_at)
    WHERE status = 'pending';

CREATE INDEX IF NOT EXISTS idx_signals_worker ON worker_signals(target_worker, status);

-- ============================================================================
-- 6. Worker Status (Heartbeat & Version Tracking)
-- ============================================================================

CREATE TABLE IF NOT EXISTS worker_status (
    id BIGSERIAL PRIMARY KEY,

    worker_id TEXT NOT NULL UNIQUE,
    worker_type TEXT NOT NULL CHECK (worker_type IN ('triage', 'labeling', 'parse', 'attachment')),

    current_config_version INTEGER REFERENCES triage_config_versions(version),
    last_reload_at TIMESTAMPTZ,

    status TEXT NOT NULL DEFAULT 'running',
    last_heartbeat TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    jobs_processed BIGINT NOT NULL DEFAULT 0,
    hostname TEXT,
    pid INTEGER
);

CREATE INDEX IF NOT EXISTS idx_worker_status_type ON worker_status(worker_type, status);
CREATE INDEX IF NOT EXISTS idx_worker_status_heartbeat ON worker_status(last_heartbeat DESC);
CREATE INDEX IF NOT EXISTS idx_worker_status_config ON worker_status(current_config_version);

-- ============================================================================
-- 7. Update Classifications Table (Add config_version_id)
-- ============================================================================

-- Note: classifications table created by triage service
-- This migration adds the config_version_id column if it doesn't exist

DO $$
BEGIN
    -- Check if table exists first
    IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'classifications') THEN
        IF NOT EXISTS (
            SELECT 1 FROM information_schema.columns
            WHERE table_name = 'classifications' AND column_name = 'config_version_id'
        ) THEN
            ALTER TABLE classifications
            ADD COLUMN config_version_id INTEGER REFERENCES triage_config_versions(version);

            CREATE INDEX idx_classifications_config_version
            ON classifications(config_version_id, classified_at DESC);
        END IF;
    END IF;
END $$;

-- ============================================================================
-- 8. Convergence View (Emails Needing Re-Processing)
-- ============================================================================

CREATE OR REPLACE VIEW emails_needing_convergence AS
SELECT
    c.gmail_id,
    c.config_version_id AS old_version,
    v.version AS current_version,
    c.classified_at,
    -- Calculate priority: older emails = lower priority
    EXTRACT(EPOCH FROM (NOW() - c.classified_at))::integer AS age_seconds
FROM classifications c
CROSS JOIN (
    SELECT version FROM triage_config_versions WHERE is_active = TRUE LIMIT 1
) v
WHERE c.config_version_id < v.version
ORDER BY c.classified_at DESC;

-- ============================================================================
-- Comments
-- ============================================================================

COMMENT ON TABLE triage_config_versions IS 'Full snapshot versioning for triage config. Each version is complete and independent.';
COMMENT ON TABLE triage_email_mappings IS 'Simple email→label mappings for O(1) hash lookup (priority before rules, fallback after).';
COMMENT ON TABLE triage_chains IS 'Named chains of rules. Main chain is the entry point.';
COMMENT ON TABLE triage_rules IS 'Rules stored as linked list (prev_rule_id/next_rule_id) for O(1) insert/delete.';
COMMENT ON TABLE worker_signals IS 'Control message queue for worker management (config reload, shutdown, etc).';
COMMENT ON TABLE worker_status IS 'Worker heartbeat and config version tracking.';

COMMENT ON COLUMN triage_config_versions.config_hash IS 'SHA256 of YAML content for deduplication';
COMMENT ON COLUMN triage_config_versions.is_active IS 'Only one config can be active. Trigger ensures exclusivity.';
COMMENT ON COLUMN triage_rules.row_version IS 'Optimistic locking counter. Incremented on every UPDATE.';
COMMENT ON COLUMN triage_rules.prev_rule_id IS 'Previous rule in chain (NULL = head of list)';
COMMENT ON COLUMN triage_rules.next_rule_id IS 'Next rule in chain (NULL = tail of list)';

COMMENT ON VIEW emails_needing_convergence IS 'Emails classified with old config versions that need re-processing during idle time.';
