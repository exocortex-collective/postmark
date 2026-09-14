-- Migration: make the email-mapping upsert arbiter match the schema
-- Related: cortex-apd6 (schema drift), cortex-rj2b (the corruption it masked)
--
-- !! ORDERING: DEPLOY THE CODE FIRST. !!
--
--   This migration DROPS the constraint that older cortex-utils names in
--   `ON CONFLICT ON CONSTRAINT unique_email_mapping`. Run it against a
--   gateway still running that code and every `PUT /config` fails with
--   "constraint unique_email_mapping ... does not exist" -- config imports
--   are dead until the deploy lands.
--
--   Learned the hard way on 2026-09-14: applied ahead of the deploy, broke
--   imports, restored the constraint by hand to unbreak production.
--
--   Correct order:
--     1. merge + deploy gateway with cortex-utils >= 01bf644
--        (upsert infers the partial index instead of naming the constraint)
--     2. confirm: PUT /config succeeds
--     3. THEN run this migration
--     4. confirm: PUT /config still succeeds
--
--   Safe to run twice; the DROP is IF EXISTS.
--
-- PROBLEM
--   import_yaml_to_db() upserted with `ON CONFLICT ON CONSTRAINT
--   unique_email_mapping`. That constraint is created by migration 002 on the
--   OLD config-versioned table, over THREE columns
--   (config_version, mapping_type, email_address), and migration 003 explicitly
--   drops it while building the replacement table:
--
--       ALTER TABLE IF EXISTS triage_email_mappings_old
--           DROP CONSTRAINT IF EXISTS unique_email_mapping;
--
--   Migration 003 gives the new table only a PARTIAL unique index,
--   idx_email_mappings_active (mapping_type, email_address)
--   WHERE deleted_at IS NULL.
--
--   So no migration in this repo creates unique_email_mapping on the current
--   table. Every config import worked in production only because someone had
--   hand-added an unconditional UNIQUE (mapping_type, email_address) named
--   unique_email_mapping directly on Hades. On a database built from these
--   migrations, `PUT /config` fails outright with
--   "constraint unique_email_mapping for table ... does not exist".
--
--   The hand-added constraint is also semantically WRONG for this table.
--   Being unconditional, it counts soft-deleted rows, so a mapping that was
--   soft-deleted via /mappings can never be recreated. Uniqueness here is
--   meant to hold over LIVE rows only, which is exactly what the partial
--   index expresses.
--
-- FIX
--   Drop the stray constraint and let the partial index be the single arbiter.
--   importer.py now infers it by column list + predicate rather than by name.
--
-- SAFETY
--   Dropping a constraint cannot fail on data. The partial index continues to
--   enforce uniqueness over live rows. Verified before writing this migration:
--   triage_email_mappings holds 131 rows with 0 duplicate (mapping_type,
--   email_address) keys among live rows.
--
--   NOTE: this table's btree indexes were silently corrupted by a glibc
--   collation change (cortex-rj2b), which let 17 duplicate keys accumulate
--   past both unique indexes. That was repaired 2026-09-14 by dedupe +
--   REINDEX. If you are restoring an older snapshot, re-run the dedupe in
--   processes/repair-collation-corruption.sql before applying this.

BEGIN;

-- 1. Drop the stray unconditional constraint if present (production only).
ALTER TABLE triage_email_mappings
    DROP CONSTRAINT IF EXISTS unique_email_mapping;

-- Same name may exist as a bare index rather than a constraint.
DROP INDEX IF EXISTS unique_email_mapping;

-- 2. Guarantee the real arbiter exists. Idempotent: 003 creates it, but a DB
--    that skipped straight here (or had it dropped) still ends up correct.
CREATE UNIQUE INDEX IF NOT EXISTS idx_email_mappings_active
    ON triage_email_mappings (mapping_type, email_address)
    WHERE deleted_at IS NULL;

-- 3. Verify the upsert arbiter is now inferable, and fail loudly if not.
DO $$
DECLARE arbiter_ok BOOLEAN;
BEGIN
    SELECT EXISTS (
        SELECT 1
        FROM pg_index i
        JOIN pg_class c ON c.oid = i.indexrelid
        WHERE i.indrelid = 'triage_email_mappings'::regclass
          AND c.relname = 'idx_email_mappings_active'
          AND i.indisunique
          AND i.indisvalid
          AND pg_get_expr(i.indpred, i.indrelid) = '(deleted_at IS NULL)'
    ) INTO arbiter_ok;

    IF NOT arbiter_ok THEN
        RAISE EXCEPTION
            'idx_email_mappings_active missing/invalid: import_yaml_to_db upsert will fail';
    END IF;

    IF EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conrelid = 'triage_email_mappings'::regclass
          AND conname = 'unique_email_mapping'
    ) THEN
        RAISE EXCEPTION 'unique_email_mapping still present after drop';
    END IF;

    RAISE NOTICE 'OK: partial unique index is the sole upsert arbiter';
END $$;

COMMIT;
