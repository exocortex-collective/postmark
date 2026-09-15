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

-- 0. ORDERING GATE.
--
-- A comment is not a control. The warning above was written by the same
-- person who then ran this ahead of the deploy twice on 2026-09-14/15,
-- breaking PUT /config both times and restoring the constraint by hand.
-- So the ordering is now enforced, not merely documented.
--
-- Run it like this, once the gateway is serving cortex-utils >= 01bf644:
--
--   psql -v ON_ERROR_STOP=1 \
--        -c "SET cortex.code_deployed = 'yes'" \
--        -f migrations/004_fix_email_mapping_conflict_arbiter.sql
--
-- A SET in a session is normally transient. It is NOT guaranteed to be:
--     ALTER DATABASE cortex SET cortex.code_deployed = 'yes';
--     ALTER ROLE cortex     SET cortex.code_deployed = 'yes';
-- either of which persists the value for every future session, with no
-- expiry and nothing in this file to notice -- permanently defeating the
-- gate. So the gate also refuses when the value arrives from one of those
-- persistent sources, and only accepts a value set for this session.
DO $$
DECLARE
    persisted TEXT;
    persisted_scope TEXT;
BEGIN
    -- Match ANY persisted cortex.code_deployed, at any scope.
    --
    -- The previous version compared setdatabase/setrole against real oids,
    -- which silently missed the cluster-wide form:
    --     ALTER ROLE ALL SET cortex.code_deployed = 'yes';
    -- writes setdatabase = 0 AND setrole = 0, matching neither branch. A
    -- reviewer demonstrated the migration running to completion with only
    -- that in place and no session SET at all -- the gate bypassed by the
    -- exact mechanism it was added to block, one flavour of persistence
    -- later.
    --
    -- So scope is no longer part of the test. Any persisted value is
    -- evidence that someone tried to make this permanent, and over-matching
    -- costs an operator one RESET while under-matching costs a production
    -- incident. We have already had two.
    SELECT s.setting,
           CASE
               WHEN r.setdatabase = 0 AND r.setrole = 0 THEN 'ALL ROLES, ALL DATABASES'
               WHEN r.setdatabase = 0 THEN 'role ' || coalesce(rol.rolname, r.setrole::TEXT)
               WHEN r.setrole = 0 THEN 'database ' || coalesce(db.datname, r.setdatabase::TEXT)
               ELSE 'role ' || coalesce(rol.rolname, r.setrole::TEXT)
                    || ' in database ' || coalesce(db.datname, r.setdatabase::TEXT)
           END
    INTO persisted, persisted_scope
    FROM pg_db_role_setting r
    CROSS JOIN LATERAL unnest(r.setconfig) AS s(setting)
    LEFT JOIN pg_database db ON db.oid = r.setdatabase
    LEFT JOIN pg_roles rol ON rol.oid = r.setrole
    WHERE s.setting LIKE 'cortex.code_deployed=%'
    LIMIT 1;

    IF persisted IS NOT NULL THEN
        RAISE EXCEPTION
            'cortex.code_deployed is persisted for % (%): that defeats this gate '
            'for every future session. RESET it at that scope -- e.g. '
            'ALTER ROLE ALL RESET cortex.code_deployed; / '
            'ALTER DATABASE % RESET cortex.code_deployed; / '
            'ALTER ROLE % RESET cortex.code_deployed; -- then SET it '
            'per-session when you actually mean to run this.',
            persisted_scope, persisted, current_database(), current_user;
    END IF;

    IF coalesce(current_setting('cortex.code_deployed', TRUE), '') <> 'yes' THEN
        RAISE EXCEPTION
            'refusing to run: this migration drops the constraint older '
            'cortex-utils names in ON CONFLICT ON CONSTRAINT unique_email_mapping. '
            'Deploy the gateway with cortex-utils >= 01bf644 first, confirm '
            'PUT /config succeeds, then re-run with: '
            'SET cortex.code_deployed = ''yes'';';
    END IF;
END $$;

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
--
-- The COLUMN LIST check is not redundant with the name check. CREATE UNIQUE
-- INDEX IF NOT EXISTS at step 2 no-ops on a pre-existing index of the same
-- name, so an index that is unique, valid and correctly predicated but built
-- on the WRONG columns (say email_address alone) would pass every other
-- assertion here while Postgres still refused to infer
-- `ON CONFLICT (mapping_type, email_address) WHERE deleted_at IS NULL` at
-- runtime -- the failure this migration exists to prevent, reported as
-- "no unique or exclusion constraint matching the ON CONFLICT specification".
DO $$
DECLARE
    arbiter_ok BOOLEAN;
    arbiter_cols TEXT[];
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

    -- Slice to indnkeyatts: INCLUDE columns sit in indkey after the key
    -- columns but play no part in ON CONFLICT inference, so folding them
    -- into this comparison would false-fail on an index that is actually
    -- fine. LEFT JOIN because an expression column has attnum 0 and no
    -- pg_attribute row -- an INNER JOIN silently drops it and reports a
    -- truncated column list, which fails safe but misleadingly.
    SELECT array_agg(coalesce(a.attname, '(expression)') ORDER BY k.ord)
    INTO arbiter_cols
    FROM pg_index i
    JOIN LATERAL unnest(i.indkey[0:i.indnkeyatts - 1]) WITH ORDINALITY AS k(attnum, ord)
         ON TRUE
    LEFT JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = k.attnum
    WHERE i.indexrelid = 'idx_email_mappings_active'::regclass;

    IF arbiter_cols IS DISTINCT FROM ARRAY['mapping_type', 'email_address']::TEXT[] THEN
        RAISE EXCEPTION
            'idx_email_mappings_active is on columns %, expected (mapping_type, email_address): '
            'ON CONFLICT inference will fail at runtime',
            arbiter_cols;
    END IF;

    IF EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conrelid = 'triage_email_mappings'::regclass
          AND conname = 'unique_email_mapping'
    ) THEN
        RAISE EXCEPTION 'unique_email_mapping still present after drop';
    END IF;

    RAISE NOTICE 'OK: partial unique index on % is the sole upsert arbiter', arbiter_cols;
END $$;

COMMIT;
