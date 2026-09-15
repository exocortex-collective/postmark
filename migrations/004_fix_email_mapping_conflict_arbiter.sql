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
--        -c "CREATE TEMP TABLE cortex_code_deployed ()" \
--        -f migrations/004_fix_email_mapping_conflict_arbiter.sql
--
-- WHY A TEMP TABLE AND NOT A GUC
--
-- The gate needs proof that a human decided to run this *now*, in this
-- session. Three review rounds found three different ways to make a custom
-- GUC permanent, each of which defeats the gate for every future session
-- while leaving it looking like it works:
--
--   1. ALTER DATABASE cortex SET cortex.code_deployed = 'yes';
--   2. ALTER ROLE cortex     SET cortex.code_deployed = 'yes';
--   3. ALTER ROLE ALL        SET cortex.code_deployed = 'yes';
--        -- writes setdatabase = 0 AND setrole = 0, matching neither branch
--           of a naive pg_db_role_setting query
--   4. postgresql.conf + pg_reload_conf()
--        -- cluster-wide, and writes NO row to pg_db_role_setting at all,
--           so no catalog query over that table can see it
--
-- Widening the query does not converge. (4) is invisible to it; ALTER SYSTEM
-- is rejected for a custom GUC; and a placeholder GUC does not appear in
-- pg_settings, so `source = 'session'` is not available either. Each round
-- closed one hole and the next round found another.
--
-- A temp table has no such ladder. It lives in a schema private to one
-- backend, is destroyed when that backend disconnects, and there is no
-- configuration mechanism -- file, catalog, role, database or cluster -- that
-- can create one. Its existence is therefore proof of exactly the thing the
-- gate is asking about, rather than a proxy for it. Eight lines instead of
-- fifty-six, and nothing left to widen.
DO $$
BEGIN
    IF to_regclass('pg_temp.cortex_code_deployed') IS NULL THEN
        RAISE EXCEPTION
            'refusing to run: this migration drops the constraint older '
            'cortex-utils names in ON CONFLICT ON CONSTRAINT unique_email_mapping. '
            'Deploy the gateway with cortex-utils >= 01bf644 first, confirm '
            'PUT /config succeeds, then re-run in a session that has done: '
            'CREATE TEMP TABLE cortex_code_deployed ();';
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
    offender TEXT;
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

    -- Check by SHAPE, not just by name. Step 1 drops two forms -- the named
    -- constraint and a bare index of the same name -- but the check above
    -- only looks at pg_constraint. An unqualified DROP INDEX IF EXISTS
    -- no-ops on an index outside search_path, which would let this commit
    -- with an UNCONDITIONAL unique object still live on these columns: the
    -- exact semantic bug the header describes, passing under a different
    -- name. Uniqueness here must hold over live rows only, so that a
    -- soft-deleted mapping can be recreated.
    -- One query, so the name in the message is necessarily the index that
    -- tripped the check. An EXISTS plus a separate lookup for the message
    -- drifted apart on the first attempt: the lookup omitted the column
    -- filter and reported triage_email_mappings_pkey -- unique and
    -- unconditional, but on (id), and not what matched. An error that names
    -- the primary key invites an operator to drop the primary key.
    SELECT i.indexrelid::regclass::TEXT
      INTO offender
      FROM pg_index i
     WHERE i.indrelid = 'triage_email_mappings'::regclass
       AND i.indisunique
       AND i.indpred IS NULL                    -- unconditional
       AND (SELECT array_agg(a.attname::TEXT ORDER BY a.attname)
              FROM unnest(i.indkey[0:i.indnkeyatts - 1]) AS k(attnum)
              JOIN pg_attribute a
                ON a.attrelid = i.indrelid AND a.attnum = k.attnum)
           = ARRAY['email_address', 'mapping_type']
     LIMIT 1;

    IF offender IS NOT NULL THEN
        RAISE EXCEPTION
            'an UNCONDITIONAL unique index on (mapping_type, email_address) '
            'survives (index %). It blocks recreating a soft-deleted mapping '
            'and shadows the partial arbiter. Drop it, qualifying the schema.',
            offender;
    END IF;

    RAISE NOTICE 'OK: partial unique index on % is the sole upsert arbiter', arbiter_cols;
END $$;

COMMIT;
