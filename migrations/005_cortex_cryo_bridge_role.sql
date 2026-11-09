-- Migration: the role cryo-bridge uses to put mail-derived tasks on cryo's queue
-- Related: cortex-wznn, cryo docs/MAIL-TASK-CONTRACT.md §4
--
-- Run against the `cortex` database as the superuser. Safe to rerun, including
-- concurrently.
--
-- !! ORDER !!
--   1. This migration. cryo's GRANT ... TO cortex_cryo_bridge fails without it.
--   2. cryo's migration: cryo.enqueue_mail(), and EXECUTE on it plus USAGE on
--      schema cryo granted to this role (cryo-backend).
--   3. Enable login. Export CORTEX_CRYO_BRIDGE_PASSWORD on hades, with the value
--      from stack 48's Env, then:
--        docker exec -i -e CORTEX_CRYO_BRIDGE_PASSWORD cortex-postgres \
--          psql -v ON_ERROR_STOP=1 -U cortex -d cortex <<'SQL'
--        SET log_statement = 'none';
--        SET log_min_error_statement = 'panic';
--        \getenv pw CORTEX_CRYO_BRIDGE_PASSWORD
--        SELECT length(:'pw') > 0 AS has_pw \gset
--        \if :has_pw
--        ALTER ROLE cortex_cryo_bridge LOGIN PASSWORD :'pw';
--        \else
--        SELECT 'CORTEX_CRYO_BRIDGE_PASSWORD is empty'::int;
--        \endif
--        SQL
--      The two SETs keep the password out of the server log whether the ALTER
--      succeeds or fails. Then log in AS the role by its network name, as the
--      bridge does (127.0.0.1 is `trust` in this image, so it would prove
--      nothing). Expect "t". A missing grant prints "f", a bad password errors:
--        docker exec -i -e CORTEX_CRYO_BRIDGE_PASSWORD cortex-postgres sh -c \
--          'PGPASSWORD="$CORTEX_CRYO_BRIDGE_PASSWORD" psql -h cortex-postgres \
--           -U cortex_cryo_bridge -d cortex -Atc "SELECT has_function_privilege(
--           '\''cryo.enqueue_mail(text,jsonb,text,text)'\'', '\''EXECUTE'\'')"'
--   4. Deploy cortex-actions with CRYO_DRAIN_ENABLED=false, then flip it to true.
--   Out of order: a flag flipped before step 2 fails every self-note job with
--   undefined_function, and before step 3 with a login failure. Either way the
--   job retries, then dead-letters, and nothing reaches cryo.
--
-- WHAT THE ROLE CAN DO: whatever cryo grants it, plus PUBLIC's defaults, which a
-- per-role REVOKE cannot remove: CONNECT and TEMP on the database, USAGE on
-- schema public, reading the system catalogs, and EXECUTE on functions that did
-- not revoke it from PUBLIC (today only trigger functions, none SECURITY
-- DEFINER; a future SECURITY DEFINER function must REVOKE ... FROM PUBLIC). It
-- can read and write no table. The contract's "only the function writes mail/
-- rows" holds for this role only; every other cortex service still connects as
-- the superuser.
--
-- ROLLBACK: set CRYO_DRAIN_ENABLED=false; ALTER ROLE cortex_cryo_bridge NOLOGIN;
-- cryo reverts its grants (or DROP OWNED BY cortex_cryo_bridge); DROP ROLE.

BEGIN;

-- Concurrent runs otherwise race on the role's catalog row.
SELECT pg_advisory_xact_lock(hashtext('cortex_cryo_bridge_role'));

DO $$
BEGIN
    CREATE ROLE cortex_cryo_bridge NOLOGIN;
EXCEPTION WHEN duplicate_object THEN
    NULL;
END
$$;

-- Attributes converge on every run; LOGIN and the password are step 3's.
ALTER ROLE cortex_cryo_bridge
    NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT NOREPLICATION NOBYPASSRLS
    CONNECTION LIMIT 2;

COMMIT;
