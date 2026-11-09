-- Migration: the role cryo-bridge uses to put mail-derived tasks on cryo's queue
-- Related: cortex-wznn, cryo docs/MAIL-TASK-CONTRACT.md §4
--
-- cryo's migration grants this role EXECUTE on cryo.enqueue_mail() (and USAGE
-- on schema cryo), and nothing else. Its GRANT fails if the role does not
-- exist, so this runs FIRST.
--
-- The role is created NOLOGIN. The password lives in stack 48's Env, not here,
-- so login is enabled by hand at deploy, run as the superuser:
--
--   ALTER ROLE cortex_cryo_bridge LOGIN PASSWORD '<CORTEX_CRYO_BRIDGE_PASSWORD>';
--
-- cryo-bridge keeps its existing `cortex` connection for the queue and the
-- email tables, and opens a second connection as this role only to call
-- cryo.enqueue_mail().
--
-- Safe to run twice.

DO $$
BEGIN
    IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'cortex_cryo_bridge') THEN
        CREATE ROLE cortex_cryo_bridge
            NOLOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT NOREPLICATION;
    END IF;
END
$$;
