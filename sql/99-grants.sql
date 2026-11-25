-- ============================================================================
-- Security: Set appropriate permissions
-- ============================================================================

-- Schema access for public
GRANT USAGE ON SCHEMA pgkv TO PUBLIC;

-- Function execution access
GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA pgkv TO PUBLIC;

-- Internal helpers (names starting with an underscore) are not part of the API: only the
-- pgkv functions call them, as the owner.
DO $$
DECLARE
    v_function REGPROCEDURE;
BEGIN
    FOR v_function IN
        SELECT p.oid::regprocedure FROM pg_catalog.pg_proc p
        WHERE p.pronamespace = 'pgkv'::regnamespace AND p.proname LIKE E'\\_%'
    LOOP
        EXECUTE format('REVOKE ALL ON FUNCTION %s FROM PUBLIC', v_function);
    END LOOP;
END;
$$;

-- Explicitly prevent direct table manipulation
REVOKE ALL ON ALL TABLES IN SCHEMA pgkv FROM PUBLIC;

-- Ensure future functions are accessible (critical for upgrades)
ALTER DEFAULT PRIVILEGES IN SCHEMA pgkv
    GRANT EXECUTE ON FUNCTIONS TO PUBLIC;

-- Ensure future tables remain protected
ALTER DEFAULT PRIVILEGES IN SCHEMA pgkv
    REVOKE ALL ON TABLES FROM PUBLIC;

-- Monitoring access (following pgmq pattern)
GRANT SELECT ON ALL TABLES IN SCHEMA pgkv TO pg_monitor;
ALTER DEFAULT PRIVILEGES IN SCHEMA pgkv
    GRANT SELECT ON TABLES TO pg_monitor;

-- Protect sequences
REVOKE ALL ON ALL SEQUENCES IN SCHEMA pgkv FROM PUBLIC;
ALTER DEFAULT PRIVILEGES IN SCHEMA pgkv
    REVOKE ALL ON SEQUENCES FROM PUBLIC;
