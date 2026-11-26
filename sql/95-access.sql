-- ============================================================================
-- Access: who may use pgkv
-- ============================================================================
-- pgkv is one shared keyspace behind SECURITY DEFINER functions, so a role that can execute
-- them can read, overwrite and delete every key. Nothing is granted to PUBLIC: the owner
-- (the role that created the extension) names the roles that get access.

-- GRANT_ACCESS: Let a role use pgkv: USAGE on the schema and EXECUTE on the data commands.
-- The maintenance functions (cleanup_expired, set_unlogged and these two) stay with the
-- owner. Run it again after every upgrade, so the role also gets the functions that are new.
CREATE OR REPLACE FUNCTION pgkv.grant_access(p_role REGROLE)
RETURNS VOID
LANGUAGE plpgsql
SET search_path = pgkv, pg_temp
AS $$
DECLARE
    v_function REGPROCEDURE;
BEGIN
    IF p_role IS NULL THEN
        RAISE EXCEPTION 'pgkv.grant_access: role is required';
    END IF;
    EXECUTE format('GRANT USAGE ON SCHEMA pgkv TO %s', p_role);
    FOR v_function IN
        SELECT p.oid::regprocedure FROM pg_catalog.pg_proc p
        WHERE p.pronamespace = 'pgkv'::regnamespace
          AND p.proname NOT LIKE E'\\_%'
          AND p.proname <> ALL (ARRAY['grant_access', 'revoke_access', 'cleanup_expired', 'set_unlogged'])
    LOOP
        EXECUTE format('GRANT EXECUTE ON ROUTINE %s TO %s', v_function, p_role);
    END LOOP;
END;
$$;

-- REVOKE_ACCESS: Take back everything grant_access gave, and any other privilege the role
-- holds on pgkv's schema and functions.
CREATE OR REPLACE FUNCTION pgkv.revoke_access(p_role REGROLE)
RETURNS VOID
LANGUAGE plpgsql
SET search_path = pgkv, pg_temp
AS $$
BEGIN
    IF p_role IS NULL THEN
        RAISE EXCEPTION 'pgkv.revoke_access: role is required';
    END IF;
    EXECUTE format('REVOKE ALL ON ALL ROUTINES IN SCHEMA pgkv FROM %s', p_role);
    EXECUTE format('REVOKE ALL ON SCHEMA pgkv FROM %s', p_role);
END;
$$;
