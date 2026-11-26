
-- Access starts again from nothing. Versions before 0.4.0 granted pgkv to PUBLIC, and
-- PostgreSQL records every privilege an extension script leaves behind as the extension's
-- own, which pg_dump then does not dump: a grant carried over here would silently be missing
-- after a restore. So this script leaves no role with access, and the owner grants it again
-- with pgkv.grant_access (in the same transaction as the upgrade, to avoid a gap).
DO $$
DECLARE
    v_role OID;
    v_roles TEXT[] := '{}';
BEGIN
    FOR v_role IN
        SELECT DISTINCT a.grantee
        FROM (
            SELECT p.proacl AS acl, p.proowner AS owner FROM pg_catalog.pg_proc p
            WHERE p.pronamespace = 'pgkv'::regnamespace
            UNION ALL
            SELECT n.nspacl, n.nspowner FROM pg_catalog.pg_namespace n WHERE n.nspname = 'pgkv'
        ) o
        CROSS JOIN LATERAL aclexplode(o.acl) a
        WHERE a.grantee <> 0 AND a.grantee <> o.owner
    LOOP
        PERFORM pgkv.revoke_access(v_role::regrole);
        v_roles := v_roles || v_role::regrole::text;
    END LOOP;
    RAISE WARNING 'pgkv 0.4.0 grants access to no role by default, and this upgrade has revoked the access granted before it (PUBLIC included). Run SELECT pgkv.grant_access(''<role>'') for each role that uses pgkv.%',
        CASE WHEN cardinality(v_roles) > 0
             THEN ' Roles that held grants: ' || array_to_string(v_roles, ', ') || '.' ELSE '' END;
END;
$$;

