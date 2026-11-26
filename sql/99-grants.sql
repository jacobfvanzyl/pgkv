-- ============================================================================
-- Security: closed by default
-- ============================================================================
-- Shared by the fresh install and the upgrade from 0.3.0 (which first revokes what roles
-- were granted by name, sql/upgrade-0.3.0-grants.sql). Access is given afterwards, outside
-- the extension scripts, with pgkv.grant_access.

-- PostgreSQL lets PUBLIC execute a new function unless told otherwise, and versions before
-- 0.4.0 granted the schema and every function to PUBLIC.
REVOKE ALL ON SCHEMA pgkv FROM PUBLIC;
REVOKE ALL ON ALL ROUTINES IN SCHEMA pgkv FROM PUBLIC;
ALTER DEFAULT PRIVILEGES IN SCHEMA pgkv
    REVOKE EXECUTE ON FUNCTIONS FROM PUBLIC;

-- No direct access to the tables or the sequence
REVOKE ALL ON ALL TABLES IN SCHEMA pgkv FROM PUBLIC;
ALTER DEFAULT PRIVILEGES IN SCHEMA pgkv
    REVOKE ALL ON TABLES FROM PUBLIC;
REVOKE ALL ON ALL SEQUENCES IN SCHEMA pgkv FROM PUBLIC;
ALTER DEFAULT PRIVILEGES IN SCHEMA pgkv
    REVOKE ALL ON SEQUENCES FROM PUBLIC;

-- Monitoring access (following pgmq pattern)
GRANT USAGE ON SCHEMA pgkv TO pg_monitor;
GRANT SELECT ON ALL TABLES IN SCHEMA pgkv TO pg_monitor;
ALTER DEFAULT PRIVILEGES IN SCHEMA pgkv
    GRANT SELECT ON TABLES TO pg_monitor;
