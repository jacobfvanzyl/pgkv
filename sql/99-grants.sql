-- ============================================================================
-- Security: closed by default
-- ============================================================================
-- Shared by the fresh install and the upgrade from 0.3.0 (which first revokes what roles
-- were granted by name, sql/upgrade-0.3.0-grants.sql). Access is given afterwards, outside
-- the extension scripts, with pgkv.grant_access.

-- PostgreSQL lets PUBLIC execute a new function unless told otherwise, and versions before
-- 0.4.0 granted the schema and every function to PUBLIC. This covers the functions that
-- exist now. Nothing can make it the default for later ones (default privileges set for one
-- schema can only add to the global defaults), so an upgrade script revokes PUBLIC from
-- each function it adds.
REVOKE ALL ON SCHEMA pgkv FROM PUBLIC;
REVOKE ALL ON ALL ROUTINES IN SCHEMA pgkv FROM PUBLIC;

-- No direct access to the tables or the sequence. PUBLIC has none by default; this is for
-- an install where it was granted.
REVOKE ALL ON ALL TABLES IN SCHEMA pgkv FROM PUBLIC;
REVOKE ALL ON ALL SEQUENCES IN SCHEMA pgkv FROM PUBLIC;

-- Monitoring access (following pgmq pattern)
GRANT USAGE ON SCHEMA pgkv TO pg_monitor;
GRANT SELECT ON ALL TABLES IN SCHEMA pgkv TO pg_monitor;
ALTER DEFAULT PRIVILEGES IN SCHEMA pgkv
    GRANT SELECT ON TABLES TO pg_monitor;
