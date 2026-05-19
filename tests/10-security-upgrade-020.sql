-- pgkv 0.2.0 security hardening and upgrade-adjacent tests

BEGIN;
CREATE EXTENSION IF NOT EXISTS pgtap;
SELECT plan(11);

SELECT pgkv.flushall();

-- All SECURITY DEFINER functions should pin search_path.
SELECT is(
    (
        SELECT COUNT(*)::integer
        FROM pg_proc p
        JOIN pg_namespace n ON n.oid = p.pronamespace
        WHERE n.nspname = 'pgkv'
          AND p.prosecdef
          AND NOT EXISTS (
              SELECT 1
              FROM unnest(COALESCE(p.proconfig, ARRAY[]::text[])) cfg
              WHERE cfg = 'search_path=pgkv, pg_temp'
          )
    ),
    0,
    'all SECURITY DEFINER functions pin search_path'
);

SELECT is(pgkv.set('sec:key', 'value'), 'OK', 'setup key for security tests');

CREATE ROLE pgkv_test_user LOGIN PASSWORD 'pgkv_test_user';
GRANT USAGE ON SCHEMA pgkv TO pgkv_test_user;
GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA pgkv TO pgkv_test_user;

SET ROLE pgkv_test_user;
SELECT is(pgkv.get('sec:key'), to_jsonb('value'::text), 'unprivileged user can call pgkv function');
SELECT throws_ok(
    'SELECT COUNT(*) FROM pgkv.store',
    '42501',
    NULL,
    'unprivileged user cannot read pgkv.store'
);
RESET ROLE;

REVOKE ALL PRIVILEGES ON ALL FUNCTIONS IN SCHEMA pgkv FROM pgkv_test_user;
REVOKE USAGE ON SCHEMA pgkv FROM pgkv_test_user;
DROP ROLE pgkv_test_user;

-- Error-path coverage for new options.
SELECT throws_ok(
    'SELECT pgkv.set_opts(''bad'', ''v'', p_nx := true, p_xx := true)',
    'P0001',
    NULL,
    'set_opts rejects NX and XX together'
);
SELECT throws_ok(
    'SELECT pgkv.set_opts(''bad'', ''v'', p_ex := 1, p_px := 1)',
    'P0001',
    NULL,
    'set_opts rejects conflicting expiry options'
);
SELECT throws_ok(
    'SELECT pgkv.lmove(''a'', ''b'', ''MIDDLE'', ''LEFT'')',
    'P0001',
    NULL,
    'lmove rejects invalid side'
);
SELECT throws_ok(
    'SELECT pgkv.zunionstore(''z'', ARRAY[''a''], NULL, ''AVG'')',
    'P0001',
    NULL,
    'zunionstore rejects invalid aggregate'
);

-- search_path hijack smoke: caller temp schema object must not affect definer lookup.
CREATE TEMP TABLE store(key text, value jsonb, type text, expires_at timestamptz, created_at timestamptz, updated_at timestamptz);
SET search_path = pg_temp, public;
SELECT is(pgkv.get('sec:key'), to_jsonb('value'::text), 'fixed search_path ignores caller temp store table');
RESET search_path;

SELECT is(pgkv.setnx('security:new', 'v'), 1, 'new 0.2 function callable after grants');
SELECT is(pgkv.get('security:new'), to_jsonb('v'::text), 'new 0.2 function stored value');

SELECT * FROM finish();
ROLLBACK;
