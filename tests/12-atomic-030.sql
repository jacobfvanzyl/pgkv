-- pgkv 0.3.0: atomic SET options, compare-and-set, prefix scan (single-session behaviour).
-- What two sessions do to each other is tested in stress/test_atomicity.py.

BEGIN;
CREATE EXTENSION IF NOT EXISTS pgtap;
SELECT plan(74);

SELECT pgkv.flushall();

-- A key that has already expired (1970) but has not been swept.
CREATE FUNCTION pg_temp.expired(p_key text) RETURNS void LANGUAGE sql AS
$$ SELECT pgkv.set_opts(p_key, 'old', p_exat => 1); $$;
CREATE FUNCTION pg_temp.placeholders() RETURNS integer LANGUAGE sql AS
$$ SELECT COUNT(*)::integer FROM pgkv.store WHERE expires_at = '-infinity'; $$;

-- Compare-and-set
SELECT has_function('pgkv', 'cas', ARRAY['text','text','text','bigint','bigint','bigint','bigint','boolean'], 'cas exists');
SELECT has_function('pgkv', 'cad', ARRAY['text','text'], 'cad exists');
SELECT is(pgkv.cas('c', 'x', 'v1'), 0, 'CAS on a missing key with an expected value writes nothing');
SELECT is(pgkv.exists('c'), 0, 'CAS that failed created nothing');
SELECT is(pgkv.cas('c', NULL, 'v1'), 1, 'CAS expecting absence creates the key');
SELECT is(pgkv.cas('c', NULL, 'v2'), 0, 'CAS expecting absence fails on an existing key');
SELECT is(pgkv.cas('c', 'nope', 'v2'), 0, 'CAS with the wrong expected value writes nothing');
SELECT is(pgkv.get('c'), to_jsonb('v1'::text), 'failed CAS left the value alone');
SELECT is(pgkv.cas('c', 'v1', 'v2'), 1, 'CAS with the right expected value writes');
SELECT is(pgkv.get('c'), to_jsonb('v2'::text), 'CAS wrote the new value');
SELECT is(pgkv.cas('c', 'v2', 'v3', p_ex => 100), 1, 'CAS with EX writes');
SELECT ok(pgkv.ttl('c') BETWEEN 1 AND 100, 'CAS with EX sets the expiry');
SELECT is(pgkv.cas('c', 'v3', 'v4', p_keepttl => true), 1, 'CAS with KEEPTTL writes');
SELECT ok(pgkv.ttl('c') BETWEEN 1 AND 100, 'CAS with KEEPTTL keeps the expiry');
SELECT is(pgkv.cas('c', 'v4', 'v5'), 1, 'CAS without an expiry option writes');
SELECT is(pgkv.ttl('c'), -1, 'CAS without an expiry option clears the expiry, as SET does');
SELECT pgkv.incrby('n', 5);
SELECT is(pgkv.cas('n', '5', '6'), 1, 'CAS compares a counter by its text');
SELECT pg_temp.expired('gone');
SELECT is(pgkv.cas('gone', 'old', 'new'), 0, 'CAS treats an expired key as absent');
SELECT is(pgkv.cas('gone', NULL, 'new'), 1, 'CAS expecting absence takes over an expired key');
SELECT is(pgkv.get('gone'), to_jsonb('new'::text), 'CAS replaced the expired key');
SELECT is(pgkv.ttl('gone'), -1, 'the replacement does not inherit the old expiry');
SELECT pgkv.hset('h', 'f', 'v');
SELECT throws_ok($$SELECT pgkv.cas('h', 'x', 'y')$$, 'KV001', NULL, 'CAS on a hash is WRONGTYPE');
SELECT throws_ok($$SELECT pgkv.cas('c', 'v5', 'v6', p_ex => 1, p_px => 1)$$, 'KV000', NULL, 'CAS rejects conflicting expiry options');

-- Compare-and-delete
SELECT is(pgkv.cad('c', 'nope'), 0, 'CAD with the wrong value deletes nothing');
SELECT is(pgkv.cad('missing', 'x'), 0, 'CAD on a missing key deletes nothing');
SELECT is(pgkv.cad('c', NULL), 0, 'CAD with NULL deletes nothing');
SELECT is(pgkv.cad('c', 'v5'), 1, 'CAD with the right value deletes');
SELECT is(pgkv.exists('c'), 0, 'CAD deleted the key');
SELECT throws_ok($$SELECT pgkv.cad('h', 'x')$$, 'KV001', NULL, 'CAD on a hash is WRONGTYPE');

-- SET options and expiry
SELECT pg_temp.expired('e1');
SELECT is(pgkv.set_opts('e1', 'new', p_nx => true), to_jsonb('OK'::text), 'SET NX succeeds on an expired key');
SELECT is(pgkv.get('e1'), to_jsonb('new'::text), 'SET NX replaced the expired key');
SELECT pg_temp.expired('e2');
SELECT is(pgkv.set_opts('e2', 'new', p_xx => true), NULL::jsonb, 'SET XX fails on an expired key');
SELECT is(pgkv.setnx('e2', 'new'), 1, 'SETNX succeeds on an expired key');
SELECT is(pgkv.set_opts('g', 'a', p_get => true), NULL::jsonb, 'SET GET on a missing key returns NULL');
SELECT is(pgkv.set_opts('g', 'b', p_get => true), to_jsonb('a'::text), 'SET GET returns the old value');
SELECT is(pgkv.set_opts('g', 'c', p_nx => true, p_get => true), to_jsonb('b'::text), 'SET NX GET returns the existing value');
SELECT is(pgkv.get('g'), to_jsonb('b'::text), 'SET NX GET did not overwrite');
SELECT is(pgkv.set_opts('g2', 'c', p_nx => true, p_get => true), NULL::jsonb, 'SET NX GET returns NULL when it sets');
SELECT is(pgkv.get('g2'), to_jsonb('c'::text), 'SET NX GET set the missing key');
SELECT is(pgkv.set_opts('g3', 'c', p_xx => true, p_get => true), NULL::jsonb, 'SET XX GET on a missing key returns NULL');
SELECT is(pgkv.exists('g3'), 0, 'SET XX GET on a missing key sets nothing');
SELECT pgkv.set('k', 'v', 100);
SELECT is(pgkv.set_opts('k', 'w', p_keepttl => true), to_jsonb('OK'::text), 'SET KEEPTTL returns OK');
SELECT ok(pgkv.ttl('k') BETWEEN 1 AND 100, 'SET KEEPTTL keeps the expiry');
SELECT is(pgkv.set_opts('k', 'x', p_xx => true, p_get => true, p_keepttl => true), to_jsonb('w'::text), 'SET XX GET KEEPTTL returns the old value');
SELECT ok(pgkv.ttl('k') BETWEEN 1 AND 100, 'SET XX GET KEEPTTL keeps the expiry');

-- Expired keys are absent
SELECT pg_temp.expired('e3');
SELECT is(pgkv.expire('e3', 100), 0, 'EXPIRE does not revive an expired key');
SELECT is(pgkv.get('e3'), NULL::jsonb, 'the expired key stays gone');
SELECT pg_temp.expired('e4');
SELECT is(pgkv.del('e4', 'k'), 1, 'DEL counts live keys only');
SELECT pg_temp.expired('e5');
SELECT is(pgkv.getdel('e5'), NULL::jsonb, 'GETDEL of an expired key returns NULL');
SELECT is(pgkv.getex('e5', 100), NULL::jsonb, 'GETEX of an expired key returns NULL');
SELECT throws_ok($$SELECT pgkv.getdel('h')$$, 'KV001', NULL, 'GETDEL on a hash is WRONGTYPE');
SELECT throws_ok($$SELECT pgkv.getex('h', 100)$$, 'KV001', NULL, 'GETEX on a hash is WRONGTYPE');

-- No placeholder row is ever left behind
SELECT is(pgkv.msetnx('m1', 'a', 'm1', 'b', 'm2', 'c'), 1, 'MSETNX accepts a repeated key');
SELECT is(pgkv.get('m1'), to_jsonb('b'::text), 'MSETNX: the last value for a repeated key wins');
SELECT is(pgkv.msetnx('m0', 'x', 'm2', 'y', 'm3', 'z'), 0, 'MSETNX fails when one key exists');
SELECT is(pgkv.lmove('zz-missing', 'aa-missing', 'LEFT', 'RIGHT'), NULL::jsonb, 'LMOVE from a missing list returns NULL');
SELECT throws_ok($$SELECT pgkv.lpush('g', 'x')$$, 'KV001', NULL, 'LPUSH on a string is WRONGTYPE');
SELECT is(pgkv.mset('ms1', 'a', 'ms2', 'b', 'ms1', 'c'), 'OK', 'MSET accepts a repeated key');
SELECT is(pgkv.get('ms1'), to_jsonb('c'::text), 'MSET: the last value for a repeated key wins');
SELECT pgkv.set('ms2', 'ttl', 100);
SELECT pgkv.mset('ms2', 'd');
SELECT is(pgkv.ttl('ms2'), -1, 'MSET clears the expiry, as SET does');
SELECT is(pgkv.del('ms1', 'ms2', 'ms1', 'nope'), 2, 'DEL counts each deleted key once');
SELECT is(pg_temp.placeholders(), 0, 'failed and empty operations leave no placeholder rows');
SELECT is(pgkv.exists('m0', 'm3', 'aa-missing'), 0, 'and no keys');

-- Scans
SELECT pgkv.flushall();
SELECT pgkv.set(k, 'v') FROM unnest(ARRAY['p:b', 'p:a', 'p:c', 'P:x', 'q:a', 'p_d']) k;
SELECT pg_temp.expired('p:zz');
SELECT is(
    (SELECT c.collname::text FROM pg_attribute a JOIN pg_collation c ON c.oid = a.attcollation
     WHERE a.attrelid = 'pgkv.store'::regclass AND a.attname = 'key'),
    'C', 'key is byte-ordered, so the primary key serves prefix scans');
SELECT results_eq(
    $$SELECT key, type, value FROM pgkv.scan_prefix('p:', NULL, 2)$$,
    $$VALUES ('p:a'::text, 'string'::text, to_jsonb('v'::text)), ('p:b', 'string', to_jsonb('v'::text))$$,
    'SCAN_PREFIX returns the first page in key order, with type and value');
SELECT results_eq(
    $$SELECT key FROM pgkv.scan_prefix('p:', 'p:b', 2)$$,
    $$VALUES ('p:c'::text)$$,
    'SCAN_PREFIX continues after the given key and skips expired keys');
SELECT results_eq(
    $$SELECT key FROM pgkv.scan_prefix('p_')$$,
    $$VALUES ('p_d'::text)$$,
    'SCAN_PREFIX takes the prefix literally');
SELECT throws_ok($$SELECT * FROM pgkv.scan_prefix('p:', NULL, 0)$$, 'KV000', NULL, 'SCAN_PREFIX rejects a count of 0');
SELECT results_eq(
    $$SELECT cursor, key FROM pgkv.scan(0, 'p:*', 2)$$,
    $$VALUES (2::bigint, 'p:a'::text), (2::bigint, 'p:b'::text)$$,
    'SCAN returns a page and the next cursor');
SELECT results_eq(
    $$SELECT cursor, key FROM pgkv.scan(2, 'p:*', 2)$$,
    $$VALUES (0::bigint, 'p:c'::text)$$,
    'SCAN ends with cursor 0 and skips expired keys');
SELECT is((SELECT COUNT(*)::integer FROM pgkv.store WHERE key = 'p:zz'), 1, 'SCAN did not delete the expired key');

-- Internal helpers are not callable by ordinary roles
CREATE ROLE pgkv_test_030;
SELECT ok(NOT has_function_privilege('pgkv_test_030', 'pgkv._lock_key(text, boolean)', 'EXECUTE'), 'PUBLIC cannot execute _lock_key');
SELECT ok(NOT has_function_privilege('pgkv_test_030', 'pgkv._put(text, jsonb, text, timestamptz)', 'EXECUTE'), 'PUBLIC cannot execute _put');
SELECT ok(NOT has_function_privilege('pgkv_test_030', 'pgkv.cas(text, text, text, bigint, bigint, bigint, bigint, boolean)', 'EXECUTE'), 'PUBLIC cannot execute cas either (closed by default from 0.4.0)');
DROP ROLE pgkv_test_030;

SELECT * FROM finish();
ROLLBACK;
