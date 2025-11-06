-- Advanced commands test suite for pgkv extension

BEGIN;

-- Load pgTAP extension
CREATE EXTENSION IF NOT EXISTS pgtap;

-- Plan the number of tests
SELECT plan(30);

-- ============================================================================
-- Counter Operations (INCR, DECR, INCRBY, DECRBY)
-- ============================================================================

-- Test INCR function
SELECT has_function('pgkv', 'incr', ARRAY['text'], 'pgkv.incr function should exist');

SELECT is(
    pgkv.incr('counter'),
    1::BIGINT,
    'INCR on new key should return 1'
);

SELECT is(
    pgkv.incr('counter'),
    2::BIGINT,
    'INCR should increment to 2'
);

SELECT is(
    pgkv.get('counter'),
    to_jsonb(2::bigint),
    'GET should return counter value as JSONB number'
);

-- Test DECR function
SELECT has_function('pgkv', 'decr', ARRAY['text'], 'pgkv.decr function should exist');

SELECT is(
    pgkv.decr('counter'),
    1::BIGINT,
    'DECR should decrement to 1'
);

SELECT is(
    pgkv.decr('new_counter'),
    -1::BIGINT,
    'DECR on new key should return -1'
);

-- Test INCRBY function
SELECT has_function('pgkv', 'incrby', ARRAY['text', 'bigint'], 'pgkv.incrby function should exist');

SELECT is(
    pgkv.incrby('score', 10),
    10::BIGINT,
    'INCRBY on new key should return increment value'
);

SELECT is(
    pgkv.incrby('score', 5),
    15::BIGINT,
    'INCRBY should add to existing value'
);

-- Test DECRBY function
SELECT has_function('pgkv', 'decrby', ARRAY['text', 'bigint'], 'pgkv.decrby function should exist');

SELECT is(
    pgkv.decrby('score', 3),
    12::BIGINT,
    'DECRBY should subtract from existing value'
);

-- Test INCR on non-numeric value (should raise exception)
SELECT is(
    pgkv.set('text_key', 'not_a_number'),
    'OK',
    'SET text value'
);

SELECT throws_ok(
    'SELECT pgkv.incr(''text_key'')',
    'P0001',
    'value is not an integer or out of range',
    'INCR should raise exception for non-numeric value'
);

-- ============================================================================
-- Multi-Key Operations (MGET, MSET)
-- ============================================================================

-- Test MSET function
SELECT has_function('pgkv', 'mset', ARRAY['text[]'], 'pgkv.mset function should exist');

SELECT is(
    pgkv.mset('mkey1', 'mvalue1', 'mkey2', 'mvalue2', 'mkey3', 'mvalue3'),
    'OK',
    'MSET should return OK'
);

-- Test MGET function
SELECT has_function('pgkv', 'mget', ARRAY['text[]'], 'pgkv.mget function should exist');

-- Verify MGET returns correct values (JSONB)
SELECT results_eq(
    'SELECT key, value FROM pgkv.mget(''mkey1'', ''mkey2'', ''mkey3'') ORDER BY key',
    $$VALUES ('mkey1'::text, to_jsonb('mvalue1'::text)), ('mkey2'::text, to_jsonb('mvalue2'::text)), ('mkey3'::text, to_jsonb('mvalue3'::text))$$,
    'MGET should return all JSONB values'
);

-- Test MGET with nonexistent key
SELECT results_eq(
    'SELECT key, value FROM pgkv.mget(''mkey1'', ''nonexistent'') ORDER BY key',
    $$VALUES ('mkey1'::text, to_jsonb('mvalue1'::text)), ('nonexistent'::text, NULL::jsonb)$$,
    'MGET should return NULL for nonexistent keys'
);

-- ============================================================================
-- Key Inspection (KEYS, DBSIZE)
-- ============================================================================

-- Test KEYS function
SELECT has_function('pgkv', 'keys', ARRAY['text'], 'pgkv.keys function should exist');

-- Set up test keys with pattern
SELECT is(pgkv.set('user:1', 'alice'), 'OK', 'SET user:1');
SELECT is(pgkv.set('user:2', 'bob'), 'OK', 'SET user:2');
SELECT is(pgkv.set('session:1', 'xyz'), 'OK', 'SET session:1');

-- Test Redis pattern syntax (converted from user:% SQL LIKE to user:* Redis pattern)
SELECT results_eq(
    'SELECT pgkv.keys(''user:*'') ORDER BY 1',
    $$VALUES ('user:1'::text), ('user:2'::text)$$,
    'KEYS should return matching keys with Redis * wildcard'
);

-- Test Redis ? wildcard
SELECT is(pgkv.set('user:a', 'test'), 'OK', 'SET user:a');
SELECT ok(
    (SELECT COUNT(*) FROM pgkv.keys('user:?') = 3),
    'KEYS with ? should match single character'
);

-- Test character class pattern (uses regex fallback)
SELECT is(pgkv.set('h_ello', 'test1'), 'OK', 'SET h_ello');
SELECT is(pgkv.set('h_allo', 'test2'), 'OK', 'SET h_allo');
SELECT results_eq(
    'SELECT pgkv.keys(''h_[ae]llo'') ORDER BY 1',
    $$VALUES ('h_allo'::text), ('h_ello'::text)$$,
    'KEYS should support character class patterns via regex'
);

-- Test DBSIZE function
SELECT has_function('pgkv', 'dbsize', NULL, 'pgkv.dbsize function should exist');

SELECT ok(
    pgkv.dbsize() >= 3,
    'DBSIZE should count at least 3 keys'
);

-- ============================================================================
-- Maintenance Operations (CLEANUP_EXPIRED, FLUSHALL)
-- ============================================================================

-- Test CLEANUP_EXPIRED function
SELECT has_function('pgkv', 'cleanup_expired', NULL, 'pgkv.cleanup_expired function should exist');

-- Test FLUSHALL function
SELECT has_function('pgkv', 'flushall', NULL, 'pgkv.flushall function should exist');

SELECT is(
    pgkv.flushall(),
    'OK',
    'FLUSHALL should return OK'
);

SELECT is(
    pgkv.dbsize(),
    0::BIGINT,
    'DBSIZE should return 0 after FLUSHALL'
);

-- Finish tests
SELECT * FROM finish();
ROLLBACK;
