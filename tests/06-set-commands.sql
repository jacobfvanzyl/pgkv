-- SET commands test suite for pgkv extension

BEGIN;

-- Load pgTAP extension
CREATE EXTENSION IF NOT EXISTS pgtap;

-- Plan the number of tests
SELECT plan(37);

-- ============================================================================
-- SET Operations (SADD, SREM, SMEMBERS, SISMEMBER, SCARD, SINTER, SUNION, SDIFF)
-- ============================================================================

-- Test SADD function
SELECT has_function('pgkv', 'sadd', ARRAY['text', 'text[]'], 'pgkv.sadd function should exist');

SELECT is(
    pgkv.sadd('myset', 'apple'),
    1,
    'SADD should return 1 when adding new member'
);

SELECT is(
    pgkv.sadd('myset', 'apple'),
    0,
    'SADD should return 0 when member already exists (uniqueness)'
);

-- Add multiple members
SELECT is(
    pgkv.sadd('myset', 'banana', 'cherry', 'date'),
    3,
    'SADD should return 3 when adding 3 new members'
);

-- Add with some duplicates
SELECT is(
    pgkv.sadd('myset', 'banana', 'elderberry'),
    1,
    'SADD should return 1 when only 1 of 2 members is new'
);

-- Test SCARD function
SELECT has_function('pgkv', 'scard', ARRAY['text'], 'pgkv.scard function should exist');

SELECT is(
    pgkv.scard('myset'),
    5,
    'SCARD should return 5 for set with 5 unique members'
);

SELECT is(
    pgkv.scard('nonexistent'),
    0,
    'SCARD should return 0 for nonexistent key'
);

-- Test SMEMBERS function
SELECT has_function('pgkv', 'smembers', ARRAY['text'], 'pgkv.smembers function should exist');

SELECT ok(
    (SELECT COUNT(*) FROM pgkv.smembers('myset')) = 5,
    'SMEMBERS should return 5 members'
);

SELECT ok(
    EXISTS(SELECT 1 FROM pgkv.smembers('myset') WHERE smembers = to_jsonb('apple'::text)),
    'SMEMBERS should contain apple'
);

-- Test SISMEMBER function
SELECT has_function('pgkv', 'sismember', ARRAY['text', 'text'], 'pgkv.sismember function should exist');

SELECT is(
    pgkv.sismember('myset', 'apple'),
    1,
    'SISMEMBER should return 1 for existing member'
);

SELECT is(
    pgkv.sismember('myset', 'grape'),
    0,
    'SISMEMBER should return 0 for nonexistent member'
);

SELECT is(
    pgkv.sismember('nonexistent', 'apple'),
    0,
    'SISMEMBER should return 0 for nonexistent set'
);

-- Test SREM function
SELECT has_function('pgkv', 'srem', ARRAY['text', 'text[]'], 'pgkv.srem function should exist');

SELECT is(
    pgkv.srem('myset', 'date'),
    1,
    'SREM should return 1 when removing 1 member'
);

SELECT is(
    pgkv.scard('myset'),
    4,
    'SCARD should return 4 after removing 1 member'
);

SELECT is(
    pgkv.srem('myset', 'banana', 'cherry'),
    2,
    'SREM should return 2 when removing 2 members'
);

SELECT is(
    pgkv.srem('myset', 'nonexistent'),
    0,
    'SREM should return 0 when member does not exist'
);

-- Test SINTER (Set Intersection)
SELECT has_function('pgkv', 'sinter', ARRAY['text[]'], 'pgkv.sinter function should exist');

-- Setup sets for intersection test
SELECT is(pgkv.sadd('set1', 'a', 'b', 'c'), 3, 'Setup set1');
SELECT is(pgkv.sadd('set2', 'b', 'c', 'd'), 3, 'Setup set2');
SELECT is(pgkv.sadd('set3', 'c', 'd', 'e'), 3, 'Setup set3');

SELECT results_eq(
    'SELECT pgkv.sinter(''set1'', ''set2'') ORDER BY 1',
    $$VALUES (to_jsonb('b'::text)), (to_jsonb('c'::text))$$,
    'SINTER of set1 and set2 should return {b, c}'
);

SELECT results_eq(
    'SELECT pgkv.sinter(''set1'', ''set2'', ''set3'') ORDER BY 1',
    $$VALUES (to_jsonb('c'::text))$$,
    'SINTER of set1, set2, and set3 should return {c}'
);

-- Test empty intersection
SELECT ok(
    (SELECT COUNT(*) FROM pgkv.sinter('set1', 'nonexistent')) = 0,
    'SINTER with nonexistent set should return empty'
);

-- Test SUNION (Set Union)
SELECT has_function('pgkv', 'sunion', ARRAY['text[]'], 'pgkv.sunion function should exist');

SELECT results_eq(
    'SELECT pgkv.sunion(''set1'', ''set2'') ORDER BY 1',
    $$VALUES (to_jsonb('a'::text)), (to_jsonb('b'::text)), (to_jsonb('c'::text)), (to_jsonb('d'::text))$$,
    'SUNION of set1 and set2 should return {a, b, c, d}'
);

SELECT results_eq(
    'SELECT pgkv.sunion(''set1'', ''set2'', ''set3'') ORDER BY 1',
    $$VALUES (to_jsonb('a'::text)), (to_jsonb('b'::text)), (to_jsonb('c'::text)), (to_jsonb('d'::text)), (to_jsonb('e'::text))$$,
    'SUNION of set1, set2, and set3 should return {a, b, c, d, e}'
);

-- Test SDIFF (Set Difference)
SELECT has_function('pgkv', 'sdiff', ARRAY['text[]'], 'pgkv.sdiff function should exist');

SELECT results_eq(
    'SELECT pgkv.sdiff(''set1'', ''set2'') ORDER BY 1',
    $$VALUES (to_jsonb('a'::text))$$,
    'SDIFF of set1 - set2 should return {a}'
);

SELECT results_eq(
    'SELECT pgkv.sdiff(''set1'', ''set2'', ''set3'') ORDER BY 1',
    $$VALUES (to_jsonb('a'::text))$$,
    'SDIFF of set1 - set2 - set3 should return {a}'
);

SELECT results_eq(
    'SELECT pgkv.sdiff(''set2'', ''set1'') ORDER BY 1',
    $$VALUES (to_jsonb('d'::text))$$,
    'SDIFF of set2 - set1 should return {d}'
);

-- Test empty difference
SELECT ok(
    (SELECT COUNT(*) FROM pgkv.sdiff('set1', 'set1')) = 0,
    'SDIFF of set with itself should return empty'
);

-- Test WRONGTYPE error
SELECT is(
    pgkv.set('string_key', 'value'),
    'OK',
    'SET string key for WRONGTYPE test'
);

SELECT throws_ok(
    'SELECT pgkv.sadd(''string_key'', ''member'')',
    'P0001',
    'WRONGTYPE Operation against a key holding the wrong kind of value',
    'SADD should raise WRONGTYPE for string key'
);

-- Finish tests
SELECT * FROM finish();
ROLLBACK;
