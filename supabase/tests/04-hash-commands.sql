-- HASH commands test suite for pgkv extension

BEGIN;

-- Load pgTAP extension
CREATE EXTENSION IF NOT EXISTS pgtap;

-- Plan the number of tests
SELECT plan(35);

-- ============================================================================
-- HASH Operations (HSET, HGET, HMGET, HGETALL, HDEL, HEXISTS, HLEN, HKEYS, HVALS, HINCRBY)
-- ============================================================================

-- Test HSET function
SELECT has_function('pgkv', 'hset', ARRAY['text', 'text[]'], 'pgkv.hset function should exist');

-- Set single field
SELECT is(
    pgkv.hset('user:1000', 'name', 'Alice'),
    1,
    'HSET should return 1 when adding new field'
);

-- Update existing field (should return 0)
SELECT is(
    pgkv.hset('user:1000', 'name', 'Bob'),
    0,
    'HSET should return 0 when updating existing field'
);

-- Set multiple fields
SELECT is(
    pgkv.hset('user:1000', 'age', '30', 'city', 'NYC'),
    2,
    'HSET should return 2 when adding 2 new fields'
);

-- Test HGET function
SELECT has_function('pgkv', 'hget', ARRAY['text', 'text'], 'pgkv.hget function should exist');

SELECT is(
    pgkv.hget('user:1000', 'name'),
    to_jsonb('Bob'::text),
    'HGET should return JSONB field value'
);

SELECT is(
    pgkv.hget('user:1000', 'nonexistent'),
    NULL::jsonb,
    'HGET should return NULL for nonexistent field'
);

SELECT is(
    pgkv.hget('nonexistent', 'field'),
    NULL::jsonb,
    'HGET should return NULL for nonexistent key'
);

-- Test HMGET function
SELECT has_function('pgkv', 'hmget', ARRAY['text', 'text[]'], 'pgkv.hmget function should exist');

SELECT results_eq(
    'SELECT field, value FROM pgkv.hmget(''user:1000'', ''name'', ''age'', ''nonexistent'') ORDER BY field',
    $$VALUES ('age'::text, to_jsonb('30'::text)), ('name'::text, to_jsonb('Bob'::text)), ('nonexistent'::text, NULL::jsonb)$$,
    'HMGET should return requested fields with NULL for missing'
);

-- Test HGETALL function
SELECT has_function('pgkv', 'hgetall', ARRAY['text'], 'pgkv.hgetall function should exist');

SELECT ok(
    (SELECT COUNT(*) FROM pgkv.hgetall('user:1000') = 3),
    'HGETALL should return all 3 fields'
);

SELECT results_eq(
    'SELECT field FROM pgkv.hgetall(''user:1000'') ORDER BY field',
    $$VALUES ('age'::text), ('city'::text), ('name'::text)$$,
    'HGETALL should return all field names'
);

-- Test HEXISTS function
SELECT has_function('pgkv', 'hexists', ARRAY['text', 'text'], 'pgkv.hexists function should exist');

SELECT is(
    pgkv.hexists('user:1000', 'name'),
    1,
    'HEXISTS should return 1 for existing field'
);

SELECT is(
    pgkv.hexists('user:1000', 'nonexistent'),
    0,
    'HEXISTS should return 0 for nonexistent field'
);

-- Test HLEN function
SELECT has_function('pgkv', 'hlen', ARRAY['text'], 'pgkv.hlen function should exist');

SELECT is(
    pgkv.hlen('user:1000'),
    3,
    'HLEN should return 3 for hash with 3 fields'
);

SELECT is(
    pgkv.hlen('nonexistent'),
    0,
    'HLEN should return 0 for nonexistent key'
);

-- Test HKEYS function
SELECT has_function('pgkv', 'hkeys', ARRAY['text'], 'pgkv.hkeys function should exist');

SELECT results_eq(
    'SELECT pgkv.hkeys(''user:1000'') ORDER BY 1',
    $$VALUES ('age'::text), ('city'::text), ('name'::text)$$,
    'HKEYS should return all field names'
);

-- Test HVALS function
SELECT has_function('pgkv', 'hvals', ARRAY['text'], 'pgkv.hvals function should exist');

SELECT ok(
    (SELECT COUNT(*) FROM pgkv.hvals('user:1000') = 3),
    'HVALS should return 3 values'
);

-- Test HDEL function
SELECT has_function('pgkv', 'hdel', ARRAY['text', 'text[]'], 'pgkv.hdel function should exist');

SELECT is(
    pgkv.hdel('user:1000', 'age'),
    1,
    'HDEL should return 1 when deleting 1 field'
);

SELECT is(
    pgkv.hlen('user:1000'),
    2,
    'HLEN should return 2 after deleting 1 field'
);

SELECT is(
    pgkv.hdel('user:1000', 'nonexistent'),
    0,
    'HDEL should return 0 when field does not exist'
);

-- Delete multiple fields
SELECT is(
    pgkv.hdel('user:1000', 'name', 'city'),
    2,
    'HDEL should return 2 when deleting 2 fields'
);

SELECT is(
    pgkv.hlen('user:1000'),
    0,
    'HLEN should return 0 after deleting all fields'
);

-- Test HINCRBY function
SELECT has_function('pgkv', 'hincrby', ARRAY['text', 'text', 'bigint'], 'pgkv.hincrby function should exist');

SELECT is(
    pgkv.hincrby('stats', 'views', 1),
    1::bigint,
    'HINCRBY should return 1 for new field'
);

SELECT is(
    pgkv.hincrby('stats', 'views', 5),
    6::bigint,
    'HINCRBY should increment to 6'
);

SELECT is(
    pgkv.hincrby('stats', 'views', -2),
    4::bigint,
    'HINCRBY should decrement to 4 with negative increment'
);

-- Test WRONGTYPE error
SELECT is(
    pgkv.set('string_key', 'value'),
    'OK',
    'SET string key for WRONGTYPE test'
);

SELECT throws_ok(
    'SELECT pgkv.hset(''string_key'', ''field'', ''value'')',
    'P0001',
    'WRONGTYPE Operation against a key holding the wrong kind of value',
    'HSET should raise WRONGTYPE for string key'
);

-- Test odd number of arguments error
SELECT throws_ok(
    'SELECT pgkv.hset(''test'', ''field1'')',
    'P0001',
    'wrong number of arguments for HSET',
    'HSET should raise exception for odd number of arguments'
);

-- Test HINCRBY on non-numeric value
SELECT is(
    pgkv.hset('hash2', 'text_field', 'not_a_number'),
    1,
    'HSET text field for HINCRBY error test'
);

SELECT throws_ok(
    'SELECT pgkv.hincrby(''hash2'', ''text_field'', 1)',
    'P0001',
    'hash value is not an integer or out of range',
    'HINCRBY should raise exception for non-numeric field'
);

-- Finish tests
SELECT * FROM finish();
ROLLBACK;
