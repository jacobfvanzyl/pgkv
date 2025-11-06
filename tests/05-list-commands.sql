-- LIST commands test suite for pgkv extension

BEGIN;

-- Load pgTAP extension
CREATE EXTENSION IF NOT EXISTS pgtap;

-- Plan the number of tests
SELECT plan(49);

-- ============================================================================
-- LIST Operations (LPUSH, RPUSH, LPOP, RPOP, LLEN, LRANGE, LINDEX, LSET, LTRIM, LREM)
-- ============================================================================

-- Test LPUSH function
SELECT has_function('pgkv', 'lpush', ARRAY['text', 'text[]'], 'pgkv.lpush function should exist');

SELECT is(
    pgkv.lpush('mylist', 'world'),
    1,
    'LPUSH should return 1 for first element'
);

SELECT is(
    pgkv.lpush('mylist', 'hello'),
    2,
    'LPUSH should return 2 after adding second element'
);

-- Test multiple values (pushed in reverse order as per Redis)
SELECT is(
    pgkv.lpush('list2', 'a', 'b', 'c'),
    3,
    'LPUSH with 3 values should return 3'
);

-- Test RPUSH function
SELECT has_function('pgkv', 'rpush', ARRAY['text', 'text[]'], 'pgkv.rpush function should exist');

SELECT is(
    pgkv.rpush('mylist', 'foo'),
    3,
    'RPUSH should return 3 after adding to list of 2'
);

SELECT is(
    pgkv.rpush('list3', 'x', 'y', 'z'),
    3,
    'RPUSH with 3 values should return 3'
);

-- Test LLEN function
SELECT has_function('pgkv', 'llen', ARRAY['text'], 'pgkv.llen function should exist');

SELECT is(
    pgkv.llen('mylist'),
    3,
    'LLEN should return 3 for list with 3 elements'
);

SELECT is(
    pgkv.llen('nonexistent'),
    0,
    'LLEN should return 0 for nonexistent key'
);

-- Test LRANGE function
SELECT has_function('pgkv', 'lrange', ARRAY['text', 'integer', 'integer'], 'pgkv.lrange function should exist');

-- Test basic range
SELECT results_eq(
    'SELECT pgkv.lrange(''mylist'', 0, -1)',
    $$VALUES (to_jsonb('hello'::text)), (to_jsonb('world'::text)), (to_jsonb('foo'::text))$$,
    'LRANGE 0 -1 should return all elements'
);

SELECT results_eq(
    'SELECT pgkv.lrange(''mylist'', 0, 1)',
    $$VALUES (to_jsonb('hello'::text)), (to_jsonb('world'::text))$$,
    'LRANGE 0 1 should return first 2 elements'
);

-- Test negative indices
SELECT results_eq(
    'SELECT pgkv.lrange(''mylist'', -2, -1)',
    $$VALUES (to_jsonb('world'::text)), (to_jsonb('foo'::text))$$,
    'LRANGE with negative indices should work'
);

-- Test LINDEX function
SELECT has_function('pgkv', 'lindex', ARRAY['text', 'integer'], 'pgkv.lindex function should exist');

SELECT is(
    pgkv.lindex('mylist', 0),
    to_jsonb('hello'::text),
    'LINDEX 0 should return first element'
);

SELECT is(
    pgkv.lindex('mylist', -1),
    to_jsonb('foo'::text),
    'LINDEX -1 should return last element'
);

SELECT is(
    pgkv.lindex('mylist', 99),
    NULL::jsonb,
    'LINDEX with out of range index should return NULL'
);

-- Test LSET function
SELECT has_function('pgkv', 'lset', ARRAY['text', 'integer', 'text'], 'pgkv.lset function should exist');

SELECT is(
    pgkv.lset('mylist', 1, 'WORLD'),
    'OK',
    'LSET should return OK'
);

SELECT is(
    pgkv.lindex('mylist', 1),
    to_jsonb('WORLD'::text),
    'LINDEX should return updated value'
);

-- Test LSET with negative index
SELECT is(
    pgkv.lset('mylist', -1, 'FOO'),
    'OK',
    'LSET with negative index should return OK'
);

SELECT is(
    pgkv.lindex('mylist', -1),
    to_jsonb('FOO'::text),
    'LINDEX should return updated value at negative index'
);

-- Test LSET out of range error
SELECT throws_ok(
    'SELECT pgkv.lset(''mylist'', 99, ''value'')',
    'P0001',
    'index out of range',
    'LSET should raise exception for out of range index'
);

-- Test LPOP function
SELECT has_function('pgkv', 'lpop', ARRAY['text', 'integer'], 'pgkv.lpop function should exist');

-- Single pop
SELECT is(
    pgkv.lpop('mylist'),
    to_jsonb('hello'::text),
    'LPOP should return and remove first element'
);

SELECT is(
    pgkv.llen('mylist'),
    2,
    'LLEN should return 2 after LPOP'
);

-- Test LPOP with count
SELECT is(
    pgkv.lpush('list4', 'a', 'b', 'c', 'd'),
    4,
    'Setup list for LPOP count test'
);

SELECT results_eq(
    'SELECT pgkv.lpop(''list4'', 2)',
    $$VALUES (to_jsonb('d'::text)), (to_jsonb('c'::text))$$,
    'LPOP with count 2 should return 2 elements'
);

-- Test RPOP function
SELECT has_function('pgkv', 'rpop', ARRAY['text', 'integer'], 'pgkv.rpop function should exist');

SELECT is(
    pgkv.rpop('mylist'),
    to_jsonb('FOO'::text),
    'RPOP should return and remove last element'
);

SELECT is(
    pgkv.llen('mylist'),
    1,
    'LLEN should return 1 after RPOP'
);

-- Test LTRIM function
SELECT has_function('pgkv', 'ltrim', ARRAY['text', 'integer', 'integer'], 'pgkv.ltrim function should exist');

SELECT is(
    pgkv.rpush('trimlist', 'a', 'b', 'c', 'd', 'e'),
    5,
    'Setup list for LTRIM test'
);

SELECT is(
    pgkv.ltrim('trimlist', 1, 3),
    'OK',
    'LTRIM should return OK'
);

SELECT results_eq(
    'SELECT pgkv.lrange(''trimlist'', 0, -1)',
    $$VALUES (to_jsonb('b'::text)), (to_jsonb('c'::text)), (to_jsonb('d'::text))$$,
    'LTRIM should keep only indices 1-3'
);

-- Test LTRIM with negative indices
SELECT is(
    pgkv.ltrim('trimlist', 0, -2),
    'OK',
    'LTRIM with negative stop should work'
);

SELECT is(
    pgkv.llen('trimlist'),
    2,
    'LLEN should return 2 after LTRIM 0 -2'
);

-- Test LREM function
SELECT has_function('pgkv', 'lrem', ARRAY['text', 'integer', 'text'], 'pgkv.lrem function should exist');

-- Setup list with duplicates
SELECT is(
    pgkv.rpush('remlist', 'a', 'b', 'a', 'c', 'a', 'd'),
    6,
    'Setup list for LREM test'
);

-- Remove first 2 'a' elements (count > 0)
SELECT is(
    pgkv.lrem('remlist', 2, 'a'),
    2,
    'LREM count=2 should remove 2 elements'
);

SELECT is(
    pgkv.llen('remlist'),
    4,
    'LLEN should return 4 after removing 2 elements'
);

-- Setup another list for count < 0 test
SELECT is(
    pgkv.rpush('remlist2', 'x', 'a', 'x', 'b', 'x'),
    5,
    'Setup list for LREM count<0 test'
);

-- Remove last 2 'x' elements (count < 0)
SELECT is(
    pgkv.lrem('remlist2', -2, 'x'),
    2,
    'LREM count=-2 should remove last 2 occurrences'
);

SELECT results_eq(
    'SELECT pgkv.lrange(''remlist2'', 0, -1)',
    $$VALUES (to_jsonb('x'::text)), (to_jsonb('a'::text)), (to_jsonb('b'::text))$$,
    'LREM count<0 should remove from tail'
);

-- Test LREM with count=0 (remove all)
SELECT is(
    pgkv.lrem('remlist2', 0, 'x'),
    1,
    'LREM count=0 should remove all occurrences'
);

SELECT is(
    pgkv.llen('remlist2'),
    2,
    'LLEN should return 2 after removing all x elements'
);

-- Test WRONGTYPE error
SELECT is(
    pgkv.set('string_key', 'value'),
    'OK',
    'SET string key for WRONGTYPE test'
);

SELECT throws_ok(
    'SELECT pgkv.lpush(''string_key'', ''value'')',
    'P0001',
    'WRONGTYPE Operation against a key holding the wrong kind of value',
    'LPUSH should raise WRONGTYPE for string key'
);

-- Finish tests
SELECT * FROM finish();
ROLLBACK;
