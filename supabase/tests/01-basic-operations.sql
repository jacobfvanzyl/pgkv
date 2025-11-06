-- Basic operations test suite for pgkv extension

BEGIN;

-- Load pgTAP extension
CREATE EXTENSION IF NOT EXISTS pgtap;

-- Plan the number of tests
SELECT plan(27);

-- Test extension exists
SELECT has_extension('pgkv', 'pgkv extension should be installed');

-- Test schema exists
SELECT has_schema('pgkv', 'pgkv schema should exist');

-- Test table exists
SELECT has_table('pgkv', 'store', 'pgkv.store table should exist');

-- Test SET function
SELECT has_function('pgkv', 'set', ARRAY['text', 'text', 'integer'], 'pgkv.set function should exist');

SELECT is(
    pgkv.set('test_key', 'test_value'),
    'OK',
    'SET should return OK'
);

-- Test GET function (returns JSONB)
SELECT has_function('pgkv', 'get', ARRAY['text'], 'pgkv.get function should exist');

SELECT is(
    pgkv.get('test_key'),
    to_jsonb('test_value'::text),
    'GET should return the correct JSONB value'
);

SELECT is(
    pgkv.get('nonexistent_key'),
    NULL::jsonb,
    'GET should return NULL for nonexistent key'
);

-- Test overwriting a key
SELECT is(
    pgkv.set('test_key', 'new_value'),
    'OK',
    'SET should overwrite existing key'
);

SELECT is(
    pgkv.get('test_key'),
    to_jsonb('new_value'::text),
    'GET should return the updated JSONB value'
);

-- Test EXISTS function
SELECT has_function('pgkv', 'exists', ARRAY['text[]'], 'pgkv.exists function should exist');

SELECT is(
    pgkv.exists('test_key'),
    1,
    'EXISTS should return 1 for existing key'
);

SELECT is(
    pgkv.exists('nonexistent_key'),
    0,
    'EXISTS should return 0 for nonexistent key'
);

SELECT is(
    pgkv.exists('test_key', 'nonexistent_key'),
    1,
    'EXISTS should count only existing keys'
);

-- Test DEL function
SELECT has_function('pgkv', 'del', ARRAY['text[]'], 'pgkv.del function should exist');

SELECT is(
    pgkv.del('test_key'),
    1,
    'DEL should return 1 when deleting existing key'
);

SELECT is(
    pgkv.get('test_key'),
    NULL::jsonb,
    'GET should return NULL after key is deleted'
);

SELECT is(
    pgkv.del('nonexistent_key'),
    0,
    'DEL should return 0 for nonexistent key'
);

-- Test multi-key deletion
SELECT is(
    pgkv.set('key1', 'value1'),
    'OK',
    'SET key1'
);

SELECT is(
    pgkv.set('key2', 'value2'),
    'OK',
    'SET key2'
);

SELECT is(
    pgkv.del('key1', 'key2'),
    2,
    'DEL should delete multiple keys'
);

-- Test TYPE function
SELECT has_function('pgkv', 'type', ARRAY['text'], 'pgkv.type function should exist');

SELECT is(
    pgkv.set('type_test', 'value'),
    'OK',
    'SET type_test key'
);

SELECT is(
    pgkv.type('type_test'),
    'string',
    'TYPE should return string for string keys'
);

SELECT is(
    pgkv.type('nonexistent'),
    'none',
    'TYPE should return none for nonexistent keys'
);

-- Test JSONB storage
SELECT is(
    pgkv.set('jsonb_test', '{"name":"alice","age":30}'),
    'OK',
    'SET should accept JSON string'
);

SELECT is(
    (pgkv.get('jsonb_test')#>>'{}')::jsonb->>'name',
    'alice',
    'Should be able to extract JSON fields from JSONB value'
);

-- Finish tests
SELECT * FROM finish();
ROLLBACK;
