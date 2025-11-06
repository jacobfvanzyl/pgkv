-- TTL functionality test suite for pgkv extension

BEGIN;

-- Load pgTAP extension
CREATE EXTENSION IF NOT EXISTS pgtap;

-- Plan the number of tests
SELECT plan(15);

-- Test SET with TTL
SELECT is(
    pgkv.set('ttl_key', 'ttl_value', 3600),
    'OK',
    'SET with TTL should return OK'
);

SELECT is(
    pgkv.get('ttl_key'),
    to_jsonb('ttl_value'::text),
    'GET should return JSONB value for key with TTL'
);

-- Test TTL function exists
SELECT has_function('pgkv', 'ttl', ARRAY['text'], 'pgkv.ttl function should exist');

-- Test TTL returns positive value
SELECT ok(
    pgkv.ttl('ttl_key') > 0,
    'TTL should return positive seconds for key with expiration'
);

SELECT ok(
    pgkv.ttl('ttl_key') <= 3600,
    'TTL should not exceed set value'
);

-- Test TTL for key without expiration
SELECT is(
    pgkv.set('no_ttl_key', 'value'),
    'OK',
    'SET key without TTL'
);

SELECT is(
    pgkv.ttl('no_ttl_key'),
    -1,
    'TTL should return -1 for key without expiration'
);

-- Test TTL for nonexistent key
SELECT is(
    pgkv.ttl('nonexistent_key'),
    -2,
    'TTL should return -2 for nonexistent key'
);

-- Test EXPIRE function
SELECT has_function('pgkv', 'expire', ARRAY['text', 'integer'], 'pgkv.expire function should exist');

SELECT is(
    pgkv.expire('no_ttl_key', 1800),
    1,
    'EXPIRE should return 1 when setting TTL on existing key'
);

SELECT ok(
    pgkv.ttl('no_ttl_key') > 0,
    'TTL should return positive value after EXPIRE'
);

SELECT ok(
    pgkv.ttl('no_ttl_key') <= 1800,
    'TTL should not exceed EXPIRE value'
);

-- Test EXPIRE on nonexistent key
SELECT is(
    pgkv.expire('nonexistent_key', 60),
    0,
    'EXPIRE should return 0 for nonexistent key'
);

-- Test expired key cleanup (set very short TTL and wait)
SELECT is(
    pgkv.set('short_ttl', 'value', 1),
    'OK',
    'SET key with 1 second TTL'
);

-- Wait 3 seconds for expiration (buffer for timing)
SELECT pg_sleep(3);

SELECT is(
    pgkv.get('short_ttl'),
    NULL::jsonb,
    'GET should return NULL for expired key'
);

-- Finish tests
SELECT * FROM finish();
ROLLBACK;
