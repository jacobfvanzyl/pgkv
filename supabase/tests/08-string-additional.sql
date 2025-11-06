-- Additional String commands test suite for pgkv extension

BEGIN;

-- Load pgTAP extension
CREATE EXTENSION IF NOT EXISTS pgtap;

-- Plan the number of tests
SELECT plan(30);

-- ============================================================================
-- Additional String Operations (APPEND, STRLEN, GETRANGE, SETRANGE)
-- ============================================================================

-- Test APPEND function
SELECT has_function('pgkv', 'append', ARRAY['text', 'text'], 'pgkv.append function should exist');

-- Append to new key
SELECT is(
    pgkv.append('mykey', 'Hello'),
    5,
    'APPEND to new key should return length 5'
);

SELECT is(
    pgkv.get('mykey'),
    to_jsonb('Hello'::text),
    'GET should return JSONB "Hello"'
);

-- Append to existing key
SELECT is(
    pgkv.append('mykey', ' World'),
    11,
    'APPEND should return length 11 after appending " World"'
);

SELECT is(
    pgkv.get('mykey'),
    to_jsonb('Hello World'::text),
    'GET should return JSONB "Hello World"'
);

-- Append empty string
SELECT is(
    pgkv.append('mykey', ''),
    11,
    'APPEND empty string should return same length'
);

-- Test STRLEN function
SELECT has_function('pgkv', 'strlen', ARRAY['text'], 'pgkv.strlen function should exist');

SELECT is(
    pgkv.strlen('mykey'),
    11,
    'STRLEN should return 11 for "Hello World"'
);

SELECT is(
    pgkv.strlen('nonexistent'),
    0,
    'STRLEN should return 0 for nonexistent key'
);

-- Test STRLEN with numeric value
SELECT is(
    pgkv.set('numkey', '12345'),
    'OK',
    'SET numeric string'
);

SELECT is(
    pgkv.strlen('numkey'),
    5,
    'STRLEN should return 5 for "12345"'
);

-- Test GETRANGE function
SELECT has_function('pgkv', 'getrange', ARRAY['text', 'integer', 'integer'], 'pgkv.getrange function should exist');

SELECT is(
    pgkv.set('rangekey', 'This is a string'),
    'OK',
    'SET string for GETRANGE tests'
);

-- Basic range
SELECT is(
    pgkv.getrange('rangekey', 0, 3),
    'This',
    'GETRANGE 0 3 should return "This"'
);

SELECT is(
    pgkv.getrange('rangekey', 5, 6),
    'is',
    'GETRANGE 5 6 should return "is"'
);

-- Get all
SELECT is(
    pgkv.getrange('rangekey', 0, -1),
    'This is a string',
    'GETRANGE 0 -1 should return entire string'
);

-- Negative indices
SELECT is(
    pgkv.getrange('rangekey', -6, -1),
    'string',
    'GETRANGE -6 -1 should return "string"'
);

SELECT is(
    pgkv.getrange('rangekey', -3, -1),
    'ing',
    'GETRANGE -3 -1 should return "ing"'
);

-- Out of range
SELECT is(
    pgkv.getrange('rangekey', 0, 100),
    'This is a string',
    'GETRANGE with end beyond string should return entire string'
);

-- Invalid range
SELECT is(
    pgkv.getrange('rangekey', 5, 2),
    '',
    'GETRANGE with start > end should return empty string'
);

-- Nonexistent key
SELECT is(
    pgkv.getrange('nonexistent', 0, 5),
    '',
    'GETRANGE on nonexistent key should return empty string'
);

-- Test SETRANGE function
SELECT has_function('pgkv', 'setrange', ARRAY['text', 'integer', 'text'], 'pgkv.setrange function should exist');

SELECT is(
    pgkv.set('setkey', 'Hello World'),
    'OK',
    'SET string for SETRANGE tests'
);

-- Overwrite at beginning
SELECT is(
    pgkv.setrange('setkey', 0, 'Jello'),
    11,
    'SETRANGE at offset 0 should return length 11'
);

SELECT is(
    pgkv.get('setkey'),
    to_jsonb('Jello World'::text),
    'GET should return "Jello World"'
);

-- Overwrite in middle
SELECT is(
    pgkv.setrange('setkey', 6, 'Postgres'),
    14,
    'SETRANGE in middle should return new length'
);

SELECT is(
    pgkv.get('setkey'),
    to_jsonb('Jello Postgres'::text),
    'GET should return "Jello Postgres"'
);

-- SETRANGE on new key
SELECT is(
    pgkv.setrange('newkey', 0, 'Test'),
    4,
    'SETRANGE on new key should return length 4'
);

SELECT is(
    pgkv.get('newkey'),
    to_jsonb('Test'::text),
    'GET should return "Test"'
);

-- Test WRONGTYPE errors
SELECT is(
    pgkv.hset('hash_key', 'field', 'value'),
    1,
    'Create hash for WRONGTYPE test'
);

SELECT throws_ok(
    'SELECT pgkv.append(''hash_key'', ''value'')',
    'P0001',
    'WRONGTYPE Operation against a key holding the wrong kind of value',
    'APPEND should raise WRONGTYPE for hash key'
);

SELECT throws_ok(
    'SELECT pgkv.strlen(''hash_key'')',
    'P0001',
    'WRONGTYPE Operation against a key holding the wrong kind of value',
    'STRLEN should raise WRONGTYPE for hash key'
);

-- Test SETRANGE offset error
SELECT throws_ok(
    'SELECT pgkv.setrange(''test'', -1, ''value'')',
    'P0001',
    'offset is out of range',
    'SETRANGE should raise exception for negative offset'
);

-- Finish tests
SELECT * FROM finish();
ROLLBACK;
