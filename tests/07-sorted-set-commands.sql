-- SORTED SET commands test suite for pgkv extension

BEGIN;

-- Load pgTAP extension
CREATE EXTENSION IF NOT EXISTS pgtap;

-- Plan the number of tests
SELECT plan(53);

-- ============================================================================
-- SORTED SET Operations (ZADD, ZREM, ZRANGE, ZREVRANGE, ZSCORE, ZCARD, ZRANK, ZREVRANK, ZRANGEBYSCORE, ZINCRBY, ZCOUNT)
-- ============================================================================

-- Test ZADD function
SELECT has_function('pgkv', 'zadd', ARRAY['text', 'text[]'], 'pgkv.zadd function should exist');

SELECT is(
    pgkv.zadd('leaderboard', '100', 'alice'),
    1,
    'ZADD should return 1 when adding new member'
);

SELECT is(
    pgkv.zadd('leaderboard', '150', 'alice'),
    0,
    'ZADD should return 0 when updating existing member score'
);

-- Add multiple members
SELECT is(
    pgkv.zadd('leaderboard', '200', 'bob', '150', 'charlie', '300', 'diana'),
    3,
    'ZADD should return 3 when adding 3 new members'
);

-- Test ZCARD function
SELECT has_function('pgkv', 'zcard', ARRAY['text'], 'pgkv.zcard function should exist');

SELECT is(
    pgkv.zcard('leaderboard'),
    4,
    'ZCARD should return 4 for sorted set with 4 members'
);

SELECT is(
    pgkv.zcard('nonexistent'),
    0,
    'ZCARD should return 0 for nonexistent key'
);

-- Test ZSCORE function
SELECT has_function('pgkv', 'zscore', ARRAY['text', 'text'], 'pgkv.zscore function should exist');

SELECT is(
    pgkv.zscore('leaderboard', 'alice'),
    150::numeric,
    'ZSCORE should return 150 for alice (updated score)'
);

SELECT is(
    pgkv.zscore('leaderboard', 'bob'),
    200::numeric,
    'ZSCORE should return 200 for bob'
);

SELECT is(
    pgkv.zscore('leaderboard', 'nonexistent'),
    NULL::numeric,
    'ZSCORE should return NULL for nonexistent member'
);

-- Test ZRANGE function (ascending order)
SELECT has_function('pgkv', 'zrange', ARRAY['text', 'integer', 'integer', 'boolean'], 'pgkv.zrange function should exist');

-- Without scores
SELECT results_eq(
    'SELECT member FROM pgkv.zrange(''leaderboard'', 0, -1, false) ORDER BY member',
    $$VALUES ('alice'::text), ('bob'::text), ('charlie'::text), ('diana'::text)$$,
    'ZRANGE 0 -1 should return all members in score order'
);

-- Check order by verifying first and last
SELECT is(
    (SELECT member FROM pgkv.zrange('leaderboard', 0, 0, false)),
    'alice',
    'ZRANGE 0 0 should return member with lowest score (alice=150)'
);

SELECT is(
    (SELECT member FROM pgkv.zrange('leaderboard', -1, -1, false)),
    'diana',
    'ZRANGE -1 -1 should return member with highest score (diana=300)'
);

-- With scores
SELECT ok(
    EXISTS(
        SELECT 1 FROM pgkv.zrange('leaderboard', 0, 0, true)
        WHERE member = 'alice' AND score = 150
    ),
    'ZRANGE with WITHSCORES should return alice with score 150'
);

-- Test range
SELECT is(
    (SELECT COUNT(*)::int FROM pgkv.zrange('leaderboard', 1, 2, false)),
    2,
    'ZRANGE 1 2 should return 2 members'
);

-- Test ZREVRANGE function (descending order)
SELECT has_function('pgkv', 'zrevrange', ARRAY['text', 'integer', 'integer', 'boolean'], 'pgkv.zrevrange function should exist');

SELECT is(
    (SELECT member FROM pgkv.zrevrange('leaderboard', 0, 0, false)),
    'diana',
    'ZREVRANGE 0 0 should return member with highest score (diana=300)'
);

SELECT is(
    (SELECT member FROM pgkv.zrevrange('leaderboard', -1, -1, false)),
    'alice',
    'ZREVRANGE -1 -1 should return member with lowest score (alice=150)'
);

-- With scores
SELECT ok(
    EXISTS(
        SELECT 1 FROM pgkv.zrevrange('leaderboard', 0, 1, true)
        WHERE member = 'diana' AND score = 300
    ),
    'ZREVRANGE with WITHSCORES should return diana with score 300'
);

-- Test ZRANK function (ascending rank, 0-based)
SELECT has_function('pgkv', 'zrank', ARRAY['text', 'text'], 'pgkv.zrank function should exist');

SELECT is(
    pgkv.zrank('leaderboard', 'alice'),
    0,
    'ZRANK should return 0 for alice (lowest score)'
);

SELECT is(
    pgkv.zrank('leaderboard', 'diana'),
    3,
    'ZRANK should return 3 for diana (highest score)'
);

SELECT is(
    pgkv.zrank('leaderboard', 'nonexistent'),
    NULL::integer,
    'ZRANK should return NULL for nonexistent member'
);

-- Test ZREVRANK function (descending rank, 0-based)
SELECT has_function('pgkv', 'zrevrank', ARRAY['text', 'text'], 'pgkv.zrevrank function should exist');

SELECT is(
    pgkv.zrevrank('leaderboard', 'diana'),
    0,
    'ZREVRANK should return 0 for diana (highest score)'
);

SELECT is(
    pgkv.zrevrank('leaderboard', 'alice'),
    3,
    'ZREVRANK should return 3 for alice (lowest score)'
);

SELECT is(
    pgkv.zrevrank('leaderboard', 'nonexistent'),
    NULL::integer,
    'ZREVRANK should return NULL for nonexistent member'
);

-- Test ZRANGEBYSCORE function
SELECT has_function('pgkv', 'zrangebyscore', ARRAY['text', 'numeric', 'numeric', 'boolean'], 'pgkv.zrangebyscore function should exist');

SELECT results_eq(
    'SELECT member FROM pgkv.zrangebyscore(''leaderboard'', 150, 200, false) ORDER BY member',
    $$VALUES ('alice'::text), ('bob'::text), ('charlie'::text)$$,
    'ZRANGEBYSCORE 150-200 should return alice, charlie, bob'
);

SELECT is(
    (SELECT COUNT(*)::int FROM pgkv.zrangebyscore('leaderboard', 100, 140, false)),
    0,
    'ZRANGEBYSCORE with no matches should return empty'
);

-- With scores
SELECT ok(
    EXISTS(
        SELECT 1 FROM pgkv.zrangebyscore('leaderboard', 200, 300, true)
        WHERE member = 'bob' AND score = 200
    ),
    'ZRANGEBYSCORE with WITHSCORES should include scores'
);

-- Test ZCOUNT function
SELECT has_function('pgkv', 'zcount', ARRAY['text', 'numeric', 'numeric'], 'pgkv.zcount function should exist');

SELECT is(
    pgkv.zcount('leaderboard', 150, 200),
    3,
    'ZCOUNT 150-200 should return 3'
);

SELECT is(
    pgkv.zcount('leaderboard', 100, 140),
    0,
    'ZCOUNT with no matches should return 0'
);

SELECT is(
    pgkv.zcount('leaderboard', 0, 1000),
    4,
    'ZCOUNT 0-1000 should return 4 (all members)'
);

-- Test ZINCRBY function
SELECT has_function('pgkv', 'zincrby', ARRAY['text', 'numeric', 'text'], 'pgkv.zincrby function should exist');

SELECT is(
    pgkv.zincrby('leaderboard', 50, 'alice'),
    200::numeric,
    'ZINCRBY should increment alice score to 200'
);

SELECT is(
    pgkv.zscore('leaderboard', 'alice'),
    200::numeric,
    'ZSCORE should confirm alice score is now 200'
);

-- Test ZINCRBY on new member
SELECT is(
    pgkv.zincrby('leaderboard', 175, 'eve'),
    175::numeric,
    'ZINCRBY on new member should set score to increment value'
);

SELECT is(
    pgkv.zcard('leaderboard'),
    5,
    'ZCARD should return 5 after ZINCRBY added new member'
);

-- Test negative increment
SELECT is(
    pgkv.zincrby('leaderboard', -25, 'bob'),
    175::numeric,
    'ZINCRBY with negative value should decrement score'
);

-- Test ZREM function
SELECT has_function('pgkv', 'zrem', ARRAY['text', 'text[]'], 'pgkv.zrem function should exist');

SELECT is(
    pgkv.zrem('leaderboard', 'eve'),
    1,
    'ZREM should return 1 when removing 1 member'
);

SELECT is(
    pgkv.zcard('leaderboard'),
    4,
    'ZCARD should return 4 after removing 1 member'
);

SELECT is(
    pgkv.zrem('leaderboard', 'alice', 'bob'),
    2,
    'ZREM should return 2 when removing 2 members'
);

SELECT is(
    pgkv.zrem('leaderboard', 'nonexistent'),
    0,
    'ZREM should return 0 when member does not exist'
);

-- Test score ordering with ties (members with same score should be ordered lexicographically)
SELECT is(
    pgkv.zadd('scores', '100', 'zebra', '100', 'apple', '100', 'middle'),
    3,
    'Setup sorted set with tied scores'
);

SELECT results_eq(
    'SELECT member FROM pgkv.zrange(''scores'', 0, -1, false)',
    $$VALUES ('apple'::text), ('middle'::text), ('zebra'::text)$$,
    'ZRANGE should order tied scores lexicographically'
);

-- Test WRONGTYPE error
SELECT is(
    pgkv.set('string_key', 'value'),
    'OK',
    'SET string key for WRONGTYPE test'
);

SELECT throws_ok(
    'SELECT pgkv.zadd(''string_key'', ''100'', ''member'')',
    'P0001',
    'WRONGTYPE Operation against a key holding the wrong kind of value',
    'ZADD should raise WRONGTYPE for string key'
);

-- Test ZADD with odd number of arguments error
SELECT throws_ok(
    'SELECT pgkv.zadd(''test'', ''100'')',
    'P0001',
    'wrong number of arguments for ZADD',
    'ZADD should raise exception for odd number of arguments'
);

-- Finish tests
SELECT * FROM finish();
ROLLBACK;
