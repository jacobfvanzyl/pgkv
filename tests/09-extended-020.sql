-- pgkv 0.2.0 extended command tests

BEGIN;
CREATE EXTENSION IF NOT EXISTS pgtap;
SELECT plan(51);

SELECT pgkv.flushall();

-- SET options and string helpers
SELECT has_function('pgkv', 'set_opts', ARRAY['text','text','boolean','boolean','boolean','integer','bigint','bigint','bigint','boolean'], 'set_opts exists');
SELECT is(pgkv.setnx('nx', 'one'), 1, 'SETNX sets missing key');
SELECT is(pgkv.setnx('nx', 'two'), 0, 'SETNX does not overwrite existing key');
SELECT is(pgkv.get('nx'), to_jsonb('one'::text), 'SETNX kept original value');
SELECT is(pgkv.setex('setex', 30, 'v'), 'OK', 'SETEX returns OK');
SELECT ok(pgkv.ttl('setex') > 0, 'SETEX applies TTL');
SELECT is(pgkv.psetex('psetex', 30000, 'v'), 'OK', 'PSETEX returns OK');
SELECT ok(pgkv.ttl('psetex') > 0, 'PSETEX applies TTL');
SELECT is(pgkv.getdel('nx'), to_jsonb('one'::text), 'GETDEL returns old value');
SELECT is(pgkv.exists('nx'), 0, 'GETDEL deletes key');
SELECT is(pgkv.getex('setex', 60), to_jsonb('v'::text), 'GETEX returns value');
SELECT ok(pgkv.ttl('setex') > 30, 'GETEX updates TTL');
SELECT is(pgkv.incrbyfloat('float', 1.5), 1.5::numeric, 'INCRBYFLOAT creates numeric value');
SELECT is(pgkv.incrbyfloat('float', 2.25), 3.75::numeric, 'INCRBYFLOAT increments value');
SELECT is(pgkv.msetnx('m1','v1','m2','v2'), 1, 'MSETNX sets all missing keys');
SELECT is(pgkv.msetnx('m1','new','m3','v3'), 0, 'MSETNX fails if any key exists');
SELECT is(pgkv.exists('m3'), 0, 'MSETNX failure is atomic');
SELECT is(pgkv.set_opts('opt', 'a', false, false, false, 30, NULL, NULL, NULL, false), to_jsonb('OK'::text), 'SET_OPTS returns OK');
SELECT is(pgkv.set_opts('opt', 'b', true, false, false, NULL, NULL, NULL, NULL, false), NULL::jsonb, 'SET_OPTS NX fails on existing key');
SELECT is(pgkv.set_opts('opt', 'b', false, true, true, NULL, NULL, NULL, NULL, false), to_jsonb('a'::text), 'SET_OPTS XX GET returns old value');

-- SCAN family
SELECT pgkv.set('scan:a', '1');
SELECT pgkv.set('scan:b', '2');
SELECT results_eq('SELECT key FROM pgkv.scan(0, ''scan:*'', 10) ORDER BY key', $$VALUES ('scan:a'::text), ('scan:b'::text)$$, 'SCAN returns matching keys');
SELECT pgkv.hset('hs', 'a', '1', 'b', '2');
SELECT results_eq('SELECT field, value FROM pgkv.hscan(''hs'', 0, ''*'', 10) ORDER BY field', $$VALUES ('a'::text, to_jsonb('1'::text)), ('b'::text, to_jsonb('2'::text))$$, 'HSCAN returns fields');
SELECT pgkv.sadd('ss', 'a', 'b');
SELECT results_eq('SELECT member FROM pgkv.sscan(''ss'', 0, ''*'', 10) ORDER BY member', $$VALUES (to_jsonb('a'::text)), (to_jsonb('b'::text))$$, 'SSCAN returns members');
SELECT pgkv.zadd('zs', '1', 'a', '2', 'b');
SELECT results_eq('SELECT member, score FROM pgkv.zscan(''zs'', 0, ''*'', 10) ORDER BY member', $$VALUES ('a'::text, 1::numeric), ('b'::text, 2::numeric)$$, 'ZSCAN returns members and scores');

-- List extensions
SELECT is(pgkv.lpushx('missing-list', 'x'), 0, 'LPUSHX missing returns 0');
SELECT is(pgkv.rpush('lx', 'a', 'c'), 2, 'RPUSH list setup');
SELECT is(pgkv.lpushx('lx', 'head'), 3, 'LPUSHX existing list');
SELECT is(pgkv.rpushx('lx', 'tail'), 4, 'RPUSHX existing list');
SELECT is(pgkv.linsert('lx', 'BEFORE', 'c', 'b'), 5, 'LINSERT before pivot');
SELECT is(pgkv.lpos('lx', 'b'), 2, 'LPOS returns zero-based index');
SELECT is(pgkv.lmove('lx', 'ly', 'LEFT', 'RIGHT'), to_jsonb('head'::text), 'LMOVE moves element');
SELECT is(pgkv.rpoplpush('lx', 'ly'), to_jsonb('tail'::text), 'RPOPLPUSH moves right to left');

-- ZSET extensions
SELECT pgkv.flushall();
SELECT pgkv.zadd('z1', '1', 'a', '2', 'b', '3', 'c');
SELECT pgkv.zadd('z2', '10', 'b', '20', 'c', '30', 'd');
SELECT results_eq('SELECT member, score FROM pgkv.zmscore(''z1'', ''a'', ''x'')', $$VALUES ('a'::text, 1::numeric), ('x'::text, NULL::numeric)$$, 'ZMSCORE returns scores and NULLs');
SELECT results_eq('SELECT member, score FROM pgkv.zpopmin(''z1'', 1)', $$VALUES ('a'::text, 1::numeric)$$, 'ZPOPMIN returns min');
SELECT is(pgkv.zscore('z1', 'a'), NULL::numeric, 'ZPOPMIN removes member');
SELECT results_eq('SELECT member, score FROM pgkv.zpopmax(''z1'', 1)', $$VALUES ('c'::text, 3::numeric)$$, 'ZPOPMAX returns max');
SELECT pgkv.zadd('z1', '1', 'a', '3', 'c');
SELECT is(pgkv.zremrangebyrank('z1', 0, 0), 1, 'ZREMRANGEBYRANK removes by rank');
SELECT is(pgkv.zremrangebyscore('z1', 2, 3), 2, 'ZREMRANGEBYSCORE removes by score');
SELECT pgkv.zadd('z1', '1', 'a', '2', 'b', '3', 'c');
SELECT is(pgkv.zrangestore('zr', 'z1', 0, 1, false), 2, 'ZRANGESTORE stores range');
SELECT is(pgkv.zcard('zr'), 2, 'ZRANGESTORE result cardinality');
SELECT is(pgkv.zunionstore('zu', ARRAY['z1','z2']), 4, 'ZUNIONSTORE stores union');
SELECT is(pgkv.zinterstore('zi', ARRAY['z1','z2']), 2, 'ZINTERSTORE stores intersection');
SELECT is(pgkv.zdiffstore('zd', ARRAY['z1','z2']), 1, 'ZDIFFSTORE stores difference');

-- JSON operations
SELECT is(pgkv.json_set('doc', NULL, '{"name":"Ada","tags":["math"]}'::jsonb), 'OK', 'JSON_SET root');
SELECT is(pgkv.type('doc'), 'json', 'JSON_SET creates json type');
SELECT is(pgkv.json_get('doc', ARRAY['name']), to_jsonb('Ada'::text), 'JSON_GET path');
SELECT is(pgkv.json_type('doc', ARRAY['tags']), 'array', 'JSON_TYPE path');
SELECT results_eq('SELECT pgkv.json_objkeys(''doc'')', $$VALUES ('name'::text), ('tags'::text)$$, 'JSON_OBJKEYS returns keys');
SELECT is(pgkv.json_arrappend('doc', ARRAY['tags'], '"logic"'::jsonb), 2, 'JSON_ARRAPPEND appends');
SELECT is(pgkv.json_del('doc', ARRAY['name']), 1, 'JSON_DEL deletes path');
SELECT is(pgkv.json_get('doc', ARRAY['name']), NULL::jsonb, 'JSON_DEL path gone');

SELECT * FROM finish();
ROLLBACK;
