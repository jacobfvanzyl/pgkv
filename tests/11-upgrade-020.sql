-- pgkv 0.1.0 to 0.2.0 upgrade smoke test
-- Run standalone against a database with extension files installed.

\set ON_ERROR_STOP on
DROP EXTENSION IF EXISTS pgkv CASCADE;
CREATE EXTENSION pgkv VERSION '0.1.0';

SELECT CASE WHEN extversion = '0.1.0' THEN 'ok' ELSE 'bad' END AS installed_010
FROM pg_extension WHERE extname = 'pgkv';

SELECT pgkv.set('upgrade:string', 'old');
SELECT pgkv.hset('upgrade:hash', 'field', 'value');
SELECT pgkv.rpush('upgrade:list', 'a', 'b');
SELECT pgkv.sadd('upgrade:set', 'a', 'b');
SELECT pgkv.zadd('upgrade:zset', '1', 'a', '2', 'b');

ALTER EXTENSION pgkv UPDATE TO '0.2.0';

SELECT CASE WHEN extversion = '0.2.0' THEN 'ok' ELSE 'bad' END AS upgraded_020
FROM pg_extension WHERE extname = 'pgkv';

SELECT CASE WHEN pgkv.get('upgrade:string') = to_jsonb('old'::text) THEN 'ok' ELSE 'bad' END AS string_survived;
SELECT CASE WHEN pgkv.hget('upgrade:hash', 'field') = to_jsonb('value'::text) THEN 'ok' ELSE 'bad' END AS hash_survived;
SELECT CASE WHEN pgkv.llen('upgrade:list') = 2 THEN 'ok' ELSE 'bad' END AS list_survived;
SELECT CASE WHEN pgkv.scard('upgrade:set') = 2 THEN 'ok' ELSE 'bad' END AS set_survived;
SELECT CASE WHEN pgkv.zcard('upgrade:zset') = 2 THEN 'ok' ELSE 'bad' END AS zset_survived;
SELECT CASE WHEN pgkv.setnx('upgrade:new', 'v') = 1 THEN 'ok' ELSE 'bad' END AS new_function_works;
SELECT CASE WHEN pgkv.json_set('upgrade:json', NULL::text[], '{"ok":true}'::jsonb) = 'OK' THEN 'ok' ELSE 'bad' END AS json_function_works;

-- Leave the default version installed (tests/13-upgrade-030.sql checks which one that is).
DROP EXTENSION pgkv CASCADE;
CREATE EXTENSION pgkv;
