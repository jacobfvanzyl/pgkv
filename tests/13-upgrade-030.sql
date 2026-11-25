-- pgkv 0.2.0 to 0.3.0 upgrade test
-- Run standalone against a database with extension files installed. Prints 'ok' or 'bad'.

\set ON_ERROR_STOP on
DROP EXTENSION IF EXISTS pgkv CASCADE;
CREATE EXTENSION pgkv VERSION '0.2.0';

SELECT pgkv.set('upgrade:string', 'old');
SELECT pgkv.set('upgrade:ttl', 'old', 1000);
SELECT pgkv.hset('upgrade:hash', 'field', 'value');
SELECT pgkv.rpush('upgrade:list', 'a', 'b');
SELECT pgkv.sadd('upgrade:set', 'a', 'b');
SELECT pgkv.zadd('upgrade:zset', '1', 'a', '2', 'b');
SELECT pgkv.json_set('upgrade:json', NULL::text[], '{"ok":true}'::jsonb);

ALTER EXTENSION pgkv UPDATE TO '0.3.0';

SELECT CASE WHEN extversion = '0.3.0' THEN 'ok' ELSE 'bad' END AS upgraded_030
FROM pg_extension WHERE extname = 'pgkv';

SELECT CASE WHEN pgkv.get('upgrade:string') = to_jsonb('old'::text) THEN 'ok' ELSE 'bad' END AS string_survived;
SELECT CASE WHEN pgkv.ttl('upgrade:ttl') > 900 THEN 'ok' ELSE 'bad' END AS ttl_survived;
SELECT CASE WHEN pgkv.hget('upgrade:hash', 'field') = to_jsonb('value'::text) THEN 'ok' ELSE 'bad' END AS hash_survived;
SELECT CASE WHEN pgkv.llen('upgrade:list') = 2 THEN 'ok' ELSE 'bad' END AS list_survived;
SELECT CASE WHEN pgkv.scard('upgrade:set') = 2 THEN 'ok' ELSE 'bad' END AS set_survived;
SELECT CASE WHEN pgkv.zcard('upgrade:zset') = 2 THEN 'ok' ELSE 'bad' END AS zset_survived;
SELECT CASE WHEN pgkv.json_get('upgrade:json', ARRAY['ok']) = 'true'::jsonb THEN 'ok' ELSE 'bad' END AS json_survived;
SELECT CASE WHEN pgkv.cas('upgrade:string', 'old', 'new') = 1 THEN 'ok' ELSE 'bad' END AS cas_works;
SELECT CASE WHEN pgkv.hset('upgrade:hash', 'field2', 'value') = 1 THEN 'ok' ELSE 'bad' END AS mutator_works;
SELECT CASE WHEN (SELECT COUNT(*) FROM pgkv.scan_prefix('upgrade:', NULL, 100)) = 7 THEN 'ok' ELSE 'bad' END AS scan_prefix_works;

-- An upgraded install must be the same as a fresh one: functions, their privileges, indexes.
CREATE TEMP TABLE pgkv_upgraded AS
SELECT 'function' AS kind, p.oid::regprocedure::text AS name,
       md5(pg_get_functiondef(p.oid)) || ' ' || (
           SELECT string_agg(a::text, ',' ORDER BY a::text)
           FROM unnest(COALESCE(p.proacl, acldefault('f', p.proowner))) a
       ) AS def
FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace WHERE n.nspname = 'pgkv'
UNION ALL
SELECT 'index', indexname, indexdef FROM pg_indexes WHERE schemaname = 'pgkv'
UNION ALL
SELECT 'constraint', conname, pg_get_constraintdef(oid) FROM pg_constraint WHERE conrelid = 'pgkv.store'::regclass;

DROP EXTENSION pgkv CASCADE;
CREATE EXTENSION pgkv VERSION '0.3.0';

SELECT CASE WHEN extversion = '0.3.0' THEN 'ok' ELSE 'bad' END AS fresh_install_030
FROM pg_extension WHERE extname = 'pgkv';

CREATE TEMP TABLE pgkv_fresh AS
SELECT 'function' AS kind, p.oid::regprocedure::text AS name,
       md5(pg_get_functiondef(p.oid)) || ' ' || (
           SELECT string_agg(a::text, ',' ORDER BY a::text)
           FROM unnest(COALESCE(p.proacl, acldefault('f', p.proowner))) a
       ) AS def
FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace WHERE n.nspname = 'pgkv'
UNION ALL
SELECT 'index', indexname, indexdef FROM pg_indexes WHERE schemaname = 'pgkv'
UNION ALL
SELECT 'constraint', conname, pg_get_constraintdef(oid) FROM pg_constraint WHERE conrelid = 'pgkv.store'::regclass;

SELECT CASE WHEN COUNT(*) = 0 THEN 'ok' ELSE 'bad: ' || string_agg(kind || ' ' || name, ', ') END AS upgrade_equals_fresh_install
FROM (
    (TABLE pgkv_upgraded EXCEPT TABLE pgkv_fresh)
    UNION ALL
    (TABLE pgkv_fresh EXCEPT TABLE pgkv_upgraded)
) diff;

DROP TABLE pgkv_upgraded, pgkv_fresh;

-- The whole chain from 0.1.0 also lands on 0.3.0.
DROP EXTENSION pgkv CASCADE;
CREATE EXTENSION pgkv VERSION '0.1.0';
SELECT pgkv.set('chain', 'v');
ALTER EXTENSION pgkv UPDATE TO '0.3.0';
SELECT CASE WHEN extversion = '0.3.0' AND pgkv.cas('chain', 'v', 'w') = 1 THEN 'ok' ELSE 'bad' END AS chain_010_to_030
FROM pg_extension WHERE extname = 'pgkv';

DROP EXTENSION pgkv CASCADE;
CREATE EXTENSION pgkv;
