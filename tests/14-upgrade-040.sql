-- pgkv 0.3.0 to 0.4.0 upgrade test
-- Run standalone against a database with extension files installed. Prints 'ok' or 'bad'.
-- 0.4.0 moves collections from one JSONB value per key to one row per element, so this
-- checks that every kind of value survives the conversion.

\set ON_ERROR_STOP on
DROP EXTENSION IF EXISTS pgkv CASCADE;
CREATE EXTENSION pgkv VERSION '0.3.0';

SELECT pgkv.set('upgrade:string', 'old');
SELECT pgkv.set('upgrade:ttl', 'old', 1000);
SELECT pgkv.incr('upgrade:counter');
SELECT pgkv.hset('upgrade:hash', 'field', 'value', 'other', 'x');
SELECT pgkv.hincrby('upgrade:hash', 'n', 5);
SELECT pgkv.rpush('upgrade:list', 'a', 'b', 'a', 'c');
SELECT pgkv.sadd('upgrade:set', 'a', 'b');
-- 0.3.0 can only store infinite scores on PostgreSQL 14 and later (it parses them as numeric).
SELECT current_setting('server_version_num')::integer >= 140000 AS has_numeric_infinity \gset
\if :has_numeric_infinity
    \set top_score infinity
    \set bottom_score -infinity
\else
    \set top_score 1e300
    \set bottom_score -1e300
\endif
SELECT pgkv.zadd('upgrade:zset', '1', 'a', '2.5', 'b', :'top_score', 'top', :'bottom_score', 'bottom');
SELECT pgkv.expire('upgrade:zset', 1000);
-- 0.3.0 scores are numeric and can lie outside the range of a double.
SELECT pgkv.zadd('upgrade:range', '1e400', 'huge', '-1e400', 'neghuge', '1e-400', 'tiny', '3', 'three');
SELECT pgkv.json_set('upgrade:json', NULL::text[], '{"ok":true}'::jsonb);
-- 0.3.0 leaves an empty sorted set behind here; 0.4.0 must not carry it over.
SELECT pgkv.zadd('upgrade:emptied', '1', 'a');
SELECT * FROM pgkv.zpopmin('upgrade:emptied');
SELECT pgkv.set('upgrade:expired', 'gone', 1);
UPDATE pgkv.store SET expires_at = now() - interval '1 hour' WHERE key = 'upgrade:expired';


-- The upgrade closes pgkv: PUBLIC, open until 0.3.0, and roles granted by name lose access
-- until the owner grants it again (a grant made by the upgrade script would not be dumped).
DROP ROLE IF EXISTS pgkv_upgrade_app, pgkv_upgrade_other;
CREATE ROLE pgkv_upgrade_app;
CREATE ROLE pgkv_upgrade_other;
GRANT USAGE ON SCHEMA pgkv TO pgkv_upgrade_app;
GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA pgkv TO pgkv_upgrade_app;

ALTER EXTENSION pgkv UPDATE TO '0.4.0';

SELECT CASE WHEN NOT has_schema_privilege(r.rolname, 'pgkv', 'USAGE')
             AND (SELECT COUNT(*) FROM pg_proc p WHERE p.pronamespace = 'pgkv'::regnamespace
                  AND has_function_privilege(r.rolname, p.oid, 'EXECUTE')) = 0
            THEN 'ok' ELSE 'bad' END AS upgrade_closes_access
FROM pg_roles r WHERE r.rolname IN ('pgkv_upgrade_app', 'pgkv_upgrade_other') ORDER BY r.rolname;
SELECT pgkv.grant_access('pgkv_upgrade_app');
SELECT CASE WHEN has_schema_privilege('pgkv_upgrade_app', 'pgkv', 'USAGE')
             AND has_function_privilege('pgkv_upgrade_app', 'pgkv.call(text[])', 'EXECUTE')
             AND has_function_privilege('pgkv_upgrade_app', 'pgkv.zscore(text, text)', 'EXECUTE')
             AND NOT has_function_privilege('pgkv_upgrade_app', 'pgkv.set_unlogged(boolean)', 'EXECUTE')
            THEN 'ok' ELSE 'bad' END AS grant_access_reopens_it;
-- Nothing the upgrade did ties the role to the extension, so it can be dropped once revoked.
SELECT pgkv.revoke_access('pgkv_upgrade_app');
DROP ROLE pgkv_upgrade_app, pgkv_upgrade_other;

SELECT CASE WHEN extversion = '0.4.0' THEN 'ok' ELSE 'bad' END AS upgraded_040
FROM pg_extension WHERE extname = 'pgkv';

SELECT CASE WHEN pgkv.get('upgrade:string') = to_jsonb('old'::text) THEN 'ok' ELSE 'bad' END AS string_survived;
SELECT CASE WHEN pgkv.ttl('upgrade:ttl') > 900 THEN 'ok' ELSE 'bad' END AS ttl_survived;
SELECT CASE WHEN pgkv.incr('upgrade:counter') = 2 THEN 'ok' ELSE 'bad' END AS counter_survived;
SELECT CASE WHEN pgkv.hget('upgrade:hash', 'field') = to_jsonb('value'::text) AND pgkv.hlen('upgrade:hash') = 3
            THEN 'ok' ELSE 'bad' END AS hash_survived;
SELECT CASE WHEN pgkv.hincrby('upgrade:hash', 'n', 1) = 6 THEN 'ok' ELSE 'bad' END AS hash_number_survived;
SELECT CASE WHEN (SELECT array_agg(v #>> '{}') FROM pgkv.lrange('upgrade:list', 0, -1) v) = ARRAY['a', 'b', 'a', 'c']
            THEN 'ok' ELSE 'bad' END AS list_survived_in_order;
SELECT CASE WHEN pgkv.scard('upgrade:set') = 2 AND pgkv.sismember('upgrade:set', 'b') = 1 THEN 'ok' ELSE 'bad' END AS set_survived;
SELECT CASE WHEN (SELECT array_agg(member) FROM pgkv.zrange('upgrade:zset', 0, -1)) = ARRAY['bottom', 'a', 'b', 'top']
            THEN 'ok' ELSE 'bad' END AS zset_survived_in_order;
SELECT CASE WHEN pgkv.zscore('upgrade:zset', 'top') = :'top_score'::double precision AND pgkv.zscore('upgrade:zset', 'b') = 2.5
            THEN 'ok' ELSE 'bad' END AS zset_scores_survived;
SELECT CASE WHEN pgkv.zscore('upgrade:range', 'huge') = 'infinity' AND pgkv.zscore('upgrade:range', 'neghuge') = '-infinity'
             AND pgkv.zscore('upgrade:range', 'tiny') = 0 AND pgkv.zscore('upgrade:range', 'three') = 3
            THEN 'ok' ELSE 'bad' END AS out_of_range_scores_clamped;
SELECT CASE WHEN pgkv.ttl('upgrade:zset') > 900 THEN 'ok' ELSE 'bad' END AS collection_ttl_survived;
SELECT CASE WHEN pgkv.json_get('upgrade:json', ARRAY['ok']) = 'true'::jsonb THEN 'ok' ELSE 'bad' END AS json_survived;
SELECT CASE WHEN pgkv.exists('upgrade:emptied') = 0 AND pgkv.type('upgrade:emptied') = 'none' THEN 'ok' ELSE 'bad' END AS empty_collection_dropped;
SELECT CASE WHEN NOT EXISTS (SELECT 1 FROM pgkv.store WHERE key = 'upgrade:expired') THEN 'ok' ELSE 'bad' END AS expired_key_dropped;
SELECT CASE WHEN NOT EXISTS (SELECT 1 FROM pgkv.store WHERE id IS NOT NULL AND (value IS NOT NULL OR card IS NULL OR card < 1))
            THEN 'ok' ELSE 'bad' END AS collection_headers_consistent;
SELECT CASE WHEN (SELECT v FROM pgkv.lpop('upgrade:list') v) = to_jsonb('a'::text) AND pgkv.rpush('upgrade:list', 'z') = 4 THEN 'ok' ELSE 'bad' END AS list_writable;
SELECT CASE WHEN pgkv.zadd('upgrade:zset', '0', 'new') = 1 AND pgkv.hset('upgrade:hash', 'f2', 'v') = 1 THEN 'ok' ELSE 'bad' END AS mutators_work;
SELECT CASE WHEN pgkv.call('GET', 'upgrade:string') = to_jsonb('old'::text) THEN 'ok' ELSE 'bad' END AS call_works;
SELECT CASE WHEN (SELECT COUNT(*) FROM pgkv.scan_prefix('upgrade:', NULL, 100)) = 9 THEN 'ok' ELSE 'bad' END AS scan_prefix_works;

-- An upgraded install must be the same as a fresh one: functions and their privileges,
-- tables, columns, indexes, constraints, sequences, and what pg_dump is told to dump.
CREATE OR REPLACE FUNCTION pg_temp.pgkv_catalog()
RETURNS TABLE(kind TEXT, name TEXT, def TEXT)
LANGUAGE sql
AS $$
    SELECT 'function', p.oid::regprocedure::text,
           md5(pg_get_functiondef(p.oid)) || ' ' || (
               SELECT string_agg(a::text, ',' ORDER BY a::text)
               FROM unnest(COALESCE(p.proacl, acldefault('f', p.proowner))) a
           )
    FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace
    WHERE n.nspname = 'pgkv' AND p.prokind = 'f'
    UNION ALL
    SELECT 'aggregate', p.oid::regprocedure::text, a.aggtransfn::text || ' ' || a.aggtranstype::regtype::text
    FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace JOIN pg_aggregate a ON a.aggfnoid = p.oid
    WHERE n.nspname = 'pgkv'
    UNION ALL
    SELECT 'index', indexname::text, indexdef FROM pg_indexes WHERE schemaname = 'pgkv'
    UNION ALL
    SELECT 'constraint', c.conrelid::regclass::text || '.' || c.conname, pg_get_constraintdef(c.oid)
    FROM pg_constraint c JOIN pg_class r ON r.oid = c.conrelid JOIN pg_namespace n ON n.oid = r.relnamespace
    WHERE n.nspname = 'pgkv'
    UNION ALL
    SELECT 'column', a.attrelid::regclass::text || '.' || a.attname,
           format_type(a.atttypid, a.atttypmod) || ' notnull=' || a.attnotnull || ' collation=' ||
           COALESCE((SELECT c.collname::text FROM pg_collation c WHERE c.oid = a.attcollation), '') || ' default=' ||
           COALESCE((SELECT pg_get_expr(d.adbin, d.adrelid) FROM pg_attrdef d WHERE d.adrelid = a.attrelid AND d.adnum = a.attnum), '')
    FROM pg_attribute a JOIN pg_class r ON r.oid = a.attrelid JOIN pg_namespace n ON n.oid = r.relnamespace
    WHERE n.nspname = 'pgkv' AND r.relkind = 'r' AND a.attnum > 0 AND NOT a.attisdropped
    UNION ALL
    SELECT 'relation', r.oid::regclass::text,
           r.relkind::text || ' ' || COALESCE(array_to_string(r.reloptions, ','), '') || ' ' ||
           COALESCE((SELECT string_agg(a::text, ',' ORDER BY a::text) FROM unnest(r.relacl) a), '')
    FROM pg_class r JOIN pg_namespace n ON n.oid = r.relnamespace
    WHERE n.nspname = 'pgkv' AND r.relkind IN ('r', 'S')
    UNION ALL
    SELECT 'dumped', c::regclass::text, '' FROM pg_extension e CROSS JOIN LATERAL unnest(e.extconfig) AS c
    WHERE e.extname = 'pgkv'
$$;

CREATE TEMP TABLE pgkv_upgraded AS SELECT * FROM pg_temp.pgkv_catalog();

DROP EXTENSION pgkv CASCADE;
CREATE EXTENSION pgkv VERSION '0.4.0';

CREATE TEMP TABLE pgkv_fresh AS SELECT * FROM pg_temp.pgkv_catalog();

SELECT CASE WHEN COUNT(*) = 0 THEN 'ok' ELSE 'bad: ' || string_agg(kind || ' ' || name, ', ') END AS upgrade_equals_fresh_install
FROM (
    (TABLE pgkv_upgraded EXCEPT TABLE pgkv_fresh)
    UNION ALL
    (TABLE pgkv_fresh EXCEPT TABLE pgkv_upgraded)
) diff;

SELECT CASE WHEN (SELECT COUNT(*) FROM pgkv_fresh WHERE kind = 'dumped') = 6 THEN 'ok' ELSE 'bad' END AS data_tables_are_dumped;

DROP TABLE pgkv_upgraded, pgkv_fresh;

-- The whole chain from 0.1.0 also lands on 0.4.0, and that is the default version.
DROP EXTENSION pgkv CASCADE;
CREATE EXTENSION pgkv VERSION '0.1.0';
SELECT pgkv.set('chain', 'v');
SELECT pgkv.rpush('chain:list', 'a', 'b');
ALTER EXTENSION pgkv UPDATE;
SELECT CASE WHEN extversion = '0.4.0' AND pgkv.cas('chain', 'v', 'w') = 1 AND pgkv.llen('chain:list') = 2
            THEN 'ok' ELSE 'bad' END AS chain_010_to_040
FROM pg_extension WHERE extname = 'pgkv';

DROP EXTENSION pgkv CASCADE;
CREATE EXTENSION pgkv;
SELECT CASE WHEN extversion = '0.4.0' THEN 'ok' ELSE 'bad' END AS default_install_040
FROM pg_extension WHERE extname = 'pgkv';
