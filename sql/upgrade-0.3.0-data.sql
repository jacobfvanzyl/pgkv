UPDATE pgkv.store SET id = nextval('pgkv.collection_id')
WHERE type IN ('hash', 'list', 'set', 'zset');

INSERT INTO pgkv.hash_fields (id, field, value)
SELECT s.id, kv.key, kv.value
FROM pgkv.store s CROSS JOIN LATERAL jsonb_each(s.value) kv
WHERE s.type = 'hash' AND jsonb_typeof(s.value) = 'object';

INSERT INTO pgkv.list_items (id, pos, value)
SELECT s.id, e.ord - 1, e.v #>> '{}'
FROM pgkv.store s CROSS JOIN LATERAL jsonb_array_elements(s.value) WITH ORDINALITY e(v, ord)
WHERE s.type = 'list' AND jsonb_typeof(s.value) = 'array';

INSERT INTO pgkv.set_members (id, member)
SELECT DISTINCT s.id, e.v #>> '{}'
FROM pgkv.store s CROSS JOIN LATERAL jsonb_array_elements(s.value) e(v)
WHERE s.type = 'set' AND jsonb_typeof(s.value) = 'array';

-- 0.3.0 stored infinite scores as the JSON strings "Infinity" and "-Infinity"; a NaN
-- score, which it accepted by mistake, has no place in a sorted set and is dropped. Its
-- scores were numeric, so one can lie outside the range of a double: such a score becomes
-- the nearest double there is, an infinity or zero, rather than failing the upgrade.
INSERT INTO pgkv.zset_members (id, member, score)
SELECT s.id, kv.key,
       CASE WHEN t.v IN ('Infinity', '-Infinity') THEN t.v::double precision
            WHEN abs(t.v::numeric) > 1.7976931348623157e308 THEN sign(t.v::numeric) * 'infinity'::double precision
            WHEN abs(t.v::numeric) < 4.9406564584124654e-324 THEN 0
            ELSE t.v::double precision END
FROM pgkv.store s CROSS JOIN LATERAL jsonb_each(s.value) kv CROSS JOIN LATERAL (SELECT kv.value #>> '{}') t(v)
WHERE s.type = 'zset' AND jsonb_typeof(s.value) = 'object'
  AND t.v <> 'NaN';

UPDATE pgkv.store s
SET card = c.n, head = CASE WHEN s.type = 'list' THEN 0 END, value = NULL
FROM (
    SELECT id, count(*) AS n FROM pgkv.hash_fields GROUP BY id
    UNION ALL SELECT id, count(*) FROM pgkv.list_items GROUP BY id
    UNION ALL SELECT id, count(*) FROM pgkv.set_members GROUP BY id
    UNION ALL SELECT id, count(*) FROM pgkv.zset_members GROUP BY id
) c
WHERE c.id = s.id;

-- 0.3.0 could leave an empty collection behind; an empty collection does not exist.
DELETE FROM pgkv.store WHERE id IS NOT NULL AND card IS NULL;

-- Functions whose signature or return type changes cannot be replaced in place.
DROP FUNCTION pgkv._lock_key(TEXT, BOOLEAN);
DROP FUNCTION pgkv._expires_at(INTEGER, BIGINT, BIGINT, BIGINT, BOOLEAN);
DROP FUNCTION pgkv._require_string(TEXT);
DROP FUNCTION pgkv._lock_store_keys(TEXT, TEXT[]);
DROP FUNCTION pgkv.expire(TEXT, INTEGER);
DROP FUNCTION pgkv.set_opts(TEXT, TEXT, BOOLEAN, BOOLEAN, BOOLEAN, INTEGER, BIGINT, BIGINT, BIGINT, BOOLEAN);
DROP FUNCTION pgkv.cas(TEXT, TEXT, TEXT, INTEGER, BIGINT, BIGINT, BIGINT, BOOLEAN);
DROP FUNCTION pgkv.getex(TEXT, INTEGER);
DROP FUNCTION pgkv.getrange(TEXT, INTEGER, INTEGER);
DROP FUNCTION pgkv.setrange(TEXT, INTEGER, TEXT);
DROP FUNCTION pgkv.lpop(TEXT, INTEGER);
DROP FUNCTION pgkv.rpop(TEXT, INTEGER);
DROP FUNCTION pgkv.lrange(TEXT, INTEGER, INTEGER);
DROP FUNCTION pgkv.lindex(TEXT, INTEGER);
DROP FUNCTION pgkv.lset(TEXT, INTEGER, TEXT);
DROP FUNCTION pgkv.ltrim(TEXT, INTEGER, INTEGER);
DROP FUNCTION pgkv.lrem(TEXT, INTEGER, TEXT);
DROP FUNCTION pgkv.lpos(TEXT, TEXT);
DROP FUNCTION pgkv.zrange(TEXT, INTEGER, INTEGER, BOOLEAN);
DROP FUNCTION pgkv.zrevrange(TEXT, INTEGER, INTEGER, BOOLEAN);
DROP FUNCTION pgkv.zscore(TEXT, TEXT);
DROP FUNCTION pgkv.zrangebyscore(TEXT, NUMERIC, NUMERIC, BOOLEAN);
DROP FUNCTION pgkv.zincrby(TEXT, NUMERIC, TEXT);
DROP FUNCTION pgkv.zcount(TEXT, NUMERIC, NUMERIC);
DROP FUNCTION pgkv.zscan(TEXT, BIGINT, TEXT, INTEGER);
DROP FUNCTION pgkv.zmscore(TEXT, TEXT[]);
DROP FUNCTION pgkv.zpopmin(TEXT, INTEGER);
DROP FUNCTION pgkv.zpopmax(TEXT, INTEGER);
DROP FUNCTION pgkv.zremrangebyrank(TEXT, INTEGER, INTEGER);
DROP FUNCTION pgkv.zremrangebyscore(TEXT, NUMERIC, NUMERIC);
DROP FUNCTION pgkv.zrangestore(TEXT, TEXT, INTEGER, INTEGER, BOOLEAN);
DROP FUNCTION pgkv.zunionstore(TEXT, TEXT[], NUMERIC[], TEXT);
DROP FUNCTION pgkv.zinterstore(TEXT, TEXT[], NUMERIC[], TEXT);
DROP FUNCTION pgkv.scan(BIGINT, TEXT, INTEGER);
DROP FUNCTION pgkv.cleanup_expired();

