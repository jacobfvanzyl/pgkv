-- pgkv 0.4.0: row-per-element storage, the commands added in 0.4.0, and pgkv.call.
-- Command semantics are checked against a real Redis server by conformance/; this file
-- covers what that cannot: the SQL-level API, storage invariants, privileges.

BEGIN;
CREATE EXTENSION IF NOT EXISTS pgtap;
SELECT plan(148);

SELECT pgkv.flushall();

CREATE FUNCTION pg_temp.elements() RETURNS bigint LANGUAGE sql AS $$
    SELECT (SELECT COUNT(*) FROM pgkv.hash_fields) + (SELECT COUNT(*) FROM pgkv.list_items)
         + (SELECT COUNT(*) FROM pgkv.set_members) + (SELECT COUNT(*) FROM pgkv.zset_members);
$$;

-- Storage: one row per element, a header row per key
SELECT pgkv.hset('h', 'a', '1', 'b', '2');
SELECT pgkv.rpush('l', 'x', 'y', 'z');
SELECT pgkv.sadd('s', 'm1', 'm2');
SELECT pgkv.zadd('z', '1', 'one', '2', 'two');
SELECT is(pg_temp.elements(), 9::bigint, 'each element is a row');
SELECT is((SELECT array_agg(card ORDER BY key) FROM pgkv.store), ARRAY[2, 3, 2, 2]::bigint[], 'headers carry the element count');
SELECT is((SELECT COUNT(*)::integer FROM pgkv.store WHERE value IS NOT NULL), 0, 'collections keep no value in the header');

-- An emptied collection does not exist
SELECT pgkv.hdel('h', 'a', 'b');
SELECT is(pgkv.exists('h'), 0, 'a hash with no fields does not exist');
SELECT is((SELECT COUNT(*)::integer FROM pgkv.zpopmin('z', 5)), 2, 'ZPOPMIN pops what is there');
SELECT is(pgkv.type('z'), 'none', 'a sorted set emptied by ZPOPMIN does not exist');
SELECT is(pgkv.zremrangebyrank('nokey', 0, -1), 0, 'ZREMRANGEBYRANK on a missing key');
SELECT is(pgkv.lmove('l', 'l2', 'LEFT', 'RIGHT'), to_jsonb('x'::text), 'LMOVE returns the element');
SELECT pgkv.lmove('l', 'l2', 'LEFT', 'RIGHT');
SELECT pgkv.lmove('l', 'l2', 'LEFT', 'RIGHT');
SELECT is(pgkv.exists('l'), 0, 'a list drained by LMOVE does not exist');
SELECT is((SELECT array_agg(v #>> '{}') FROM pgkv.lrange('l2', 0, -1) v), ARRAY['x', 'y', 'z'], 'LMOVE kept the order');

-- Overwriting and deleting a collection removes its elements
SELECT pgkv.set('l2', 'now a string');
SELECT pgkv.del('s');
SELECT is(pg_temp.elements(), 0::bigint, 'SET over a list and DEL of a set leave no element rows');

-- An expired collection is invisible, and a write to its key starts from nothing
SELECT pgkv.sadd('exp', 'old1', 'old2');
UPDATE pgkv.store SET expires_at = now() - interval '1 hour' WHERE key = 'exp';
SELECT is(pgkv.scard('exp'), 0, 'an expired set is empty');
SELECT is(pgkv.sismember('exp', 'old1'), 0, 'an expired set has no members');
SELECT is(pgkv.sadd('exp', 'new'), 1, 'SADD to an expired set');
SELECT is((SELECT array_agg(m #>> '{}') FROM pgkv.smembers('exp') m), ARRAY['new'], 'the old members are gone');
SELECT is(pgkv.ttl('exp'), -1, 'and so is the old expiry');
SELECT pgkv.zadd('exp2', '1', 'a');
UPDATE pgkv.store SET expires_at = now() - interval '1 hour' WHERE key = 'exp2';
SELECT is(pgkv.cleanup_expired(), 1, 'CLEANUP_EXPIRED counts the keys it removed');
SELECT is((SELECT COUNT(*)::integer FROM pgkv.zset_members), 0, 'and removes their elements');

-- Large collections: operations touch the rows they need
SELECT pgkv.flushall();
SELECT is(pgkv.rpush('big', VARIADIC ARRAY(SELECT g::text FROM generate_series(1, 5000) g)), 5000, 'RPUSH of 5000 elements');
SELECT is((SELECT v #>> '{}' FROM pgkv.lpop('big') v), '1', 'LPOP from a long list');
SELECT is(pgkv.lindex('big', 2499) #>> '{}', '2501', 'LINDEX into the middle');
SELECT is(pgkv.lrem('big', 1, '10'), 1, 'LREM near the head');
SELECT is(pgkv.linsert('big', 'AFTER', '4990', 'new'), 4999, 'LINSERT near the tail');
SELECT is(pgkv.lindex('big', -11) #>> '{}', 'new', 'the inserted element is where it should be');
SELECT is((SELECT array_agg(v #>> '{}') FROM pgkv.lrange('big', 7, 9) v), ARRAY['9', '11', '12'], 'the list closed up after LREM');
SELECT is((SELECT COUNT(*)::integer FROM pgkv.list_items i JOIN pgkv.store s ON s.id = i.id
           WHERE s.key = 'big' AND i.pos NOT BETWEEN s.head AND s.head + s.card - 1), 0, 'positions stay contiguous');

-- Sorted sets: doubles, infinities, ties in byte order
SELECT pgkv.flushall();
SELECT is(pgkv.zadd('z', 'inf', 'top', '-inf', 'bottom', '1.5', 'B', '1.5', 'a', '1.5', 'A'), 5, 'ZADD with infinite scores');
SELECT is((SELECT array_agg(member) FROM pgkv.zrange('z', 0, -1)), ARRAY['bottom', 'A', 'B', 'a', 'top'], 'ties are ordered by byte, not by collation');
SELECT is(pgkv.zscore('z', 'top'), 'infinity'::double precision, 'ZSCORE of an infinite score');
SELECT throws_ok($$SELECT pgkv.zadd('z', 'nan', 'x')$$, 'KV000', 'ERR value is not a valid float', 'ZADD rejects NaN');
SELECT throws_ok($$SELECT pgkv.zadd('z', '1e400', 'x')$$, 'KV000', 'ERR value is not a valid float', 'ZADD rejects a score outside the double range');
SELECT is(pgkv.zcard('z'), 5, 'a rejected ZADD wrote nothing');
SELECT throws_ok($$SELECT pgkv.zincrby('z', '-infinity', 'top')$$, 'KV000', 'ERR resulting score is not a number (NaN)', 'ZINCRBY to NaN is an error');
SELECT is(pgkv.zadd_opts('z', ARRAY['5', 'a', '9', 'new'], p_xx => true, p_ch => true), '1'::jsonb, 'ZADD XX CH updates only existing members');
SELECT is(pgkv.zadd_opts('z', ARRAY['1', 'a'], p_gt => true), '0'::jsonb, 'ZADD GT keeps the higher score');
SELECT is(pgkv.zadd_opts('z', ARRAY['2', 'a'], p_incr => true), '"7"'::jsonb, 'ZADD INCR returns the new score');
SELECT is(pgkv.zadd_opts('z', ARRAY['2', 'a'], p_nx => true, p_incr => true), 'null'::jsonb, 'ZADD NX INCR on an existing member');
SELECT results_eq(
    $$SELECT member, score FROM pgkv.zrange_by('z', '(1.5', '+inf', 'SCORE')$$,
    $$VALUES ('a'::text, 7::double precision), ('top', 'infinity')$$, 'ZRANGE_BY with an exclusive score bound');
SELECT is((SELECT array_agg(member) FROM pgkv.zrange_by('z', '+inf', '-inf', 'SCORE', p_rev => true, p_offset => 1, p_count => 2)),
          ARRAY['a', 'B'], 'ZRANGE_BY reversed with LIMIT');
SELECT pgkv.zadd('lex', '0', 'a', '0', 'b', '0', 'c', '0', 'd');
SELECT is((SELECT array_agg(member) FROM pgkv.zrange_by('lex', '[b', '(d', 'LEX')), ARRAY['b', 'c'], 'ZRANGE_BY by lexicographic range');
SELECT is(pgkv.zlexcount('lex', '-', '+'), 4, 'ZLEXCOUNT');
SELECT is(pgkv.zremrangebylex('lex', '[a', '[b'), 2, 'ZREMRANGEBYLEX');
SELECT is(pgkv.zrank('z', 'top'), 4, 'ZRANK');
SELECT is(pgkv.zrevrank('z', 'top'), 0, 'ZREVRANK');
SELECT pgkv.sadd('plain', 'a', 'zzz');
SELECT results_eq($$SELECT member, score FROM pgkv.zunion(ARRAY['lex', 'plain'], ARRAY[1, 10]::double precision[])$$,
    $$VALUES ('c'::text, 0::double precision), ('d', 0), ('a', 10), ('zzz', 10)$$, 'ZUNION takes a plain set as scores of 1');
SELECT is((SELECT array_agg(member) FROM pgkv.zinter(ARRAY['z', 'plain'])), ARRAY['a'], 'ZINTER');
SELECT is((SELECT array_agg(member) FROM pgkv.zdiff(ARRAY['lex', 'plain'])), ARRAY['c', 'd'], 'ZDIFF');
SELECT is(pgkv.zintercard(ARRAY['z', 'plain']), 1, 'ZINTERCARD');
SELECT pgkv.hset('h2', 'f', 'v');
SELECT throws_ok($$SELECT pgkv.zunionstore('dst', ARRAY['z', 'h2'])$$, 'KV001', NULL, 'ZUNIONSTORE over a hash is WRONGTYPE');
SELECT is(pgkv.exists('dst'), 0, 'and it created nothing');

-- Hashes
SELECT is(pgkv.hsetnx('h2', 'f', 'other'), 0, 'HSETNX keeps an existing field');
SELECT is(pgkv.hsetnx('h2', 'g', 'new'), 1, 'HSETNX sets a missing field');
SELECT is(pgkv.hincrbyfloat('h2', 'n', 1.5), 1.5, 'HINCRBYFLOAT creates the field');
SELECT is(pgkv.hincrbyfloat('h2', 'n', 0.25), 1.75, 'HINCRBYFLOAT adds');
SELECT is(pgkv.hstrlen('h2', 'g'), 3, 'HSTRLEN');
SELECT is(pgkv.hlen('h2'), 3, 'HLEN is the stored count');
SELECT is((SELECT COUNT(*)::integer FROM pgkv.hrandfield('h2', 2)), 2, 'HRANDFIELD returns distinct fields');
SELECT is((SELECT COUNT(*)::integer FROM pgkv.hrandfield('h2', -7)), 7, 'HRANDFIELD with a negative count repeats');

-- Sets
SELECT pgkv.sadd('s1', 'a', 'b', 'c');
SELECT pgkv.sadd('s2', 'b', 'c', 'd');
SELECT is(pgkv.srem('s1', 'a', 'a'), 1, 'SREM counts a member named twice once');
SELECT is(pgkv.sinterstore('s3', 's1', 's2'), 2, 'SINTERSTORE');
SELECT is(pgkv.sunionstore('s1', 's1', 's2'), 3, 'SUNIONSTORE onto one of its sources');
SELECT is(pgkv.sdiffstore('s4', 's2', 's1'), 0, 'SDIFFSTORE with an empty result');
SELECT is(pgkv.exists('s4'), 0, 'stores nothing');
SELECT is(pgkv.sintercard(ARRAY['s1', 's2'], 2), 2, 'SINTERCARD stops at the limit');
SELECT is((SELECT array_agg(i) FROM pgkv.smismember('s1', 'b', 'zz') i), ARRAY[1, 0], 'SMISMEMBER');
SELECT is(pgkv.smove('s1', 's5', 'b'), 1, 'SMOVE');
SELECT is((SELECT COUNT(*)::integer FROM pgkv.spop('s2', 2)), 2, 'SPOP with a count');
SELECT is(pgkv.scard('s2'), 1, 'SPOP removed them');
SELECT throws_ok($$SELECT pgkv.sinter('s1', 'h2')$$, 'KV001', NULL, 'SINTER over a hash is WRONGTYPE');

-- Keys and expiry
SELECT pgkv.set('k', 'v');
SELECT is(pgkv.pexpire('k', 50000), 1, 'PEXPIRE');
SELECT ok(pgkv.pttl('k') BETWEEN 49000 AND 50000, 'PTTL');
SELECT is(pgkv.expire('k', 10, 'GT'), 0, 'EXPIRE GT does not shorten');
SELECT is(pgkv.expire('k', 10, 'LT'), 1, 'EXPIRE LT shortens');
SELECT is(pgkv.expireat('k', 4102444800), 1, 'EXPIREAT');
SELECT is(pgkv.expiretime('k'), 4102444800::bigint, 'EXPIRETIME');
SELECT is(pgkv.persist('k'), 1, 'PERSIST');
SELECT is(pgkv.ttl('k'), -1, 'no expiry after PERSIST');
SELECT is(pgkv.expire('k', -1), 1, 'EXPIRE with a time in the past');
SELECT is(pgkv.exists('k'), 0, 'deletes the key');
SELECT pgkv.rpush('src', 'a', 'b');
SELECT is(pgkv.copy('src', 'cp'), 1, 'COPY of a list');
SELECT is(pgkv.rename('src', 'moved'), 'OK', 'RENAME of a list');
SELECT is(pgkv.rpush('moved', 'c'), 3, 'the renamed list is writable');
SELECT is((SELECT array_agg(v #>> '{}') FROM pgkv.lrange('cp', 0, -1) v), ARRAY['a', 'b'], 'the copy is independent');
SELECT is(pgkv.renamenx('moved', 'cp'), 0, 'RENAMENX does not replace');
SELECT throws_ok($$SELECT pgkv.rename('nokey', 'x')$$, 'KV000', 'ERR no such key', 'RENAME of a missing key');
SELECT is((SELECT COUNT(*)::integer FROM pgkv.scan(0, '*', 100, 'list')), 2, 'SCAN filtered by type');
SELECT is((SELECT value FROM pgkv.scan_prefix('cp', NULL, 1)), '["a", "b"]'::jsonb, 'SCAN_PREFIX assembles a collection''s value');

-- Strings: bytes, as in Redis
SELECT pgkv.set('str', 'héllo');
SELECT is(pgkv.strlen('str'), 6, 'STRLEN counts bytes');
SELECT is(pgkv.getrange('str', 3, -1), 'llo', 'GETRANGE takes byte offsets');
SELECT throws_ok($$SELECT pgkv.getrange('str', 0, 1)$$, 'KV000', NULL, 'GETRANGE refuses to cut a character in half');
SELECT throws_ok($$SELECT pgkv.incr('str')$$, 'KV000', 'ERR value is not an integer or out of range', 'INCR of a non-number');
SELECT pgkv.set('n', ' 12');
SELECT throws_ok($$SELECT pgkv.incr('n')$$, 'KV000', NULL, 'INCR does not accept padded numbers');
SELECT is(pgkv.getset('str', 'next') #>> '{}', 'héllo', 'GETSET returns the old value');
SELECT throws_ok($$SELECT pgkv.set_opts('h2', 'v', p_get => true)$$, 'KV001', NULL, 'SET GET on a hash is WRONGTYPE');
SELECT is(pgkv.hlen('h2'), 3, 'and leaves the hash alone');

-- pgkv.call, try_call, pipeline
SELECT is(pgkv.call('SET', 'c', 'v', 'EX', '100'), '{"status": "OK"}'::jsonb, 'call: a status reply');
SELECT is(pgkv.call('get', 'c'), '"v"'::jsonb, 'call: a string reply, command names in any case');
SELECT is(pgkv.call('GET', 'nokey'), 'null'::jsonb, 'call: nil is JSON null');
SELECT is(pgkv.call('ZRANGE', 'z', '0', '0', 'WITHSCORES'), '["bottom", "-inf"]'::jsonb, 'call: scores are strings as Redis prints them');
SELECT throws_ok($$SELECT pgkv.call('GET')$$, 'KV002', 'ERR wrong number of arguments for ''get'' command', 'call: wrong arity');
SELECT throws_ok($$SELECT pgkv.call('NOPE', 'a')$$, 'KV002', NULL, 'call: unknown command');
SELECT is(pgkv.try_call('LPUSH', 'c', 'x') ->> 'error', 'WRONGTYPE Operation against a key holding the wrong kind of value', 'try_call returns the error');
SELECT is(pgkv.pipeline('[["INCR","p"],["LPUSH","p","x"],["INCR","p"]]') -> 2, '2'::jsonb, 'pipeline: a failed command undoes only itself');
SELECT is(pgkv.pipeline('[["SET","q","1"],["GET"],["NOPE"]]', true),
    '{"error": "EXECABORT Transaction discarded because of previous errors.", "previous": [{"index": 1, "error": "ERR wrong number of arguments for ''get'' command"}, {"index": 2, "error": "ERR unknown command ''NOPE''"}]}'::jsonb,
    'an atomic pipeline with malformed commands is discarded, and says which');
SELECT is(pgkv.exists('q'), 0, 'and applies nothing');
SELECT is(pgkv.pipeline('[["SET","q","1"],["MSET","a","1","b"]]', true) -> 1 ->> 'error',
    'ERR wrong number of arguments for ''mset'' command', 'an error Redis finds only when the command runs does not discard the transaction');
SELECT is(pgkv.get('q') #>> '{}', '1', 'the other commands apply');
SELECT ok((SELECT COUNT(*) FROM pgkv.commands()) > 130, 'commands() lists what call understands');

-- Review fixes: bounds are validated without reading a key, and scan limits do not overflow
SELECT is(pgkv.set('', 'a string at the empty key'), 'OK', 'a string can live at the empty key');
SELECT is(pgkv.call('ZREMRANGEBYSCORE', 'nokey', '0', '1'), '0'::jsonb, 'ZREMRANGEBYSCORE on a missing key is 0 whatever the empty key holds');
SELECT is(pgkv.call('ZREMRANGEBYRANK', 'nokey', '0', '-1'), '0'::jsonb, 'ZREMRANGEBYRANK too');
SELECT is(pgkv.call('ZREMRANGEBYLEX', 'nokey', '-', '+'), '0'::jsonb, 'ZREMRANGEBYLEX too');
SELECT throws_ok($$SELECT pgkv.call('ZREMRANGEBYSCORE', 'nokey', 'x', '1')$$, 'KV000', 'ERR min or max is not a float', 'and bad bounds are still an error on a missing key');
SELECT is(pgkv.del(''), 1, 'cleanup');
SELECT is(pgkv.call('SCAN', '0', 'COUNT', '9223372036854775807') ->> 0, '0', 'SCAN with the largest COUNT');
SELECT is(pgkv.call('SCAN', '9223372036854775807', 'COUNT', '9223372036854775807'), '["0", []]'::jsonb, 'SCAN with the largest cursor and COUNT');
SELECT is(pgkv.call('HSCAN', 'nokey', '9223372036854775807', 'COUNT', '2147483647'), '["0", []]'::jsonb, 'HSCAN at the limits');
SELECT is(pgkv.call('SSCAN', 'nokey', '9223372036854775807', 'COUNT', '9223372036854775807'), '["0", []]'::jsonb, 'SSCAN at the limits');
SELECT is(pgkv.call('ZSCAN', 'nokey', '9223372036854775807', 'COUNT', '4294967296'), '["0", []]'::jsonb, 'ZSCAN at the limits');
SELECT is((SELECT COUNT(*)::integer FROM pgkv.hscan('nokey', 9223372036854775807, '*', 2147483647)), 0, 'hscan() at the limits');
SELECT is((SELECT COUNT(*)::integer FROM pgkv.scan(9223372036854775807, '*', 2147483647)), 0, 'scan() at the limits');
SELECT is(pgkv.hset('nullcur:h', 'f', 'v') + pgkv.sadd('nullcur:s', 'm') + pgkv.zadd('nullcur:z', '1', 'm'), 3, 'setup for NULL cursors');
SELECT ok((SELECT COUNT(*) FROM pgkv.scan(NULL, 'nullcur:*')) = 3, 'scan() with a NULL cursor starts at the beginning');
SELECT is((SELECT COUNT(*)::integer FROM pgkv.hscan('nullcur:h', NULL)), 1, 'hscan() with a NULL cursor');
SELECT is((SELECT COUNT(*)::integer FROM pgkv.sscan('nullcur:s', NULL)), 1, 'sscan() with a NULL cursor');
SELECT is((SELECT COUNT(*)::integer FROM pgkv.zscan('nullcur:z', NULL)), 1, 'zscan() with a NULL cursor');
SELECT is(pgkv.del('nullcur:h', 'nullcur:s', 'nullcur:z'), 3, 'cleanup');

-- Privileges: closed by default, opened per role
CREATE ROLE pgkv_test_040;
SELECT ok(NOT has_schema_privilege('pgkv_test_040', 'pgkv', 'USAGE'), 'PUBLIC cannot use the schema');
SELECT is((SELECT COUNT(*)::integer FROM pg_proc p WHERE p.pronamespace = 'pgkv'::regnamespace
           AND has_function_privilege('pgkv_test_040', p.oid, 'EXECUTE')), 0, 'no function is executable by PUBLIC');
SELECT is((SELECT COUNT(*)::integer FROM pg_class c WHERE c.relnamespace = 'pgkv'::regnamespace AND c.relkind = 'r'
           AND has_table_privilege('pgkv_test_040', c.oid, 'SELECT,INSERT,UPDATE,DELETE')), 0, 'no table is accessible to PUBLIC');
SELECT lives_ok($$SELECT pgkv.grant_access('pgkv_test_040')$$, 'GRANT_ACCESS');
SELECT ok(has_schema_privilege('pgkv_test_040', 'pgkv', 'USAGE'), 'the role can use the schema');
SELECT is((SELECT COUNT(*)::integer FROM pg_proc p WHERE p.pronamespace = 'pgkv'::regnamespace AND p.proname NOT LIKE E'\\_%'
           AND p.proname NOT IN ('grant_access', 'revoke_access', 'cleanup_expired', 'set_unlogged')
           AND NOT has_function_privilege('pgkv_test_040', p.oid, 'EXECUTE')), 0, 'and execute every data command');
SELECT is((SELECT COUNT(*)::integer FROM pg_proc p WHERE p.pronamespace = 'pgkv'::regnamespace
           AND (p.proname LIKE E'\\_%' OR p.proname IN ('grant_access', 'revoke_access', 'cleanup_expired', 'set_unlogged'))
           AND has_function_privilege('pgkv_test_040', p.oid, 'EXECUTE')), 0, 'but no internal or maintenance function');
SELECT is((SELECT COUNT(*)::integer FROM pg_class c WHERE c.relnamespace = 'pgkv'::regnamespace AND c.relkind = 'r'
           AND has_table_privilege('pgkv_test_040', c.oid, 'SELECT,INSERT,UPDATE,DELETE')), 0, 'and no table');
SET ROLE pgkv_test_040;
SELECT is(pgkv.call('GET', 'c') #>> '{}', 'v', 'the role reads through call');
SELECT is(pgkv.pipeline('[["SET","acl","1"],["INCR","acl"]]') -> 1, '2'::jsonb, 'and writes through pipeline');
SELECT throws_ok($$SELECT pgkv.grant_access('pgkv_test_040')$$, '42501', NULL, 'it cannot grant access itself');
SELECT throws_ok($$SELECT pgkv.cleanup_expired()$$, '42501', NULL, 'or run maintenance');
RESET ROLE;
SELECT lives_ok($$SELECT pgkv.revoke_access('pgkv_test_040')$$, 'REVOKE_ACCESS');
SELECT ok(NOT has_schema_privilege('pgkv_test_040', 'pgkv', 'USAGE')
          AND NOT has_function_privilege('pgkv_test_040', 'pgkv.call(text[])', 'EXECUTE'), 'takes it all back');
SELECT ok(has_schema_privilege('pg_monitor', 'pgkv', 'USAGE') AND has_table_privilege('pg_monitor', 'pgkv.store', 'SELECT'),
          'pg_monitor can read the tables');
DROP ROLE pgkv_test_040;

-- Durability: the tables switch between logged and unlogged together
SELECT is(pgkv.set_unlogged(), 'unlogged', 'SET_UNLOGGED');
SELECT is((SELECT array_agg(DISTINCT c.relpersistence::text) FROM pg_class c
           WHERE c.relnamespace = 'pgkv'::regnamespace AND c.relkind = 'r'), ARRAY['u'], 'every table is unlogged');
SELECT is(pgkv.get('c') #>> '{}', 'v', 'the data is still there');
SELECT is(pgkv.set_unlogged(false), 'logged', 'and back');
SELECT is((SELECT array_agg(DISTINCT c.relpersistence::text) FROM pg_class c
           WHERE c.relnamespace = 'pgkv'::regnamespace AND c.relkind = 'r'), ARRAY['p'], 'every table is logged');

-- Reads write nothing, so they work in a read-only transaction (and on a standby)
SET LOCAL transaction_read_only = on;
SELECT lives_ok($$SELECT pgkv.get('c'), pgkv.hget('h2', 'f'), pgkv.llen('cp'), pgkv.ttl('c'), pgkv.exists('c'), pgkv.type('z'),
                         pgkv.call('ZSCORE', 'z', 'top'), (SELECT COUNT(*) FROM pgkv.keys('*'))$$, 'reads run in a read-only transaction');

SELECT * FROM finish();
ROLLBACK;
