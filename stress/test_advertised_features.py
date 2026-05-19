from pgkv_client import rows, scalar


def json_text(value):
    return value if isinstance(value, str) else str(value)


def test_all_advertised_features_smoke(conn):
    assert scalar(conn, "SELECT pgkv.set('s', 'hello')") == 'OK'
    assert scalar(conn, "SELECT pgkv.get('s') #>> '{}'") == 'hello'
    assert scalar(conn, "SELECT pgkv.exists('s', 'missing')") == 1
    assert scalar(conn, "SELECT pgkv.type('s')") == 'string'
    assert scalar(conn, "SELECT pgkv.del('missing')") == 0

    assert scalar(conn, "SELECT pgkv.incr('ctr')") == 1
    assert scalar(conn, "SELECT pgkv.incrby('ctr', 9)") == 10
    assert scalar(conn, "SELECT pgkv.decr('ctr')") == 9
    assert scalar(conn, "SELECT pgkv.decrby('ctr', 4)") == 5

    assert scalar(conn, "SELECT pgkv.append('msg', 'Hello')") == 5
    assert scalar(conn, "SELECT pgkv.append('msg', ' World')") == 11
    assert scalar(conn, "SELECT pgkv.strlen('msg')") == 11
    assert scalar(conn, "SELECT pgkv.getrange('msg', -5, -1)") == 'World'
    assert scalar(conn, "SELECT pgkv.setrange('msg', 0, 'Jello')") == 11
    assert scalar(conn, "SELECT pgkv.get('msg') #>> '{}'") == 'Jello World'

    assert scalar(conn, "SELECT pgkv.hset('h', 'name', 'alice', 'age', '30')") == 2
    assert scalar(conn, "SELECT pgkv.hget('h', 'name') #>> '{}'") == 'alice'
    assert rows(conn, "SELECT field, value #>> '{}' FROM pgkv.hmget('h', 'name', 'missing') ORDER BY field") == [('missing', None), ('name', 'alice')]
    assert sorted(rows(conn, "SELECT field, value #>> '{}' FROM pgkv.hgetall('h')")) == [('age', '30'), ('name', 'alice')]
    assert scalar(conn, "SELECT pgkv.hexists('h', 'age')") == 1
    assert scalar(conn, "SELECT pgkv.hlen('h')") == 2
    assert sorted(r[0] for r in rows(conn, "SELECT pgkv.hkeys('h')")) == ['age', 'name']
    assert sorted(r[0] for r in rows(conn, "SELECT pgkv.hvals('h')")) == ['30', 'alice']
    assert scalar(conn, "SELECT pgkv.hincrby('h', 'age', 2)") == 32
    assert scalar(conn, "SELECT pgkv.hdel('h', 'name')") == 1

    assert scalar(conn, "SELECT pgkv.lpush('l', 'a', 'b', 'c')") == 3
    assert scalar(conn, "SELECT pgkv.rpush('l', 'd')") == 4
    assert scalar(conn, "SELECT pgkv.llen('l')") == 4
    assert [r[0] for r in rows(conn, "SELECT pgkv.lrange('l', 0, -1)")] == ['c', 'b', 'a', 'd']
    assert scalar(conn, "SELECT pgkv.lindex('l', -1)") == 'd'
    assert scalar(conn, "SELECT pgkv.lset('l', 1, 'B')") == 'OK'
    assert scalar(conn, "SELECT pgkv.lpop('l')") == 'c'
    assert scalar(conn, "SELECT pgkv.rpop('l')") == 'd'
    assert scalar(conn, "SELECT pgkv.lrem('l', 0, 'B')") == 1
    assert scalar(conn, "SELECT pgkv.ltrim('l', 0, 0)") == 'OK'

    assert scalar(conn, "SELECT pgkv.sadd('set1', 'a', 'b', 'c')") == 3
    assert scalar(conn, "SELECT pgkv.sadd('set2', 'b', 'c', 'd')") == 3
    assert scalar(conn, "SELECT pgkv.sismember('set1', 'a')") == 1
    assert scalar(conn, "SELECT pgkv.scard('set1')") == 3
    assert set(r[0] for r in rows(conn, "SELECT pgkv.smembers('set1')")) == {'a', 'b', 'c'}
    assert set(r[0] for r in rows(conn, "SELECT pgkv.sinter('set1', 'set2')")) == {'b', 'c'}
    assert set(r[0] for r in rows(conn, "SELECT pgkv.sunion('set1', 'set2')")) == {'a', 'b', 'c', 'd'}
    assert set(r[0] for r in rows(conn, "SELECT pgkv.sdiff('set1', 'set2')")) == {'a'}
    assert scalar(conn, "SELECT pgkv.srem('set1', 'a')") == 1

    assert scalar(conn, "SELECT pgkv.zadd('z', 100::numeric::text, 'alice', 200::numeric::text, 'bob', 150::numeric::text, 'charlie')") == 3
    assert scalar(conn, "SELECT pgkv.zscore('z', 'alice')") == 100
    assert scalar(conn, "SELECT pgkv.zcard('z')") == 3
    assert [r[0] for r in rows(conn, "SELECT member FROM pgkv.zrange('z', 0, -1, false)")] == ['alice', 'charlie', 'bob']
    assert [r[0] for r in rows(conn, "SELECT member FROM pgkv.zrevrange('z', 0, -1, false)")] == ['bob', 'charlie', 'alice']
    assert scalar(conn, "SELECT pgkv.zrank('z', 'bob')") == 2
    assert scalar(conn, "SELECT pgkv.zrevrank('z', 'bob')") == 0
    assert [r[0] for r in rows(conn, "SELECT member FROM pgkv.zrangebyscore('z', 100, 150, false)")] == ['alice', 'charlie']
    assert scalar(conn, "SELECT pgkv.zcount('z', 100, 150)") == 2
    assert scalar(conn, "SELECT pgkv.zincrby('z', 25, 'alice')") == 125
    assert scalar(conn, "SELECT pgkv.zrem('z', 'bob')") == 1

    assert scalar(conn, "SELECT pgkv.mset('m1', 'v1', 'm2', 'v2')") == 'OK'
    assert rows(conn, "SELECT key, value #>> '{}' FROM pgkv.mget('m1', 'm2') ORDER BY key") == [('m1', 'v1'), ('m2', 'v2')]
    assert set(r[0] for r in rows(conn, "SELECT pgkv.keys('m?')")) == {'m1', 'm2'}
    assert scalar(conn, "SELECT pgkv.dbsize()") > 0
    assert scalar(conn, "SELECT pgkv.cleanup_expired()") >= 0
    assert scalar(conn, "SELECT pgkv.flushall()") == 'OK'
    assert scalar(conn, "SELECT pgkv.dbsize()") == 0
