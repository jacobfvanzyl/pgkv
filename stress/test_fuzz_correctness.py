import random
import string

from pgkv_client import rows, scalar


def redis_getrange(value: str, start: int, end: int) -> str:
    # As Redis computes it (t_string.c getrangeCommand): an end before the start of the
    # string is clamped to the first character, not treated as an empty range.
    n = len(value)
    if start < 0 and end < 0 and start > end:
        return ''
    if start < 0:
        start = n + start
    if end < 0:
        end = n + end
    start = max(start, 0)
    end = max(end, 0)
    end = min(end, n - 1)
    if n == 0 or start > end:
        return ''
    return value[start:end + 1]


def test_random_string_counter_hash_list_set_zset_sequences(conn):
    rng = random.Random(20260519)

    strings = {}
    counters = {}
    hashes = {}
    lists = {}
    sets = {}
    zsets = {}

    for i in range(500):
        op = rng.choice(['string', 'counter', 'hash', 'list', 'set', 'zset'])
        key = f'{op}:{rng.randrange(20)}'

        if op == 'string':
            value = ''.join(rng.choice(string.ascii_letters) for _ in range(rng.randrange(0, 64)))
            assert scalar(conn, "SELECT pgkv.set(%s, %s)", (key, value)) == 'OK'
            strings[key] = value
            assert scalar(conn, "SELECT pgkv.get(%s) #>> '{}'", (key,)) == value
            start, end = rng.randrange(-80, 80), rng.randrange(-80, 80)
            assert scalar(conn, "SELECT pgkv.getrange(%s, %s, %s)", (key, start, end)) == redis_getrange(value, start, end)

        elif op == 'counter':
            delta = rng.randrange(-100, 101)
            expected = counters.get(key, 0) + delta
            assert scalar(conn, "SELECT pgkv.incrby(%s, %s)", (key, delta)) == expected
            counters[key] = expected
            assert scalar(conn, "SELECT (pgkv.get(%s))::bigint", (key,)) == expected

        elif op == 'hash':
            field = f'f{rng.randrange(15)}'
            value = str(rng.randrange(100000))
            added = 0 if field in hashes.setdefault(key, {}) else 1
            hashes[key][field] = value
            assert scalar(conn, "SELECT pgkv.hset(%s, %s, %s)", (key, field, value)) == added
            assert scalar(conn, "SELECT pgkv.hget(%s, %s) #>> '{}'", (key, field)) == value
            assert scalar(conn, "SELECT pgkv.hlen(%s)", (key,)) == len(hashes[key])

        elif op == 'list':
            value = f'v{rng.randrange(8)}'
            model = lists.setdefault(key, [])
            if rng.choice([True, False]):
                model.insert(0, value)
                assert scalar(conn, "SELECT pgkv.lpush(%s, %s)", (key, value)) == len(model)
            else:
                model.append(value)
                assert scalar(conn, "SELECT pgkv.rpush(%s, %s)", (key, value)) == len(model)
            assert scalar(conn, "SELECT pgkv.llen(%s)", (key,)) == len(model)
            assert [r[0] for r in rows(conn, "SELECT pgkv.lrange(%s, 0, -1)", (key,))] == model

        elif op == 'set':
            member = f'm{rng.randrange(25)}'
            model = sets.setdefault(key, set())
            added = 0 if member in model else 1
            model.add(member)
            assert scalar(conn, "SELECT pgkv.sadd(%s, %s)", (key, member)) == added
            assert scalar(conn, "SELECT pgkv.sismember(%s, %s)", (key, member)) == 1
            assert set(r[0] for r in rows(conn, "SELECT pgkv.smembers(%s)", (key,))) == model

        elif op == 'zset':
            member = f'm{rng.randrange(25)}'
            score = rng.randrange(-100, 101)
            model = zsets.setdefault(key, {})
            added = 0 if member in model else 1
            model[member] = score
            assert scalar(conn, "SELECT pgkv.zadd(%s, %s::text, %s)", (key, score, member)) == added
            expected = [m for m, _ in sorted(model.items(), key=lambda kv: (kv[1], kv[0]))]
            assert [r[0] for r in rows(conn, "SELECT member FROM pgkv.zrange(%s, 0, -1, false)", (key,))] == expected
