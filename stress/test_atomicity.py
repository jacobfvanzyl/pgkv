"""Two-session atomicity tests (READ COMMITTED).

Each test forces the interleaving that exposes a read-then-write race:

    session A   BEGIN; <operation A>          (not yet committed)
    session B   <operation B>                 (starts while A is open)
    session A   COMMIT                        (once B is blocked on A's lock, or done)

A ran first and commits, so a correct store must answer as if A happened before B
(or, where B cannot see A yet, as if B happened first). Losing an acknowledged write,
or both sessions winning a one-winner operation, is a failure.
"""
from concurrent.futures import ThreadPoolExecutor
import time

import psycopg
import pytest

from pgkv_client import rows, scalar

EXPIRED = "SELECT pgkv.set_opts(%s, 'old', p_exat => 1)"  # 1970: already expired


def interleave(dsn, a_sql, b_sql, a_params=(), b_params=()):
    """Run A in an open transaction, B in a second session, then commit A. Returns both results."""
    with psycopg.connect(dsn) as a, psycopg.connect(dsn, autocommit=True) as b, psycopg.connect(
        dsn, autocommit=True
    ) as watch:
        a_rows = a.execute(a_sql, a_params).fetchall()
        b_pid = b.info.backend_pid
        with ThreadPoolExecutor(max_workers=1) as pool:
            future = pool.submit(lambda: b.execute(b_sql, b_params).fetchall())
            deadline = time.monotonic() + 5
            while time.monotonic() < deadline and not future.done():
                blocked = watch.execute(
                    "SELECT wait_event_type = 'Lock' FROM pg_stat_activity WHERE pid = %s", (b_pid,)
                ).fetchone()
                if blocked and blocked[0]:
                    break
                time.sleep(0.01)
            a.commit()
            b_rows = future.result(timeout=15)
    return a_rows, b_rows


def one(result_rows):
    return result_rows[0][0] if result_rows else None


@pytest.fixture()
def kv(dsn):
    with psycopg.connect(dsn, autocommit=True) as conn:
        conn.execute("SELECT pgkv.flushall()")
        yield conn
        conn.execute("SELECT pgkv.flushall()")


def text(conn, key):
    return scalar(conn, "SELECT pgkv.get(%s) #>> '{}'", (key,))


def lrange(conn, key):
    return [r[0] for r in rows(conn, "SELECT pgkv.lrange(%s, 0, -1)", (key,))]


# --- the two races the spike found -------------------------------------------------------------


def test_getdel_gives_the_value_to_one_session_only(dsn, kv):
    scalar(kv, "SELECT pgkv.set('k', 'v')")
    a, b = interleave(dsn, "SELECT pgkv.getdel('k')", "SELECT pgkv.getdel('k')")
    assert sorted([one(a), one(b)], key=str) == [None, 'v']


def test_set_nx_has_one_winner(dsn, kv):
    a, b = interleave(
        dsn, "SELECT pgkv.set_opts('k', 'a', p_nx => true)", "SELECT pgkv.set_opts('k', 'b', p_nx => true)"
    )
    assert (one(a), one(b)) == ('OK', None)
    assert text(kv, 'k') == 'a'


def test_setnx_has_one_winner(dsn, kv):
    a, b = interleave(dsn, "SELECT pgkv.setnx('k', 'a')", "SELECT pgkv.setnx('k', 'b')")
    assert (one(a), one(b)) == (1, 0)
    assert text(kv, 'k') == 'a'


def test_set_nx_treats_an_expired_key_as_absent_and_has_one_winner(dsn, kv):
    scalar(kv, EXPIRED, ('k',))
    a, b = interleave(
        dsn, "SELECT pgkv.set_opts('k', 'a', p_nx => true)", "SELECT pgkv.set_opts('k', 'b', p_nx => true)"
    )
    assert (one(a), one(b)) == ('OK', None)
    assert text(kv, 'k') == 'a'


def test_msetnx_has_one_winner_and_is_all_or_nothing(dsn, kv):
    a, b = interleave(dsn, "SELECT pgkv.msetnx('k1', 'a', 'k2', 'a')", "SELECT pgkv.msetnx('k2', 'b', 'k3', 'b')")
    assert (one(a), one(b)) == (1, 0)
    assert text(kv, 'k2') == 'a'
    assert scalar(kv, "SELECT pgkv.exists('k3')") == 0


def test_set_xx_does_not_recreate_a_deleted_key(dsn, kv):
    scalar(kv, "SELECT pgkv.set('k', 'v')")
    _, b = interleave(dsn, "SELECT pgkv.del('k')", "SELECT pgkv.set_opts('k', 'b', p_xx => true)")
    assert one(b) is None
    assert scalar(kv, "SELECT pgkv.exists('k')") == 0


def test_set_get_returns_the_value_it_replaced(dsn, kv):
    scalar(kv, "SELECT pgkv.set('k', 'v0')")
    _, b = interleave(dsn, "SELECT pgkv.set('k', 'a')", "SELECT pgkv.set_opts('k', 'b', p_get => true)")
    assert one(b) == 'a'
    assert text(kv, 'k') == 'b'


# --- compare-and-set, compare-and-delete --------------------------------------------------------


def test_cas_has_one_winner(dsn, kv):
    scalar(kv, "SELECT pgkv.set('k', 'v0')")
    a, b = interleave(dsn, "SELECT pgkv.cas('k', 'v0', 'a')", "SELECT pgkv.cas('k', 'v0', 'b')")
    assert (one(a), one(b)) == (1, 0)
    assert text(kv, 'k') == 'a'


def test_cas_requiring_absence_has_one_winner(dsn, kv):
    a, b = interleave(dsn, "SELECT pgkv.cas('k', NULL, 'a')", "SELECT pgkv.cas('k', NULL, 'b')")
    assert (one(a), one(b)) == (1, 0)
    assert text(kv, 'k') == 'a'


def test_cas_loses_to_a_concurrent_delete(dsn, kv):
    scalar(kv, "SELECT pgkv.set('k', 'v0')")
    _, b = interleave(dsn, "SELECT pgkv.del('k')", "SELECT pgkv.cas('k', 'v0', 'b')")
    assert one(b) == 0
    assert scalar(kv, "SELECT pgkv.exists('k')") == 0


def test_cad_has_one_winner(dsn, kv):
    scalar(kv, "SELECT pgkv.set('k', 'v0')")
    a, b = interleave(dsn, "SELECT pgkv.cad('k', 'v0')", "SELECT pgkv.cad('k', 'v0')")
    assert (one(a), one(b)) == (1, 0)


def test_cas_under_contention_never_loses_an_update(dsn, kv):
    """A counter kept with get + cas: every successful cas must be reflected."""
    scalar(kv, "SELECT pgkv.set('n', '0')")
    workers, wins_needed = 8, 25

    def work(_):
        with psycopg.connect(dsn, autocommit=True) as conn:
            wins = 0
            while wins < wins_needed:
                current = scalar(conn, "SELECT pgkv.get('n') #>> '{}'")
                wins += scalar(conn, "SELECT pgkv.cas('n', %s, %s)", (current, str(int(current) + 1)))

    with ThreadPoolExecutor(max_workers=workers) as pool:
        list(pool.map(work, range(workers)))
    assert text(kv, 'n') == str(workers * wins_needed)


# --- reads must not destroy a concurrent write --------------------------------------------------


def test_get_of_an_expired_key_does_not_delete_its_replacement(dsn, kv):
    scalar(kv, EXPIRED, ('k',))
    _, b = interleave(dsn, "SELECT pgkv.set('k', 'new')", "SELECT pgkv.get('k')")
    assert one(b) in (None, 'new')
    assert text(kv, 'k') == 'new'


def test_expire_does_not_revive_an_expired_key(kv):
    scalar(kv, EXPIRED, ('k',))
    assert scalar(kv, "SELECT pgkv.expire('k', 100)") == 0
    assert scalar(kv, "SELECT pgkv.get('k')") is None


# --- strings ------------------------------------------------------------------------------------


def test_incr_keeps_a_concurrent_set(dsn, kv):
    scalar(kv, "SELECT pgkv.set('k', '1')")
    _, b = interleave(dsn, "SELECT pgkv.set('k', '10')", "SELECT pgkv.incr('k')")
    assert one(b) == 11


@pytest.mark.parametrize('seed', [None, 'x'])
def test_append_keeps_both_appends(dsn, kv, seed):
    if seed:
        scalar(kv, "SELECT pgkv.set('k', %s)", (seed,))
    interleave(dsn, "SELECT pgkv.append('k', 'a')", "SELECT pgkv.append('k', 'b')")
    assert text(kv, 'k') == (seed or '') + 'ab'


def test_setrange_keeps_both_writes(dsn, kv):
    scalar(kv, "SELECT pgkv.set('k', 'hello')")
    interleave(dsn, "SELECT pgkv.setrange('k', 0, 'J')", "SELECT pgkv.setrange('k', 4, 'y')")
    assert text(kv, 'k') == 'Jelly'


def test_incrbyfloat_keeps_a_concurrent_set(dsn, kv):
    scalar(kv, "SELECT pgkv.set('k', '1')")
    _, b = interleave(dsn, "SELECT pgkv.set('k', '10')", "SELECT pgkv.incrbyfloat('k', 0.5)")
    assert float(one(b)) == 10.5


# --- hashes -------------------------------------------------------------------------------------


@pytest.mark.parametrize('existing', [False, True])
def test_hset_keeps_both_fields(dsn, kv, existing):
    if existing:
        scalar(kv, "SELECT pgkv.hset('h', 'seed', '0')")
    interleave(dsn, "SELECT pgkv.hset('h', 'a', '1')", "SELECT pgkv.hset('h', 'b', '2')")
    assert scalar(kv, "SELECT pgkv.hlen('h')") == (3 if existing else 2)


def test_hdel_removes_both_fields(dsn, kv):
    scalar(kv, "SELECT pgkv.hset('h', 'a', '1', 'b', '2', 'c', '3')")
    interleave(dsn, "SELECT pgkv.hdel('h', 'a')", "SELECT pgkv.hdel('h', 'b')")
    assert [r[0] for r in rows(kv, "SELECT pgkv.hkeys('h')")] == ['c']


@pytest.mark.parametrize('existing', [False, True])
def test_hincrby_counts_both(dsn, kv, existing):
    if existing:
        scalar(kv, "SELECT pgkv.hset('h', 'n', '0')")
    interleave(dsn, "SELECT pgkv.hincrby('h', 'n', 1)", "SELECT pgkv.hincrby('h', 'n', 1)")
    assert scalar(kv, "SELECT pgkv.hget('h', 'n') #>> '{}'") == '2'


# --- lists --------------------------------------------------------------------------------------


@pytest.mark.parametrize('push', ['lpush', 'rpush'])
@pytest.mark.parametrize('existing', [False, True])
def test_push_keeps_both_elements(dsn, kv, push, existing):
    if existing:
        scalar(kv, "SELECT pgkv.rpush('l', 'seed')")
    interleave(dsn, f"SELECT pgkv.{push}('l', 'a')", f"SELECT pgkv.{push}('l', 'b')")
    assert scalar(kv, "SELECT pgkv.llen('l')") == (3 if existing else 2)


@pytest.mark.parametrize('pop', ['lpop', 'rpop'])
def test_pop_gives_each_element_once(dsn, kv, pop):
    scalar(kv, "SELECT pgkv.rpush('l', 'a', 'b', 'c')")
    a, b = interleave(dsn, f"SELECT pgkv.{pop}('l')", f"SELECT pgkv.{pop}('l')")
    assert one(a) != one(b)
    assert scalar(kv, "SELECT pgkv.llen('l')") == 1


def test_lset_keeps_both_writes(dsn, kv):
    scalar(kv, "SELECT pgkv.rpush('l', 'a', 'b')")
    interleave(dsn, "SELECT pgkv.lset('l', 0, 'x')", "SELECT pgkv.lset('l', 1, 'y')")
    assert lrange(kv, 'l') == ['x', 'y']


def test_lrem_removes_both(dsn, kv):
    scalar(kv, "SELECT pgkv.rpush('l', 'a', 'b', 'c')")
    interleave(dsn, "SELECT pgkv.lrem('l', 0, 'a')", "SELECT pgkv.lrem('l', 0, 'c')")
    assert lrange(kv, 'l') == ['b']


def test_ltrim_keeps_a_concurrent_push(dsn, kv):
    scalar(kv, "SELECT pgkv.rpush('l', 'a', 'b', 'c')")
    interleave(dsn, "SELECT pgkv.rpush('l', 'd')", "SELECT pgkv.ltrim('l', 1, -1)")
    assert lrange(kv, 'l') == ['b', 'c', 'd']


def test_linsert_keeps_a_concurrent_push(dsn, kv):
    scalar(kv, "SELECT pgkv.rpush('l', 'a', 'c')")
    interleave(dsn, "SELECT pgkv.rpush('l', 'd')", "SELECT pgkv.linsert('l', 'BEFORE', 'c', 'b')")
    assert lrange(kv, 'l') == ['a', 'b', 'c', 'd']


def test_lpushx_does_not_recreate_a_deleted_list(dsn, kv):
    scalar(kv, "SELECT pgkv.rpush('l', 'a')")
    _, b = interleave(dsn, "SELECT pgkv.del('l')", "SELECT pgkv.lpushx('l', 'x')")
    assert one(b) == 0
    assert scalar(kv, "SELECT pgkv.exists('l')") == 0


def test_lmove_keeps_a_concurrent_push_to_the_destination(dsn, kv):
    scalar(kv, "SELECT pgkv.rpush('src', 'a')")
    scalar(kv, "SELECT pgkv.rpush('dst', 'x')")
    interleave(dsn, "SELECT pgkv.rpush('dst', 'y')", "SELECT pgkv.lmove('src', 'dst', 'LEFT', 'RIGHT')")
    assert lrange(kv, 'dst') == ['x', 'y', 'a']


# --- sets ---------------------------------------------------------------------------------------


@pytest.mark.parametrize('existing', [False, True])
def test_sadd_keeps_both_members(dsn, kv, existing):
    if existing:
        scalar(kv, "SELECT pgkv.sadd('s', 'seed')")
    interleave(dsn, "SELECT pgkv.sadd('s', 'a')", "SELECT pgkv.sadd('s', 'b')")
    assert scalar(kv, "SELECT pgkv.scard('s')") == (3 if existing else 2)


def test_srem_removes_both_members(dsn, kv):
    scalar(kv, "SELECT pgkv.sadd('s', 'a', 'b', 'c')")
    interleave(dsn, "SELECT pgkv.srem('s', 'a')", "SELECT pgkv.srem('s', 'b')")
    assert [r[0] for r in rows(kv, "SELECT pgkv.smembers('s')")] == ['c']


# --- sorted sets --------------------------------------------------------------------------------


@pytest.mark.parametrize('existing', [False, True])
def test_zadd_keeps_both_members(dsn, kv, existing):
    if existing:
        scalar(kv, "SELECT pgkv.zadd('z', '0', 'seed')")
    interleave(dsn, "SELECT pgkv.zadd('z', '1', 'a')", "SELECT pgkv.zadd('z', '2', 'b')")
    assert scalar(kv, "SELECT pgkv.zcard('z')") == (3 if existing else 2)


def test_zrem_removes_both_members(dsn, kv):
    scalar(kv, "SELECT pgkv.zadd('z', '1', 'a', '2', 'b', '3', 'c')")
    interleave(dsn, "SELECT pgkv.zrem('z', 'a')", "SELECT pgkv.zrem('z', 'b')")
    assert [r[0] for r in rows(kv, "SELECT member FROM pgkv.zrange('z', 0, -1)")] == ['c']


@pytest.mark.parametrize('existing', [False, True])
def test_zincrby_counts_both(dsn, kv, existing):
    if existing:
        scalar(kv, "SELECT pgkv.zadd('z', '0', 'm')")
    interleave(dsn, "SELECT pgkv.zincrby('z', 1, 'm')", "SELECT pgkv.zincrby('z', 1, 'm')")
    assert scalar(kv, "SELECT pgkv.zscore('z', 'm')") == 2


@pytest.mark.parametrize(
    'remove',
    ["SELECT * FROM pgkv.zpopmin('z')", "SELECT pgkv.zremrangebyrank('z', 0, 0)", "SELECT pgkv.zremrangebyscore('z', 1, 1)"],
)
def test_zset_removal_keeps_a_concurrent_zadd(dsn, kv, remove):
    scalar(kv, "SELECT pgkv.zadd('z', '1', 'a')")
    interleave(dsn, "SELECT pgkv.zadd('z', '2', 'b')", remove)
    assert [r[0] for r in rows(kv, "SELECT member FROM pgkv.zrange('z', 0, -1)")] == ['b']


def test_zunionstore_onto_a_source_keeps_a_concurrent_zadd(dsn, kv):
    scalar(kv, "SELECT pgkv.zadd('z1', '1', 'a')")
    scalar(kv, "SELECT pgkv.zadd('z2', '3', 'c')")
    interleave(dsn, "SELECT pgkv.zadd('z1', '2', 'b')", "SELECT pgkv.zunionstore('z1', ARRAY['z1', 'z2'])")
    assert [r[0] for r in rows(kv, "SELECT member FROM pgkv.zrange('z1', 0, -1)")] == ['a', 'b', 'c']


# --- json ---------------------------------------------------------------------------------------


@pytest.mark.parametrize('existing', [False, True])
def test_json_set_keeps_both_paths(dsn, kv, existing):
    if existing:
        scalar(kv, "SELECT pgkv.json_set('d', NULL, '{\"seed\": 0}'::jsonb)")
    interleave(
        dsn, "SELECT pgkv.json_set('d', ARRAY['a'], '1'::jsonb)", "SELECT pgkv.json_set('d', ARRAY['b'], '2'::jsonb)"
    )
    assert sorted(r[0] for r in rows(kv, "SELECT pgkv.json_objkeys('d')")) == (
        ['a', 'b', 'seed'] if existing else ['a', 'b']
    )


def test_json_del_removes_both_paths(dsn, kv):
    scalar(kv, "SELECT pgkv.json_set('d', NULL, '{\"a\": 1, \"b\": 2, \"c\": 3}'::jsonb)")
    interleave(dsn, "SELECT pgkv.json_del('d', ARRAY['a'])", "SELECT pgkv.json_del('d', ARRAY['b'])")
    assert [r[0] for r in rows(kv, "SELECT pgkv.json_objkeys('d')")] == ['c']


def test_json_arrappend_keeps_both_items(dsn, kv):
    scalar(kv, "SELECT pgkv.json_set('d', NULL, '{\"t\": [0]}'::jsonb)")
    interleave(
        dsn, "SELECT pgkv.json_arrappend('d', ARRAY['t'], '1'::jsonb)", "SELECT pgkv.json_arrappend('d', ARRAY['t'], '2'::jsonb)"
    )
    assert scalar(kv, "SELECT pgkv.json_get('d', ARRAY['t'])") == [0, 1, 2]


# --- many sessions at once ----------------------------------------------------------------------


def test_many_sessions_on_one_key_lose_nothing(dsn, kv):
    workers, loops = 12, 40

    def work(worker):
        with psycopg.connect(dsn, autocommit=True) as conn:
            for i in range(loops):
                member = f'{worker}:{i}'
                scalar(conn, "SELECT pgkv.hset('h', %s, 'x')", (member,))
                scalar(conn, "SELECT pgkv.rpush('l', %s)", (member,))
                scalar(conn, "SELECT pgkv.sadd('s', %s)", (member,))
                scalar(conn, "SELECT pgkv.zadd('z', '1', %s)", (member,))
                scalar(conn, "SELECT pgkv.zincrby('z', 1, 'total')")
                scalar(conn, "SELECT pgkv.hincrby('h', 'total', 1)")
                scalar(conn, "SELECT pgkv.append('a', 'x')")
                scalar(conn, "SELECT pgkv.json_arrappend('d', ARRAY['t'], to_jsonb(%s::text))", (member,))

    with ThreadPoolExecutor(max_workers=workers) as pool:
        list(pool.map(work, range(workers)))

    total = workers * loops
    assert scalar(kv, "SELECT pgkv.hlen('h')") == total + 1
    assert scalar(kv, "SELECT pgkv.llen('l')") == total
    assert scalar(kv, "SELECT pgkv.scard('s')") == total
    assert scalar(kv, "SELECT pgkv.zcard('z')") == total + 1
    assert scalar(kv, "SELECT pgkv.zscore('z', 'total')") == total
    assert scalar(kv, "SELECT pgkv.hget('h', 'total') #>> '{}'") == str(total)
    assert scalar(kv, "SELECT pgkv.strlen('a')") == total
    assert scalar(kv, "SELECT jsonb_array_length(pgkv.json_get('d', ARRAY['t']))") == total


def test_getdel_under_contention_hands_out_each_value_once(dsn, kv):
    keys = [f'code:{i}' for i in range(60)]
    for key in keys:
        scalar(kv, "SELECT pgkv.set(%s, 'v')", (key,))

    def work(_):
        with psycopg.connect(dsn, autocommit=True) as conn:
            return sum(1 for key in keys if scalar(conn, "SELECT pgkv.getdel(%s)", (key,)) is not None)

    with ThreadPoolExecutor(max_workers=8) as pool:
        assert sum(pool.map(work, range(8))) == len(keys)


# --- prefix scan --------------------------------------------------------------------------------


def test_scan_prefix_pages_are_stable_under_inserts(kv):
    for i in range(0, 40, 2):
        scalar(kv, "SELECT pgkv.set(%s, 'v')", (f'grant:{i:03d}',))
    scalar(kv, "SELECT pgkv.set('other:1', 'v')")
    scalar(kv, EXPIRED, ('grant:999',))

    seen, after = [], None
    while True:
        page = [r[0] for r in rows(kv, "SELECT key FROM pgkv.scan_prefix('grant:', %s, 7)", (after,))]
        seen += page
        if len(page) < 7:
            break
        after = page[-1]
        # A key inserted before the cursor must not shift later pages; one after it is picked up.
        scalar(kv, "SELECT pgkv.set('grant:001', 'v')")
        scalar(kv, "SELECT pgkv.set('grant:037', 'v')")

    assert seen == [f'grant:{i:03d}' for i in range(0, 40, 2)][:19] + ['grant:037', 'grant:038']
    assert len(seen) == len(set(seen))


def test_scan_prefix_takes_the_prefix_literally(kv):
    for key in ['a%:1', 'a_:1', 'ab:1', 'a\\:1']:
        scalar(kv, "SELECT pgkv.set(%s, 'v')", (key,))
    assert [r[0] for r in rows(kv, "SELECT key FROM pgkv.scan_prefix('a%%')")] == ['a%:1']
    assert [r[0] for r in rows(kv, "SELECT key FROM pgkv.scan_prefix('a_')")] == ['a_:1']
    assert [r[0] for r in rows(kv, "SELECT key FROM pgkv.scan_prefix('a\\')")] == ['a\\:1']


def test_scan_skips_expired_keys_without_deleting_them(kv):
    scalar(kv, "SELECT pgkv.set('scan:a', 'v')")
    scalar(kv, EXPIRED, ('scan:b',))
    scalar(kv, EXPIRED, ('unrelated',))
    assert rows(kv, "SELECT cursor, key FROM pgkv.scan(0, 'scan:*', 10)") == [(0, 'scan:a')]
    assert scalar(kv, "SELECT pgkv.cleanup_expired()") == 2


def test_scan_walks_every_key_once(kv):
    keys = sorted(f'k:{i:02d}' for i in range(25))
    for key in keys:
        scalar(kv, "SELECT pgkv.set(%s, 'v')", (key,))
    seen, cursor = [], 0
    while True:
        page = rows(kv, "SELECT cursor, key FROM pgkv.scan(%s, 'k:*', 10)", (cursor,))
        seen += [r[1] for r in page]
        cursor = page[0][0] if page else 0
        if cursor == 0:
            break
    assert seen == keys


# --- snapshot isolation levels ------------------------------------------------------------------


@pytest.mark.parametrize('existing', [False, True])
def test_repeatable_read_reports_a_lost_race_instead_of_losing_the_write(dsn, kv, existing):
    """At REPEATABLE READ a writer cannot see a concurrent commit; it must fail, to be retried."""
    if existing:
        scalar(kv, "SELECT pgkv.hset('h', 'seed', '0')")
    with psycopg.connect(dsn) as a:
        a.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
        a.execute("SELECT 1")  # takes the snapshot
        scalar(kv, "SELECT pgkv.hset('h', 'b', '2')")
        with pytest.raises(psycopg.errors.SerializationFailure):
            a.execute("SELECT pgkv.hset('h', 'a', '1')")
        a.rollback()
    assert scalar(kv, "SELECT pgkv.hlen('h')") == (2 if existing else 1)


# --- multi-key commands lock in key order -------------------------------------------------------


def test_multi_key_commands_in_opposite_order_do_not_deadlock(dsn, kv):
    scalar(kv, "SELECT pgkv.rpush('l1', 'x')")
    commands = [
        ("SELECT pgkv.mset('a', '1', 'b', '1')", "SELECT pgkv.mset('b', '2', 'a', '2')"),
        ("SELECT pgkv.del('a', 'b')", "SELECT pgkv.del('b', 'a')"),
        ("SELECT pgkv.msetnx('a', '1', 'b', '1')", "SELECT pgkv.msetnx('b', '2', 'a', '2')"),
        ("SELECT pgkv.lmove('l1', 'l2', 'LEFT', 'RIGHT')", "SELECT pgkv.lmove('l2', 'l1', 'LEFT', 'RIGHT')"),
        ("SELECT pgkv.zunionstore('z1', ARRAY['z2'])", "SELECT pgkv.zunionstore('z2', ARRAY['z1'])"),
    ]

    def work(worker):
        with psycopg.connect(dsn, autocommit=True) as conn:
            for i in range(150):
                conn.execute(commands[i % len(commands)][worker % 2]).fetchall()

    with ThreadPoolExecutor(max_workers=8) as pool:
        list(pool.map(work, range(8)))  # a deadlock would raise DeadlockDetected here


def test_mset_is_one_write(dsn, kv):
    scalar(kv, "SELECT pgkv.mset('a', '0', 'b', '0')")
    interleave(dsn, "SELECT pgkv.mset('a', '1', 'b', '1')", "SELECT pgkv.mset('b', '2', 'a', '2')")
    assert text(kv, 'a') == text(kv, 'b') == '2'
