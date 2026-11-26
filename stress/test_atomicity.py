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


# --- 0.4.0: collections are stored one row per element ------------------------------------------
# The key's row in pgkv.store is the lock for the whole collection; these check that it is
# taken everywhere it must be, and that no element rows are ever left without a key.

ORPHANS = """
SELECT (SELECT count(*) FROM pgkv.hash_fields e WHERE NOT EXISTS (SELECT 1 FROM pgkv.store s WHERE s.id = e.id))
     + (SELECT count(*) FROM pgkv.list_items e WHERE NOT EXISTS (SELECT 1 FROM pgkv.store s WHERE s.id = e.id))
     + (SELECT count(*) FROM pgkv.set_members e WHERE NOT EXISTS (SELECT 1 FROM pgkv.store s WHERE s.id = e.id))
     + (SELECT count(*) FROM pgkv.zset_members e WHERE NOT EXISTS (SELECT 1 FROM pgkv.store s WHERE s.id = e.id))
"""


def members(conn, key):
    return sorted(r[0] for r in rows(conn, "SELECT pgkv.smembers(%s) #>> '{}'", (key,)))


def hash_of(conn, key):
    return {f: v for f, v in rows(conn, "SELECT field, value #>> '{}' FROM pgkv.hgetall(%s)", (key,))}


def test_hsetnx_has_one_winner(dsn, kv):
    a, b = interleave(dsn, "SELECT pgkv.hsetnx('h', 'f', 'a')", "SELECT pgkv.hsetnx('h', 'f', 'b')")
    assert (one(a), one(b)) == (1, 0)
    assert hash_of(kv, 'h') == {'f': 'a'}
    assert scalar(kv, "SELECT pgkv.hlen('h')") == 1


@pytest.mark.parametrize('existing', [False, True])
def test_hincrbyfloat_counts_both(dsn, kv, existing):
    if existing:
        scalar(kv, "SELECT pgkv.hset('h', 'n', '1.5')")
    interleave(dsn, "SELECT pgkv.hincrbyfloat('h', 'n', 0.25)", "SELECT pgkv.hincrbyfloat('h', 'n', 0.5)")
    assert hash_of(kv, 'h') == {'n': '2.25' if existing else '0.75'}


def test_del_of_a_hash_and_a_concurrent_hset_leave_no_orphan_fields(dsn, kv):
    scalar(kv, "SELECT pgkv.hset('h', 'old1', 'v', 'old2', 'v')")
    interleave(dsn, "SELECT pgkv.del('h')", "SELECT pgkv.hset('h', 'new', 'v')")
    assert hash_of(kv, 'h') == {'new': 'v'}
    assert scalar(kv, "SELECT pgkv.hlen('h')") == 1
    assert scalar(kv, ORPHANS) == 0


def test_set_over_a_collection_and_a_concurrent_push_leave_no_orphan_items(dsn, kv):
    scalar(kv, "SELECT pgkv.rpush('k', 'a', 'b')")
    # B runs after A: the key is a string by then, so the push is a WRONGTYPE error.
    with pytest.raises(psycopg.Error, match='WRONGTYPE'):
        interleave(dsn, "SELECT pgkv.set('k', 'string')", "SELECT pgkv.rpush('k', 'c')")
    assert text(kv, 'k') == 'string'
    assert scalar(kv, ORPHANS) == 0


def test_two_writers_recreating_an_expired_hash_keep_both_fields(dsn, kv):
    scalar(kv, "SELECT pgkv.hset('h', 'stale', 'v')")
    scalar(kv, "SELECT pgkv.expireat('h', 4102444800)")
    with psycopg.connect(dsn, autocommit=True) as admin:
        admin.execute("UPDATE pgkv.store SET expires_at = now() - interval '1 hour' WHERE key = 'h'")
    interleave(dsn, "SELECT pgkv.hset('h', 'a', '1')", "SELECT pgkv.hset('h', 'b', '2')")
    assert hash_of(kv, 'h') == {'a': '1', 'b': '2'}
    assert scalar(kv, "SELECT pgkv.ttl('h')") == -1
    assert scalar(kv, ORPHANS) == 0


def test_spop_gives_each_member_once(dsn, kv):
    scalar(kv, "SELECT pgkv.sadd('s', 'a', 'b')")
    a, b = interleave(dsn, "SELECT pgkv.spop('s')", "SELECT pgkv.spop('s')")
    assert sorted([one(a), one(b)]) == ['a', 'b']
    assert scalar(kv, "SELECT pgkv.exists('s')") == 0


def test_smove_moves_a_member_once(dsn, kv):
    scalar(kv, "SELECT pgkv.sadd('src', 'm')")
    a, b = interleave(dsn, "SELECT pgkv.smove('src', 'd1', 'm')", "SELECT pgkv.smove('src', 'd2', 'm')")
    assert (one(a), one(b)) == (1, 0)
    assert members(kv, 'd1') == ['m'] and members(kv, 'd2') == []
    assert scalar(kv, "SELECT pgkv.exists('src')") == 0


def test_sunionstore_onto_a_source_keeps_a_concurrent_sadd(dsn, kv):
    scalar(kv, "SELECT pgkv.sadd('a', 'x')")
    scalar(kv, "SELECT pgkv.sadd('b', 'y')")
    interleave(dsn, "SELECT pgkv.sunionstore('a', 'a', 'b')", "SELECT pgkv.sadd('a', 'new')")
    assert members(kv, 'a') == ['new', 'x', 'y']
    assert scalar(kv, "SELECT pgkv.scard('a')") == 3
    assert scalar(kv, ORPHANS) == 0


def test_zadd_nx_has_one_winner(dsn, kv):
    a, b = interleave(
        dsn,
        "SELECT pgkv.zadd_opts('z', ARRAY['1', 'm'], p_nx => true)",
        "SELECT pgkv.zadd_opts('z', ARRAY['2', 'm'], p_nx => true)",
    )
    assert (one(a), one(b)) == (1, 0)
    assert scalar(kv, "SELECT pgkv.zscore('z', 'm')") == 1


def test_zpopmin_gives_each_member_once(dsn, kv):
    scalar(kv, "SELECT pgkv.zadd('z', '1', 'a', '2', 'b')")
    a, b = interleave(dsn, "SELECT member FROM pgkv.zpopmin('z')", "SELECT member FROM pgkv.zpopmin('z')")
    assert (one(a), one(b)) == ('a', 'b')
    assert scalar(kv, "SELECT pgkv.exists('z')") == 0


def test_lmpop_gives_each_element_once(dsn, kv):
    scalar(kv, "SELECT pgkv.rpush('l', 'a', 'b')")
    a, b = interleave(
        dsn, "SELECT pgkv.call('LMPOP', '1', 'l', 'LEFT')", "SELECT pgkv.call('LMPOP', '1', 'l', 'LEFT')"
    )
    assert (one(a), one(b)) == (['l', ['a']], ['l', ['b']])
    assert scalar(kv, "SELECT pgkv.exists('l')") == 0


def test_list_stays_contiguous_under_concurrent_pushes_and_pops_at_both_ends(dsn, kv):
    scalar(kv, "SELECT pgkv.rpush('l', '1', '2', '3', '4')")
    interleave(dsn, "SELECT pgkv.lpush('l', 'head')", "SELECT pgkv.rpush('l', 'tail')")
    interleave(dsn, "SELECT pgkv.lpop('l')", "SELECT pgkv.rpop('l')")
    interleave(dsn, "SELECT pgkv.lrem('l', 0, '2')", "SELECT pgkv.linsert('l', 'BEFORE', '4', 'x')")
    assert lrange(kv, 'l') == ['1', '3', 'x', '4']
    assert scalar(kv, "SELECT pgkv.llen('l')") == 4
    assert [scalar(kv, "SELECT pgkv.lindex('l', %s) #>> '{}'", (i,)) for i in range(4)] == ['1', '3', 'x', '4']


def test_rename_has_one_winner(dsn, kv):
    scalar(kv, "SELECT pgkv.set('k', 'v')")
    with pytest.raises(psycopg.Error, match='no such key'):
        interleave(dsn, "SELECT pgkv.rename('k', 'a')", "SELECT pgkv.rename('k', 'b')")
    assert text(kv, 'a') == 'v'
    assert scalar(kv, "SELECT pgkv.exists('k', 'b')") == 0


def test_rename_of_a_collection_keeps_a_concurrent_write_to_it(dsn, kv):
    scalar(kv, "SELECT pgkv.sadd('s', 'a')")
    interleave(dsn, "SELECT pgkv.rename('s', 't')", "SELECT pgkv.sadd('s', 'b')")
    # B ran after A: the set moved to 't', and 's' is a new set holding only B's member.
    assert members(kv, 't') == ['a'] and members(kv, 's') == ['b']
    assert scalar(kv, ORPHANS) == 0


def test_copy_does_not_tear_a_concurrently_modified_source(dsn, kv):
    scalar(kv, "SELECT pgkv.rpush('src', 'a', 'b')")
    interleave(dsn, "SELECT pgkv.rpush('src', 'c')", "SELECT pgkv.copy('src', 'dst')")
    assert lrange(kv, 'dst') == ['a', 'b', 'c']
    assert scalar(kv, "SELECT pgkv.llen('dst')") == 3


def test_expire_nx_has_one_winner(dsn, kv):
    scalar(kv, "SELECT pgkv.set('k', 'v')")
    a, b = interleave(dsn, "SELECT pgkv.expire('k', 100, 'NX')", "SELECT pgkv.expire('k', 5000, 'NX')")
    assert (one(a), one(b)) == (1, 0)
    assert scalar(kv, "SELECT pgkv.ttl('k')") <= 100


def test_persist_and_expire_do_not_lose_each_other(dsn, kv):
    scalar(kv, "SELECT pgkv.set('k', 'v', 100)")
    interleave(dsn, "SELECT pgkv.persist('k')", "SELECT pgkv.expire('k', 500)")
    assert 400 < scalar(kv, "SELECT pgkv.ttl('k')") <= 500


def test_call_and_pipeline_count_every_increment(dsn, kv):
    a, b = interleave(
        dsn,
        """SELECT pgkv.pipeline('[["INCR","n"],["INCR","n"],["INCR","n"]]', true)""",
        "SELECT pgkv.call('INCRBY', 'n', '10')",
    )
    assert one(a) == [1, 2, 3] and one(b) == 13


def test_a_failed_command_in_a_pipeline_undoes_only_itself(kv):
    reply = scalar(
        kv,
        """SELECT pgkv.pipeline('[["SET","s","v"],["LPUSH","s","x"],["RPUSH","l","a"],["INCR","l"],["RPUSH","l","b"]]', true)""",
    )
    assert reply[0] == {'status': 'OK'} and reply[2] == 1 and reply[4] == 2
    assert reply[1]['error'].startswith('WRONGTYPE') and reply[3]['error'].startswith('WRONGTYPE')
    assert text(kv, 's') == 'v' and lrange(kv, 'l') == ['a', 'b']
    assert scalar(kv, ORPHANS) == 0


def test_try_call_leaves_the_transaction_usable(dsn, kv):
    scalar(kv, "SELECT pgkv.set('s', 'v')")
    with psycopg.connect(dsn) as conn:
        assert scalar(conn, "SELECT pgkv.call('SET', 'before', '1')") == {'status': 'OK'}
        assert scalar(conn, "SELECT pgkv.try_call('LPUSH', 's', 'x')")['error'].startswith('WRONGTYPE')
        assert scalar(conn, "SELECT pgkv.try_call('SET', 'after', '2')") == {'status': 'OK'}
        conn.commit()
    assert text(kv, 'before') == '1' and text(kv, 'after') == '2'


def test_many_sessions_on_one_collection_keep_its_count_right(dsn, kv):
    def worker(n):
        with psycopg.connect(dsn, autocommit=True) as conn:
            for i in range(40):
                conn.execute("SELECT pgkv.hset('h', %s, 'v')", (f'{n}:{i}',))
                conn.execute("SELECT pgkv.sadd('s', %s)", (f'{n}:{i}',))
                conn.execute("SELECT pgkv.zadd('z', %s, %s)", (str(i), f'{n}:{i}'))
                conn.execute("SELECT pgkv.rpush('l', %s)", (f'{n}:{i}',))
                if i % 4 == 3:
                    conn.execute("SELECT pgkv.hdel('h', %s)", (f'{n}:{i - 1}',))
                    conn.execute("SELECT pgkv.srem('s', %s)", (f'{n}:{i - 1}',))
                    conn.execute("SELECT pgkv.zrem('z', %s)", (f'{n}:{i - 1}',))
                    conn.execute("SELECT pgkv.lpop('l')")

    with ThreadPoolExecutor(max_workers=8) as pool:
        list(pool.map(worker, range(8)))

    assert scalar(kv, "SELECT pgkv.hlen('h')") == scalar(kv, "SELECT count(*) FROM pgkv.hgetall('h')") == 240
    assert scalar(kv, "SELECT pgkv.scard('s')") == scalar(kv, "SELECT count(*) FROM pgkv.smembers('s')") == 240
    assert scalar(kv, "SELECT pgkv.zcard('z')") == scalar(kv, "SELECT count(*) FROM pgkv.zrange('z', 0, -1)") == 240
    assert scalar(kv, "SELECT pgkv.llen('l')") == scalar(kv, "SELECT count(*) FROM pgkv.lrange('l', 0, -1)") == 240
    assert scalar(kv, ORPHANS) == 0



def test_renamenx_does_not_overwrite_a_concurrent_setnx_of_the_destination(dsn, kv):
    scalar(kv, "SELECT pgkv.set('src', 'moved')")
    a, b = interleave(dsn, "SELECT pgkv.setnx('dst', 'first')", "SELECT pgkv.renamenx('src', 'dst')")
    assert (one(a), one(b)) == (1, 0)
    assert text(kv, 'dst') == 'first' and text(kv, 'src') == 'moved'


def test_setnx_loses_to_a_concurrent_renamenx_onto_the_key(dsn, kv):
    scalar(kv, "SELECT pgkv.set('src', 'moved')")
    a, b = interleave(dsn, "SELECT pgkv.renamenx('src', 'dst')", "SELECT pgkv.setnx('dst', 'late')")
    assert (one(a), one(b)) == (1, 0)
    assert text(kv, 'dst') == 'moved' and scalar(kv, "SELECT pgkv.exists('src')") == 0


def test_rename_onto_a_concurrently_created_key_replaces_it(dsn, kv):
    scalar(kv, "SELECT pgkv.set('src', 'moved')")
    a, b = interleave(dsn, "SELECT pgkv.rpush('dst', 'a', 'b')", "SELECT pgkv.rename('src', 'dst')")
    assert one(b) == 'OK'
    assert text(kv, 'dst') == 'moved' and scalar(kv, "SELECT pgkv.exists('src')") == 0
    assert scalar(kv, ORPHANS) == 0


def test_a_failed_renamenx_in_a_pipeline_leaves_no_placeholder(kv):
    reply = scalar(kv, """SELECT pgkv.pipeline('[["RENAMENX","nosuch","other"],["SET","x","1"]]')""")
    assert reply[0] == {'error': 'ERR no such key'} and reply[1] == {'status': 'OK'}
    assert scalar(kv, "SELECT count(*) FROM pgkv.store WHERE key = 'other'") == 0
