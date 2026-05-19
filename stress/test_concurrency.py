from concurrent.futures import ThreadPoolExecutor

import psycopg

from pgkv_client import scalar


def test_hot_counter_concurrent_increments(dsn):
    workers = 16
    per_worker = 125
    with psycopg.connect(dsn, autocommit=True) as conn:
        conn.execute("SELECT pgkv.flushall()")

    def work(worker_id: int) -> None:
        with psycopg.connect(dsn, autocommit=True) as conn:
            for _ in range(per_worker):
                scalar(conn, "SELECT pgkv.incr('hot-counter')")

    with ThreadPoolExecutor(max_workers=workers) as pool:
        list(pool.map(work, range(workers)))

    with psycopg.connect(dsn, autocommit=True) as conn:
        assert scalar(conn, "SELECT (pgkv.get('hot-counter'))::bigint") == workers * per_worker


def test_mixed_concurrent_keyspaces(dsn):
    workers = 12
    loops = 80
    with psycopg.connect(dsn, autocommit=True) as conn:
        conn.execute("SELECT pgkv.flushall()")

    def work(worker_id: int) -> None:
        with psycopg.connect(dsn, autocommit=True) as conn:
            for i in range(loops):
                key = f'w:{worker_id}:{i % 10}'
                scalar(conn, "SELECT pgkv.set(%s, %s)", (key, f'v{i}'))
                scalar(conn, "SELECT pgkv.hset(%s, 'last', %s)", (f'h:{worker_id}', str(i)))
                scalar(conn, "SELECT pgkv.lpush(%s, %s)", (f'l:{worker_id}', str(i)))
                scalar(conn, "SELECT pgkv.sadd(%s, %s)", (f's:{worker_id}', str(i % 20)))
                scalar(conn, "SELECT pgkv.zadd(%s, %s::text, %s)", (f'z:{worker_id}', i, str(i % 20)))

    with ThreadPoolExecutor(max_workers=workers) as pool:
        list(pool.map(work, range(workers)))

    with psycopg.connect(dsn, autocommit=True) as conn:
        for worker_id in range(workers):
            assert scalar(conn, "SELECT pgkv.hlen(%s)", (f'h:{worker_id}',)) == 1
            assert scalar(conn, "SELECT pgkv.llen(%s)", (f'l:{worker_id}',)) == loops
            assert scalar(conn, "SELECT pgkv.scard(%s)", (f's:{worker_id}',)) == 20
            assert scalar(conn, "SELECT pgkv.zcard(%s)", (f'z:{worker_id}',)) == 20
