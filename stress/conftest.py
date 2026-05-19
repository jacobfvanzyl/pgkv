import os
import subprocess
import time
import pytest
import psycopg

DSN = os.environ.get("PGKV_DSN", "postgresql://postgres:postgres@localhost:55432/postgres")


def wait_for_db(dsn: str, timeout: float = 30.0) -> None:
    deadline = time.time() + timeout
    last_error = None
    while time.time() < deadline:
        try:
            with psycopg.connect(dsn, autocommit=True) as conn:
                conn.execute("SELECT 1")
            return
        except Exception as exc:  # pragma: no cover - diagnostic path
            last_error = exc
            time.sleep(0.25)
    raise RuntimeError(f"database not ready: {last_error}")


@pytest.fixture(scope="session", autouse=True)
def database_ready():
    wait_for_db(DSN)
    with psycopg.connect(DSN, autocommit=True) as conn:
        conn.execute("CREATE EXTENSION IF NOT EXISTS pgkv")
    yield


@pytest.fixture()
def conn(database_ready):
    with psycopg.connect(DSN, autocommit=True) as conn:
        conn.execute("SELECT pgkv.flushall()")
        yield conn
        conn.execute("SELECT pgkv.flushall()")


@pytest.fixture()
def dsn(database_ready):
    return DSN
