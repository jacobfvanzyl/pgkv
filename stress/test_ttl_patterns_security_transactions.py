import time

import psycopg
import pytest

from pgkv_client import rows, scalar


def test_ttl_churn_and_cleanup(conn):
    for i in range(300):
        assert scalar(conn, "SELECT pgkv.set(%s, %s, 1)", (f'ttl:{i}', f'v{i}')) == 'OK'
    assert scalar(conn, "SELECT pgkv.dbsize()") == 300
    time.sleep(1.25)
    assert scalar(conn, "SELECT pgkv.get('ttl:1')") is None
    assert scalar(conn, "SELECT pgkv.exists('ttl:1')") == 0
    deleted = scalar(conn, "SELECT pgkv.cleanup_expired()")
    assert deleted >= 299
    assert scalar(conn, "SELECT pgkv.dbsize()") == 0


def test_pattern_matching_special_cases(conn):
    keys = ['user:1', 'user:2', 'user:a', 'user:10', 'h_ello', 'h_allo', 'literal%key', 'literal_key']
    for key in keys:
        scalar(conn, "SELECT pgkv.set(%s, 'x')", (key,))
    assert set(r[0] for r in rows(conn, "SELECT pgkv.keys('user:?')")) == {'user:1', 'user:2', 'user:a'}
    assert set(r[0] for r in rows(conn, "SELECT pgkv.keys('user:*')")) == {'user:1', 'user:2', 'user:a', 'user:10'}
    assert set(r[0] for r in rows(conn, "SELECT pgkv.keys('h_[ae]llo')")) == {'h_ello', 'h_allo'}
    assert set(r[0] for r in rows(conn, "SELECT pgkv.keys('literal*key')")) == {'literal%key', 'literal_key'}


def test_transactions_rollback_and_commit(dsn):
    with psycopg.connect(dsn, autocommit=True) as c:
        c.execute("SELECT pgkv.flushall()")
    with psycopg.connect(dsn) as c:
        c.execute("SELECT pgkv.set('tx', 'rollback')")
        c.rollback()
    with psycopg.connect(dsn, autocommit=True) as c:
        assert scalar(c, "SELECT pgkv.get('tx')") is None
    with psycopg.connect(dsn) as c:
        c.execute("SELECT pgkv.set('tx', 'commit')")
        c.commit()
    with psycopg.connect(dsn, autocommit=True) as c:
        assert scalar(c, "SELECT pgkv.get('tx') #>> '{}'") == 'commit'


def test_security_definer_and_table_protection(dsn):
    admin = psycopg.connect(dsn, autocommit=True)
    try:
        admin.execute("DROP USER IF EXISTS pgkv_stress_user")
        admin.execute("CREATE USER pgkv_stress_user PASSWORD 'pgkv_stress_user'")
        admin.execute("GRANT USAGE ON SCHEMA pgkv TO pgkv_stress_user")
        admin.execute("GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA pgkv TO pgkv_stress_user")
        user_dsn = dsn.replace('postgres:postgres@', 'pgkv_stress_user:pgkv_stress_user@')
        with psycopg.connect(user_dsn, autocommit=True) as user:
            assert scalar(user, "SELECT pgkv.set('secure', 'ok')") == 'OK'
            assert scalar(user, "SELECT pgkv.get('secure') #>> '{}'") == 'ok'
            with pytest.raises(psycopg.errors.InsufficientPrivilege):
                user.execute("SELECT * FROM pgkv.store").fetchall()
    finally:
        admin.execute("REVOKE ALL PRIVILEGES ON ALL FUNCTIONS IN SCHEMA pgkv FROM pgkv_stress_user")
        admin.execute("REVOKE USAGE ON SCHEMA pgkv FROM pgkv_stress_user")
        admin.execute("DROP USER IF EXISTS pgkv_stress_user")
        admin.close()
