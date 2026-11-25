from pgkv_client import rows, scalar


def list_values(conn, key):
    return [r[0] for r in rows(conn, "SELECT pgkv.lrange(%s, 0, -1)", (key,))]


def test_extended_list_operations(conn):
    assert scalar(conn, "SELECT pgkv.lpushx('missing', 'a')") == 0
    assert scalar(conn, "SELECT pgkv.rpush('list', 'a', 'c')") == 2
    assert scalar(conn, "SELECT pgkv.lpushx('list', 'head')") == 3
    assert scalar(conn, "SELECT pgkv.rpushx('list', 'tail')") == 4
    assert list_values(conn, 'list') == ['head', 'a', 'c', 'tail']

    assert scalar(conn, "SELECT pgkv.linsert('list', 'BEFORE', 'c', 'b')") == 5
    assert scalar(conn, "SELECT pgkv.linsert('list', 'AFTER', 'tail', 'end')") == 6
    # As in Redis: -1 when the pivot is not in the list, 0 when the key does not exist.
    assert scalar(conn, "SELECT pgkv.linsert('list', 'BEFORE', 'missing', 'x')") == -1
    assert scalar(conn, "SELECT pgkv.linsert('missing', 'BEFORE', 'x', 'y')") == 0
    assert list_values(conn, 'list') == ['head', 'a', 'b', 'c', 'tail', 'end']

    assert scalar(conn, "SELECT pgkv.lpos('list', 'b')") == 2
    assert scalar(conn, "SELECT pgkv.lpos('list', 'missing')") is None

    assert scalar(conn, "SELECT pgkv.lmove('list', 'dst', 'LEFT', 'RIGHT')") == 'head'
    assert list_values(conn, 'list') == ['a', 'b', 'c', 'tail', 'end']
    assert list_values(conn, 'dst') == ['head']

    assert scalar(conn, "SELECT pgkv.rpoplpush('list', 'dst')") == 'end'
    assert list_values(conn, 'list') == ['a', 'b', 'c', 'tail']
    assert list_values(conn, 'dst') == ['end', 'head']

    assert scalar(conn, "SELECT pgkv.lmove('dst', 'dst', 'LEFT', 'RIGHT')") == 'end'
    assert list_values(conn, 'dst') == ['head', 'end']
