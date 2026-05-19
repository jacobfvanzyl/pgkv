from pgkv_client import rows, scalar


def zitems(conn, key):
    return rows(conn, "SELECT member, score FROM pgkv.zrange(%s, 0, -1, true)", (key,))


def test_extended_zset_operations(conn):
    assert scalar(conn, "SELECT pgkv.zadd('z', '1', 'a', '2', 'b', '3', 'c', '4', 'd')") == 4
    assert rows(conn, "SELECT member, score FROM pgkv.zmscore('z', 'a', 'missing', 'd')") == [('a', 1), ('missing', None), ('d', 4)]

    assert rows(conn, "SELECT member, score FROM pgkv.zpopmin('z', 2)") == [('a', 1), ('b', 2)]
    assert zitems(conn, 'z') == [('c', 3), ('d', 4)]

    assert scalar(conn, "SELECT pgkv.zadd('z', '1', 'a', '2', 'b')") == 2
    assert rows(conn, "SELECT member, score FROM pgkv.zpopmax('z', 1)") == [('d', 4)]
    assert zitems(conn, 'z') == [('a', 1), ('b', 2), ('c', 3)]

    assert scalar(conn, "SELECT pgkv.zremrangebyrank('z', 0, 0)") == 1
    assert zitems(conn, 'z') == [('b', 2), ('c', 3)]

    assert scalar(conn, "SELECT pgkv.zadd('z', '4', 'd', '5', 'e')") == 2
    assert scalar(conn, "SELECT pgkv.zremrangebyscore('z', 3, 4)") == 2
    assert zitems(conn, 'z') == [('b', 2), ('e', 5)]

    assert scalar(conn, "SELECT pgkv.zadd('z', '1', 'a', '3', 'c', '4', 'd')") == 3
    assert scalar(conn, "SELECT pgkv.zrangestore('zr', 'z', 0, 2, false)") == 3
    assert zitems(conn, 'zr') == [('a', 1), ('b', 2), ('c', 3)]
    assert scalar(conn, "SELECT pgkv.zrangestore('zrrev', 'z', 0, 1, true)") == 2
    assert zitems(conn, 'zrrev') == [('d', 4), ('e', 5)]
