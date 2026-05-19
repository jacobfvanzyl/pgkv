from pgkv_client import rows, scalar


def zitems(conn, key):
    return rows(conn, "SELECT member, score FROM pgkv.zrange(%s, 0, -1, true)", (key,))


def test_zset_algebra_store(conn):
    assert scalar(conn, "SELECT pgkv.zadd('z1', '1', 'a', '2', 'b', '3', 'c')") == 3
    assert scalar(conn, "SELECT pgkv.zadd('z2', '10', 'b', '20', 'c', '30', 'd')") == 3
    assert scalar(conn, "SELECT pgkv.zunionstore('zu', ARRAY['z1','z2'])") == 4
    assert zitems(conn, 'zu') == [('a', 1), ('b', 12), ('c', 23), ('d', 30)]
    assert scalar(conn, "SELECT pgkv.zinterstore('zi', ARRAY['z1','z2'])") == 2
    assert zitems(conn, 'zi') == [('b', 12), ('c', 23)]
    assert scalar(conn, "SELECT pgkv.zdiffstore('zd', ARRAY['z1','z2'])") == 1
    assert zitems(conn, 'zd') == [('a', 1)]
    assert scalar(conn, "SELECT pgkv.zunionstore('zw', ARRAY['z1','z2'], ARRAY[2,3], 'MIN')") == 4
    assert zitems(conn, 'zw') == [('a', 2), ('b', 4), ('c', 6), ('d', 90)]


def test_json_operations(conn):
    assert scalar(conn, "SELECT pgkv.json_set('doc', NULL, '{\"name\":\"Ada\",\"tags\":[\"math\"]}'::jsonb)") == 'OK'
    assert scalar(conn, "SELECT pgkv.json_get('doc', ARRAY['name'])") == 'Ada'
    assert scalar(conn, "SELECT pgkv.json_type('doc', ARRAY['tags'])") == 'array'
    assert rows(conn, "SELECT pgkv.json_objkeys('doc')") == [('name',), ('tags',)]
    assert scalar(conn, "SELECT pgkv.json_arrappend('doc', ARRAY['tags'], '\"logic\"'::jsonb, '\"code\"'::jsonb)") == 3
    assert scalar(conn, "SELECT pgkv.json_get('doc', ARRAY['tags'])") == ['math', 'logic', 'code']
    assert scalar(conn, "SELECT pgkv.json_set('doc', ARRAY['meta'], '{\"born\":1815}'::jsonb)") == 'OK'
    assert scalar(conn, "SELECT pgkv.json_get('doc', ARRAY['meta','born'])") == 1815
    assert scalar(conn, "SELECT pgkv.json_del('doc', ARRAY['meta'])") == 1
    assert scalar(conn, "SELECT pgkv.json_get('doc', ARRAY['meta'])") is None
    assert scalar(conn, "SELECT pgkv.type('doc')") == 'json'
