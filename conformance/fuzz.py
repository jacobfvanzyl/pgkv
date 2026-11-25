"""Random command sequences over a handful of keys, biased toward edge cases: empty and
non-ASCII strings, numbers at the limits of their range, malformed arguments, commands
applied to keys of the wrong type. Sequences are a pure function of the seed."""
from __future__ import annotations

import random

KEYS = ["k1", "k2", "k3", "k4"]
VALS = ["", "a", "b", "c", "hello", "0", "1", "-1", "10", "007", " 5", "5 ", "1.5", "3.0e3", "9223372036854775807",
        "-9223372036854775808", "9223372036854775808", "héllo", "日本語", "😀", "a b", "%", "_", "*", '"q"', '{"j":1}',
        "[1,2]", "null", "true", "x" * 70, "A", "inf", "nan", "1e400", "\\", "it's"]
INTS = ["0", "1", "-1", "2", "-2", "3", "5", "-5", "100", "-100", "9223372036854775807", "-9223372036854775808",
        "2147483648", "abc", "1.5", ""]
IDX = ["0", "1", "-1", "2", "-2", "3", "-3", "10", "-10", "100", "-100"]
SCORES = ["0", "1", "-1", "1.5", "2", "3", "10", "-0", "inf", "-inf", "+inf", "1e10", "1e-5", "0.1",
          "3.14159265358979", "1e308", "nan", "abc", ""]
BOUNDS = SCORES + ["(1", "(2", "(-inf", "(abc"]
LEX = ["-", "+", "[a", "(a", "[b", "(c", "[", "a", "[hello"]
FIELDS = ["f1", "f2", "f3", "", "a.b", "f 1"]


def generate(seed: int, count: int, length: int = 40):
    rnd = random.Random(seed)
    k = lambda: rnd.choice(KEYS)
    v = lambda: rnd.choice(VALS)
    i = lambda: rnd.choice(INTS)
    x = lambda: rnd.choice(IDX)
    s = lambda: rnd.choice(SCORES)
    # INCRBYFLOAT is long double arithmetic in Redis: near the limits of a double the two
    # servers print different digits, which says nothing about the commands under test.
    fl = lambda: rnd.choice([x for x in SCORES if x != "1e308"])
    b = lambda: rnd.choice(BOUNDS)
    lx = lambda: rnd.choice(LEX)
    f = lambda: rnd.choice(FIELDS)
    m = lambda: rnd.choice(VALS[:12])
    many = lambda g, lo=1, hi=3: [g() for _ in range(rnd.randint(lo, hi))]
    pick = rnd.choice

    def setopts():
        o = []
        for opt in rnd.sample(["NX", "XX", "GET", "KEEPTTL", "EX", "PX", "EXAT", "PXAT"], rnd.randint(0, 3)):
            o.append(opt)
            if opt in ("EX", "PX"):
                o.append(pick(["100", "0", "-1", "1000000", "abc"]))
            if opt == "EXAT":
                o.append(pick(["4102444800", "1", "0"]))
            if opt == "PXAT":
                o.append(pick(["4102444800000", "1"]))
        return o

    def zs():
        return [e for _ in range(rnd.randint(1, 3)) for e in (s(), m())]

    def numkeys(name, with_scores=False, weights=True):
        ks = many(k, 1, 3)
        r = [str(pick([len(ks), len(ks), len(ks), 0, len(ks) + 1]))] + ks
        if weights and rnd.random() < 0.4:
            r += ["WEIGHTS"] + [pick(["1", "2", "0", "-1", "0.5", "inf"]) for _ in ks]
        if weights and rnd.random() < 0.4:
            r += ["AGGREGATE", pick(["SUM", "MIN", "MAX", "min", "bogus"])]
        if with_scores and rnd.random() < 0.5:
            r += ["WITHSCORES"]
        return r

    def limit():
        return pick([[], [], ["LIMIT", pick(["0", "1", "-1"]), pick(["1", "2", "-1", "0"])]])

    G = {
        "SET": lambda: ["SET", k(), v()] + setopts(), "GET": lambda: ["GET", k()], "DEL": lambda: ["DEL"] + many(k),
        "UNLINK": lambda: ["UNLINK"] + many(k), "EXISTS": lambda: ["EXISTS"] + many(k), "TOUCH": lambda: ["TOUCH"] + many(k),
        "EXPIRE": lambda: ["EXPIRE", k(), pick(["100", "0", "-1", "1000", "abc"])] + pick([[], [], ["NX"], ["XX"], ["GT"], ["LT"], ["NX", "XX"], ["bogus"]]),
        "PEXPIRE": lambda: ["PEXPIRE", k(), pick(["100000", "0", "-1", "abc"])] + pick([[], ["GT"], ["LT"]]),
        "EXPIREAT": lambda: ["EXPIREAT", k(), pick(["4102444800", "1", "0", "abc"])],
        "PEXPIREAT": lambda: ["PEXPIREAT", k(), pick(["4102444800000", "1"])],
        "PERSIST": lambda: ["PERSIST", k()], "TTL": lambda: ["TTL", k()], "PTTL": lambda: ["PTTL", k()],
        "EXPIRETIME": lambda: ["EXPIRETIME", k()],
        "RENAME": lambda: ["RENAME", k(), k()], "RENAMENX": lambda: ["RENAMENX", k(), k()],
        "COPY": lambda: ["COPY", k(), k()] + pick([[], ["REPLACE"], ["bogus"]]),
        "INCR": lambda: ["INCR", k()], "DECR": lambda: ["DECR", k()], "INCRBY": lambda: ["INCRBY", k(), i()],
        "DECRBY": lambda: ["DECRBY", k(), i()], "INCRBYFLOAT": lambda: ["INCRBYFLOAT", k(), fl()],
        "MGET": lambda: ["MGET"] + many(k), "MSET": lambda: ["MSET"] + [e for _ in range(rnd.randint(1, 3)) for e in (k(), v())],
        "MSETNX": lambda: ["MSETNX"] + [e for _ in range(rnd.randint(1, 2)) for e in (k(), v())],
        "KEYS": lambda: ["KEYS", pick(["*", "k*", "k?", "k[12]", "k[^1]", "k[1-3]", "?1", "k\\1", "", "K*"])],
        "TYPE": lambda: ["TYPE", k()], "APPEND": lambda: ["APPEND", k(), v()], "STRLEN": lambda: ["STRLEN", k()],
        "GETRANGE": lambda: ["GETRANGE", k(), x(), x()], "SUBSTR": lambda: ["SUBSTR", k(), x(), x()],
        "SETRANGE": lambda: ["SETRANGE", k(), pick(["0", "1", "3", "10", "-1"]), v()],
        "SETNX": lambda: ["SETNX", k(), v()], "SETEX": lambda: ["SETEX", k(), pick(["100", "0", "-1"]), v()],
        "PSETEX": lambda: ["PSETEX", k(), pick(["100000", "0", "-1"]), v()],
        "GETSET": lambda: ["GETSET", k(), v()], "GETDEL": lambda: ["GETDEL", k()],
        "GETEX": lambda: ["GETEX", k()] + pick([[], ["EX", "100"], ["EX", "0"], ["PERSIST"], ["PX", "100000"], ["EXAT", "4102444800"], ["bogus"]]),
        "HSET": lambda: ["HSET", k()] + [e for _ in range(rnd.randint(1, 3)) for e in (f(), v())],
        "HSETNX": lambda: ["HSETNX", k(), f(), v()], "HGET": lambda: ["HGET", k(), f()], "HMGET": lambda: ["HMGET", k()] + many(f),
        "HGETALL": lambda: ["HGETALL", k()], "HDEL": lambda: ["HDEL", k()] + many(f), "HEXISTS": lambda: ["HEXISTS", k(), f()],
        "HLEN": lambda: ["HLEN", k()], "HSTRLEN": lambda: ["HSTRLEN", k(), f()], "HKEYS": lambda: ["HKEYS", k()],
        "HVALS": lambda: ["HVALS", k()], "HINCRBY": lambda: ["HINCRBY", k(), f(), i()],
        "HINCRBYFLOAT": lambda: ["HINCRBYFLOAT", k(), f(), fl()],
        "LPUSH": lambda: ["LPUSH", k()] + many(m), "RPUSH": lambda: ["RPUSH", k()] + many(m),
        "LPUSHX": lambda: ["LPUSHX", k()] + many(m), "RPUSHX": lambda: ["RPUSHX", k()] + many(m),
        "LPOP": lambda: ["LPOP", k()] + pick([[], [], ["0"], ["1"], ["2"], ["100"], ["-1"]]),
        "RPOP": lambda: ["RPOP", k()] + pick([[], [], ["0"], ["2"], ["100"], ["-1"]]),
        "LLEN": lambda: ["LLEN", k()], "LRANGE": lambda: ["LRANGE", k(), x(), x()], "LINDEX": lambda: ["LINDEX", k(), x()],
        "LSET": lambda: ["LSET", k(), x(), m()], "LTRIM": lambda: ["LTRIM", k(), x(), x()],
        "LREM": lambda: ["LREM", k(), pick(["0", "1", "-1", "2", "-2"]), m()],
        "LINSERT": lambda: ["LINSERT", k(), pick(["BEFORE", "AFTER", "before", "bogus"]), m(), m()],
        "LPOS": lambda: ["LPOS", k(), m()] + pick([[], [], ["RANK", pick(["1", "2", "-1", "0"])], ["COUNT", pick(["0", "2", "-1"])],
                                                   ["RANK", "-1", "COUNT", "0"], ["MAXLEN", pick(["1", "0", "-1"])]]),
        "LMOVE": lambda: ["LMOVE", k(), k(), pick(["LEFT", "RIGHT", "left"]), pick(["LEFT", "RIGHT", "bogus"])],
        "RPOPLPUSH": lambda: ["RPOPLPUSH", k(), k()],
        "LMPOP": lambda: ["LMPOP"] + numkeys("LMPOP", weights=False) + [pick(["LEFT", "RIGHT", "bogus"])] + pick([[], ["COUNT", pick(["1", "2", "0"])]]),
        "SADD": lambda: ["SADD", k()] + many(m), "SREM": lambda: ["SREM", k()] + many(m), "SMEMBERS": lambda: ["SMEMBERS", k()],
        "SISMEMBER": lambda: ["SISMEMBER", k(), m()], "SMISMEMBER": lambda: ["SMISMEMBER", k()] + many(m),
        "SCARD": lambda: ["SCARD", k()], "SINTER": lambda: ["SINTER"] + many(k), "SUNION": lambda: ["SUNION"] + many(k),
        "SDIFF": lambda: ["SDIFF"] + many(k), "SINTERSTORE": lambda: ["SINTERSTORE", k()] + many(k),
        "SUNIONSTORE": lambda: ["SUNIONSTORE", k()] + many(k), "SDIFFSTORE": lambda: ["SDIFFSTORE", k()] + many(k),
        "SINTERCARD": lambda: ["SINTERCARD"] + numkeys("SINTERCARD", weights=False) + pick([[], ["LIMIT", pick(["0", "1", "-1"])]]),
        "SMOVE": lambda: ["SMOVE", k(), k(), m()],
        "ZADD": lambda: ["ZADD", k()] + pick([[], [], [], ["NX"], ["XX"], ["GT"], ["LT"], ["CH"], ["XX", "CH"], ["NX", "GT"], ["INCR"], ["XX", "INCR"], ["GT", "CH"]]) + zs(),
        "ZREM": lambda: ["ZREM", k()] + many(m),
        "ZRANGE": lambda: ["ZRANGE", k(), x(), x()] + pick([[], ["WITHSCORES"], ["REV"], ["REV", "WITHSCORES"]]),
        "ZRANGE_BYSCORE": lambda: ["ZRANGE", k(), b(), b(), "BYSCORE"] + pick([[], ["REV"]]) + limit() + pick([[], ["WITHSCORES"]]),
        "ZRANGE_BYLEX": lambda: ["ZRANGE", k(), lx(), lx(), "BYLEX"] + pick([[], ["REV"]]) + limit(),
        "ZREVRANGE": lambda: ["ZREVRANGE", k(), x(), x()] + pick([[], ["WITHSCORES"]]),
        "ZSCORE": lambda: ["ZSCORE", k(), m()], "ZMSCORE": lambda: ["ZMSCORE", k()] + many(m), "ZCARD": lambda: ["ZCARD", k()],
        "ZRANK": lambda: ["ZRANK", k(), m()] + pick([[], [], ["WITHSCORE"]]), "ZREVRANK": lambda: ["ZREVRANK", k(), m()],
        "ZRANGEBYSCORE": lambda: ["ZRANGEBYSCORE", k(), b(), b()] + pick([[], ["WITHSCORES"]]) + limit(),
        "ZREVRANGEBYSCORE": lambda: ["ZREVRANGEBYSCORE", k(), b(), b()] + pick([[], ["WITHSCORES"]]) + limit(),
        "ZRANGEBYLEX": lambda: ["ZRANGEBYLEX", k(), lx(), lx()] + limit(),
        "ZREVRANGEBYLEX": lambda: ["ZREVRANGEBYLEX", k(), lx(), lx()] + limit(),
        "ZLEXCOUNT": lambda: ["ZLEXCOUNT", k(), lx(), lx()],
        "ZINCRBY": lambda: ["ZINCRBY", k(), s(), m()], "ZCOUNT": lambda: ["ZCOUNT", k(), b(), b()],
        "ZPOPMIN": lambda: ["ZPOPMIN", k()] + pick([[], ["2"], ["0"], ["-1"]]),
        "ZPOPMAX": lambda: ["ZPOPMAX", k()] + pick([[], ["2"], ["0"]]),
        "ZMPOP": lambda: ["ZMPOP"] + numkeys("ZMPOP", weights=False) + [pick(["MIN", "MAX", "bogus"])] + pick([[], ["COUNT", pick(["1", "2", "0"])]]),
        "ZREMRANGEBYRANK": lambda: ["ZREMRANGEBYRANK", k(), x(), x()],
        "ZREMRANGEBYSCORE": lambda: ["ZREMRANGEBYSCORE", k(), b(), b()],
        "ZREMRANGEBYLEX": lambda: ["ZREMRANGEBYLEX", k(), lx(), lx()],
        "ZRANGESTORE": lambda: ["ZRANGESTORE", k(), k(), x(), x()] + pick([[], ["REV"]]),
        "ZUNIONSTORE": lambda: ["ZUNIONSTORE", k()] + numkeys("ZUNIONSTORE"),
        "ZINTERSTORE": lambda: ["ZINTERSTORE", k()] + numkeys("ZINTERSTORE"),
        "ZDIFFSTORE": lambda: ["ZDIFFSTORE", k()] + numkeys("ZDIFFSTORE", weights=False),
        "ZUNION": lambda: ["ZUNION"] + numkeys("ZUNION", with_scores=True),
        "ZINTER": lambda: ["ZINTER"] + numkeys("ZINTER", with_scores=True),
        "ZDIFF": lambda: ["ZDIFF"] + numkeys("ZDIFF", with_scores=True, weights=False),
        "ZINTERCARD": lambda: ["ZINTERCARD"] + numkeys("ZINTERCARD", weights=False) + pick([[], ["LIMIT", pick(["0", "1"])]]),
        "DBSIZE": lambda: ["DBSIZE"],
    }
    string_cmds = ["SET", "GET", "INCR", "DECR", "INCRBY", "DECRBY", "INCRBYFLOAT", "MGET", "MSET", "MSETNX", "APPEND",
                   "STRLEN", "GETRANGE", "SUBSTR", "SETRANGE", "SETNX", "SETEX", "PSETEX", "GETSET", "GETDEL", "GETEX"]
    generic = ["DEL", "UNLINK", "EXISTS", "TOUCH", "EXPIRE", "PEXPIRE", "EXPIREAT", "PEXPIREAT", "PERSIST", "TTL", "PTTL",
               "EXPIRETIME", "RENAME", "RENAMENX", "COPY", "KEYS", "TYPE", "DBSIZE"]
    families = {
        "str": string_cmds,
        "gen": generic,
        "hash": [c for c in G if c.startswith("H")],
        "list": [c for c in G if c[0] == "L" or c in ("RPUSH", "RPOP", "RPUSHX", "RPOPLPUSH")],
        "set": [c for c in G if c.startswith("S") and c not in string_cmds],
        "zset": [c for c in G if c.startswith("Z")],
    }
    for n in range(count):
        fam = pick(list(families) + ["mix"])
        pool = list(G) if fam == "mix" else families[fam] * 4 + generic
        yield {"name": f"fuzz-{seed}-{n}-{fam}", "cmds": [G[pick(pool)]() for _ in range(length)], "keys": KEYS}
