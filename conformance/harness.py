"""Differential conformance harness: replay Redis commands against a Redis-compatible
server (the oracle) and against pgkv.call, and report the first point where they disagree.

A scenario is a list of commands (each a list of strings). After every command the
harness compares the two replies and then the state of the keys the scenario touches
(type, expiry, contents), so a command that answers correctly but leaves different data
behind is caught at the step that caused it.
"""
from __future__ import annotations

import json
import os
import re
import time

import psycopg
import redis

PG_DSN = os.environ.get("PGKV_DSN", "postgresql://postgres:postgres@127.0.0.1:55811/postgres")
REDIS_URL = os.environ.get("PGKV_ORACLE_URL", "redis://127.0.0.1:55812/0")

# Replies whose order Redis does not define (pgkv returns byte order).
UNORDERED = {"KEYS", "HKEYS", "HVALS", "SMEMBERS", "SINTER", "SUNION", "SDIFF"}
PAIRS = {"HGETALL"}
# Replies that are random by design: only their shape is compared, and because the two
# servers then hold different data the scenario stops there.
RANDOM = {"SPOP", "SRANDMEMBER", "HRANDFIELD", "ZRANDMEMBER", "RANDOMKEY"}
# Replies that depend on the clock.
CLOCK = {"TTL": 1, "PTTL": 1500, "EXPIRETIME": 1, "PEXPIRETIME": 1500}
SKIP_REPLY = {"TIME"}
SCANS = {"SCAN": 1, "HSCAN": 2, "SSCAN": 2, "ZSCAN": 2}

# Lexicographic range commands are only defined for sorted sets whose scores are all equal.
LEX_COMMANDS = {"ZRANGEBYLEX", "ZREVRANGEBYLEX", "ZLEXCOUNT", "ZREMRANGEBYLEX", "ZRANGE", "ZRANGESTORE"}

DUMP = {
    "string": lambda k: ["GET", k],
    "list": lambda k: ["LRANGE", k, "0", "-1"],
    "hash": lambda k: ["HGETALL", k],
    "set": lambda k: ["SMEMBERS", k],
    "zset": lambda k: ["ZRANGE", k, "0", "-1", "WITHSCORES"],
}


class Err:
    """An error reply. Two errors are the same if their messages are, ignoring case."""

    def __init__(self, message: str):
        # redis-py drops the generic "ERR " code from the message; do the same for pgkv's.
        message = message.strip()
        self.message = message[4:] if message.startswith("ERR ") else message

    def __eq__(self, other):
        return isinstance(other, Err) and self.message.lower() == other.message.lower()

    def __repr__(self):
        return f"Err({self.message!r})"

    @property
    def code(self):
        return self.message.split(" ", 1)[0]


def connect():
    oracle = redis.Redis.from_url(REDIS_URL, protocol=2, decode_responses=True, encoding_errors="backslashreplace")
    oracle.response_callbacks.clear()
    pg = psycopg.connect(PG_DSN, autocommit=True)
    return oracle, pg


def run_oracle(oracle, argv):
    try:
        return oracle.execute_command(*argv)
    except redis.ResponseError as e:
        return Err(str(e))


def _unwrap(reply):
    if isinstance(reply, dict):
        return reply["status"]
    if isinstance(reply, list):
        return [_unwrap(x) for x in reply]
    return reply


def run_pgkv(pg, argv):
    try:
        with pg.cursor() as cur:
            cur.execute("SELECT pgkv.call(VARIADIC %s::text[])", (list(argv),))
            return _unwrap(cur.fetchone()[0])
    except psycopg.Error as e:
        state = e.sqlstate or ""
        message = (e.diag.message_primary if e.diag else None) or str(e)
        if state.startswith("KV"):
            return Err(message)
        return Err(f"SQLSTATE {state}: {message}")


def flush(oracle, pg):
    oracle.execute_command("FLUSHDB")
    with pg.cursor() as cur:
        cur.execute("SELECT pgkv.flushall()")


def _is_error(x):
    return isinstance(x, Err)


def equal(argv, a, b, unspecified=False) -> bool:
    """Whether reply a (oracle) and reply b (pgkv) to argv are the same answer.
    unspecified: the command's result is not defined by Redis for the data it ran on."""
    name = argv[0].upper()
    if _is_error(a) or _is_error(b):
        return a == b
    if name in SKIP_REPLY:
        return True
    if name in RANDOM:
        return type(a) is type(b) and (not isinstance(a, list) or len(a) == len(b))
    if name in CLOCK:
        if not (isinstance(a, int) and isinstance(b, int)):
            return a == b
        return a == b if a < 0 or b < 0 else abs(a - b) <= CLOCK[name]
    if name in SCANS:
        # Cursors are opaque. Compare the elements when both finished in one call.
        if not (isinstance(a, list) and isinstance(b, list) and len(a) == 2 and len(b) == 2):
            return a == b
        if a[0] != "0" or b[0] != "0":
            return True
        if name in ("HSCAN", "ZSCAN") and "NOVALUES" not in [x.upper() for x in argv]:
            return sorted(zip(a[1][::2], a[1][1::2])) == sorted(zip(b[1][::2], b[1][1::2]))
        return sorted(a[1]) == sorted(b[1])
    if name in LEX_COMMANDS and unspecified:
        return True
    if isinstance(a, list) and isinstance(b, list):
        if name in UNORDERED:
            return _same_unordered(a, b)
        if name in PAIRS:
            return _same_unordered([list(p) for p in zip(a[::2], a[1::2])], [list(p) for p in zip(b[::2], b[1::2])])
    return _same(a, b)


def _same(a, b):
    """Equality, except that two long decimal strings standing for the same number to 15
    significant digits are the same: INCRBYFLOAT is long double arithmetic in Redis and exact
    decimal arithmetic in pgkv, and they part ways in the last digits."""
    if isinstance(a, list) and isinstance(b, list):
        return len(a) == len(b) and all(_same(x, y) for x, y in zip(a, b))
    if a == b:
        return True
    if isinstance(a, str) and isinstance(b, str) and max(len(a), len(b)) >= 15 and _NUMBER.match(a) and _NUMBER.match(b):
        fa, fb = float(a), float(b)
        if fa == fb or abs(fa - fb) <= 2e-17:
            return True
        return fa != 0 and abs(fa - fb) / abs(fa) < 1e-15 and fa not in (float("inf"), float("-inf"))
    return False


def _same_unordered(a, b):
    if len(a) != len(b):
        return False
    rest = list(b)
    for x in a:
        for i, y in enumerate(rest):
            if _same(x, y):
                del rest[i]
                break
        else:
            return False
    return True


_NUMBER = re.compile(r"^-?[0-9]+(\.[0-9]+)?$")


def _ttl_class(x):
    return x if isinstance(x, int) and x < 0 else "set"


def state_diff(oracle, pg, keys):
    """The first difference in what the two servers hold for keys, or None."""
    for k in keys:
        a, b = run_oracle(oracle, ["TYPE", k]), run_pgkv(pg, ["TYPE", k])
        if a != b:
            return f"TYPE {k}: oracle={a!r} pgkv={b!r}"
        if a == "none":
            continue
        ta, tb = run_oracle(oracle, ["TTL", k]), run_pgkv(pg, ["TTL", k])
        if _ttl_class(ta) != _ttl_class(tb):
            return f"TTL {k}: oracle={ta!r} pgkv={tb!r}"
        cmd = DUMP[a](k)
        ra, rb = run_oracle(oracle, cmd), run_pgkv(pg, cmd)
        if not equal(cmd, ra, rb):
            return f"{' '.join(cmd)}: oracle={ra!r} pgkv={rb!r}"
    return None


def _lex_over_mixed_scores(oracle, cmd):
    name = cmd[0].upper()
    if name not in LEX_COMMANDS or len(cmd) < 3:
        return False
    if name in ("ZRANGE", "ZRANGESTORE") and "BYLEX" not in [x.upper() for x in cmd]:
        return False
    key = cmd[2] if name == "ZRANGESTORE" else cmd[1]
    scores = run_oracle(oracle, ["ZRANGE", key, "0", "-1", "WITHSCORES"])
    return isinstance(scores, list) and len(set(scores[1::2])) > 1


# What must always hold in pgkv's tables, whatever commands ran: a collection's header agrees
# with its element rows, nothing is left behind by a deleted or overwritten key.
INVARIANTS = """
SELECT 'orphan hash fields' WHERE EXISTS (
    SELECT 1 FROM pgkv.hash_fields e WHERE NOT EXISTS (SELECT 1 FROM pgkv.store s WHERE s.id = e.id AND s.type = 'hash'))
UNION ALL SELECT 'orphan list items' WHERE EXISTS (
    SELECT 1 FROM pgkv.list_items e WHERE NOT EXISTS (SELECT 1 FROM pgkv.store s WHERE s.id = e.id AND s.type = 'list'))
UNION ALL SELECT 'orphan set members' WHERE EXISTS (
    SELECT 1 FROM pgkv.set_members e WHERE NOT EXISTS (SELECT 1 FROM pgkv.store s WHERE s.id = e.id AND s.type = 'set'))
UNION ALL SELECT 'orphan zset members' WHERE EXISTS (
    SELECT 1 FROM pgkv.zset_members e WHERE NOT EXISTS (SELECT 1 FROM pgkv.store s WHERE s.id = e.id AND s.type = 'zset'))
UNION ALL SELECT 'wrong element count: ' || s.key FROM pgkv.store s
    WHERE s.type IN ('hash', 'list', 'set', 'zset') AND s.card IS DISTINCT FROM (CASE s.type
        WHEN 'hash' THEN (SELECT count(*) FROM pgkv.hash_fields e WHERE e.id = s.id)
        WHEN 'list' THEN (SELECT count(*) FROM pgkv.list_items e WHERE e.id = s.id)
        WHEN 'set' THEN (SELECT count(*) FROM pgkv.set_members e WHERE e.id = s.id)
        ELSE (SELECT count(*) FROM pgkv.zset_members e WHERE e.id = s.id) END)
UNION ALL SELECT 'empty collection: ' || s.key FROM pgkv.store s
    WHERE s.type IN ('hash', 'list', 'set', 'zset') AND (s.id IS NULL OR s.card IS NULL OR s.card < 1 OR s.value IS NOT NULL)
UNION ALL SELECT 'list positions not contiguous: ' || s.key FROM pgkv.store s
    WHERE s.type = 'list' AND EXISTS (
        SELECT 1 FROM pgkv.list_items e WHERE e.id = s.id AND (e.pos < s.head OR e.pos >= s.head + s.card))
UNION ALL SELECT 'scalar with collection fields: ' || s.key FROM pgkv.store s
    WHERE s.type IN ('string', 'json') AND (s.id IS NOT NULL OR s.card IS NOT NULL OR s.head IS NOT NULL)
UNION ALL SELECT 'placeholder row left behind: ' || s.key FROM pgkv.store s WHERE s.expires_at = '-infinity'
UNION ALL SELECT 'stored negative zero score' WHERE EXISTS (SELECT 1 FROM pgkv.zset_members e WHERE e.score::text = '-0')
"""


def invariant_violations(pg):
    with pg.cursor() as cur:
        cur.execute(INVARIANTS)
        return [r[0] for r in cur.fetchall()]


def _invariant_divergence(pg, scenario, ran):
    broken = invariant_violations(pg)
    if not broken:
        return None
    return {"kind": "invariant", "scenario": scenario["name"], "source": scenario.get("source"),
            "step": ran, "cmd": ["(after the scenario)"], "prev": scenario["cmds"][-4:],
            "oracle": "-", "pgkv": "-", "state": "; ".join(broken)}


def scenario_keys(scenario, limit=12):
    keys = {a for cmd in scenario["cmds"] for a in cmd[1:2]} | set(scenario.get("keys", []))
    return sorted(keys)[:limit]


def run_scenario(oracle, pg, scenario, keys=None):
    """Replay one scenario. Returns (commands_run, divergence or None)."""
    flush(oracle, pg)
    keys = keys or scenario_keys(scenario)
    ran = 0
    for step, cmd in enumerate(scenario["cmds"]):
        name = cmd[0].upper()
        if name == "SLEEP_MS":
            time.sleep(int(cmd[1]) / 1000)
            continue
        unspecified = _lex_over_mixed_scores(oracle, cmd)
        a, b = run_oracle(oracle, cmd), run_pgkv(pg, cmd)
        ran += 1
        kind = detail = None
        if not equal(cmd, a, b, unspecified):
            kind = "reply"
        elif (name in RANDOM or unspecified) and not _is_error(a):
            return ran, _invariant_divergence(pg, scenario, ran)
        elif name in ("INCRBYFLOAT", "HINCRBYFLOAT") and a != b:
            # Same number, different digits (long double against decimal arithmetic): the
            # stored strings now differ, so later byte-level commands would too.
            return ran, _invariant_divergence(pg, scenario, ran)
        else:
            detail = state_diff(oracle, pg, keys)
            if detail:
                kind = "state"
        if kind:
            return ran, {
                "kind": kind,
                "scenario": scenario["name"],
                "source": scenario.get("source"),
                "step": step,
                "cmd": cmd,
                "prev": scenario["cmds"][max(0, step - 4):step],
                "oracle": repr(a),
                "pgkv": repr(b),
                "state": detail,
            }
    return ran, _invariant_divergence(pg, scenario, ran)


def load_scenarios(path):
    with open(path, encoding="utf-8") as f:
        return [json.loads(line) for line in f if line.strip()]


def load_expected(path):
    """Known, documented differences: a list of {"command", "match", "why"}; match is a
    regular expression over 'oracle=<reply> pgkv=<reply> state=<detail>'."""
    if not path or not os.path.exists(path):
        return []
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def expected_reason(expected, divergence):
    text = f"oracle={divergence['oracle']} pgkv={divergence['pgkv']} state={divergence['state']}"
    name = divergence["cmd"][0].upper()
    for e in expected:
        if e.get("command") in (None, "*", name) and re.search(e["match"], text):
            return e["why"]
    return None


def run_pipeline_scenario(oracle, pg, scenario):
    """Replay a whole scenario as one MULTI/EXEC against the oracle and one atomic
    pgkv.pipeline, and compare the reply arrays. Returns (commands_run, divergence or None).
    Scenarios that sleep or use random or clock-dependent commands are skipped (0, None)."""
    cmds = scenario["cmds"]
    names = {c[0].upper() for c in cmds}
    if names & (RANDOM | set(CLOCK) | set(SCANS) | SKIP_REPLY | {"SLEEP_MS", "INCRBYFLOAT", "HINCRBYFLOAT"}):
        return 0, None
    if any(_lex_command(c) for c in cmds):
        return 0, None
    flush(oracle, pg)

    pipe = oracle.pipeline(transaction=True)
    for c in cmds:
        pipe.execute_command(*c)
    try:
        a = [Err(str(x)) if isinstance(x, Exception) else x for x in pipe.execute(raise_on_error=False)]
    except redis.ResponseError as e:
        a = Err(str(e))
    finally:
        pipe.reset()

    try:
        with pg.cursor() as cur:
            cur.execute("SELECT pgkv.pipeline(%s::jsonb, true)", (json.dumps(cmds),))
            reply = cur.fetchone()[0]
            if isinstance(reply, dict):  # the transaction was discarded
                b = Err(reply["error"])
            else:
                b = [Err(x["error"]) if isinstance(x, dict) and "error" in x else _unwrap(x) for x in reply]
    except psycopg.Error as e:
        b = Err((e.diag.message_primary if e.diag else None) or str(e))

    def diverged(step, x, y, detail=None):
        return len(cmds), {"kind": "pipeline", "scenario": scenario["name"], "source": scenario.get("source"),
                           "step": step, "cmd": cmds[step] if step is not None else ["EXEC"], "prev": [],
                           "oracle": repr(x), "pgkv": repr(y), "state": detail}

    # Redis discards the transaction when a command fails at queue time (unknown command,
    # arity its command table rules out): redis-py then raises instead of returning replies.
    if _is_error(a):
        if _is_error(b) and b.code == "EXECABORT" and state_diff(oracle, pg, scenario_keys(scenario)) is None:
            return len(cmds), _invariant_divergence(pg, scenario, len(cmds))
        return diverged(None, a, b)
    if _is_error(a) or _is_error(b):
        return diverged(None, a, b)
    if len(a) != len(b):
        return diverged(None, a, b)
    for step, (x, y) in enumerate(zip(a, b)):
        if not equal(cmds[step], x, y):
            return diverged(step, x, y)
    detail = state_diff(oracle, pg, scenario_keys(scenario))
    if detail:
        return diverged(None, a, b, detail)
    return len(cmds), _invariant_divergence(pg, scenario, len(cmds))


def _lex_command(cmd):
    name = cmd[0].upper()
    return name in LEX_COMMANDS and (name not in ("ZRANGE", "ZRANGESTORE") or "BYLEX" in [x.upper() for x in cmd])
