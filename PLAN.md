# pgkv Redis/Valkey Feature Expansion Plan

## Scope

Implement missing Redis/Valkey feature groups:

1. Cursor scans: `SCAN`, `HSCAN`, `SSCAN`, `ZSCAN`
2. Full `SET` options via compatible helper API
3. String helpers: `SETNX`, `SETEX`, `PSETEX`, `GETDEL`, `GETEX`, optional `MSETNX`, `INCRBYFLOAT`
4. List move/blocking-ish ops
5. Sorted set range/store/removal/pop ops
9. `SECURITY DEFINER` hardening
10. JSONB API

## Phase 0 — Security hardening

- Add fixed `search_path` to every `SECURITY DEFINER` function:
  ```sql
  SET search_path = pgkv, pg_temp
  ```
- Add tests proving unprivileged users can call API but cannot directly access `pgkv.store`.
- Add search-path hijack regression test.

## Phase 1 — SCAN family

Add cursor scans with deterministic key/member ordering and offset-style cursor semantics:

```sql
pgkv.scan(cursor bigint, pattern text default '*', count integer default 10)
RETURNS TABLE(cursor bigint, key text)

pgkv.hscan(key text, cursor bigint, pattern text default '*', count integer default 10)
RETURNS TABLE(cursor bigint, field text, value jsonb)

pgkv.sscan(key text, cursor bigint, pattern text default '*', count integer default 10)
RETURNS TABLE(cursor bigint, member jsonb)

pgkv.zscan(key text, cursor bigint, pattern text default '*', count integer default 10)
RETURNS TABLE(cursor bigint, member text, score numeric)
```

Tests:
- complete cursor iteration
- match filtering
- count limiting
- empty key/type mismatch
- expired key behavior

## Phase 2 — Full SET semantics

Keep current `pgkv.set(text,text,integer)` unchanged.

Add:

```sql
pgkv.set_opts(
  key text,
  value text,
  nx boolean default false,
  xx boolean default false,
  get boolean default false,
  ex integer default null,
  px bigint default null,
  exat bigint default null,
  pxat bigint default null,
  keepttl boolean default false
) RETURNS jsonb
```

Return:
- JSONB string `"OK"` on success with `get=false`
- old value on success with `get=true`
- `NULL` when condition fails

Tests:
- `NX`, `XX`, `GET`
- `EX`, `PX`, `EXAT`, `PXAT`, `KEEPTTL`
- conflicting options rejected

## Phase 3 — String helpers

Add:

```sql
pgkv.setnx(key text, value text) RETURNS integer
pgkv.setex(key text, seconds integer, value text) RETURNS text
pgkv.psetex(key text, milliseconds bigint, value text) RETURNS text
pgkv.getdel(key text) RETURNS jsonb
pgkv.getex(key text, seconds integer default null) RETURNS jsonb
pgkv.incrbyfloat(key text, increment numeric) RETURNS numeric
pgkv.msetnx(VARIADIC pairs text[]) RETURNS integer
```

Tests:
- return values
- TTL behavior
- wrong type behavior
- expired key behavior

## Phase 4 — List ops

Add non-blocking commands first:

```sql
pgkv.lpushx(key text, variadic values text[]) RETURNS integer
pgkv.rpushx(key text, variadic values text[]) RETURNS integer
pgkv.linsert(key text, before_after text, pivot text, value text) RETURNS integer
pgkv.lpos(key text, value text) RETURNS integer
pgkv.lmove(source text, destination text, from_side text, to_side text) RETURNS jsonb
pgkv.rpoplpush(source text, destination text) RETURNS jsonb
```

Blocking commands are intentionally skipped for 0.2.0 because PostgreSQL polling/timeout semantics would not be Redis-identical.

Tests:
- source/destination same key
- empty/missing lists
- wrong type
- pivot insert before/after

## Phase 5 — Sorted set expansion

Add:

```sql
pgkv.zmscore(key text, variadic members text[]) RETURNS TABLE(member text, score numeric)
pgkv.zpopmin(key text, count integer default 1) RETURNS TABLE(member text, score numeric)
pgkv.zpopmax(key text, count integer default 1) RETURNS TABLE(member text, score numeric)
pgkv.zremrangebyrank(key text, start integer, stop integer) RETURNS integer
pgkv.zremrangebyscore(key text, min numeric, max numeric) RETURNS integer
pgkv.zrangestore(dst text, src text, start integer, stop integer, rev boolean default false) RETURNS integer
pgkv.zinterstore(destination text, keys text[], weights numeric[] default null, aggregate text default 'SUM') RETURNS integer
pgkv.zunionstore(destination text, keys text[], weights numeric[] default null, aggregate text default 'SUM') RETURNS integer
pgkv.zdiffstore(destination text, keys text[]) RETURNS integer
```

Tests:
- pop ordering and removal
- rank/score removal
- tie-breaking by member
- aggregate `SUM|MIN|MAX`
- weights
- wrong types

## Phase 6 — JSONB API

Add `json` type and JSONB helpers:

```sql
pgkv.json_set(key text, path text[], value jsonb) RETURNS text
pgkv.json_get(key text, path text[] default null) RETURNS jsonb
pgkv.json_del(key text, path text[]) RETURNS integer
pgkv.json_type(key text, path text[] default null) RETURNS text
pgkv.json_objkeys(key text, path text[] default null) RETURNS SETOF text
pgkv.json_arrappend(key text, path text[], variadic values jsonb[]) RETURNS integer
```

Tests:
- nested set/get/delete
- object keys
- array append
- wrong type
- missing path
- JSON null vs SQL NULL

## Acceptance criteria

- Existing pgTAP suite passes.
- Stress suite passes.
- New tests cover each new function.
- No `SECURITY DEFINER` function lacks fixed `search_path`.
- README documents Redis-compatible and PostgreSQL-adapted semantics.
- Existing public function signatures remain compatible.
