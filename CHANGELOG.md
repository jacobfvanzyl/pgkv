# Changelog

## 0.3.0

Concurrency fixes. Upgrade with `ALTER EXTENSION pgkv UPDATE TO '0.3.0'`; see "Upgrading to
0.3.0" in the README for copies applied as plain SQL. No function changed its signature and
stored data is untouched.

### Fixed

- `getdel` was not atomic: two sessions could both receive the value. It is now one
  `DELETE ... RETURNING`.
- `set_opts` with NX or XX, `setnx` and `msetnx` were not atomic: two sessions could both
  succeed with NX, and XX could recreate a key that was being deleted. NX is now one
  `INSERT ... ON CONFLICT`, XX one conditional `UPDATE`; `msetnx` locks all its keys first.
- `set_opts` with GET could return a value older than the one it replaced.
- Lost updates under READ COMMITTED in every function that read a value and wrote it back:
  `append`, `setrange`, `hset`, `hdel`, `hincrby`, `lpush`, `rpush`, `lpop`, `rpop`, `lset`,
  `ltrim`, `lrem`, `linsert`, `lpushx`, `rpushx`, `lmove`, `rpoplpush`, `sadd`, `srem`,
  `zadd`, `zrem`, `zincrby`, `zremrangebyrank`, `zremrangebyscore`, `zrangestore`,
  `zunionstore`, `zinterstore`, `zdiffstore`, `json_set`, `json_del`, `json_arrappend`. They
  now hold the key's row while they work. `incrby` (and `incr`, `decr`, `decrby`),
  `incrbyfloat`, `zpopmin` and `zpopmax` used advisory locks that only excluded each other,
  so a concurrent `set`, `zadd` or `del` could still be lost; they use the same row lock now.
- A read of an expired key (`get`, `exists`, `ttl`, `type`, and the other readers) deleted
  the key unconditionally, and so could delete a value another session had just written.
  The delete now applies only if the key is still expired.
- `expire` (and `getex` with a TTL) revived a key that had expired but had not been swept.
- `del` counted keys that had already expired.
- `mset` wrote its keys one statement at a time in argument order, so two calls naming the
  same keys in a different order could deadlock, and a reader could see half of one. It is
  now one statement that writes in key order. `del` with several keys locks them in key
  order too.
- `scan` counted and pattern-matched the whole table twice and deleted every expired key in
  it on each call. It now reads one page through an index and deletes nothing.

### Added

- `cas(key, expected, value, ...)`: compare-and-set for string keys, with the expiry options
  of `set_opts`. A NULL expected value requires the key to be absent. Returns 1 or 0.
- `cad(key, expected)`: compare-and-delete. Returns 1 or 0.
- `scan_prefix(prefix, after, count)`: keys with a literal prefix, by keyset paging over an
  index range, with each key's type, value and expiry.
- Index `idx_pgkv_key_pattern` on `pgkv.store (key text_pattern_ops)`.
- `stress/test_atomicity.py`: two-session tests for each of the races above.

### Changed

- `set_opts(..., p_nx => true, p_get => true)` returns the existing value when the key
  exists and NULL when it set the key (Redis 7 behaviour). 0.2.0 returned NULL either way.
- `scan` orders keys by byte value instead of the database collation, and skips expired keys
  without deleting them.
- Internal helpers `_lock_key`, `_put`, `_lock_store_keys`, `_expires_at`, `_require_string`
  and `_like_prefix` exist in the `pgkv` schema. `PUBLIC` cannot execute them.

## 0.2.0

- Cursor scans (`scan`, `hscan`, `sscan`, `zscan`), `set_opts` and string helpers (`setnx`,
  `setex`, `psetex`, `getdel`, `getex`, `incrbyfloat`, `msetnx`), list, sorted-set and JSON
  additions.
- Every `SECURITY DEFINER` function pins `search_path`.

## 0.1.0

- Initial release: strings, TTL, hashes, lists, sets, sorted sets.
