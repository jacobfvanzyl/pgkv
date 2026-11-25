# Changelog

## 0.4.0

Redis conformance, row-per-element collections, a Redis command interface, and client
libraries for Rust and TypeScript. Upgrade with `ALTER EXTENSION pgkv UPDATE TO '0.4.0'`.
This release changes results and some signatures: read "Behaviour changes from 0.3.0" and
"Upgrading to 0.4.0" in the README first.

### Added

- `pgkv.call(VARIADIC text[])`: run a command as Redis spells it (`pgkv.call('SET', 'k', 'v',
  'EX', '10')`) and get the reply Redis would send, as JSONB. `pgkv.try_call` returns an error
  reply as `{"error": ...}` instead of raising it. `pgkv.pipeline(commands, atomic)` runs many
  commands in one statement with errors in place, and is `MULTI`/`EXEC` when atomic.
  `pgkv.commands()` lists the 135 commands understood.
- Client libraries in `clients/rust` (implements redis-rs's `ConnectionLike` over
  tokio-postgres or sqlx) and `clients/typescript` (mirrors ioredis over node-postgres,
  postgres.js or any `query` function). Both accept a connection, a pool or an open
  transaction.
- A conformance suite in `conformance/` that replays commands against a real Redis server and
  against pgkv and fails on any undocumented difference in a reply or in the data left
  behind. It runs 372 sequences derived from Valkey's test suite, seeded random sequences,
  and a `MULTI`/`EXEC` replay, in CI on every supported PostgreSQL version.
- Keys and expiry: `pexpire`, `expireat`, `pexpireat`, `persist`, `pttl`, `expiretime`,
  `pexpiretime`, `rename`, `renamenx`, `copy`, `touch`, `randomkey`; `expire` takes `NX`, `XX`,
  `GT` or `LT`; `scan` filters by type; `cleanup_expired` takes a limit and skips locked keys.
- Strings: `getset`; `getex` takes every Redis option; `SET ... IFEQ`, `DELIFEQ`, `DELEX`,
  `SUBSTR` through `pgkv.call`.
- Hashes: `hsetnx`, `hincrbyfloat`, `hstrlen`, `hrandfield`; `HMSET` through `pgkv.call`.
- Lists: `lpos` with a rank; `LPOS ... COUNT/MAXLEN` and `LMPOP` through `pgkv.call`.
- Sets: `smove`, `spop`, `srandmember`, `smismember`, `sinterstore`, `sunionstore`,
  `sdiffstore`, `sintercard`.
- Sorted sets: `zadd_opts` (`NX`, `XX`, `GT`, `LT`, `CH`, `INCR`), `zrange_by` (ranges by rank,
  score or member, reversed, with a limit), `zlexcount`, `zremrangebylex`, `zunion`, `zinter`,
  `zdiff`, `zintercard`, `zrandmember`; `ZREVRANGEBYSCORE`, `ZRANGEBYLEX`, `ZREVRANGEBYLEX`,
  `ZMPOP` through `pgkv.call`.
- `pgkv.set_unlogged()`: switch all of pgkv's tables to unlogged (or back) together, for
  use as a cache.
- PostgreSQL 17 and 18 in the test matrix.

### Changed

- Collections are stored one row per element (`pgkv.hash_fields`, `pgkv.list_items`,
  `pgkv.set_members`, `pgkv.zset_members`) rather than one JSONB value per key. A write to
  one field, member or list end no longer rewrites the collection: eight connections adding
  fields to one hash went from about 600 writes a second (and falling as it grew) to about
  6,800 (steady), and a push and pop on a list of 100,000 elements from minutes to a third
  of a millisecond. Small collections cost
  within about 25% of what they did, some faster (`hset`) and some slower (`hget`, short
  lists).
- Errors that are Redis error replies are raised with SQLSTATE `KV000` (`ERR ...`) or
  `KV001` (`WRONGTYPE ...`) instead of `P0001`, with Redis's message text.
- Sorted set scores are `double precision` instead of `numeric` throughout.
- Index, offset, count and expiry parameters are `bigint` instead of `integer`.
- Reads never write: they no longer delete expired keys, are `STABLE`, and work in a
  read-only transaction and on a standby. `keys()` and `dbsize()` no longer sweep the table.
- Keys, fields and members are compared in byte order (`COLLATE "C"`). The primary key
  serves prefix scans; `idx_pgkv_key_pattern` and `idx_pgkv_type` are dropped.
- String lengths and offsets are in bytes.
- The versioned scripts are generated from `sql/` by `build.sh`.

### Fixed

Differences from Redis found by the conformance suite:

- An infinite score, stored by 0.3.0 as the JSON string `"Infinity"`, broke every later read
  of that sorted set. `NaN` and out-of-range scores were accepted by `zadd`, `zincrby`,
  `incrbyfloat` and as weights.
- Empty collections were left behind by `zpopmin`, `zpopmax`, `zremrangebyrank`,
  `zremrangebyscore`, `z*store` with an empty result, and `lmove`/`rpoplpush` draining
  their source.
- `linsert` returned -1 for a missing key and 0 for a missing pivot, the reverse of Redis.
- `set_opts(..., p_get => true)` on a hash, list, set or sorted set returned the collection
  and replaced it with a string instead of raising `WRONGTYPE`.
- Members with equal scores were ordered by the database collation instead of by byte.
- `setrange` past the end of a string failed with an unrelated error; `getrange` did not
  clamp an end index before the start of the string.
- Integer arguments and stored values were parsed leniently (`' 12'`, `'12 '`, `'007'`), and
  overflow was not reported as Redis reports it.
- `sinter`, `sdiff` and the `z*store` functions treated a key of the wrong type as empty,
  and the `z*store` functions ignored plain sets.
- `srem k b b` returned 2; a negative pop count returned nothing instead of an error; `ttl`
  returned -2 for a key with under half a second left and failed for expiries more than 68
  years away; `getex ... EX 0` and `SET ... EXAT 0` were accepted; large index arguments
  overflowed.
- `mget` ran one subtransaction per key.
- `pg_dump` did not include pgkv's data: the table was not registered with
  `pg_extension_config_dump`. It is now, with the new tables.

### Upgrade notes

- The upgrade rewrites `pgkv.store` and converts collections under an exclusive lock; its
  duration grows with the amount of data.
- `tests/14-upgrade-040.sql` checks that an upgraded install equals a fresh one and that
  every kind of value survives.

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
