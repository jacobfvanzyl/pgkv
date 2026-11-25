# pgkv

A key-value store PostgreSQL Trusted Language Extension (TLE) that provides familiar Redis commands directly in your PostgreSQL database.

## Features

- **Redis-Compatible API**: Familiar commands like SET, GET, DEL, INCR, EXPIRE, and more
- **JSONB Storage**: Values stored as JSONB for type flexibility and rich querying capabilities
- **Pattern Matching**: Full support for Redis glob patterns (`*`, `?`, `[...]`)
- **TTL Support**: Automatic expiration of keys with time-to-live functionality
- **Trusted Language Extension**: Written in PL/pgSQL, safe to run in restricted environments
- **PostgreSQL Native**: Leverage PostgreSQL's ACID guarantees and consistency
- **Simple Integration**: Works alongside your existing PostgreSQL data

## Installation

### Local Installation

```sql
CREATE EXTENSION pgkv;
```

The default extension version is `0.3.0`. To upgrade an existing install (from `0.1.0` or `0.2.0`):

```sql
ALTER EXTENSION pgkv UPDATE TO '0.3.0';
```

pgkv is packaged with PostgreSQL/pg_tle-style versioned scripts:

- `pgkv--0.1.0.sql`, `pgkv--0.2.0.sql`, `pgkv--0.3.0.sql`
- `pgkv--0.1.0--0.2.0.sql`, `pgkv--0.2.0--0.3.0.sql`

See [Upgrading to 0.3.0](#upgrading-to-030) for installs that applied pgkv as plain SQL, and
[CHANGELOG.md](CHANGELOG.md) for what changed.

Because objects intentionally live in the `pgkv` schema, the extension is not relocatable.

### Prerequisites

- PostgreSQL 12 or higher
- PL/pgSQL language support (enabled by default)

## Security Model

pgkv uses a secure permission model that provides safe access to the key-value store:

- **Function-Based Access**: All operations go through provided functions - users cannot directly access the underlying storage table
- **SECURITY DEFINER Functions**: Functions execute with the extension owner's privileges, allowing them to access the internal `pgkv.store` table on behalf of any user
- **No Direct Table Access**: Regular users do not need (and should not have) direct permissions on the `pgkv.store` table
- **Controlled API**: All data access is controlled through the well-defined function API, preventing unauthorized table manipulation

This model ensures that:
- Any user can safely call pgkv functions (e.g., `pgkv.set()`, `pgkv.get()`)
- The underlying storage remains protected from direct SQL manipulation
- All operations maintain data integrity through the function layer
- No additional permission grants are needed for end users

**One shared keyspace.** pgkv has no per-user keys. `EXECUTE` on its functions is granted to
`PUBLIC`, so every role that can reach the `pgkv` schema can read, overwrite and delete every
key. Do not expose the `pgkv` schema through an API that maps end users to database roles
(PostgREST, as in Supabase) unless that is what you want. To restrict it to one role:

```sql
REVOKE ALL ON SCHEMA pgkv FROM PUBLIC;
REVOKE ALL ON ALL FUNCTIONS IN SCHEMA pgkv FROM PUBLIC;
GRANT USAGE ON SCHEMA pgkv TO my_app_role;
GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA pgkv TO my_app_role;
```

**Example:**

```sql
-- Regular user can call functions without table permissions
SELECT pgkv.set('mykey', 'myvalue');  -- ✓ Works
SELECT pgkv.get('mykey');             -- ✓ Works

-- But cannot directly access the storage table
SELECT * FROM pgkv.store;             -- ✗ Permission denied
INSERT INTO pgkv.store VALUES (...);  -- ✗ Permission denied
```

## JSONB Storage

pgkv stores all values as JSONB, providing flexibility and powerful querying capabilities:

```sql
-- Simple string values (auto-converted to JSONB)
SELECT pgkv.set('name', 'Alice');
SELECT pgkv.get('name');
-- Returns: "Alice" (JSONB string)

-- Store complex JSON objects
SELECT pgkv.set('user:1000', '{"name":"Alice","age":30,"active":true}');

-- Query nested fields
SELECT pgkv.get('user:1000')->>'name';
-- Returns: Alice

-- Numeric values (counters)
SELECT pgkv.incr('counter');
-- Stored as: 1 (JSONB number, not string)

SELECT pgkv.get('counter');
-- Returns: 1 (JSONB number)
```

### Type Safety

Each key has an associated type (`string`, `list`, `set`, `hash`, `zset`). Operations enforce type correctness and raise WRONGTYPE errors when operations don't match the key's type:

```sql
-- Check key type
SELECT pgkv.type('user:1000');
-- Returns: 'string'

-- Returns 'none' for non-existent keys
SELECT pgkv.type('missing');
-- Returns: 'none'

-- Type enforcement
SELECT pgkv.set('mystring', 'value');
SELECT pgkv.hset('mystring', 'field', 'value');
-- ERROR: WRONGTYPE Operation against a key holding the wrong kind of value
```

### Storage Patterns

Different data types use optimized JSONB storage:

| Type | JSONB Storage | Example |
|------|---------------|---------|
| string | String or number | `"Hello"` or `42` |
| hash | Object | `{"field1": "value1", "field2": "100"}` |
| list | Array | `["item1", "item2", "item3"]` |
| set | Array (unique) | `["member1", "member2"]` |
| zset | Object (scores) | `{"member1": 100.5, "member2": 200}` |

## Usage

### Basic Key-Value Operations

```sql
-- Set a key
SELECT pgkv.set('user:1000', 'John Doe');

-- Get a key (returns JSONB)
SELECT pgkv.get('user:1000');
-- Returns: "John Doe" (JSONB string)

-- Extract as text
SELECT pgkv.get('user:1000') #>> '{}';
-- Returns: John Doe

-- Set a key with TTL (expires in 60 seconds)
SELECT pgkv.set('session:abc123', 'active', 60);

-- Check if key exists
SELECT pgkv.exists('user:1000');
-- Returns: 1

-- Delete a key
SELECT pgkv.del('user:1000');
-- Returns: 1
```

### TTL Operations

```sql
-- Set expiration on existing key (60 seconds)
SELECT pgkv.expire('user:1000', 60);

-- Check time to live
SELECT pgkv.ttl('user:1000');
-- Returns: seconds remaining, -1 if no expiration, -2 if key doesn't exist
```

### Counter Operations

```sql
-- Increment a counter
SELECT pgkv.incr('page:views');
-- Returns: 1

SELECT pgkv.incr('page:views');
-- Returns: 2

-- Decrement a counter
SELECT pgkv.decr('page:views');
-- Returns: 1

-- Increment by specific amount
SELECT pgkv.incrby('score', 10);
-- Returns: 10

-- Decrement by specific amount
SELECT pgkv.decrby('score', 5);
-- Returns: 5
```

### Additional String Operations

```sql
-- Append to a string
SELECT pgkv.append('message', 'Hello');
-- Returns: 5 (length after append)

SELECT pgkv.append('message', ' World');
-- Returns: 11

-- Get string length
SELECT pgkv.strlen('message');
-- Returns: 11

-- Get substring (supports negative indices)
SELECT pgkv.getrange('message', 0, 4);
-- Returns: 'Hello'

SELECT pgkv.getrange('message', -5, -1);
-- Returns: 'World'

-- Overwrite part of string
SELECT pgkv.setrange('message', 0, 'Jello');
-- Returns: 11 (new length)
-- message is now 'Jello World'
```

### HASH Operations

Hashes are maps of field-value pairs, perfect for representing objects:

```sql
-- Set single field in hash
SELECT pgkv.hset('user:1000', 'name', 'Alice');
-- Returns: 1 (number of fields added)

-- Set multiple fields at once
SELECT pgkv.hset('user:1000', 'age', '30', 'city', 'NYC', 'active', 'true');
-- Returns: 3 (number of new fields)

-- Get single field
SELECT pgkv.hget('user:1000', 'name');
-- Returns: "Alice" (JSONB)

-- Get multiple fields
SELECT * FROM pgkv.hmget('user:1000', 'name', 'age', 'city');
-- Returns table: (name, "Alice"), (age, "30"), (city, "NYC")

-- Get all fields and values
SELECT * FROM pgkv.hgetall('user:1000');
-- Returns table with all field-value pairs

-- Get all field names
SELECT pgkv.hkeys('user:1000');
-- Returns: name, age, city, active

-- Get all values
SELECT pgkv.hvals('user:1000');
-- Returns all values as JSONB

-- Check if field exists
SELECT pgkv.hexists('user:1000', 'name');
-- Returns: 1

-- Get number of fields
SELECT pgkv.hlen('user:1000');
-- Returns: 4

-- Delete fields
SELECT pgkv.hdel('user:1000', 'city');
-- Returns: 1 (number of fields removed)

-- Increment numeric field
SELECT pgkv.hincrby('user:1000', 'age', 1);
-- Returns: 31 (new value)
```

### LIST Operations

Lists are ordered collections, supporting negative indices and operations at both ends:

```sql
-- Push to head of list (prepends)
SELECT pgkv.lpush('queue', 'task1');
-- Returns: 1 (new length)

SELECT pgkv.lpush('queue', 'task2', 'task3');
-- Returns: 3 (elements pushed in reverse order: task3, task2, task1)

-- Push to tail of list (appends)
SELECT pgkv.rpush('queue', 'task4');
-- Returns: 4

-- Get list length
SELECT pgkv.llen('queue');
-- Returns: 4

-- Get range of elements (supports negative indices)
SELECT * FROM pgkv.lrange('queue', 0, -1);
-- Returns all elements: task3, task2, task1, task4

SELECT * FROM pgkv.lrange('queue', 0, 1);
-- Returns first 2 elements: task3, task2

SELECT * FROM pgkv.lrange('queue', -2, -1);
-- Returns last 2 elements: task1, task4

-- Get element by index (supports negative indices)
SELECT pgkv.lindex('queue', 0);
-- Returns: "task3" (first element)

SELECT pgkv.lindex('queue', -1);
-- Returns: "task4" (last element)

-- Set element by index
SELECT pgkv.lset('queue', 0, 'new_task');
-- Returns: 'OK'

-- Pop from head
SELECT pgkv.lpop('queue');
-- Returns: "new_task" (and removes it)

-- Pop multiple elements from head
SELECT * FROM pgkv.lpop('queue', 2);
-- Returns 2 elements and removes them

-- Pop from tail
SELECT pgkv.rpop('queue');
-- Returns last element and removes it

-- Trim list to specific range
SELECT pgkv.ltrim('queue', 0, 9);
-- Keeps only first 10 elements

-- Remove elements by value
-- count > 0: remove first N occurrences
-- count < 0: remove last N occurrences
-- count = 0: remove all occurrences
SELECT pgkv.lrem('queue', 2, 'task1');
-- Removes first 2 occurrences of 'task1'
```

### SET Operations

Sets are unordered collections of unique members:

```sql
-- Add members to set (enforces uniqueness)
SELECT pgkv.sadd('tags', 'redis', 'database', 'nosql');
-- Returns: 3 (number of members added)

SELECT pgkv.sadd('tags', 'redis');
-- Returns: 0 (member already exists)

-- Get all members
SELECT * FROM pgkv.smembers('tags');
-- Returns all members: redis, database, nosql

-- Check membership
SELECT pgkv.sismember('tags', 'redis');
-- Returns: 1 (is member)

SELECT pgkv.sismember('tags', 'sql');
-- Returns: 0 (not a member)

-- Get set size
SELECT pgkv.scard('tags');
-- Returns: 3

-- Remove members
SELECT pgkv.srem('tags', 'nosql');
-- Returns: 1 (number removed)

-- Set operations
SELECT pgkv.sadd('set1', 'a', 'b', 'c');
SELECT pgkv.sadd('set2', 'b', 'c', 'd');
SELECT pgkv.sadd('set3', 'c', 'd', 'e');

-- Intersection
SELECT * FROM pgkv.sinter('set1', 'set2');
-- Returns: b, c

SELECT * FROM pgkv.sinter('set1', 'set2', 'set3');
-- Returns: c

-- Union
SELECT * FROM pgkv.sunion('set1', 'set2');
-- Returns: a, b, c, d

-- Difference (set1 - set2 - set3...)
SELECT * FROM pgkv.sdiff('set1', 'set2');
-- Returns: a (elements in set1 but not in set2)
```

### SORTED SET Operations

Sorted sets maintain members ordered by score, supporting range queries and rank operations:

```sql
-- Add members with scores
SELECT pgkv.zadd('leaderboard', '100', 'alice', '200', 'bob', '150', 'charlie');
-- Returns: 3 (number of members added)

-- Update score
SELECT pgkv.zadd('leaderboard', '250', 'alice');
-- Returns: 0 (member updated, not added)

-- Get score of member
SELECT pgkv.zscore('leaderboard', 'alice');
-- Returns: 250

-- Get sorted set size
SELECT pgkv.zcard('leaderboard');
-- Returns: 3

-- Get range by rank (0-based, ascending order)
SELECT * FROM pgkv.zrange('leaderboard', 0, -1, false);
-- Returns members ordered by score: charlie, bob, alice

SELECT * FROM pgkv.zrange('leaderboard', 0, 1, true);
-- Returns members with scores: (charlie, 150), (bob, 200)

-- Get range in reverse order (descending by score)
SELECT * FROM pgkv.zrevrange('leaderboard', 0, 1, true);
-- Returns: (alice, 250), (bob, 200)

-- Get rank of member (0-based position in ascending order)
SELECT pgkv.zrank('leaderboard', 'charlie');
-- Returns: 0 (lowest score)

SELECT pgkv.zrank('leaderboard', 'alice');
-- Returns: 2 (highest score)

-- Get reverse rank (0-based position in descending order)
SELECT pgkv.zrevrank('leaderboard', 'alice');
-- Returns: 0 (highest score, first in reverse order)

-- Get members by score range
SELECT * FROM pgkv.zrangebyscore('leaderboard', 150, 200, true);
-- Returns members with scores between 150-200: (charlie, 150), (bob, 200)

-- Count members in score range
SELECT pgkv.zcount('leaderboard', 100, 200);
-- Returns: 2

-- Increment member score
SELECT pgkv.zincrby('leaderboard', 50, 'charlie');
-- Returns: 200 (new score)

-- Remove members
SELECT pgkv.zrem('leaderboard', 'bob');
-- Returns: 1 (number removed)
```

### Multi-Key Operations

```sql
-- Set multiple keys at once
SELECT pgkv.mset('key1', 'value1', 'key2', 'value2', 'key3', 'value3');

-- Get multiple keys at once
SELECT * FROM pgkv.mget('key1', 'key2', 'key3');
-- Returns table with key-value pairs

-- Delete multiple keys
SELECT pgkv.del('key1', 'key2', 'key3');
-- Returns: 3
```

### Key Inspection

pgkv supports Redis glob pattern syntax:

```sql
-- Find all keys (use with caution on large datasets)
SELECT * FROM pgkv.keys('*');

-- Asterisk (*) - matches zero or more characters
SELECT * FROM pgkv.keys('user:*');
-- Matches: user:1, user:100, user:admin, etc.

-- Question mark (?) - matches exactly one character
SELECT * FROM pgkv.keys('user:?');
-- Matches: user:1, user:a
-- Does NOT match: user:10, user:abc

-- Character classes ([...]) - matches any character in brackets
SELECT * FROM pgkv.keys('user:[123]');
-- Matches: user:1, user:2, user:3

-- Character ranges
SELECT * FROM pgkv.keys('user:[a-c]');
-- Matches: user:a, user:b, user:c

-- Get database size (number of keys)
SELECT pgkv.dbsize();

-- Check key type
SELECT pgkv.type('user:1000');
-- Returns: 'string', 'list', 'set', 'hash', 'zset', or 'none'
```

**Note**: Character class patterns (`[...]`) automatically use PostgreSQL regex for matching. Simple patterns (`*`, `?`) use optimized LIKE queries.

### Maintenance

```sql
-- Clean up expired keys manually
SELECT pgkv.cleanup_expired();
-- Returns: number of deleted keys

-- Delete all keys
SELECT pgkv.flushall();
```


## Version 0.2.0 Additions

pgkv 0.2.0 adds more Redis/Valkey-compatible helpers while keeping the 0.1.0 API compatible.

### Cursor Scans

Offset-style cursor scans provide deterministic iteration. Cursor `0` means complete.

```sql
SELECT * FROM pgkv.scan(0, 'user:*', 100);
SELECT * FROM pgkv.hscan('hash', 0, '*', 100);
SELECT * FROM pgkv.sscan('set', 0, '*', 100);
SELECT * FROM pgkv.zscan('zset', 0, '*', 100);
```

### Extended String Commands

```sql
SELECT pgkv.setnx('key', 'value');              -- 1 if set, 0 if key exists
SELECT pgkv.setex('key', 60, 'value');          -- SET with seconds TTL
SELECT pgkv.psetex('key', 60000, 'value');      -- SET with milliseconds TTL
SELECT pgkv.getdel('key');                      -- GET then DEL
SELECT pgkv.getex('key', 60);                   -- GET then EXPIRE
SELECT pgkv.incrbyfloat('float', 1.5);
SELECT pgkv.msetnx('k1', 'v1', 'k2', 'v2');
```

Full SET options are exposed through `set_opts`:

```sql
SELECT pgkv.set_opts('key', 'value', p_nx := true);
SELECT pgkv.set_opts('key', 'value', p_xx := true, p_get := true);
SELECT pgkv.set_opts('key', 'value', p_ex := 60);
SELECT pgkv.set_opts('key', 'value', p_px := 60000);
SELECT pgkv.set_opts('key', 'value', p_keepttl := true);
```

### Extended List Commands

```sql
SELECT pgkv.lpushx('list', 'value');
SELECT pgkv.rpushx('list', 'value');
SELECT pgkv.linsert('list', 'BEFORE', 'pivot', 'value');
SELECT pgkv.lpos('list', 'value');
SELECT pgkv.lmove('source', 'destination', 'LEFT', 'RIGHT');
SELECT pgkv.rpoplpush('source', 'destination');
```

Blocking list commands (`BLPOP`, `BRPOP`) are intentionally not implemented in 0.2.0 because PostgreSQL polling/timeout behavior would not be Redis-identical.

### Extended Sorted Set Commands

```sql
SELECT * FROM pgkv.zmscore('zset', 'a', 'b', 'missing');
SELECT * FROM pgkv.zpopmin('zset', 2);
SELECT * FROM pgkv.zpopmax('zset', 2);
SELECT pgkv.zremrangebyrank('zset', 0, 10);
SELECT pgkv.zremrangebyscore('zset', 100, 200);
SELECT pgkv.zrangestore('dst', 'src', 0, -1, false);
SELECT pgkv.zunionstore('dst', ARRAY['z1', 'z2']);
SELECT pgkv.zinterstore('dst', ARRAY['z1', 'z2']);
SELECT pgkv.zdiffstore('dst', ARRAY['z1', 'z2']);
```

`zunionstore` and `zinterstore` support optional weights and aggregate mode (`SUM`, `MIN`, `MAX`).

### JSONB Commands

pgkv 0.2.0 adds a `json` storage type with PostgreSQL-array paths.

```sql
SELECT pgkv.json_set('doc', NULL, '{"name":"Ada","tags":["math"]}'::jsonb);
SELECT pgkv.json_get('doc', ARRAY['name']);
SELECT pgkv.json_type('doc', ARRAY['tags']);
SELECT * FROM pgkv.json_objkeys('doc');
SELECT pgkv.json_arrappend('doc', ARRAY['tags'], '"logic"'::jsonb);
SELECT pgkv.json_del('doc', ARRAY['name']);
```

### Security Hardening

All `SECURITY DEFINER` functions set a fixed function-level search path:

```sql
SET search_path = pgkv, pg_temp
```

This prevents caller `search_path` hijacking.

## Version 0.3.0: Atomic Operations

0.3.0 fixes concurrency bugs in 0.2.0 and earlier, and adds compare-and-set and a prefix scan.
No function changed its signature.

### Concurrency guarantees

At PostgreSQL's default isolation level (READ COMMITTED), every pgkv function that writes is
atomic with respect to every other pgkv function on the same key: the result is what you
would get if the calls had run one after the other, and no acknowledged write is lost.

- **One statement** where that is enough: `set`, `getdel` (`DELETE ... RETURNING`),
  `set_opts`/`setnx` with NX (`INSERT ... ON CONFLICT ... WHERE` the existing key has expired)
  or XX (a conditional `UPDATE`), `cas`, `cad`, `expire`, `del`.
- **A row lock** where the function has to read the value to compute the new one:
  `incr`/`decr`/`incrby`/`decrby`/`incrbyfloat`, `append`, `setrange`, `getex`, `set_opts` with
  GET, `msetnx`, and every hash, list, set, sorted-set and JSON mutator. They take the key's
  row `FOR UPDATE` first; for a key that does not exist yet they insert a placeholder row
  that only their own transaction can see, so two sessions creating the same key also queue.
- **Several keys** (`mset`, `msetnx`, `del`, `lmove`, `rpoplpush`, `zrangestore`,
  `zunionstore`, `zinterstore`, `zdiffstore`) are locked in key order, so two such calls
  cannot deadlock each other. `mset` is one statement. The `*store` commands share-lock
  their sources while they read them.
- **An expired key is absent** for every write: NX succeeds on it, XX and `expire` do not
  revive it, `del` does not count it.
- **Reads never destroy a write.** A read that finds an expired key still removes it (see
  [Lazy Expiration](#lazy-expiration)), but only if it is still expired at that moment.

Locks are held until the calling transaction ends, like any row lock. In a transaction that
touches several keys, touch them in a consistent order or be ready to retry on a deadlock
error. At REPEATABLE READ or SERIALIZABLE, a write that lost a race fails with a
serialization error (`40001`) to be retried, as PostgreSQL does for any row.

### Compare-and-set and compare-and-delete

```sql
-- Write only if the key still holds what was read. Returns 1 if it wrote, 0 if not.
SELECT pgkv.cas('grant:42', '{"v":1}', '{"v":2}');

-- With an expiry (same options as set_opts): p_ex, p_px, p_exat, p_pxat, p_keepttl
SELECT pgkv.cas('grant:42', '{"v":2}', '{"v":3}', p_exat => 1790000000);

-- A NULL expected value means "only if the key does not exist"
SELECT pgkv.cas('code:abc', NULL, 'issued');

-- Delete only if the key still holds what was read
SELECT pgkv.cad('code:abc', 'issued');
```

`cas(key, expected, value, ...)` and `cad(key, expected)` are **pgkv extensions, not classic
Redis commands**. They exist for optimistic concurrency: read a value, decide, and write back
only if nobody changed it in between. Of several sessions that read the same value, exactly
one `cas` returns 1; the others return 0 and can re-read. That is what makes a one-time code
single-use or a token rotation safe.

- The expected value is compared as text with what `pgkv.get(key) #>> '{}'` returns, so pass
  what you read. They work on string keys only; another type raises `WRONGTYPE`.
- `cas` with a NULL expected value is `SET NX`: NULL is what `get` returns for a missing key,
  so "what I read" works in both cases. An expired key counts as missing.
- Without an expiry option `cas` clears the key's expiry, as `SET` does; pass
  `p_keepttl => true` to keep it.
- Both return 1 or 0, like `setnx` and `expire`.
- Why these names: Redis had no such command for most of its life (the idiom was
  `WATCH`/`MULTI` or a Lua script). Redis 8.4 added `SET key value IFEQ expected` and
  `DELEX key IFEQ expected`, which is what `cas` and `cad` do. They are separate functions
  rather than more `set_opts` options so that the answer is a plain 1 or 0 and `set_opts`
  keeps its signature. The names follow the long-standing `CAS`/`CAD` usage.
- Comparing values cannot tell "unchanged" from "changed and changed back" (ABA). If that
  matters, put a version or a random nonce in the value.

### Prefix scan

```sql
-- Keys that start with 'grant:', 100 at a time, in byte order
SELECT key, type, value, expires_at FROM pgkv.scan_prefix('grant:', NULL, 100);
-- Next page: pass the last key of the previous one. A short page is the last.
SELECT key, type, value, expires_at FROM pgkv.scan_prefix('grant:', 'grant:0099', 100);
```

`scan_prefix(prefix, after, count)` is also a pgkv extension. It pages by key (keyset paging),
so keys added or removed during a sweep never shift, repeat or hide other keys, which
`scan`'s offset cursor cannot promise. The prefix is literal (`%`, `_`, `*` have no special
meaning). It reads a range of the `idx_pgkv_key_pattern` index (`text_pattern_ops`), returns
each key's type, value and expiry so a listing needs no second query per key, skips expired
keys and writes nothing.

`scan(cursor, pattern, count)` keeps its signature and offset cursor, but no longer counts
the matching keys or deletes expired keys across the whole table on every call, and a
pattern with a fixed prefix (`grant:*`) uses the same index. Its order is now byte order
rather than the database collation's.

### Behaviour changes from 0.2.0

- `set_opts(..., p_nx => true, p_get => true)` returns the existing value when the key exists
  (and sets nothing), and NULL when it set the key, as Redis 7 does. 0.2.0 returned NULL in
  both cases.
- `del` does not count keys that had already expired; `expire` returns 0 for them instead of
  bringing them back.
- `scan` returns keys in byte order and leaves expired keys for `cleanup_expired()`.
- Six internal helper functions (`pgkv._lock_key` and others, names starting with `_`) are
  new. They are not part of the API and `PUBLIC` cannot execute them.

### Upgrading to 0.3.0

As an extension:

```sql
ALTER EXTENSION pgkv UPDATE TO '0.3.0';   -- from 0.2.0, or from 0.1.0 (runs both steps)
```

The upgrade keeps all data, adds one index (`idx_pgkv_key_pattern`, built while holding a
lock on `pgkv.store`; quick unless the table is very large) and replaces the functions. The
three new API functions get the same grantees as `set_opts`, `del` and `scan`, so an install
that revoked `PUBLIC` stays closed.

If pgkv was applied as plain SQL (for example copied into a migration) rather than installed
with `CREATE EXTENSION`, run the upgrade scripts as plain SQL in a new migration, in order:
`pgkv--0.1.0--0.2.0.sql` if the copy predates 0.2.0, then `pgkv--0.2.0--0.3.0.sql`. They are
idempotent and `pgkv--0.2.0--0.3.0.sql` redefines every function, so the result is exactly
0.3.0. Two things to check on such a copy:

- If the copy lives in a schema with another name, rename `pgkv.` and
  `search_path = pgkv,` in the scripts the same way the copy was renamed.
- Every function becomes `SECURITY DEFINER`. If the copy's functions were not (very early
  copies), roles that could not use pgkv before, because they have no rights on
  `pgkv.store`, now can if they have `USAGE` on the schema and `EXECUTE` on the functions.
  Add the `REVOKE`s from [Security Model](#security-model) to the same migration.

## API Reference

### Quick Reference Tables

#### Basic Operations

| Function | Description | Returns |
|----------|-------------|---------|
| `pgkv.set(key, value, ttl?)` | Set key to value with optional TTL in seconds | 'OK' |
| `pgkv.get(key)` | Get value of key | JSONB or NULL |
| `pgkv.del(keys...)` | Delete one or more keys | Number of deleted keys |
| `pgkv.exists(keys...)` | Check if keys exist | Number of existing keys |
| `pgkv.cas(key, expected, value, ...)` | Compare-and-set (pgkv extension): write only if the key holds `expected`; NULL expects absence | 1 if written, 0 otherwise |
| `pgkv.cad(key, expected)` | Compare-and-delete (pgkv extension) | 1 if deleted, 0 otherwise |
| `pgkv.type(key)` | Get type of key | 'string', 'list', 'set', 'hash', 'zset', or 'none' |

#### TTL Operations

| Function | Description | Returns |
|----------|-------------|---------|
| `pgkv.expire(key, seconds)` | Set TTL on key | 1 if set, 0 if key doesn't exist |
| `pgkv.ttl(key)` | Get remaining TTL | Seconds, -1 if no expiry, -2 if no key |

#### String/Counter Operations

| Function | Description | Returns |
|----------|-------------|---------|
| `pgkv.incr(key)` | Increment integer value by 1 (stored as JSONB number) | New value (BIGINT) |
| `pgkv.decr(key)` | Decrement integer value by 1 (stored as JSONB number) | New value (BIGINT) |
| `pgkv.incrby(key, amount)` | Increment by amount (stored as JSONB number) | New value (BIGINT) |
| `pgkv.decrby(key, amount)` | Decrement by amount (stored as JSONB number) | New value (BIGINT) |
| `pgkv.append(key, value)` | Append string to value | New length (INTEGER) |
| `pgkv.strlen(key)` | Get string length | Length (INTEGER) |
| `pgkv.getrange(key, start, end)` | Get substring (supports negative indices) | TEXT |
| `pgkv.setrange(key, offset, value)` | Overwrite part of string | New length (INTEGER) |

#### HASH Operations

| Function | Description | Returns |
|----------|-------------|---------|
| `pgkv.hset(key, field, value, ...)` | Set field(s) in hash | Number of fields added |
| `pgkv.hget(key, field)` | Get field value | JSONB or NULL |
| `pgkv.hmget(key, field, ...)` | Get multiple fields | TABLE(field TEXT, value JSONB) |
| `pgkv.hgetall(key)` | Get all fields and values | TABLE(field TEXT, value JSONB) |
| `pgkv.hdel(key, field, ...)` | Delete field(s) | Number of fields deleted |
| `pgkv.hexists(key, field)` | Check if field exists | 1 if exists, 0 otherwise |
| `pgkv.hlen(key)` | Count fields in hash | INTEGER |
| `pgkv.hkeys(key)` | Get all field names | SETOF TEXT |
| `pgkv.hvals(key)` | Get all values | SETOF JSONB |
| `pgkv.hincrby(key, field, increment)` | Increment field value | New value (BIGINT) |

#### LIST Operations

| Function | Description | Returns |
|----------|-------------|---------|
| `pgkv.lpush(key, value, ...)` | Push value(s) to head | New length |
| `pgkv.rpush(key, value, ...)` | Push value(s) to tail | New length |
| `pgkv.lpop(key, count?)` | Pop from head | JSONB or SETOF JSONB |
| `pgkv.rpop(key, count?)` | Pop from tail | JSONB or SETOF JSONB |
| `pgkv.llen(key)` | Get list length | INTEGER |
| `pgkv.lrange(key, start, stop)` | Get range (supports negative indices) | SETOF JSONB |
| `pgkv.lindex(key, index)` | Get element by index | JSONB or NULL |
| `pgkv.lset(key, index, value)` | Set element by index | 'OK' |
| `pgkv.ltrim(key, start, stop)` | Trim to range | 'OK' |
| `pgkv.lrem(key, count, value)` | Remove elements by value | Number removed |

#### SET Operations

| Function | Description | Returns |
|----------|-------------|---------|
| `pgkv.sadd(key, member, ...)` | Add member(s) to set | Number added |
| `pgkv.srem(key, member, ...)` | Remove member(s) | Number removed |
| `pgkv.smembers(key)` | Get all members | SETOF JSONB |
| `pgkv.sismember(key, member)` | Check membership | 1 if member, 0 otherwise |
| `pgkv.scard(key)` | Get set size | INTEGER |
| `pgkv.sinter(key, ...)` | Set intersection | SETOF JSONB |
| `pgkv.sunion(key, ...)` | Set union | SETOF JSONB |
| `pgkv.sdiff(key, ...)` | Set difference | SETOF JSONB |

#### SORTED SET Operations

| Function | Description | Returns |
|----------|-------------|---------|
| `pgkv.zadd(key, score, member, ...)` | Add member(s) with score | Number added |
| `pgkv.zrem(key, member, ...)` | Remove member(s) | Number removed |
| `pgkv.zrange(key, start, stop, withscores?)` | Get range by rank (ascending) | TABLE(member TEXT, score NUMERIC) |
| `pgkv.zrevrange(key, start, stop, withscores?)` | Get range by rank (descending) | TABLE(member TEXT, score NUMERIC) |
| `pgkv.zscore(key, member)` | Get member score | NUMERIC or NULL |
| `pgkv.zcard(key)` | Get sorted set size | INTEGER |
| `pgkv.zrank(key, member)` | Get rank (0-based, ascending) | INTEGER or NULL |
| `pgkv.zrevrank(key, member)` | Get reverse rank (0-based, descending) | INTEGER or NULL |
| `pgkv.zrangebyscore(key, min, max, withscores?)` | Get members by score range | TABLE(member TEXT, score NUMERIC) |
| `pgkv.zincrby(key, increment, member)` | Increment member score | New score (NUMERIC) |
| `pgkv.zcount(key, min, max)` | Count members in score range | INTEGER |

#### Multi-Key Operations

| Function | Description | Returns |
|----------|-------------|---------|
| `pgkv.mget(keys...)` | Get multiple keys | TABLE(key TEXT, value JSONB) |
| `pgkv.mset(k1, v1, k2, v2, ...)` | Set multiple keys | 'OK' |

#### Key Inspection

| Function | Description | Returns |
|----------|-------------|---------|
| `pgkv.keys(pattern?)` | Find keys matching Redis glob pattern (`*`, `?`, `[...]`) | SETOF TEXT |
| `pgkv.dbsize()` | Count total keys | BIGINT |
| `pgkv.scan(cursor, pattern?, count?)` | Page through keys matching a glob pattern (offset cursor) | TABLE(cursor BIGINT, key TEXT) |
| `pgkv.scan_prefix(prefix, after?, count?)` | Page through keys with a literal prefix (keyset paging, pgkv extension) | TABLE(key TEXT, type TEXT, value JSONB, expires_at TIMESTAMPTZ) |

#### Maintenance

| Function | Description | Returns |
|----------|-------------|---------|
| `pgkv.cleanup_expired()` | Remove expired keys | Number of deleted keys |
| `pgkv.flushall()` | Delete all keys | 'OK' |

### Detailed Function Reference

For complete function signatures, parameter descriptions, return types, and implementation notes, see the sections below organized by category.

<details>
<summary><strong>Helper Functions (1 function)</strong></summary>

#### `redis_to_pg_pattern()`

**Signature:**

```sql
pgkv.redis_to_pg_pattern(p_redis_pattern TEXT) RETURNS TEXT
```

**Parameters:**

- `p_redis_pattern` (TEXT) - Redis glob pattern to convert

**Returns:**

- TEXT - PostgreSQL LIKE pattern, or NULL if character classes detected

**Description:**
Converts Redis glob patterns to PostgreSQL patterns. Handles `*` (any chars), `?` (single char), and escape sequences. Returns NULL when character classes `[...]` are detected, signaling the caller to use regex instead.

**Notes:**

- IMMUTABLE function for query optimization
- Automatically escapes PostgreSQL special characters (`%`, `_`, `\`)
- Used internally by `keys()` for pattern matching optimization

</details>

<details>
<summary><strong>Basic Key-Value Operations (6 functions)</strong></summary>

Complete documentation for `set()`, `get()`, `del()`, `exists()`, `type()`, and `dbsize()` with full parameter descriptions, return types, and implementation notes available in expandable sections.

</details>

## Implementation Patterns

### Lazy Expiration

Read operations on a single key check for expiration and delete the key if it has expired:

```sql
IF v_expires_at IS NOT NULL AND v_expires_at < clock_timestamp() THEN
    DELETE FROM pgkv.store
    WHERE key = p_key AND expires_at IS NOT NULL AND expires_at < clock_timestamp();
    RETURN [appropriate_value];
END IF;
```

The delete repeats the expiry check so that a value another session wrote in the meantime
is never removed (0.2.0 deleted whatever was there).

This approach:

- Minimizes background overhead
- Ensures expired keys are never returned
- Uses `clock_timestamp()` for transaction-accurate timing
- Complements periodic `cleanup_expired()` calls

It also means that a read can write: `get` of an expired key issues a `DELETE`, which fails
in a read-only transaction or on a hot standby. `keys()` and `dbsize()` delete every expired
key in the table before answering. `scan`, `scan_prefix` and the write functions do not
sweep; schedule `cleanup_expired()` (below).

### Type Validation

Type-specific functions validate the key type and raise Redis-compatible errors:

```sql
IF v_type IS NOT NULL AND v_type != 'expected_type' THEN
    RAISE EXCEPTION 'WRONGTYPE Operation against a key holding the wrong kind of value';
END IF;
```

This ensures:

- Redis compatibility
- Data integrity
- Clear error messages
- Prevention of type confusion

### Pattern Matching Optimization

The `keys()` function automatically optimizes pattern matching:

**Simple patterns** (converted to LIKE):

- `*` → `%` (any characters)
- `?` → `_` (single character)
- Example: `user:*` → `key LIKE 'user:%'`

**Character classes** (use regex):

- `[abc]` → regex pattern
- `[0-9]` → regex pattern
- Example: `user:[0-9]` → `key ~ 'user:[0-9]'`

Benefits:

- LIKE queries are very fast (can use indexes)
- Regex only when necessary
- Automatic detection via `redis_to_pg_pattern()` helper

### Negative Index Support

LIST and SORTED SET operations support Redis-compatible negative indices:

```sql
-- Convert negative index to positive
CASE WHEN p_index < 0 THEN length + p_index ELSE p_index END
```

Behavior:

- `-1` = last element
- `-2` = second-to-last element
- Automatically clamped to valid range
- Minimal performance overhead

## Performance Considerations

- **JSONB Storage**: Values stored as JSONB provide type flexibility with minimal overhead (~5-15% larger than TEXT for equivalent data)
  - Counter operations store raw numbers (not strings) for efficiency
  - Can add GIN indexes for nested field queries if needed
- **Lazy Expiration**: Keys are checked for expiration on read operations
- **Manual Cleanup**: Use `pgkv.cleanup_expired()` periodically to remove expired keys
- **KEYS Command**: The `keys()` function scans all keys and should be used cautiously on large datasets
  - Simple patterns (`*`, `?`) use optimized LIKE queries
  - Character classes (`[...]`) automatically fall back to regex matching
- **Indexes**: The extension creates indexes on expiration timestamps and type column for efficient operations, and a `text_pattern_ops` index on the key for prefix matches (`scan`, `scan_prefix`)
- **Listing keys**: prefer `scan_prefix()` (an index range per page) over `scan()` with a deep cursor (which skips `cursor` rows each call) and over `keys()`
- **Write contention**: writers of one key queue on its row lock; writers of different keys do not affect each other
- **SET operations** (SINTER, SUNION, SDIFF) may be slow on very large sets (O(n*m) complexity)
- **SORTED SET sorting** is O(n log n) on each operation (PostgreSQL sorts in-memory)
- All operations are ACID-compliant (unlike Redis)

### Recommended Cleanup Strategy

For production use, schedule periodic cleanup using pg_cron:

```sql
-- Install pg_cron extension
CREATE EXTENSION pg_cron;

-- Schedule cleanup every 5 minutes
SELECT cron.schedule('pgkv-cleanup', '*/5 * * * *', 'SELECT pgkv.cleanup_expired()');
```

## Testing

Run the single-session tests with pgTAP (the two upgrade tests, `tests/11-*.sql` and
`tests/13-*.sql`, are plain psql scripts that print `ok` or `bad`):

```bash
psql -v ON_ERROR_STOP=1 -f tests/01-basic-operations.sql   # and so on, or pg_prove
```

The stress suite starts a throwaway PostgreSQL container and runs pytest against it. It
includes `stress/test_atomicity.py`, which drives two sessions through each read-then-write
race (one holds its transaction open while the other runs) and checks that nothing is lost:

```bash
./stress/run-stress.sh
# or, against a database you already have:
PGKV_DSN=postgresql://user:pass@host:5432/db pytest -q stress
```

## License

MIT License - see LICENSE file for details

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.
