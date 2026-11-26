# pgkv

A key-value store PostgreSQL Trusted Language Extension (TLE) that provides familiar Redis commands directly in your PostgreSQL database.

## Features

- **Redis-Compatible API**: 135 Redis commands for strings, hashes, lists, sets and sorted sets, checked reply for reply against a real Redis server
- **Redis command interface**: `pgkv.call('SET', 'k', 'v', 'EX', '10')` takes a command as Redis spells it and answers as Redis would
- **Client libraries**: thin [Rust](clients/rust) and [TypeScript](clients/typescript) clients that run existing redis-rs and ioredis code, inside your own database transaction if you want
- **Collections that scale**: one row per element, so a write to a hash of a million fields costs the same as a write to a hash of ten
- **JSONB Storage**: String values stored as JSONB for type flexibility and rich querying capabilities
- **Pattern Matching**: Full support for Redis glob patterns (`*`, `?`, `[...]`)
- **TTL Support**: Automatic expiration of keys with time-to-live functionality
- **Trusted Language Extension**: Written in PL/pgSQL, safe to run in restricted environments
- **PostgreSQL Native**: Leverage PostgreSQL's ACID guarantees and consistency
- **Simple Integration**: Works alongside your existing PostgreSQL data

## Installation

### Local Installation

```sql
CREATE EXTENSION pgkv;
SELECT pgkv.grant_access('my_app_role');   -- nothing is granted by default
```

The default extension version is `0.4.0`. To upgrade an existing install (from any earlier version):

```sql
ALTER EXTENSION pgkv UPDATE TO '0.4.0';
```

0.4.0 changes how collections are stored and tightens several behaviours to match Redis:
read [Upgrading to 0.4.0](#upgrading-to-040) and
[Behaviour changes from 0.3.0](#behaviour-changes-from-030) first.

pgkv is packaged with PostgreSQL/pg_tle-style versioned scripts:

- `pgkv--0.1.0.sql`, `pgkv--0.2.0.sql`, `pgkv--0.3.0.sql`, `pgkv--0.4.0.sql`
- `pgkv--0.1.0--0.2.0.sql`, `pgkv--0.2.0--0.3.0.sql`, `pgkv--0.3.0--0.4.0.sql`

From 0.4.0 the versioned scripts are assembled from the files in `sql/` by `./build.sh`.

See [Upgrading to 0.3.0](#upgrading-to-030) for installs that applied pgkv as plain SQL, and
[CHANGELOG.md](CHANGELOG.md) for what changed.

Because objects intentionally live in the `pgkv` schema, the extension is not relocatable.

### Prerequisites

- PostgreSQL 12 or higher
- PL/pgSQL language support (enabled by default)

## Security Model

pgkv is closed by default. The role that creates the extension owns it and can use it;
no other role can until the owner grants access:

```sql
SELECT pgkv.grant_access('my_app_role');    -- USAGE on the schema, EXECUTE on the data commands
SELECT pgkv.revoke_access('my_app_role');   -- take it back
```

- **Function-based access**: every operation goes through a function. No role is given
  rights on the tables, and the functions are `SECURITY DEFINER` with a pinned
  `search_path`, so they reach the tables on the caller's behalf.
- **One shared keyspace**: pgkv has no per-user keys. A role with access can read, overwrite
  and delete every key, so grant it to the roles your backend connects as, not to roles
  that stand for end users.
- **Maintenance stays with the owner**: `grant_access` does not cover `cleanup_expired`,
  `set_unlogged`, `grant_access` or `revoke_access`. Internal helpers (names starting with
  an underscore) are never granted.
- **Run `grant_access` again after every upgrade**, so the role also gets the functions the
  new version adds. It is safe to repeat.
- `pg_monitor` can read the tables.

```sql
SET ROLE my_app_role;
SELECT pgkv.set('mykey', 'myvalue');  -- works
SELECT * FROM pgkv.store;             -- permission denied
SET ROLE some_other_role;
SELECT pgkv.get('mykey');             -- permission denied for schema pgkv
```

**Supabase.** Grant the server-side role only:

```sql
SELECT pgkv.grant_access('service_role');
```

`service_role` is the role the API (PostgREST) switches to for the service key, so this is
what `supabase.rpc()` needs once `pgkv` is added to the exposed schemas, and it keeps
`anon` and `authenticated` out. A direct database connection as `postgres` owns the
extension and needs no grant.

## JSONB Storage

pgkv stores string values as JSONB, providing flexibility and powerful querying capabilities:

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

Every key has one row in `pgkv.store`. A string or JSON document keeps its value there. A
collection keeps one row per element in its own table, found through the key's `id`, and
the key's row carries the element count:

| Type | Where the data is | Shape |
|------|-------------------|-------|
| string | `pgkv.store.value` | JSONB string or number: `"Hello"` or `42` |
| json | `pgkv.store.value` | any JSONB document |
| hash | `pgkv.hash_fields` | `(id, field, value)`, value a JSONB string or number |
| list | `pgkv.list_items` | `(id, pos, value)`, positions consecutive from the key's `head` |
| set | `pgkv.set_members` | `(id, member)` |
| zset | `pgkv.zset_members` | `(id, member, score)`, score a double, indexed by `(id, score, member)` |

Members, fields and keys are compared and ordered by byte (`COLLATE "C"`), as in Redis.
The functions that return a value return it as before: `hget` gives JSONB, `lrange` a set of
JSONB strings. `scan_prefix` still returns a whole collection as one JSONB document,
assembled from its rows.

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

**Note**: Patterns are matched as Redis matches them, including character classes, ranges, negation (`[^a]`) and backslash escapes. A pattern that starts with fixed characters (`user:*`) reads only that part of the key index.

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
- **Reads never destroy a write.** In 0.3.0 a read that found an expired key removed it, but
  only if it was still expired at that moment. From 0.4.0 reads do not write at all (see
  [Lazy Expiration](#lazy-expiration)).

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
meaning). It reads a range of the key index (from 0.4.0 the primary key, which is in byte
order), returns each key's type, value and expiry so a listing needs no second query per key, skips expired
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
  Upgrading on to 0.4.0 closes this: see [Security Model](#security-model).

## Version 0.4.0: Redis Conformance, Scalable Collections, Clients

0.4.0 sets out to make pgkv a drop-in replacement for Redis and Valkey for the data types
it covers. Four things changed:

- **Behaviour is checked against a real Redis server.** Every supported command is replayed
  against Redis 8 and against pgkv, reply by reply and key by key (see [Testing](#testing)).
  That found, and this release fixes, ten classes of difference in 0.3.0; what remains is
  listed under [Differences from Redis](#differences-from-redis).
- **Collections are stored one row per element** instead of one JSONB value per key, so a
  command costs what it touches rather than the size of the collection.
- **`pgkv.call`** takes a command the way Redis spells it and answers the way Redis does.
  The typed functions (`pgkv.hset(...)`) are still there for SQL.
- **Client libraries** for Rust and TypeScript, built on `pgkv.call`, that run code written
  for redis-rs and ioredis.

It also adds 45 commands: `PEXPIRE`, `EXPIREAT`, `PEXPIREAT`, `PERSIST`, `PTTL`, `EXPIRETIME`,
`PEXPIRETIME`, the `NX`/`XX`/`GT`/`LT` options of `EXPIRE`, `RENAME`, `RENAMENX`, `COPY`,
`UNLINK`, `TOUCH`, `RANDOMKEY`, `GETSET`, `SUBSTR`, every `GETEX` option, `SET ... IFEQ`,
`HSETNX`, `HMSET`, `HINCRBYFLOAT`, `HSTRLEN`, `HRANDFIELD`, `LMPOP`, the `RANK`/`COUNT`/`MAXLEN`
options of `LPOS`, `SMOVE`, `SPOP`, `SRANDMEMBER`, `SMISMEMBER`, `SINTERSTORE`, `SUNIONSTORE`,
`SDIFFSTORE`, `SINTERCARD`, the `NX`/`XX`/`GT`/`LT`/`CH`/`INCR` options of `ZADD`, `ZRANGE` with
`BYSCORE`/`BYLEX`/`REV`/`LIMIT`, `ZREVRANGEBYSCORE`, `ZRANGEBYLEX`, `ZREVRANGEBYLEX`,
`ZLEXCOUNT`, `ZREMRANGEBYLEX`, `ZUNION`, `ZINTER`, `ZDIFF`, `ZINTERCARD`, `ZRANDMEMBER`, `ZMPOP`.
`SELECT pgkv.commands()` lists everything `pgkv.call` understands.

### The Redis command interface

```sql
SELECT pgkv.call('SET', 'session:42', 'alice', 'EX', '3600');   -- {"status": "OK"}
SELECT pgkv.call('GET', 'session:42');                          -- "alice"
SELECT pgkv.call('RPUSH', 'jobs', 'a', 'b');                    -- 2
SELECT pgkv.call('LRANGE', 'jobs', '0', '-1');                  -- ["a", "b"]
SELECT pgkv.call('ZADD', 'board', 'GT', 'CH', '120', 'ann');    -- 1
SELECT pgkv.call('ZRANGE', 'board', '0', '-1', 'WITHSCORES');   -- ["ann", "120"]
```

Arguments are the words of the command, as text. The reply is what Redis sends over RESP2,
as JSONB:

| Redis reply | JSONB |
|-------------|-------|
| integer | number |
| bulk string | string |
| nil | `null` (the function never returns SQL NULL) |
| array | array |
| simple string (`+OK`) | `{"status": "OK"}`, so that `+OK` and the string `"OK"` stay distinct |
| error | see below |

Scores and other doubles are strings, formatted as Redis formats them (`"1.5"`, `"inf"`).
No command's reply is itself a JSON object, so an object is always one of the markers.

There are three entry points, which differ in what happens to an error reply:

| Function | A Redis error reply (`WRONGTYPE ...`, `ERR ...`) | Use it for |
|----------|--------------------------------------------------|------------|
| `pgkv.call(VARIADIC text[])` | is raised, and so aborts the transaction it is in, like any PostgreSQL error | SQL, scripts, psql |
| `pgkv.try_call(VARIADIC text[])` | is returned as `{"error": "WRONGTYPE ..."}`; only that command is undone | a command inside a transaction that should survive it |
| `pgkv.pipeline(commands jsonb, atomic boolean)` | is returned in that command's place; the other commands still apply, as in Redis | many commands in one round trip, and `MULTI`/`EXEC` |

```sql
SELECT pgkv.pipeline('[["INCR","hits"],["LPUSH","hits","x"],["EXPIRE","hits","60"]]');
-- [1, {"error": "WRONGTYPE Operation against a key holding the wrong kind of value"}, 1]
```

A pipeline is one statement: other sessions see all of its effects or none. With
`atomic => true` it behaves as `MULTI`/`EXEC` does: if a command is unknown or has a number
of arguments Redis would refuse when queueing it, nothing is applied and the result is
`{"error": "EXECABORT Transaction discarded because of previous errors.", "previous": [{"index": 1, "error": "..."}]}`.

Raised errors carry an SQLSTATE in class `KV`, and their message starts with the Redis
error code:

| SQLSTATE | Message starts with | Meaning |
|----------|---------------------|---------|
| `KV000` | `ERR` | bad argument, syntax, value out of range |
| `KV001` | `WRONGTYPE` | the key holds another type |
| `KV002` | `ERR` | unknown command, or a number of arguments Redis refuses when queueing (`pgkv.call` only) |

Anything else (a deadlock, a serialization failure, a lost connection) is a database error,
not a Redis reply, and is raised by all three.

### Client libraries

Both clients are thin: they turn a call into the words of a command, run one of the
functions above, and shape the reply. Neither opens connections. You hand them a
connection, a pool or an open transaction, which is the reason to use pgkv instead of
Redis in the first place: key-value writes commit or roll back with your other writes.

**Rust** ([clients/rust](clients/rust)) implements redis-rs's `ConnectionLike`, so
`AsyncCommands`, `redis::cmd` and `redis::pipe` work unchanged over tokio-postgres or sqlx:

```rust
use redis::AsyncCommands;

let mut tx = client.transaction().await?;
tx.execute("INSERT INTO orders (id) VALUES ($1)", &[&42]).await?;
let mut kv = pgkv::Connection::new(&tx);
let _: () = kv.set_ex("order:42:status", "new", 3600).await?;
let _: i64 = kv.rpush("fulfilment", 42).await?;
tx.commit().await?;   // the row, the key and the queue entry, or none of them
```

**TypeScript** ([clients/typescript](clients/typescript)) mirrors ioredis, with ioredis's
own type signatures, over node-postgres, postgres.js or anything with a `query` method:

```ts
import { Pgkv } from "pgkv";

const kv = new Pgkv(pool);                        // or a PoolClient inside your transaction
await kv.set("session:42", "alice", "EX", 3600);  // "OK"
await kv.multi().incr("hits").ttl("session:42").exec();   // [[null, 1], [null, 3600]]
```

Each client's README lists exactly where it still differs from the library it mirrors.

### Differences from Redis

These are deliberate, documented in `conformance/expected.json` where a test can reach
them, and everything else in the suite must match.

- **Values are text**, not bytes: UTF-8 without NUL characters. Lengths and offsets
  (`STRLEN`, `APPEND`, `GETRANGE`, `SETRANGE`) are in bytes, as in Redis, but a range that
  would cut a multi-byte character in half is an error rather than raw bytes, and so is a
  `SETRANGE` past the end of the string, which Redis pads with zero bytes.
- **`INCRBYFLOAT` and `HINCRBYFLOAT` use exact decimal arithmetic**; Redis uses long double.
  Results agree to about 17 significant digits and can differ beyond that.
- **Replies whose order Redis leaves undefined** (`KEYS`, `SMEMBERS`, `HGETALL`, `HKEYS`,
  `HVALS`, `SINTER`, `SUNION`, `SDIFF`, the scan commands) come back in byte order.
- **Lexicographic ranges** (`ZRANGEBYLEX` and friends) are only defined by Redis for sorted
  sets whose scores are all equal. On other sets pgkv answers in member order.
- **Scan cursors are offsets**, `COUNT` is an exact page size, and keys added or removed
  during a sweep can shift later pages. `pgkv.scan_prefix` pages by key instead.
- **Expiry is lazy** (see [Lazy Expiration](#lazy-expiration)). An expiry beyond the year
  9999 is refused.
- **Transactions are PostgreSQL's.** Every call is atomic and durable on its own, and several
  calls in one transaction commit together. `WATCH` has no equivalent; use `pgkv.cas`, or
  the transaction's own isolation.
- **Not implemented**: pub/sub, blocking pops (`BLPOP` and friends), Lua scripts and
  functions, streams, bitmaps, HyperLogLog, geo, hash field expiry, `OBJECT`, `SORT`,
  `DUMP`/`RESTORE`, databases other than 0, and the server and connection commands.

### Durability and speed

By default every write is flushed to disk before it is acknowledged, which is more than
Redis does by default and costs accordingly. Reads are unaffected. There are three modes:

| Mode | How | A crash loses | Single-key writes per second, 50 connections |
|------|-----|---------------|-----------------------------------------------|
| Durable (default) | nothing to do | nothing | about 4,000 (Redis with `appendfsync always`: about 4,700) |
| Asynchronous commit | `ALTER ROLE app SET synchronous_commit = off;` or `SET LOCAL synchronous_commit = off` in a transaction | the last fraction of a second of acknowledged writes; the data stays consistent | about 44,000 (Redis default settings: about 78,000) |
| Unlogged | `SELECT pgkv.set_unlogged();` | everything in pgkv: the tables are emptied after a crash, and are not replicated | about 45,000 |

On this hardware unlogged tables are barely faster than asynchronous commit, because by
then the processor is the limit and not the log. They do write far less to disk.

Asynchronous commit is the right setting for a cache or for sessions, and can be set for
the role that uses pgkv without affecting the rest of the database. It has to be set on the
role, the database, the session or the transaction: setting it on a function has no
effect, because the commit happens after the function returns. `pgkv.set_unlogged()`
switches all of pgkv's tables together (`pgkv.set_unlogged(false)` switches back); it
rewrites them under an exclusive lock, and whoever calls it must own them.

Measured on one 8-core machine over loopback with default PostgreSQL settings, 50
connections, asynchronous commit, against Redis 8 with default settings:

| Operations per second | pgkv 0.4.0 | pgkv 0.3.0 | Redis 8 |
|-----------------------|-----------:|-----------:|--------:|
| `GET` | 72,000 | 69,000 | 76,000 |
| `SET` | 44,000 | 43,000 | 78,000 |
| `INCR` | 26,000 | 26,000 | 76,000 |
| `HSET`, 10,000 small hashes | 30,000 | 27,000 | 77,000 |
| `HGET`, 10,000 small hashes | 56,000 | 68,000 | - |
| `ZADD`, 10,000 small sorted sets | 26,000 | 28,000 | 76,000 |
| `LPUSH` then `LPOP`, 10,000 short lists (pairs) | 9,600 | 12,700 | - |
| `GET` in batches of 16 (`pgkv.pipeline`; Redis pipelined) | 157,000 | - | 837,000 |
| `HSET` of new fields into one hash, 8 connections, 8 seconds | 6,800 (reaching 42,000 fields) | 600 (reaching 3,600) | - |
| `ZADD` of new members into one sorted set, 8 connections, 8 seconds | 6,700 (reaching 41,000) | 520 (reaching 3,100) | - |
| `RPUSH` then `LPOP` on one list of 100,000 elements, 8 connections (pairs) | 3,200 | under 1 | - |

These are the typed functions; the same command through `pgkv.call` runs 10 to 20% slower
(`GET` 64,000, `SET` 35,000). Reads are within reach of Redis because both are bounded by
the network round trip. Writes are about half as fast, and Redis's cheap pipelining keeps a
fivefold lead on batches. Small collections cost within about 25% of 0.3.0 either way;
large ones no longer slow down as they grow.

### Behaviour changes from 0.3.0

0.4.0 changes results where 0.3.0 disagreed with Redis, and changes some signatures. Review
these before upgrading.

**Access**

- Nothing is granted to `PUBLIC` any more. Earlier versions let every role that could reach
  the schema use pgkv; from 0.4.0 a role needs `SELECT pgkv.grant_access('<role>')`. The
  upgrade revokes the access roles had, `PUBLIC` and named roles alike, and prints the
  named ones: see [Upgrading to 0.4.0](#upgrading-to-040).
- `cleanup_expired` and `set_unlogged` are for the owner only.

**Errors**

- Errors a Redis server would send as an error reply are raised with SQLSTATE `KV000` or
  `KV001` (0.3.0: `P0001`), and their text is Redis's, starting with `ERR` or `WRONGTYPE`
  (0.3.0 left out `ERR` in places). Code that catches `P0001` or matches on message text
  needs updating.

**Types and signatures**

- Sorted set scores are `double precision`, as in Redis, not `numeric`: `zscore`, `zincrby`,
  `zrange`, `zrevrange`, `zrangebyscore`, `zcount`, `zremrangebyscore`, `zmscore`, `zpopmin`,
  `zpopmax`, `zscan`, and the weights of `zunionstore` and `zinterstore`. Infinite scores
  work on every PostgreSQL version; `NaN` and scores outside the double range are refused.
- Index, offset and count parameters are `bigint` instead of `integer` (`getrange`,
  `setrange`, `lrange`, `lindex`, `lset`, `ltrim`, `lrem`, `lpop`, `rpop`, `zrange`,
  `zrevrange`, `zremrangebyrank`, `zrangestore`, `zpopmin`, `zpopmax`), and so are the expiry
  parameters of `set_opts`, `cas` and `expire`. Calls with integer arguments are unaffected.
- `expire` takes an optional mode; `getex(key, seconds)` became
  `getex(key, p_ex, p_px, p_exat, p_pxat, p_persist)`; `lpos` takes an optional rank; `scan`
  an optional type; `cleanup_expired` an optional limit.

**Results**

- An emptied collection no longer exists. 0.3.0 left an empty key behind after `zpopmin`,
  `zpopmax`, `zremrangebyrank`, `zremrangebyscore`, a `z*store` with an empty result, and an
  `lmove` that drained its source.
- `linsert` returns -1 when the pivot is missing and 0 when the key is (0.3.0 had these the
  wrong way round).
- `set_opts(..., p_get => true)` on a key that is not a string raises `WRONGTYPE` (0.3.0
  returned the collection and overwrote it).
- `strlen`, `append`, `getrange` and `setrange` count bytes, not characters. `getrange`
  clamps out-of-range indexes as Redis does.
- Integers are parsed strictly: `' 12'`, `'12 '`, `'+5'` and `'007'` are not integers for
  `incr`, `hincrby` and friends. Overflow is an error rather than a wrapped or widened value.
- `sinter`, `sunion`, `sdiff` and the `z*store` functions raise `WRONGTYPE` for a key of the
  wrong type (0.3.0 treated it as empty); the `z*store` functions accept plain sets, with
  score 1.
- Members with equal scores are ordered by byte, not by the database's collation.
- `srem` counts a member given twice once. A negative count for `lpop`, `rpop` and friends
  is an error. `expire` with a time in the past deletes the key. `set_opts` and `getex`
  refuse an expiry of zero or less. `ttl` rounds to the nearest second.
- `hgetall`, `hkeys`, `hvals`, `smembers` and the scan functions return byte order.
- `zadd` with an odd number of arguments, and `hset` likewise, report Redis's error text.

**Storage and operations**

- **`pg_dump` now includes pgkv's data.** Earlier versions did not register their table
  with the extension's dump configuration, so a `pg_dump` of a database with pgkv installed
  restored the extension with an empty store. If you rely on dumps, upgrade before the next
  one.
- Reads no longer delete expired keys, and `keys()` and `dbsize()` no longer sweep the
  table. Schedule `cleanup_expired()` if you relied on that.
- `updated_at` on a collection's row changes when its element count changes, not on every
  write to it.
- The indexes `idx_pgkv_type` and `idx_pgkv_key_pattern` are gone; the primary key, now in
  byte order, does the work of the second.
- Reading `pgkv.store.value` directly no longer shows a collection's contents; they are in
  the element tables. `scan_prefix` still returns them.

### Upgrading to 0.4.0

```sql
BEGIN;
ALTER EXTENSION pgkv UPDATE TO '0.4.0';   -- from any earlier version
SELECT pgkv.grant_access('my_app_role');  -- once for each role that uses pgkv
COMMIT;
```

**The upgrade revokes all access to pgkv**, from `PUBLIC` and from roles granted by name,
and warns with the names of the roles that held grants. Grant it again with
`pgkv.grant_access` in the same transaction, as above, so no session sees pgkv closed. The
upgrade does not carry grants over itself because PostgreSQL treats privileges set by an
extension script as part of the extension, and `pg_dump` does not dump those: they would
be missing after a restore. Grants made with `grant_access` are dumped.

The upgrade converts every collection to rows and rebuilds the primary key in byte order.
It holds an exclusive lock on `pgkv.store` while it runs, and the time it takes grows with
the amount of data: try it on a copy first if the store is large. Expired keys and the
empty collections 0.3.0 could leave behind are dropped; everything else is kept, including
expiries. `tests/14-upgrade-040.sql` checks that an upgraded install is identical to a
fresh one, functions, privileges, tables and indexes alike.

If pgkv was applied as plain SQL rather than with `CREATE EXTENSION`, run
`pgkv--0.3.0--0.4.0.sql` once, as plain SQL, on a copy that is at 0.3.0 (see
[Upgrading to 0.3.0](#upgrading-to-030) for getting there and for copies in a renamed
schema). Unlike the earlier upgrade scripts it is not idempotent.

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
| `pgkv.zscore(key, member)` | Get member score | DOUBLE PRECISION or NULL |
| `pgkv.zcard(key)` | Get sorted set size | INTEGER |
| `pgkv.zrank(key, member)` | Get rank (0-based, ascending) | INTEGER or NULL |
| `pgkv.zrevrank(key, member)` | Get reverse rank (0-based, descending) | INTEGER or NULL |
| `pgkv.zrangebyscore(key, min, max, withscores?)` | Get members by score range | TABLE(member TEXT, score NUMERIC) |
| `pgkv.zincrby(key, increment, member)` | Increment member score | New score (DOUBLE PRECISION) |
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
| `pgkv.cleanup_expired([limit])` | Remove expired keys, at most `limit` | Number of deleted keys |
| `pgkv.flushall()` | Delete all keys | 'OK' |
| `pgkv.set_unlogged([unlogged])` | Make all pgkv tables unlogged, or logged again | 'unlogged' or 'logged' |
| `pgkv.grant_access(role)` | Let a role use pgkv (schema and data commands) | void |
| `pgkv.revoke_access(role)` | Remove a role's access to pgkv | void |

`flushall` is a data command; the others are for the extension owner.

#### Added in 0.4.0

Scores are `double precision`; `-2`/`-1` from the expiry functions mean no key / no expiry.

| Function | Description | Returns |
|----------|-------------|---------|
| `pgkv.call(VARIADIC words)` | Run a Redis command; errors are raised | JSONB reply |
| `pgkv.try_call(VARIADIC words)` | Run a Redis command; an error reply is returned | JSONB reply or `{"error"}` |
| `pgkv.pipeline(commands, [atomic])` | Run several commands in one statement | JSONB array of replies |
| `pgkv.commands()` | Commands `pgkv.call` understands | Set of names |
| `pgkv.expire(key, seconds, [mode])` | Set a timeout; mode is `NX`, `XX`, `GT` or `LT` | 1 or 0 |
| `pgkv.pexpire(key, ms, [mode])` | Timeout in milliseconds | 1 or 0 |
| `pgkv.expireat(key, unix_seconds, [mode])` | Expire at a Unix time | 1 or 0 |
| `pgkv.pexpireat(key, unix_ms, [mode])` | Expire at a Unix time in milliseconds | 1 or 0 |
| `pgkv.persist(key)` | Remove the expiry | 1 or 0 |
| `pgkv.pttl(key)` | Time to live in milliseconds | bigint |
| `pgkv.expiretime(key)`, `pgkv.pexpiretime(key)` | When the key expires, as a Unix time | bigint |
| `pgkv.rename(key, new_key)` | Rename, replacing `new_key` | 'OK' |
| `pgkv.renamenx(key, new_key)` | Rename unless `new_key` exists | 1 or 0 |
| `pgkv.copy(source, destination, [replace])` | Copy a key with its expiry | 1 or 0 |
| `pgkv.touch(key, ...)` | Count the keys that exist | integer |
| `pgkv.randomkey()` | A random key | text or NULL |
| `pgkv.scan(cursor, pattern, count, [type])` | `scan`, optionally of one type | (cursor, key) rows |
| `pgkv.getset(key, value)` | Set and return the old value | JSONB or NULL |
| `pgkv.getex(key, [p_ex], [p_px], [p_exat], [p_pxat], [p_persist])` | Get and change the expiry | JSONB or NULL |
| `pgkv.hsetnx(key, field, value)` | Set a field if it is missing | 1 or 0 |
| `pgkv.hincrbyfloat(key, field, increment)` | Add a decimal to a field | numeric |
| `pgkv.hstrlen(key, field)` | Length of a field's value in bytes | integer |
| `pgkv.hrandfield(key, [count])` | Random fields | (field, value) rows |
| `pgkv.lpos(key, value, [rank])` | Index of the rank-th match | integer or NULL |
| `pgkv.smove(source, destination, member)` | Move a member | 1 or 0 |
| `pgkv.spop(key, [count])` | Remove and return random members | Set of text |
| `pgkv.srandmember(key, [count])` | Random members | Set of text |
| `pgkv.smismember(key, member, ...)` | Membership of each member | Set of 1 or 0 |
| `pgkv.sinterstore(dest, key, ...)`, `sunionstore`, `sdiffstore` | Store the result of set algebra | Size of the result |
| `pgkv.sintercard(keys[], [limit])` | Size of an intersection | integer |
| `pgkv.zadd_opts(key, pairs[], [p_nx], [p_xx], [p_gt], [p_lt], [p_ch], [p_incr])` | `ZADD` with options | JSONB count or new score |
| `pgkv.zrange_by(key, min, max, [by], [rev], [offset], [count])` | Range by `RANK`, `SCORE` or `LEX` | (member, score) rows |
| `pgkv.zlexcount(key, min, max)` | Members in a lexicographic range | integer |
| `pgkv.zremrangebylex(key, min, max)` | Remove a lexicographic range | Number removed |
| `pgkv.zunion(keys[], [weights[]], [aggregate])`, `zinter` | Combine sorted sets without storing | (member, score) rows |
| `pgkv.zdiff(keys[])` | First set minus the others | (member, score) rows |
| `pgkv.zintercard(keys[], [limit])` | Size of an intersection | integer |
| `pgkv.zrandmember(key, [count])` | Random members | (member, score) rows |

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

Every function treats a key whose `expires_at` has passed as absent. Reads only filter:
they never write, so they run in a read-only transaction and on a hot standby, and are
declared `STABLE`. A write to an expired key removes it (with its elements, for a
collection) and starts from nothing. Nothing else removes expired keys, so if keys expire
without being written again, schedule `cleanup_expired()` (below) to reclaim the space.

Before 0.4.0 a read of an expired key deleted it, and `keys()` and `dbsize()` swept the
whole table before answering.

### Type Validation

Type-specific functions validate the key type and raise Redis-compatible errors:

```sql
IF v_type IS NOT NULL AND v_type != 'expected_type' THEN
    RAISE EXCEPTION 'WRONGTYPE Operation against a key holding the wrong kind of value'
        USING ERRCODE = 'KV001';
END IF;
```

This ensures:

- Redis compatibility
- Data integrity
- Clear error messages
- Prevention of type confusion

### Pattern Matching

`keys()`, `scan()` and the `MATCH` option of the scan commands turn a Redis glob (`*`, `?`,
`[abc]`, `[a-z]`, `[^a]`, `\x`) into an anchored regular expression. For `keys()` and
`scan()` it goes into the query as a literal, so a pattern that starts with fixed
characters (`user:*`) becomes a range of the primary key index rather than a scan of the
table. `scan_prefix()` takes its prefix literally and pages by key.

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

See [Durability and speed](#durability-and-speed) for measured throughput and the settings
that trade durability for it.

- **Cost follows what a command touches.** A write to one hash field, set member, sorted set
  member or list end is a few index operations whatever the size of the collection. The
  exceptions are the ones Redis has too (`LREM`, `LINSERT`, `LRANGE 0 -1`, set algebra), plus:
  - `ZRANK`, and `ZRANGE` / `ZREVRANGE` by rank, walk the index from the nearest end, so they
    cost in proportion to the rank they reach (Redis: logarithmic).
  - `SRANDMEMBER`, `SPOP`, `HRANDFIELD`, `ZRANDMEMBER` and `RANDOMKEY` read every candidate.
  - `LREM` and `LINSERT` rewrite the shorter side of the list around the change.
- **One lock per key.** Writers of one key queue on its row in `pgkv.store`; writers of
  different keys do not affect each other. A collection with one very hot writer path
  (thousands of writes a second to a single key) is limited by that queue.
- **Expired keys** stay until written to or swept: schedule `cleanup_expired()`.
- **`keys()` and `dbsize()`** read every key (`keys('prefix*')` reads only the prefix range).
  Prefer `scan_prefix()` (an index range per page) over `scan()` with a deep cursor, which
  skips `cursor` rows on each call.
- **Long transactions.** PostgreSQL cannot clean up row versions a still-open transaction
  might need. A transaction that pushes and pops the same queue tens of thousands of times
  slows down as it goes; commit in between.
- **`pgkv.call` against the typed functions**: the dispatcher costs 10 to 20% of a
  single-key command (`GET` is answered before the dispatcher and is at the low end).
- All operations are ACID-compliant (unlike Redis)

### Recommended Cleanup Strategy

Expired keys are invisible to every command and are removed when written to, so
`cleanup_expired` only reclaims the space of keys that expire without being touched again.
pgkv schedules nothing itself. With pg_cron:

```sql
CREATE EXTENSION IF NOT EXISTS pg_cron;

-- Every minute, at most 10,000 keys a run
SELECT cron.schedule('pgkv-cleanup', '* * * * *', 'SELECT pgkv.cleanup_expired(10000)');
```

- A limit keeps each run short: it removes the keys that expired longest ago first and
  skips keys another transaction is working on rather than waiting for them. Raise the
  limit, or run more often, if more than that many keys expire unread each minute.
- Schedule the job as the role that owns the extension (`postgres` on Supabase):
  `cleanup_expired` is not part of `grant_access`.
- To stop it: `SELECT cron.unschedule('pgkv-cleanup');`

## Testing

There are four suites. All of them run in CI on PostgreSQL 12 to 18.

**pgTAP** (`tests/`), single-session behaviour of the SQL API. The upgrade tests
(`tests/1[134]-upgrade-*.sql`) are plain psql scripts that print `ok` or `bad`; the 0.4.0
one checks that an upgraded install is identical to a fresh one, down to privileges.

```bash
psql -v ON_ERROR_STOP=1 -f tests/01-basic-operations.sql   # and so on, or pg_prove
```

**Stress and atomicity** (`stress/`). Starts a throwaway PostgreSQL container and runs
pytest against it. `stress/test_atomicity.py` drives two sessions through each
read-then-write race (one holds its transaction open while the other runs) and checks that
nothing is lost and that no element rows are left without a key:

```bash
./stress/run-stress.sh
# or, against a database you already have:
PGKV_DSN=postgresql://user:pass@host:5432/db pytest -q stress
```

**Conformance** (`conformance/`). Replays Redis commands against a real Redis server and
against `pgkv.call`, command by command, and stops at the first difference in a reply or in
the data left behind (type, expiry and contents of every key the sequence touches). It also
checks pgkv's storage invariants after each sequence. The commands are 372 sequences taken
from Valkey's own test suite (see [conformance/NOTICE.md](conformance/NOTICE.md)) and
seeded random sequences biased toward edge cases; `--pipeline` replays each sequence as one
`MULTI`/`EXEC` against one atomic `pgkv.pipeline`. The differences pgkv keeps on purpose
are listed with their reasons in `conformance/expected.json`; anything else fails.

```bash
./conformance/run.sh                    # everything, in throwaway containers
./conformance/run.sh --fuzz 7 2000      # 2000 random sequences from seed 7
PGKV_ORACLE_IMAGE=valkey/valkey:9 ./conformance/run.sh
```

**Client tests** (`clients/rust`, `clients/typescript`). Each client replays the same
sequences through its own API and through the real Redis client library it mirrors, and
compares what the application would see. See each client's README.

After editing anything in `sql/`, run `./build.sh` to regenerate the versioned scripts.

## License

MIT License - see LICENSE file for details

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.
