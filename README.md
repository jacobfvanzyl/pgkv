# pgkv

A Redis-like key-value store PostgreSQL Trusted Language Extension (TLE) that provides familiar Redis commands directly in your PostgreSQL database.

## Features

- **Redis-Compatible API**: Familiar commands like SET, GET, DEL, INCR, EXPIRE, and more
- **JSONB Storage**: Values stored as JSONB for type flexibility and rich querying capabilities
- **Redis Pattern Matching**: Full support for Redis glob patterns (`*`, `?`, `[...]`)
- **Type System**: Track data types (string, list, set, hash, zset) for future Redis command compatibility
- **TTL Support**: Automatic expiration of keys with time-to-live functionality
- **Trusted Language Extension**: Written in PL/pgSQL, safe to run in restricted environments
- **PostgreSQL Native**: Leverage PostgreSQL's ACID guarantees and consistency
- **Simple Integration**: Works alongside your existing PostgreSQL data

## Installation

### Local Installation

```sql
CREATE EXTENSION pgkv;
```

### Prerequisites

- PostgreSQL 12 or higher
- PL/pgSQL language support (enabled by default)

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

## API Reference

### Basic Operations

| Function | Description | Returns |
|----------|-------------|---------|
| `pgkv.set(key, value, ttl?)` | Set key to value with optional TTL in seconds | 'OK' |
| `pgkv.get(key)` | Get value of key | JSONB or NULL |
| `pgkv.del(keys...)` | Delete one or more keys | Number of deleted keys |
| `pgkv.exists(keys...)` | Check if keys exist | Number of existing keys |
| `pgkv.type(key)` | Get type of key | 'string', 'list', 'set', 'hash', 'zset', or 'none' |

### TTL Operations

| Function | Description | Returns |
|----------|-------------|---------|
| `pgkv.expire(key, seconds)` | Set TTL on key | 1 if set, 0 if key doesn't exist |
| `pgkv.ttl(key)` | Get remaining TTL | Seconds, -1 if no expiry, -2 if no key |

### String/Counter Operations

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

### HASH Operations

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

### LIST Operations

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

### SET Operations

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

### SORTED SET Operations

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

### Multi-Key Operations

| Function | Description | Returns |
|----------|-------------|---------|
| `pgkv.mget(keys...)` | Get multiple keys | TABLE(key TEXT, value JSONB) |
| `pgkv.mset(k1, v1, k2, v2, ...)` | Set multiple keys | 'OK' |

### Key Inspection

| Function | Description | Returns |
|----------|-------------|---------|
| `pgkv.keys(pattern?)` | Find keys matching Redis glob pattern (`*`, `?`, `[...]`) | SETOF TEXT |
| `pgkv.dbsize()` | Count total keys | BIGINT |

### Maintenance

| Function | Description | Returns |
|----------|-------------|---------|
| `pgkv.cleanup_expired()` | Remove expired keys | Number of deleted keys |
| `pgkv.flushall()` | Delete all keys | 'OK' |

## Performance Considerations

- **JSONB Storage**: Values stored as JSONB provide type flexibility with minimal overhead (~5-15% larger than TEXT for equivalent data)
- **Lazy Expiration**: Keys are checked for expiration on read operations
- **Manual Cleanup**: Use `pgkv.cleanup_expired()` periodically to remove expired keys
- **KEYS Command**: The `keys()` function scans all keys and should be used cautiously on large datasets
  - Simple patterns (`*`, `?`) use optimized LIKE queries
  - Character classes (`[...]`) automatically fall back to regex matching
- **Indexes**: The extension creates indexes on expiration timestamps and type column for efficient operations

### Recommended Cleanup Strategy

For production use, schedule periodic cleanup using pg_cron:

```sql
-- Install pg_cron extension
CREATE EXTENSION pg_cron;

-- Schedule cleanup every 5 minutes
SELECT cron.schedule('pgkv-cleanup', '*/5 * * * *', 'SELECT pgkv.cleanup_expired()');
```

## Testing

Run tests using pgTAP:

```bash
# Install pgTAP if not already installed
# Then run tests
pg_prove supabase/tests/*.sql
```

## License

MIT License - see LICENSE file for details

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.
