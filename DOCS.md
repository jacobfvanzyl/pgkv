# pgkv Function Reference

Complete documentation for all pgkv PostgreSQL extension functions.

## Table of Contents

- [Helper Functions](#helper-functions)
- [Basic Key-Value Operations](#basic-key-value-operations)
- [TTL Operations](#ttl-operations)
- [String Operations](#string-operations)
- [Multi-Key Operations](#multi-key-operations)
- [Hash Operations](#hash-operations)
- [List Operations](#list-operations)
- [Set Operations](#set-operations)
- [Sorted Set Operations](#sorted-set-operations)
- [Maintenance Operations](#maintenance-operations)
- [Common Implementation Patterns](#common-implementation-patterns)

---

## Helper Functions

### `redis_to_pg_pattern()`

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

---

## Basic Key-Value Operations

### `set()`

**Signature:**
```sql
pgkv.set(p_key TEXT, p_value TEXT, p_ttl_seconds INTEGER DEFAULT NULL) RETURNS TEXT
```

**Parameters:**
- `p_key` (TEXT) - The key to set
- `p_value` (TEXT) - The value to store (converted to JSONB)
- `p_ttl_seconds` (INTEGER, optional) - Time-to-live in seconds

**Returns:**
- TEXT - Always returns `'OK'`

**Description:**

Sets a key to a value with optional TTL. The value is automatically converted to JSONB for storage. Sets the key type to `'string'`.

**Notes:**
- Uses UPSERT pattern (`INSERT ... ON CONFLICT UPDATE`)
- Stores value as JSONB: `to_jsonb(p_value)`
- Updates `updated_at` timestamp on modification
- If TTL provided, sets `expires_at` to current time + TTL seconds

---

### `get()`

**Signature:**
```sql
pgkv.get(p_key TEXT) RETURNS JSONB
```

**Parameters:**
- `p_key` (TEXT) - The key to retrieve

**Returns:**
- JSONB - The value, or NULL if not found or expired

**Description:**

Retrieves the value for a key. Returns NULL if the key doesn't exist or has expired.

**Notes:**
- Type checking: Raises `WRONGTYPE` error if key is not type `'string'`
- Lazy expiration: Automatically deletes expired keys on read
- Uses `clock_timestamp()` for expiration checks (not `now()` for transaction accuracy)

---

### `del()`

**Signature:**
```sql
pgkv.del(VARIADIC p_keys TEXT[]) RETURNS INTEGER
```

**Parameters:**
- `p_keys` (TEXT[]) - One or more keys to delete

**Returns:**
- INTEGER - Number of keys successfully deleted

**Description:**

Deletes one or more keys of any type.

**Notes:**
- Works on all data types (not type-specific)
- Accepts variadic arguments: `del('key1')` or `del('key1', 'key2', 'key3')`

---

### `exists()`

**Signature:**
```sql
pgkv.exists(VARIADIC p_keys TEXT[]) RETURNS INTEGER
```

**Parameters:**
- `p_keys` (TEXT[]) - One or more keys to check

**Returns:**
- INTEGER - Count of existing keys

**Description:**

Checks if keys exist and returns the count of keys that exist.

**Notes:**
- Cleans up expired keys during check (lazy expiration)
- Works on all data types
- Returns total count, not boolean per key

---

### `type()`

**Signature:**
```sql
pgkv.type(p_key TEXT) RETURNS TEXT
```

**Parameters:**
- `p_key` (TEXT) - The key to check

**Returns:**
- TEXT - One of: `'string'`, `'list'`, `'set'`, `'hash'`, `'zset'`, or `'none'`

**Description:**

Returns the data type of a key, or `'none'` if the key doesn't exist or has expired.

**Notes:**
- Checks expiration first before returning type
- Redis-compatible return values

---

### `dbsize()`

**Signature:**
```sql
pgkv.dbsize() RETURNS BIGINT
```

**Parameters:**
- None

**Returns:**
- BIGINT - Total count of all keys in the store

**Description:**

Returns the total number of keys in the database after cleaning up expired keys.

**Notes:**
- Automatically calls `cleanup_expired()` first
- May be slow on large datasets due to full table scan

---

## TTL Operations

### `expire()`

**Signature:**
```sql
pgkv.expire(p_key TEXT, p_seconds INTEGER) RETURNS INTEGER
```

**Parameters:**
- `p_key` (TEXT) - The key to set expiration on
- `p_seconds` (INTEGER) - Seconds until expiration

**Returns:**
- INTEGER - `1` if expiration was set, `0` if key doesn't exist

**Description:**

Sets a timeout on an existing key. Works on all data types.

**Notes:**
- Updates `updated_at` timestamp
- Can override existing TTL
- Does not create the key if it doesn't exist

---

### `ttl()`

**Signature:**
```sql
pgkv.ttl(p_key TEXT) RETURNS INTEGER
```

**Parameters:**
- `p_key` (TEXT) - The key to check

**Returns:**
- INTEGER - Seconds remaining, `-1` if no expiration, `-2` if key doesn't exist

**Description:**

Returns the time-to-live for a key in seconds.

**Notes:**
- Redis-compatible return values
- Automatically deletes key if already expired (then returns `-2`)
- Rounds down to nearest second

---

## String Operations

### `incr()`

**Signature:**
```sql
pgkv.incr(p_key TEXT) RETURNS BIGINT
```

**Parameters:**
- `p_key` (TEXT) - The key to increment

**Returns:**
- BIGINT - The new value after increment

**Description:**

Increments the integer value of a key by 1. Creates the key at 0 if it doesn't exist.

**Notes:**
- Type checking: Requires type `'string'`
- Stores result as raw JSONB number: `to_jsonb(42::bigint)`
- Raises exception if value cannot be parsed as integer

---

### `decr()`

**Signature:**
```sql
pgkv.decr(p_key TEXT) RETURNS BIGINT
```

**Parameters:**
- `p_key` (TEXT) - The key to decrement

**Returns:**
- BIGINT - The new value after decrement

**Description:**

Decrements the integer value of a key by 1.

**Notes:**
- Wrapper around `incrby(p_key, -1)`
- Same behavior and constraints as `incr()`

---

### `incrby()`

**Signature:**
```sql
pgkv.incrby(p_key TEXT, p_increment BIGINT) RETURNS BIGINT
```

**Parameters:**
- `p_key` (TEXT) - The key to increment
- `p_increment` (BIGINT) - Amount to increment by (can be negative)

**Returns:**
- BIGINT - The new value after increment

**Description:**

Increments the integer value by the given amount.

**Notes:**
- Type checking: Requires type `'string'`
- Handles negative increments (decrement behavior)
- Creates key at 0 if doesn't exist

---

### `decrby()`

**Signature:**
```sql
pgkv.decrby(p_key TEXT, p_decrement BIGINT) RETURNS BIGINT
```

**Parameters:**
- `p_key` (TEXT) - The key to decrement
- `p_decrement` (BIGINT) - Amount to decrement by

**Returns:**
- BIGINT - The new value after decrement

**Description:**

Decrements the integer value by the given amount.

**Notes:**
- Wrapper around `incrby(p_key, -p_decrement)`

---

### `append()`

**Signature:**
```sql
pgkv.append(p_key TEXT, p_value TEXT) RETURNS INTEGER
```

**Parameters:**
- `p_key` (TEXT) - The key to append to
- `p_value` (TEXT) - The value to append

**Returns:**
- INTEGER - The length of the string after append

**Description:**

Appends a value to a string key. Creates the key with the value if it doesn't exist.

**Notes:**
- Type checking: Requires type `'string'`
- Handles both string and numeric JSONB values
- Returns final string length

---

### `strlen()`

**Signature:**
```sql
pgkv.strlen(p_key TEXT) RETURNS INTEGER
```

**Parameters:**
- `p_key` (TEXT) - The key to measure

**Returns:**
- INTEGER - Length of the string, or `0` if key doesn't exist

**Description:**

Returns the length of the value stored in a key.

**Notes:**
- Type checking: Requires type `'string'`
- Handles both string and numeric JSONB values
- Returns `0` for non-existent keys

---

### `getrange()`

**Signature:**
```sql
pgkv.getrange(p_key TEXT, p_start INTEGER, p_end INTEGER) RETURNS TEXT
```

**Parameters:**
- `p_key` (TEXT) - The key to read from
- `p_start` (INTEGER) - Start index (0-based, supports negative)
- `p_end` (INTEGER) - End index (0-based, inclusive, supports negative)

**Returns:**
- TEXT - Substring, or empty string if key doesn't exist

**Description:**

Returns a substring of the string stored at a key.

**Notes:**
- Supports negative indices: `-1` = last character, `-2` = second-to-last, etc.
- Type checking: Requires type `'string'`
- Redis-compatible 0-based indexing
- End index is inclusive (unlike many programming languages)

---

### `setrange()`

**Signature:**
```sql
pgkv.setrange(p_key TEXT, p_offset INTEGER, p_value TEXT) RETURNS INTEGER
```

**Parameters:**
- `p_key` (TEXT) - The key to modify
- `p_offset` (INTEGER) - Starting position (0-based, must be >= 0)
- `p_value` (TEXT) - The value to write

**Returns:**
- INTEGER - The length of the string after modification

**Description:**

Overwrites part of a string at the specified offset.

**Notes:**
- Type checking: Requires type `'string'`
- Pads with NULL bytes (`\x00`) if offset is beyond current length
- Creates key if doesn't exist (initializes with NULL byte padding)

---

### `keys()`

**Signature:**
```sql
pgkv.keys(p_pattern TEXT DEFAULT '*') RETURNS SETOF TEXT
```

**Parameters:**
- `p_pattern` (TEXT, optional) - Redis glob pattern (default: `'*'`)

**Returns:**
- SETOF TEXT - Matching key names (sorted alphabetically)

**Description:**

Finds all keys matching the given pattern. Supports Redis glob patterns: `*` (any chars), `?` (single char), `[...]` (character classes).

**Notes:**
- **WARNING:** Expensive on large datasets - full table scan
- Automatic optimization:
  - Simple patterns (`user:*`, `session:?????`) use fast LIKE queries
  - Character classes (`h[ae]llo`, `user:[0-9]`) automatically use regex (`~` operator)
- Cleans up expired keys first
- Results sorted alphabetically

---

## Multi-Key Operations

### `mget()`

**Signature:**
```sql
pgkv.mget(VARIADIC p_keys TEXT[]) RETURNS TABLE(key TEXT, value JSONB)
```

**Parameters:**
- `p_keys` (TEXT[]) - One or more keys to retrieve

**Returns:**
- TABLE - Rows with `key` and `value` columns (value is NULL for missing/wrong type keys)

**Description:**

Gets values for multiple keys in a single operation. Only works with type `'string'` keys.

**Notes:**
- Type filtering: Only returns values for `'string'` type keys
- Returns NULL value for wrong type (no exception thrown)
- Always returns a row for each requested key
- More efficient than multiple `get()` calls

---

### `mset()`

**Signature:**
```sql
pgkv.mset(VARIADIC p_pairs TEXT[]) RETURNS TEXT
```

**Parameters:**
- `p_pairs` (TEXT[]) - Key-value pairs: `['key1', 'val1', 'key2', 'val2', ...]`

**Returns:**
- TEXT - Always returns `'OK'`

**Description:**

Sets multiple keys to multiple values atomically.

**Notes:**
- Requires even number of arguments (pairs)
- Raises exception if argument count is odd
- All keys set to type `'string'`
- Atomic operation (all or nothing)

---

## Hash Operations

### `hset()`

**Signature:**
```sql
pgkv.hset(p_key TEXT, VARIADIC p_pairs TEXT[]) RETURNS INTEGER
```

**Parameters:**
- `p_key` (TEXT) - The hash key
- `p_pairs` (TEXT[]) - Field-value pairs: `['field1', 'val1', 'field2', 'val2', ...]`

**Returns:**
- INTEGER - Number of fields added (not counting updates)

**Description:**

Sets field(s) in a hash. Creates the hash if it doesn't exist.

**Notes:**
- Type checking: Requires type `'hash'`
- Storage format: `{"field1": "value1", "field2": "value2"}`
- Returns count of NEW fields only (updates don't count)
- Requires even number of variadic arguments

---

### `hget()`

**Signature:**
```sql
pgkv.hget(p_key TEXT, p_field TEXT) RETURNS JSONB
```

**Parameters:**
- `p_key` (TEXT) - The hash key
- `p_field` (TEXT) - The field name

**Returns:**
- JSONB - Field value, or NULL if not found

**Description:**

Gets a field value from a hash.

**Notes:**
- Type checking: Requires type `'hash'`
- Lazy expiration on read
- Returns NULL for missing field or missing key

---

### `hmget()`

**Signature:**
```sql
pgkv.hmget(p_key TEXT, VARIADIC p_fields TEXT[]) RETURNS TABLE(field TEXT, value JSONB)
```

**Parameters:**
- `p_key` (TEXT) - The hash key
- `p_fields` (TEXT[]) - One or more field names

**Returns:**
- TABLE - Rows with `field` and `value` columns

**Description:**

Gets multiple field values from a hash in a single operation.

**Notes:**
- Type checking: Requires type `'hash'`
- Returns NULL value for missing fields
- Always returns a row for each requested field

---

### `hgetall()`

**Signature:**
```sql
pgkv.hgetall(p_key TEXT) RETURNS TABLE(field TEXT, value JSONB)
```

**Parameters:**
- `p_key` (TEXT) - The hash key

**Returns:**
- TABLE - All fields and values

**Description:**

Gets all fields and values from a hash.

**Notes:**
- Type checking: Requires type `'hash'`
- Returns empty set if key doesn't exist
- Field order is not guaranteed

---

### `hdel()`

**Signature:**
```sql
pgkv.hdel(p_key TEXT, VARIADIC p_fields TEXT[]) RETURNS INTEGER
```

**Parameters:**
- `p_key` (TEXT) - The hash key
- `p_fields` (TEXT[]) - One or more fields to delete

**Returns:**
- INTEGER - Number of fields successfully deleted

**Description:**

Deletes field(s) from a hash.

**Notes:**
- Type checking: Requires type `'hash'`
- Automatically deletes the key if hash becomes empty
- Returns `0` if key doesn't exist

---

### `hexists()`

**Signature:**
```sql
pgkv.hexists(p_key TEXT, p_field TEXT) RETURNS INTEGER
```

**Parameters:**
- `p_key` (TEXT) - The hash key
- `p_field` (TEXT) - The field name

**Returns:**
- INTEGER - `1` if field exists, `0` otherwise

**Description:**

Checks if a field exists in a hash.

**Notes:**
- Type checking: Requires type `'hash'`
- Returns `0` for missing key
- Redis-compatible return values (1/0 not boolean)

---

### `hlen()`

**Signature:**
```sql
pgkv.hlen(p_key TEXT) RETURNS INTEGER
```

**Parameters:**
- `p_key` (TEXT) - The hash key

**Returns:**
- INTEGER - Number of fields in the hash

**Description:**

Gets the number of fields in a hash.

**Notes:**
- Type checking: Requires type `'hash'`
- Returns `0` for missing key

---

### `hkeys()`

**Signature:**
```sql
pgkv.hkeys(p_key TEXT) RETURNS SETOF TEXT
```

**Parameters:**
- `p_key` (TEXT) - The hash key

**Returns:**
- SETOF TEXT - All field names

**Description:**

Gets all field names from a hash.

**Notes:**
- Type checking: Requires type `'hash'`
- Returns empty set if key doesn't exist
- Field order is not guaranteed

---

### `hvals()`

**Signature:**
```sql
pgkv.hvals(p_key TEXT) RETURNS SETOF JSONB
```

**Parameters:**
- `p_key` (TEXT) - The hash key

**Returns:**
- SETOF JSONB - All values

**Description:**

Gets all values from a hash.

**Notes:**
- Type checking: Requires type `'hash'`
- Returns empty set if key doesn't exist
- Value order is not guaranteed

---

### `hincrby()`

**Signature:**
```sql
pgkv.hincrby(p_key TEXT, p_field TEXT, p_increment BIGINT) RETURNS BIGINT
```

**Parameters:**
- `p_key` (TEXT) - The hash key
- `p_field` (TEXT) - The field name
- `p_increment` (BIGINT) - Amount to increment by (can be negative)

**Returns:**
- BIGINT - New value after increment

**Description:**

Increments a numeric field in a hash by the given amount.

**Notes:**
- Type checking: Requires type `'hash'`
- Creates hash and/or field if doesn't exist (starts at 0)
- Raises exception if field value is not an integer
- Supports negative increment (decrement)

---

## List Operations

### `lpush()`

**Signature:**
```sql
pgkv.lpush(p_key TEXT, VARIADIC p_values TEXT[]) RETURNS INTEGER
```

**Parameters:**
- `p_key` (TEXT) - The list key
- `p_values` (TEXT[]) - One or more values to push

**Returns:**
- INTEGER - Length of list after push

**Description:**

Pushes value(s) to the head of a list. Creates the list if it doesn't exist.

**Notes:**
- Type checking: Requires type `'list'`
- **Redis behavior:** Multiple values prepended in reverse order
  - `LPUSH key a b c` results in `[c, b, a]` (not `[a, b, c]`)
- Storage format: JSONB array `["item1", "item2"]`

---

### `rpush()`

**Signature:**
```sql
pgkv.rpush(p_key TEXT, VARIADIC p_values TEXT[]) RETURNS INTEGER
```

**Parameters:**
- `p_key` (TEXT) - The list key
- `p_values` (TEXT[]) - One or more values to push

**Returns:**
- INTEGER - Length of list after push

**Description:**

Pushes value(s) to the tail of a list. Creates the list if it doesn't exist.

**Notes:**
- Type checking: Requires type `'list'`
- Values appended in order (left to right)

---

### `lpop()`

**Signature:**
```sql
pgkv.lpop(p_key TEXT, p_count INTEGER DEFAULT 1) RETURNS SETOF JSONB
```

**Parameters:**
- `p_key` (TEXT) - The list key
- `p_count` (INTEGER, optional) - Number of elements to pop (default: 1)

**Returns:**
- SETOF JSONB - Popped values from head

**Description:**

Pops value(s) from the head of a list.

**Notes:**
- Type checking: Requires type `'list'`
- Returns empty set if key doesn't exist
- Automatically deletes key if list becomes empty
- Returns values in order popped

---

### `rpop()`

**Signature:**
```sql
pgkv.rpop(p_key TEXT, p_count INTEGER DEFAULT 1) RETURNS SETOF JSONB
```

**Parameters:**
- `p_key` (TEXT) - The list key
- `p_count` (INTEGER, optional) - Number of elements to pop (default: 1)

**Returns:**
- SETOF JSONB - Popped values from tail (in reverse order)

**Description:**

Pops value(s) from the tail of a list.

**Notes:**
- Type checking: Requires type `'list'`
- Returns values in reverse order from tail
- Automatically deletes key if list becomes empty

---

### `llen()`

**Signature:**
```sql
pgkv.llen(p_key TEXT) RETURNS INTEGER
```

**Parameters:**
- `p_key` (TEXT) - The list key

**Returns:**
- INTEGER - Length of list, or `0` if doesn't exist

**Description:**

Gets the length of a list.

**Notes:**
- Type checking: Requires type `'list'`
- Returns `0` for missing key

---

### `lrange()`

**Signature:**
```sql
pgkv.lrange(p_key TEXT, p_start INTEGER, p_stop INTEGER) RETURNS SETOF JSONB
```

**Parameters:**
- `p_key` (TEXT) - The list key
- `p_start` (INTEGER) - Start index (0-based, supports negative)
- `p_stop` (INTEGER) - Stop index (0-based, inclusive, supports negative)

**Returns:**
- SETOF JSONB - Elements in range

**Description:**

Gets a range of elements from a list.

**Notes:**
- Type checking: Requires type `'list'`
- Negative indices supported: `-1` = last, `-2` = second-to-last
- Stop index is inclusive
- Indices automatically clamped to valid range
- Returns elements in order

---

### `lindex()`

**Signature:**
```sql
pgkv.lindex(p_key TEXT, p_index INTEGER) RETURNS JSONB
```

**Parameters:**
- `p_key` (TEXT) - The list key
- `p_index` (INTEGER) - Element index (0-based, supports negative)

**Returns:**
- JSONB - Element at index, or NULL if out of range

**Description:**

Gets an element from a list by its index.

**Notes:**
- Type checking: Requires type `'list'`
- Negative indices supported
- Returns NULL for out-of-bounds index
- Returns NULL for missing key

---

### `lset()`

**Signature:**
```sql
pgkv.lset(p_key TEXT, p_index INTEGER, p_value TEXT) RETURNS TEXT
```

**Parameters:**
- `p_key` (TEXT) - The list key
- `p_index` (INTEGER) - Element index (0-based, supports negative)
- `p_value` (TEXT) - New value

**Returns:**
- TEXT - Always returns `'OK'`

**Description:**

Sets the value of an element at a specific index.

**Notes:**
- Type checking: Requires type `'list'`
- Raises `'no such key'` error if key doesn't exist
- Raises `'index out of range'` error if invalid index
- Negative indices supported

---

### `ltrim()`

**Signature:**
```sql
pgkv.ltrim(p_key TEXT, p_start INTEGER, p_stop INTEGER) RETURNS TEXT
```

**Parameters:**
- `p_key` (TEXT) - The list key
- `p_start` (INTEGER) - Start index (0-based, supports negative)
- `p_stop` (INTEGER) - Stop index (0-based, inclusive, supports negative)

**Returns:**
- TEXT - Always returns `'OK'`

**Description:**

Trims a list to the specified range, removing all elements outside the range.

**Notes:**
- Type checking: Requires type `'list'`
- Automatically deletes key if resulting range is empty
- Negative indices supported
- Stop index is inclusive

---

### `lrem()`

**Signature:**
```sql
pgkv.lrem(p_key TEXT, p_count INTEGER, p_value TEXT) RETURNS INTEGER
```

**Parameters:**
- `p_key` (TEXT) - The list key
- `p_count` (INTEGER) - Direction and limit
  - `> 0`: Remove first N occurrences from head
  - `< 0`: Remove last N occurrences from tail
  - `= 0`: Remove all occurrences
- `p_value` (TEXT) - Value to remove

**Returns:**
- INTEGER - Number of elements removed

**Description:**

Removes elements matching value from a list with directional control.

**Notes:**
- Type checking: Requires type `'list'`
- Automatically deletes key if list becomes empty
- Count parameter controls direction and limit

---

## Set Operations

### `sadd()`

**Signature:**
```sql
pgkv.sadd(p_key TEXT, VARIADIC p_members TEXT[]) RETURNS INTEGER
```

**Parameters:**
- `p_key` (TEXT) - The set key
- `p_members` (TEXT[]) - One or more members to add

**Returns:**
- INTEGER - Number of members actually added (excludes duplicates)

**Description:**

Adds member(s) to a set, enforcing uniqueness. Creates the set if it doesn't exist.

**Notes:**
- Type checking: Requires type `'set'`
- Storage format: JSONB array `["member1", "member2"]`
- Uniqueness enforced: only new members counted in return value
- Attempting to add existing member returns `0`

---

### `srem()`

**Signature:**
```sql
pgkv.srem(p_key TEXT, VARIADIC p_members TEXT[]) RETURNS INTEGER
```

**Parameters:**
- `p_key` (TEXT) - The set key
- `p_members` (TEXT[]) - One or more members to remove

**Returns:**
- INTEGER - Number of members successfully removed

**Description:**

Removes member(s) from a set.

**Notes:**
- Type checking: Requires type `'set'`
- Automatically deletes key if set becomes empty
- Returns `0` if key doesn't exist

---

### `smembers()`

**Signature:**
```sql
pgkv.smembers(p_key TEXT) RETURNS SETOF JSONB
```

**Parameters:**
- `p_key` (TEXT) - The set key

**Returns:**
- SETOF JSONB - All members

**Description:**

Gets all members of a set.

**Notes:**
- Type checking: Requires type `'set'`
- Returns empty set if key doesn't exist
- Member order is not guaranteed (sets are unordered)

---

### `sismember()`

**Signature:**
```sql
pgkv.sismember(p_key TEXT, p_member TEXT) RETURNS INTEGER
```

**Parameters:**
- `p_key` (TEXT) - The set key
- `p_member` (TEXT) - The member to check

**Returns:**
- INTEGER - `1` if member exists, `0` otherwise

**Description:**

Checks if a member exists in a set.

**Notes:**
- Type checking: Requires type `'set'`
- Returns `0` for missing key
- Redis-compatible return values (1/0 not boolean)

---

### `scard()`

**Signature:**
```sql
pgkv.scard(p_key TEXT) RETURNS INTEGER
```

**Parameters:**
- `p_key` (TEXT) - The set key

**Returns:**
- INTEGER - Set cardinality (size)

**Description:**

Gets the number of members in a set.

**Notes:**
- Type checking: Requires type `'set'`
- Returns `0` for missing key

---

### `sinter()`

**Signature:**
```sql
pgkv.sinter(VARIADIC p_keys TEXT[]) RETURNS SETOF JSONB
```

**Parameters:**
- `p_keys` (TEXT[]) - One or more set keys

**Returns:**
- SETOF JSONB - Members present in all sets (intersection)

**Description:**

Returns the intersection of multiple sets (members that exist in all specified sets).

**Notes:**
- Type checking: All keys must be type `'set'`
- Returns empty set if any set is missing
- **Performance:** May be slow on very large sets (O(n*m) complexity)

---

### `sunion()`

**Signature:**
```sql
pgkv.sunion(VARIADIC p_keys TEXT[]) RETURNS SETOF JSONB
```

**Parameters:**
- `p_keys` (TEXT[]) - One or more set keys

**Returns:**
- SETOF JSONB - All unique members from all sets (union)

**Description:**

Returns the union of multiple sets (all unique members from all specified sets).

**Notes:**
- Type checking: All keys must be type `'set'`
- Skips missing keys (continues with remaining sets)
- **Performance:** May be slow on very large sets

---

### `sdiff()`

**Signature:**
```sql
pgkv.sdiff(VARIADIC p_keys TEXT[]) RETURNS SETOF JSONB
```

**Parameters:**
- `p_keys` (TEXT[]) - One or more set keys (first set minus others)

**Returns:**
- SETOF JSONB - Members in first set but not in any other set

**Description:**

Returns the set difference (members in the first set that are not in any of the other sets).

**Notes:**
- Type checking: All keys must be type `'set'`
- Returns empty set if first set is missing
- Order matters: first set is base, others are subtracted
- **Performance:** May be slow on very large sets

---

## Sorted Set Operations

**Storage Format:** JSONB object with members as keys and scores as values: `{"member1": 100.5, "member2": 200}`

### `zadd()`

**Signature:**
```sql
pgkv.zadd(p_key TEXT, VARIADIC p_pairs TEXT[]) RETURNS INTEGER
```

**Parameters:**
- `p_key` (TEXT) - The sorted set key
- `p_pairs` (TEXT[]) - Score-member pairs: `['100', 'member1', '200', 'member2', ...]`

**Returns:**
- INTEGER - Number of members added (not including score updates)

**Description:**

Adds members with scores to a sorted set, or updates scores if members already exist.

**Notes:**
- Type checking: Requires type `'zset'`
- Format: score, member, score, member... (alternating)
- Returns count of NEW members only (updates don't count)
- Requires even number of arguments

---

### `zrem()`

**Signature:**
```sql
pgkv.zrem(p_key TEXT, VARIADIC p_members TEXT[]) RETURNS INTEGER
```

**Parameters:**
- `p_key` (TEXT) - The sorted set key
- `p_members` (TEXT[]) - One or more members to remove

**Returns:**
- INTEGER - Number of members successfully removed

**Description:**

Removes member(s) from a sorted set.

**Notes:**
- Type checking: Requires type `'zset'`
- Automatically deletes key if set becomes empty
- Returns `0` if key doesn't exist

---

### `zrange()`

**Signature:**
```sql
pgkv.zrange(p_key TEXT, p_start INTEGER, p_stop INTEGER, p_withscores BOOLEAN DEFAULT FALSE)
RETURNS TABLE(member TEXT, score NUMERIC)
```

**Parameters:**
- `p_key` (TEXT) - The sorted set key
- `p_start` (INTEGER) - Start rank (0-based, supports negative)
- `p_stop` (INTEGER) - Stop rank (0-based, inclusive, supports negative)
- `p_withscores` (BOOLEAN, optional) - Include scores in output (default: FALSE)

**Returns:**
- TABLE - Members and optional scores (sorted ascending by score)

**Description:**

Gets members by rank range, sorted in ascending order by score.

**Notes:**
- Type checking: Requires type `'zset'`
- Sorts by score ASC, then member name ASC (for ties)
- Negative indices supported: `-1` = highest score
- Score column is NULL if `withscores=FALSE`
- Uses window functions for ranking

---

### `zrevrange()`

**Signature:**
```sql
pgkv.zrevrange(p_key TEXT, p_start INTEGER, p_stop INTEGER, p_withscores BOOLEAN DEFAULT FALSE)
RETURNS TABLE(member TEXT, score NUMERIC)
```

**Parameters:**
- Same as `zrange()`

**Returns:**
- TABLE - Members and optional scores (sorted descending by score)

**Description:**

Gets members by rank range, sorted in descending order by score (highest first).

**Notes:**
- Type checking: Requires type `'zset'`
- Sorts by score DESC, then member name DESC (for ties)
- Otherwise same behavior as `zrange()`

---

### `zscore()`

**Signature:**
```sql
pgkv.zscore(p_key TEXT, p_member TEXT) RETURNS NUMERIC
```

**Parameters:**
- `p_key` (TEXT) - The sorted set key
- `p_member` (TEXT) - The member name

**Returns:**
- NUMERIC - Score of the member, or NULL if not found

**Description:**

Gets the score of a member in a sorted set.

**Notes:**
- Type checking: Requires type `'zset'`
- Returns NULL for missing member or missing key

---

### `zcard()`

**Signature:**
```sql
pgkv.zcard(p_key TEXT) RETURNS INTEGER
```

**Parameters:**
- `p_key` (TEXT) - The sorted set key

**Returns:**
- INTEGER - Number of members in the sorted set

**Description:**

Gets the number of members in a sorted set.

**Notes:**
- Type checking: Requires type `'zset'`
- Returns `0` for missing key

---

### `zrank()`

**Signature:**
```sql
pgkv.zrank(p_key TEXT, p_member TEXT) RETURNS INTEGER
```

**Parameters:**
- `p_key` (TEXT) - The sorted set key
- `p_member` (TEXT) - The member name

**Returns:**
- INTEGER - Rank (0-based, ascending order), or NULL if not found

**Description:**

Gets the rank of a member when sorted in ascending order by score (lowest score = rank 0).

**Notes:**
- Type checking: Requires type `'zset'`
- 0-based indexing: `0` = lowest score
- Ties broken by lexicographic member name order
- Returns NULL for missing member

---

### `zrevrank()`

**Signature:**
```sql
pgkv.zrevrank(p_key TEXT, p_member TEXT) RETURNS INTEGER
```

**Parameters:**
- `p_key` (TEXT) - The sorted set key
- `p_member` (TEXT) - The member name

**Returns:**
- INTEGER - Reverse rank (0-based, descending order), or NULL if not found

**Description:**

Gets the rank of a member when sorted in descending order by score (highest score = rank 0).

**Notes:**
- Type checking: Requires type `'zset'`
- 0-based indexing: `0` = highest score
- Returns NULL for missing member

---

### `zrangebyscore()`

**Signature:**
```sql
pgkv.zrangebyscore(p_key TEXT, p_min NUMERIC, p_max NUMERIC, p_withscores BOOLEAN DEFAULT FALSE)
RETURNS TABLE(member TEXT, score NUMERIC)
```

**Parameters:**
- `p_key` (TEXT) - The sorted set key
- `p_min` (NUMERIC) - Minimum score (inclusive)
- `p_max` (NUMERIC) - Maximum score (inclusive)
- `p_withscores` (BOOLEAN, optional) - Include scores in output

**Returns:**
- TABLE - Members with scores between min and max (sorted ascending)

**Description:**

Gets members with scores between min and max (inclusive range).

**Notes:**
- Type checking: Requires type `'zset'`
- Sorts by score ASC, then member name ASC
- Both min and max are inclusive
- Score column NULL if `withscores=FALSE`

---

### `zincrby()`

**Signature:**
```sql
pgkv.zincrby(p_key TEXT, p_increment NUMERIC, p_member TEXT) RETURNS NUMERIC
```

**Parameters:**
- `p_key` (TEXT) - The sorted set key
- `p_increment` (NUMERIC) - Amount to increment by (can be negative)
- `p_member` (TEXT) - The member name

**Returns:**
- NUMERIC - New score after increment

**Description:**

Increments the score of a member by the given amount.

**Notes:**
- Type checking: Requires type `'zset'`
- Creates sorted set and/or member if doesn't exist (starts at score 0)
- Supports negative increment (decrement)
- Returns new score value

---

### `zcount()`

**Signature:**
```sql
pgkv.zcount(p_key TEXT, p_min NUMERIC, p_max NUMERIC) RETURNS INTEGER
```

**Parameters:**
- `p_key` (TEXT) - The sorted set key
- `p_min` (NUMERIC) - Minimum score (inclusive)
- `p_max` (NUMERIC) - Maximum score (inclusive)

**Returns:**
- INTEGER - Count of members with scores in range

**Description:**

Counts members with scores between min and max (inclusive).

**Notes:**
- Type checking: Requires type `'zset'`
- Returns `0` for missing key
- Both min and max are inclusive

---

## Maintenance Operations

### `cleanup_expired()`

**Signature:**
```sql
pgkv.cleanup_expired() RETURNS INTEGER
```

**Parameters:**
- None

**Returns:**
- INTEGER - Number of keys deleted

**Description:**

Removes all expired keys from the store (all data types).

**Notes:**
- Works on all data types
- Should be scheduled periodically (e.g., with pg_cron)
- Uses `clock_timestamp()` for accuracy
- Consider scheduling every 5-15 minutes in production

**Example scheduling:**
```sql
-- Using pg_cron
SELECT cron.schedule('pgkv-cleanup', '*/5 * * * *', 'SELECT pgkv.cleanup_expired()');
```

---

### `flushall()`

**Signature:**
```sql
pgkv.flushall() RETURNS TEXT
```

**Parameters:**
- None

**Returns:**
- TEXT - Always returns `'OK'`

**Description:**

Deletes all keys in the store using TRUNCATE for performance.

**Notes:**
- **WARNING:** Irreversible operation - all data will be lost
- Works on all data types
- Very fast operation (TRUNCATE TABLE)
- Use with extreme caution in production

---

## Common Implementation Patterns

### Lazy Expiration

All read operations check for expiration and automatically delete expired keys:

```sql
IF v_expires_at IS NOT NULL AND v_expires_at < clock_timestamp() THEN
    DELETE FROM pgkv.store WHERE key = p_key;
    RETURN [appropriate_value];
END IF;
```

This approach:
- Minimizes background overhead
- Ensures expired keys never returned
- Uses `clock_timestamp()` for transaction-accurate timing
- Complements periodic `cleanup_expired()` calls

---

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

---

### JSONB Storage Patterns

Different data types use optimized JSONB representations:

| Type | JSONB Storage | Example | Notes |
|------|---------------|---------|-------|
| string | String or number | `"Hello"` or `42` | Counters stored as raw numbers |
| hash | Object | `{"field": "value"}` | Field-value pairs |
| list | Array | `["a", "b", "c"]` | Ordered elements |
| set | Array | `["x", "y"]` | Uniqueness enforced by code |
| zset | Object | `{"member": 100.5}` | Members as keys, scores as values |

Benefits:
- ~5-15% overhead vs TEXT
- Rich querying capabilities with JSONB operators
- Native PostgreSQL type support
- Can add GIN indexes for nested queries

---

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

---

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

---

### Uniqueness Enforcement (Sets)

`sadd()` enforces uniqueness by checking before adding:

```sql
IF NOT (v_value @> to_jsonb(p_member)) THEN
    v_value := v_value || to_jsonb(p_member);
    v_added := v_added + 1;
END IF;
```

This ensures:
- O(n) complexity per add (JSONB containment check)
- Accurate count of new members
- No duplicate storage

---

### Sorted Set Ranking

SORTED SET operations use PostgreSQL window functions for efficient ranking:

```sql
SELECT
    kv.key::text AS member,
    (kv.value::text)::numeric AS score,
    ROW_NUMBER() OVER (ORDER BY (kv.value::text)::numeric ASC, kv.key::text ASC) - 1 AS rank
FROM jsonb_each(v_value) kv
```

Features:
- O(n log n) sorting per operation
- Ties broken by lexicographic member name order (Redis-compatible)
- Window functions for ranking
- In-memory sorting (fast for typical dataset sizes)

---

## Summary

**Total Functions:** 48

**By Category:**
- Helper Functions: 1
- Basic Operations: 6
- TTL Operations: 2
- String Operations: 9
- Multi-Key Operations: 2
- Hash Operations: 10
- List Operations: 10
- Set Operations: 8
- Sorted Set Operations: 11
- Maintenance: 2

**All 5 core Redis data types implemented:**
- String (with counters and ranges)
- Hash (field-value maps)
- List (ordered collections with negative indices)
- Set (unordered unique collections)
- Sorted Set (score-ordered collections)

**Key Features:**
- JSONB storage for type flexibility
- Lazy expiration on all reads
- Type validation with WRONGTYPE errors
- Negative index support (Lists, Sorted Sets)
- Pattern matching optimization
- ACID compliance (PostgreSQL transactions)
- Safe for restricted environments (PL/pgSQL only)
