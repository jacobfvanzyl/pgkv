# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

pgkv is a Redis-like key-value store PostgreSQL Trusted Language Extension (TLE) that provides familiar Redis commands directly in PostgreSQL. It's written entirely in PL/pgSQL, making it safe to run in restricted environments.

## Architecture

### Core Components

- **Storage**: Single table `pgkv.store` with columns:
  - `key` (TEXT, PRIMARY KEY): The key
  - `value` (JSONB): The value (stored as JSONB for type flexibility and querying)
  - `type` (TEXT): Data type - 'string', 'list', 'set', 'hash', 'zset' (all implemented in v0.1.0)
  - `expires_at` (TIMESTAMPTZ): Optional expiration timestamp for TTL support
  - `created_at` (TIMESTAMPTZ): Creation timestamp
  - `updated_at` (TIMESTAMPTZ): Last update timestamp

- **Schema**: All functions and tables are in the `pgkv` schema for namespace isolation

- **Type System**: Each key has an associated Redis data type (all 5 core types implemented)
  - `string` - Simple strings and numbers
  - `hash` - Field-value maps (like objects)
  - `list` - Ordered lists with negative index support
  - `set` - Unordered collections with uniqueness enforcement
  - `zset` - Sorted sets ordered by numeric score
  - Functions check type and raise `WRONGTYPE` error for mismatches

- **JSONB Storage Patterns**:
  - `string`: `"text value"` or `42` (raw number for counters)
  - `hash`: `{"field1": "value1", "field2": "100"}`
  - `list`: `["item1", "item2", "item3"]`
  - `set`: `["member1", "member2"]` (uniqueness enforced in SADD)
  - `zset`: `{"member1": 100.5, "member2": 200}` (members as keys, scores as values)

- **TTL Strategy**:
  - Lazy expiration: Keys are checked on read operations
  - Manual cleanup: `cleanup_expired()` function removes expired keys
  - Index on `expires_at` for efficient queries

- **Pattern Matching**: Redis glob patterns with automatic optimization
  - Simple patterns (`*`, `?`) converted to SQL LIKE for performance
  - Character classes (`[...]`) automatically fall back to regex (`~` operator)
  - Helper function: `redis_to_pg_pattern()` handles conversion

### Implemented Redis Commands (43 functions)

**Helper Functions** (`pgkv--0.1.0.sql:29-82`):
- `redis_to_pg_pattern(pattern)` - Convert Redis glob to PostgreSQL pattern

**Basic Operations** (`pgkv--0.1.0.sql:88-509`):
- `set(key, value, ttl?)` - Set key with optional TTL, converts to JSONB, type='string'
- `get(key)` - Get JSONB value, returns NULL if expired
- `del(keys...)` - Delete one or more keys (any type)
- `exists(keys...)` - Check if keys exist (any type)
- `expire(key, seconds)` - Set TTL on existing key (any type)
- `ttl(key)` - Get remaining seconds (-1 if no expiry, -2 if no key)
- `incr/decr/incrby/decrby(key, ...)` - Counter operations (type='string')
- `mget(keys...)` / `mset(k1, v1, ...)` - Multi-key operations
- `keys(pattern)` - Pattern matching with auto-optimization
- `dbsize()` - Count all keys
- `type(key)` - Return data type or 'none'

**Additional String Operations** (`pgkv--0.1.0.sql:511-755`):
- `append(key, value)` - Append to string, returns new length
- `strlen(key)` - Get string length
- `getrange(key, start, end)` - Get substring (supports negative indices)
- `setrange(key, offset, value)` - Overwrite substring, returns new length

**HASH Operations** (`pgkv--0.1.0.sql:757-1203`):
- `hset(key, field, value, ...)` - Set field(s), returns count of new fields
- `hget(key, field)` - Get field value as JSONB
- `hmget(key, field, ...)` - Get multiple fields as TABLE
- `hgetall(key)` - Get all fields and values as TABLE
- `hdel(key, field, ...)` - Delete field(s), returns count deleted
- `hexists(key, field)` - Check if field exists (1/0)
- `hlen(key)` - Count fields in hash
- `hkeys(key)` - Get all field names
- `hvals(key)` - Get all values
- `hincrby(key, field, increment)` - Increment numeric field

**LIST Operations** (`pgkv--0.1.0.sql:1205-1747`):
- `lpush(key, value, ...)` - Push to head (prepends in reverse order)
- `rpush(key, value, ...)` - Push to tail (appends)
- `lpop(key, count?)` - Pop from head (single value or SETOF)
- `rpop(key, count?)` - Pop from tail (single value or SETOF)
- `llen(key)` - Get list length
- `lrange(key, start, stop)` - Get range (negative indices supported)
- `lindex(key, index)` - Get element by index (negative supported)
- `lset(key, index, value)` - Set element by index
- `ltrim(key, start, stop)` - Trim list to range
- `lrem(key, count, value)` - Remove by value (directional with count)

**SET Operations** (`pgkv--0.1.0.sql:1749-2206`):
- `sadd(key, member, ...)` - Add members (enforces uniqueness)
- `srem(key, member, ...)` - Remove members
- `smembers(key)` - Get all members
- `sismember(key, member)` - Check membership (1/0)
- `scard(key)` - Get set size
- `sinter(key, ...)` - Set intersection
- `sunion(key, ...)` - Set union
- `sdiff(key, ...)` - Set difference

**SORTED SET Operations** (`pgkv--0.1.0.sql:2208-2768`):
- `zadd(key, score, member, ...)` - Add with scores (returns count of new)
- `zrem(key, member, ...)` - Remove members
- `zrange(key, start, stop, withscores?)` - Get by rank (ascending)
- `zrevrange(key, start, stop, withscores?)` - Get by rank (descending)
- `zscore(key, member)` - Get score of member
- `zcard(key)` - Get sorted set size
- `zrank(key, member)` - Get rank (0-based, ascending)
- `zrevrank(key, member)` - Get reverse rank (0-based, descending)
- `zrangebyscore(key, min, max, withscores?)` - Get by score range
- `zincrby(key, increment, member)` - Increment score
- `zcount(key, min, max)` - Count in score range

**Maintenance** (`pgkv--0.1.0.sql:2770+`):
- `cleanup_expired()` - Remove expired keys (all types)
- `flushall()` - Delete all keys (all types)

## Development Commands

### Local Testing with PostgreSQL

```bash
# Start local PostgreSQL (if using Docker)
docker run -d --name postgres -e POSTGRES_PASSWORD=postgres -p 5432:5432 postgres:16

# Copy extension files
docker cp pgkv.control postgres:/tmp/
docker cp pgkv--0.1.0.sql postgres:/tmp/

# Install extension files
docker exec postgres bash -c "
  cp /tmp/pgkv.control /usr/share/postgresql/16/extension/
  cp /tmp/pgkv--0.1.0.sql /usr/share/postgresql/16/extension/
"

# Create extension in database
docker exec -it postgres psql -U postgres -c "CREATE EXTENSION pgkv;"

# Test interactively
docker exec -it postgres psql -U postgres
```

### Running Tests

```bash
# Install pgTAP in container
docker exec postgres bash -c "apt-get update && apt-get install -y postgresql-16-pgtap"

# Run all tests
for test in tests/*.sql; do
  echo "Running $test..."
  docker exec -i postgres psql -U postgres < "$test"
done

# Run specific test
docker exec -i postgres psql -U postgres < tests/01-basic-operations.sql
```

## File Structure

```
pgkv/
├── pgkv.control                        # Extension metadata
├── pgkv--0.1.0.sql                     # Version 0.1.0 implementation (~2,800 lines)
├── README.md                           # User documentation
├── CLAUDE.md                           # This file
├── LICENSE                             # MIT license
├── .gitignore                          # Git ignore rules
├── .github/
│   └── workflows/
│       └── test.yml                    # CI/CD testing (Postgres 12-16)
└── tests/
    ├── 01-basic-operations.sql         # Basic command tests (25 tests)
    ├── 02-ttl-functionality.sql        # TTL/expiration tests (14 tests)
    ├── 03-advanced-commands.sql        # INCR/MGET/MSET/KEYS tests (30 tests)
    ├── 04-hash-commands.sql            # HASH operations tests (35 tests)
    ├── 05-list-commands.sql            # LIST operations tests (45 tests)
    ├── 06-set-commands.sql             # SET operations tests (35 tests)
    ├── 07-sorted-set-commands.sql      # SORTED SET operations tests (50 tests)
    └── 08-string-additional.sql        # Additional string ops tests (30 tests)
```

## Modifying the Extension

### Adding New Functions

1. Add the function to `pgkv--0.1.0.sql` in the appropriate section
2. Follow the pattern: use `pgkv.` schema prefix
3. Add proper error handling
4. Check for expired keys on read operations
5. **Type Checking**: If function is type-specific, check the `type` column
   - Raise `WRONGTYPE Operation against a key holding the wrong kind of value` for mismatches
6. **JSONB Handling**: Remember values are JSONB
   - Use `to_jsonb()` to store
   - Use `#>>'{}'` to extract as text
   - Counter values stored as raw JSONB numbers: `to_jsonb(42::bigint)`
7. Update `README.md` with new function documentation
8. Add tests to appropriate test file in `tests/`

### Creating a New Version

1. Create new SQL file: `pgkv--0.2.0.sql` (copy from 0.1.0 and modify)
2. Create upgrade script: `pgkv--0.1.0--0.2.0.sql` (only the changes)
3. Update `default_version` in `pgkv.control`
4. Update README.md and tests as needed

## Security Model

- All objects are in the `pgkv` schema
- Public has USAGE on schema and EXECUTE on functions
- Public does NOT have direct access to `pgkv.store` table
- All operations must go through the provided functions
- Functions are NOT SECURITY DEFINER (run with caller's privileges)

## Implementation Notes

### Negative Index Support (LIST operations)

LIST operations (`lrange`, `lindex`, `lset`, `ltrim`) support Redis-compatible negative indices:
- Negative indices count from the end: `-1` = last element, `-2` = second-to-last, etc.
- Implementation: `CASE WHEN index < 0 THEN length + index ELSE index END`
- Clamping ensures indices stay within valid range

### Uniqueness Enforcement (SET operations)

`sadd()` enforces uniqueness by checking before adding:
```sql
IF NOT (v_value @> to_jsonb(v_member)) THEN
    v_value := v_value || to_jsonb(v_member);
    v_added := v_added + 1;
END IF;
```

### Score Sorting (SORTED SET operations)

SORTED SET operations use PostgreSQL window functions for ranking:
- Extract JSONB object to (key, value) pairs with `jsonb_each()`
- Cast values to NUMERIC for score comparison
- Use `ROW_NUMBER() OVER (ORDER BY score, member)` for ranking
- Ties broken by lexicographic member name order (Redis-compatible)
- WITHSCORES option conditionally returns scores using `CASE`

Example from `zrange()`:
```sql
SELECT
    kv.key::text AS member,
    (kv.value::text)::numeric AS score,
    ROW_NUMBER() OVER (ORDER BY (kv.value::text)::numeric ASC, kv.key::text ASC) - 1 AS rank
FROM jsonb_each(v_value) kv
```

### LPUSH/RPUSH Behavior

Redis LPUSH with multiple values prepends them in reverse order:
- `LPUSH mylist a b c` results in `[c, b, a]` (if list was empty)
- Implementation: reverse loop through variadic args for LPUSH
- RPUSH maintains order as expected

### LREM Directional Removal

`lrem(key, count, value)` supports directional removal:
- `count > 0`: Remove first N occurrences from head
- `count < 0`: Remove last N occurrences from tail
- `count = 0`: Remove all occurrences
- Implementation uses array iteration with conditional removal

### Type Checking Pattern

All data-type-specific functions check type and raise WRONGTYPE:
```sql
IF v_type IS NOT NULL AND v_type != 'expected_type' THEN
    RAISE EXCEPTION 'WRONGTYPE Operation against a key holding the wrong kind of value';
END IF;
```

### Expiration Handling Pattern

All read operations check expiration and lazily delete:
```sql
IF v_expires_at IS NOT NULL AND v_expires_at < NOW() THEN
    DELETE FROM pgkv.store WHERE key = p_key;
    RETURN [appropriate_value];
END IF;
```

## Performance Notes

- **JSONB Storage**: ~5-15% overhead vs TEXT, but enables rich querying
  - Can add GIN indexes for nested field queries if needed
  - Counter operations store raw numbers (not strings) for efficiency
- **Pattern Matching Optimization**:
  - Simple patterns (`user:*`, `session:?????`) use fast LIKE queries
  - Character classes (`h[ae]llo`) automatically use regex (slower but correct)
- **Type Column**: Indexed for future filtering by data type
- The `keys()` function scans all keys - use sparingly in production
- TTL cleanup happens on-demand; consider scheduling `cleanup_expired()` with pg_cron
- All operations are ACID-compliant (unlike Redis)
- Single table design - simple but not optimized for complex queries
- Consider UNLOGGED table for cache-like behavior (faster, less durable)
- **SET operations** (SINTER, SUNION, SDIFF) may be slow on very large sets (O(n*m))
- **SORTED SET sorting** is O(n log n) on each operation (PostgreSQL sorts in-memory)
- Negative index calculations add minimal overhead (simple arithmetic)
