-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION pgkv" to load this file. \quit

-- Create dedicated schema for namespace isolation
CREATE SCHEMA IF NOT EXISTS pgkv;

-- Create storage table for key-value pairs
CREATE TABLE IF NOT EXISTS pgkv.store (
    key TEXT PRIMARY KEY,
    value JSONB,
    type TEXT NOT NULL DEFAULT 'string',
    expires_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    CONSTRAINT valid_type CHECK (type IN ('string', 'list', 'set', 'hash', 'zset'))
);

-- Create index for TTL cleanup and expiration checks
CREATE INDEX IF NOT EXISTS idx_pgkv_expires_at ON pgkv.store(expires_at)
    WHERE expires_at IS NOT NULL;

-- Create index on type for future queries
CREATE INDEX IF NOT EXISTS idx_pgkv_type ON pgkv.store(type);

-- ============================================================================
-- Helper Functions
-- ============================================================================

-- REDIS_TO_PG_PATTERN: Convert Redis glob pattern to PostgreSQL pattern
-- Returns NULL if pattern contains character classes (requires regex)
CREATE OR REPLACE FUNCTION pgkv.redis_to_pg_pattern(p_redis_pattern TEXT)
RETURNS TEXT
LANGUAGE plpgsql
IMMUTABLE
AS $$
DECLARE
    v_result TEXT := '';
    v_i INTEGER := 1;
    v_char TEXT;
    v_next_char TEXT;
    v_len INTEGER := length(p_redis_pattern);
BEGIN
    WHILE v_i <= v_len LOOP
        v_char := substring(p_redis_pattern FROM v_i FOR 1);

        -- Handle Redis escape sequences
        IF v_char = E'\\' AND v_i < v_len THEN
            v_next_char := substring(p_redis_pattern FROM v_i + 1 FOR 1);
            -- Escaped character becomes literal, but must escape PostgreSQL special chars
            IF v_next_char IN ('%', '_', E'\\') THEN
                v_result := v_result || E'\\' || v_next_char;
            ELSE
                v_result := v_result || v_next_char;
            END IF;
            v_i := v_i + 2;
            CONTINUE;
        END IF;

        -- Check for character classes (not supported by LIKE, need regex)
        IF v_char = '[' THEN
            -- Return NULL to signal caller to use regex
            RETURN NULL;
        END IF;

        -- Convert Redis wildcards to PostgreSQL LIKE wildcards
        IF v_char = '*' THEN
            v_result := v_result || '%';
        ELSIF v_char = '?' THEN
            v_result := v_result || '_';
        -- Escape PostgreSQL special characters
        ELSIF v_char IN ('%', '_', E'\\') THEN
            v_result := v_result || E'\\' || v_char;
        ELSE
            v_result := v_result || v_char;
        END IF;

        v_i := v_i + 1;
    END LOOP;

    RETURN v_result;
END;
$$;

-- ============================================================================
-- Basic Key-Value Operations
-- ============================================================================

-- SET: Set a key with optional TTL (time-to-live in seconds)
-- Value is automatically converted to JSONB (strings must be valid JSON strings)
CREATE OR REPLACE FUNCTION pgkv.set(p_key TEXT, p_value TEXT, p_ttl_seconds INTEGER DEFAULT NULL)
RETURNS TEXT
LANGUAGE plpgsql
AS $$
BEGIN
    INSERT INTO pgkv.store (key, value, type, expires_at, updated_at)
    VALUES (
        p_key,
        to_jsonb(p_value),
        'string',
        CASE WHEN p_ttl_seconds IS NOT NULL
            THEN NOW() + (p_ttl_seconds || ' seconds')::INTERVAL
            ELSE NULL
        END,
        NOW()
    )
    ON CONFLICT (key) DO UPDATE
    SET value = EXCLUDED.value,
        type = EXCLUDED.type,
        expires_at = EXCLUDED.expires_at,
        updated_at = NOW();

    RETURN 'OK';
END;
$$;

-- GET: Retrieve value for a key (returns NULL if expired or not found)
-- Returns JSONB value, raises error if key is not of type 'string'
CREATE OR REPLACE FUNCTION pgkv.get(p_key TEXT)
RETURNS JSONB
LANGUAGE plpgsql
AS $$
DECLARE
    v_value JSONB;
    v_type TEXT;
    v_expires_at TIMESTAMPTZ;
BEGIN
    SELECT value, type, expires_at INTO v_value, v_type, v_expires_at
    FROM pgkv.store
    WHERE key = p_key;

    -- Check if key exists
    IF NOT FOUND THEN
        RETURN NULL;
    END IF;

    -- Check if expired
    IF v_expires_at IS NOT NULL AND v_expires_at < NOW() THEN
        DELETE FROM pgkv.store WHERE key = p_key;
        RETURN NULL;
    END IF;

    -- Check type
    IF v_type != 'string' THEN
        RAISE EXCEPTION 'WRONGTYPE Operation against a key holding the wrong kind of value';
    END IF;

    RETURN v_value;
END;
$$;

-- DEL: Delete one or more keys
CREATE OR REPLACE FUNCTION pgkv.del(VARIADIC p_keys TEXT[])
RETURNS INTEGER
LANGUAGE plpgsql
AS $$
DECLARE
    v_deleted INTEGER;
BEGIN
    DELETE FROM pgkv.store WHERE key = ANY(p_keys);
    GET DIAGNOSTICS v_deleted = ROW_COUNT;
    RETURN v_deleted;
END;
$$;

-- EXISTS: Check if a key exists (returns 1 if exists, 0 otherwise)
CREATE OR REPLACE FUNCTION pgkv.exists(VARIADIC p_keys TEXT[])
RETURNS INTEGER
LANGUAGE plpgsql
AS $$
DECLARE
    v_count INTEGER;
    v_key TEXT;
    v_expires_at TIMESTAMPTZ;
    v_exists_count INTEGER := 0;
BEGIN
    FOREACH v_key IN ARRAY p_keys
    LOOP
        SELECT expires_at INTO v_expires_at
        FROM pgkv.store
        WHERE key = v_key;

        IF FOUND THEN
            -- Check if expired
            IF v_expires_at IS NULL OR v_expires_at >= NOW() THEN
                v_exists_count := v_exists_count + 1;
            ELSE
                -- Clean up expired key
                DELETE FROM pgkv.store WHERE key = v_key;
            END IF;
        END IF;
    END LOOP;

    RETURN v_exists_count;
END;
$$;

-- ============================================================================
-- TTL Operations
-- ============================================================================

-- EXPIRE: Set a timeout on a key (in seconds)
CREATE OR REPLACE FUNCTION pgkv.expire(p_key TEXT, p_seconds INTEGER)
RETURNS INTEGER
LANGUAGE plpgsql
AS $$
DECLARE
    v_updated INTEGER;
BEGIN
    UPDATE pgkv.store
    SET expires_at = NOW() + (p_seconds || ' seconds')::INTERVAL,
        updated_at = NOW()
    WHERE key = p_key;

    GET DIAGNOSTICS v_updated = ROW_COUNT;
    RETURN CASE WHEN v_updated > 0 THEN 1 ELSE 0 END;
END;
$$;

-- TTL: Get the time to live for a key (in seconds)
-- Returns -2 if key doesn't exist, -1 if no expiration, or seconds remaining
CREATE OR REPLACE FUNCTION pgkv.ttl(p_key TEXT)
RETURNS INTEGER
LANGUAGE plpgsql
AS $$
DECLARE
    v_expires_at TIMESTAMPTZ;
    v_seconds INTEGER;
BEGIN
    SELECT expires_at INTO v_expires_at
    FROM pgkv.store
    WHERE key = p_key;

    -- Key doesn't exist
    IF NOT FOUND THEN
        RETURN -2;
    END IF;

    -- Key exists but has no expiration
    IF v_expires_at IS NULL THEN
        RETURN -1;
    END IF;

    -- Calculate remaining seconds
    v_seconds := EXTRACT(EPOCH FROM (v_expires_at - NOW()))::INTEGER;

    -- If already expired, delete and return -2
    IF v_seconds <= 0 THEN
        DELETE FROM pgkv.store WHERE key = p_key;
        RETURN -2;
    END IF;

    RETURN v_seconds;
END;
$$;

-- ============================================================================
-- String Operations
-- ============================================================================

-- INCR: Increment the integer value of a key by 1
-- Stores result as raw JSONB number
CREATE OR REPLACE FUNCTION pgkv.incr(p_key TEXT)
RETURNS BIGINT
LANGUAGE plpgsql
AS $$
DECLARE
    v_value JSONB;
    v_numeric BIGINT;
    v_type TEXT;
    v_expires_at TIMESTAMPTZ;
BEGIN
    -- Get current value and type
    SELECT value, type, expires_at INTO v_value, v_type, v_expires_at
    FROM pgkv.store
    WHERE key = p_key;

    -- If key doesn't exist, start from 0
    IF NOT FOUND THEN
        v_numeric := 0;
    ELSE
        -- Check if expired
        IF v_expires_at IS NOT NULL AND v_expires_at < NOW() THEN
            DELETE FROM pgkv.store WHERE key = p_key;
            v_numeric := 0;
        ELSE
            -- Verify type is string
            IF v_type != 'string' THEN
                RAISE EXCEPTION 'WRONGTYPE Operation against a key holding the wrong kind of value';
            END IF;

            -- Try to convert JSONB value to integer
            BEGIN
                v_numeric := (v_value #>> '{}')::BIGINT;
            EXCEPTION WHEN OTHERS THEN
                RAISE EXCEPTION 'value is not an integer or out of range';
            END;
        END IF;
    END IF;

    -- Increment
    v_numeric := v_numeric + 1;

    -- Store as raw JSONB number
    INSERT INTO pgkv.store (key, value, type, updated_at)
    VALUES (p_key, to_jsonb(v_numeric), 'string', NOW())
    ON CONFLICT (key) DO UPDATE
    SET value = EXCLUDED.value,
        type = EXCLUDED.type,
        updated_at = NOW();

    RETURN v_numeric;
END;
$$;

-- DECR: Decrement the integer value of a key by 1
-- Stores result as raw JSONB number
CREATE OR REPLACE FUNCTION pgkv.decr(p_key TEXT)
RETURNS BIGINT
LANGUAGE plpgsql
AS $$
BEGIN
    RETURN pgkv.incrby(p_key, -1);
END;
$$;

-- INCRBY: Increment the integer value of a key by the given amount
-- Stores result as raw JSONB number
CREATE OR REPLACE FUNCTION pgkv.incrby(p_key TEXT, p_increment BIGINT)
RETURNS BIGINT
LANGUAGE plpgsql
AS $$
DECLARE
    v_value JSONB;
    v_numeric BIGINT;
    v_type TEXT;
    v_expires_at TIMESTAMPTZ;
BEGIN
    -- Get current value and type
    SELECT value, type, expires_at INTO v_value, v_type, v_expires_at
    FROM pgkv.store
    WHERE key = p_key;

    -- If key doesn't exist, start from 0
    IF NOT FOUND THEN
        v_numeric := 0;
    ELSE
        -- Check if expired
        IF v_expires_at IS NOT NULL AND v_expires_at < NOW() THEN
            DELETE FROM pgkv.store WHERE key = p_key;
            v_numeric := 0;
        ELSE
            -- Verify type is string
            IF v_type != 'string' THEN
                RAISE EXCEPTION 'WRONGTYPE Operation against a key holding the wrong kind of value';
            END IF;

            -- Try to convert JSONB value to integer
            BEGIN
                v_numeric := (v_value #>> '{}')::BIGINT;
            EXCEPTION WHEN OTHERS THEN
                RAISE EXCEPTION 'value is not an integer or out of range';
            END;
        END IF;
    END IF;

    -- Increment by the specified amount
    v_numeric := v_numeric + p_increment;

    -- Store as raw JSONB number
    INSERT INTO pgkv.store (key, value, type, updated_at)
    VALUES (p_key, to_jsonb(v_numeric), 'string', NOW())
    ON CONFLICT (key) DO UPDATE
    SET value = EXCLUDED.value,
        type = EXCLUDED.type,
        updated_at = NOW();

    RETURN v_numeric;
END;
$$;

-- DECRBY: Decrement the integer value of a key by the given amount
CREATE OR REPLACE FUNCTION pgkv.decrby(p_key TEXT, p_decrement BIGINT)
RETURNS BIGINT
LANGUAGE plpgsql
AS $$
BEGIN
    RETURN pgkv.incrby(p_key, -p_decrement);
END;
$$;

-- ============================================================================
-- Multi-Key Operations
-- ============================================================================

-- MGET: Get the values of all specified keys
-- Returns JSONB values, filters by type='string'
CREATE OR REPLACE FUNCTION pgkv.mget(VARIADIC p_keys TEXT[])
RETURNS TABLE(key TEXT, value JSONB)
LANGUAGE plpgsql
AS $$
DECLARE
    v_key TEXT;
BEGIN
    FOREACH v_key IN ARRAY p_keys
    LOOP
        key := v_key;
        BEGIN
            value := pgkv.get(v_key);
        EXCEPTION
            WHEN OTHERS THEN
                -- If wrong type or other error, return NULL for that key
                value := NULL;
        END;
        RETURN NEXT;
    END LOOP;
END;
$$;

-- MSET: Set multiple keys to multiple values
-- Input format: array of key-value pairs like ARRAY['key1', 'val1', 'key2', 'val2']
CREATE OR REPLACE FUNCTION pgkv.mset(VARIADIC p_pairs TEXT[])
RETURNS TEXT
LANGUAGE plpgsql
AS $$
DECLARE
    v_i INTEGER;
BEGIN
    -- Ensure we have an even number of arguments
    IF array_length(p_pairs, 1) % 2 != 0 THEN
        RAISE EXCEPTION 'wrong number of arguments for MSET';
    END IF;

    -- Set each key-value pair
    FOR v_i IN 1..array_length(p_pairs, 1) BY 2
    LOOP
        PERFORM pgkv.set(p_pairs[v_i], p_pairs[v_i + 1]);
    END LOOP;

    RETURN 'OK';
END;
$$;

-- ============================================================================
-- Key Inspection
-- ============================================================================

-- KEYS: Find all keys matching the given pattern
-- Supports Redis glob patterns: * (any chars), ? (single char), [...] (char class)
-- WARNING: This is expensive on large datasets, use with caution
CREATE OR REPLACE FUNCTION pgkv.keys(p_pattern TEXT DEFAULT '*')
RETURNS SETOF TEXT
LANGUAGE plpgsql
AS $$
DECLARE
    v_pg_pattern TEXT;
BEGIN
    -- First, clean up expired keys
    DELETE FROM pgkv.store
    WHERE expires_at IS NOT NULL AND expires_at < NOW();

    -- Convert Redis pattern to PostgreSQL pattern
    v_pg_pattern := pgkv.redis_to_pg_pattern(p_pattern);

    -- If pattern contains character classes (NULL returned), use regex
    IF v_pg_pattern IS NULL THEN
        RETURN QUERY
        SELECT s.key
        FROM pgkv.store s
        WHERE s.key ~ p_pattern
        ORDER BY s.key;
    ELSE
        -- Use LIKE for simple patterns
        RETURN QUERY
        SELECT s.key
        FROM pgkv.store s
        WHERE s.key LIKE v_pg_pattern
        ORDER BY s.key;
    END IF;
END;
$$;

-- TYPE: Return the type of a key
-- Returns 'string', 'list', 'set', 'hash', 'zset', or 'none' if key doesn't exist
CREATE OR REPLACE FUNCTION pgkv.type(p_key TEXT)
RETURNS TEXT
LANGUAGE plpgsql
AS $$
DECLARE
    v_type TEXT;
    v_expires_at TIMESTAMPTZ;
BEGIN
    SELECT type, expires_at INTO v_type, v_expires_at
    FROM pgkv.store
    WHERE key = p_key;

    -- Key doesn't exist
    IF NOT FOUND THEN
        RETURN 'none';
    END IF;

    -- Check if expired
    IF v_expires_at IS NOT NULL AND v_expires_at < NOW() THEN
        DELETE FROM pgkv.store WHERE key = p_key;
        RETURN 'none';
    END IF;

    RETURN v_type;
END;
$$;

-- APPEND: Append a value to a key (string operation)
-- Returns the length of the string after append
CREATE OR REPLACE FUNCTION pgkv.append(p_key TEXT, p_value TEXT)
RETURNS INTEGER
LANGUAGE plpgsql
AS $$
DECLARE
    v_type TEXT;
    v_value JSONB;
    v_expires_at TIMESTAMPTZ;
    v_current TEXT;
    v_new TEXT;
BEGIN
    -- Get existing value and type
    SELECT value, type, expires_at INTO v_value, v_type, v_expires_at
    FROM pgkv.store
    WHERE key = p_key;

    -- Check if expired
    IF v_expires_at IS NOT NULL AND v_expires_at < NOW() THEN
        DELETE FROM pgkv.store WHERE key = p_key;
        v_value := NULL;
        v_type := NULL;
    END IF;

    -- Check type
    IF v_type IS NOT NULL AND v_type != 'string' THEN
        RAISE EXCEPTION 'WRONGTYPE Operation against a key holding the wrong kind of value';
    END IF;

    -- Get current value as text (or empty string if doesn't exist)
    IF v_value IS NULL THEN
        v_current := '';
    ELSE
        -- Handle both string and numeric JSONB values
        IF jsonb_typeof(v_value) = 'string' THEN
            v_current := v_value #>> '{}';
        ELSE
            v_current := v_value::text;
        END IF;
    END IF;

    -- Append
    v_new := v_current || p_value;

    -- Store
    INSERT INTO pgkv.store (key, value, type, updated_at)
    VALUES (p_key, to_jsonb(v_new), 'string', NOW())
    ON CONFLICT (key) DO UPDATE
    SET value = to_jsonb(v_new),
        type = 'string',
        updated_at = NOW();

    RETURN length(v_new);
END;
$$;

-- STRLEN: Get the length of the value stored in a key
CREATE OR REPLACE FUNCTION pgkv.strlen(p_key TEXT)
RETURNS INTEGER
LANGUAGE plpgsql
AS $$
DECLARE
    v_type TEXT;
    v_value JSONB;
    v_expires_at TIMESTAMPTZ;
    v_text TEXT;
BEGIN
    -- Get existing value and type
    SELECT value, type, expires_at INTO v_value, v_type, v_expires_at
    FROM pgkv.store
    WHERE key = p_key;

    -- Key doesn't exist or expired
    IF v_value IS NULL OR (v_expires_at IS NOT NULL AND v_expires_at < NOW()) THEN
        IF v_expires_at IS NOT NULL AND v_expires_at < NOW() THEN
            DELETE FROM pgkv.store WHERE key = p_key;
        END IF;
        RETURN 0;
    END IF;

    -- Check type
    IF v_type != 'string' THEN
        RAISE EXCEPTION 'WRONGTYPE Operation against a key holding the wrong kind of value';
    END IF;

    -- Get length (handle both string and numeric JSONB values)
    IF jsonb_typeof(v_value) = 'string' THEN
        v_text := v_value #>> '{}';
    ELSE
        v_text := v_value::text;
    END IF;

    RETURN length(v_text);
END;
$$;

-- GETRANGE: Get a substring of the string stored at a key
-- Supports negative indices (Redis-compatible)
CREATE OR REPLACE FUNCTION pgkv.getrange(p_key TEXT, p_start INTEGER, p_end INTEGER)
RETURNS TEXT
LANGUAGE plpgsql
AS $$
DECLARE
    v_type TEXT;
    v_value JSONB;
    v_expires_at TIMESTAMPTZ;
    v_text TEXT;
    v_len INTEGER;
    v_actual_start INTEGER;
    v_actual_end INTEGER;
BEGIN
    -- Get existing value and type
    SELECT value, type, expires_at INTO v_value, v_type, v_expires_at
    FROM pgkv.store
    WHERE key = p_key;

    -- Key doesn't exist or expired
    IF v_value IS NULL OR (v_expires_at IS NOT NULL AND v_expires_at < NOW()) THEN
        IF v_expires_at IS NOT NULL AND v_expires_at < NOW() THEN
            DELETE FROM pgkv.store WHERE key = p_key;
        END IF;
        RETURN '';
    END IF;

    -- Check type
    IF v_type != 'string' THEN
        RAISE EXCEPTION 'WRONGTYPE Operation against a key holding the wrong kind of value';
    END IF;

    -- Get text value (handle both string and numeric JSONB values)
    IF jsonb_typeof(v_value) = 'string' THEN
        v_text := v_value #>> '{}';
    ELSE
        v_text := v_value::text;
    END IF;

    v_len := length(v_text);

    -- Handle negative indices (Redis uses 0-based indexing)
    v_actual_start := CASE WHEN p_start < 0 THEN v_len + p_start ELSE p_start END;
    v_actual_end := CASE WHEN p_end < 0 THEN v_len + p_end ELSE p_end END;

    -- Clamp to valid range (0-based)
    v_actual_start := GREATEST(0, v_actual_start);
    v_actual_end := LEAST(v_len - 1, v_actual_end);

    -- Return empty if invalid range
    IF v_actual_start > v_actual_end OR v_len = 0 THEN
        RETURN '';
    END IF;

    -- PostgreSQL substring uses 1-based indexing, so adjust
    -- Length is (end - start + 1)
    RETURN substring(v_text FROM (v_actual_start + 1) FOR (v_actual_end - v_actual_start + 1));
END;
$$;

-- SETRANGE: Overwrite part of a string at key starting at the specified offset
-- Returns the length of the string after modification
CREATE OR REPLACE FUNCTION pgkv.setrange(p_key TEXT, p_offset INTEGER, p_value TEXT)
RETURNS INTEGER
LANGUAGE plpgsql
AS $$
DECLARE
    v_type TEXT;
    v_value JSONB;
    v_expires_at TIMESTAMPTZ;
    v_current TEXT;
    v_new TEXT;
    v_len INTEGER;
    v_value_len INTEGER;
BEGIN
    -- Offset must be non-negative
    IF p_offset < 0 THEN
        RAISE EXCEPTION 'offset is out of range';
    END IF;

    -- Get existing value and type
    SELECT value, type, expires_at INTO v_value, v_type, v_expires_at
    FROM pgkv.store
    WHERE key = p_key;

    -- Check if expired
    IF v_expires_at IS NOT NULL AND v_expires_at < NOW() THEN
        DELETE FROM pgkv.store WHERE key = p_key;
        v_value := NULL;
        v_type := NULL;
    END IF;

    -- Check type
    IF v_type IS NOT NULL AND v_type != 'string' THEN
        RAISE EXCEPTION 'WRONGTYPE Operation against a key holding the wrong kind of value';
    END IF;

    -- Get current value as text (or empty string if doesn't exist)
    IF v_value IS NULL THEN
        v_current := '';
    ELSE
        -- Handle both string and numeric JSONB values
        IF jsonb_typeof(v_value) = 'string' THEN
            v_current := v_value #>> '{}';
        ELSE
            v_current := v_value::text;
        END IF;
    END IF;

    v_len := length(v_current);
    v_value_len := length(p_value);

    -- If offset is beyond current length, pad with NULL bytes (\x00)
    IF p_offset > v_len THEN
        v_current := v_current || repeat(chr(0), p_offset - v_len);
    END IF;

    -- Build new string: prefix + new value + suffix
    -- Prefix: characters before offset
    -- New value: p_value
    -- Suffix: characters after (offset + p_value length), if any
    IF p_offset = 0 THEN
        -- Starting at beginning
        IF v_value_len >= v_len THEN
            v_new := p_value;
        ELSE
            v_new := p_value || substring(v_current FROM (v_value_len + 1));
        END IF;
    ELSE
        v_new := substring(v_current FROM 1 FOR p_offset) || p_value;
        -- Add remaining characters if original string was longer
        IF p_offset + v_value_len < length(v_current) THEN
            v_new := v_new || substring(v_current FROM (p_offset + v_value_len + 1));
        END IF;
    END IF;

    -- Store
    INSERT INTO pgkv.store (key, value, type, updated_at)
    VALUES (p_key, to_jsonb(v_new), 'string', NOW())
    ON CONFLICT (key) DO UPDATE
    SET value = to_jsonb(v_new),
        type = 'string',
        updated_at = NOW();

    RETURN length(v_new);
END;
$$;

-- ============================================================================
-- HASH Operations
-- ============================================================================

-- HSET: Set field(s) in hash
-- Returns number of fields added (not updated)
CREATE OR REPLACE FUNCTION pgkv.hset(p_key TEXT, VARIADIC p_pairs TEXT[])
RETURNS INTEGER
LANGUAGE plpgsql
AS $$
DECLARE
    v_type TEXT;
    v_value JSONB;
    v_expires_at TIMESTAMPTZ;
    v_i INTEGER;
    v_added INTEGER := 0;
    v_field TEXT;
    v_field_value TEXT;
BEGIN
    -- Validate pairs
    IF array_length(p_pairs, 1) % 2 != 0 THEN
        RAISE EXCEPTION 'wrong number of arguments for HSET';
    END IF;

    -- Get existing value and type
    SELECT value, type, expires_at INTO v_value, v_type, v_expires_at
    FROM pgkv.store
    WHERE key = p_key;

    -- Check if expired
    IF v_expires_at IS NOT NULL AND v_expires_at < NOW() THEN
        DELETE FROM pgkv.store WHERE key = p_key;
        v_value := NULL;
        v_type := NULL;
    END IF;

    -- Check type
    IF v_type IS NOT NULL AND v_type != 'hash' THEN
        RAISE EXCEPTION 'WRONGTYPE Operation against a key holding the wrong kind of value';
    END IF;

    -- Initialize if new
    IF v_value IS NULL THEN
        v_value := '{}'::jsonb;
    END IF;

    -- Set each field
    FOR v_i IN 1..array_length(p_pairs, 1) BY 2
    LOOP
        v_field := p_pairs[v_i];
        v_field_value := p_pairs[v_i + 1];

        -- Count as added if field didn't exist
        IF NOT (v_value ? v_field) THEN
            v_added := v_added + 1;
        END IF;

        -- Set field
        v_value := jsonb_set(v_value, ARRAY[v_field], to_jsonb(v_field_value), true);
    END LOOP;

    -- Store
    INSERT INTO pgkv.store (key, value, type, updated_at)
    VALUES (p_key, v_value, 'hash', NOW())
    ON CONFLICT (key) DO UPDATE
    SET value = EXCLUDED.value,
        type = EXCLUDED.type,
        updated_at = NOW();

    RETURN v_added;
END;
$$;

-- HGET: Get field value from hash
CREATE OR REPLACE FUNCTION pgkv.hget(p_key TEXT, p_field TEXT)
RETURNS JSONB
LANGUAGE plpgsql
AS $$
DECLARE
    v_value JSONB;
    v_type TEXT;
    v_expires_at TIMESTAMPTZ;
BEGIN
    SELECT value, type, expires_at INTO v_value, v_type, v_expires_at
    FROM pgkv.store
    WHERE key = p_key;

    IF NOT FOUND THEN
        RETURN NULL;
    END IF;

    -- Check if expired
    IF v_expires_at IS NOT NULL AND v_expires_at < NOW() THEN
        DELETE FROM pgkv.store WHERE key = p_key;
        RETURN NULL;
    END IF;

    -- Check type
    IF v_type != 'hash' THEN
        RAISE EXCEPTION 'WRONGTYPE Operation against a key holding the wrong kind of value';
    END IF;

    RETURN v_value -> p_field;
END;
$$;

-- HMGET: Get multiple field values from hash
CREATE OR REPLACE FUNCTION pgkv.hmget(p_key TEXT, VARIADIC p_fields TEXT[])
RETURNS TABLE(field TEXT, value JSONB)
LANGUAGE plpgsql
AS $$
DECLARE
    v_value JSONB;
    v_type TEXT;
    v_expires_at TIMESTAMPTZ;
    v_field TEXT;
BEGIN
    SELECT store.value, store.type, store.expires_at INTO v_value, v_type, v_expires_at
    FROM pgkv.store
    WHERE key = p_key;

    IF NOT FOUND THEN
        -- Return NULL for all fields
        FOREACH v_field IN ARRAY p_fields
        LOOP
            field := v_field;
            value := NULL;
            RETURN NEXT;
        END LOOP;
        RETURN;
    END IF;

    -- Check if expired
    IF v_expires_at IS NOT NULL AND v_expires_at < NOW() THEN
        DELETE FROM pgkv.store WHERE key = p_key;
        FOREACH v_field IN ARRAY p_fields
        LOOP
            field := v_field;
            value := NULL;
            RETURN NEXT;
        END LOOP;
        RETURN;
    END IF;

    -- Check type
    IF v_type != 'hash' THEN
        RAISE EXCEPTION 'WRONGTYPE Operation against a key holding the wrong kind of value';
    END IF;

    -- Return each field
    FOREACH v_field IN ARRAY p_fields
    LOOP
        field := v_field;
        value := v_value -> v_field;
        RETURN NEXT;
    END LOOP;
END;
$$;

-- HGETALL: Get all fields and values from hash
CREATE OR REPLACE FUNCTION pgkv.hgetall(p_key TEXT)
RETURNS TABLE(field TEXT, value JSONB)
LANGUAGE plpgsql
AS $$
DECLARE
    v_value JSONB;
    v_type TEXT;
    v_expires_at TIMESTAMPTZ;
BEGIN
    SELECT store.value, store.type, store.expires_at INTO v_value, v_type, v_expires_at
    FROM pgkv.store
    WHERE key = p_key;

    IF NOT FOUND THEN
        RETURN;
    END IF;

    -- Check if expired
    IF v_expires_at IS NOT NULL AND v_expires_at < NOW() THEN
        DELETE FROM pgkv.store WHERE key = p_key;
        RETURN;
    END IF;

    -- Check type
    IF v_type != 'hash' THEN
        RAISE EXCEPTION 'WRONGTYPE Operation against a key holding the wrong kind of value';
    END IF;

    -- Return all key-value pairs
    RETURN QUERY
    SELECT key::TEXT, value
    FROM jsonb_each(v_value);
END;
$$;

-- HDEL: Delete field(s) from hash
CREATE OR REPLACE FUNCTION pgkv.hdel(p_key TEXT, VARIADIC p_fields TEXT[])
RETURNS INTEGER
LANGUAGE plpgsql
AS $$
DECLARE
    v_value JSONB;
    v_type TEXT;
    v_expires_at TIMESTAMPTZ;
    v_field TEXT;
    v_deleted INTEGER := 0;
BEGIN
    SELECT value, type, expires_at INTO v_value, v_type, v_expires_at
    FROM pgkv.store
    WHERE key = p_key;

    IF NOT FOUND THEN
        RETURN 0;
    END IF;

    -- Check if expired
    IF v_expires_at IS NOT NULL AND v_expires_at < NOW() THEN
        DELETE FROM pgkv.store WHERE key = p_key;
        RETURN 0;
    END IF;

    -- Check type
    IF v_type != 'hash' THEN
        RAISE EXCEPTION 'WRONGTYPE Operation against a key holding the wrong kind of value';
    END IF;

    -- Delete each field
    FOREACH v_field IN ARRAY p_fields
    LOOP
        IF v_value ? v_field THEN
            v_value := v_value - v_field;
            v_deleted := v_deleted + 1;
        END IF;
    END LOOP;

    -- Update or delete key if empty
    IF jsonb_object_keys(v_value) IS NULL OR v_value = '{}'::jsonb THEN
        DELETE FROM pgkv.store WHERE key = p_key;
    ELSE
        UPDATE pgkv.store
        SET value = v_value, updated_at = NOW()
        WHERE key = p_key;
    END IF;

    RETURN v_deleted;
END;
$$;

-- HEXISTS: Check if field exists in hash
CREATE OR REPLACE FUNCTION pgkv.hexists(p_key TEXT, p_field TEXT)
RETURNS INTEGER
LANGUAGE plpgsql
AS $$
DECLARE
    v_value JSONB;
    v_type TEXT;
    v_expires_at TIMESTAMPTZ;
BEGIN
    SELECT value, type, expires_at INTO v_value, v_type, v_expires_at
    FROM pgkv.store
    WHERE key = p_key;

    IF NOT FOUND THEN
        RETURN 0;
    END IF;

    -- Check if expired
    IF v_expires_at IS NOT NULL AND v_expires_at < NOW() THEN
        DELETE FROM pgkv.store WHERE key = p_key;
        RETURN 0;
    END IF;

    -- Check type
    IF v_type != 'hash' THEN
        RAISE EXCEPTION 'WRONGTYPE Operation against a key holding the wrong kind of value';
    END IF;

    RETURN CASE WHEN v_value ? p_field THEN 1 ELSE 0 END;
END;
$$;

-- HLEN: Get number of fields in hash
CREATE OR REPLACE FUNCTION pgkv.hlen(p_key TEXT)
RETURNS INTEGER
LANGUAGE plpgsql
AS $$
DECLARE
    v_value JSONB;
    v_type TEXT;
    v_expires_at TIMESTAMPTZ;
BEGIN
    SELECT value, type, expires_at INTO v_value, v_type, v_expires_at
    FROM pgkv.store
    WHERE key = p_key;

    IF NOT FOUND THEN
        RETURN 0;
    END IF;

    -- Check if expired
    IF v_expires_at IS NOT NULL AND v_expires_at < NOW() THEN
        DELETE FROM pgkv.store WHERE key = p_key;
        RETURN 0;
    END IF;

    -- Check type
    IF v_type != 'hash' THEN
        RAISE EXCEPTION 'WRONGTYPE Operation against a key holding the wrong kind of value';
    END IF;

    RETURN (SELECT COUNT(*) FROM jsonb_object_keys(v_value))::INTEGER;
END;
$$;

-- HKEYS: Get all field names from hash
CREATE OR REPLACE FUNCTION pgkv.hkeys(p_key TEXT)
RETURNS SETOF TEXT
LANGUAGE plpgsql
AS $$
DECLARE
    v_value JSONB;
    v_type TEXT;
    v_expires_at TIMESTAMPTZ;
BEGIN
    SELECT value, type, expires_at INTO v_value, v_type, v_expires_at
    FROM pgkv.store
    WHERE key = p_key;

    IF NOT FOUND THEN
        RETURN;
    END IF;

    -- Check if expired
    IF v_expires_at IS NOT NULL AND v_expires_at < NOW() THEN
        DELETE FROM pgkv.store WHERE key = p_key;
        RETURN;
    END IF;

    -- Check type
    IF v_type != 'hash' THEN
        RAISE EXCEPTION 'WRONGTYPE Operation against a key holding the wrong kind of value';
    END IF;

    RETURN QUERY
    SELECT jsonb_object_keys(v_value);
END;
$$;

-- HVALS: Get all values from hash
CREATE OR REPLACE FUNCTION pgkv.hvals(p_key TEXT)
RETURNS SETOF JSONB
LANGUAGE plpgsql
AS $$
DECLARE
    v_value JSONB;
    v_type TEXT;
    v_expires_at TIMESTAMPTZ;
BEGIN
    SELECT store.value, store.type, store.expires_at INTO v_value, v_type, v_expires_at
    FROM pgkv.store
    WHERE key = p_key;

    IF NOT FOUND THEN
        RETURN;
    END IF;

    -- Check if expired
    IF v_expires_at IS NOT NULL AND v_expires_at < NOW() THEN
        DELETE FROM pgkv.store WHERE key = p_key;
        RETURN;
    END IF;

    -- Check type
    IF v_type != 'hash' THEN
        RAISE EXCEPTION 'WRONGTYPE Operation against a key holding the wrong kind of value';
    END IF;

    RETURN QUERY
    SELECT value FROM jsonb_each(v_value);
END;
$$;

-- HINCRBY: Increment hash field by integer
CREATE OR REPLACE FUNCTION pgkv.hincrby(p_key TEXT, p_field TEXT, p_increment BIGINT)
RETURNS BIGINT
LANGUAGE plpgsql
AS $$
DECLARE
    v_value JSONB;
    v_type TEXT;
    v_expires_at TIMESTAMPTZ;
    v_field_value JSONB;
    v_numeric BIGINT;
BEGIN
    SELECT value, type, expires_at INTO v_value, v_type, v_expires_at
    FROM pgkv.store
    WHERE key = p_key;

    -- Check if expired
    IF v_expires_at IS NOT NULL AND v_expires_at < NOW() THEN
        DELETE FROM pgkv.store WHERE key = p_key;
        v_value := NULL;
        v_type := NULL;
    END IF;

    -- Check type
    IF v_type IS NOT NULL AND v_type != 'hash' THEN
        RAISE EXCEPTION 'WRONGTYPE Operation against a key holding the wrong kind of value';
    END IF;

    -- Initialize if new
    IF v_value IS NULL THEN
        v_value := '{}'::jsonb;
    END IF;

    -- Get field value
    v_field_value := v_value -> p_field;

    IF v_field_value IS NULL THEN
        v_numeric := 0;
    ELSE
        BEGIN
            v_numeric := (v_field_value #>> '{}')::BIGINT;
        EXCEPTION WHEN OTHERS THEN
            RAISE EXCEPTION 'hash value is not an integer or out of range';
        END;
    END IF;

    -- Increment
    v_numeric := v_numeric + p_increment;

    -- Set field
    v_value := jsonb_set(v_value, ARRAY[p_field], to_jsonb(v_numeric), true);

    -- Store
    INSERT INTO pgkv.store (key, value, type, updated_at)
    VALUES (p_key, v_value, 'hash', NOW())
    ON CONFLICT (key) DO UPDATE
    SET value = EXCLUDED.value,
        type = EXCLUDED.type,
        updated_at = NOW();

    RETURN v_numeric;
END;
$$;

-- ============================================================================
-- LIST Operations
-- ============================================================================

-- LPUSH: Push value(s) to head of list
CREATE OR REPLACE FUNCTION pgkv.lpush(p_key TEXT, VARIADIC p_values TEXT[])
RETURNS INTEGER
LANGUAGE plpgsql
AS $$
DECLARE
    v_value JSONB;
    v_type TEXT;
    v_expires_at TIMESTAMPTZ;
    v_val TEXT;
    v_new_array JSONB;
BEGIN
    SELECT value, type, expires_at INTO v_value, v_type, v_expires_at
    FROM pgkv.store
    WHERE key = p_key;

    -- Check if expired
    IF v_expires_at IS NOT NULL AND v_expires_at < NOW() THEN
        DELETE FROM pgkv.store WHERE key = p_key;
        v_value := NULL;
        v_type := NULL;
    END IF;

    -- Check type
    IF v_type IS NOT NULL AND v_type != 'list' THEN
        RAISE EXCEPTION 'WRONGTYPE Operation against a key holding the wrong kind of value';
    END IF;

    -- Initialize if new
    IF v_value IS NULL THEN
        v_value := '[]'::jsonb;
    END IF;

    -- Prepend values in reverse order to maintain Redis behavior
    FOR v_i IN REVERSE array_length(p_values, 1)..1
    LOOP
        v_value := jsonb_insert(v_value, '{0}', to_jsonb(p_values[v_i]));
    END LOOP;

    -- Store
    INSERT INTO pgkv.store (key, value, type, updated_at)
    VALUES (p_key, v_value, 'list', NOW())
    ON CONFLICT (key) DO UPDATE
    SET value = EXCLUDED.value,
        type = EXCLUDED.type,
        updated_at = NOW();

    RETURN jsonb_array_length(v_value);
END;
$$;

-- RPUSH: Push value(s) to tail of list
CREATE OR REPLACE FUNCTION pgkv.rpush(p_key TEXT, VARIADIC p_values TEXT[])
RETURNS INTEGER
LANGUAGE plpgsql
AS $$
DECLARE
    v_value JSONB;
    v_type TEXT;
    v_expires_at TIMESTAMPTZ;
    v_val TEXT;
BEGIN
    SELECT value, type, expires_at INTO v_value, v_type, v_expires_at
    FROM pgkv.store
    WHERE key = p_key;

    -- Check if expired
    IF v_expires_at IS NOT NULL AND v_expires_at < NOW() THEN
        DELETE FROM pgkv.store WHERE key = p_key;
        v_value := NULL;
        v_type := NULL;
    END IF;

    -- Check type
    IF v_type IS NOT NULL AND v_type != 'list' THEN
        RAISE EXCEPTION 'WRONGTYPE Operation against a key holding the wrong kind of value';
    END IF;

    -- Initialize if new
    IF v_value IS NULL THEN
        v_value := '[]'::jsonb;
    END IF;

    -- Append values
    FOREACH v_val IN ARRAY p_values
    LOOP
        v_value := v_value || to_jsonb(v_val);
    END LOOP;

    -- Store
    INSERT INTO pgkv.store (key, value, type, updated_at)
    VALUES (p_key, v_value, 'list', NOW())
    ON CONFLICT (key) DO UPDATE
    SET value = EXCLUDED.value,
        type = EXCLUDED.type,
        updated_at = NOW();

    RETURN jsonb_array_length(v_value);
END;
$$;

-- LPOP: Pop value(s) from head of list
CREATE OR REPLACE FUNCTION pgkv.lpop(p_key TEXT, p_count INTEGER DEFAULT 1)
RETURNS SETOF JSONB
LANGUAGE plpgsql
AS $$
DECLARE
    v_value JSONB;
    v_type TEXT;
    v_expires_at TIMESTAMPTZ;
    v_len INTEGER;
    v_i INTEGER;
    v_new_array JSONB;
BEGIN
    SELECT value, type, expires_at INTO v_value, v_type, v_expires_at
    FROM pgkv.store
    WHERE key = p_key;

    IF NOT FOUND THEN
        RETURN;
    END IF;

    -- Check if expired
    IF v_expires_at IS NOT NULL AND v_expires_at < NOW() THEN
        DELETE FROM pgkv.store WHERE key = p_key;
        RETURN;
    END IF;

    -- Check type
    IF v_type != 'list' THEN
        RAISE EXCEPTION 'WRONGTYPE Operation against a key holding the wrong kind of value';
    END IF;

    v_len := jsonb_array_length(v_value);

    -- Return popped elements
    FOR v_i IN 0..LEAST(p_count, v_len) - 1
    LOOP
        RETURN NEXT v_value -> v_i;
    END LOOP;

    -- Remove popped elements
    IF p_count >= v_len THEN
        DELETE FROM pgkv.store WHERE key = p_key;
    ELSE
        v_new_array := '[]'::jsonb;
        FOR v_i IN p_count..v_len - 1
        LOOP
            v_new_array := v_new_array || (v_value -> v_i);
        END LOOP;

        UPDATE pgkv.store
        SET value = v_new_array, updated_at = NOW()
        WHERE key = p_key;
    END IF;
END;
$$;

-- RPOP: Pop value(s) from tail of list
CREATE OR REPLACE FUNCTION pgkv.rpop(p_key TEXT, p_count INTEGER DEFAULT 1)
RETURNS SETOF JSONB
LANGUAGE plpgsql
AS $$
DECLARE
    v_value JSONB;
    v_type TEXT;
    v_expires_at TIMESTAMPTZ;
    v_len INTEGER;
    v_i INTEGER;
    v_new_array JSONB;
BEGIN
    SELECT value, type, expires_at INTO v_value, v_type, v_expires_at
    FROM pgkv.store
    WHERE key = p_key;

    IF NOT FOUND THEN
        RETURN;
    END IF;

    -- Check if expired
    IF v_expires_at IS NOT NULL AND v_expires_at < NOW() THEN
        DELETE FROM pgkv.store WHERE key = p_key;
        RETURN;
    END IF;

    -- Check type
    IF v_type != 'list' THEN
        RAISE EXCEPTION 'WRONGTYPE Operation against a key holding the wrong kind of value';
    END IF;

    v_len := jsonb_array_length(v_value);

    -- Return popped elements (in reverse order from tail)
    FOR v_i IN REVERSE v_len - 1..GREATEST(v_len - p_count, 0)
    LOOP
        RETURN NEXT v_value -> v_i;
    END LOOP;

    -- Remove popped elements
    IF p_count >= v_len THEN
        DELETE FROM pgkv.store WHERE key = p_key;
    ELSE
        v_new_array := '[]'::jsonb;
        FOR v_i IN 0..v_len - p_count - 1
        LOOP
            v_new_array := v_new_array || (v_value -> v_i);
        END LOOP;

        UPDATE pgkv.store
        SET value = v_new_array, updated_at = NOW()
        WHERE key = p_key;
    END IF;
END;
$$;

-- LLEN: Get list length
CREATE OR REPLACE FUNCTION pgkv.llen(p_key TEXT)
RETURNS INTEGER
LANGUAGE plpgsql
AS $$
DECLARE
    v_value JSONB;
    v_type TEXT;
    v_expires_at TIMESTAMPTZ;
BEGIN
    SELECT value, type, expires_at INTO v_value, v_type, v_expires_at
    FROM pgkv.store
    WHERE key = p_key;

    IF NOT FOUND THEN
        RETURN 0;
    END IF;

    -- Check if expired
    IF v_expires_at IS NOT NULL AND v_expires_at < NOW() THEN
        DELETE FROM pgkv.store WHERE key = p_key;
        RETURN 0;
    END IF;

    -- Check type
    IF v_type != 'list' THEN
        RAISE EXCEPTION 'WRONGTYPE Operation against a key holding the wrong kind of value';
    END IF;

    RETURN jsonb_array_length(v_value);
END;
$$;

-- LRANGE: Get range of elements (supports negative indices)
CREATE OR REPLACE FUNCTION pgkv.lrange(p_key TEXT, p_start INTEGER, p_stop INTEGER)
RETURNS SETOF JSONB
LANGUAGE plpgsql
AS $$
DECLARE
    v_value JSONB;
    v_type TEXT;
    v_expires_at TIMESTAMPTZ;
    v_len INTEGER;
    v_start INTEGER;
    v_stop INTEGER;
    v_i INTEGER;
BEGIN
    SELECT value, type, expires_at INTO v_value, v_type, v_expires_at
    FROM pgkv.store
    WHERE key = p_key;

    IF NOT FOUND THEN
        RETURN;
    END IF;

    -- Check if expired
    IF v_expires_at IS NOT NULL AND v_expires_at < NOW() THEN
        DELETE FROM pgkv.store WHERE key = p_key;
        RETURN;
    END IF;

    -- Check type
    IF v_type != 'list' THEN
        RAISE EXCEPTION 'WRONGTYPE Operation against a key holding the wrong kind of value';
    END IF;

    v_len := jsonb_array_length(v_value);

    -- Handle negative indices
    v_start := CASE WHEN p_start < 0 THEN v_len + p_start ELSE p_start END;
    v_stop := CASE WHEN p_stop < 0 THEN v_len + p_stop ELSE p_stop END;

    -- Clamp to valid range
    v_start := GREATEST(0, v_start);
    v_stop := LEAST(v_len - 1, v_stop);

    -- Return range
    FOR v_i IN v_start..v_stop
    LOOP
        RETURN NEXT v_value -> v_i;
    END LOOP;
END;
$$;

-- LINDEX: Get element by index (supports negative indices)
CREATE OR REPLACE FUNCTION pgkv.lindex(p_key TEXT, p_index INTEGER)
RETURNS JSONB
LANGUAGE plpgsql
AS $$
DECLARE
    v_value JSONB;
    v_type TEXT;
    v_expires_at TIMESTAMPTZ;
    v_len INTEGER;
    v_idx INTEGER;
BEGIN
    SELECT value, type, expires_at INTO v_value, v_type, v_expires_at
    FROM pgkv.store
    WHERE key = p_key;

    IF NOT FOUND THEN
        RETURN NULL;
    END IF;

    -- Check if expired
    IF v_expires_at IS NOT NULL AND v_expires_at < NOW() THEN
        DELETE FROM pgkv.store WHERE key = p_key;
        RETURN NULL;
    END IF;

    -- Check type
    IF v_type != 'list' THEN
        RAISE EXCEPTION 'WRONGTYPE Operation against a key holding the wrong kind of value';
    END IF;

    v_len := jsonb_array_length(v_value);

    -- Handle negative index
    v_idx := CASE WHEN p_index < 0 THEN v_len + p_index ELSE p_index END;

    -- Check bounds
    IF v_idx < 0 OR v_idx >= v_len THEN
        RETURN NULL;
    END IF;

    RETURN v_value -> v_idx;
END;
$$;

-- LSET: Set element at index
CREATE OR REPLACE FUNCTION pgkv.lset(p_key TEXT, p_index INTEGER, p_value TEXT)
RETURNS TEXT
LANGUAGE plpgsql
AS $$
DECLARE
    v_value JSONB;
    v_type TEXT;
    v_expires_at TIMESTAMPTZ;
    v_len INTEGER;
    v_idx INTEGER;
BEGIN
    SELECT value, type, expires_at INTO v_value, v_type, v_expires_at
    FROM pgkv.store
    WHERE key = p_key;

    IF NOT FOUND THEN
        RAISE EXCEPTION 'no such key';
    END IF;

    -- Check if expired
    IF v_expires_at IS NOT NULL AND v_expires_at < NOW() THEN
        DELETE FROM pgkv.store WHERE key = p_key;
        RAISE EXCEPTION 'no such key';
    END IF;

    -- Check type
    IF v_type != 'list' THEN
        RAISE EXCEPTION 'WRONGTYPE Operation against a key holding the wrong kind of value';
    END IF;

    v_len := jsonb_array_length(v_value);

    -- Handle negative index
    v_idx := CASE WHEN p_index < 0 THEN v_len + p_index ELSE p_index END;

    -- Check bounds
    IF v_idx < 0 OR v_idx >= v_len THEN
        RAISE EXCEPTION 'index out of range';
    END IF;

    -- Set value
    v_value := jsonb_set(v_value, ARRAY[v_idx::TEXT], to_jsonb(p_value));

    UPDATE pgkv.store
    SET value = v_value, updated_at = NOW()
    WHERE key = p_key;

    RETURN 'OK';
END;
$$;

-- LTRIM: Trim list to specified range
CREATE OR REPLACE FUNCTION pgkv.ltrim(p_key TEXT, p_start INTEGER, p_stop INTEGER)
RETURNS TEXT
LANGUAGE plpgsql
AS $$
DECLARE
    v_value JSONB;
    v_type TEXT;
    v_expires_at TIMESTAMPTZ;
    v_len INTEGER;
    v_start INTEGER;
    v_stop INTEGER;
    v_i INTEGER;
    v_new_array JSONB;
BEGIN
    SELECT value, type, expires_at INTO v_value, v_type, v_expires_at
    FROM pgkv.store
    WHERE key = p_key;

    IF NOT FOUND THEN
        RETURN 'OK';
    END IF;

    -- Check if expired
    IF v_expires_at IS NOT NULL AND v_expires_at < NOW() THEN
        DELETE FROM pgkv.store WHERE key = p_key;
        RETURN 'OK';
    END IF;

    -- Check type
    IF v_type != 'list' THEN
        RAISE EXCEPTION 'WRONGTYPE Operation against a key holding the wrong kind of value';
    END IF;

    v_len := jsonb_array_length(v_value);

    -- Handle negative indices
    v_start := CASE WHEN p_start < 0 THEN v_len + p_start ELSE p_start END;
    v_stop := CASE WHEN p_stop < 0 THEN v_len + p_stop ELSE p_stop END;

    -- Clamp to valid range
    v_start := GREATEST(0, v_start);
    v_stop := LEAST(v_len - 1, v_stop);

    -- If range is empty, delete key
    IF v_start > v_stop OR v_start >= v_len THEN
        DELETE FROM pgkv.store WHERE key = p_key;
        RETURN 'OK';
    END IF;

    -- Build new array
    v_new_array := '[]'::jsonb;
    FOR v_i IN v_start..v_stop
    LOOP
        v_new_array := v_new_array || (v_value -> v_i);
    END LOOP;

    UPDATE pgkv.store
    SET value = v_new_array, updated_at = NOW()
    WHERE key = p_key;

    RETURN 'OK';
END;
$$;

-- LREM: Remove elements matching value
-- count > 0: Remove first count occurrences
-- count < 0: Remove last count occurrences
-- count = 0: Remove all occurrences
CREATE OR REPLACE FUNCTION pgkv.lrem(p_key TEXT, p_count INTEGER, p_value TEXT)
RETURNS INTEGER
LANGUAGE plpgsql
AS $$
DECLARE
    v_value JSONB;
    v_type TEXT;
    v_expires_at TIMESTAMPTZ;
    v_len INTEGER;
    v_i INTEGER;
    v_removed INTEGER := 0;
    v_new_array JSONB;
    v_elem JSONB;
    v_target JSONB;
BEGIN
    SELECT value, type, expires_at INTO v_value, v_type, v_expires_at
    FROM pgkv.store
    WHERE key = p_key;

    IF NOT FOUND THEN
        RETURN 0;
    END IF;

    -- Check if expired
    IF v_expires_at IS NOT NULL AND v_expires_at < NOW() THEN
        DELETE FROM pgkv.store WHERE key = p_key;
        RETURN 0;
    END IF;

    -- Check type
    IF v_type != 'list' THEN
        RAISE EXCEPTION 'WRONGTYPE Operation against a key holding the wrong kind of value';
    END IF;

    v_len := jsonb_array_length(v_value);
    v_target := to_jsonb(p_value);
    v_new_array := '[]'::jsonb;

    IF p_count >= 0 THEN
        -- Remove from head
        FOR v_i IN 0..v_len - 1
        LOOP
            v_elem := v_value -> v_i;
            IF v_elem = v_target AND (p_count = 0 OR v_removed < p_count) THEN
                v_removed := v_removed + 1;
            ELSE
                v_new_array := v_new_array || v_elem;
            END IF;
        END LOOP;
    ELSE
        -- Remove from tail (build array, then reverse removal count)
        FOR v_i IN REVERSE v_len - 1..0
        LOOP
            v_elem := v_value -> v_i;
            IF v_elem = v_target AND v_removed < ABS(p_count) THEN
                v_removed := v_removed + 1;
            ELSE
                v_new_array := (v_elem) || v_new_array;
            END IF;
        END LOOP;
    END IF;

    -- Update or delete
    IF jsonb_array_length(v_new_array) = 0 THEN
        DELETE FROM pgkv.store WHERE key = p_key;
    ELSE
        UPDATE pgkv.store
        SET value = v_new_array, updated_at = NOW()
        WHERE key = p_key;
    END IF;

    RETURN v_removed;
END;
$$;

-- ============================================================================
-- SET Operations
-- ============================================================================

-- SADD: Add member(s) to set (enforces uniqueness)
CREATE OR REPLACE FUNCTION pgkv.sadd(p_key TEXT, VARIADIC p_members TEXT[])
RETURNS INTEGER
LANGUAGE plpgsql
AS $$
DECLARE
    v_value JSONB;
    v_type TEXT;
    v_expires_at TIMESTAMPTZ;
    v_member TEXT;
    v_added INTEGER := 0;
    v_member_json JSONB;
BEGIN
    SELECT value, type, expires_at INTO v_value, v_type, v_expires_at
    FROM pgkv.store
    WHERE key = p_key;

    -- Check if expired
    IF v_expires_at IS NOT NULL AND v_expires_at < NOW() THEN
        DELETE FROM pgkv.store WHERE key = p_key;
        v_value := NULL;
        v_type := NULL;
    END IF;

    -- Check type
    IF v_type IS NOT NULL AND v_type != 'set' THEN
        RAISE EXCEPTION 'WRONGTYPE Operation against a key holding the wrong kind of value';
    END IF;

    -- Initialize if new
    IF v_value IS NULL THEN
        v_value := '[]'::jsonb;
    END IF;

    -- Add each unique member
    FOREACH v_member IN ARRAY p_members
    LOOP
        v_member_json := to_jsonb(v_member);
        -- Check if member already exists
        IF NOT EXISTS (
            SELECT 1 FROM jsonb_array_elements(v_value) elem
            WHERE elem = v_member_json
        ) THEN
            v_value := v_value || v_member_json;
            v_added := v_added + 1;
        END IF;
    END LOOP;

    -- Store if any members were added
    IF v_added > 0 OR v_type IS NULL THEN
        INSERT INTO pgkv.store (key, value, type, updated_at)
        VALUES (p_key, v_value, 'set', NOW())
        ON CONFLICT (key) DO UPDATE
        SET value = EXCLUDED.value,
            type = EXCLUDED.type,
            updated_at = NOW();
    END IF;

    RETURN v_added;
END;
$$;

-- SREM: Remove member(s) from set
CREATE OR REPLACE FUNCTION pgkv.srem(p_key TEXT, VARIADIC p_members TEXT[])
RETURNS INTEGER
LANGUAGE plpgsql
AS $$
DECLARE
    v_value JSONB;
    v_type TEXT;
    v_expires_at TIMESTAMPTZ;
    v_member TEXT;
    v_removed INTEGER := 0;
    v_member_json JSONB;
    v_new_array JSONB;
    v_elem JSONB;
BEGIN
    SELECT value, type, expires_at INTO v_value, v_type, v_expires_at
    FROM pgkv.store
    WHERE key = p_key;

    IF NOT FOUND THEN
        RETURN 0;
    END IF;

    -- Check if expired
    IF v_expires_at IS NOT NULL AND v_expires_at < NOW() THEN
        DELETE FROM pgkv.store WHERE key = p_key;
        RETURN 0;
    END IF;

    -- Check type
    IF v_type != 'set' THEN
        RAISE EXCEPTION 'WRONGTYPE Operation against a key holding the wrong kind of value';
    END IF;

    v_new_array := '[]'::jsonb;

    -- Build new array without specified members
    FOR v_elem IN SELECT * FROM jsonb_array_elements(v_value)
    LOOP
        FOREACH v_member IN ARRAY p_members
        LOOP
            v_member_json := to_jsonb(v_member);
            IF v_elem = v_member_json THEN
                v_removed := v_removed + 1;
                CONTINUE;
            END IF;
        END LOOP;

        -- Add if not removed
        IF NOT EXISTS (
            SELECT 1 FROM unnest(p_members) m
            WHERE to_jsonb(m) = v_elem
        ) THEN
            v_new_array := v_new_array || v_elem;
        END IF;
    END LOOP;

    -- Update or delete
    IF jsonb_array_length(v_new_array) = 0 THEN
        DELETE FROM pgkv.store WHERE key = p_key;
    ELSE
        UPDATE pgkv.store
        SET value = v_new_array, updated_at = NOW()
        WHERE key = p_key;
    END IF;

    RETURN v_removed;
END;
$$;

-- SMEMBERS: Get all members of set
CREATE OR REPLACE FUNCTION pgkv.smembers(p_key TEXT)
RETURNS SETOF JSONB
LANGUAGE plpgsql
AS $$
DECLARE
    v_value JSONB;
    v_type TEXT;
    v_expires_at TIMESTAMPTZ;
BEGIN
    SELECT value, type, expires_at INTO v_value, v_type, v_expires_at
    FROM pgkv.store
    WHERE key = p_key;

    IF NOT FOUND THEN
        RETURN;
    END IF;

    -- Check if expired
    IF v_expires_at IS NOT NULL AND v_expires_at < NOW() THEN
        DELETE FROM pgkv.store WHERE key = p_key;
        RETURN;
    END IF;

    -- Check type
    IF v_type != 'set' THEN
        RAISE EXCEPTION 'WRONGTYPE Operation against a key holding the wrong kind of value';
    END IF;

    RETURN QUERY
    SELECT * FROM jsonb_array_elements(v_value);
END;
$$;

-- SISMEMBER: Check if member exists in set
CREATE OR REPLACE FUNCTION pgkv.sismember(p_key TEXT, p_member TEXT)
RETURNS INTEGER
LANGUAGE plpgsql
AS $$
DECLARE
    v_value JSONB;
    v_type TEXT;
    v_expires_at TIMESTAMPTZ;
    v_member_json JSONB;
BEGIN
    SELECT value, type, expires_at INTO v_value, v_type, v_expires_at
    FROM pgkv.store
    WHERE key = p_key;

    IF NOT FOUND THEN
        RETURN 0;
    END IF;

    -- Check if expired
    IF v_expires_at IS NOT NULL AND v_expires_at < NOW() THEN
        DELETE FROM pgkv.store WHERE key = p_key;
        RETURN 0;
    END IF;

    -- Check type
    IF v_type != 'set' THEN
        RAISE EXCEPTION 'WRONGTYPE Operation against a key holding the wrong kind of value';
    END IF;

    v_member_json := to_jsonb(p_member);

    RETURN CASE WHEN EXISTS (
        SELECT 1 FROM jsonb_array_elements(v_value) elem
        WHERE elem = v_member_json
    ) THEN 1 ELSE 0 END;
END;
$$;

-- SCARD: Get set cardinality (size)
CREATE OR REPLACE FUNCTION pgkv.scard(p_key TEXT)
RETURNS INTEGER
LANGUAGE plpgsql
AS $$
DECLARE
    v_value JSONB;
    v_type TEXT;
    v_expires_at TIMESTAMPTZ;
BEGIN
    SELECT value, type, expires_at INTO v_value, v_type, v_expires_at
    FROM pgkv.store
    WHERE key = p_key;

    IF NOT FOUND THEN
        RETURN 0;
    END IF;

    -- Check if expired
    IF v_expires_at IS NOT NULL AND v_expires_at < NOW() THEN
        DELETE FROM pgkv.store WHERE key = p_key;
        RETURN 0;
    END IF;

    -- Check type
    IF v_type != 'set' THEN
        RAISE EXCEPTION 'WRONGTYPE Operation against a key holding the wrong kind of value';
    END IF;

    RETURN jsonb_array_length(v_value);
END;
$$;

-- SINTER: Set intersection
CREATE OR REPLACE FUNCTION pgkv.sinter(VARIADIC p_keys TEXT[])
RETURNS SETOF JSONB
LANGUAGE plpgsql
AS $$
DECLARE
    v_first_key TEXT;
    v_key TEXT;
    v_result JSONB;
    v_value JSONB;
    v_type TEXT;
    v_expires_at TIMESTAMPTZ;
    v_elem JSONB;
BEGIN
    IF array_length(p_keys, 1) = 0 THEN
        RETURN;
    END IF;

    v_first_key := p_keys[1];

    -- Get first set
    SELECT value, type, expires_at INTO v_result, v_type, v_expires_at
    FROM pgkv.store
    WHERE key = v_first_key;

    IF NOT FOUND THEN
        RETURN;
    END IF;

    -- Check if expired
    IF v_expires_at IS NOT NULL AND v_expires_at < NOW() THEN
        DELETE FROM pgkv.store WHERE key = v_first_key;
        RETURN;
    END IF;

    -- Check type
    IF v_type != 'set' THEN
        RAISE EXCEPTION 'WRONGTYPE Operation against a key holding the wrong kind of value';
    END IF;

    -- Intersect with remaining sets
    FOR v_i IN 2..array_length(p_keys, 1)
    LOOP
        v_key := p_keys[v_i];

        SELECT value, type, expires_at INTO v_value, v_type, v_expires_at
        FROM pgkv.store
        WHERE key = v_key;

        IF NOT FOUND THEN
            RETURN;
        END IF;

        IF v_expires_at IS NOT NULL AND v_expires_at < NOW() THEN
            DELETE FROM pgkv.store WHERE key = v_key;
            RETURN;
        END IF;

        IF v_type != 'set' THEN
            RAISE EXCEPTION 'WRONGTYPE Operation against a key holding the wrong kind of value';
        END IF;

        -- Keep only elements present in both sets
        v_result := (
            SELECT jsonb_agg(elem)
            FROM jsonb_array_elements(v_result) elem
            WHERE elem IN (SELECT jsonb_array_elements(v_value))
        );

        IF v_result IS NULL OR jsonb_array_length(v_result) = 0 THEN
            RETURN;
        END IF;
    END LOOP;

    -- Return intersection
    RETURN QUERY
    SELECT * FROM jsonb_array_elements(v_result);
END;
$$;

-- SUNION: Set union
CREATE OR REPLACE FUNCTION pgkv.sunion(VARIADIC p_keys TEXT[])
RETURNS SETOF JSONB
LANGUAGE plpgsql
AS $$
DECLARE
    v_key TEXT;
    v_result JSONB := '[]'::jsonb;
    v_value JSONB;
    v_type TEXT;
    v_expires_at TIMESTAMPTZ;
    v_elem JSONB;
BEGIN
    -- Union all sets
    FOREACH v_key IN ARRAY p_keys
    LOOP
        SELECT value, type, expires_at INTO v_value, v_type, v_expires_at
        FROM pgkv.store
        WHERE key = v_key;

        IF NOT FOUND THEN
            CONTINUE;
        END IF;

        IF v_expires_at IS NOT NULL AND v_expires_at < NOW() THEN
            DELETE FROM pgkv.store WHERE key = v_key;
            CONTINUE;
        END IF;

        IF v_type != 'set' THEN
            RAISE EXCEPTION 'WRONGTYPE Operation against a key holding the wrong kind of value';
        END IF;

        -- Add all elements from this set (maintaining uniqueness)
        FOR v_elem IN SELECT * FROM jsonb_array_elements(v_value)
        LOOP
            IF NOT EXISTS (
                SELECT 1 FROM jsonb_array_elements(v_result) r
                WHERE r = v_elem
            ) THEN
                v_result := v_result || v_elem;
            END IF;
        END LOOP;
    END LOOP;

    -- Return union
    RETURN QUERY
    SELECT * FROM jsonb_array_elements(v_result);
END;
$$;

-- SDIFF: Set difference (first set minus all others)
CREATE OR REPLACE FUNCTION pgkv.sdiff(VARIADIC p_keys TEXT[])
RETURNS SETOF JSONB
LANGUAGE plpgsql
AS $$
DECLARE
    v_first_key TEXT;
    v_key TEXT;
    v_result JSONB;
    v_value JSONB;
    v_type TEXT;
    v_expires_at TIMESTAMPTZ;
    v_elem JSONB;
    v_remove JSONB := '[]'::jsonb;
BEGIN
    IF array_length(p_keys, 1) = 0 THEN
        RETURN;
    END IF;

    v_first_key := p_keys[1];

    -- Get first set
    SELECT value, type, expires_at INTO v_result, v_type, v_expires_at
    FROM pgkv.store
    WHERE key = v_first_key;

    IF NOT FOUND THEN
        RETURN;
    END IF;

    -- Check if expired
    IF v_expires_at IS NOT NULL AND v_expires_at < NOW() THEN
        DELETE FROM pgkv.store WHERE key = v_first_key;
        RETURN;
    END IF;

    -- Check type
    IF v_type != 'set' THEN
        RAISE EXCEPTION 'WRONGTYPE Operation against a key holding the wrong kind of value';
    END IF;

    -- Collect all elements to remove from other sets
    FOR v_i IN 2..array_length(p_keys, 1)
    LOOP
        v_key := p_keys[v_i];

        SELECT value, type, expires_at INTO v_value, v_type, v_expires_at
        FROM pgkv.store
        WHERE key = v_key;

        IF NOT FOUND THEN
            CONTINUE;
        END IF;

        IF v_expires_at IS NOT NULL AND v_expires_at < NOW() THEN
            DELETE FROM pgkv.store WHERE key = v_key;
            CONTINUE;
        END IF;

        IF v_type != 'set' THEN
            RAISE EXCEPTION 'WRONGTYPE Operation against a key holding the wrong kind of value';
        END IF;

        -- Add elements to removal list
        FOR v_elem IN SELECT * FROM jsonb_array_elements(v_value)
        LOOP
            IF NOT EXISTS (
                SELECT 1 FROM jsonb_array_elements(v_remove) r
                WHERE r = v_elem
            ) THEN
                v_remove := v_remove || v_elem;
            END IF;
        END LOOP;
    END LOOP;

    -- Return difference
    RETURN QUERY
    SELECT elem
    FROM jsonb_array_elements(v_result) elem
    WHERE NOT EXISTS (
        SELECT 1 FROM jsonb_array_elements(v_remove) r
        WHERE r = elem
    );
END;
$$;

-- ============================================================================
-- SORTED SET Operations
-- ============================================================================
-- Storage: JSONB object {"member1": score1, "member2": score2, ...}
-- Members are object keys, scores are numeric values

-- ZADD: Add one or more members to a sorted set, or update its score if it already exists
-- Returns the number of elements added (not including updated scores)
CREATE OR REPLACE FUNCTION pgkv.zadd(p_key TEXT, VARIADIC p_pairs TEXT[])
RETURNS INTEGER
LANGUAGE plpgsql
AS $$
DECLARE
    v_type TEXT;
    v_value JSONB;
    v_expires_at TIMESTAMPTZ;
    v_i INTEGER;
    v_added INTEGER := 0;
    v_score NUMERIC;
    v_member TEXT;
BEGIN
    -- Validate pairs (score, member, score, member, ...)
    IF array_length(p_pairs, 1) % 2 != 0 THEN
        RAISE EXCEPTION 'wrong number of arguments for ZADD';
    END IF;

    -- Get existing value and type
    SELECT value, type, expires_at INTO v_value, v_type, v_expires_at
    FROM pgkv.store
    WHERE key = p_key;

    -- Check if expired
    IF v_expires_at IS NOT NULL AND v_expires_at < NOW() THEN
        DELETE FROM pgkv.store WHERE key = p_key;
        v_value := NULL;
        v_type := NULL;
    END IF;

    -- Check type
    IF v_type IS NOT NULL AND v_type != 'zset' THEN
        RAISE EXCEPTION 'WRONGTYPE Operation against a key holding the wrong kind of value';
    END IF;

    -- Initialize if new
    IF v_value IS NULL THEN
        v_value := '{}'::jsonb;
    END IF;

    -- Add each score-member pair
    FOR v_i IN 1..array_length(p_pairs, 1) BY 2
    LOOP
        v_score := p_pairs[v_i]::numeric;
        v_member := p_pairs[v_i + 1];

        -- Count as added if member didn't exist
        IF NOT (v_value ? v_member) THEN
            v_added := v_added + 1;
        END IF;

        -- Set member with score
        v_value := jsonb_set(v_value, ARRAY[v_member], to_jsonb(v_score), true);
    END LOOP;

    -- Store
    INSERT INTO pgkv.store (key, value, type, updated_at)
    VALUES (p_key, v_value, 'zset', NOW())
    ON CONFLICT (key) DO UPDATE
    SET value = EXCLUDED.value,
        type = EXCLUDED.type,
        updated_at = NOW();

    RETURN v_added;
END;
$$;

-- ZREM: Remove one or more members from a sorted set
-- Returns the number of members removed
CREATE OR REPLACE FUNCTION pgkv.zrem(p_key TEXT, VARIADIC p_members TEXT[])
RETURNS INTEGER
LANGUAGE plpgsql
AS $$
DECLARE
    v_type TEXT;
    v_value JSONB;
    v_expires_at TIMESTAMPTZ;
    v_member TEXT;
    v_removed INTEGER := 0;
BEGIN
    -- Get existing value and type
    SELECT value, type, expires_at INTO v_value, v_type, v_expires_at
    FROM pgkv.store
    WHERE key = p_key;

    -- Key doesn't exist or expired
    IF v_value IS NULL OR (v_expires_at IS NOT NULL AND v_expires_at < NOW()) THEN
        IF v_expires_at IS NOT NULL AND v_expires_at < NOW() THEN
            DELETE FROM pgkv.store WHERE key = p_key;
        END IF;
        RETURN 0;
    END IF;

    -- Check type
    IF v_type != 'zset' THEN
        RAISE EXCEPTION 'WRONGTYPE Operation against a key holding the wrong kind of value';
    END IF;

    -- Remove each member
    FOREACH v_member IN ARRAY p_members
    LOOP
        IF v_value ? v_member THEN
            v_value := v_value - v_member;
            v_removed := v_removed + 1;
        END IF;
    END LOOP;

    -- Update or delete if empty
    IF v_value = '{}'::jsonb THEN
        DELETE FROM pgkv.store WHERE key = p_key;
    ELSE
        UPDATE pgkv.store
        SET value = v_value, updated_at = NOW()
        WHERE key = p_key;
    END IF;

    RETURN v_removed;
END;
$$;

-- ZRANGE: Return a range of members in a sorted set, by index (sorted by score ascending)
-- Supports optional WITHSCORES flag
CREATE OR REPLACE FUNCTION pgkv.zrange(p_key TEXT, p_start INTEGER, p_stop INTEGER, p_withscores BOOLEAN DEFAULT FALSE)
RETURNS TABLE(member TEXT, score NUMERIC)
LANGUAGE plpgsql
AS $$
DECLARE
    v_type TEXT;
    v_value JSONB;
    v_expires_at TIMESTAMPTZ;
    v_count INTEGER;
    v_actual_start INTEGER;
    v_actual_stop INTEGER;
BEGIN
    -- Get existing value and type
    SELECT pgkv.store.value, pgkv.store.type, pgkv.store.expires_at
    INTO v_value, v_type, v_expires_at
    FROM pgkv.store
    WHERE key = p_key;

    -- Key doesn't exist or expired
    IF v_value IS NULL OR (v_expires_at IS NOT NULL AND v_expires_at < NOW()) THEN
        IF v_expires_at IS NOT NULL AND v_expires_at < NOW() THEN
            DELETE FROM pgkv.store WHERE key = p_key;
        END IF;
        RETURN;
    END IF;

    -- Check type
    IF v_type != 'zset' THEN
        RAISE EXCEPTION 'WRONGTYPE Operation against a key holding the wrong kind of value';
    END IF;

    -- Get count
    SELECT COUNT(*) INTO v_count FROM jsonb_each(v_value);

    -- Handle negative indices
    v_actual_start := CASE WHEN p_start < 0 THEN v_count + p_start ELSE p_start END;
    v_actual_stop := CASE WHEN p_stop < 0 THEN v_count + p_stop ELSE p_stop END;

    -- Clamp to valid range
    v_actual_start := GREATEST(0, v_actual_start);
    v_actual_stop := LEAST(v_count - 1, v_actual_stop);

    -- Return empty if invalid range
    IF v_actual_start > v_actual_stop THEN
        RETURN;
    END IF;

    -- Return sorted members with optional scores
    RETURN QUERY
    WITH sorted_members AS (
        SELECT
            kv.key::text AS m,
            (kv.value::text)::numeric AS s,
            ROW_NUMBER() OVER (ORDER BY (kv.value::text)::numeric ASC, kv.key::text ASC) - 1 AS rank
        FROM jsonb_each(v_value) kv
    )
    SELECT
        m,
        CASE WHEN p_withscores THEN s ELSE NULL END
    FROM sorted_members
    WHERE rank BETWEEN v_actual_start AND v_actual_stop
    ORDER BY rank;
END;
$$;

-- ZREVRANGE: Return a range of members in a sorted set, by index (sorted by score descending)
-- Supports optional WITHSCORES flag
CREATE OR REPLACE FUNCTION pgkv.zrevrange(p_key TEXT, p_start INTEGER, p_stop INTEGER, p_withscores BOOLEAN DEFAULT FALSE)
RETURNS TABLE(member TEXT, score NUMERIC)
LANGUAGE plpgsql
AS $$
DECLARE
    v_type TEXT;
    v_value JSONB;
    v_expires_at TIMESTAMPTZ;
    v_count INTEGER;
    v_actual_start INTEGER;
    v_actual_stop INTEGER;
BEGIN
    -- Get existing value and type
    SELECT pgkv.store.value, pgkv.store.type, pgkv.store.expires_at
    INTO v_value, v_type, v_expires_at
    FROM pgkv.store
    WHERE key = p_key;

    -- Key doesn't exist or expired
    IF v_value IS NULL OR (v_expires_at IS NOT NULL AND v_expires_at < NOW()) THEN
        IF v_expires_at IS NOT NULL AND v_expires_at < NOW() THEN
            DELETE FROM pgkv.store WHERE key = p_key;
        END IF;
        RETURN;
    END IF;

    -- Check type
    IF v_type != 'zset' THEN
        RAISE EXCEPTION 'WRONGTYPE Operation against a key holding the wrong kind of value';
    END IF;

    -- Get count
    SELECT COUNT(*) INTO v_count FROM jsonb_each(v_value);

    -- Handle negative indices
    v_actual_start := CASE WHEN p_start < 0 THEN v_count + p_start ELSE p_start END;
    v_actual_stop := CASE WHEN p_stop < 0 THEN v_count + p_stop ELSE p_stop END;

    -- Clamp to valid range
    v_actual_start := GREATEST(0, v_actual_start);
    v_actual_stop := LEAST(v_count - 1, v_actual_stop);

    -- Return empty if invalid range
    IF v_actual_start > v_actual_stop THEN
        RETURN;
    END IF;

    -- Return reverse sorted members with optional scores
    RETURN QUERY
    WITH sorted_members AS (
        SELECT
            kv.key::text AS m,
            (kv.value::text)::numeric AS s,
            ROW_NUMBER() OVER (ORDER BY (kv.value::text)::numeric DESC, kv.key::text DESC) - 1 AS rank
        FROM jsonb_each(v_value) kv
    )
    SELECT
        m,
        CASE WHEN p_withscores THEN s ELSE NULL END
    FROM sorted_members
    WHERE rank BETWEEN v_actual_start AND v_actual_stop
    ORDER BY rank;
END;
$$;

-- ZSCORE: Get the score associated with the given member in a sorted set
CREATE OR REPLACE FUNCTION pgkv.zscore(p_key TEXT, p_member TEXT)
RETURNS NUMERIC
LANGUAGE plpgsql
AS $$
DECLARE
    v_type TEXT;
    v_value JSONB;
    v_expires_at TIMESTAMPTZ;
    v_score JSONB;
BEGIN
    -- Get existing value and type
    SELECT value, type, expires_at INTO v_value, v_type, v_expires_at
    FROM pgkv.store
    WHERE key = p_key;

    -- Key doesn't exist or expired
    IF v_value IS NULL OR (v_expires_at IS NOT NULL AND v_expires_at < NOW()) THEN
        IF v_expires_at IS NOT NULL AND v_expires_at < NOW() THEN
            DELETE FROM pgkv.store WHERE key = p_key;
        END IF;
        RETURN NULL;
    END IF;

    -- Check type
    IF v_type != 'zset' THEN
        RAISE EXCEPTION 'WRONGTYPE Operation against a key holding the wrong kind of value';
    END IF;

    -- Get score
    v_score := v_value -> p_member;

    IF v_score IS NULL THEN
        RETURN NULL;
    END IF;

    RETURN (v_score::text)::numeric;
END;
$$;

-- ZCARD: Get the number of members in a sorted set
CREATE OR REPLACE FUNCTION pgkv.zcard(p_key TEXT)
RETURNS INTEGER
LANGUAGE plpgsql
AS $$
DECLARE
    v_type TEXT;
    v_value JSONB;
    v_expires_at TIMESTAMPTZ;
BEGIN
    -- Get existing value and type
    SELECT value, type, expires_at INTO v_value, v_type, v_expires_at
    FROM pgkv.store
    WHERE key = p_key;

    -- Key doesn't exist or expired
    IF v_value IS NULL OR (v_expires_at IS NOT NULL AND v_expires_at < NOW()) THEN
        IF v_expires_at IS NOT NULL AND v_expires_at < NOW() THEN
            DELETE FROM pgkv.store WHERE key = p_key;
        END IF;
        RETURN 0;
    END IF;

    -- Check type
    IF v_type != 'zset' THEN
        RAISE EXCEPTION 'WRONGTYPE Operation against a key holding the wrong kind of value';
    END IF;

    -- Return count
    RETURN (SELECT COUNT(*) FROM jsonb_each(v_value))::integer;
END;
$$;

-- ZRANK: Determine the index of a member in a sorted set (0-based, ascending order)
CREATE OR REPLACE FUNCTION pgkv.zrank(p_key TEXT, p_member TEXT)
RETURNS INTEGER
LANGUAGE plpgsql
AS $$
DECLARE
    v_type TEXT;
    v_value JSONB;
    v_expires_at TIMESTAMPTZ;
    v_rank INTEGER;
BEGIN
    -- Get existing value and type
    SELECT value, type, expires_at INTO v_value, v_type, v_expires_at
    FROM pgkv.store
    WHERE key = p_key;

    -- Key doesn't exist or expired
    IF v_value IS NULL OR (v_expires_at IS NOT NULL AND v_expires_at < NOW()) THEN
        IF v_expires_at IS NOT NULL AND v_expires_at < NOW() THEN
            DELETE FROM pgkv.store WHERE key = p_key;
        END IF;
        RETURN NULL;
    END IF;

    -- Check type
    IF v_type != 'zset' THEN
        RAISE EXCEPTION 'WRONGTYPE Operation against a key holding the wrong kind of value';
    END IF;

    -- Check if member exists
    IF NOT (v_value ? p_member) THEN
        RETURN NULL;
    END IF;

    -- Calculate rank
    SELECT (ROW_NUMBER() OVER (ORDER BY (kv.value::text)::numeric ASC, kv.key::text ASC) - 1)::integer
    INTO v_rank
    FROM jsonb_each(v_value) kv
    WHERE kv.key = p_member;

    RETURN v_rank;
END;
$$;

-- ZREVRANK: Determine the index of a member in a sorted set (0-based, descending order)
CREATE OR REPLACE FUNCTION pgkv.zrevrank(p_key TEXT, p_member TEXT)
RETURNS INTEGER
LANGUAGE plpgsql
AS $$
DECLARE
    v_type TEXT;
    v_value JSONB;
    v_expires_at TIMESTAMPTZ;
    v_rank INTEGER;
BEGIN
    -- Get existing value and type
    SELECT value, type, expires_at INTO v_value, v_type, v_expires_at
    FROM pgkv.store
    WHERE key = p_key;

    -- Key doesn't exist or expired
    IF v_value IS NULL OR (v_expires_at IS NOT NULL AND v_expires_at < NOW()) THEN
        IF v_expires_at IS NOT NULL AND v_expires_at < NOW() THEN
            DELETE FROM pgkv.store WHERE key = p_key;
        END IF;
        RETURN NULL;
    END IF;

    -- Check type
    IF v_type != 'zset' THEN
        RAISE EXCEPTION 'WRONGTYPE Operation against a key holding the wrong kind of value';
    END IF;

    -- Check if member exists
    IF NOT (v_value ? p_member) THEN
        RETURN NULL;
    END IF;

    -- Calculate reverse rank
    SELECT (ROW_NUMBER() OVER (ORDER BY (kv.value::text)::numeric DESC, kv.key::text DESC) - 1)::integer
    INTO v_rank
    FROM jsonb_each(v_value) kv
    WHERE kv.key = p_member;

    RETURN v_rank;
END;
$$;

-- ZRANGEBYSCORE: Return a range of members in a sorted set, by score
-- Supports optional WITHSCORES flag
CREATE OR REPLACE FUNCTION pgkv.zrangebyscore(p_key TEXT, p_min NUMERIC, p_max NUMERIC, p_withscores BOOLEAN DEFAULT FALSE)
RETURNS TABLE(member TEXT, score NUMERIC)
LANGUAGE plpgsql
AS $$
DECLARE
    v_type TEXT;
    v_value JSONB;
    v_expires_at TIMESTAMPTZ;
BEGIN
    -- Get existing value and type
    SELECT pgkv.store.value, pgkv.store.type, pgkv.store.expires_at
    INTO v_value, v_type, v_expires_at
    FROM pgkv.store
    WHERE key = p_key;

    -- Key doesn't exist or expired
    IF v_value IS NULL OR (v_expires_at IS NOT NULL AND v_expires_at < NOW()) THEN
        IF v_expires_at IS NOT NULL AND v_expires_at < NOW() THEN
            DELETE FROM pgkv.store WHERE key = p_key;
        END IF;
        RETURN;
    END IF;

    -- Check type
    IF v_type != 'zset' THEN
        RAISE EXCEPTION 'WRONGTYPE Operation against a key holding the wrong kind of value';
    END IF;

    -- Return members in score range
    RETURN QUERY
    SELECT
        kv.key::text,
        CASE WHEN p_withscores THEN (kv.value::text)::numeric ELSE NULL END
    FROM jsonb_each(v_value) kv
    WHERE (kv.value::text)::numeric BETWEEN p_min AND p_max
    ORDER BY (kv.value::text)::numeric ASC, kv.key::text ASC;
END;
$$;

-- ZINCRBY: Increment the score of a member in a sorted set
CREATE OR REPLACE FUNCTION pgkv.zincrby(p_key TEXT, p_increment NUMERIC, p_member TEXT)
RETURNS NUMERIC
LANGUAGE plpgsql
AS $$
DECLARE
    v_type TEXT;
    v_value JSONB;
    v_expires_at TIMESTAMPTZ;
    v_current_score NUMERIC;
    v_new_score NUMERIC;
BEGIN
    -- Get existing value and type
    SELECT value, type, expires_at INTO v_value, v_type, v_expires_at
    FROM pgkv.store
    WHERE key = p_key;

    -- Check if expired
    IF v_expires_at IS NOT NULL AND v_expires_at < NOW() THEN
        DELETE FROM pgkv.store WHERE key = p_key;
        v_value := NULL;
        v_type := NULL;
    END IF;

    -- Check type
    IF v_type IS NOT NULL AND v_type != 'zset' THEN
        RAISE EXCEPTION 'WRONGTYPE Operation against a key holding the wrong kind of value';
    END IF;

    -- Initialize if new
    IF v_value IS NULL THEN
        v_value := '{}'::jsonb;
        v_current_score := 0;
    ELSE
        -- Get current score or 0 if member doesn't exist
        IF v_value ? p_member THEN
            v_current_score := ((v_value -> p_member)::text)::numeric;
        ELSE
            v_current_score := 0;
        END IF;
    END IF;

    -- Calculate new score
    v_new_score := v_current_score + p_increment;

    -- Update score
    v_value := jsonb_set(v_value, ARRAY[p_member], to_jsonb(v_new_score), true);

    -- Store
    INSERT INTO pgkv.store (key, value, type, updated_at)
    VALUES (p_key, v_value, 'zset', NOW())
    ON CONFLICT (key) DO UPDATE
    SET value = EXCLUDED.value,
        type = EXCLUDED.type,
        updated_at = NOW();

    RETURN v_new_score;
END;
$$;

-- ZCOUNT: Count the members in a sorted set with scores within the given range
CREATE OR REPLACE FUNCTION pgkv.zcount(p_key TEXT, p_min NUMERIC, p_max NUMERIC)
RETURNS INTEGER
LANGUAGE plpgsql
AS $$
DECLARE
    v_type TEXT;
    v_value JSONB;
    v_expires_at TIMESTAMPTZ;
    v_count INTEGER;
BEGIN
    -- Get existing value and type
    SELECT value, type, expires_at INTO v_value, v_type, v_expires_at
    FROM pgkv.store
    WHERE key = p_key;

    -- Key doesn't exist or expired
    IF v_value IS NULL OR (v_expires_at IS NOT NULL AND v_expires_at < NOW()) THEN
        IF v_expires_at IS NOT NULL AND v_expires_at < NOW() THEN
            DELETE FROM pgkv.store WHERE key = p_key;
        END IF;
        RETURN 0;
    END IF;

    -- Check type
    IF v_type != 'zset' THEN
        RAISE EXCEPTION 'WRONGTYPE Operation against a key holding the wrong kind of value';
    END IF;

    -- Count members in range
    SELECT COUNT(*)::integer INTO v_count
    FROM jsonb_each(v_value) kv
    WHERE (kv.value::text)::numeric BETWEEN p_min AND p_max;

    RETURN v_count;
END;
$$;

-- ============================================================================
-- Maintenance Operations
-- ============================================================================

-- CLEANUP_EXPIRED: Remove all expired keys (returns count of deleted keys)
CREATE OR REPLACE FUNCTION pgkv.cleanup_expired()
RETURNS INTEGER
LANGUAGE plpgsql
AS $$
DECLARE
    v_deleted INTEGER;
BEGIN
    DELETE FROM pgkv.store
    WHERE expires_at IS NOT NULL AND expires_at < NOW();

    GET DIAGNOSTICS v_deleted = ROW_COUNT;
    RETURN v_deleted;
END;
$$;

-- FLUSHALL: Delete all keys in the store
CREATE OR REPLACE FUNCTION pgkv.flushall()
RETURNS TEXT
LANGUAGE plpgsql
AS $$
BEGIN
    TRUNCATE TABLE pgkv.store;
    RETURN 'OK';
END;
$$;

-- DBSIZE: Return the number of keys in the store
CREATE OR REPLACE FUNCTION pgkv.dbsize()
RETURNS BIGINT
LANGUAGE plpgsql
AS $$
DECLARE
    v_count BIGINT;
BEGIN
    -- Clean up expired keys first
    PERFORM pgkv.cleanup_expired();

    SELECT COUNT(*) INTO v_count FROM pgkv.store;
    RETURN v_count;
END;
$$;

-- ============================================================================
-- Security: Set appropriate permissions
-- ============================================================================

-- Revoke all permissions from public on the schema
REVOKE ALL ON SCHEMA pgkv FROM PUBLIC;

-- Grant usage on schema to public
GRANT USAGE ON SCHEMA pgkv TO PUBLIC;

-- Grant execute on all functions to public
GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA pgkv TO PUBLIC;

-- Revoke direct table access from public (must use functions)
REVOKE ALL ON pgkv.store FROM PUBLIC;
