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
    CONSTRAINT valid_type CHECK (type IN ('string', 'list', 'set', 'hash', 'zset', 'json'))
);

-- Create index for TTL cleanup and expiration checks
CREATE INDEX IF NOT EXISTS idx_pgkv_expires_at ON pgkv.store(expires_at)
    WHERE expires_at IS NOT NULL;

-- Create index on type for future queries
CREATE INDEX IF NOT EXISTS idx_pgkv_type ON pgkv.store(type);

-- Index for prefix matches (LIKE 'prefix%') and byte-ordered key scans, whatever the
-- database collation. Used by pgkv.scan and pgkv.scan_prefix.
CREATE INDEX IF NOT EXISTS idx_pgkv_key_pattern ON pgkv.store (key text_pattern_ops);

-- ============================================================================
-- Helper Functions
-- ============================================================================

-- REDIS_TO_PG_PATTERN: Convert Redis glob pattern to PostgreSQL pattern
-- Returns NULL if pattern contains character classes (requires regex)
CREATE OR REPLACE FUNCTION pgkv.redis_to_pg_pattern(p_redis_pattern TEXT)
RETURNS TEXT
LANGUAGE plpgsql
IMMUTABLE
SECURITY DEFINER
SET search_path = pgkv, pg_temp
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

-- _LOCK_KEY (internal): lock a key's row for the rest of the transaction and return its live contents.
-- o_type IS NULL means the key is absent (an expired key counts as absent and is removed).
-- With p_create, an absent key gets a placeholder row (already expired, so no reader sees it as a
-- value) that this transaction holds; the caller must overwrite it with pgkv._put or delete it.
-- This is what makes read-modify-write safe under READ COMMITTED: a second writer of the same key
-- waits here, then reads what the first one committed.
CREATE OR REPLACE FUNCTION pgkv._lock_key(
    p_key TEXT,
    p_create BOOLEAN,
    OUT o_value JSONB,
    OUT o_type TEXT,
    OUT o_expires_at TIMESTAMPTZ
)
LANGUAGE plpgsql
SET search_path = pgkv, pg_temp
AS $$
DECLARE
    v_attempt INTEGER;
BEGIN
    FOR v_attempt IN 1..100 LOOP
        SELECT s.value, s.type, s.expires_at INTO o_value, o_type, o_expires_at
        FROM pgkv.store s
        WHERE s.key = p_key
        FOR UPDATE;

        IF FOUND THEN
            IF o_expires_at IS NULL OR o_expires_at >= clock_timestamp() THEN
                RETURN;
            END IF;
            -- Expired: the row is locked, so nobody can have replaced it. Remove it.
            DELETE FROM pgkv.store s WHERE s.key = p_key;
            o_value := NULL;
            o_type := NULL;
            o_expires_at := NULL;
        END IF;

        IF NOT p_create THEN
            RETURN;
        END IF;

        INSERT INTO pgkv.store (key, value, type, expires_at)
        VALUES (p_key, NULL, 'string', '-infinity')
        ON CONFLICT (key) DO NOTHING;
        IF FOUND THEN
            RETURN;
        END IF;

        -- Another session created the key in the meantime. At READ COMMITTED the next pass sees it;
        -- a snapshot isolation level never will, so report the conflict for the caller to retry.
        IF current_setting('transaction_isolation') <> 'read committed' THEN
            RAISE EXCEPTION 'could not serialize access to key due to concurrent update'
                USING ERRCODE = 'serialization_failure';
        END IF;
    END LOOP;

    RAISE EXCEPTION 'could not lock key after repeated concurrent changes'
        USING ERRCODE = 'serialization_failure';
END;
$$;

-- _PUT (internal): write a key whose row the transaction already holds through pgkv._lock_key.
CREATE OR REPLACE FUNCTION pgkv._put(p_key TEXT, p_value JSONB, p_type TEXT, p_expires_at TIMESTAMPTZ)
RETURNS VOID
LANGUAGE plpgsql
SET search_path = pgkv, pg_temp
AS $$
BEGIN
    UPDATE pgkv.store s
    SET value = p_value,
        type = p_type,
        expires_at = p_expires_at,
        updated_at = NOW()
    WHERE s.key = p_key;
END;
$$;

-- _LOCK_STORE_KEYS (internal): for commands that read source keys and overwrite a destination.
-- Locks the destination (creating a placeholder if absent) and share-locks the sources, in key
-- order so that two such commands cannot deadlock each other.
CREATE OR REPLACE FUNCTION pgkv._lock_store_keys(p_destination TEXT, p_sources TEXT[])
RETURNS VOID
LANGUAGE plpgsql
SET search_path = pgkv, pg_temp
AS $$
DECLARE
    v_key TEXT;
BEGIN
    FOR v_key IN
        SELECT k FROM unnest(p_sources || p_destination) AS u(k) WHERE k IS NOT NULL GROUP BY k ORDER BY k COLLATE "C"
    LOOP
        IF v_key = p_destination THEN
            PERFORM pgkv._lock_key(v_key, TRUE);
        ELSE
            PERFORM 1 FROM pgkv.store s WHERE s.key = v_key FOR SHARE;
        END IF;
    END LOOP;
END;
$$;

-- _EXPIRES_AT (internal): the expiry time for SET-style options. NULL means no expiry.
CREATE OR REPLACE FUNCTION pgkv._expires_at(p_ex INTEGER, p_px BIGINT, p_exat BIGINT, p_pxat BIGINT, p_keepttl BOOLEAN)
RETURNS TIMESTAMPTZ
LANGUAGE plpgsql
SET search_path = pgkv, pg_temp
AS $$
BEGIN
    IF (CASE WHEN p_ex IS NOT NULL THEN 1 ELSE 0 END +
        CASE WHEN p_px IS NOT NULL THEN 1 ELSE 0 END +
        CASE WHEN p_exat IS NOT NULL THEN 1 ELSE 0 END +
        CASE WHEN p_pxat IS NOT NULL THEN 1 ELSE 0 END +
        CASE WHEN p_keepttl THEN 1 ELSE 0 END) > 1 THEN
        RAISE EXCEPTION 'ERR syntax error: conflicting expiration options';
    END IF;

    IF p_ex IS NOT NULL THEN
        IF p_ex <= 0 THEN RAISE EXCEPTION 'ERR invalid expire time'; END IF;
        RETURN clock_timestamp() + (p_ex || ' seconds')::interval;
    ELSIF p_px IS NOT NULL THEN
        IF p_px <= 0 THEN RAISE EXCEPTION 'ERR invalid expire time'; END IF;
        RETURN clock_timestamp() + (p_px || ' milliseconds')::interval;
    ELSIF p_exat IS NOT NULL THEN
        RETURN to_timestamp(p_exat);
    ELSIF p_pxat IS NOT NULL THEN
        RETURN to_timestamp(p_pxat / 1000.0);
    END IF;
    RETURN NULL;
END;
$$;

-- _REQUIRE_STRING (internal): raise WRONGTYPE if the key exists and is not a string.
CREATE OR REPLACE FUNCTION pgkv._require_string(p_key TEXT)
RETURNS VOID
LANGUAGE plpgsql
SET search_path = pgkv, pg_temp
AS $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM pgkv.store s
        WHERE s.key = p_key
          AND s.type != 'string'
          AND (s.expires_at IS NULL OR s.expires_at >= clock_timestamp())
    ) THEN
        RAISE EXCEPTION 'WRONGTYPE Operation against a key holding the wrong kind of value';
    END IF;
END;
$$;

-- _LIKE_PREFIX (internal): a LIKE pattern matching keys that start with p_prefix, taken literally.
CREATE OR REPLACE FUNCTION pgkv._like_prefix(p_prefix TEXT)
RETURNS TEXT
LANGUAGE sql
IMMUTABLE
SET search_path = pgkv, pg_temp
AS $$
    SELECT replace(replace(replace(COALESCE(p_prefix, ''), E'\\', E'\\\\'), '%', E'\\%'), '_', E'\\_') || '%'
$$;

-- ============================================================================
-- Basic Key-Value Operations
-- ============================================================================

-- SET: Set a key with optional TTL (time-to-live in seconds)
-- Value is automatically converted to JSONB (strings must be valid JSON strings)
CREATE OR REPLACE FUNCTION pgkv.set(p_key TEXT, p_value TEXT, p_ttl_seconds INTEGER DEFAULT NULL)
RETURNS TEXT
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pgkv, pg_temp
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
SECURITY DEFINER
SET search_path = pgkv, pg_temp
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
    IF v_expires_at IS NOT NULL AND v_expires_at < clock_timestamp() THEN
        DELETE FROM pgkv.store WHERE key = p_key AND expires_at IS NOT NULL AND expires_at < clock_timestamp();
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
-- Returns the number of keys that existed (an expired key is removed but not counted)
CREATE OR REPLACE FUNCTION pgkv.del(VARIADIC p_keys TEXT[])
RETURNS INTEGER
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pgkv, pg_temp
AS $$
DECLARE
    v_deleted INTEGER;
BEGIN
    -- Lock the rows in key order first, so two multi-key calls cannot deadlock each other.
    WITH locked AS (
        SELECT s.key FROM pgkv.store s WHERE s.key = ANY(p_keys) ORDER BY s.key COLLATE "C" FOR UPDATE
    ), deleted AS (
        DELETE FROM pgkv.store s USING locked l WHERE s.key = l.key RETURNING s.expires_at
    )
    SELECT COUNT(*)::integer INTO v_deleted
    FROM deleted d
    WHERE d.expires_at IS NULL OR d.expires_at >= clock_timestamp();

    RETURN v_deleted;
END;
$$;

-- EXISTS: Check if a key exists (returns 1 if exists, 0 otherwise)
CREATE OR REPLACE FUNCTION pgkv.exists(VARIADIC p_keys TEXT[])
RETURNS INTEGER
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pgkv, pg_temp
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
                DELETE FROM pgkv.store WHERE key = v_key AND expires_at IS NOT NULL AND expires_at < clock_timestamp();
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
-- Returns 0 if the key does not exist; an expired key does not exist and is not revived
CREATE OR REPLACE FUNCTION pgkv.expire(p_key TEXT, p_seconds INTEGER)
RETURNS INTEGER
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pgkv, pg_temp
AS $$
DECLARE
    v_updated INTEGER;
BEGIN
    UPDATE pgkv.store
    SET expires_at = NOW() + (p_seconds || ' seconds')::INTERVAL,
        updated_at = NOW()
    WHERE key = p_key
      AND (expires_at IS NULL OR expires_at >= clock_timestamp());

    GET DIAGNOSTICS v_updated = ROW_COUNT;
    RETURN CASE WHEN v_updated > 0 THEN 1 ELSE 0 END;
END;
$$;

-- TTL: Get the time to live for a key (in seconds)
-- Returns -2 if key doesn't exist, -1 if no expiration, or seconds remaining
CREATE OR REPLACE FUNCTION pgkv.ttl(p_key TEXT)
RETURNS INTEGER
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pgkv, pg_temp
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
        DELETE FROM pgkv.store WHERE key = p_key AND expires_at IS NOT NULL AND expires_at < clock_timestamp();
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
SECURITY DEFINER
SET search_path = pgkv, pg_temp
AS $$
BEGIN
    RETURN pgkv.incrby(p_key, 1);
END;
$$;

-- DECR: Decrement the integer value of a key by 1
-- Stores result as raw JSONB number
CREATE OR REPLACE FUNCTION pgkv.decr(p_key TEXT)
RETURNS BIGINT
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pgkv, pg_temp
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
SECURITY DEFINER
SET search_path = pgkv, pg_temp
AS $$
DECLARE
    v_value JSONB;
    v_numeric BIGINT;
    v_type TEXT;
    v_expires_at TIMESTAMPTZ;
BEGIN
    SELECT o_value, o_type, o_expires_at INTO v_value, v_type, v_expires_at
    FROM pgkv._lock_key(p_key, TRUE);

    -- If key doesn't exist, start from 0
    IF v_type IS NULL THEN
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

    -- Increment by the specified amount
    v_numeric := v_numeric + p_increment;

    -- Store as raw JSONB number
    PERFORM pgkv._put(p_key, to_jsonb(v_numeric), 'string', v_expires_at);

    RETURN v_numeric;
END;
$$;

-- DECRBY: Decrement the integer value of a key by the given amount
CREATE OR REPLACE FUNCTION pgkv.decrby(p_key TEXT, p_decrement BIGINT)
RETURNS BIGINT
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pgkv, pg_temp
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
SECURITY DEFINER
SET search_path = pgkv, pg_temp
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
-- One statement, written in key order so two calls cannot deadlock each other. If a key is
-- given more than once, its last value wins.
CREATE OR REPLACE FUNCTION pgkv.mset(VARIADIC p_pairs TEXT[])
RETURNS TEXT
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pgkv, pg_temp
AS $$
BEGIN
    -- Ensure we have an even number of arguments
    IF array_length(p_pairs, 1) % 2 != 0 THEN
        RAISE EXCEPTION 'wrong number of arguments for MSET';
    END IF;

    INSERT INTO pgkv.store AS s (key, value, type, expires_at, updated_at)
    SELECT d.k, to_jsonb(d.v), 'string', NULL, NOW()
    FROM (
        SELECT DISTINCT ON (p_pairs[i]) p_pairs[i] AS k, p_pairs[i + 1] AS v
        FROM generate_series(1, array_length(p_pairs, 1), 2) AS i
        ORDER BY p_pairs[i], i DESC
    ) d
    ORDER BY d.k COLLATE "C"
    ON CONFLICT (key) DO UPDATE
    SET value = EXCLUDED.value,
        type = EXCLUDED.type,
        expires_at = EXCLUDED.expires_at,
        updated_at = NOW();

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
SECURITY DEFINER
SET search_path = pgkv, pg_temp
AS $$
DECLARE
    v_pg_pattern TEXT;
BEGIN
    -- First, clean up expired keys
    DELETE FROM pgkv.store
    WHERE expires_at IS NOT NULL AND expires_at < clock_timestamp();

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
SECURITY DEFINER
SET search_path = pgkv, pg_temp
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
    IF v_expires_at IS NOT NULL AND v_expires_at < clock_timestamp() THEN
        DELETE FROM pgkv.store WHERE key = p_key AND expires_at IS NOT NULL AND expires_at < clock_timestamp();
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
SECURITY DEFINER
SET search_path = pgkv, pg_temp
AS $$
DECLARE
    v_type TEXT;
    v_value JSONB;
    v_expires_at TIMESTAMPTZ;
    v_new TEXT;
BEGIN
    SELECT o_value, o_type, o_expires_at INTO v_value, v_type, v_expires_at
    FROM pgkv._lock_key(p_key, TRUE);

    -- Check type
    IF v_type IS NOT NULL AND v_type != 'string' THEN
        RAISE EXCEPTION 'WRONGTYPE Operation against a key holding the wrong kind of value';
    END IF;

    -- Current value as text (empty if the key doesn't exist), then append
    v_new := COALESCE(v_value #>> '{}', '') || p_value;

    PERFORM pgkv._put(p_key, to_jsonb(v_new), 'string', v_expires_at);

    RETURN length(v_new);
END;
$$;

-- STRLEN: Get the length of the value stored in a key
CREATE OR REPLACE FUNCTION pgkv.strlen(p_key TEXT)
RETURNS INTEGER
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pgkv, pg_temp
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
    IF v_value IS NULL OR (v_expires_at IS NOT NULL AND v_expires_at < clock_timestamp()) THEN
        IF v_expires_at IS NOT NULL AND v_expires_at < clock_timestamp() THEN
            DELETE FROM pgkv.store WHERE key = p_key AND expires_at IS NOT NULL AND expires_at < clock_timestamp();
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
SECURITY DEFINER
SET search_path = pgkv, pg_temp
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
    IF v_value IS NULL OR (v_expires_at IS NOT NULL AND v_expires_at < clock_timestamp()) THEN
        IF v_expires_at IS NOT NULL AND v_expires_at < clock_timestamp() THEN
            DELETE FROM pgkv.store WHERE key = p_key AND expires_at IS NOT NULL AND expires_at < clock_timestamp();
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
SECURITY DEFINER
SET search_path = pgkv, pg_temp
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

    SELECT o_value, o_type, o_expires_at INTO v_value, v_type, v_expires_at
    FROM pgkv._lock_key(p_key, TRUE);

    -- Check type
    IF v_type IS NOT NULL AND v_type != 'string' THEN
        RAISE EXCEPTION 'WRONGTYPE Operation against a key holding the wrong kind of value';
    END IF;

    -- Current value as text (empty if the key doesn't exist)
    v_current := COALESCE(v_value #>> '{}', '');

    v_len := length(v_current);
    v_value_len := length(p_value);

    -- If offset is beyond current length, pad with NULL bytes (\x00)
    IF p_offset > v_len THEN
        v_current := v_current || repeat(chr(0), p_offset - v_len);
    END IF;

    -- Build new string: prefix + new value + suffix
    IF p_offset = 0 THEN
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

    PERFORM pgkv._put(p_key, to_jsonb(v_new), 'string', v_expires_at);

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
SECURITY DEFINER
SET search_path = pgkv, pg_temp
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

    SELECT o_value, o_type, o_expires_at INTO v_value, v_type, v_expires_at
    FROM pgkv._lock_key(p_key, TRUE);

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

    PERFORM pgkv._put(p_key, v_value, 'hash', v_expires_at);

    RETURN v_added;
END;
$$;

-- HGET: Get field value from hash
CREATE OR REPLACE FUNCTION pgkv.hget(p_key TEXT, p_field TEXT)
RETURNS JSONB
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pgkv, pg_temp
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
    IF v_expires_at IS NOT NULL AND v_expires_at < clock_timestamp() THEN
        DELETE FROM pgkv.store WHERE key = p_key AND expires_at IS NOT NULL AND expires_at < clock_timestamp();
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
SECURITY DEFINER
SET search_path = pgkv, pg_temp
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
    IF v_expires_at IS NOT NULL AND v_expires_at < clock_timestamp() THEN
        DELETE FROM pgkv.store WHERE key = p_key AND expires_at IS NOT NULL AND expires_at < clock_timestamp();
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
SECURITY DEFINER
SET search_path = pgkv, pg_temp
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
    IF v_expires_at IS NOT NULL AND v_expires_at < clock_timestamp() THEN
        DELETE FROM pgkv.store WHERE key = p_key AND expires_at IS NOT NULL AND expires_at < clock_timestamp();
        RETURN;
    END IF;

    -- Check type
    IF v_type != 'hash' THEN
        RAISE EXCEPTION 'WRONGTYPE Operation against a key holding the wrong kind of value';
    END IF;

    -- Return all key-value pairs
    RETURN QUERY
    SELECT kv.key::TEXT, kv.value
    FROM jsonb_each(v_value) AS kv;
END;
$$;

-- HDEL: Delete field(s) from hash
CREATE OR REPLACE FUNCTION pgkv.hdel(p_key TEXT, VARIADIC p_fields TEXT[])
RETURNS INTEGER
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pgkv, pg_temp
AS $$
DECLARE
    v_value JSONB;
    v_type TEXT;
    v_expires_at TIMESTAMPTZ;
    v_field TEXT;
    v_deleted INTEGER := 0;
BEGIN
    SELECT o_value, o_type, o_expires_at INTO v_value, v_type, v_expires_at
    FROM pgkv._lock_key(p_key, FALSE);

    IF v_type IS NULL THEN
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
    IF v_value = '{}'::jsonb THEN
        DELETE FROM pgkv.store WHERE key = p_key;
    ELSE
        PERFORM pgkv._put(p_key, v_value, 'hash', v_expires_at);
    END IF;

    RETURN v_deleted;
END;
$$;

-- HEXISTS: Check if field exists in hash
CREATE OR REPLACE FUNCTION pgkv.hexists(p_key TEXT, p_field TEXT)
RETURNS INTEGER
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pgkv, pg_temp
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
    IF v_expires_at IS NOT NULL AND v_expires_at < clock_timestamp() THEN
        DELETE FROM pgkv.store WHERE key = p_key AND expires_at IS NOT NULL AND expires_at < clock_timestamp();
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
SECURITY DEFINER
SET search_path = pgkv, pg_temp
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
    IF v_expires_at IS NOT NULL AND v_expires_at < clock_timestamp() THEN
        DELETE FROM pgkv.store WHERE key = p_key AND expires_at IS NOT NULL AND expires_at < clock_timestamp();
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
SECURITY DEFINER
SET search_path = pgkv, pg_temp
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
    IF v_expires_at IS NOT NULL AND v_expires_at < clock_timestamp() THEN
        DELETE FROM pgkv.store WHERE key = p_key AND expires_at IS NOT NULL AND expires_at < clock_timestamp();
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
SECURITY DEFINER
SET search_path = pgkv, pg_temp
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
    IF v_expires_at IS NOT NULL AND v_expires_at < clock_timestamp() THEN
        DELETE FROM pgkv.store WHERE key = p_key AND expires_at IS NOT NULL AND expires_at < clock_timestamp();
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
SECURITY DEFINER
SET search_path = pgkv, pg_temp
AS $$
DECLARE
    v_value JSONB;
    v_type TEXT;
    v_expires_at TIMESTAMPTZ;
    v_field_value JSONB;
    v_numeric BIGINT;
BEGIN
    SELECT o_value, o_type, o_expires_at INTO v_value, v_type, v_expires_at
    FROM pgkv._lock_key(p_key, TRUE);

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

    PERFORM pgkv._put(p_key, v_value, 'hash', v_expires_at);

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
SECURITY DEFINER
SET search_path = pgkv, pg_temp
AS $$
DECLARE
    v_value JSONB;
    v_type TEXT;
    v_expires_at TIMESTAMPTZ;
    v_i INTEGER;
BEGIN
    SELECT o_value, o_type, o_expires_at INTO v_value, v_type, v_expires_at
    FROM pgkv._lock_key(p_key, TRUE);

    -- Check type
    IF v_type IS NOT NULL AND v_type != 'list' THEN
        RAISE EXCEPTION 'WRONGTYPE Operation against a key holding the wrong kind of value';
    END IF;

    -- Initialize if new
    IF v_value IS NULL THEN
        v_value := '[]'::jsonb;
    END IF;

    -- Prepend values one by one to maintain Redis behavior
    -- LPUSH a b c d results in [d, c, b, a]
    FOR v_i IN 1..array_length(p_values, 1)
    LOOP
        v_value := jsonb_insert(v_value, '{0}', to_jsonb(p_values[v_i]));
    END LOOP;

    PERFORM pgkv._put(p_key, v_value, 'list', v_expires_at);

    RETURN jsonb_array_length(v_value);
END;
$$;

-- RPUSH: Push value(s) to tail of list
CREATE OR REPLACE FUNCTION pgkv.rpush(p_key TEXT, VARIADIC p_values TEXT[])
RETURNS INTEGER
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pgkv, pg_temp
AS $$
DECLARE
    v_value JSONB;
    v_type TEXT;
    v_expires_at TIMESTAMPTZ;
    v_val TEXT;
BEGIN
    SELECT o_value, o_type, o_expires_at INTO v_value, v_type, v_expires_at
    FROM pgkv._lock_key(p_key, TRUE);

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

    PERFORM pgkv._put(p_key, v_value, 'list', v_expires_at);

    RETURN jsonb_array_length(v_value);
END;
$$;

-- LPOP: Pop value(s) from head of list
CREATE OR REPLACE FUNCTION pgkv.lpop(p_key TEXT, p_count INTEGER DEFAULT 1)
RETURNS SETOF JSONB
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pgkv, pg_temp
AS $$
DECLARE
    v_value JSONB;
    v_type TEXT;
    v_expires_at TIMESTAMPTZ;
    v_len INTEGER;
    v_i INTEGER;
    v_new_array JSONB;
BEGIN
    SELECT o_value, o_type, o_expires_at INTO v_value, v_type, v_expires_at
    FROM pgkv._lock_key(p_key, FALSE);

    IF v_type IS NULL THEN
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

        PERFORM pgkv._put(p_key, v_new_array, 'list', v_expires_at);
    END IF;
END;
$$;

-- RPOP: Pop value(s) from tail of list
CREATE OR REPLACE FUNCTION pgkv.rpop(p_key TEXT, p_count INTEGER DEFAULT 1)
RETURNS SETOF JSONB
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pgkv, pg_temp
AS $$
DECLARE
    v_value JSONB;
    v_type TEXT;
    v_expires_at TIMESTAMPTZ;
    v_len INTEGER;
    v_i INTEGER;
    v_new_array JSONB;
BEGIN
    SELECT o_value, o_type, o_expires_at INTO v_value, v_type, v_expires_at
    FROM pgkv._lock_key(p_key, FALSE);

    IF v_type IS NULL THEN
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

        PERFORM pgkv._put(p_key, v_new_array, 'list', v_expires_at);
    END IF;
END;
$$;

-- LLEN: Get list length
CREATE OR REPLACE FUNCTION pgkv.llen(p_key TEXT)
RETURNS INTEGER
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pgkv, pg_temp
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
    IF v_expires_at IS NOT NULL AND v_expires_at < clock_timestamp() THEN
        DELETE FROM pgkv.store WHERE key = p_key AND expires_at IS NOT NULL AND expires_at < clock_timestamp();
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
SECURITY DEFINER
SET search_path = pgkv, pg_temp
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
    IF v_expires_at IS NOT NULL AND v_expires_at < clock_timestamp() THEN
        DELETE FROM pgkv.store WHERE key = p_key AND expires_at IS NOT NULL AND expires_at < clock_timestamp();
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
SECURITY DEFINER
SET search_path = pgkv, pg_temp
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
    IF v_expires_at IS NOT NULL AND v_expires_at < clock_timestamp() THEN
        DELETE FROM pgkv.store WHERE key = p_key AND expires_at IS NOT NULL AND expires_at < clock_timestamp();
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
SECURITY DEFINER
SET search_path = pgkv, pg_temp
AS $$
DECLARE
    v_value JSONB;
    v_type TEXT;
    v_expires_at TIMESTAMPTZ;
    v_len INTEGER;
    v_idx INTEGER;
BEGIN
    SELECT o_value, o_type, o_expires_at INTO v_value, v_type, v_expires_at
    FROM pgkv._lock_key(p_key, FALSE);

    IF v_type IS NULL THEN
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

    PERFORM pgkv._put(p_key, v_value, 'list', v_expires_at);

    RETURN 'OK';
END;
$$;

-- LTRIM: Trim list to specified range
CREATE OR REPLACE FUNCTION pgkv.ltrim(p_key TEXT, p_start INTEGER, p_stop INTEGER)
RETURNS TEXT
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pgkv, pg_temp
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
    SELECT o_value, o_type, o_expires_at INTO v_value, v_type, v_expires_at
    FROM pgkv._lock_key(p_key, FALSE);

    IF v_type IS NULL THEN
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

    PERFORM pgkv._put(p_key, v_new_array, 'list', v_expires_at);

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
SECURITY DEFINER
SET search_path = pgkv, pg_temp
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
    SELECT o_value, o_type, o_expires_at INTO v_value, v_type, v_expires_at
    FROM pgkv._lock_key(p_key, FALSE);

    IF v_type IS NULL THEN
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
        PERFORM pgkv._put(p_key, v_new_array, 'list', v_expires_at);
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
SECURITY DEFINER
SET search_path = pgkv, pg_temp
AS $$
DECLARE
    v_value JSONB;
    v_type TEXT;
    v_expires_at TIMESTAMPTZ;
    v_member TEXT;
    v_added INTEGER := 0;
    v_member_json JSONB;
BEGIN
    SELECT o_value, o_type, o_expires_at INTO v_value, v_type, v_expires_at
    FROM pgkv._lock_key(p_key, TRUE);

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

    -- Store if any members were added (a new key always, to replace its placeholder)
    IF v_added > 0 OR v_type IS NULL THEN
        PERFORM pgkv._put(p_key, v_value, 'set', v_expires_at);
    END IF;

    RETURN v_added;
END;
$$;

-- SREM: Remove member(s) from set
CREATE OR REPLACE FUNCTION pgkv.srem(p_key TEXT, VARIADIC p_members TEXT[])
RETURNS INTEGER
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pgkv, pg_temp
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
    SELECT o_value, o_type, o_expires_at INTO v_value, v_type, v_expires_at
    FROM pgkv._lock_key(p_key, FALSE);

    IF v_type IS NULL THEN
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
        PERFORM pgkv._put(p_key, v_new_array, 'set', v_expires_at);
    END IF;

    RETURN v_removed;
END;
$$;

-- SMEMBERS: Get all members of set
CREATE OR REPLACE FUNCTION pgkv.smembers(p_key TEXT)
RETURNS SETOF JSONB
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pgkv, pg_temp
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
    IF v_expires_at IS NOT NULL AND v_expires_at < clock_timestamp() THEN
        DELETE FROM pgkv.store WHERE key = p_key AND expires_at IS NOT NULL AND expires_at < clock_timestamp();
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
SECURITY DEFINER
SET search_path = pgkv, pg_temp
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
    IF v_expires_at IS NOT NULL AND v_expires_at < clock_timestamp() THEN
        DELETE FROM pgkv.store WHERE key = p_key AND expires_at IS NOT NULL AND expires_at < clock_timestamp();
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
SECURITY DEFINER
SET search_path = pgkv, pg_temp
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
    IF v_expires_at IS NOT NULL AND v_expires_at < clock_timestamp() THEN
        DELETE FROM pgkv.store WHERE key = p_key AND expires_at IS NOT NULL AND expires_at < clock_timestamp();
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
SECURITY DEFINER
SET search_path = pgkv, pg_temp
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
    IF v_expires_at IS NOT NULL AND v_expires_at < clock_timestamp() THEN
        DELETE FROM pgkv.store WHERE key = v_first_key AND expires_at IS NOT NULL AND expires_at < clock_timestamp();
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

        IF v_expires_at IS NOT NULL AND v_expires_at < clock_timestamp() THEN
            DELETE FROM pgkv.store WHERE key = v_key AND expires_at IS NOT NULL AND expires_at < clock_timestamp();
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
SECURITY DEFINER
SET search_path = pgkv, pg_temp
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

        IF v_expires_at IS NOT NULL AND v_expires_at < clock_timestamp() THEN
            DELETE FROM pgkv.store WHERE key = v_key AND expires_at IS NOT NULL AND expires_at < clock_timestamp();
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
SECURITY DEFINER
SET search_path = pgkv, pg_temp
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
    IF v_expires_at IS NOT NULL AND v_expires_at < clock_timestamp() THEN
        DELETE FROM pgkv.store WHERE key = v_first_key AND expires_at IS NOT NULL AND expires_at < clock_timestamp();
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

        IF v_expires_at IS NOT NULL AND v_expires_at < clock_timestamp() THEN
            DELETE FROM pgkv.store WHERE key = v_key AND expires_at IS NOT NULL AND expires_at < clock_timestamp();
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
SECURITY DEFINER
SET search_path = pgkv, pg_temp
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

    SELECT o_value, o_type, o_expires_at INTO v_value, v_type, v_expires_at
    FROM pgkv._lock_key(p_key, TRUE);

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

    PERFORM pgkv._put(p_key, v_value, 'zset', v_expires_at);

    RETURN v_added;
END;
$$;

-- ZREM: Remove one or more members from a sorted set
-- Returns the number of members removed
CREATE OR REPLACE FUNCTION pgkv.zrem(p_key TEXT, VARIADIC p_members TEXT[])
RETURNS INTEGER
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pgkv, pg_temp
AS $$
DECLARE
    v_type TEXT;
    v_value JSONB;
    v_expires_at TIMESTAMPTZ;
    v_member TEXT;
    v_removed INTEGER := 0;
BEGIN
    SELECT o_value, o_type, o_expires_at INTO v_value, v_type, v_expires_at
    FROM pgkv._lock_key(p_key, FALSE);

    IF v_type IS NULL THEN
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
        PERFORM pgkv._put(p_key, v_value, 'zset', v_expires_at);
    END IF;

    RETURN v_removed;
END;
$$;

-- ZRANGE: Return a range of members in a sorted set, by index (sorted by score ascending)
-- Supports optional WITHSCORES flag
CREATE OR REPLACE FUNCTION pgkv.zrange(p_key TEXT, p_start INTEGER, p_stop INTEGER, p_withscores BOOLEAN DEFAULT FALSE)
RETURNS TABLE(member TEXT, score NUMERIC)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pgkv, pg_temp
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
    IF v_value IS NULL OR (v_expires_at IS NOT NULL AND v_expires_at < clock_timestamp()) THEN
        IF v_expires_at IS NOT NULL AND v_expires_at < clock_timestamp() THEN
            DELETE FROM pgkv.store WHERE key = p_key AND expires_at IS NOT NULL AND expires_at < clock_timestamp();
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
SECURITY DEFINER
SET search_path = pgkv, pg_temp
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
    IF v_value IS NULL OR (v_expires_at IS NOT NULL AND v_expires_at < clock_timestamp()) THEN
        IF v_expires_at IS NOT NULL AND v_expires_at < clock_timestamp() THEN
            DELETE FROM pgkv.store WHERE key = p_key AND expires_at IS NOT NULL AND expires_at < clock_timestamp();
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
SECURITY DEFINER
SET search_path = pgkv, pg_temp
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
    IF v_value IS NULL OR (v_expires_at IS NOT NULL AND v_expires_at < clock_timestamp()) THEN
        IF v_expires_at IS NOT NULL AND v_expires_at < clock_timestamp() THEN
            DELETE FROM pgkv.store WHERE key = p_key AND expires_at IS NOT NULL AND expires_at < clock_timestamp();
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
SECURITY DEFINER
SET search_path = pgkv, pg_temp
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
    IF v_value IS NULL OR (v_expires_at IS NOT NULL AND v_expires_at < clock_timestamp()) THEN
        IF v_expires_at IS NOT NULL AND v_expires_at < clock_timestamp() THEN
            DELETE FROM pgkv.store WHERE key = p_key AND expires_at IS NOT NULL AND expires_at < clock_timestamp();
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
SECURITY DEFINER
SET search_path = pgkv, pg_temp
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
    IF v_value IS NULL OR (v_expires_at IS NOT NULL AND v_expires_at < clock_timestamp()) THEN
        IF v_expires_at IS NOT NULL AND v_expires_at < clock_timestamp() THEN
            DELETE FROM pgkv.store WHERE key = p_key AND expires_at IS NOT NULL AND expires_at < clock_timestamp();
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
    WITH ranked AS (
        SELECT kv.key,
               (ROW_NUMBER() OVER (ORDER BY (kv.value::text)::numeric ASC, kv.key::text ASC) - 1)::integer AS rank
        FROM jsonb_each(v_value) kv
    )
    SELECT rank INTO v_rank
    FROM ranked
    WHERE key = p_member;

    RETURN v_rank;
END;
$$;

-- ZREVRANK: Determine the index of a member in a sorted set (0-based, descending order)
CREATE OR REPLACE FUNCTION pgkv.zrevrank(p_key TEXT, p_member TEXT)
RETURNS INTEGER
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pgkv, pg_temp
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
    IF v_value IS NULL OR (v_expires_at IS NOT NULL AND v_expires_at < clock_timestamp()) THEN
        IF v_expires_at IS NOT NULL AND v_expires_at < clock_timestamp() THEN
            DELETE FROM pgkv.store WHERE key = p_key AND expires_at IS NOT NULL AND expires_at < clock_timestamp();
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
    WITH ranked AS (
        SELECT kv.key,
               (ROW_NUMBER() OVER (ORDER BY (kv.value::text)::numeric DESC, kv.key::text DESC) - 1)::integer AS rank
        FROM jsonb_each(v_value) kv
    )
    SELECT rank INTO v_rank
    FROM ranked
    WHERE key = p_member;

    RETURN v_rank;
END;
$$;

-- ZRANGEBYSCORE: Return a range of members in a sorted set, by score
-- Supports optional WITHSCORES flag
CREATE OR REPLACE FUNCTION pgkv.zrangebyscore(p_key TEXT, p_min NUMERIC, p_max NUMERIC, p_withscores BOOLEAN DEFAULT FALSE)
RETURNS TABLE(member TEXT, score NUMERIC)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pgkv, pg_temp
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
    IF v_value IS NULL OR (v_expires_at IS NOT NULL AND v_expires_at < clock_timestamp()) THEN
        IF v_expires_at IS NOT NULL AND v_expires_at < clock_timestamp() THEN
            DELETE FROM pgkv.store WHERE key = p_key AND expires_at IS NOT NULL AND expires_at < clock_timestamp();
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
SECURITY DEFINER
SET search_path = pgkv, pg_temp
AS $$
DECLARE
    v_type TEXT;
    v_value JSONB;
    v_expires_at TIMESTAMPTZ;
    v_current_score NUMERIC;
    v_new_score NUMERIC;
BEGIN
    SELECT o_value, o_type, o_expires_at INTO v_value, v_type, v_expires_at
    FROM pgkv._lock_key(p_key, TRUE);

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

    PERFORM pgkv._put(p_key, v_value, 'zset', v_expires_at);

    RETURN v_new_score;
END;
$$;

-- ZCOUNT: Count the members in a sorted set with scores within the given range
CREATE OR REPLACE FUNCTION pgkv.zcount(p_key TEXT, p_min NUMERIC, p_max NUMERIC)
RETURNS INTEGER
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pgkv, pg_temp
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
    IF v_value IS NULL OR (v_expires_at IS NOT NULL AND v_expires_at < clock_timestamp()) THEN
        IF v_expires_at IS NOT NULL AND v_expires_at < clock_timestamp() THEN
            DELETE FROM pgkv.store WHERE key = p_key AND expires_at IS NOT NULL AND expires_at < clock_timestamp();
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
SECURITY DEFINER
SET search_path = pgkv, pg_temp
AS $$
DECLARE
    v_deleted INTEGER;
BEGIN
    DELETE FROM pgkv.store
    WHERE expires_at IS NOT NULL AND expires_at < clock_timestamp();

    GET DIAGNOSTICS v_deleted = ROW_COUNT;
    RETURN v_deleted;
END;
$$;

-- FLUSHALL: Delete all keys in the store
CREATE OR REPLACE FUNCTION pgkv.flushall()
RETURNS TEXT
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pgkv, pg_temp
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
SECURITY DEFINER
SET search_path = pgkv, pg_temp
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
-- Extended String Operations and SET Options
-- ============================================================================

-- SET_OPTS: SET with Redis options (NX, XX, GET, EX, PX, EXAT, PXAT, KEEPTTL)
-- Returns "OK" when the key was set, NULL when NX or XX stopped it.
-- With p_get: the value the key held before (NULL if it did not exist). With p_get and p_nx,
-- as in Redis 7: the existing value when the key exists (nothing is set), NULL when it was set.
-- An expired key counts as absent. Each form is atomic.
CREATE OR REPLACE FUNCTION pgkv.set_opts(
    p_key TEXT,
    p_value TEXT,
    p_nx BOOLEAN DEFAULT FALSE,
    p_xx BOOLEAN DEFAULT FALSE,
    p_get BOOLEAN DEFAULT FALSE,
    p_ex INTEGER DEFAULT NULL,
    p_px BIGINT DEFAULT NULL,
    p_exat BIGINT DEFAULT NULL,
    p_pxat BIGINT DEFAULT NULL,
    p_keepttl BOOLEAN DEFAULT FALSE
)
RETURNS JSONB
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pgkv, pg_temp
AS $$
DECLARE
    v_value JSONB;
    v_type TEXT;
    v_expires_at TIMESTAMPTZ;
    v_new_expires_at TIMESTAMPTZ;
BEGIN
    IF p_nx AND p_xx THEN
        RAISE EXCEPTION 'ERR NX and XX options at the same time are not compatible';
    END IF;

    v_new_expires_at := pgkv._expires_at(p_ex, p_px, p_exat, p_pxat, p_keepttl);

    IF p_get THEN
        -- The old value has to be read and replaced as one step: hold the row.
        SELECT o_value, o_type, o_expires_at INTO v_value, v_type, v_expires_at
        FROM pgkv._lock_key(p_key, NOT p_xx);

        IF p_nx AND v_type IS NOT NULL THEN
            RETURN v_value;
        END IF;
        IF p_xx AND v_type IS NULL THEN
            RETURN NULL;
        END IF;

        PERFORM pgkv._put(
            p_key,
            to_jsonb(p_value),
            'string',
            CASE WHEN p_keepttl THEN v_expires_at ELSE v_new_expires_at END
        );
        RETURN v_value;
    END IF;

    IF p_nx THEN
        -- One statement: insert, or take over the row only if what is there has expired.
        INSERT INTO pgkv.store AS s (key, value, type, expires_at, updated_at)
        VALUES (p_key, to_jsonb(p_value), 'string', v_new_expires_at, NOW())
        ON CONFLICT (key) DO UPDATE
        SET value = EXCLUDED.value,
            type = EXCLUDED.type,
            expires_at = EXCLUDED.expires_at,
            created_at = NOW(),
            updated_at = NOW()
        WHERE s.expires_at IS NOT NULL AND s.expires_at < clock_timestamp();
    ELSIF p_xx THEN
        -- One statement: update only a key that exists and has not expired.
        UPDATE pgkv.store s
        SET value = to_jsonb(p_value),
            type = 'string',
            expires_at = CASE WHEN p_keepttl THEN s.expires_at ELSE v_new_expires_at END,
            updated_at = NOW()
        WHERE s.key = p_key
          AND (s.expires_at IS NULL OR s.expires_at >= clock_timestamp());
    ELSE
        INSERT INTO pgkv.store AS s (key, value, type, expires_at, updated_at)
        VALUES (p_key, to_jsonb(p_value), 'string', v_new_expires_at, NOW())
        ON CONFLICT (key) DO UPDATE
        SET value = EXCLUDED.value,
            type = EXCLUDED.type,
            expires_at = CASE
                WHEN p_keepttl AND (s.expires_at IS NULL OR s.expires_at >= clock_timestamp()) THEN s.expires_at
                ELSE EXCLUDED.expires_at
            END,
            updated_at = NOW();
    END IF;

    IF NOT FOUND THEN
        RETURN NULL;
    END IF;
    RETURN to_jsonb('OK'::text);
END;
$$;

CREATE OR REPLACE FUNCTION pgkv.setnx(p_key TEXT, p_value TEXT)
RETURNS INTEGER
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pgkv, pg_temp
AS $$
BEGIN
    RETURN CASE WHEN pgkv.set_opts(p_key, p_value, p_nx := TRUE) IS NULL THEN 0 ELSE 1 END;
END;
$$;

CREATE OR REPLACE FUNCTION pgkv.setex(p_key TEXT, p_seconds INTEGER, p_value TEXT)
RETURNS TEXT
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pgkv, pg_temp
AS $$
BEGIN
    PERFORM pgkv.set_opts(p_key, p_value, p_ex := p_seconds);
    RETURN 'OK';
END;
$$;

CREATE OR REPLACE FUNCTION pgkv.psetex(p_key TEXT, p_milliseconds BIGINT, p_value TEXT)
RETURNS TEXT
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pgkv, pg_temp
AS $$
BEGIN
    PERFORM pgkv.set_opts(p_key, p_value, p_px := p_milliseconds);
    RETURN 'OK';
END;
$$;

-- GETDEL: Get the value of a string key and delete it, in one statement.
-- Of several sessions calling it on one key at the same time, exactly one gets the value.
CREATE OR REPLACE FUNCTION pgkv.getdel(p_key TEXT)
RETURNS JSONB
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pgkv, pg_temp
AS $$
DECLARE
    v_value JSONB;
BEGIN
    DELETE FROM pgkv.store s
    WHERE s.key = p_key
      AND s.type = 'string'
      AND (s.expires_at IS NULL OR s.expires_at >= clock_timestamp())
    RETURNING s.value INTO v_value;

    IF FOUND THEN
        RETURN v_value;
    END IF;

    -- Nothing deleted: the key is absent or expired (NULL), or holds another type (WRONGTYPE).
    PERFORM pgkv._require_string(p_key);
    RETURN NULL;
END;
$$;

-- GETEX: Get the value of a string key and, with p_seconds, set its expiry in the same step
CREATE OR REPLACE FUNCTION pgkv.getex(p_key TEXT, p_seconds INTEGER DEFAULT NULL)
RETURNS JSONB
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pgkv, pg_temp
AS $$
DECLARE
    v_value JSONB;
    v_type TEXT;
    v_expires_at TIMESTAMPTZ;
BEGIN
    IF p_seconds IS NULL THEN
        RETURN pgkv.get(p_key);
    END IF;

    SELECT o_value, o_type, o_expires_at INTO v_value, v_type, v_expires_at
    FROM pgkv._lock_key(p_key, FALSE);

    IF v_type IS NULL THEN
        RETURN NULL;
    END IF;
    IF v_type != 'string' THEN
        RAISE EXCEPTION 'WRONGTYPE Operation against a key holding the wrong kind of value';
    END IF;

    PERFORM pgkv._put(p_key, v_value, v_type, NOW() + (p_seconds || ' seconds')::INTERVAL);
    RETURN v_value;
END;
$$;

-- INCRBYFLOAT: Increment the numeric value of a key by the given amount
CREATE OR REPLACE FUNCTION pgkv.incrbyfloat(p_key TEXT, p_increment NUMERIC)
RETURNS NUMERIC
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pgkv, pg_temp
AS $$
DECLARE
    v_value JSONB;
    v_numeric NUMERIC;
    v_type TEXT;
    v_expires_at TIMESTAMPTZ;
BEGIN
    SELECT o_value, o_type, o_expires_at INTO v_value, v_type, v_expires_at
    FROM pgkv._lock_key(p_key, TRUE);

    IF v_type IS NULL THEN
        v_numeric := 0;
    ELSE
        IF v_type != 'string' THEN
            RAISE EXCEPTION 'WRONGTYPE Operation against a key holding the wrong kind of value';
        END IF;
        BEGIN
            v_numeric := (v_value #>> '{}')::NUMERIC;
        EXCEPTION WHEN OTHERS THEN
            RAISE EXCEPTION 'value is not a valid float';
        END;
    END IF;

    v_numeric := v_numeric + p_increment;

    PERFORM pgkv._put(p_key, to_jsonb(v_numeric), 'string', v_expires_at);

    RETURN v_numeric;
END;
$$;

-- MSETNX: Set several keys only if none of them exists. All or nothing, and atomic:
-- every key is locked (in key order) before any is written.
CREATE OR REPLACE FUNCTION pgkv.msetnx(VARIADIC p_pairs TEXT[])
RETURNS INTEGER
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pgkv, pg_temp
AS $$
DECLARE
    v_i INTEGER;
    v_key TEXT;
    v_type TEXT;
    v_keys TEXT[] := ARRAY[]::text[];
    v_exists BOOLEAN := FALSE;
BEGIN
    IF array_length(p_pairs, 1) IS NULL OR array_length(p_pairs, 1) % 2 != 0 THEN
        RAISE EXCEPTION 'ERR wrong number of arguments for MSETNX';
    END IF;

    FOR v_i IN 1..array_length(p_pairs, 1) BY 2 LOOP
        v_keys := v_keys || p_pairs[v_i];
    END LOOP;

    FOR v_key IN SELECT k FROM unnest(v_keys) AS u(k) GROUP BY k ORDER BY k COLLATE "C" LOOP
        SELECT o_type INTO v_type FROM pgkv._lock_key(v_key, TRUE);
        IF v_type IS NOT NULL THEN
            v_exists := TRUE;
            EXIT;
        END IF;
    END LOOP;

    IF v_exists THEN
        -- Remove the placeholders taken for the keys that were absent.
        DELETE FROM pgkv.store s WHERE s.key = ANY(v_keys) AND s.expires_at = '-infinity';
        RETURN 0;
    END IF;

    FOR v_i IN 1..array_length(p_pairs, 1) BY 2 LOOP
        PERFORM pgkv._put(p_pairs[v_i], to_jsonb(p_pairs[v_i + 1]), 'string', NULL);
    END LOOP;

    RETURN 1;
END;
$$;

-- ============================================================================
-- Cursor Scan Operations
-- ============================================================================

-- SCAN: Iterate keys matching a Redis glob pattern, p_count at a time.
-- The cursor is an offset into the matching keys in byte order: 0 starts, 0 returned means done.
-- Because it is an offset, keys added or removed during a sweep shift later pages; use
-- pgkv.scan_prefix for paging that is stable under writes. Expired keys are skipped, not deleted.
CREATE OR REPLACE FUNCTION pgkv.scan(p_cursor BIGINT DEFAULT 0, p_pattern TEXT DEFAULT '*', p_count INTEGER DEFAULT 10)
RETURNS TABLE(cursor BIGINT, key TEXT)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pgkv, pg_temp
AS $$
DECLARE
    v_like TEXT;
    v_keys TEXT[];
    v_next BIGINT;
BEGIN
    IF p_cursor < 0 OR p_count <= 0 THEN
        RAISE EXCEPTION 'ERR invalid cursor or count';
    END IF;

    v_like := pgkv.redis_to_pg_pattern(p_pattern);

    -- One row more than asked for tells whether another page follows, without counting the table.
    IF v_like IS NULL THEN
        -- Character classes: regex match, as pgkv.keys does.
        EXECUTE
            'SELECT ARRAY(
                 SELECT s.key FROM pgkv.store s
                 WHERE s.key ~ $3
                   AND (s.expires_at IS NULL OR s.expires_at >= clock_timestamp())
                 ORDER BY s.key USING OPERATOR(pg_catalog.~<~)
                 OFFSET $1 LIMIT $2)'
        INTO v_keys USING p_cursor, p_count + 1, p_pattern;
    ELSE
        -- The pattern goes in as a literal so the planner can turn its fixed prefix into an
        -- index range on idx_pgkv_key_pattern.
        EXECUTE format(
            'SELECT ARRAY(
                 SELECT s.key FROM pgkv.store s
                 WHERE s.key LIKE %L
                   AND (s.expires_at IS NULL OR s.expires_at >= clock_timestamp())
                 ORDER BY s.key USING OPERATOR(pg_catalog.~<~)
                 OFFSET $1 LIMIT $2)', v_like)
        INTO v_keys USING p_cursor, p_count + 1;
    END IF;

    v_next := CASE WHEN cardinality(v_keys) > p_count THEN p_cursor + p_count ELSE 0 END;

    RETURN QUERY
    SELECT v_next, u.k
    FROM unnest(v_keys[1:p_count]) WITH ORDINALITY AS u(k, ord)
    ORDER BY u.ord;
END;
$$;

CREATE OR REPLACE FUNCTION pgkv.hscan(p_key TEXT, p_cursor BIGINT DEFAULT 0, p_pattern TEXT DEFAULT '*', p_count INTEGER DEFAULT 10)
RETURNS TABLE(cursor BIGINT, field TEXT, value JSONB)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pgkv, pg_temp
AS $$
DECLARE
    v_value JSONB;
    v_type TEXT;
    v_expires_at TIMESTAMPTZ;
    v_total BIGINT;
BEGIN
    SELECT s.value, s.type, s.expires_at INTO v_value, v_type, v_expires_at FROM pgkv.store s WHERE s.key = p_key;
    IF NOT FOUND OR (v_expires_at IS NOT NULL AND v_expires_at < clock_timestamp()) THEN
        IF FOUND THEN DELETE FROM pgkv.store WHERE key = p_key AND expires_at IS NOT NULL AND expires_at < clock_timestamp(); END IF;
        RETURN;
    END IF;
    IF v_type != 'hash' THEN RAISE EXCEPTION 'WRONGTYPE Operation against a key holding the wrong kind of value'; END IF;

    SELECT COUNT(*) INTO v_total
    FROM jsonb_each(v_value) kv
    WHERE kv.key LIKE COALESCE(pgkv.redis_to_pg_pattern(p_pattern), kv.key) OR (pgkv.redis_to_pg_pattern(p_pattern) IS NULL AND kv.key ~ p_pattern);

    RETURN QUERY
    WITH matched AS (
        SELECT kv.key AS field, kv.value
        FROM jsonb_each(v_value) kv
        WHERE kv.key LIKE COALESCE(pgkv.redis_to_pg_pattern(p_pattern), kv.key) OR (pgkv.redis_to_pg_pattern(p_pattern) IS NULL AND kv.key ~ p_pattern)
        ORDER BY kv.key
        OFFSET p_cursor LIMIT p_count
    ), next_cursor AS (
        SELECT CASE WHEN p_cursor + COUNT(*) >= v_total THEN 0 ELSE p_cursor + COUNT(*) END AS c FROM matched
    )
    SELECT next_cursor.c, matched.field, matched.value FROM matched CROSS JOIN next_cursor;
END;
$$;

CREATE OR REPLACE FUNCTION pgkv.sscan(p_key TEXT, p_cursor BIGINT DEFAULT 0, p_pattern TEXT DEFAULT '*', p_count INTEGER DEFAULT 10)
RETURNS TABLE(cursor BIGINT, member JSONB)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pgkv, pg_temp
AS $$
DECLARE
    v_value JSONB;
    v_type TEXT;
    v_expires_at TIMESTAMPTZ;
    v_total BIGINT;
BEGIN
    SELECT s.value, s.type, s.expires_at INTO v_value, v_type, v_expires_at FROM pgkv.store s WHERE s.key = p_key;
    IF NOT FOUND OR (v_expires_at IS NOT NULL AND v_expires_at < clock_timestamp()) THEN
        IF FOUND THEN DELETE FROM pgkv.store WHERE key = p_key AND expires_at IS NOT NULL AND expires_at < clock_timestamp(); END IF;
        RETURN;
    END IF;
    IF v_type != 'set' THEN RAISE EXCEPTION 'WRONGTYPE Operation against a key holding the wrong kind of value'; END IF;

    SELECT COUNT(*) INTO v_total
    FROM jsonb_array_elements(v_value) e(member)
    WHERE e.member #>> '{}' LIKE COALESCE(pgkv.redis_to_pg_pattern(p_pattern), e.member #>> '{}') OR (pgkv.redis_to_pg_pattern(p_pattern) IS NULL AND e.member #>> '{}' ~ p_pattern);

    RETURN QUERY
    WITH matched AS (
        SELECT e.member
        FROM jsonb_array_elements(v_value) e(member)
        WHERE e.member #>> '{}' LIKE COALESCE(pgkv.redis_to_pg_pattern(p_pattern), e.member #>> '{}') OR (pgkv.redis_to_pg_pattern(p_pattern) IS NULL AND e.member #>> '{}' ~ p_pattern)
        ORDER BY e.member #>> '{}'
        OFFSET p_cursor LIMIT p_count
    ), next_cursor AS (
        SELECT CASE WHEN p_cursor + COUNT(*) >= v_total THEN 0 ELSE p_cursor + COUNT(*) END AS c FROM matched
    )
    SELECT next_cursor.c, matched.member FROM matched CROSS JOIN next_cursor;
END;
$$;

CREATE OR REPLACE FUNCTION pgkv.zscan(p_key TEXT, p_cursor BIGINT DEFAULT 0, p_pattern TEXT DEFAULT '*', p_count INTEGER DEFAULT 10)
RETURNS TABLE(cursor BIGINT, member TEXT, score NUMERIC)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pgkv, pg_temp
AS $$
DECLARE
    v_value JSONB;
    v_type TEXT;
    v_expires_at TIMESTAMPTZ;
    v_total BIGINT;
BEGIN
    SELECT s.value, s.type, s.expires_at INTO v_value, v_type, v_expires_at FROM pgkv.store s WHERE s.key = p_key;
    IF NOT FOUND OR (v_expires_at IS NOT NULL AND v_expires_at < clock_timestamp()) THEN
        IF FOUND THEN DELETE FROM pgkv.store WHERE key = p_key AND expires_at IS NOT NULL AND expires_at < clock_timestamp(); END IF;
        RETURN;
    END IF;
    IF v_type != 'zset' THEN RAISE EXCEPTION 'WRONGTYPE Operation against a key holding the wrong kind of value'; END IF;

    SELECT COUNT(*) INTO v_total
    FROM jsonb_each(v_value) kv
    WHERE kv.key LIKE COALESCE(pgkv.redis_to_pg_pattern(p_pattern), kv.key) OR (pgkv.redis_to_pg_pattern(p_pattern) IS NULL AND kv.key ~ p_pattern);

    RETURN QUERY
    WITH matched AS (
        SELECT kv.key AS member, (kv.value::text)::numeric AS score
        FROM jsonb_each(v_value) kv
        WHERE kv.key LIKE COALESCE(pgkv.redis_to_pg_pattern(p_pattern), kv.key) OR (pgkv.redis_to_pg_pattern(p_pattern) IS NULL AND kv.key ~ p_pattern)
        ORDER BY kv.key
        OFFSET p_cursor LIMIT p_count
    ), next_cursor AS (
        SELECT CASE WHEN p_cursor + COUNT(*) >= v_total THEN 0 ELSE p_cursor + COUNT(*) END AS c FROM matched
    )
    SELECT next_cursor.c, matched.member, matched.score FROM matched CROSS JOIN next_cursor;
END;
$$;


-- ============================================================================
-- Extended List Operations
-- ============================================================================

-- LPUSHX: Push value(s) to head of list only if the list exists
CREATE OR REPLACE FUNCTION pgkv.lpushx(p_key TEXT, VARIADIC p_values TEXT[])
RETURNS INTEGER
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pgkv, pg_temp
AS $$
DECLARE
    v_type TEXT;
BEGIN
    -- Hold the row, so the list cannot disappear between this check and the push.
    SELECT o_type INTO v_type FROM pgkv._lock_key(p_key, FALSE);
    IF v_type IS NULL THEN RETURN 0; END IF;
    IF v_type != 'list' THEN RAISE EXCEPTION 'WRONGTYPE Operation against a key holding the wrong kind of value'; END IF;
    RETURN pgkv.lpush(p_key, VARIADIC p_values);
END;
$$;

-- RPUSHX: Push value(s) to tail of list only if the list exists
CREATE OR REPLACE FUNCTION pgkv.rpushx(p_key TEXT, VARIADIC p_values TEXT[])
RETURNS INTEGER
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pgkv, pg_temp
AS $$
DECLARE
    v_type TEXT;
BEGIN
    SELECT o_type INTO v_type FROM pgkv._lock_key(p_key, FALSE);
    IF v_type IS NULL THEN RETURN 0; END IF;
    IF v_type != 'list' THEN RAISE EXCEPTION 'WRONGTYPE Operation against a key holding the wrong kind of value'; END IF;
    RETURN pgkv.rpush(p_key, VARIADIC p_values);
END;
$$;

CREATE OR REPLACE FUNCTION pgkv.linsert(p_key TEXT, p_before_after TEXT, p_pivot TEXT, p_value TEXT)
RETURNS INTEGER
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pgkv, pg_temp
AS $$
DECLARE
    v_value JSONB;
    v_type TEXT;
    v_expires_at TIMESTAMPTZ;
    v_mode TEXT := upper(p_before_after);
    v_pivot_ord INTEGER;
    v_new_value JSONB;
BEGIN
    IF v_mode NOT IN ('BEFORE', 'AFTER') THEN
        RAISE EXCEPTION 'ERR syntax error';
    END IF;

    SELECT o_value, o_type, o_expires_at INTO v_value, v_type, v_expires_at
    FROM pgkv._lock_key(p_key, FALSE);
    IF v_type IS NULL THEN RETURN -1; END IF;
    IF v_type != 'list' THEN RAISE EXCEPTION 'WRONGTYPE Operation against a key holding the wrong kind of value'; END IF;

    SELECT ord::integer INTO v_pivot_ord
    FROM jsonb_array_elements(v_value) WITH ORDINALITY AS e(elem, ord)
    WHERE elem #>> '{}' = p_pivot
    ORDER BY ord
    LIMIT 1;

    IF v_pivot_ord IS NULL THEN RETURN 0; END IF;

    WITH expanded AS (
        SELECT elem, ord::numeric AS sort_ord FROM jsonb_array_elements(v_value) WITH ORDINALITY AS e(elem, ord)
        UNION ALL
        SELECT to_jsonb(p_value), CASE WHEN v_mode = 'BEFORE' THEN v_pivot_ord - 0.5 ELSE v_pivot_ord + 0.5 END
    )
    SELECT jsonb_agg(elem ORDER BY sort_ord) INTO v_new_value FROM expanded;

    PERFORM pgkv._put(p_key, v_new_value, 'list', v_expires_at);
    RETURN jsonb_array_length(v_new_value);
END;
$$;

CREATE OR REPLACE FUNCTION pgkv.lpos(p_key TEXT, p_value TEXT)
RETURNS INTEGER
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pgkv, pg_temp
AS $$
DECLARE
    v_value JSONB;
    v_type TEXT;
    v_expires_at TIMESTAMPTZ;
    v_pos INTEGER;
BEGIN
    SELECT value, type, expires_at INTO v_value, v_type, v_expires_at FROM pgkv.store WHERE key = p_key;
    IF NOT FOUND OR (v_expires_at IS NOT NULL AND v_expires_at < clock_timestamp()) THEN
        IF FOUND THEN DELETE FROM pgkv.store WHERE key = p_key AND expires_at IS NOT NULL AND expires_at < clock_timestamp(); END IF;
        RETURN NULL;
    END IF;
    IF v_type != 'list' THEN RAISE EXCEPTION 'WRONGTYPE Operation against a key holding the wrong kind of value'; END IF;

    SELECT (ord - 1)::integer INTO v_pos
    FROM jsonb_array_elements(v_value) WITH ORDINALITY AS e(elem, ord)
    WHERE elem #>> '{}' = p_value
    ORDER BY ord
    LIMIT 1;

    RETURN v_pos;
END;
$$;

CREATE OR REPLACE FUNCTION pgkv.lmove(p_source TEXT, p_destination TEXT, p_from_side TEXT, p_to_side TEXT)
RETURNS JSONB
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pgkv, pg_temp
AS $$
DECLARE
    v_source JSONB;
    v_dest JSONB;
    v_source_type TEXT;
    v_dest_type TEXT;
    v_source_expires TIMESTAMPTZ;
    v_dest_expires TIMESTAMPTZ;
    v_elem JSONB;
    v_same BOOLEAN := p_source = p_destination;
    v_from TEXT := upper(p_from_side);
    v_to TEXT := upper(p_to_side);
BEGIN
    IF v_from NOT IN ('LEFT', 'RIGHT') OR v_to NOT IN ('LEFT', 'RIGHT') THEN
        RAISE EXCEPTION 'ERR syntax error';
    END IF;

    -- Hold both rows, taken in key order so two moves in opposite directions cannot deadlock.
    IF v_same OR p_source COLLATE "C" < p_destination COLLATE "C" THEN
        SELECT o_value, o_type, o_expires_at INTO v_source, v_source_type, v_source_expires
        FROM pgkv._lock_key(p_source, FALSE);
        IF v_source_type IS NULL THEN RETURN NULL; END IF;
        IF NOT v_same THEN
            SELECT o_value, o_type, o_expires_at INTO v_dest, v_dest_type, v_dest_expires
            FROM pgkv._lock_key(p_destination, TRUE);
        END IF;
    ELSE
        SELECT o_value, o_type, o_expires_at INTO v_dest, v_dest_type, v_dest_expires
        FROM pgkv._lock_key(p_destination, TRUE);
        SELECT o_value, o_type, o_expires_at INTO v_source, v_source_type, v_source_expires
        FROM pgkv._lock_key(p_source, FALSE);
    END IF;

    IF v_source_type IS NOT NULL AND v_source_type != 'list' THEN
        RAISE EXCEPTION 'WRONGTYPE Operation against a key holding the wrong kind of value';
    END IF;
    IF v_dest_type IS NOT NULL AND v_dest_type != 'list' THEN
        RAISE EXCEPTION 'WRONGTYPE Operation against a key holding the wrong kind of value';
    END IF;

    IF v_source_type IS NULL OR jsonb_array_length(v_source) = 0 THEN
        -- Nothing to move. Drop the placeholder taken for an absent destination.
        IF NOT v_same AND v_dest_type IS NULL THEN
            DELETE FROM pgkv.store WHERE key = p_destination;
        END IF;
        RETURN NULL;
    END IF;

    IF v_from = 'LEFT' THEN
        v_elem := v_source->0;
        SELECT COALESCE(jsonb_agg(elem ORDER BY ord), '[]'::jsonb) INTO v_source
        FROM jsonb_array_elements(v_source) WITH ORDINALITY AS e(elem, ord)
        WHERE ord > 1;
    ELSE
        v_elem := v_source->(jsonb_array_length(v_source) - 1);
        SELECT COALESCE(jsonb_agg(elem ORDER BY ord), '[]'::jsonb) INTO v_source
        FROM jsonb_array_elements(v_source) WITH ORDINALITY AS e(elem, ord)
        WHERE ord < jsonb_array_length(v_source);
    END IF;

    IF v_same THEN
        v_dest := v_source;
        v_dest_expires := v_source_expires;
    ELSIF v_dest_type IS NULL THEN
        v_dest := '[]'::jsonb;
    END IF;

    IF v_to = 'LEFT' THEN
        v_dest := jsonb_build_array(v_elem) || v_dest;
    ELSE
        v_dest := v_dest || jsonb_build_array(v_elem);
    END IF;

    IF NOT v_same THEN
        PERFORM pgkv._put(p_source, v_source, 'list', v_source_expires);
    END IF;
    PERFORM pgkv._put(p_destination, v_dest, 'list', v_dest_expires);

    RETURN v_elem;
END;
$$;

CREATE OR REPLACE FUNCTION pgkv.rpoplpush(p_source TEXT, p_destination TEXT)
RETURNS JSONB
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pgkv, pg_temp
AS $$
BEGIN
    RETURN pgkv.lmove(p_source, p_destination, 'RIGHT', 'LEFT');
END;
$$;


-- ============================================================================
-- Extended Sorted Set Operations
-- ============================================================================

CREATE OR REPLACE FUNCTION pgkv.zmscore(p_key TEXT, VARIADIC p_members TEXT[])
RETURNS TABLE(member TEXT, score NUMERIC)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pgkv, pg_temp
AS $$
DECLARE
    v_member TEXT;
BEGIN
    FOREACH v_member IN ARRAY p_members LOOP
        member := v_member;
        score := pgkv.zscore(p_key, v_member);
        RETURN NEXT;
    END LOOP;
END;
$$;

CREATE OR REPLACE FUNCTION pgkv.zpopmin(p_key TEXT, p_count INTEGER DEFAULT 1)
RETURNS TABLE(member TEXT, score NUMERIC)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pgkv, pg_temp
AS $$
DECLARE
    v_value JSONB;
    v_type TEXT;
    v_expires_at TIMESTAMPTZ;
    v_rest JSONB;
BEGIN
    IF p_count < 0 THEN RAISE EXCEPTION 'ERR value is out of range'; END IF;

    SELECT o_value, o_type, o_expires_at INTO v_value, v_type, v_expires_at
    FROM pgkv._lock_key(p_key, FALSE);
    IF v_type IS NULL THEN RETURN; END IF;
    IF v_type != 'zset' THEN RAISE EXCEPTION 'WRONGTYPE Operation against a key holding the wrong kind of value'; END IF;

    SELECT COALESCE(jsonb_object_agg(r.k, r.v), '{}'::jsonb) INTO v_rest
    FROM (
        SELECT kv.key AS k, kv.value AS v
        FROM jsonb_each(v_value) kv
        ORDER BY (kv.value::text)::numeric ASC, kv.key ASC
        OFFSET p_count
    ) r;

    PERFORM pgkv._put(p_key, v_rest, 'zset', v_expires_at);

    RETURN QUERY
    SELECT kv.key, (kv.value::text)::numeric
    FROM jsonb_each(v_value) kv
    ORDER BY (kv.value::text)::numeric ASC, kv.key ASC
    LIMIT p_count;
END;
$$;

CREATE OR REPLACE FUNCTION pgkv.zpopmax(p_key TEXT, p_count INTEGER DEFAULT 1)
RETURNS TABLE(member TEXT, score NUMERIC)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pgkv, pg_temp
AS $$
DECLARE
    v_value JSONB;
    v_type TEXT;
    v_expires_at TIMESTAMPTZ;
    v_rest JSONB;
BEGIN
    IF p_count < 0 THEN RAISE EXCEPTION 'ERR value is out of range'; END IF;

    SELECT o_value, o_type, o_expires_at INTO v_value, v_type, v_expires_at
    FROM pgkv._lock_key(p_key, FALSE);
    IF v_type IS NULL THEN RETURN; END IF;
    IF v_type != 'zset' THEN RAISE EXCEPTION 'WRONGTYPE Operation against a key holding the wrong kind of value'; END IF;

    SELECT COALESCE(jsonb_object_agg(r.k, r.v), '{}'::jsonb) INTO v_rest
    FROM (
        SELECT kv.key AS k, kv.value AS v
        FROM jsonb_each(v_value) kv
        ORDER BY (kv.value::text)::numeric DESC, kv.key ASC
        OFFSET p_count
    ) r;

    PERFORM pgkv._put(p_key, v_rest, 'zset', v_expires_at);

    RETURN QUERY
    SELECT kv.key, (kv.value::text)::numeric
    FROM jsonb_each(v_value) kv
    ORDER BY (kv.value::text)::numeric DESC, kv.key ASC
    LIMIT p_count;
END;
$$;

CREATE OR REPLACE FUNCTION pgkv.zremrangebyrank(p_key TEXT, p_start INTEGER, p_stop INTEGER)
RETURNS INTEGER
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pgkv, pg_temp
AS $$
DECLARE
    v_value JSONB;
    v_type TEXT;
    v_expires_at TIMESTAMPTZ;
    v_len INTEGER;
    v_start INTEGER;
    v_stop INTEGER;
    v_rest JSONB;
    v_kept INTEGER;
BEGIN
    SELECT o_value, o_type, o_expires_at INTO v_value, v_type, v_expires_at
    FROM pgkv._lock_key(p_key, FALSE);
    IF v_type IS NULL THEN RETURN 0; END IF;
    IF v_type != 'zset' THEN RAISE EXCEPTION 'WRONGTYPE Operation against a key holding the wrong kind of value'; END IF;

    SELECT COUNT(*)::integer INTO v_len FROM jsonb_each(v_value);
    v_start := CASE WHEN p_start < 0 THEN v_len + p_start ELSE p_start END;
    v_stop := CASE WHEN p_stop < 0 THEN v_len + p_stop ELSE p_stop END;
    v_start := GREATEST(v_start, 0);
    v_stop := LEAST(v_stop, v_len - 1);
    IF v_start > v_stop OR v_start >= v_len THEN RETURN 0; END IF;

    WITH ranked AS (
        SELECT kv.key, kv.value, ROW_NUMBER() OVER (ORDER BY (kv.value::text)::numeric ASC, kv.key ASC) - 1 AS rank
        FROM jsonb_each(v_value) kv
    )
    SELECT COALESCE(jsonb_object_agg(key, value), '{}'::jsonb), COUNT(*)::integer INTO v_rest, v_kept
    FROM ranked
    WHERE rank NOT BETWEEN v_start AND v_stop;

    PERFORM pgkv._put(p_key, v_rest, 'zset', v_expires_at);

    RETURN v_len - v_kept;
END;
$$;

CREATE OR REPLACE FUNCTION pgkv.zremrangebyscore(p_key TEXT, p_min NUMERIC, p_max NUMERIC)
RETURNS INTEGER
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pgkv, pg_temp
AS $$
DECLARE
    v_value JSONB;
    v_type TEXT;
    v_expires_at TIMESTAMPTZ;
    v_removed INTEGER;
    v_rest JSONB;
BEGIN
    SELECT o_value, o_type, o_expires_at INTO v_value, v_type, v_expires_at
    FROM pgkv._lock_key(p_key, FALSE);
    IF v_type IS NULL THEN RETURN 0; END IF;
    IF v_type != 'zset' THEN RAISE EXCEPTION 'WRONGTYPE Operation against a key holding the wrong kind of value'; END IF;

    SELECT COUNT(*)::integer INTO v_removed FROM jsonb_each(v_value) kv WHERE (kv.value::text)::numeric BETWEEN p_min AND p_max;

    SELECT COALESCE(jsonb_object_agg(kv.key, kv.value), '{}'::jsonb) INTO v_rest
    FROM jsonb_each(v_value) kv
    WHERE (kv.value::text)::numeric NOT BETWEEN p_min AND p_max;

    PERFORM pgkv._put(p_key, v_rest, 'zset', v_expires_at);

    RETURN v_removed;
END;
$$;

CREATE OR REPLACE FUNCTION pgkv.zrangestore(p_destination TEXT, p_source TEXT, p_start INTEGER, p_stop INTEGER, p_rev BOOLEAN DEFAULT FALSE)
RETURNS INTEGER
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pgkv, pg_temp
AS $$
DECLARE
    v_value JSONB;
    v_type TEXT;
    v_len INTEGER;
    v_start INTEGER;
    v_stop INTEGER;
    v_new_value JSONB;
    v_count INTEGER;
BEGIN
    -- Hold the destination and share-lock the source before reading it.
    PERFORM pgkv._lock_store_keys(p_destination, ARRAY[p_source]);

    SELECT s.value, s.type INTO v_value, v_type
    FROM pgkv.store s
    WHERE s.key = p_source AND (s.expires_at IS NULL OR s.expires_at >= clock_timestamp());

    IF NOT FOUND THEN
        DELETE FROM pgkv.store WHERE key = p_destination;
        RETURN 0;
    END IF;
    IF v_type != 'zset' THEN RAISE EXCEPTION 'WRONGTYPE Operation against a key holding the wrong kind of value'; END IF;

    SELECT COUNT(*)::integer INTO v_len FROM jsonb_each(v_value);
    v_start := CASE WHEN p_start < 0 THEN v_len + p_start ELSE p_start END;
    v_stop := CASE WHEN p_stop < 0 THEN v_len + p_stop ELSE p_stop END;
    v_start := GREATEST(v_start, 0);
    v_stop := LEAST(v_stop, v_len - 1);

    IF v_start > v_stop OR v_start >= v_len THEN
        DELETE FROM pgkv.store WHERE key = p_destination;
        RETURN 0;
    END IF;

    WITH ranked AS (
        SELECT kv.key, kv.value,
               ROW_NUMBER() OVER (ORDER BY
                   CASE WHEN NOT p_rev THEN (kv.value::text)::numeric END ASC,
                   CASE WHEN p_rev THEN (kv.value::text)::numeric END DESC,
                   kv.key ASC) - 1 AS rank
        FROM jsonb_each(v_value) kv
    ), selected AS (
        SELECT key, value FROM ranked WHERE rank BETWEEN v_start AND v_stop
    )
    SELECT COALESCE(jsonb_object_agg(key, value), '{}'::jsonb), COUNT(*)::integer INTO v_new_value, v_count FROM selected;

    PERFORM pgkv._put(p_destination, v_new_value, 'zset', NULL);

    RETURN v_count;
END;
$$;


-- ============================================================================
-- Sorted Set Algebra Store Operations
-- ============================================================================

CREATE OR REPLACE FUNCTION pgkv.zunionstore(p_destination TEXT, p_keys TEXT[], p_weights NUMERIC[] DEFAULT NULL, p_aggregate TEXT DEFAULT 'SUM')
RETURNS INTEGER
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pgkv, pg_temp
AS $$
DECLARE
    v_aggregate TEXT := upper(p_aggregate);
    v_result JSONB;
    v_count INTEGER;
BEGIN
    IF v_aggregate NOT IN ('SUM', 'MIN', 'MAX') THEN RAISE EXCEPTION 'ERR syntax error'; END IF;

    -- Hold the destination and share-lock the sources before reading them.
    PERFORM pgkv._lock_store_keys(p_destination, p_keys);

    WITH input_keys AS (
        SELECT key, ord, COALESCE(p_weights[ord], 1) AS weight FROM unnest(p_keys) WITH ORDINALITY AS k(key, ord)
    ), pairs AS (
        SELECT kv.key AS member, (kv.value::text)::numeric * ik.weight AS score
        FROM input_keys ik
        JOIN pgkv.store s ON s.key = ik.key AND s.type = 'zset' AND (s.expires_at IS NULL OR s.expires_at >= clock_timestamp())
        CROSS JOIN LATERAL jsonb_each(s.value) kv
    ), aggregated AS (
        SELECT member,
               CASE v_aggregate WHEN 'MIN' THEN MIN(score) WHEN 'MAX' THEN MAX(score) ELSE SUM(score) END AS score
        FROM pairs GROUP BY member
    )
    SELECT COALESCE(jsonb_object_agg(member, to_jsonb(score)), '{}'::jsonb), COUNT(*)::integer INTO v_result, v_count FROM aggregated;

    PERFORM pgkv._put(p_destination, v_result, 'zset', NULL);
    RETURN v_count;
END;
$$;

CREATE OR REPLACE FUNCTION pgkv.zinterstore(p_destination TEXT, p_keys TEXT[], p_weights NUMERIC[] DEFAULT NULL, p_aggregate TEXT DEFAULT 'SUM')
RETURNS INTEGER
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pgkv, pg_temp
AS $$
DECLARE
    v_aggregate TEXT := upper(p_aggregate);
    v_key_count INTEGER := COALESCE(array_length(p_keys, 1), 0);
    v_result JSONB;
    v_count INTEGER;
BEGIN
    IF v_aggregate NOT IN ('SUM', 'MIN', 'MAX') THEN RAISE EXCEPTION 'ERR syntax error'; END IF;
    IF v_key_count = 0 THEN RAISE EXCEPTION 'ERR wrong number of arguments'; END IF;

    -- Hold the destination and share-lock the sources before reading them.
    PERFORM pgkv._lock_store_keys(p_destination, p_keys);

    WITH input_keys AS (
        SELECT key, ord, COALESCE(p_weights[ord], 1) AS weight FROM unnest(p_keys) WITH ORDINALITY AS k(key, ord)
    ), pairs AS (
        SELECT kv.key AS member, (kv.value::text)::numeric * ik.weight AS score, ik.key
        FROM input_keys ik
        JOIN pgkv.store s ON s.key = ik.key AND s.type = 'zset' AND (s.expires_at IS NULL OR s.expires_at >= clock_timestamp())
        CROSS JOIN LATERAL jsonb_each(s.value) kv
    ), aggregated AS (
        SELECT member,
               CASE v_aggregate WHEN 'MIN' THEN MIN(score) WHEN 'MAX' THEN MAX(score) ELSE SUM(score) END AS score,
               COUNT(DISTINCT key) AS seen
        FROM pairs GROUP BY member
    ), selected AS (
        SELECT member, score FROM aggregated WHERE seen = v_key_count
    )
    SELECT COALESCE(jsonb_object_agg(member, to_jsonb(score)), '{}'::jsonb), COUNT(*)::integer INTO v_result, v_count FROM selected;

    PERFORM pgkv._put(p_destination, v_result, 'zset', NULL);
    RETURN v_count;
END;
$$;

CREATE OR REPLACE FUNCTION pgkv.zdiffstore(p_destination TEXT, p_keys TEXT[])
RETURNS INTEGER
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pgkv, pg_temp
AS $$
DECLARE
    v_first TEXT;
    v_rest TEXT[];
    v_result JSONB;
    v_count INTEGER;
BEGIN
    IF COALESCE(array_length(p_keys, 1), 0) = 0 THEN RAISE EXCEPTION 'ERR wrong number of arguments'; END IF;
    v_first := p_keys[1];
    v_rest := p_keys[2:array_length(p_keys, 1)];

    -- Hold the destination and share-lock the sources before reading them.
    PERFORM pgkv._lock_store_keys(p_destination, p_keys);

    WITH first_members AS (
        SELECT kv.key AS member, kv.value AS score
        FROM pgkv.store s CROSS JOIN LATERAL jsonb_each(s.value) kv
        WHERE s.key = v_first AND s.type = 'zset' AND (s.expires_at IS NULL OR s.expires_at >= clock_timestamp())
    ), rest_members AS (
        SELECT DISTINCT kv.key AS member
        FROM unnest(COALESCE(v_rest, ARRAY[]::text[])) rk
        JOIN pgkv.store s ON s.key = rk AND s.type = 'zset' AND (s.expires_at IS NULL OR s.expires_at >= clock_timestamp())
        CROSS JOIN LATERAL jsonb_each(s.value) kv
    ), selected AS (
        SELECT fm.member, fm.score FROM first_members fm WHERE NOT EXISTS (SELECT 1 FROM rest_members rm WHERE rm.member = fm.member)
    )
    SELECT COALESCE(jsonb_object_agg(member, score), '{}'::jsonb), COUNT(*)::integer INTO v_result, v_count FROM selected;

    PERFORM pgkv._put(p_destination, v_result, 'zset', NULL);
    RETURN v_count;
END;
$$;

-- ============================================================================
-- JSON Operations
-- ============================================================================

CREATE OR REPLACE FUNCTION pgkv.json_set(p_key TEXT, p_path TEXT[], p_value JSONB)
RETURNS TEXT
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pgkv, pg_temp
AS $$
DECLARE
    v_value JSONB;
    v_type TEXT;
    v_expires_at TIMESTAMPTZ;
BEGIN
    SELECT o_value, o_type, o_expires_at INTO v_value, v_type, v_expires_at
    FROM pgkv._lock_key(p_key, TRUE);

    IF v_type IS NULL THEN
        v_value := '{}'::jsonb;
    ELSIF v_type != 'json' THEN
        RAISE EXCEPTION 'WRONGTYPE Operation against a key holding the wrong kind of value';
    END IF;

    IF array_length(p_path, 1) IS NULL THEN
        v_value := p_value;
    ELSE
        v_value := jsonb_set(v_value, p_path, p_value, true);
    END IF;

    PERFORM pgkv._put(p_key, v_value, 'json', v_expires_at);
    RETURN 'OK';
END;
$$;

CREATE OR REPLACE FUNCTION pgkv.json_get(p_key TEXT, p_path TEXT[] DEFAULT NULL)
RETURNS JSONB
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pgkv, pg_temp
AS $$
DECLARE
    v_value JSONB;
    v_type TEXT;
    v_expires_at TIMESTAMPTZ;
BEGIN
    SELECT value, type, expires_at INTO v_value, v_type, v_expires_at FROM pgkv.store WHERE key = p_key;
    IF NOT FOUND OR (v_expires_at IS NOT NULL AND v_expires_at < clock_timestamp()) THEN
        IF FOUND THEN DELETE FROM pgkv.store WHERE key = p_key AND expires_at IS NOT NULL AND expires_at < clock_timestamp(); END IF;
        RETURN NULL;
    END IF;
    IF v_type != 'json' THEN RAISE EXCEPTION 'WRONGTYPE Operation against a key holding the wrong kind of value'; END IF;
    IF p_path IS NULL OR array_length(p_path, 1) IS NULL THEN RETURN v_value; END IF;
    RETURN v_value #> p_path;
END;
$$;

CREATE OR REPLACE FUNCTION pgkv.json_del(p_key TEXT, p_path TEXT[])
RETURNS INTEGER
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pgkv, pg_temp
AS $$
DECLARE
    v_value JSONB;
    v_type TEXT;
    v_expires_at TIMESTAMPTZ;
    v_parent_path TEXT[];
    v_leaf TEXT;
    v_parent JSONB;
BEGIN
    SELECT o_value, o_type, o_expires_at INTO v_value, v_type, v_expires_at
    FROM pgkv._lock_key(p_key, FALSE);
    IF v_type IS NULL THEN RETURN 0; END IF;
    IF v_type != 'json' THEN RAISE EXCEPTION 'WRONGTYPE Operation against a key holding the wrong kind of value'; END IF;
    IF p_path IS NULL OR array_length(p_path, 1) IS NULL THEN
        DELETE FROM pgkv.store WHERE key = p_key;
        RETURN 1;
    END IF;
    IF v_value #> p_path IS NULL THEN RETURN 0; END IF;

    v_leaf := p_path[array_length(p_path, 1)];
    v_parent_path := p_path[1:array_length(p_path, 1)-1];
    IF array_length(v_parent_path, 1) IS NULL THEN
        v_value := v_value - v_leaf;
    ELSE
        v_parent := (v_value #> v_parent_path) - v_leaf;
        v_value := jsonb_set(v_value, v_parent_path, v_parent, false);
    END IF;
    PERFORM pgkv._put(p_key, v_value, 'json', v_expires_at);
    RETURN 1;
END;
$$;

CREATE OR REPLACE FUNCTION pgkv.json_type(p_key TEXT, p_path TEXT[] DEFAULT NULL)
RETURNS TEXT
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pgkv, pg_temp
AS $$
DECLARE
    v_value JSONB;
BEGIN
    v_value := pgkv.json_get(p_key, p_path);
    IF v_value IS NULL THEN RETURN NULL; END IF;
    RETURN jsonb_typeof(v_value);
END;
$$;

CREATE OR REPLACE FUNCTION pgkv.json_objkeys(p_key TEXT, p_path TEXT[] DEFAULT NULL)
RETURNS SETOF TEXT
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pgkv, pg_temp
AS $$
DECLARE
    v_value JSONB;
BEGIN
    v_value := pgkv.json_get(p_key, p_path);
    IF v_value IS NULL THEN RETURN; END IF;
    IF jsonb_typeof(v_value) != 'object' THEN RAISE EXCEPTION 'WRONGTYPE JSON value is not an object'; END IF;
    RETURN QUERY SELECT key FROM jsonb_object_keys(v_value) AS key ORDER BY key;
END;
$$;

CREATE OR REPLACE FUNCTION pgkv.json_arrappend(p_key TEXT, p_path TEXT[], VARIADIC p_values JSONB[])
RETURNS INTEGER
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pgkv, pg_temp
AS $$
DECLARE
    v_value JSONB;
    v_type TEXT;
    v_expires_at TIMESTAMPTZ;
    v_array JSONB;
    v_item JSONB;
    v_root BOOLEAN := p_path IS NULL OR array_length(p_path, 1) IS NULL;
BEGIN
    SELECT o_value, o_type, o_expires_at INTO v_value, v_type, v_expires_at
    FROM pgkv._lock_key(p_key, TRUE);

    IF v_type IS NULL THEN
        v_value := '{}'::jsonb;
        v_array := '[]'::jsonb;
    ELSE
        IF v_type != 'json' THEN RAISE EXCEPTION 'WRONGTYPE Operation against a key holding the wrong kind of value'; END IF;
        v_array := CASE WHEN v_root THEN v_value ELSE v_value #> p_path END;
        IF v_array IS NULL THEN v_array := '[]'::jsonb; END IF;
    END IF;
    IF jsonb_typeof(v_array) != 'array' THEN RAISE EXCEPTION 'WRONGTYPE JSON value is not an array'; END IF;
    FOREACH v_item IN ARRAY p_values LOOP
        v_array := v_array || jsonb_build_array(v_item);
    END LOOP;

    PERFORM pgkv._put(
        p_key,
        CASE WHEN v_root THEN v_array ELSE jsonb_set(v_value, p_path, v_array, true) END,
        'json',
        v_expires_at
    );
    RETURN jsonb_array_length(v_array);
END;
$$;

-- ============================================================================
-- 0.3.0: Compare-and-set, compare-and-delete, prefix scan (pgkv extensions)
-- ============================================================================

-- CAS: Compare-and-set. Set p_key to p_value only if it currently holds p_expected,
-- compared as text (what `pgkv.get(key) #>> '{}'` returns). A NULL p_expected means
-- "only if the key does not exist" (an expired key does not exist), which is SET NX.
-- Returns 1 if it wrote, 0 if the key held something else (or, for NULL, existed).
-- Expiry options are those of set_opts; without one the new value has no expiry.
-- One statement, so of several sessions holding the same expected value exactly one wins.
-- The Redis equivalent is `SET key value IFEQ expected` (Redis 8.4); earlier Redis needs a script.
CREATE OR REPLACE FUNCTION pgkv.cas(
    p_key TEXT,
    p_expected TEXT,
    p_value TEXT,
    p_ex INTEGER DEFAULT NULL,
    p_px BIGINT DEFAULT NULL,
    p_exat BIGINT DEFAULT NULL,
    p_pxat BIGINT DEFAULT NULL,
    p_keepttl BOOLEAN DEFAULT FALSE
)
RETURNS INTEGER
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pgkv, pg_temp
AS $$
DECLARE
    v_new_expires_at TIMESTAMPTZ;
BEGIN
    v_new_expires_at := pgkv._expires_at(p_ex, p_px, p_exat, p_pxat, p_keepttl);

    IF p_expected IS NULL THEN
        INSERT INTO pgkv.store AS s (key, value, type, expires_at, updated_at)
        VALUES (p_key, to_jsonb(p_value), 'string', v_new_expires_at, NOW())
        ON CONFLICT (key) DO UPDATE
        SET value = EXCLUDED.value,
            type = EXCLUDED.type,
            expires_at = EXCLUDED.expires_at,
            created_at = NOW(),
            updated_at = NOW()
        WHERE s.expires_at IS NOT NULL AND s.expires_at < clock_timestamp();

        RETURN CASE WHEN FOUND THEN 1 ELSE 0 END;
    END IF;

    UPDATE pgkv.store s
    SET value = to_jsonb(p_value),
        expires_at = CASE WHEN p_keepttl THEN s.expires_at ELSE v_new_expires_at END,
        updated_at = NOW()
    WHERE s.key = p_key
      AND s.type = 'string'
      AND (s.expires_at IS NULL OR s.expires_at >= clock_timestamp())
      AND s.value #>> '{}' = p_expected;

    IF FOUND THEN
        RETURN 1;
    END IF;

    -- Not written. Say so, unless the key holds another type, which is an error as for get.
    PERFORM pgkv._require_string(p_key);
    RETURN 0;
END;
$$;

-- CAD: Compare-and-delete. Delete p_key only if it currently holds p_expected (compared as
-- text, as for cas). Returns 1 if it deleted, 0 otherwise. One statement.
-- The Redis equivalent is `DELEX key IFEQ expected` (Redis 8.4).
CREATE OR REPLACE FUNCTION pgkv.cad(p_key TEXT, p_expected TEXT)
RETURNS INTEGER
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pgkv, pg_temp
AS $$
BEGIN
    DELETE FROM pgkv.store s
    WHERE s.key = p_key
      AND s.type = 'string'
      AND (s.expires_at IS NULL OR s.expires_at >= clock_timestamp())
      AND s.value #>> '{}' = p_expected;

    IF FOUND THEN
        RETURN 1;
    END IF;

    PERFORM pgkv._require_string(p_key);
    RETURN 0;
END;
$$;

-- SCAN_PREFIX: List the keys that start with p_prefix (taken literally, no wildcards), in
-- byte order, p_count at a time. To page, pass the last key of the previous page as p_after;
-- a page shorter than p_count is the last. This is keyset paging: keys added or removed
-- during a sweep never shift, repeat or hide other keys. It reads an index range
-- (idx_pgkv_key_pattern), skips expired keys and writes nothing.
CREATE OR REPLACE FUNCTION pgkv.scan_prefix(p_prefix TEXT DEFAULT '', p_after TEXT DEFAULT NULL, p_count INTEGER DEFAULT 10)
RETURNS TABLE(key TEXT, type TEXT, value JSONB, expires_at TIMESTAMPTZ)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pgkv, pg_temp
AS $$
BEGIN
    IF p_count IS NULL OR p_count <= 0 THEN
        RAISE EXCEPTION 'ERR invalid count';
    END IF;

    -- Prefix and cursor go in as literals so the planner can use them as index bounds.
    RETURN QUERY EXECUTE format(
        'SELECT s.key, s.type, s.value, s.expires_at
         FROM pgkv.store s
         WHERE s.key LIKE %L
           AND %s
           AND (s.expires_at IS NULL OR s.expires_at >= clock_timestamp())
         ORDER BY s.key USING OPERATOR(pg_catalog.~<~)
         LIMIT %s',
        pgkv._like_prefix(p_prefix),
        CASE WHEN p_after IS NULL THEN 'TRUE' ELSE format('s.key OPERATOR(pg_catalog.~>~) %L', p_after) END,
        p_count
    );
END;
$$;

-- ============================================================================
-- Security: Set appropriate permissions
-- ============================================================================

-- Schema access for public
GRANT USAGE ON SCHEMA pgkv TO PUBLIC;

-- Function execution access
GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA pgkv TO PUBLIC;

-- Internal helpers (names starting with an underscore) are not part of the API: only the
-- pgkv functions call them, as the owner.
REVOKE ALL ON FUNCTION pgkv._lock_key(TEXT, BOOLEAN) FROM PUBLIC;
REVOKE ALL ON FUNCTION pgkv._put(TEXT, JSONB, TEXT, TIMESTAMPTZ) FROM PUBLIC;
REVOKE ALL ON FUNCTION pgkv._lock_store_keys(TEXT, TEXT[]) FROM PUBLIC;
REVOKE ALL ON FUNCTION pgkv._expires_at(INTEGER, BIGINT, BIGINT, BIGINT, BOOLEAN) FROM PUBLIC;
REVOKE ALL ON FUNCTION pgkv._require_string(TEXT) FROM PUBLIC;
REVOKE ALL ON FUNCTION pgkv._like_prefix(TEXT) FROM PUBLIC;

-- Explicitly prevent direct table manipulation
REVOKE ALL ON pgkv.store FROM PUBLIC;

-- Ensure future functions are accessible (critical for upgrades)
ALTER DEFAULT PRIVILEGES IN SCHEMA pgkv
    GRANT EXECUTE ON FUNCTIONS TO PUBLIC;

-- Ensure future tables remain protected
ALTER DEFAULT PRIVILEGES IN SCHEMA pgkv
    REVOKE ALL ON TABLES FROM PUBLIC;

-- Monitoring access (following pgmq pattern)
GRANT SELECT ON pgkv.store TO pg_monitor;
ALTER DEFAULT PRIVILEGES IN SCHEMA pgkv
    GRANT SELECT ON TABLES TO pg_monitor;

-- Protect sequences (if added in future)
REVOKE ALL ON ALL SEQUENCES IN SCHEMA pgkv FROM PUBLIC;
ALTER DEFAULT PRIVILEGES IN SCHEMA pgkv
    REVOKE ALL ON SEQUENCES FROM PUBLIC;
