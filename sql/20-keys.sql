-- ============================================================================
-- Keys and expiry
-- ============================================================================

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
    v_ids BIGINT[];
BEGIN
    -- Lock the rows in key order first, so two multi-key calls cannot deadlock each other.
    WITH locked AS (
        SELECT s.key FROM pgkv.store s WHERE s.key = ANY(p_keys) ORDER BY s.key FOR UPDATE
    ), deleted AS (
        DELETE FROM pgkv.store s USING locked l WHERE s.key = l.key RETURNING s.expires_at, s.id
    )
    SELECT (COUNT(*) FILTER (WHERE d.expires_at IS NULL OR d.expires_at >= clock_timestamp()))::integer,
           array_agg(d.id) FILTER (WHERE d.id IS NOT NULL)
    INTO v_deleted, v_ids
    FROM deleted d;

    IF v_ids IS NOT NULL THEN
        PERFORM pgkv._drop_many(v_ids);
    END IF;
    RETURN v_deleted;
END;
$$;

-- EXISTS: Count the keys that exist (a key named twice counts twice)
CREATE OR REPLACE FUNCTION pgkv.exists(VARIADIC p_keys TEXT[])
RETURNS INTEGER
LANGUAGE sql
STABLE
SECURITY DEFINER
SET search_path = pgkv, pg_temp
AS $$
    SELECT COUNT(*)::integer
    FROM unnest(p_keys) AS k(key)
    JOIN pgkv.store s ON s.key = k.key
    WHERE s.expires_at IS NULL OR s.expires_at >= clock_timestamp()
$$;

-- TOUCH: Count the keys that exist (pgkv keeps no access time to update)
CREATE OR REPLACE FUNCTION pgkv.touch(VARIADIC p_keys TEXT[])
RETURNS INTEGER
LANGUAGE sql
STABLE
SECURITY DEFINER
SET search_path = pgkv, pg_temp
AS $$
    SELECT pgkv.exists(VARIADIC p_keys)
$$;

-- TYPE: Get the type of value stored at key ('none' if the key does not exist)
CREATE OR REPLACE FUNCTION pgkv.type(p_key TEXT)
RETURNS TEXT
LANGUAGE sql
STABLE
SECURITY DEFINER
SET search_path = pgkv, pg_temp
AS $$
    SELECT COALESCE((
        SELECT s.type FROM pgkv.store s
        WHERE s.key = p_key AND (s.expires_at IS NULL OR s.expires_at >= clock_timestamp())
    ), 'none')
$$;

-- _EXPIRE_MODE: check the options of EXPIRE and friends (any of NX, XX, GT, LT) and return
-- them in upper case.
CREATE OR REPLACE FUNCTION pgkv._expire_mode(p_options TEXT[])
RETURNS TEXT
LANGUAGE plpgsql
IMMUTABLE
AS $$
DECLARE
    v_option TEXT;
    v_mode TEXT := '';
BEGIN
    FOREACH v_option IN ARRAY COALESCE(p_options, '{}') LOOP
        IF upper(v_option) NOT IN ('NX', 'XX', 'GT', 'LT') THEN
            RAISE EXCEPTION 'ERR Unsupported option %', v_option USING ERRCODE = 'KV000';
        END IF;
        v_mode := v_mode || upper(v_option) || ' ';
    END LOOP;
    IF v_mode ~ 'NX' AND v_mode ~ '(XX|GT|LT)' THEN
        RAISE EXCEPTION 'ERR NX and XX, GT or LT options at the same time are not compatible' USING ERRCODE = 'KV000';
    ELSIF v_mode ~ 'GT' AND v_mode ~ 'LT' THEN
        RAISE EXCEPTION 'ERR GT and LT options at the same time are not compatible' USING ERRCODE = 'KV000';
    END IF;
    RETURN v_mode;
END;
$$;

-- _EXPIRE: set a key's expiry time. p_mode holds any of NX, XX, GT, LT (Redis 7 semantics:
-- a key without expiry counts as infinite for GT and LT). A time in the past deletes the key.
CREATE OR REPLACE FUNCTION pgkv._expire(p_key TEXT, p_at TIMESTAMPTZ, p_mode TEXT)
RETURNS INTEGER
LANGUAGE plpgsql
AS $$
DECLARE
    v_locked RECORD;
    v_type TEXT;
    v_id BIGINT;
    v_current TIMESTAMPTZ;
    v_mode TEXT := pgkv._expire_mode(regexp_split_to_array(NULLIF(btrim(p_mode), ''), '\s+'));
BEGIN
    v_locked := pgkv._lock_key(p_key, FALSE);
    v_type := v_locked.o_type;
    v_id := v_locked.o_id;
    v_current := v_locked.o_expires_at;
    IF v_type IS NULL THEN
        RETURN 0;
    END IF;

    IF (v_mode ~ 'NX' AND v_current IS NOT NULL)
        OR (v_mode ~ 'XX' AND v_current IS NULL)
        OR (v_mode ~ 'GT' AND (v_current IS NULL OR p_at <= v_current))
        OR (v_mode ~ 'LT' AND v_current IS NOT NULL AND p_at >= v_current) THEN
        RETURN 0;
    END IF;

    IF p_at <= clock_timestamp() THEN
        PERFORM pgkv._drop_elems(v_id, v_type);
        DELETE FROM pgkv.store s WHERE s.key = p_key;
    ELSE
        UPDATE pgkv.store s SET expires_at = p_at, updated_at = NOW() WHERE s.key = p_key;
    END IF;
    RETURN 1;
END;
$$;

-- EXPIRE: Set a timeout on a key, in seconds. p_mode is NX, XX, GT or LT.
-- Returns 1 if the timeout was set, 0 if not (the key does not exist, or p_mode stopped it).
CREATE OR REPLACE FUNCTION pgkv.expire(p_key TEXT, p_seconds BIGINT, p_mode TEXT DEFAULT NULL)
RETURNS INTEGER
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pgkv, pg_temp
AS $$
BEGIN
    RETURN pgkv._expire(p_key, pgkv._when(p_seconds, 's', FALSE, 'expire'), p_mode);
END;
$$;

-- PEXPIRE: EXPIRE in milliseconds
CREATE OR REPLACE FUNCTION pgkv.pexpire(p_key TEXT, p_milliseconds BIGINT, p_mode TEXT DEFAULT NULL)
RETURNS INTEGER
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pgkv, pg_temp
AS $$
BEGIN
    RETURN pgkv._expire(p_key, pgkv._when(p_milliseconds, 'ms', FALSE, 'pexpire'), p_mode);
END;
$$;

-- EXPIREAT: Set the expiry of a key as a Unix time in seconds
CREATE OR REPLACE FUNCTION pgkv.expireat(p_key TEXT, p_unix_seconds BIGINT, p_mode TEXT DEFAULT NULL)
RETURNS INTEGER
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pgkv, pg_temp
AS $$
BEGIN
    RETURN pgkv._expire(p_key, pgkv._when(p_unix_seconds, 's', TRUE, 'expireat'), p_mode);
END;
$$;

-- PEXPIREAT: Set the expiry of a key as a Unix time in milliseconds
CREATE OR REPLACE FUNCTION pgkv.pexpireat(p_key TEXT, p_unix_milliseconds BIGINT, p_mode TEXT DEFAULT NULL)
RETURNS INTEGER
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pgkv, pg_temp
AS $$
BEGIN
    RETURN pgkv._expire(p_key, pgkv._when(p_unix_milliseconds, 'ms', TRUE, 'pexpireat'), p_mode);
END;
$$;

-- PERSIST: Remove the expiry from a key. Returns 1 if an expiry was removed.
CREATE OR REPLACE FUNCTION pgkv.persist(p_key TEXT)
RETURNS INTEGER
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pgkv, pg_temp
AS $$
BEGIN
    UPDATE pgkv.store s SET expires_at = NULL, updated_at = NOW()
    WHERE s.key = p_key AND s.expires_at >= clock_timestamp();
    RETURN CASE WHEN FOUND THEN 1 ELSE 0 END;
END;
$$;

-- PEXPIRETIME: The Unix time in milliseconds at which a key expires
-- Returns -2 if the key does not exist, -1 if it has no expiry
CREATE OR REPLACE FUNCTION pgkv.pexpiretime(p_key TEXT)
RETURNS BIGINT
LANGUAGE sql
STABLE
SECURITY DEFINER
SET search_path = pgkv, pg_temp
AS $$
    SELECT COALESCE((
        SELECT CASE WHEN s.expires_at IS NULL THEN -1
                    ELSE round(extract(epoch FROM s.expires_at)::numeric * 1000)::bigint END
        FROM pgkv.store s
        WHERE s.key = p_key AND (s.expires_at IS NULL OR s.expires_at >= clock_timestamp())
    ), -2)
$$;

-- EXPIRETIME: The Unix time in seconds at which a key expires (-2 no key, -1 no expiry)
CREATE OR REPLACE FUNCTION pgkv.expiretime(p_key TEXT)
RETURNS BIGINT
LANGUAGE sql
STABLE
SECURITY DEFINER
SET search_path = pgkv, pg_temp
AS $$
    SELECT CASE WHEN t < 0 THEN t ELSE t / 1000 END FROM pgkv.pexpiretime(p_key) AS t
$$;

-- PTTL: Time to live of a key in milliseconds (-2 no key, -1 no expiry)
CREATE OR REPLACE FUNCTION pgkv.pttl(p_key TEXT)
RETURNS BIGINT
LANGUAGE sql
STABLE
SECURITY DEFINER
SET search_path = pgkv, pg_temp
AS $$
    SELECT COALESCE((
        SELECT CASE WHEN s.expires_at IS NULL THEN -1
                    ELSE GREATEST(round(extract(epoch FROM s.expires_at - clock_timestamp())::numeric * 1000), 0)::bigint END
        FROM pgkv.store s
        WHERE s.key = p_key AND (s.expires_at IS NULL OR s.expires_at >= clock_timestamp())
    ), -2)
$$;

-- TTL: Time to live of a key in seconds, rounded (-2 no key, -1 no expiry)
-- The result is capped at the largest integer; pgkv.pttl has no cap.
CREATE OR REPLACE FUNCTION pgkv.ttl(p_key TEXT)
RETURNS INTEGER
LANGUAGE sql
STABLE
SECURITY DEFINER
SET search_path = pgkv, pg_temp
AS $$
    SELECT CASE WHEN t < 0 THEN t ELSE LEAST((t + 500) / 1000, 2147483647) END::integer FROM pgkv.pttl(p_key) AS t
$$;

-- KEYS: Find all keys matching a Redis glob pattern (*, ?, [abc], [a-z], [^a], \x)
-- Reads the whole table unless the pattern starts with a literal prefix.
CREATE OR REPLACE FUNCTION pgkv.keys(p_pattern TEXT DEFAULT '*')
RETURNS SETOF TEXT
LANGUAGE plpgsql
STABLE
SECURITY DEFINER
SET search_path = pgkv, pg_temp
AS $$
BEGIN
    -- The pattern goes in as a literal so the planner can turn its prefix into index bounds.
    RETURN QUERY EXECUTE format(
        'SELECT s.key::text FROM pgkv.store s
         WHERE %s AND (s.expires_at IS NULL OR s.expires_at >= clock_timestamp())
         ORDER BY s.key',
        CASE WHEN p_pattern = '*' THEN 'TRUE' ELSE format('s.key ~ %L', pgkv._glob_re(p_pattern)) END
    );
END;
$$;

-- _SCAN: one page of keys in byte order. o_next is 0 when the sweep is complete.
CREATE OR REPLACE FUNCTION pgkv._scan(p_cursor BIGINT, p_pattern TEXT, p_count BIGINT, p_type TEXT, OUT o_next BIGINT, OUT o_keys TEXT[])
LANGUAGE plpgsql
STABLE
AS $$
BEGIN
    -- Keep cursor + count inside bigint.
    p_cursor := LEAST(p_cursor, 4611686018427387904);
    p_count := LEAST(p_count, 4611686018427387904);
    EXECUTE format(
        'SELECT ARRAY(
             SELECT s.key::text FROM pgkv.store s
             WHERE %s AND %s AND (s.expires_at IS NULL OR s.expires_at >= clock_timestamp())
             ORDER BY s.key
             OFFSET $1 LIMIT $2)',
        CASE WHEN COALESCE(p_pattern, '*') = '*' THEN 'TRUE' ELSE format('s.key ~ %L', pgkv._glob_re(p_pattern)) END,
        CASE WHEN p_type IS NULL THEN 'TRUE' ELSE format('s.type = %L', lower(p_type)) END)
    INTO o_keys USING p_cursor, p_count + 1;

    IF cardinality(o_keys) > p_count THEN
        o_next := p_cursor + p_count;
        o_keys := o_keys[1:p_count];
    ELSE
        o_next := 0;
    END IF;
END;
$$;

-- SCAN: Iterate the keys matching a pattern, p_count at a time, optionally of one type.
-- Each row carries the cursor for the next call; 0 means the sweep is complete. The cursor
-- is an offset, so keys added or removed during a sweep can shift later pages:
-- pgkv.scan_prefix pages by key and does not have that problem.
CREATE OR REPLACE FUNCTION pgkv.scan(p_cursor BIGINT DEFAULT 0, p_pattern TEXT DEFAULT '*', p_count INTEGER DEFAULT 10, p_type TEXT DEFAULT NULL)
RETURNS TABLE(cursor BIGINT, key TEXT)
LANGUAGE plpgsql
STABLE
SECURITY DEFINER
SET search_path = pgkv, pg_temp
AS $$
DECLARE
    v_next BIGINT;
    v_keys TEXT[];
BEGIN
    IF p_cursor < 0 OR p_count <= 0 THEN
        PERFORM pgkv._err('ERR invalid cursor or count');
    END IF;
    SELECT s.o_next, s.o_keys INTO v_next, v_keys FROM pgkv._scan(p_cursor, p_pattern, p_count, p_type) s;
    RETURN QUERY SELECT v_next, u.k FROM unnest(v_keys) WITH ORDINALITY AS u(k, ord) ORDER BY u.ord;
END;
$$;

-- _VALUE: a key's value as one JSONB document: the stored value for a string or JSON key,
-- and for a collection its elements (hash: object, list and set: array, sorted set: object
-- of member to score, infinite scores as strings).
CREATE OR REPLACE FUNCTION pgkv._value(p_type TEXT, p_value JSONB, p_id BIGINT)
RETURNS JSONB
LANGUAGE sql
STABLE
AS $$
    SELECT CASE p_type
        WHEN 'hash' THEN (SELECT COALESCE(jsonb_object_agg(f.field, f.value), '{}'::jsonb) FROM pgkv.hash_fields f WHERE f.id = p_id)
        WHEN 'list' THEN (SELECT COALESCE(jsonb_agg(i.value ORDER BY i.pos), '[]'::jsonb) FROM pgkv.list_items i WHERE i.id = p_id)
        WHEN 'set' THEN (SELECT COALESCE(jsonb_agg(m.member ORDER BY m.member), '[]'::jsonb) FROM pgkv.set_members m WHERE m.id = p_id)
        WHEN 'zset' THEN (SELECT COALESCE(jsonb_object_agg(z.member,
                CASE WHEN z.score IN ('infinity', '-infinity') THEN to_jsonb(z.score::text) ELSE to_jsonb(z.score) END), '{}'::jsonb)
            FROM pgkv.zset_members z WHERE z.id = p_id)
        ELSE p_value
    END
$$;

-- SCAN_PREFIX: List the keys that start with p_prefix (taken literally, no wildcards), in
-- byte order, p_count at a time. To page, pass the last key of the previous page as p_after;
-- a page shorter than p_count is the last. This is keyset paging: keys added or removed
-- during a sweep never shift, repeat or hide other keys. It reads an index range, skips
-- expired keys and writes nothing. A collection's value is assembled from its elements.
CREATE OR REPLACE FUNCTION pgkv.scan_prefix(p_prefix TEXT DEFAULT '', p_after TEXT DEFAULT NULL, p_count INTEGER DEFAULT 10)
RETURNS TABLE(key TEXT, type TEXT, value JSONB, expires_at TIMESTAMPTZ)
LANGUAGE plpgsql
STABLE
SECURITY DEFINER
SET search_path = pgkv, pg_temp
AS $$
BEGIN
    IF p_count IS NULL OR p_count <= 0 THEN
        PERFORM pgkv._err('ERR invalid count');
    END IF;

    -- Prefix and cursor go in as literals so the planner can use them as index bounds.
    RETURN QUERY EXECUTE format(
        'SELECT p.key::text, p.type, pgkv._value(p.type, p.value, p.id), p.expires_at
         FROM (
             SELECT s.key, s.type, s.value, s.id, s.expires_at
             FROM pgkv.store s
             WHERE s.key LIKE %L
               AND %s
               AND (s.expires_at IS NULL OR s.expires_at >= clock_timestamp())
             ORDER BY s.key
             LIMIT %s
         ) p
         ORDER BY p.key',
        pgkv._like_prefix(p_prefix),
        CASE WHEN p_after IS NULL THEN 'TRUE' ELSE format('s.key > %L', p_after) END,
        p_count
    );
END;
$$;

-- RANDOMKEY: A random key, NULL if there are none. Reads every key.
CREATE OR REPLACE FUNCTION pgkv.randomkey()
RETURNS TEXT
LANGUAGE sql
SECURITY DEFINER
SET search_path = pgkv, pg_temp
AS $$
    SELECT s.key::text FROM pgkv.store s
    WHERE s.expires_at IS NULL OR s.expires_at >= clock_timestamp()
    ORDER BY random() LIMIT 1
$$;

-- RENAME: Rename a key, replacing p_new_key if it exists. The key keeps its expiry.
CREATE OR REPLACE FUNCTION pgkv.rename(p_key TEXT, p_new_key TEXT)
RETURNS TEXT
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pgkv, pg_temp
AS $$
DECLARE
    v_id BIGINT;
    v_type TEXT;
BEGIN
    -- The destination is reserved even when absent, so a concurrent writer of it waits here
    -- instead of being overwritten or colliding with the rename.
    PERFORM pgkv._lock_keys(ARRAY[p_key], ARRAY[p_new_key]);
    IF NOT EXISTS (SELECT 1 FROM pgkv.store s WHERE s.key = p_key AND s.expires_at IS DISTINCT FROM '-infinity') THEN
        PERFORM pgkv._err('ERR no such key');
    END IF;
    IF p_key <> p_new_key THEN
        DELETE FROM pgkv.store s WHERE s.key = p_new_key RETURNING s.id, s.type INTO v_id, v_type;
        PERFORM pgkv._drop_elems(v_id, v_type);
        UPDATE pgkv.store s SET key = p_new_key, updated_at = NOW() WHERE s.key = p_key;
    END IF;
    RETURN 'OK';
END;
$$;

-- RENAMENX: Rename a key unless p_new_key exists. Returns 1 if renamed, 0 if not.
CREATE OR REPLACE FUNCTION pgkv.renamenx(p_key TEXT, p_new_key TEXT)
RETURNS INTEGER
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pgkv, pg_temp
AS $$
BEGIN
    -- The destination is reserved even when absent, so a concurrent writer of it waits here
    -- instead of being overwritten or colliding with the rename.
    PERFORM pgkv._lock_keys(ARRAY[p_key], ARRAY[p_new_key]);
    IF NOT EXISTS (SELECT 1 FROM pgkv.store s WHERE s.key = p_key AND s.expires_at IS DISTINCT FROM '-infinity') THEN
        PERFORM pgkv._err('ERR no such key');
    END IF;
    IF EXISTS (SELECT 1 FROM pgkv.store s WHERE s.key = p_new_key AND s.expires_at IS DISTINCT FROM '-infinity') THEN
        RETURN 0;
    END IF;
    DELETE FROM pgkv.store s WHERE s.key = p_new_key;
    UPDATE pgkv.store s SET key = p_new_key, updated_at = NOW() WHERE s.key = p_key;
    RETURN 1;
END;
$$;

-- COPY: Copy a key's value and expiry to p_destination. Returns 1 if copied, 0 if the
-- source does not exist or the destination exists and p_replace is false.
CREATE OR REPLACE FUNCTION pgkv.copy(p_source TEXT, p_destination TEXT, p_replace BOOLEAN DEFAULT FALSE)
RETURNS INTEGER
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pgkv, pg_temp
AS $$
DECLARE
    v_src pgkv.store%ROWTYPE;
    v_dst pgkv.store%ROWTYPE;
    v_id BIGINT;
BEGIN
    IF p_source = p_destination THEN
        PERFORM pgkv._err('ERR source and destination objects are the same');
    END IF;
    PERFORM pgkv._lock_keys(ARRAY[p_source], ARRAY[p_destination]);
    SELECT * INTO v_src FROM pgkv.store s WHERE s.key = p_source;
    SELECT * INTO v_dst FROM pgkv.store s WHERE s.key = p_destination;

    IF v_src.key IS NULL OR v_src.expires_at = '-infinity'
        OR (v_dst.expires_at IS DISTINCT FROM '-infinity' AND NOT p_replace) THEN
        DELETE FROM pgkv.store s WHERE s.key = p_destination AND s.expires_at = '-infinity';
        RETURN 0;
    END IF;

    PERFORM pgkv._drop_elems(v_dst.id, v_dst.type);
    IF v_src.id IS NOT NULL THEN
        v_id := nextval('pgkv.collection_id');
        CASE v_src.type
            WHEN 'hash' THEN INSERT INTO pgkv.hash_fields SELECT v_id, f.field, f.value FROM pgkv.hash_fields f WHERE f.id = v_src.id;
            WHEN 'list' THEN INSERT INTO pgkv.list_items SELECT v_id, i.pos, i.value FROM pgkv.list_items i WHERE i.id = v_src.id;
            WHEN 'set' THEN INSERT INTO pgkv.set_members SELECT v_id, m.member FROM pgkv.set_members m WHERE m.id = v_src.id;
            WHEN 'zset' THEN INSERT INTO pgkv.zset_members SELECT v_id, z.member, z.score FROM pgkv.zset_members z WHERE z.id = v_src.id;
        END CASE;
    END IF;
    UPDATE pgkv.store s
    SET value = v_src.value, type = v_src.type, expires_at = v_src.expires_at,
        id = v_id, card = v_src.card, head = v_src.head, updated_at = NOW()
    WHERE s.key = p_destination;
    RETURN 1;
END;
$$;

-- CLEANUP_EXPIRED: Delete expired keys, at most p_limit of them if given. Returns the number
-- deleted. Expired keys are invisible to every command and are removed when written to, so
-- this only reclaims space: schedule it (pg_cron, for example) if keys expire unread.
-- Keys another transaction is working on are skipped rather than waited for.
CREATE OR REPLACE FUNCTION pgkv.cleanup_expired(p_limit INTEGER DEFAULT NULL)
RETURNS INTEGER
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pgkv, pg_temp
AS $$
DECLARE
    v_deleted INTEGER;
    v_ids BIGINT[];
BEGIN
    WITH expired AS (
        SELECT s.key FROM pgkv.store s
        WHERE s.expires_at IS NOT NULL AND s.expires_at < clock_timestamp()
        ORDER BY s.expires_at
        LIMIT p_limit
        FOR UPDATE SKIP LOCKED
    ), deleted AS (
        DELETE FROM pgkv.store s USING expired e WHERE s.key = e.key RETURNING s.id
    )
    SELECT COUNT(*)::integer, array_agg(d.id) FILTER (WHERE d.id IS NOT NULL) INTO v_deleted, v_ids FROM deleted d;

    IF v_ids IS NOT NULL THEN
        PERFORM pgkv._drop_many(v_ids);
    END IF;
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
    TRUNCATE TABLE pgkv.store, pgkv.hash_fields, pgkv.list_items, pgkv.set_members, pgkv.zset_members;
    RETURN 'OK';
END;
$$;

-- DBSIZE: Return the number of keys in the store. Counts every row.
CREATE OR REPLACE FUNCTION pgkv.dbsize()
RETURNS BIGINT
LANGUAGE sql
STABLE
SECURITY DEFINER
SET search_path = pgkv, pg_temp
AS $$
    SELECT COUNT(*) FROM pgkv.store s WHERE s.expires_at IS NULL OR s.expires_at >= clock_timestamp()
$$;

-- SET_UNLOGGED: Make all of pgkv's tables UNLOGGED (true) or logged again (false).
-- Unlogged tables skip the write-ahead log: writes are several times faster, but the data
-- is emptied after a crash or unclean shutdown and is not replicated to standbys. That is
-- a reasonable trade for a cache and nothing else. The tables must change together (a key's
-- row without its elements would be corrupt), which is why this is a function.
-- Rewrites the tables under an exclusive lock; the caller must own them.
CREATE OR REPLACE FUNCTION pgkv.set_unlogged(p_unlogged BOOLEAN DEFAULT TRUE)
RETURNS TEXT
LANGUAGE plpgsql
SET search_path = pgkv, pg_temp
AS $$
DECLARE
    v_table TEXT;
BEGIN
    FOREACH v_table IN ARRAY ARRAY['store', 'hash_fields', 'list_items', 'set_members', 'zset_members'] LOOP
        EXECUTE format('ALTER TABLE pgkv.%I SET %s', v_table, CASE WHEN p_unlogged THEN 'UNLOGGED' ELSE 'LOGGED' END);
    END LOOP;
    RETURN CASE WHEN p_unlogged THEN 'unlogged' ELSE 'logged' END;
END;
$$;

