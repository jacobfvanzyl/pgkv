-- Upgrade pgkv from 0.1.0 to 0.2.0

-- Harden existing SECURITY DEFINER functions against search_path hijacking.
DO $$
DECLARE
    v_function REGPROCEDURE;
BEGIN
    FOR v_function IN
        SELECT p.oid::regprocedure
        FROM pg_proc p
        JOIN pg_namespace n ON n.oid = p.pronamespace
        WHERE n.nspname = 'pgkv'
    LOOP
        EXECUTE format('ALTER FUNCTION %s SET search_path = pgkv, pg_temp', v_function);
    END LOOP;
END;
$$;

-- Add JSON as a supported storage type for 0.2.0.
ALTER TABLE pgkv.store DROP CONSTRAINT valid_type;
ALTER TABLE pgkv.store ADD CONSTRAINT valid_type CHECK (type IN ('string', 'list', 'set', 'hash', 'zset', 'json'));

-- ============================================================================
-- Extended String Operations and SET Options
-- ============================================================================

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
    v_expire_options INTEGER := 0;
    v_exists BOOLEAN := FALSE;
BEGIN
    IF p_nx AND p_xx THEN
        RAISE EXCEPTION 'ERR NX and XX options at the same time are not compatible';
    END IF;

    v_expire_options :=
        CASE WHEN p_ex IS NOT NULL THEN 1 ELSE 0 END +
        CASE WHEN p_px IS NOT NULL THEN 1 ELSE 0 END +
        CASE WHEN p_exat IS NOT NULL THEN 1 ELSE 0 END +
        CASE WHEN p_pxat IS NOT NULL THEN 1 ELSE 0 END +
        CASE WHEN p_keepttl THEN 1 ELSE 0 END;

    IF v_expire_options > 1 THEN
        RAISE EXCEPTION 'ERR syntax error: conflicting expiration options';
    END IF;

    SELECT value, type, expires_at INTO v_value, v_type, v_expires_at
    FROM pgkv.store
    WHERE key = p_key;

    IF FOUND THEN
        IF v_expires_at IS NOT NULL AND v_expires_at < clock_timestamp() THEN
            DELETE FROM pgkv.store WHERE key = p_key;
            v_value := NULL;
            v_type := NULL;
            v_expires_at := NULL;
        ELSE
            v_exists := TRUE;
        END IF;
    END IF;

    IF p_nx AND v_exists THEN
        RETURN NULL;
    END IF;
    IF p_xx AND NOT v_exists THEN
        RETURN NULL;
    END IF;

    IF p_ex IS NOT NULL THEN
        IF p_ex <= 0 THEN RAISE EXCEPTION 'ERR invalid expire time'; END IF;
        v_new_expires_at := clock_timestamp() + (p_ex || ' seconds')::interval;
    ELSIF p_px IS NOT NULL THEN
        IF p_px <= 0 THEN RAISE EXCEPTION 'ERR invalid expire time'; END IF;
        v_new_expires_at := clock_timestamp() + (p_px || ' milliseconds')::interval;
    ELSIF p_exat IS NOT NULL THEN
        v_new_expires_at := to_timestamp(p_exat);
    ELSIF p_pxat IS NOT NULL THEN
        v_new_expires_at := to_timestamp(p_pxat / 1000.0);
    ELSIF p_keepttl THEN
        v_new_expires_at := v_expires_at;
    ELSE
        v_new_expires_at := NULL;
    END IF;

    INSERT INTO pgkv.store (key, value, type, expires_at, updated_at)
    VALUES (p_key, to_jsonb(p_value), 'string', v_new_expires_at, NOW())
    ON CONFLICT (key) DO UPDATE
    SET value = EXCLUDED.value,
        type = EXCLUDED.type,
        expires_at = EXCLUDED.expires_at,
        updated_at = NOW();

    IF p_get THEN
        RETURN v_value;
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

CREATE OR REPLACE FUNCTION pgkv.getdel(p_key TEXT)
RETURNS JSONB
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pgkv, pg_temp
AS $$
DECLARE
    v_value JSONB;
BEGIN
    v_value := pgkv.get(p_key);
    IF v_value IS NOT NULL THEN
        DELETE FROM pgkv.store WHERE key = p_key AND type = 'string';
    END IF;
    RETURN v_value;
END;
$$;

CREATE OR REPLACE FUNCTION pgkv.getex(p_key TEXT, p_seconds INTEGER DEFAULT NULL)
RETURNS JSONB
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pgkv, pg_temp
AS $$
DECLARE
    v_value JSONB;
BEGIN
    v_value := pgkv.get(p_key);
    IF v_value IS NOT NULL AND p_seconds IS NOT NULL THEN
        PERFORM pgkv.expire(p_key, p_seconds);
    END IF;
    RETURN v_value;
END;
$$;

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
    PERFORM pg_advisory_xact_lock(hashtextextended('pgkv:' || p_key, 0));

    SELECT value, type, expires_at INTO v_value, v_type, v_expires_at
    FROM pgkv.store
    WHERE key = p_key;

    IF NOT FOUND OR (v_expires_at IS NOT NULL AND v_expires_at < clock_timestamp()) THEN
        IF FOUND THEN DELETE FROM pgkv.store WHERE key = p_key; END IF;
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

    INSERT INTO pgkv.store (key, value, type, updated_at)
    VALUES (p_key, to_jsonb(v_numeric), 'string', NOW())
    ON CONFLICT (key) DO UPDATE
    SET value = EXCLUDED.value,
        type = EXCLUDED.type,
        updated_at = NOW();

    RETURN v_numeric;
END;
$$;

CREATE OR REPLACE FUNCTION pgkv.msetnx(VARIADIC p_pairs TEXT[])
RETURNS INTEGER
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pgkv, pg_temp
AS $$
DECLARE
    v_i INTEGER;
BEGIN
    IF array_length(p_pairs, 1) IS NULL OR array_length(p_pairs, 1) % 2 != 0 THEN
        RAISE EXCEPTION 'ERR wrong number of arguments for MSETNX';
    END IF;

    FOR v_i IN 1..array_length(p_pairs, 1) BY 2 LOOP
        IF pgkv.exists(p_pairs[v_i]) = 1 THEN
            RETURN 0;
        END IF;
    END LOOP;

    FOR v_i IN 1..array_length(p_pairs, 1) BY 2 LOOP
        PERFORM pgkv.set(p_pairs[v_i], p_pairs[v_i + 1]);
    END LOOP;

    RETURN 1;
END;
$$;

-- ============================================================================
-- Cursor Scan Operations
-- ============================================================================

CREATE OR REPLACE FUNCTION pgkv.scan(p_cursor BIGINT DEFAULT 0, p_pattern TEXT DEFAULT '*', p_count INTEGER DEFAULT 10)
RETURNS TABLE(cursor BIGINT, key TEXT)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pgkv, pg_temp
AS $$
DECLARE
    v_total BIGINT;
BEGIN
    IF p_cursor < 0 OR p_count <= 0 THEN
        RAISE EXCEPTION 'ERR invalid cursor or count';
    END IF;

    PERFORM pgkv.cleanup_expired();

    SELECT COUNT(*) INTO v_total FROM pgkv.store s WHERE s.key IN (SELECT pgkv.keys(p_pattern));

    RETURN QUERY
    WITH matched AS (
        SELECT s.key
        FROM pgkv.store s
        WHERE s.key IN (SELECT pgkv.keys(p_pattern))
        ORDER BY s.key
        OFFSET p_cursor LIMIT p_count
    ), next_cursor AS (
        SELECT CASE WHEN p_cursor + COUNT(*) >= v_total THEN 0 ELSE p_cursor + COUNT(*) END AS c FROM matched
    )
    SELECT next_cursor.c, matched.key FROM matched CROSS JOIN next_cursor;
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
        IF FOUND THEN DELETE FROM pgkv.store WHERE key = p_key; END IF;
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
        IF FOUND THEN DELETE FROM pgkv.store WHERE key = p_key; END IF;
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
        IF FOUND THEN DELETE FROM pgkv.store WHERE key = p_key; END IF;
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

CREATE OR REPLACE FUNCTION pgkv.lpushx(p_key TEXT, VARIADIC p_values TEXT[])
RETURNS INTEGER
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pgkv, pg_temp
AS $$
DECLARE
    v_type TEXT;
BEGIN
    SELECT type INTO v_type FROM pgkv.store WHERE key = p_key AND (expires_at IS NULL OR expires_at >= clock_timestamp());
    IF NOT FOUND THEN RETURN 0; END IF;
    IF v_type != 'list' THEN RAISE EXCEPTION 'WRONGTYPE Operation against a key holding the wrong kind of value'; END IF;
    RETURN pgkv.lpush(p_key, VARIADIC p_values);
END;
$$;

CREATE OR REPLACE FUNCTION pgkv.rpushx(p_key TEXT, VARIADIC p_values TEXT[])
RETURNS INTEGER
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pgkv, pg_temp
AS $$
DECLARE
    v_type TEXT;
BEGIN
    SELECT type INTO v_type FROM pgkv.store WHERE key = p_key AND (expires_at IS NULL OR expires_at >= clock_timestamp());
    IF NOT FOUND THEN RETURN 0; END IF;
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

    SELECT value, type, expires_at INTO v_value, v_type, v_expires_at FROM pgkv.store WHERE key = p_key;
    IF NOT FOUND OR (v_expires_at IS NOT NULL AND v_expires_at < clock_timestamp()) THEN
        IF FOUND THEN DELETE FROM pgkv.store WHERE key = p_key; END IF;
        RETURN -1;
    END IF;
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

    UPDATE pgkv.store SET value = v_new_value, updated_at = NOW() WHERE key = p_key;
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
        IF FOUND THEN DELETE FROM pgkv.store WHERE key = p_key; END IF;
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
    v_from TEXT := upper(p_from_side);
    v_to TEXT := upper(p_to_side);
BEGIN
    IF v_from NOT IN ('LEFT', 'RIGHT') OR v_to NOT IN ('LEFT', 'RIGHT') THEN
        RAISE EXCEPTION 'ERR syntax error';
    END IF;

    PERFORM pg_advisory_xact_lock(hashtextextended('pgkv:list:' || LEAST(p_source, p_destination), 0));
    PERFORM pg_advisory_xact_lock(hashtextextended('pgkv:list:' || GREATEST(p_source, p_destination), 0));

    SELECT value, type, expires_at INTO v_source, v_source_type, v_source_expires FROM pgkv.store WHERE key = p_source;
    IF NOT FOUND OR (v_source_expires IS NOT NULL AND v_source_expires < clock_timestamp()) THEN
        IF FOUND THEN DELETE FROM pgkv.store WHERE key = p_source; END IF;
        RETURN NULL;
    END IF;
    IF v_source_type != 'list' THEN RAISE EXCEPTION 'WRONGTYPE Operation against a key holding the wrong kind of value'; END IF;
    IF jsonb_array_length(v_source) = 0 THEN RETURN NULL; END IF;

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

    IF p_source = p_destination THEN
        v_dest := v_source;
        v_dest_type := 'list';
        v_dest_expires := v_source_expires;
    ELSE
        SELECT value, type, expires_at INTO v_dest, v_dest_type, v_dest_expires FROM pgkv.store WHERE key = p_destination;
        IF NOT FOUND OR (v_dest_expires IS NOT NULL AND v_dest_expires < clock_timestamp()) THEN
            IF FOUND THEN DELETE FROM pgkv.store WHERE key = p_destination; END IF;
            v_dest := '[]'::jsonb;
            v_dest_type := NULL;
        END IF;
        IF v_dest_type IS NOT NULL AND v_dest_type != 'list' THEN RAISE EXCEPTION 'WRONGTYPE Operation against a key holding the wrong kind of value'; END IF;
    END IF;

    IF v_to = 'LEFT' THEN
        v_dest := jsonb_build_array(v_elem) || v_dest;
    ELSE
        v_dest := v_dest || jsonb_build_array(v_elem);
    END IF;

    UPDATE pgkv.store SET value = v_source, updated_at = NOW() WHERE key = p_source;
    INSERT INTO pgkv.store (key, value, type, expires_at, updated_at)
    VALUES (p_destination, v_dest, 'list', CASE WHEN p_source = p_destination THEN v_source_expires ELSE v_dest_expires END, NOW())
    ON CONFLICT (key) DO UPDATE
    SET value = EXCLUDED.value,
        type = 'list',
        expires_at = EXCLUDED.expires_at,
        updated_at = NOW();

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
BEGIN
    IF p_count < 0 THEN RAISE EXCEPTION 'ERR value is out of range'; END IF;
    PERFORM pg_advisory_xact_lock(hashtextextended('pgkv:zset:' || p_key, 0));
    SELECT value, type, expires_at INTO v_value, v_type, v_expires_at FROM pgkv.store WHERE key = p_key;
    IF NOT FOUND OR (v_expires_at IS NOT NULL AND v_expires_at < clock_timestamp()) THEN
        IF FOUND THEN DELETE FROM pgkv.store WHERE key = p_key; END IF;
        RETURN;
    END IF;
    IF v_type != 'zset' THEN RAISE EXCEPTION 'WRONGTYPE Operation against a key holding the wrong kind of value'; END IF;

    RETURN QUERY
    WITH picked AS (
        SELECT kv.key AS member, (kv.value::text)::numeric AS score
        FROM jsonb_each(v_value) kv
        ORDER BY (kv.value::text)::numeric ASC, kv.key ASC
        LIMIT p_count
    ), updated AS (
        UPDATE pgkv.store s
        SET value = COALESCE((SELECT jsonb_object_agg(kv.key, kv.value) FROM jsonb_each(v_value) kv WHERE NOT EXISTS (SELECT 1 FROM picked p WHERE p.member = kv.key)), '{}'::jsonb),
            updated_at = NOW()
        WHERE s.key = p_key
        RETURNING 1
    )
    SELECT picked.member, picked.score FROM picked, updated;
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
BEGIN
    IF p_count < 0 THEN RAISE EXCEPTION 'ERR value is out of range'; END IF;
    PERFORM pg_advisory_xact_lock(hashtextextended('pgkv:zset:' || p_key, 0));
    SELECT value, type, expires_at INTO v_value, v_type, v_expires_at FROM pgkv.store WHERE key = p_key;
    IF NOT FOUND OR (v_expires_at IS NOT NULL AND v_expires_at < clock_timestamp()) THEN
        IF FOUND THEN DELETE FROM pgkv.store WHERE key = p_key; END IF;
        RETURN;
    END IF;
    IF v_type != 'zset' THEN RAISE EXCEPTION 'WRONGTYPE Operation against a key holding the wrong kind of value'; END IF;

    RETURN QUERY
    WITH picked AS (
        SELECT kv.key AS member, (kv.value::text)::numeric AS score
        FROM jsonb_each(v_value) kv
        ORDER BY (kv.value::text)::numeric DESC, kv.key ASC
        LIMIT p_count
    ), updated AS (
        UPDATE pgkv.store s
        SET value = COALESCE((SELECT jsonb_object_agg(kv.key, kv.value) FROM jsonb_each(v_value) kv WHERE NOT EXISTS (SELECT 1 FROM picked p WHERE p.member = kv.key)), '{}'::jsonb),
            updated_at = NOW()
        WHERE s.key = p_key
        RETURNING 1
    )
    SELECT picked.member, picked.score FROM picked, updated;
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
    v_removed INTEGER;
BEGIN
    SELECT value, type, expires_at INTO v_value, v_type, v_expires_at FROM pgkv.store WHERE key = p_key;
    IF NOT FOUND OR (v_expires_at IS NOT NULL AND v_expires_at < clock_timestamp()) THEN
        IF FOUND THEN DELETE FROM pgkv.store WHERE key = p_key; END IF;
        RETURN 0;
    END IF;
    IF v_type != 'zset' THEN RAISE EXCEPTION 'WRONGTYPE Operation against a key holding the wrong kind of value'; END IF;

    SELECT COUNT(*)::integer INTO v_len FROM jsonb_each(v_value);
    v_start := CASE WHEN p_start < 0 THEN v_len + p_start ELSE p_start END;
    v_stop := CASE WHEN p_stop < 0 THEN v_len + p_stop ELSE p_stop END;
    v_start := GREATEST(v_start, 0);
    v_stop := LEAST(v_stop, v_len - 1);
    IF v_start > v_stop OR v_start >= v_len THEN RETURN 0; END IF;

    WITH ranked AS (
        SELECT kv.key, ROW_NUMBER() OVER (ORDER BY (kv.value::text)::numeric ASC, kv.key ASC) - 1 AS rank
        FROM jsonb_each(v_value) kv
    ), removed AS (
        SELECT key FROM ranked WHERE rank BETWEEN v_start AND v_stop
    )
    SELECT COUNT(*)::integer INTO v_removed FROM removed;

    WITH ranked AS (
        SELECT kv.key, kv.value, ROW_NUMBER() OVER (ORDER BY (kv.value::text)::numeric ASC, kv.key ASC) - 1 AS rank
        FROM jsonb_each(v_value) kv
    )
    UPDATE pgkv.store
    SET value = COALESCE((SELECT jsonb_object_agg(key, value) FROM ranked WHERE rank NOT BETWEEN v_start AND v_stop), '{}'::jsonb),
        updated_at = NOW()
    WHERE key = p_key;

    RETURN v_removed;
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
BEGIN
    SELECT value, type, expires_at INTO v_value, v_type, v_expires_at FROM pgkv.store WHERE key = p_key;
    IF NOT FOUND OR (v_expires_at IS NOT NULL AND v_expires_at < clock_timestamp()) THEN
        IF FOUND THEN DELETE FROM pgkv.store WHERE key = p_key; END IF;
        RETURN 0;
    END IF;
    IF v_type != 'zset' THEN RAISE EXCEPTION 'WRONGTYPE Operation against a key holding the wrong kind of value'; END IF;

    SELECT COUNT(*)::integer INTO v_removed FROM jsonb_each(v_value) kv WHERE (kv.value::text)::numeric BETWEEN p_min AND p_max;

    UPDATE pgkv.store
    SET value = COALESCE((SELECT jsonb_object_agg(kv.key, kv.value) FROM jsonb_each(v_value) kv WHERE (kv.value::text)::numeric NOT BETWEEN p_min AND p_max), '{}'::jsonb),
        updated_at = NOW()
    WHERE key = p_key;

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
    v_expires_at TIMESTAMPTZ;
    v_len INTEGER;
    v_start INTEGER;
    v_stop INTEGER;
    v_new_value JSONB;
    v_count INTEGER;
BEGIN
    SELECT value, type, expires_at INTO v_value, v_type, v_expires_at FROM pgkv.store WHERE key = p_source;
    IF NOT FOUND OR (v_expires_at IS NOT NULL AND v_expires_at < clock_timestamp()) THEN
        IF FOUND THEN DELETE FROM pgkv.store WHERE key = p_source; END IF;
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

    INSERT INTO pgkv.store (key, value, type, updated_at)
    VALUES (p_destination, v_new_value, 'zset', NOW())
    ON CONFLICT (key) DO UPDATE
    SET value = EXCLUDED.value,
        type = 'zset',
        expires_at = NULL,
        updated_at = NOW();

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

    INSERT INTO pgkv.store (key, value, type, updated_at)
    VALUES (p_destination, v_result, 'zset', NOW())
    ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value, type = 'zset', expires_at = NULL, updated_at = NOW();
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

    INSERT INTO pgkv.store (key, value, type, updated_at)
    VALUES (p_destination, v_result, 'zset', NOW())
    ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value, type = 'zset', expires_at = NULL, updated_at = NOW();
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

    INSERT INTO pgkv.store (key, value, type, updated_at)
    VALUES (p_destination, v_result, 'zset', NOW())
    ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value, type = 'zset', expires_at = NULL, updated_at = NOW();
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
    SELECT value, type, expires_at INTO v_value, v_type, v_expires_at FROM pgkv.store WHERE key = p_key;
    IF NOT FOUND OR (v_expires_at IS NOT NULL AND v_expires_at < clock_timestamp()) THEN
        IF FOUND THEN DELETE FROM pgkv.store WHERE key = p_key; END IF;
        v_value := CASE WHEN array_length(p_path, 1) IS NULL THEN p_value ELSE '{}'::jsonb END;
    ELSE
        IF v_type != 'json' THEN RAISE EXCEPTION 'WRONGTYPE Operation against a key holding the wrong kind of value'; END IF;
    END IF;

    IF array_length(p_path, 1) IS NULL THEN
        v_value := p_value;
    ELSE
        v_value := jsonb_set(v_value, p_path, p_value, true);
    END IF;

    INSERT INTO pgkv.store (key, value, type, updated_at)
    VALUES (p_key, v_value, 'json', NOW())
    ON CONFLICT (key) DO UPDATE
    SET value = EXCLUDED.value,
        type = 'json',
        updated_at = NOW();
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
        IF FOUND THEN DELETE FROM pgkv.store WHERE key = p_key; END IF;
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
    SELECT value, type, expires_at INTO v_value, v_type, v_expires_at FROM pgkv.store WHERE key = p_key;
    IF NOT FOUND OR (v_expires_at IS NOT NULL AND v_expires_at < clock_timestamp()) THEN
        IF FOUND THEN DELETE FROM pgkv.store WHERE key = p_key; END IF;
        RETURN 0;
    END IF;
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
    UPDATE pgkv.store SET value = v_value, updated_at = NOW() WHERE key = p_key;
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
    v_array JSONB;
    v_item JSONB;
BEGIN
    v_value := pgkv.json_get(p_key, NULL);
    IF v_value IS NULL THEN
        v_value := '{}'::jsonb;
        v_array := '[]'::jsonb;
    ELSE
        v_array := CASE WHEN p_path IS NULL OR array_length(p_path, 1) IS NULL THEN v_value ELSE v_value #> p_path END;
        IF v_array IS NULL THEN v_array := '[]'::jsonb; END IF;
    END IF;
    IF jsonb_typeof(v_array) != 'array' THEN RAISE EXCEPTION 'WRONGTYPE JSON value is not an array'; END IF;
    FOREACH v_item IN ARRAY p_values LOOP
        v_array := v_array || jsonb_build_array(v_item);
    END LOOP;
    PERFORM pgkv.json_set(p_key, p_path, v_array);
    RETURN jsonb_array_length(v_array);
END;
$$;


-- Function execution access for newly added functions.
GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA pgkv TO PUBLIC;
