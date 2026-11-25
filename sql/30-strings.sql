-- ============================================================================
-- Strings
--
-- A string value is stored as JSONB: a JSON string, or a JSON number after INCR-style
-- commands. Functions that return the value return that JSONB; `#>> '{}'` gives its text.
-- Lengths and offsets are in bytes of the UTF-8 encoding, as in Redis.
-- ============================================================================

-- _REPLACE: slow path of SET for a key that holds a collection (or an expired one).
CREATE OR REPLACE FUNCTION pgkv._replace(p_key TEXT, p_value JSONB, p_expires_at TIMESTAMPTZ, p_keepttl BOOLEAN)
RETURNS VOID
LANGUAGE plpgsql
AS $$
DECLARE
    v_locked RECORD;
    v_type TEXT;
    v_id BIGINT;
    v_expires_at TIMESTAMPTZ;
BEGIN
    v_locked := pgkv._lock_key(p_key, TRUE);
    v_type := v_locked.o_type;
    v_id := v_locked.o_id;
    v_expires_at := v_locked.o_expires_at;
    PERFORM pgkv._drop_elems(v_id, v_type);
    PERFORM pgkv._put(p_key, p_value, 'string', CASE WHEN p_keepttl THEN v_expires_at ELSE p_expires_at END);
END;
$$;

-- _SET: write a string unconditionally. One statement unless the key holds a collection.
CREATE OR REPLACE FUNCTION pgkv._set(p_key TEXT, p_value JSONB, p_expires_at TIMESTAMPTZ, p_keepttl BOOLEAN)
RETURNS VOID
LANGUAGE plpgsql
AS $$
BEGIN
    INSERT INTO pgkv.store AS s (key, value, type, expires_at, updated_at)
    VALUES (p_key, p_value, 'string', p_expires_at, NOW())
    ON CONFLICT (key) DO UPDATE
    SET value = EXCLUDED.value,
        type = 'string',
        expires_at = CASE
            WHEN p_keepttl AND (s.expires_at IS NULL OR s.expires_at >= clock_timestamp()) THEN s.expires_at
            ELSE EXCLUDED.expires_at
        END,
        updated_at = NOW()
    WHERE s.id IS NULL;

    IF NOT FOUND THEN
        PERFORM pgkv._replace(p_key, p_value, p_expires_at, p_keepttl);
    END IF;
END;
$$;

-- SET: Set a key to a string value, with an optional TTL (time-to-live in seconds)
CREATE OR REPLACE FUNCTION pgkv.set(p_key TEXT, p_value TEXT, p_ttl_seconds INTEGER DEFAULT NULL)
RETURNS TEXT
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pgkv, pg_temp
AS $$
DECLARE
    v_expires_at TIMESTAMPTZ := clock_timestamp() + make_interval(secs => p_ttl_seconds);
BEGIN
    INSERT INTO pgkv.store AS s (key, value, type, expires_at, updated_at)
    VALUES (p_key, to_jsonb(p_value), 'string', v_expires_at, NOW())
    ON CONFLICT (key) DO UPDATE
    SET value = EXCLUDED.value,
        type = 'string',
        expires_at = EXCLUDED.expires_at,
        updated_at = NOW()
    WHERE s.id IS NULL;

    IF NOT FOUND THEN
        PERFORM pgkv._replace(p_key, to_jsonb(p_value), v_expires_at, FALSE);
    END IF;
    RETURN 'OK';
END;
$$;

-- GET: Retrieve the value of a key (NULL if expired or not found)
-- Returns the JSONB value; raises WRONGTYPE if the key does not hold a string
CREATE OR REPLACE FUNCTION pgkv.get(p_key TEXT)
RETURNS JSONB
LANGUAGE plpgsql
STABLE
SECURITY DEFINER
SET search_path = pgkv, pg_temp
AS $$
DECLARE
    v_value JSONB;
    v_type TEXT;
BEGIN
    SELECT s.value, s.type INTO v_value, v_type
    FROM pgkv.store s
    WHERE s.key = p_key AND (s.expires_at IS NULL OR s.expires_at >= clock_timestamp());

    IF v_type <> 'string' THEN
        PERFORM pgkv._wrongtype();
    END IF;
    RETURN v_value;
END;
$$;

-- SET_OPTS: SET with Redis options (NX, XX, GET, EX, PX, EXAT, PXAT, KEEPTTL)
-- Returns "OK" when the key was set, NULL when NX or XX stopped it.
-- With p_get: the value the key held before (NULL if it did not exist), whether or not the
-- key was set; WRONGTYPE if it holds something other than a string.
-- An expired key counts as absent. Each form is atomic.
CREATE OR REPLACE FUNCTION pgkv.set_opts(
    p_key TEXT,
    p_value TEXT,
    p_nx BOOLEAN DEFAULT FALSE,
    p_xx BOOLEAN DEFAULT FALSE,
    p_get BOOLEAN DEFAULT FALSE,
    p_ex BIGINT DEFAULT NULL,
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
    v_locked RECORD;
    v_value JSONB;
    v_type TEXT;
    v_id BIGINT;
    v_expires_at TIMESTAMPTZ;
    v_new_expires_at TIMESTAMPTZ;
BEGIN
    IF p_nx AND p_xx THEN
        PERFORM pgkv._err('ERR syntax error');
    END IF;

    v_new_expires_at := pgkv._expires_at(p_ex, p_px, p_exat, p_pxat, p_keepttl);

    IF NOT (p_nx OR p_xx OR p_get) THEN
        PERFORM pgkv._set(p_key, to_jsonb(p_value), v_new_expires_at, p_keepttl);
        RETURN to_jsonb('OK'::text);
    END IF;

    IF p_nx AND NOT p_get THEN
        -- Fast path: the key is not there at all.
        INSERT INTO pgkv.store (key, value, type, expires_at, updated_at)
        VALUES (p_key, to_jsonb(p_value), 'string', v_new_expires_at, NOW())
        ON CONFLICT (key) DO NOTHING;
        IF FOUND THEN
            RETURN to_jsonb('OK'::text);
        END IF;
    END IF;

    v_locked := pgkv._lock_key(p_key, NOT p_xx);
    v_value := v_locked.o_value;
    v_type := v_locked.o_type;
    v_id := v_locked.o_id;
    v_expires_at := v_locked.o_expires_at;

    IF p_get AND v_type IS NOT NULL AND v_type <> 'string' THEN
        PERFORM pgkv._wrongtype();
    END IF;

    IF (p_nx AND v_type IS NOT NULL) OR (p_xx AND v_type IS NULL) THEN
        RETURN CASE WHEN p_get THEN v_value END;
    END IF;

    PERFORM pgkv._drop_elems(v_id, v_type);
    PERFORM pgkv._put(p_key, to_jsonb(p_value), 'string', CASE WHEN p_keepttl THEN v_expires_at ELSE v_new_expires_at END);
    RETURN CASE WHEN p_get THEN v_value ELSE to_jsonb('OK'::text) END;
END;
$$;

-- SETNX: Set key only if it does not already exist. Returns 1 if set, 0 if not.
CREATE OR REPLACE FUNCTION pgkv.setnx(p_key TEXT, p_value TEXT)
RETURNS INTEGER
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pgkv, pg_temp
AS $$
BEGIN
    RETURN CASE WHEN pgkv.set_opts(p_key, p_value, p_nx => TRUE) IS NULL THEN 0 ELSE 1 END;
END;
$$;

-- SETEX: Set key with an expiry in seconds
CREATE OR REPLACE FUNCTION pgkv.setex(p_key TEXT, p_seconds INTEGER, p_value TEXT)
RETURNS TEXT
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pgkv, pg_temp
AS $$
BEGIN
    PERFORM pgkv._set(p_key, to_jsonb(p_value), pgkv._expires_at(p_seconds, NULL, NULL, NULL, FALSE, 'setex'), FALSE);
    RETURN 'OK';
END;
$$;

-- PSETEX: Set key with an expiry in milliseconds
CREATE OR REPLACE FUNCTION pgkv.psetex(p_key TEXT, p_milliseconds BIGINT, p_value TEXT)
RETURNS TEXT
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pgkv, pg_temp
AS $$
BEGIN
    PERFORM pgkv._set(p_key, to_jsonb(p_value), pgkv._expires_at(NULL, p_milliseconds, NULL, NULL, FALSE, 'psetex'), FALSE);
    RETURN 'OK';
END;
$$;

-- GETSET: Set a key and return the value it held (the expiry is cleared)
CREATE OR REPLACE FUNCTION pgkv.getset(p_key TEXT, p_value TEXT)
RETURNS JSONB
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pgkv, pg_temp
AS $$
BEGIN
    RETURN pgkv.set_opts(p_key, p_value, p_get => TRUE);
END;
$$;

-- GETDEL: Get the value of a key and delete it
CREATE OR REPLACE FUNCTION pgkv.getdel(p_key TEXT)
RETURNS JSONB
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pgkv, pg_temp
AS $$
DECLARE
    v_locked RECORD;
    v_value JSONB;
    v_type TEXT;
BEGIN
    v_locked := pgkv._lock_key(p_key, FALSE);
    v_value := v_locked.o_value;
    v_type := v_locked.o_type;
    IF v_type IS NULL THEN
        RETURN NULL;
    END IF;
    IF v_type <> 'string' THEN
        PERFORM pgkv._wrongtype();
    END IF;
    DELETE FROM pgkv.store s WHERE s.key = p_key;
    RETURN v_value;
END;
$$;

-- GETEX: Get the value of a key and set its expiry (p_ex seconds, p_px milliseconds,
-- p_exat / p_pxat Unix time) or remove it (p_persist). With no option it is GET.
CREATE OR REPLACE FUNCTION pgkv.getex(
    p_key TEXT,
    p_ex BIGINT DEFAULT NULL,
    p_px BIGINT DEFAULT NULL,
    p_exat BIGINT DEFAULT NULL,
    p_pxat BIGINT DEFAULT NULL,
    p_persist BOOLEAN DEFAULT FALSE
)
RETURNS JSONB
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pgkv, pg_temp
AS $$
DECLARE
    v_locked RECORD;
    v_value JSONB;
    v_type TEXT;
    v_at TIMESTAMPTZ;
BEGIN
    v_at := pgkv._expires_at(p_ex, p_px, p_exat, p_pxat, p_persist, 'getex');
    IF v_at IS NULL AND NOT p_persist THEN
        RETURN pgkv.get(p_key);
    END IF;

    v_locked := pgkv._lock_key(p_key, FALSE);
    v_value := v_locked.o_value;
    v_type := v_locked.o_type;
    IF v_type IS NULL THEN
        RETURN NULL;
    END IF;
    IF v_type <> 'string' THEN
        PERFORM pgkv._wrongtype();
    END IF;
    IF v_at <= clock_timestamp() THEN
        DELETE FROM pgkv.store s WHERE s.key = p_key;
    ELSE
        UPDATE pgkv.store s SET expires_at = v_at, updated_at = NOW() WHERE s.key = p_key;
    END IF;
    RETURN v_value;
END;
$$;

-- INCRBY: Increment the integer value of a key by the given amount
-- Stores the result as a JSONB number
CREATE OR REPLACE FUNCTION pgkv.incrby(p_key TEXT, p_increment BIGINT)
RETURNS BIGINT
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pgkv, pg_temp
AS $$
DECLARE
    v_locked RECORD;
    v_value JSONB;
    v_type TEXT;
    v_expires_at TIMESTAMPTZ;
    v_result NUMERIC;
BEGIN
    v_locked := pgkv._lock_key(p_key, TRUE);
    v_value := v_locked.o_value;
    v_type := v_locked.o_type;
    v_expires_at := v_locked.o_expires_at;

    IF v_type IS NULL THEN
        v_result := p_increment;
    ELSIF v_type <> 'string' THEN
        PERFORM pgkv._wrongtype();
    ELSE
        v_result := pgkv._int(v_value #>> '{}')::numeric + p_increment;
    END IF;

    IF v_result NOT BETWEEN -9223372036854775808 AND 9223372036854775807 THEN
        PERFORM pgkv._err('ERR increment or decrement would overflow');
    END IF;

    PERFORM pgkv._put(p_key, to_jsonb(v_result::bigint), 'string', v_expires_at);
    RETURN v_result::bigint;
END;
$$;

-- INCR: Increment the integer value of a key by 1
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

-- DECRBY: Decrement the integer value of a key by the given amount
CREATE OR REPLACE FUNCTION pgkv.decrby(p_key TEXT, p_decrement BIGINT)
RETURNS BIGINT
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pgkv, pg_temp
AS $$
BEGIN
    IF p_decrement = -9223372036854775808 THEN
        PERFORM pgkv._err('ERR decrement would overflow');
    END IF;
    RETURN pgkv.incrby(p_key, -p_decrement);
END;
$$;

-- _INCRBYFLOAT: INCRBYFLOAT with the increment as text, checked in the order Redis checks:
-- the key's type, the stored value, then the increment.
CREATE OR REPLACE FUNCTION pgkv._incrbyfloat(p_key TEXT, p_increment TEXT)
RETURNS NUMERIC
LANGUAGE plpgsql
AS $$
DECLARE
    v_locked RECORD;
    v_value JSONB;
    v_type TEXT;
    v_expires_at TIMESTAMPTZ;
    v_current NUMERIC := 0;
    v_increment NUMERIC;
    v_inf INTEGER := 0;
    v_inf2 INTEGER;
    v_result NUMERIC;
BEGIN
    v_locked := pgkv._lock_key(p_key, TRUE);
    v_value := v_locked.o_value;
    v_type := v_locked.o_type;
    v_expires_at := v_locked.o_expires_at;

    IF v_type IS NOT NULL THEN
        IF v_type <> 'string' THEN
            PERFORM pgkv._wrongtype();
        END IF;
        SELECT d.o_value, d.o_infinite INTO v_current, v_inf FROM pgkv._decimal(v_value #>> '{}') d;
    END IF;
    SELECT d.o_value, d.o_infinite INTO v_increment, v_inf2 FROM pgkv._decimal(p_increment) d;
    IF v_current IS NULL OR v_increment IS NULL THEN
        PERFORM pgkv._err('ERR value is not a valid float');
    END IF;

    v_result := v_current + v_increment;
    IF v_inf <> 0 OR v_inf2 <> 0 OR abs(v_result) > 1.18973149535723176e4932 THEN
        PERFORM pgkv._err('ERR increment would produce NaN or Infinity');
    END IF;

    v_result := pgkv._num_text(pgkv._round17(v_result))::numeric;
    PERFORM pgkv._put(p_key, to_jsonb(v_result), 'string', v_expires_at);
    RETURN v_result;
END;
$$;

-- INCRBYFLOAT: Increment the numeric value of a key by a decimal amount. Exact decimal
-- arithmetic, where Redis uses long double: results can differ past the 17th digit.
CREATE OR REPLACE FUNCTION pgkv.incrbyfloat(p_key TEXT, p_increment NUMERIC)
RETURNS NUMERIC
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pgkv, pg_temp
AS $$
BEGIN
    RETURN pgkv._incrbyfloat(p_key, p_increment::text);
END;
$$;

-- APPEND: Append a value to a key's string, creating the key if needed
-- Returns the length of the string after the append, in bytes
CREATE OR REPLACE FUNCTION pgkv.append(p_key TEXT, p_value TEXT)
RETURNS INTEGER
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pgkv, pg_temp
AS $$
DECLARE
    v_locked RECORD;
    v_value JSONB;
    v_type TEXT;
    v_expires_at TIMESTAMPTZ;
    v_text TEXT;
BEGIN
    v_locked := pgkv._lock_key(p_key, TRUE);
    v_value := v_locked.o_value;
    v_type := v_locked.o_type;
    v_expires_at := v_locked.o_expires_at;

    IF v_type IS NOT NULL AND v_type <> 'string' THEN
        PERFORM pgkv._wrongtype();
    END IF;

    v_text := COALESCE(v_value #>> '{}', '') || p_value;
    PERFORM pgkv._put(p_key, to_jsonb(v_text), 'string', v_expires_at);
    RETURN octet_length(v_text);
END;
$$;

-- STRLEN: Length of the string stored at key, in bytes (0 if the key does not exist)
CREATE OR REPLACE FUNCTION pgkv.strlen(p_key TEXT)
RETURNS INTEGER
LANGUAGE plpgsql
STABLE
SECURITY DEFINER
SET search_path = pgkv, pg_temp
AS $$
BEGIN
    RETURN COALESCE(octet_length(pgkv.get(p_key) #>> '{}'), 0);
END;
$$;

-- _SLICE: bytes p_from .. p_to (zero-based, inclusive) of a text value. A range that would
-- cut a multi-byte character in half is an error: pgkv stores text, not bytes.
CREATE OR REPLACE FUNCTION pgkv._slice(p_bytes BYTEA, p_from BIGINT, p_to BIGINT, p_command TEXT)
RETURNS TEXT
LANGUAGE plpgsql
IMMUTABLE
AS $$
DECLARE
    v_len BIGINT := length(p_bytes);
BEGIN
    IF p_from > p_to THEN
        RETURN '';
    END IF;
    IF (get_byte(p_bytes, p_from::integer) & 192) = 128
        OR (p_to + 1 < v_len AND (get_byte(p_bytes, (p_to + 1)::integer) & 192) = 128) THEN
        RAISE EXCEPTION 'ERR % range splits a multi-byte UTF-8 character (pgkv stores text, not bytes)', p_command
            USING ERRCODE = 'KV000';
    END IF;
    RETURN convert_from(substring(p_bytes FROM (p_from + 1)::integer FOR (p_to - p_from + 1)::integer), 'UTF8');
END;
$$;

-- GETRANGE: Substring of the string stored at key, by byte offsets (negative from the end)
CREATE OR REPLACE FUNCTION pgkv.getrange(p_key TEXT, p_start BIGINT, p_end BIGINT)
RETURNS TEXT
LANGUAGE plpgsql
STABLE
SECURITY DEFINER
SET search_path = pgkv, pg_temp
AS $$
DECLARE
    v_bytes BYTEA := convert_to(COALESCE(pgkv.get(p_key) #>> '{}', ''), 'UTF8');
    v_len BIGINT := length(v_bytes);
    v_start NUMERIC := p_start;
    v_end NUMERIC := p_end;
BEGIN
    IF v_start < 0 AND v_end < 0 AND v_start > v_end THEN
        RETURN '';
    END IF;
    IF v_start < 0 THEN v_start := GREATEST(v_len + v_start, 0); END IF;
    IF v_end < 0 THEN v_end := GREATEST(v_len + v_end, 0); END IF;
    IF v_end >= v_len THEN v_end := v_len - 1; END IF;
    IF v_len = 0 OR v_start > v_end THEN
        RETURN '';
    END IF;
    RETURN pgkv._slice(v_bytes, v_start::bigint, v_end::bigint, 'GETRANGE');
END;
$$;

-- SETRANGE: Overwrite part of the string stored at key, starting at a byte offset
-- Returns the length of the string afterwards, in bytes. Writing past the end would need
-- zero-byte padding, which a text value cannot hold, and is an error.
CREATE OR REPLACE FUNCTION pgkv.setrange(p_key TEXT, p_offset BIGINT, p_value TEXT)
RETURNS INTEGER
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pgkv, pg_temp
AS $$
DECLARE
    v_locked RECORD;
    v_value JSONB;
    v_type TEXT;
    v_expires_at TIMESTAMPTZ;
    v_old BYTEA;
    v_new BYTEA := convert_to(p_value, 'UTF8');
    v_len BIGINT;
    v_text TEXT;
BEGIN
    IF p_offset < 0 THEN
        PERFORM pgkv._err('ERR offset is out of range');
    END IF;
    IF p_offset::numeric + length(v_new) > 536870912 THEN
        PERFORM pgkv._err('ERR string exceeds maximum allowed size (proto-max-bulk-len)');
    END IF;

    v_locked := pgkv._lock_key(p_key, length(v_new) > 0);
    v_value := v_locked.o_value;
    v_type := v_locked.o_type;
    v_expires_at := v_locked.o_expires_at;

    IF v_type IS NOT NULL AND v_type <> 'string' THEN
        PERFORM pgkv._wrongtype();
    END IF;

    v_old := convert_to(COALESCE(v_value #>> '{}', ''), 'UTF8');
    v_len := length(v_old);
    IF length(v_new) = 0 THEN
        RETURN v_len;
    END IF;
    IF p_offset > v_len THEN
        PERFORM pgkv._err('ERR SETRANGE past the end of the string needs zero-byte padding, which pgkv text values cannot hold');
    END IF;

    v_text := pgkv._slice(v_old, 0, p_offset - 1, 'SETRANGE') || p_value
        || CASE WHEN p_offset + length(v_new) < v_len
                THEN pgkv._slice(v_old, p_offset + length(v_new), v_len - 1, 'SETRANGE') ELSE '' END;
    PERFORM pgkv._put(p_key, to_jsonb(v_text), 'string', v_expires_at);
    RETURN octet_length(v_text);
END;
$$;

-- MGET: Get the values of all specified keys, in order
-- value is NULL for a key that does not exist or does not hold a string
CREATE OR REPLACE FUNCTION pgkv.mget(VARIADIC p_keys TEXT[])
RETURNS TABLE(key TEXT, value JSONB)
LANGUAGE sql
STABLE
SECURITY DEFINER
SET search_path = pgkv, pg_temp
AS $$
    SELECT k.key, s.value
    FROM unnest(p_keys) WITH ORDINALITY AS k(key, ord)
    LEFT JOIN pgkv.store s
        ON s.key = k.key AND s.type = 'string'
        AND (s.expires_at IS NULL OR s.expires_at >= clock_timestamp())
    ORDER BY k.ord
$$;

-- MSET: Set multiple keys to multiple values, atomically
CREATE OR REPLACE FUNCTION pgkv.mset(VARIADIC p_pairs TEXT[])
RETURNS TEXT
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pgkv, pg_temp
AS $$
DECLARE
    v_count INTEGER;
    v_written INTEGER;
    v_key TEXT;
    v_value TEXT;
BEGIN
    IF p_pairs IS NULL OR array_length(p_pairs, 1) % 2 != 0 THEN
        PERFORM pgkv._err('ERR wrong number of arguments for ''mset'' command');
    END IF;

    -- A key given twice takes its last value. Rows are written in key order, so two
    -- multi-key calls cannot deadlock each other.
    WITH input AS (
        SELECT DISTINCT ON (p_pairs[i]) p_pairs[i] AS k, p_pairs[i + 1] AS v
        FROM generate_series(1, array_length(p_pairs, 1), 2) AS i
        ORDER BY p_pairs[i], i DESC
    ), written AS (
        INSERT INTO pgkv.store AS s (key, value, type, expires_at, updated_at)
        SELECT d.k, to_jsonb(d.v), 'string', NULL, NOW()
        FROM input d
        ORDER BY d.k COLLATE "C"
        ON CONFLICT (key) DO UPDATE
        SET value = EXCLUDED.value, type = 'string', expires_at = NULL, updated_at = NOW()
        WHERE s.id IS NULL
        RETURNING 1
    )
    SELECT (SELECT COUNT(*) FROM input), (SELECT COUNT(*) FROM written) INTO v_count, v_written;

    IF v_written < v_count THEN
        -- Some of the keys hold collections: replace those one at a time.
        FOR v_key, v_value IN
            SELECT DISTINCT ON (p_pairs[i]) p_pairs[i], p_pairs[i + 1]
            FROM generate_series(1, array_length(p_pairs, 1), 2) AS i
            ORDER BY p_pairs[i], i DESC
        LOOP
            IF EXISTS (SELECT 1 FROM pgkv.store s WHERE s.key = v_key AND s.id IS NOT NULL) THEN
                PERFORM pgkv._replace(v_key, to_jsonb(v_value), NULL, FALSE);
            END IF;
        END LOOP;
    END IF;
    RETURN 'OK';
END;
$$;

-- MSETNX: Set multiple keys only if none of them exist. Returns 1 if set, 0 if not.
CREATE OR REPLACE FUNCTION pgkv.msetnx(VARIADIC p_pairs TEXT[])
RETURNS INTEGER
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pgkv, pg_temp
AS $$
DECLARE
    v_keys TEXT[];
BEGIN
    IF p_pairs IS NULL OR array_length(p_pairs, 1) % 2 != 0 THEN
        PERFORM pgkv._err('ERR wrong number of arguments for ''msetnx'' command');
    END IF;

    SELECT array_agg(p_pairs[i]) INTO v_keys FROM generate_series(1, array_length(p_pairs, 1), 2) AS i;
    PERFORM pgkv._lock_keys('{}', v_keys);

    IF EXISTS (SELECT 1 FROM pgkv.store s WHERE s.key = ANY(v_keys) AND s.expires_at IS DISTINCT FROM '-infinity') THEN
        DELETE FROM pgkv.store s WHERE s.key = ANY(v_keys) AND s.expires_at = '-infinity';
        RETURN 0;
    END IF;

    UPDATE pgkv.store s
    SET value = to_jsonb(d.v), type = 'string', expires_at = NULL, updated_at = NOW()
    FROM (
        SELECT DISTINCT ON (p_pairs[i]) p_pairs[i] AS k, p_pairs[i + 1] AS v
        FROM generate_series(1, array_length(p_pairs, 1), 2) AS i
        ORDER BY p_pairs[i], i DESC
    ) d
    WHERE s.key = d.k;
    RETURN 1;
END;
$$;

-- CAS: Compare-and-set. Set p_key to p_value only if it currently holds p_expected,
-- compared as text (what `pgkv.get(key) #>> '{}'` returns). A NULL p_expected means
-- "only if the key does not exist" (an expired key does not exist), which is SET NX.
-- Returns 1 if it wrote, 0 if the key held something else (or, for NULL, existed).
-- Expiry options are those of set_opts; without one the new value has no expiry.
-- Of several sessions holding the same expected value exactly one wins.
-- The Redis equivalent is `SET key value IFEQ expected` (Redis 8.4); earlier Redis needs a script.
CREATE OR REPLACE FUNCTION pgkv.cas(
    p_key TEXT,
    p_expected TEXT,
    p_value TEXT,
    p_ex BIGINT DEFAULT NULL,
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
        RETURN CASE WHEN pgkv.set_opts(p_key, p_value, p_nx => TRUE, p_ex => p_ex, p_px => p_px,
                                       p_exat => p_exat, p_pxat => p_pxat) IS NULL THEN 0 ELSE 1 END;
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

