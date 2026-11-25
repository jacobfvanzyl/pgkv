-- ============================================================================
-- Internal helpers
--
-- Names starting with an underscore are not part of the API. They are called only by the
-- public functions (which are SECURITY DEFINER and pin search_path), so they carry neither
-- attribute themselves; every reference to a pgkv object is schema-qualified.
--
-- Errors a Redis server would send as an error reply are raised with SQLSTATE class KV,
-- and their message starts with the Redis error code:
--   KV000  ERR ...        (bad argument, syntax, range)
--   KV001  WRONGTYPE ...  (key holds another type)
--   KV002  ERR ...        (unknown command or wrong number of arguments, from pgkv.call)
--   KV003  EXECABORT ...  (internal to pgkv.pipeline, which returns it as a value)
-- ============================================================================

CREATE OR REPLACE FUNCTION pgkv._err(p_message TEXT)
RETURNS VOID
LANGUAGE plpgsql
AS $$
BEGIN
    RAISE EXCEPTION '%', p_message USING ERRCODE = 'KV000';
END;
$$;

CREATE OR REPLACE FUNCTION pgkv._wrongtype()
RETURNS VOID
LANGUAGE plpgsql
AS $$
BEGIN
    RAISE EXCEPTION 'WRONGTYPE Operation against a key holding the wrong kind of value' USING ERRCODE = 'KV001';
END;
$$;

-- _INT: parse an integer the way Redis does: no spaces, no plus sign, no leading zeros.
CREATE OR REPLACE FUNCTION pgkv._int(p_text TEXT, p_message TEXT DEFAULT 'ERR value is not an integer or out of range')
RETURNS BIGINT
LANGUAGE plpgsql
IMMUTABLE
AS $$
DECLARE
    v NUMERIC;
BEGIN
    IF p_text ~ '^(0|-?[1-9][0-9]{0,18})$' THEN
        v := p_text::numeric;
        IF v BETWEEN -9223372036854775808 AND 9223372036854775807 THEN
            RETURN v::bigint;
        END IF;
    END IF;
    RAISE EXCEPTION '%', p_message USING ERRCODE = 'KV000';
END;
$$;

-- _FLOAT: parse a double the way Redis does (C strtod, without leading spaces): decimal or
-- exponent notation, inf, hexadecimal integers. NaN and values outside the double range are
-- errors.
CREATE OR REPLACE FUNCTION pgkv._float(p_text TEXT, p_message TEXT DEFAULT 'ERR value is not a valid float')
RETURNS DOUBLE PRECISION
LANGUAGE plpgsql
IMMUTABLE
AS $$
DECLARE
    v NUMERIC;
BEGIN
    IF p_text ~ '^-?[0-9]{1,15}$' THEN
        RETURN p_text::double precision;
    END IF;
    IF length(p_text) <= 400 AND p_text ~ '^[+-]?([0-9]+\.?[0-9]*|\.[0-9]+)([eE][+-]?[0-9]{1,4})?$' THEN
        v := p_text::numeric;
        IF abs(v) <= 1.7976931348623158e308 AND (v = 0 OR abs(v) >= 2.4703282292062328e-324) THEN
            RETURN p_text::double precision;
        END IF;
    ELSIF p_text ~* '^[+-]?inf(inity)?$' THEN
        RETURN CASE WHEN left(p_text, 1) = '-' THEN '-infinity' ELSE 'infinity' END::double precision;
    ELSIF p_text ~* '^[+-]?0x[0-9a-f]{1,15}$' THEN
        RETURN (CASE WHEN left(p_text, 1) = '-' THEN -1 ELSE 1 END)
            * ('x' || lpad(regexp_replace(p_text, '^[+-]?0[xX]', ''), 16, '0'))::bit(64)::bigint;
    END IF;
    RAISE EXCEPTION '%', p_message USING ERRCODE = 'KV000';
END;
$$;

-- _DECIMAL: parse a number for INCRBYFLOAT, which Redis computes in long double: the same
-- syntax as pgkv._float over a wider range. o_value is NULL if the text is not a number;
-- o_infinite is 1 or -1 for an infinity.
CREATE OR REPLACE FUNCTION pgkv._decimal(p_text TEXT, OUT o_value NUMERIC, OUT o_infinite INTEGER)
LANGUAGE plpgsql
IMMUTABLE
AS $$
BEGIN
    o_infinite := 0;
    IF length(p_text) <= 5000 AND p_text ~ '^[+-]?([0-9]+\.?[0-9]*|\.[0-9]+)([eE][+-]?[0-9]{1,4})?$' THEN
        o_value := p_text::numeric;
        IF abs(o_value) > 1.18973149535723176e4932 THEN
            o_value := NULL;
        END IF;
    ELSIF p_text ~* '^[+-]?inf(inity)?$' THEN
        o_infinite := CASE WHEN left(p_text, 1) = '-' THEN -1 ELSE 1 END;
        o_value := 0;
    ELSIF p_text ~* '^[+-]?0x[0-9a-f]{1,15}$' THEN
        o_value := pgkv._float(p_text);
    END IF;
END;
$$;

-- _FMT: a double as Redis prints it: whole numbers as integers, otherwise the shortest digits
-- that read back as the same double, in plain or exponent notation by the rules of Redis's
-- fpconv_dtoa ("0.000001", "1e-7", "123456789012345680", "1e+20").
CREATE OR REPLACE FUNCTION pgkv._fmt(p_value DOUBLE PRECISION)
RETURNS TEXT
LANGUAGE plpgsql
IMMUTABLE
AS $$
DECLARE
    v_text TEXT;
    v_parts TEXT[];
    v_digits TEXT;
    v_k INTEGER;
    v_n INTEGER;
    v_exp INTEGER;
    v_sign TEXT := '';
BEGIN
    IF p_value IS NULL THEN
        RETURN NULL;
    ELSIF p_value = 'infinity' THEN
        RETURN 'inf';
    ELSIF p_value = '-infinity' THEN
        RETURN '-inf';
    ELSIF p_value = 0 THEN
        RETURN p_value::text;
    ELSIF abs(p_value) <= 4611686018427387903 AND p_value = trunc(p_value) THEN
        RETURN (p_value::bigint)::text;
    END IF;

    -- PostgreSQL prints the shortest round-trip digits; only the layout differs.
    v_text := p_value::text;
    IF left(v_text, 1) = '-' THEN
        v_sign := '-';
        v_text := substr(v_text, 2);
    END IF;
    v_parts := regexp_match(v_text, '^([0-9]*)\.?([0-9]*)(?:e([+-]?[0-9]+))?$');
    v_k := COALESCE(v_parts[3]::integer, 0) - length(v_parts[2]);
    v_digits := ltrim(v_parts[1] || v_parts[2], '0');
    v_k := v_k + length(v_digits) - length(rtrim(v_digits, '0'));
    v_digits := rtrim(v_digits, '0');
    v_n := length(v_digits);
    v_exp := abs(v_k + v_n - 1);

    IF v_k >= 0 AND v_exp < v_n + 7 THEN
        RETURN v_sign || v_digits || repeat('0', v_k);
    ELSIF v_k < 0 AND (v_k > -7 OR v_exp < 4) THEN
        IF v_n + v_k <= 0 THEN
            RETURN v_sign || '0.' || repeat('0', -(v_n + v_k)) || v_digits;
        END IF;
        RETURN v_sign || left(v_digits, v_n + v_k) || '.' || substr(v_digits, v_n + v_k + 1);
    END IF;
    RETURN v_sign || left(v_digits, 1) || CASE WHEN v_n > 1 THEN '.' || substr(v_digits, 2) ELSE '' END
        || 'e' || CASE WHEN v_k + v_n - 1 < 0 THEN '-' ELSE '+' END || v_exp;
END;
$$;

-- _FADD, _FMUL, _FSUM: double arithmetic that overflows to infinity (and underflows to 0)
-- as IEEE arithmetic does in Redis, where PostgreSQL raises an error.
CREATE OR REPLACE FUNCTION pgkv._fadd(a DOUBLE PRECISION, b DOUBLE PRECISION)
RETURNS DOUBLE PRECISION
LANGUAGE sql
IMMUTABLE
STRICT
AS $$
    SELECT CASE
        WHEN a > 0 AND b > 0 AND a > 1.7976931348623157e308::double precision - b THEN 'infinity'::double precision
        WHEN a < 0 AND b < 0 AND a < -1.7976931348623157e308::double precision - b THEN '-infinity'::double precision
        ELSE a + b END
$$;

CREATE OR REPLACE FUNCTION pgkv._fmul(a DOUBLE PRECISION, b DOUBLE PRECISION)
RETURNS DOUBLE PRECISION
LANGUAGE sql
IMMUTABLE
STRICT
AS $$
    SELECT CASE
        WHEN a = 0 OR b = 0 OR a = 'NaN' OR b = 'NaN' THEN a * b
        WHEN abs(a) = 'infinity' OR abs(b) = 'infinity' THEN sign(a) * sign(b) * 'infinity'::double precision
        WHEN abs(a) > 1 AND abs(b) > 1.7976931348623157e308::double precision / abs(a)
            THEN sign(a) * sign(b) * 'infinity'::double precision
        WHEN abs(a) < 1 AND abs(b) < 4.9406564584124654e-324::double precision / abs(a) THEN 0
        ELSE a * b END
$$;

CREATE OR REPLACE AGGREGATE pgkv._fsum(DOUBLE PRECISION) (
    SFUNC = pgkv._fadd,
    STYPE = DOUBLE PRECISION
);

-- _ROUND17: a numeric rounded to 17 decimal places, which is how Redis prints an
-- INCRBYFLOAT result ("%.17Lf", trailing zeros removed).
CREATE OR REPLACE FUNCTION pgkv._round17(p_value NUMERIC)
RETURNS NUMERIC
LANGUAGE sql
IMMUTABLE
AS $$
    SELECT round(p_value, 17)
$$;

-- _NUM_TEXT: a numeric without trailing zeros (INCRBYFLOAT results).
CREATE OR REPLACE FUNCTION pgkv._num_text(p_value NUMERIC)
RETURNS TEXT
LANGUAGE sql
IMMUTABLE
AS $$
    SELECT CASE WHEN p_value::text LIKE '%.%'
        THEN regexp_replace(regexp_replace(p_value::text, '0+$', ''), '\.$', '')
        ELSE p_value::text END
$$;

-- _GLOB_RE: a Redis glob pattern as an anchored regular expression.
CREATE OR REPLACE FUNCTION pgkv._glob_re(p_pattern TEXT)
RETURNS TEXT
LANGUAGE plpgsql
IMMUTABLE
AS $$
DECLARE
    v_out TEXT := '^';
    v_i INTEGER := 1;
    v_len INTEGER := length(p_pattern);
    v_ch TEXT;
    v_j INTEGER;
    v_class TEXT;
    v_closed BOOLEAN;
BEGIN
    WHILE v_i <= v_len LOOP
        v_ch := substr(p_pattern, v_i, 1);
        IF v_ch = '*' THEN
            v_out := v_out || '.*';
        ELSIF v_ch = '?' THEN
            v_out := v_out || '.';
        ELSIF v_ch = E'\\' AND v_i < v_len THEN
            v_i := v_i + 1;
            v_out := v_out || regexp_replace(substr(p_pattern, v_i, 1), '([!-/:-@\[-`{-~])', '\\\1');
        ELSIF v_ch = '[' THEN
            v_j := v_i + 1;
            v_class := '';
            v_closed := FALSE;
            IF substr(p_pattern, v_j, 1) = '^' THEN
                v_class := '^';
                v_j := v_j + 1;
            END IF;
            WHILE v_j <= v_len LOOP
                v_ch := substr(p_pattern, v_j, 1);
                IF v_ch = ']' THEN
                    v_closed := TRUE;
                    EXIT;
                ELSIF v_ch = E'\\' AND v_j < v_len THEN
                    v_j := v_j + 1;
                    v_class := v_class || regexp_replace(substr(p_pattern, v_j, 1), '([!-/:-@\[-`{-~])', '\\\1');
                ELSIF v_ch = '-' AND v_class NOT IN ('', '^') AND v_j < v_len AND substr(p_pattern, v_j + 1, 1) <> ']' THEN
                    v_class := v_class || '-';
                ELSE
                    v_class := v_class || regexp_replace(v_ch, '([!-/:-@\[-`{-~])', '\\\1');
                END IF;
                v_j := v_j + 1;
            END LOOP;
            IF v_closed AND v_class NOT IN ('', '^') THEN
                v_out := v_out || '[' || v_class || ']';
                v_i := v_j;
            ELSE
                v_out := v_out || '\[';
            END IF;
        ELSE
            v_out := v_out || regexp_replace(v_ch, '([!-/:-@\[-`{-~])', '\\\1');
        END IF;
        v_i := v_i + 1;
    END LOOP;
    RETURN v_out || '$';
END;
$$;

-- _LIKE_PREFIX: a LIKE pattern matching keys that start with p_prefix, taken literally.
CREATE OR REPLACE FUNCTION pgkv._like_prefix(p_prefix TEXT)
RETURNS TEXT
LANGUAGE sql
IMMUTABLE
AS $$
    SELECT replace(replace(replace(COALESCE(p_prefix, ''), E'\\', E'\\\\'), '%', E'\\%'), '_', E'\\_') || '%'
$$;

-- _DROP_ELEMS: delete the elements of one collection.
CREATE OR REPLACE FUNCTION pgkv._drop_elems(p_id BIGINT, p_type TEXT)
RETURNS VOID
LANGUAGE plpgsql
AS $$
BEGIN
    IF p_id IS NULL THEN
        RETURN;
    END IF;
    CASE p_type
        WHEN 'hash' THEN DELETE FROM pgkv.hash_fields WHERE id = p_id;
        WHEN 'list' THEN DELETE FROM pgkv.list_items WHERE id = p_id;
        WHEN 'set' THEN DELETE FROM pgkv.set_members WHERE id = p_id;
        WHEN 'zset' THEN DELETE FROM pgkv.zset_members WHERE id = p_id;
        ELSE NULL;
    END CASE;
END;
$$;

-- _DROP_MANY: delete the elements of several collections whose rows are already gone.
CREATE OR REPLACE FUNCTION pgkv._drop_many(p_ids BIGINT[])
RETURNS VOID
LANGUAGE plpgsql
AS $$
BEGIN
    IF p_ids IS NULL OR cardinality(p_ids) = 0 THEN
        RETURN;
    END IF;
    DELETE FROM pgkv.hash_fields WHERE id = ANY(p_ids);
    DELETE FROM pgkv.list_items WHERE id = ANY(p_ids);
    DELETE FROM pgkv.set_members WHERE id = ANY(p_ids);
    DELETE FROM pgkv.zset_members WHERE id = ANY(p_ids);
END;
$$;

-- _LOCK_KEY: lock a key's row for the rest of the transaction and return its live contents.
-- o_type IS NULL means the key is absent (an expired key counts as absent and is removed,
-- with its elements). With p_create, an absent key gets a placeholder row (already expired,
-- so no reader sees it) that this transaction holds; the caller must overwrite it with
-- pgkv._put or pgkv._save, or delete it. This is what makes read-modify-write safe under
-- READ COMMITTED: a second writer of the same key waits here, then reads what the first
-- one committed.
CREATE OR REPLACE FUNCTION pgkv._lock_key(
    p_key TEXT,
    p_create BOOLEAN,
    OUT o_value JSONB,
    OUT o_type TEXT,
    OUT o_expires_at TIMESTAMPTZ,
    OUT o_id BIGINT,
    OUT o_card BIGINT,
    OUT o_head BIGINT
)
LANGUAGE plpgsql
AS $$
DECLARE
    v_attempt INTEGER;
BEGIN
    FOR v_attempt IN 1..100 LOOP
        SELECT s.value, s.type, s.expires_at, s.id, s.card, s.head
        INTO o_value, o_type, o_expires_at, o_id, o_card, o_head
        FROM pgkv.store s
        WHERE s.key = p_key
        FOR UPDATE;

        IF FOUND THEN
            IF o_expires_at IS NULL OR o_expires_at >= clock_timestamp() THEN
                RETURN;
            END IF;
            -- Expired: the row is locked, so nobody can have replaced it. Remove it.
            PERFORM pgkv._drop_elems(o_id, o_type);
            DELETE FROM pgkv.store s WHERE s.key = p_key;
            o_value := NULL;
            o_type := NULL;
            o_expires_at := NULL;
            o_id := NULL;
            o_card := NULL;
            o_head := NULL;
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

-- _PUT: write a string or JSON value to a key whose row the transaction holds through
-- pgkv._lock_key. The caller removes the elements first if the key held a collection.
CREATE OR REPLACE FUNCTION pgkv._put(p_key TEXT, p_value JSONB, p_type TEXT, p_expires_at TIMESTAMPTZ)
RETURNS VOID
LANGUAGE plpgsql
AS $$
BEGIN
    UPDATE pgkv.store s
    SET value = p_value,
        type = p_type,
        expires_at = p_expires_at,
        id = NULL,
        card = NULL,
        head = NULL,
        updated_at = NOW()
    WHERE s.key = p_key;
END;
$$;

-- _SAVE: write a collection's header to a key whose row the transaction holds through
-- pgkv._lock_key. A collection with no elements does not exist, so its row is deleted.
CREATE OR REPLACE FUNCTION pgkv._save(p_key TEXT, p_type TEXT, p_id BIGINT, p_card BIGINT, p_head BIGINT, p_expires_at TIMESTAMPTZ)
RETURNS VOID
LANGUAGE plpgsql
AS $$
BEGIN
    IF p_card <= 0 THEN
        DELETE FROM pgkv.store s WHERE s.key = p_key;
    ELSE
        UPDATE pgkv.store s
        SET value = NULL,
            type = p_type,
            expires_at = p_expires_at,
            id = p_id,
            card = p_card,
            head = p_head,
            updated_at = NOW()
        WHERE s.key = p_key;
    END IF;
END;
$$;

-- _OPEN: lock a key for a write to a collection of type p_type. Raises WRONGTYPE if the key
-- holds something else. A key that is absent comes back with o_new set and, with p_create,
-- a fresh id and a placeholder row that the caller must finish with pgkv._save.
CREATE OR REPLACE FUNCTION pgkv._open(
    p_key TEXT,
    p_type TEXT,
    p_create BOOLEAN,
    OUT o_id BIGINT,
    OUT o_card BIGINT,
    OUT o_head BIGINT,
    OUT o_expires_at TIMESTAMPTZ,
    OUT o_new BOOLEAN
)
LANGUAGE plpgsql
AS $$
DECLARE
    v_locked RECORD;
    v_type TEXT;
BEGIN
    -- The common case, a key that exists and has not expired, takes one statement.
    SELECT s.type, s.id, s.card, s.head, s.expires_at INTO v_type, o_id, o_card, o_head, o_expires_at
    FROM pgkv.store s
    WHERE s.key = p_key
    FOR UPDATE;

    IF NOT FOUND OR o_expires_at < clock_timestamp() THEN
        v_locked := pgkv._lock_key(p_key, p_create);
        v_type := v_locked.o_type;
        o_id := v_locked.o_id;
        o_card := v_locked.o_card;
        o_head := v_locked.o_head;
        o_expires_at := v_locked.o_expires_at;
    END IF;

    IF v_type IS NULL THEN
        o_new := TRUE;
        o_card := 0;
        o_head := 0;
        o_expires_at := NULL;
        IF p_create THEN
            o_id := nextval('pgkv.collection_id');
        END IF;
    ELSIF v_type <> p_type THEN
        PERFORM pgkv._wrongtype();
    ELSE
        o_new := FALSE;
    END IF;
END;
$$;

-- _PEEK: read a collection's header without locking. o_id IS NULL means the key is absent.
-- Raises WRONGTYPE if the key holds another type.
CREATE OR REPLACE FUNCTION pgkv._peek(p_key TEXT, p_type TEXT, OUT o_id BIGINT, OUT o_card BIGINT, OUT o_head BIGINT)
LANGUAGE plpgsql
STABLE
AS $$
DECLARE
    v_type TEXT;
BEGIN
    SELECT s.type, s.id, s.card, s.head INTO v_type, o_id, o_card, o_head
    FROM pgkv.store s
    WHERE s.key = p_key AND (s.expires_at IS NULL OR s.expires_at >= clock_timestamp());

    IF v_type IS NOT NULL AND v_type <> p_type THEN
        PERFORM pgkv._wrongtype();
    END IF;
END;
$$;

-- _REQUIRE_STRING: raise WRONGTYPE if the key exists and is not a string.
CREATE OR REPLACE FUNCTION pgkv._require_string(p_key TEXT)
RETURNS VOID
LANGUAGE plpgsql
STABLE
AS $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM pgkv.store s
        WHERE s.key = p_key
          AND s.type != 'string'
          AND (s.expires_at IS NULL OR s.expires_at >= clock_timestamp())
    ) THEN
        PERFORM pgkv._wrongtype();
    END IF;
END;
$$;

-- _WHEN: the expiry time for a Redis expire argument. p_unit is 's' or 'ms'; p_absolute means
-- a Unix time rather than a duration. Times PostgreSQL cannot hold are an error, as overflow
-- is in Redis.
CREATE OR REPLACE FUNCTION pgkv._when(p_amount BIGINT, p_unit TEXT, p_absolute BOOLEAN, p_command TEXT)
RETURNS TIMESTAMPTZ
LANGUAGE plpgsql
AS $$
DECLARE
    v_ms NUMERIC := p_amount::numeric * CASE WHEN p_unit = 's' THEN 1000 ELSE 1 END;
BEGIN
    -- Redis keeps the time as a 64-bit count of milliseconds and rejects what overflows it.
    IF v_ms NOT BETWEEN -9223372036854775808 AND 9223372036854775807 THEN
        RAISE EXCEPTION 'ERR invalid expire time in ''%'' command', p_command USING ERRCODE = 'KV000';
    END IF;
    IF NOT p_absolute THEN
        v_ms := v_ms + trunc(extract(epoch FROM clock_timestamp())::numeric * 1000);
    END IF;
    -- 9999-12-31; beyond it a timestamp is no longer a usable expiry.
    IF v_ms > 253402300799999 THEN
        RAISE EXCEPTION 'ERR invalid expire time in ''%'' command', p_command USING ERRCODE = 'KV000';
    END IF;
    IF v_ms < -62135596800000 THEN
        RETURN '-infinity';
    END IF;
    RETURN to_timestamp(v_ms / 1000.0);
END;
$$;

-- _EXPIRES_AT: the expiry time for SET-style options. NULL means no expiry.
CREATE OR REPLACE FUNCTION pgkv._expires_at(p_ex BIGINT, p_px BIGINT, p_exat BIGINT, p_pxat BIGINT, p_keepttl BOOLEAN, p_command TEXT DEFAULT 'set')
RETURNS TIMESTAMPTZ
LANGUAGE plpgsql
AS $$
BEGIN
    IF (CASE WHEN p_ex IS NOT NULL THEN 1 ELSE 0 END +
        CASE WHEN p_px IS NOT NULL THEN 1 ELSE 0 END +
        CASE WHEN p_exat IS NOT NULL THEN 1 ELSE 0 END +
        CASE WHEN p_pxat IS NOT NULL THEN 1 ELSE 0 END +
        CASE WHEN p_keepttl THEN 1 ELSE 0 END) > 1 THEN
        RAISE EXCEPTION 'ERR syntax error' USING ERRCODE = 'KV000';
    END IF;

    IF COALESCE(p_ex, p_px, p_exat, p_pxat) <= 0 THEN
        RAISE EXCEPTION 'ERR invalid expire time in ''%'' command', p_command USING ERRCODE = 'KV000';
    END IF;

    IF p_ex IS NOT NULL THEN
        RETURN pgkv._when(p_ex, 's', FALSE, p_command);
    ELSIF p_px IS NOT NULL THEN
        RETURN pgkv._when(p_px, 'ms', FALSE, p_command);
    ELSIF p_exat IS NOT NULL THEN
        RETURN pgkv._when(p_exat, 's', TRUE, p_command);
    ELSIF p_pxat IS NOT NULL THEN
        RETURN pgkv._when(p_pxat, 'ms', TRUE, p_command);
    END IF;
    RETURN NULL;
END;
$$;

-- _LOCK_KEYS: lock several keys in key order, so that two multi-key commands cannot
-- deadlock each other. p_create lists the keys that get a placeholder row if absent.
CREATE OR REPLACE FUNCTION pgkv._lock_keys(p_keys TEXT[], p_create TEXT[] DEFAULT NULL)
RETURNS VOID
LANGUAGE plpgsql
AS $$
DECLARE
    v_key TEXT;
BEGIN
    FOR v_key IN
        SELECT k FROM unnest(p_keys || COALESCE(p_create, '{}')) AS u(k) WHERE k IS NOT NULL GROUP BY k ORDER BY k COLLATE "C"
    LOOP
        PERFORM pgkv._lock_key(v_key, v_key = ANY(COALESCE(p_create, '{}')));
    END LOOP;
END;
$$;

-- _RANGE: turn Redis start/stop indexes (negative counts from the end) into a zero-based
-- offset and a length over p_card elements. o_len is 0 when the range is empty.
CREATE OR REPLACE FUNCTION pgkv._range(p_start BIGINT, p_stop BIGINT, p_card BIGINT, OUT o_off BIGINT, OUT o_len BIGINT)
LANGUAGE plpgsql
IMMUTABLE
AS $$
DECLARE
    v_start NUMERIC := p_start;
    v_stop NUMERIC := p_stop;
BEGIN
    IF v_start < 0 THEN v_start := GREATEST(v_start + p_card, 0); END IF;
    IF v_stop < 0 THEN v_stop := v_stop + p_card; END IF;
    IF v_stop >= p_card THEN v_stop := p_card - 1; END IF;
    IF p_card IS NULL OR v_start > v_stop OR v_start >= p_card THEN
        o_off := 0;
        o_len := 0;
    ELSE
        o_off := v_start;
        o_len := v_stop - v_start + 1;
    END IF;
END;
$$;

