-- ============================================================================
-- Hashes: one row per field in pgkv.hash_fields
-- ============================================================================

-- HSET: Set field(s) in a hash. Returns the number of fields added (not updated).
CREATE OR REPLACE FUNCTION pgkv.hset(p_key TEXT, VARIADIC p_pairs TEXT[])
RETURNS INTEGER
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pgkv, pg_temp
AS $$
DECLARE
    v_opened RECORD;
    v_id BIGINT;
    v_card BIGINT;
    v_expires_at TIMESTAMPTZ;
    v_new BOOLEAN;
    v_added INTEGER;
BEGIN
    IF p_pairs IS NULL OR array_length(p_pairs, 1) % 2 != 0 THEN
        PERFORM pgkv._err('ERR wrong number of arguments for ''hset'' command');
    END IF;

    v_opened := pgkv._open(p_key, 'hash', TRUE);
    v_id := v_opened.o_id;
    v_card := v_opened.o_card;
    v_expires_at := v_opened.o_expires_at;
    v_new := v_opened.o_new;

    IF array_length(p_pairs, 1) = 2 THEN
        INSERT INTO pgkv.hash_fields AS f (id, field, value)
        VALUES (v_id, p_pairs[1], to_jsonb(p_pairs[2]))
        ON CONFLICT (id, field) DO UPDATE SET value = EXCLUDED.value
        RETURNING (f.xmax = 0)::integer INTO v_added;
    ELSE
        WITH written AS (
            INSERT INTO pgkv.hash_fields AS f (id, field, value)
            SELECT v_id, d.k, to_jsonb(d.v)
            FROM (
                SELECT DISTINCT ON (p_pairs[i]) p_pairs[i] AS k, p_pairs[i + 1] AS v
                FROM generate_series(1, array_length(p_pairs, 1), 2) AS i
                ORDER BY p_pairs[i], i DESC
            ) d
            ON CONFLICT (id, field) DO UPDATE SET value = EXCLUDED.value
            RETURNING (f.xmax = 0) AS inserted
        )
        SELECT (COUNT(*) FILTER (WHERE inserted))::integer INTO v_added FROM written;
    END IF;

    IF v_added > 0 THEN
        PERFORM pgkv._save(p_key, 'hash', v_id, v_card + v_added, NULL, v_expires_at);
    END IF;
    RETURN v_added;
END;
$$;

-- HSETNX: Set a field only if it does not exist. Returns 1 if set, 0 if not.
CREATE OR REPLACE FUNCTION pgkv.hsetnx(p_key TEXT, p_field TEXT, p_value TEXT)
RETURNS INTEGER
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pgkv, pg_temp
AS $$
DECLARE
    v_opened RECORD;
    v_id BIGINT;
    v_card BIGINT;
    v_expires_at TIMESTAMPTZ;
BEGIN
    v_opened := pgkv._open(p_key, 'hash', TRUE);
    v_id := v_opened.o_id;
    v_card := v_opened.o_card;
    v_expires_at := v_opened.o_expires_at;

    INSERT INTO pgkv.hash_fields (id, field, value) VALUES (v_id, p_field, to_jsonb(p_value))
    ON CONFLICT (id, field) DO NOTHING;
    IF NOT FOUND THEN
        RETURN 0;
    END IF;
    PERFORM pgkv._save(p_key, 'hash', v_id, v_card + 1, NULL, v_expires_at);
    RETURN 1;
END;
$$;

-- HGET: Get the value of a hash field
CREATE OR REPLACE FUNCTION pgkv.hget(p_key TEXT, p_field TEXT)
RETURNS JSONB
LANGUAGE plpgsql
STABLE
SECURITY DEFINER
SET search_path = pgkv, pg_temp
AS $$
DECLARE
    v_type TEXT;
    v_value JSONB;
BEGIN
    SELECT s.type, f.value INTO v_type, v_value
    FROM pgkv.store s
    LEFT JOIN pgkv.hash_fields f ON f.id = s.id AND f.field = p_field
    WHERE s.key = p_key AND (s.expires_at IS NULL OR s.expires_at >= clock_timestamp());

    IF v_type <> 'hash' THEN
        PERFORM pgkv._wrongtype();
    END IF;
    RETURN v_value;
END;
$$;

-- HMGET: Get the values of several hash fields, in order (NULL for a missing field)
CREATE OR REPLACE FUNCTION pgkv.hmget(p_key TEXT, VARIADIC p_fields TEXT[])
RETURNS TABLE(field TEXT, value JSONB)
LANGUAGE plpgsql
STABLE
SECURITY DEFINER
SET search_path = pgkv, pg_temp
AS $$
DECLARE
    v_peeked RECORD;
    v_id BIGINT;
BEGIN
    v_peeked := pgkv._peek(p_key, 'hash');
    v_id := v_peeked.o_id;
    RETURN QUERY
    SELECT k.f, h.value
    FROM unnest(p_fields) WITH ORDINALITY AS k(f, ord)
    LEFT JOIN pgkv.hash_fields h ON h.id = v_id AND h.field = k.f
    ORDER BY k.ord;
END;
$$;

-- HGETALL: Get all fields and values of a hash, in byte order of the field
CREATE OR REPLACE FUNCTION pgkv.hgetall(p_key TEXT)
RETURNS TABLE(field TEXT, value JSONB)
LANGUAGE plpgsql
STABLE
SECURITY DEFINER
SET search_path = pgkv, pg_temp
AS $$
DECLARE
    v_peeked RECORD;
    v_id BIGINT;
BEGIN
    v_peeked := pgkv._peek(p_key, 'hash');
    v_id := v_peeked.o_id;
    RETURN QUERY SELECT h.field::text, h.value FROM pgkv.hash_fields h WHERE h.id = v_id ORDER BY h.field;
END;
$$;

-- HDEL: Delete field(s) from a hash. Returns the number of fields removed.
CREATE OR REPLACE FUNCTION pgkv.hdel(p_key TEXT, VARIADIC p_fields TEXT[])
RETURNS INTEGER
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pgkv, pg_temp
AS $$
DECLARE
    v_opened RECORD;
    v_id BIGINT;
    v_card BIGINT;
    v_expires_at TIMESTAMPTZ;
    v_new BOOLEAN;
    v_removed INTEGER;
BEGIN
    v_opened := pgkv._open(p_key, 'hash', FALSE);
    v_id := v_opened.o_id;
    v_card := v_opened.o_card;
    v_expires_at := v_opened.o_expires_at;
    v_new := v_opened.o_new;
    IF v_new THEN
        RETURN 0;
    END IF;

    DELETE FROM pgkv.hash_fields f WHERE f.id = v_id AND f.field = ANY(p_fields);
    GET DIAGNOSTICS v_removed = ROW_COUNT;
    IF v_removed > 0 THEN
        PERFORM pgkv._save(p_key, 'hash', v_id, v_card - v_removed, NULL, v_expires_at);
    END IF;
    RETURN v_removed;
END;
$$;

-- HEXISTS: 1 if the field exists in the hash, 0 if not
CREATE OR REPLACE FUNCTION pgkv.hexists(p_key TEXT, p_field TEXT)
RETURNS INTEGER
LANGUAGE plpgsql
STABLE
SECURITY DEFINER
SET search_path = pgkv, pg_temp
AS $$
BEGIN
    RETURN CASE WHEN pgkv.hget(p_key, p_field) IS NULL THEN 0 ELSE 1 END;
END;
$$;

-- HLEN: Number of fields in a hash
CREATE OR REPLACE FUNCTION pgkv.hlen(p_key TEXT)
RETURNS INTEGER
LANGUAGE plpgsql
STABLE
SECURITY DEFINER
SET search_path = pgkv, pg_temp
AS $$
BEGIN
    RETURN COALESCE((SELECT p.o_card FROM pgkv._peek(p_key, 'hash') p), 0);
END;
$$;

-- HSTRLEN: Length in bytes of the value of a hash field (0 if missing)
CREATE OR REPLACE FUNCTION pgkv.hstrlen(p_key TEXT, p_field TEXT)
RETURNS INTEGER
LANGUAGE plpgsql
STABLE
SECURITY DEFINER
SET search_path = pgkv, pg_temp
AS $$
BEGIN
    RETURN COALESCE(octet_length(pgkv.hget(p_key, p_field) #>> '{}'), 0);
END;
$$;

-- HKEYS: All field names of a hash, in byte order
CREATE OR REPLACE FUNCTION pgkv.hkeys(p_key TEXT)
RETURNS SETOF TEXT
LANGUAGE plpgsql
STABLE
SECURITY DEFINER
SET search_path = pgkv, pg_temp
AS $$
BEGIN
    RETURN QUERY SELECT h.field FROM pgkv.hgetall(p_key) h;
END;
$$;

-- HVALS: All values of a hash, in byte order of their fields
CREATE OR REPLACE FUNCTION pgkv.hvals(p_key TEXT)
RETURNS SETOF JSONB
LANGUAGE plpgsql
STABLE
SECURITY DEFINER
SET search_path = pgkv, pg_temp
AS $$
BEGIN
    RETURN QUERY SELECT h.value FROM pgkv.hgetall(p_key) h;
END;
$$;

-- HINCRBY: Increment the integer value of a hash field
CREATE OR REPLACE FUNCTION pgkv.hincrby(p_key TEXT, p_field TEXT, p_increment BIGINT)
RETURNS BIGINT
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pgkv, pg_temp
AS $$
DECLARE
    v_opened RECORD;
    v_id BIGINT;
    v_card BIGINT;
    v_expires_at TIMESTAMPTZ;
    v_current JSONB;
    v_result NUMERIC;
BEGIN
    v_opened := pgkv._open(p_key, 'hash', TRUE);
    v_id := v_opened.o_id;
    v_card := v_opened.o_card;
    v_expires_at := v_opened.o_expires_at;

    SELECT f.value INTO v_current FROM pgkv.hash_fields f WHERE f.id = v_id AND f.field = p_field;
    IF v_current IS NULL THEN
        v_result := p_increment;
    ELSE
        v_result := pgkv._int(v_current #>> '{}', 'ERR hash value is not an integer')::numeric + p_increment;
    END IF;
    IF v_result NOT BETWEEN -9223372036854775808 AND 9223372036854775807 THEN
        PERFORM pgkv._err('ERR increment or decrement would overflow');
    END IF;

    IF v_current IS NULL THEN
        INSERT INTO pgkv.hash_fields (id, field, value) VALUES (v_id, p_field, to_jsonb(v_result::bigint));
        PERFORM pgkv._save(p_key, 'hash', v_id, v_card + 1, NULL, v_expires_at);
    ELSE
        UPDATE pgkv.hash_fields f SET value = to_jsonb(v_result::bigint) WHERE f.id = v_id AND f.field = p_field;
    END IF;
    RETURN v_result::bigint;
END;
$$;

-- _HINCRBYFLOAT: HINCRBYFLOAT with the increment as text, checked in the order Redis checks.
CREATE OR REPLACE FUNCTION pgkv._hincrbyfloat(p_key TEXT, p_field TEXT, p_increment TEXT)
RETURNS NUMERIC
LANGUAGE plpgsql
AS $$
DECLARE
    v_opened RECORD;
    v_id BIGINT;
    v_card BIGINT;
    v_expires_at TIMESTAMPTZ;
    v_stored JSONB;
    v_current NUMERIC := 0;
    v_increment NUMERIC;
    v_inf INTEGER := 0;
    v_inf2 INTEGER;
    v_result NUMERIC;
BEGIN
    SELECT d.o_value, d.o_infinite INTO v_increment, v_inf2 FROM pgkv._decimal(p_increment) d;
    IF v_increment IS NULL THEN
        PERFORM pgkv._err('ERR value is not a valid float');
    ELSIF v_inf2 <> 0 THEN
        PERFORM pgkv._err('ERR value is NaN or Infinity');
    END IF;

    v_opened := pgkv._open(p_key, 'hash', TRUE);
    v_id := v_opened.o_id;
    v_card := v_opened.o_card;
    v_expires_at := v_opened.o_expires_at;

    SELECT f.value INTO v_stored FROM pgkv.hash_fields f WHERE f.id = v_id AND f.field = p_field;
    IF v_stored IS NOT NULL THEN
        SELECT d.o_value, d.o_infinite INTO v_current, v_inf FROM pgkv._decimal(v_stored #>> '{}') d;
        IF v_current IS NULL THEN
            PERFORM pgkv._err('ERR hash value is not a float');
        END IF;
    END IF;

    v_result := v_current + v_increment;
    IF v_inf <> 0 OR abs(v_result) > 1.18973149535723176e4932 THEN
        PERFORM pgkv._err('ERR increment would produce NaN or Infinity');
    END IF;
    v_result := pgkv._num_text(pgkv._round17(v_result))::numeric;

    IF v_stored IS NULL THEN
        INSERT INTO pgkv.hash_fields (id, field, value) VALUES (v_id, p_field, to_jsonb(v_result));
        PERFORM pgkv._save(p_key, 'hash', v_id, v_card + 1, NULL, v_expires_at);
    ELSE
        UPDATE pgkv.hash_fields f SET value = to_jsonb(v_result) WHERE f.id = v_id AND f.field = p_field;
    END IF;
    RETURN v_result;
END;
$$;

-- HINCRBYFLOAT: Increment the numeric value of a hash field by a decimal amount
CREATE OR REPLACE FUNCTION pgkv.hincrbyfloat(p_key TEXT, p_field TEXT, p_increment NUMERIC)
RETURNS NUMERIC
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pgkv, pg_temp
AS $$
BEGIN
    RETURN pgkv._hincrbyfloat(p_key, p_field, p_increment::text);
END;
$$;

-- HRANDFIELD: Random fields of a hash. A positive p_count returns up to that many distinct
-- fields; a negative one returns exactly that many, possibly repeating.
CREATE OR REPLACE FUNCTION pgkv.hrandfield(p_key TEXT, p_count BIGINT DEFAULT 1)
RETURNS TABLE(field TEXT, value JSONB)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pgkv, pg_temp
AS $$
DECLARE
    v_peeked RECORD;
    v_id BIGINT;
    v_card BIGINT;
BEGIN
    v_peeked := pgkv._peek(p_key, 'hash');
    v_id := v_peeked.o_id;
    v_card := v_peeked.o_card;
    IF v_id IS NULL OR p_count = 0 THEN
        RETURN;
    END IF;
    IF p_count > 0 THEN
        RETURN QUERY SELECT h.field::text, h.value FROM pgkv.hash_fields h WHERE h.id = v_id ORDER BY random() LIMIT p_count;
    ELSE
        RETURN QUERY
        SELECT r.field::text, r.value
        FROM generate_series(1, -p_count) AS g
        CROSS JOIN LATERAL (
            SELECT h.field, h.value FROM pgkv.hash_fields h
            WHERE h.id = v_id ORDER BY h.field OFFSET floor(random() * v_card + g * 0)::bigint LIMIT 1
        ) r;
    END IF;
END;
$$;

-- HSCAN: Iterate the fields of a hash matching a pattern, p_count at a time, in byte order.
-- Each row carries the cursor for the next call; 0 means the sweep is complete.
CREATE OR REPLACE FUNCTION pgkv.hscan(p_key TEXT, p_cursor BIGINT DEFAULT 0, p_pattern TEXT DEFAULT '*', p_count INTEGER DEFAULT 10)
RETURNS TABLE(cursor BIGINT, field TEXT, value JSONB)
LANGUAGE plpgsql
STABLE
SECURITY DEFINER
SET search_path = pgkv, pg_temp
AS $$
DECLARE
    v_peeked RECORD;
    v_id BIGINT;
    v_re TEXT := CASE WHEN COALESCE(p_pattern, '*') = '*' THEN NULL ELSE pgkv._glob_re(p_pattern) END;
BEGIN
    p_cursor := LEAST(COALESCE(p_cursor, 0), 4611686018427387904);  -- keeps cursor + count inside bigint
    v_peeked := pgkv._peek(p_key, 'hash');
    v_id := v_peeked.o_id;
    RETURN QUERY
    WITH page AS (
        SELECT h.field, h.value, row_number() OVER (ORDER BY h.field) AS rn
        FROM pgkv.hash_fields h
        WHERE h.id = v_id AND (v_re IS NULL OR h.field ~ v_re)
        ORDER BY h.field
        OFFSET p_cursor LIMIT p_count::bigint + 1
    )
    SELECT CASE WHEN (SELECT COUNT(*) FROM page) > p_count THEN p_cursor + p_count ELSE 0 END,
           page.field::text, page.value
    FROM page WHERE page.rn <= p_cursor + p_count ORDER BY page.field;
END;
$$;

