-- ============================================================================
-- Sets: one row per member in pgkv.set_members
-- ============================================================================

-- SADD: Add member(s) to a set. Returns the number of members added.
CREATE OR REPLACE FUNCTION pgkv.sadd(p_key TEXT, VARIADIC p_members TEXT[])
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
    v_added INTEGER;
BEGIN
    v_opened := pgkv._open(p_key, 'set', TRUE);
    v_id := v_opened.o_id;
    v_card := v_opened.o_card;
    v_expires_at := v_opened.o_expires_at;

    IF array_length(p_members, 1) = 1 THEN
        INSERT INTO pgkv.set_members (id, member) VALUES (v_id, p_members[1])
        ON CONFLICT (id, member) DO NOTHING;
    ELSE
        INSERT INTO pgkv.set_members (id, member)
        SELECT v_id, m FROM unnest(p_members) AS u(m) GROUP BY m
        ON CONFLICT (id, member) DO NOTHING;
    END IF;
    GET DIAGNOSTICS v_added = ROW_COUNT;

    IF v_added > 0 OR v_card = 0 THEN
        PERFORM pgkv._save(p_key, 'set', v_id, v_card + v_added, NULL, v_expires_at);
    END IF;
    RETURN v_added;
END;
$$;

-- SREM: Remove member(s) from a set. Returns the number of members removed.
CREATE OR REPLACE FUNCTION pgkv.srem(p_key TEXT, VARIADIC p_members TEXT[])
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
    v_opened := pgkv._open(p_key, 'set', FALSE);
    v_id := v_opened.o_id;
    v_card := v_opened.o_card;
    v_expires_at := v_opened.o_expires_at;
    v_new := v_opened.o_new;
    IF v_new THEN
        RETURN 0;
    END IF;

    DELETE FROM pgkv.set_members m WHERE m.id = v_id AND m.member = ANY(p_members);
    GET DIAGNOSTICS v_removed = ROW_COUNT;
    IF v_removed > 0 THEN
        PERFORM pgkv._save(p_key, 'set', v_id, v_card - v_removed, NULL, v_expires_at);
    END IF;
    RETURN v_removed;
END;
$$;

-- _SMEMBERS: the members of a set as an array, in byte order.
CREATE OR REPLACE FUNCTION pgkv._smembers(p_key TEXT)
RETURNS TEXT[]
LANGUAGE plpgsql
STABLE
AS $$
DECLARE
    v_peeked RECORD;
    v_id BIGINT;
BEGIN
    v_peeked := pgkv._peek(p_key, 'set');
    v_id := v_peeked.o_id;
    RETURN ARRAY(SELECT m.member::text FROM pgkv.set_members m WHERE m.id = v_id ORDER BY m.member);
END;
$$;

-- SMEMBERS: All members of a set, in byte order
CREATE OR REPLACE FUNCTION pgkv.smembers(p_key TEXT)
RETURNS SETOF JSONB
LANGUAGE plpgsql
STABLE
SECURITY DEFINER
SET search_path = pgkv, pg_temp
AS $$
BEGIN
    RETURN QUERY SELECT to_jsonb(u.m) FROM unnest(pgkv._smembers(p_key)) WITH ORDINALITY AS u(m, ord) ORDER BY u.ord;
END;
$$;

-- SISMEMBER: 1 if the member is in the set, 0 if not
CREATE OR REPLACE FUNCTION pgkv.sismember(p_key TEXT, p_member TEXT)
RETURNS INTEGER
LANGUAGE plpgsql
STABLE
SECURITY DEFINER
SET search_path = pgkv, pg_temp
AS $$
DECLARE
    v_type TEXT;
    v_found BOOLEAN;
BEGIN
    SELECT s.type, m.member IS NOT NULL INTO v_type, v_found
    FROM pgkv.store s
    LEFT JOIN pgkv.set_members m ON m.id = s.id AND m.member = p_member
    WHERE s.key = p_key AND (s.expires_at IS NULL OR s.expires_at >= clock_timestamp());

    IF v_type <> 'set' THEN
        PERFORM pgkv._wrongtype();
    END IF;
    RETURN CASE WHEN v_found THEN 1 ELSE 0 END;
END;
$$;

-- SMISMEMBER: For each member, in order, 1 if it is in the set and 0 if not
CREATE OR REPLACE FUNCTION pgkv.smismember(p_key TEXT, VARIADIC p_members TEXT[])
RETURNS SETOF INTEGER
LANGUAGE plpgsql
STABLE
SECURITY DEFINER
SET search_path = pgkv, pg_temp
AS $$
DECLARE
    v_peeked RECORD;
    v_id BIGINT;
BEGIN
    v_peeked := pgkv._peek(p_key, 'set');
    v_id := v_peeked.o_id;
    RETURN QUERY
    SELECT CASE WHEN m.member IS NULL THEN 0 ELSE 1 END
    FROM unnest(p_members) WITH ORDINALITY AS u(v, ord)
    LEFT JOIN pgkv.set_members m ON m.id = v_id AND m.member = u.v
    ORDER BY u.ord;
END;
$$;

-- SCARD: Number of members in a set
CREATE OR REPLACE FUNCTION pgkv.scard(p_key TEXT)
RETURNS INTEGER
LANGUAGE plpgsql
STABLE
SECURITY DEFINER
SET search_path = pgkv, pg_temp
AS $$
BEGIN
    RETURN COALESCE((SELECT p.o_card FROM pgkv._peek(p_key, 'set') p), 0);
END;
$$;

-- _SETOP: the result of SINTER, SUNION or SDIFF over p_keys, in byte order. Every key is
-- type-checked, including those after an empty one.
CREATE OR REPLACE FUNCTION pgkv._setop(p_op TEXT, p_keys TEXT[])
RETURNS TEXT[]
LANGUAGE plpgsql
STABLE
AS $$
DECLARE
    v_peeked RECORD;
    v_ids BIGINT[] := '{}';
    v_key TEXT;
    v_id BIGINT;
BEGIN
    -- 0 stands for a key that does not exist: no member has that id.
    FOREACH v_key IN ARRAY p_keys LOOP
        v_peeked := pgkv._peek(v_key, 'set');
        v_id := v_peeked.o_id;
        v_ids := v_ids || COALESCE(v_id, 0);
    END LOOP;

    IF p_op = 'union' THEN
        RETURN ARRAY(
            SELECT u.member::text
            FROM (SELECT DISTINCT m.member FROM pgkv.set_members m WHERE m.id = ANY(v_ids)) u
            ORDER BY u.member);
    ELSIF p_op = 'inter' THEN
        IF 0 = ANY(v_ids) THEN
            RETURN '{}';
        END IF;
        RETURN ARRAY(
            SELECT m.member::text FROM pgkv.set_members m
            WHERE m.id = ANY(v_ids)
            GROUP BY m.member
            HAVING COUNT(*) = (SELECT COUNT(DISTINCT i) FROM unnest(v_ids) AS i)
            ORDER BY m.member);
    END IF;
    RETURN ARRAY(
        SELECT a.member::text FROM pgkv.set_members a
        WHERE a.id = v_ids[1]
          AND NOT EXISTS (SELECT 1 FROM pgkv.set_members b WHERE b.id = ANY(v_ids[2:]) AND b.member = a.member)
        ORDER BY a.member);
END;
$$;

-- SINTER: Members common to all the given sets
CREATE OR REPLACE FUNCTION pgkv.sinter(VARIADIC p_keys TEXT[])
RETURNS SETOF JSONB
LANGUAGE plpgsql
STABLE
SECURITY DEFINER
SET search_path = pgkv, pg_temp
AS $$
BEGIN
    RETURN QUERY SELECT to_jsonb(u.m) FROM unnest(pgkv._setop('inter', p_keys)) WITH ORDINALITY AS u(m, ord) ORDER BY u.ord;
END;
$$;

-- SUNION: Members of any of the given sets
CREATE OR REPLACE FUNCTION pgkv.sunion(VARIADIC p_keys TEXT[])
RETURNS SETOF JSONB
LANGUAGE plpgsql
STABLE
SECURITY DEFINER
SET search_path = pgkv, pg_temp
AS $$
BEGIN
    RETURN QUERY SELECT to_jsonb(u.m) FROM unnest(pgkv._setop('union', p_keys)) WITH ORDINALITY AS u(m, ord) ORDER BY u.ord;
END;
$$;

-- SDIFF: Members of the first set that are in none of the others
CREATE OR REPLACE FUNCTION pgkv.sdiff(VARIADIC p_keys TEXT[])
RETURNS SETOF JSONB
LANGUAGE plpgsql
STABLE
SECURITY DEFINER
SET search_path = pgkv, pg_temp
AS $$
BEGIN
    RETURN QUERY SELECT to_jsonb(u.m) FROM unnest(pgkv._setop('diff', p_keys)) WITH ORDINALITY AS u(m, ord) ORDER BY u.ord;
END;
$$;

-- SINTERCARD: Size of the intersection of the given sets, counting no further than p_limit (0 = no limit)
CREATE OR REPLACE FUNCTION pgkv.sintercard(p_keys TEXT[], p_limit BIGINT DEFAULT 0)
RETURNS INTEGER
LANGUAGE plpgsql
STABLE
SECURITY DEFINER
SET search_path = pgkv, pg_temp
AS $$
DECLARE
    v_n INTEGER;
BEGIN
    IF p_limit < 0 THEN
        PERFORM pgkv._err('ERR LIMIT can''t be negative');
    END IF;
    v_n := cardinality(pgkv._setop('inter', p_keys));
    RETURN CASE WHEN p_limit > 0 THEN LEAST(v_n, p_limit) ELSE v_n END;
END;
$$;

-- _STORE_SET: replace p_destination with a set of p_members (the key is deleted if there are none).
CREATE OR REPLACE FUNCTION pgkv._store_set(p_destination TEXT, p_members TEXT[])
RETURNS INTEGER
LANGUAGE plpgsql
AS $$
DECLARE
    v_locked RECORD;
    v_type TEXT;
    v_id BIGINT;
    v_n INTEGER := COALESCE(cardinality(p_members), 0);
BEGIN
    v_locked := pgkv._lock_key(p_destination, TRUE);
    v_type := v_locked.o_type;
    v_id := v_locked.o_id;
    PERFORM pgkv._drop_elems(v_id, v_type);
    IF v_n > 0 THEN
        v_id := nextval('pgkv.collection_id');
        INSERT INTO pgkv.set_members (id, member) SELECT v_id, m FROM unnest(p_members) AS u(m);
    END IF;
    PERFORM pgkv._save(p_destination, 'set', v_id, v_n, NULL, NULL);
    RETURN v_n;
END;
$$;

-- _SETOP_STORE: SINTERSTORE, SUNIONSTORE, SDIFFSTORE.
CREATE OR REPLACE FUNCTION pgkv._setop_store(p_op TEXT, p_destination TEXT, p_keys TEXT[])
RETURNS INTEGER
LANGUAGE plpgsql
AS $$
BEGIN
    PERFORM pgkv._lock_keys(p_keys, ARRAY[p_destination]);
    RETURN pgkv._store_set(p_destination, pgkv._setop(p_op, p_keys));
END;
$$;

-- SINTERSTORE: Store the intersection of the given sets in p_destination. Returns its size.
CREATE OR REPLACE FUNCTION pgkv.sinterstore(p_destination TEXT, VARIADIC p_keys TEXT[])
RETURNS INTEGER
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pgkv, pg_temp
AS $$
BEGIN
    RETURN pgkv._setop_store('inter', p_destination, p_keys);
END;
$$;

-- SUNIONSTORE: Store the union of the given sets in p_destination. Returns its size.
CREATE OR REPLACE FUNCTION pgkv.sunionstore(p_destination TEXT, VARIADIC p_keys TEXT[])
RETURNS INTEGER
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pgkv, pg_temp
AS $$
BEGIN
    RETURN pgkv._setop_store('union', p_destination, p_keys);
END;
$$;

-- SDIFFSTORE: Store the difference of the given sets in p_destination. Returns its size.
CREATE OR REPLACE FUNCTION pgkv.sdiffstore(p_destination TEXT, VARIADIC p_keys TEXT[])
RETURNS INTEGER
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pgkv, pg_temp
AS $$
BEGIN
    RETURN pgkv._setop_store('diff', p_destination, p_keys);
END;
$$;

-- SMOVE: Move a member from one set to another. Returns 1 if moved, 0 if it was not in the source.
CREATE OR REPLACE FUNCTION pgkv.smove(p_source TEXT, p_destination TEXT, p_member TEXT)
RETURNS INTEGER
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pgkv, pg_temp
AS $$
BEGIN
    PERFORM pgkv._lock_keys(ARRAY[p_source, p_destination]);
    IF (SELECT p.o_id FROM pgkv._peek(p_source, 'set') p) IS NULL THEN
        RETURN 0;
    END IF;
    PERFORM pgkv._peek(p_destination, 'set');
    IF pgkv.sismember(p_source, p_member) = 0 THEN
        RETURN 0;
    END IF;
    IF p_source <> p_destination THEN
        PERFORM pgkv.srem(p_source, p_member);
        PERFORM pgkv.sadd(p_destination, p_member);
    END IF;
    RETURN 1;
END;
$$;

-- SRANDMEMBER: Random members of a set. A positive p_count returns up to that many distinct
-- members; a negative one returns exactly that many, possibly repeating.
CREATE OR REPLACE FUNCTION pgkv.srandmember(p_key TEXT, p_count BIGINT DEFAULT 1)
RETURNS SETOF TEXT
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pgkv, pg_temp
AS $$
DECLARE
    v_peeked RECORD;
    v_id BIGINT;
    v_card BIGINT;
BEGIN
    v_peeked := pgkv._peek(p_key, 'set');
    v_id := v_peeked.o_id;
    v_card := v_peeked.o_card;
    IF v_id IS NULL OR p_count = 0 THEN
        RETURN;
    END IF;
    IF p_count > 0 THEN
        RETURN QUERY SELECT m.member::text FROM pgkv.set_members m WHERE m.id = v_id ORDER BY random() LIMIT p_count;
    ELSE
        RETURN QUERY
        SELECT r.member::text
        FROM generate_series(1, -p_count) AS g
        CROSS JOIN LATERAL (
            SELECT m.member FROM pgkv.set_members m
            WHERE m.id = v_id ORDER BY m.member OFFSET floor(random() * v_card + g * 0)::bigint LIMIT 1
        ) r;
    END IF;
END;
$$;

-- SPOP: Remove and return up to p_count random members of a set
CREATE OR REPLACE FUNCTION pgkv.spop(p_key TEXT, p_count BIGINT DEFAULT 1)
RETURNS SETOF TEXT
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
    v_members TEXT[];
BEGIN
    IF p_count < 0 THEN
        PERFORM pgkv._err('ERR value is out of range, must be positive');
    END IF;
    v_opened := pgkv._open(p_key, 'set', FALSE);
    v_id := v_opened.o_id;
    v_card := v_opened.o_card;
    v_expires_at := v_opened.o_expires_at;
    v_new := v_opened.o_new;
    IF v_new OR p_count = 0 THEN
        RETURN;
    END IF;

    v_members := ARRAY(SELECT m.member::text FROM pgkv.set_members m WHERE m.id = v_id ORDER BY random() LIMIT p_count);
    DELETE FROM pgkv.set_members m WHERE m.id = v_id AND m.member = ANY(v_members);
    PERFORM pgkv._save(p_key, 'set', v_id, v_card - cardinality(v_members), NULL, v_expires_at);
    RETURN QUERY SELECT u.m FROM unnest(v_members) AS u(m);
END;
$$;

-- SSCAN: Iterate the members of a set matching a pattern, p_count at a time, in byte order.
-- Each row carries the cursor for the next call; 0 means the sweep is complete.
CREATE OR REPLACE FUNCTION pgkv.sscan(p_key TEXT, p_cursor BIGINT DEFAULT 0, p_pattern TEXT DEFAULT '*', p_count INTEGER DEFAULT 10)
RETURNS TABLE(cursor BIGINT, member JSONB)
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
    p_cursor := LEAST(p_cursor, 4611686018427387904);  -- keeps cursor + count inside bigint
    v_peeked := pgkv._peek(p_key, 'set');
    v_id := v_peeked.o_id;
    RETURN QUERY
    WITH page AS (
        SELECT m.member AS mem, row_number() OVER (ORDER BY m.member) AS rn
        FROM pgkv.set_members m
        WHERE m.id = v_id AND (v_re IS NULL OR m.member ~ v_re)
        ORDER BY m.member
        OFFSET p_cursor LIMIT p_count::bigint + 1
    )
    SELECT CASE WHEN (SELECT COUNT(*) FROM page) > p_count THEN p_cursor + p_count ELSE 0 END,
           to_jsonb(page.mem::text)
    FROM page WHERE page.rn <= p_cursor + p_count ORDER BY page.mem;
END;
$$;

