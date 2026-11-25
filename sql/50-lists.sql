-- ============================================================================
-- Lists: one row per element in pgkv.list_items, at positions head .. head + card - 1.
-- Pushing and popping at either end touch only the rows they add or remove.
-- ============================================================================

-- _LIST_REPACK: rewrite the elements at positions p_from .. p_to so that they sit at
-- consecutive positions starting at p_start, keeping their order.
CREATE OR REPLACE FUNCTION pgkv._list_repack(p_id BIGINT, p_from BIGINT, p_to BIGINT, p_start BIGINT)
RETURNS VOID
LANGUAGE plpgsql
AS $$
DECLARE
    v_values TEXT[];
BEGIN
    WITH moved AS (
        DELETE FROM pgkv.list_items i WHERE i.id = p_id AND i.pos BETWEEN p_from AND p_to RETURNING i.pos, i.value
    )
    SELECT array_agg(m.value ORDER BY m.pos) INTO v_values FROM moved m;

    IF v_values IS NOT NULL THEN
        INSERT INTO pgkv.list_items (id, pos, value)
        SELECT p_id, p_start + u.ord - 1, u.v FROM unnest(v_values) WITH ORDINALITY AS u(v, ord);
    END IF;
END;
$$;

-- _PUSH: add values to the head (p_left) or tail of a list. Each value is pushed in turn,
-- so the last one given ends up outermost. p_must_exist is LPUSHX / RPUSHX.
CREATE OR REPLACE FUNCTION pgkv._push(p_key TEXT, p_values TEXT[], p_left BOOLEAN, p_must_exist BOOLEAN)
RETURNS INTEGER
LANGUAGE plpgsql
AS $$
DECLARE
    v_opened RECORD;
    v_id BIGINT;
    v_card BIGINT;
    v_head BIGINT;
    v_expires_at TIMESTAMPTZ;
    v_new BOOLEAN;
    v_n INTEGER := COALESCE(array_length(p_values, 1), 0);
BEGIN
    v_opened := pgkv._open(p_key, 'list', NOT p_must_exist);
    v_id := v_opened.o_id;
    v_card := v_opened.o_card;
    v_head := v_opened.o_head;
    v_expires_at := v_opened.o_expires_at;
    v_new := v_opened.o_new;
    IF v_new AND p_must_exist THEN
        RETURN 0;
    END IF;

    IF v_n = 1 THEN
        INSERT INTO pgkv.list_items (id, pos, value)
        VALUES (v_id, CASE WHEN p_left THEN v_head - 1 ELSE v_head + v_card END, p_values[1]);
    ELSIF p_left THEN
        INSERT INTO pgkv.list_items (id, pos, value)
        SELECT v_id, v_head - u.ord, u.v FROM unnest(p_values) WITH ORDINALITY AS u(v, ord);
    ELSE
        INSERT INTO pgkv.list_items (id, pos, value)
        SELECT v_id, v_head + v_card + u.ord - 1, u.v FROM unnest(p_values) WITH ORDINALITY AS u(v, ord);
    END IF;

    IF p_left THEN
        v_head := v_head - v_n;
    END IF;
    PERFORM pgkv._save(p_key, 'list', v_id, v_card + v_n, v_head, v_expires_at);
    RETURN (v_card + v_n)::integer;
END;
$$;

-- LPUSH: Insert value(s) at the head of a list. Returns the length of the list afterwards.
CREATE OR REPLACE FUNCTION pgkv.lpush(p_key TEXT, VARIADIC p_values TEXT[])
RETURNS INTEGER
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pgkv, pg_temp
AS $$
BEGIN
    RETURN pgkv._push(p_key, p_values, TRUE, FALSE);
END;
$$;

-- RPUSH: Insert value(s) at the tail of a list. Returns the length of the list afterwards.
CREATE OR REPLACE FUNCTION pgkv.rpush(p_key TEXT, VARIADIC p_values TEXT[])
RETURNS INTEGER
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pgkv, pg_temp
AS $$
BEGIN
    RETURN pgkv._push(p_key, p_values, FALSE, FALSE);
END;
$$;

-- LPUSHX: LPUSH only if the list exists
CREATE OR REPLACE FUNCTION pgkv.lpushx(p_key TEXT, VARIADIC p_values TEXT[])
RETURNS INTEGER
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pgkv, pg_temp
AS $$
BEGIN
    RETURN pgkv._push(p_key, p_values, TRUE, TRUE);
END;
$$;

-- RPUSHX: RPUSH only if the list exists
CREATE OR REPLACE FUNCTION pgkv.rpushx(p_key TEXT, VARIADIC p_values TEXT[])
RETURNS INTEGER
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pgkv, pg_temp
AS $$
BEGIN
    RETURN pgkv._push(p_key, p_values, FALSE, TRUE);
END;
$$;

-- _POP: remove up to p_count elements from the head (p_left) or tail of a list and return
-- them in the order popped. NULL if the key does not exist.
CREATE OR REPLACE FUNCTION pgkv._pop(p_key TEXT, p_count BIGINT, p_left BOOLEAN)
RETURNS TEXT[]
LANGUAGE plpgsql
AS $$
DECLARE
    v_opened RECORD;
    v_id BIGINT;
    v_card BIGINT;
    v_head BIGINT;
    v_expires_at TIMESTAMPTZ;
    v_new BOOLEAN;
    v_n BIGINT;
    v_values TEXT[];
BEGIN
    IF p_count < 0 THEN
        PERFORM pgkv._err('ERR value is out of range, must be positive');
    END IF;

    v_opened := pgkv._open(p_key, 'list', FALSE);
    v_id := v_opened.o_id;
    v_card := v_opened.o_card;
    v_head := v_opened.o_head;
    v_expires_at := v_opened.o_expires_at;
    v_new := v_opened.o_new;
    IF v_new THEN
        RETURN NULL;
    END IF;

    v_n := LEAST(p_count, v_card);
    IF v_n = 0 THEN
        RETURN '{}';
    END IF;

    IF v_n = 1 THEN
        DELETE FROM pgkv.list_items i
        WHERE i.id = v_id AND i.pos = CASE WHEN p_left THEN v_head ELSE v_head + v_card - 1 END
        RETURNING ARRAY[i.value] INTO v_values;
        IF p_left THEN
            v_head := v_head + 1;
        END IF;
    ELSIF p_left THEN
        WITH popped AS (
            DELETE FROM pgkv.list_items i WHERE i.id = v_id AND i.pos < v_head + v_n RETURNING i.pos, i.value
        )
        SELECT array_agg(p.value ORDER BY p.pos) INTO v_values FROM popped p;
        v_head := v_head + v_n;
    ELSE
        WITH popped AS (
            DELETE FROM pgkv.list_items i WHERE i.id = v_id AND i.pos >= v_head + v_card - v_n RETURNING i.pos, i.value
        )
        SELECT array_agg(p.value ORDER BY p.pos DESC) INTO v_values FROM popped p;
    END IF;

    PERFORM pgkv._save(p_key, 'list', v_id, v_card - v_n, v_head, v_expires_at);
    RETURN v_values;
END;
$$;

-- LPOP: Remove and return the first element(s) of a list
CREATE OR REPLACE FUNCTION pgkv.lpop(p_key TEXT, p_count BIGINT DEFAULT 1)
RETURNS SETOF JSONB
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pgkv, pg_temp
AS $$
BEGIN
    RETURN QUERY SELECT to_jsonb(u.v) FROM unnest(pgkv._pop(p_key, p_count, TRUE)) WITH ORDINALITY AS u(v, ord) ORDER BY u.ord;
END;
$$;

-- RPOP: Remove and return the last element(s) of a list
CREATE OR REPLACE FUNCTION pgkv.rpop(p_key TEXT, p_count BIGINT DEFAULT 1)
RETURNS SETOF JSONB
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pgkv, pg_temp
AS $$
BEGIN
    RETURN QUERY SELECT to_jsonb(u.v) FROM unnest(pgkv._pop(p_key, p_count, FALSE)) WITH ORDINALITY AS u(v, ord) ORDER BY u.ord;
END;
$$;

-- LLEN: Length of a list
CREATE OR REPLACE FUNCTION pgkv.llen(p_key TEXT)
RETURNS INTEGER
LANGUAGE plpgsql
STABLE
SECURITY DEFINER
SET search_path = pgkv, pg_temp
AS $$
BEGIN
    RETURN COALESCE((SELECT p.o_card FROM pgkv._peek(p_key, 'list') p), 0);
END;
$$;

-- _LRANGE: the elements of a list between two indexes, as an array.
CREATE OR REPLACE FUNCTION pgkv._lrange(p_key TEXT, p_start BIGINT, p_stop BIGINT)
RETURNS TEXT[]
LANGUAGE plpgsql
STABLE
AS $$
DECLARE
    v_peeked RECORD;
    v_id BIGINT;
    v_card BIGINT;
    v_head BIGINT;
    v_off BIGINT;
    v_len BIGINT;
BEGIN
    v_peeked := pgkv._peek(p_key, 'list');
    v_id := v_peeked.o_id;
    v_card := v_peeked.o_card;
    v_head := v_peeked.o_head;
    IF v_id IS NULL THEN
        RETURN '{}';
    END IF;
    SELECT r.o_off, r.o_len INTO v_off, v_len FROM pgkv._range(p_start, p_stop, v_card) r;
    IF v_len = 0 THEN
        RETURN '{}';
    END IF;
    RETURN ARRAY(
        SELECT i.value FROM pgkv.list_items i
        WHERE i.id = v_id AND i.pos BETWEEN v_head + v_off AND v_head + v_off + v_len - 1
        ORDER BY i.pos
    );
END;
$$;

-- LRANGE: Get a range of elements from a list (indexes are zero-based, negative from the end)
CREATE OR REPLACE FUNCTION pgkv.lrange(p_key TEXT, p_start BIGINT, p_stop BIGINT)
RETURNS SETOF JSONB
LANGUAGE plpgsql
STABLE
SECURITY DEFINER
SET search_path = pgkv, pg_temp
AS $$
BEGIN
    RETURN QUERY SELECT to_jsonb(u.v) FROM unnest(pgkv._lrange(p_key, p_start, p_stop)) WITH ORDINALITY AS u(v, ord) ORDER BY u.ord;
END;
$$;

-- LINDEX: Get an element of a list by its index
CREATE OR REPLACE FUNCTION pgkv.lindex(p_key TEXT, p_index BIGINT)
RETURNS JSONB
LANGUAGE plpgsql
STABLE
SECURITY DEFINER
SET search_path = pgkv, pg_temp
AS $$
DECLARE
    v_peeked RECORD;
    v_id BIGINT;
    v_card BIGINT;
    v_head BIGINT;
BEGIN
    v_peeked := pgkv._peek(p_key, 'list');
    v_id := v_peeked.o_id;
    v_card := v_peeked.o_card;
    v_head := v_peeked.o_head;
    IF v_id IS NULL OR p_index >= v_card OR p_index < -v_card THEN
        RETURN NULL;
    END IF;
    RETURN (
        SELECT to_jsonb(i.value) FROM pgkv.list_items i
        WHERE i.id = v_id AND i.pos = v_head + CASE WHEN p_index < 0 THEN v_card + p_index ELSE p_index END
    );
END;
$$;

-- LSET: Set the value of an element of a list by its index
CREATE OR REPLACE FUNCTION pgkv.lset(p_key TEXT, p_index BIGINT, p_value TEXT)
RETURNS TEXT
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pgkv, pg_temp
AS $$
DECLARE
    v_opened RECORD;
    v_id BIGINT;
    v_card BIGINT;
    v_head BIGINT;
    v_new BOOLEAN;
BEGIN
    v_opened := pgkv._open(p_key, 'list', FALSE);
    v_id := v_opened.o_id;
    v_card := v_opened.o_card;
    v_head := v_opened.o_head;
    v_new := v_opened.o_new;
    IF v_new THEN
        PERFORM pgkv._err('ERR no such key');
    END IF;
    IF p_index >= v_card OR p_index < -v_card THEN
        PERFORM pgkv._err('ERR index out of range');
    END IF;

    UPDATE pgkv.list_items i SET value = p_value
    WHERE i.id = v_id AND i.pos = v_head + CASE WHEN p_index < 0 THEN v_card + p_index ELSE p_index END;
    RETURN 'OK';
END;
$$;

-- LTRIM: Trim a list to the given range of indexes
CREATE OR REPLACE FUNCTION pgkv.ltrim(p_key TEXT, p_start BIGINT, p_stop BIGINT)
RETURNS TEXT
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pgkv, pg_temp
AS $$
DECLARE
    v_opened RECORD;
    v_id BIGINT;
    v_card BIGINT;
    v_head BIGINT;
    v_expires_at TIMESTAMPTZ;
    v_new BOOLEAN;
    v_off BIGINT;
    v_len BIGINT;
BEGIN
    v_opened := pgkv._open(p_key, 'list', FALSE);
    v_id := v_opened.o_id;
    v_card := v_opened.o_card;
    v_head := v_opened.o_head;
    v_expires_at := v_opened.o_expires_at;
    v_new := v_opened.o_new;
    IF v_new THEN
        RETURN 'OK';
    END IF;

    SELECT r.o_off, r.o_len INTO v_off, v_len FROM pgkv._range(p_start, p_stop, v_card) r;
    IF v_len = v_card THEN
        RETURN 'OK';
    END IF;
    IF v_len = 0 THEN
        DELETE FROM pgkv.list_items i WHERE i.id = v_id;
    ELSE
        DELETE FROM pgkv.list_items i
        WHERE i.id = v_id AND (i.pos < v_head + v_off OR i.pos >= v_head + v_off + v_len);
    END IF;
    PERFORM pgkv._save(p_key, 'list', v_id, v_len, v_head + v_off, v_expires_at);
    RETURN 'OK';
END;
$$;

-- LREM: Remove elements equal to p_value: the first p_count from the head if p_count > 0,
-- the last -p_count from the tail if p_count < 0, all of them if p_count = 0.
-- Returns the number removed. Rewrites the shorter side of the list around the removals.
CREATE OR REPLACE FUNCTION pgkv.lrem(p_key TEXT, p_count BIGINT, p_value TEXT)
RETURNS INTEGER
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pgkv, pg_temp
AS $$
DECLARE
    v_opened RECORD;
    v_id BIGINT;
    v_card BIGINT;
    v_head BIGINT;
    v_expires_at TIMESTAMPTZ;
    v_new BOOLEAN;
    v_positions BIGINT[];
    v_n INTEGER;
    v_first BIGINT;
    v_last BIGINT;
    v_tail BIGINT;
BEGIN
    v_opened := pgkv._open(p_key, 'list', FALSE);
    v_id := v_opened.o_id;
    v_card := v_opened.o_card;
    v_head := v_opened.o_head;
    v_expires_at := v_opened.o_expires_at;
    v_new := v_opened.o_new;
    IF v_new THEN
        RETURN 0;
    END IF;

    IF p_count >= 0 THEN
        v_positions := ARRAY(
            SELECT i.pos FROM pgkv.list_items i WHERE i.id = v_id AND i.value = p_value
            ORDER BY i.pos LIMIT NULLIF(p_count, 0));
    ELSE
        v_positions := ARRAY(
            SELECT i.pos FROM pgkv.list_items i WHERE i.id = v_id AND i.value = p_value
            ORDER BY i.pos DESC LIMIT -p_count);
    END IF;
    v_n := cardinality(v_positions);
    IF v_n = 0 THEN
        RETURN 0;
    END IF;

    DELETE FROM pgkv.list_items i WHERE i.id = v_id AND i.pos = ANY(v_positions);
    SELECT min(p), max(p) INTO v_first, v_last FROM unnest(v_positions) AS p;
    v_tail := v_head + v_card - 1;

    IF v_n < v_card THEN
        IF v_tail - v_first <= v_last - v_head THEN
            -- Close the gaps by pulling the tail side down.
            PERFORM pgkv._list_repack(v_id, v_first, v_tail, v_first);
        ELSE
            -- Close the gaps by pushing the head side up.
            PERFORM pgkv._list_repack(v_id, v_head, v_last, v_head + v_n);
            v_head := v_head + v_n;
        END IF;
    END IF;
    PERFORM pgkv._save(p_key, 'list', v_id, v_card - v_n, v_head, v_expires_at);
    RETURN v_n;
END;
$$;

-- LINSERT: Insert a value before or after the first element equal to p_pivot.
-- Returns the length of the list afterwards, -1 if the pivot was not found, 0 if the key
-- does not exist.
CREATE OR REPLACE FUNCTION pgkv.linsert(p_key TEXT, p_before_after TEXT, p_pivot TEXT, p_value TEXT)
RETURNS INTEGER
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pgkv, pg_temp
AS $$
DECLARE
    v_opened RECORD;
    v_id BIGINT;
    v_card BIGINT;
    v_head BIGINT;
    v_expires_at TIMESTAMPTZ;
    v_new BOOLEAN;
    v_where TEXT := upper(p_before_after);
    v_pos BIGINT;
BEGIN
    IF v_where NOT IN ('BEFORE', 'AFTER') THEN
        PERFORM pgkv._err('ERR syntax error');
    END IF;

    v_opened := pgkv._open(p_key, 'list', FALSE);
    v_id := v_opened.o_id;
    v_card := v_opened.o_card;
    v_head := v_opened.o_head;
    v_expires_at := v_opened.o_expires_at;
    v_new := v_opened.o_new;
    IF v_new THEN
        RETURN 0;
    END IF;

    SELECT i.pos INTO v_pos FROM pgkv.list_items i WHERE i.id = v_id AND i.value = p_pivot
    ORDER BY i.pos LIMIT 1;
    IF v_pos IS NULL THEN
        RETURN -1;
    END IF;

    -- v_pos becomes the position the new element takes if the tail side moves up by one.
    IF v_where = 'AFTER' THEN
        v_pos := v_pos + 1;
    END IF;
    IF v_head + v_card - v_pos <= v_pos - v_head THEN
        PERFORM pgkv._list_repack(v_id, v_pos, v_head + v_card - 1, v_pos + 1);
    ELSE
        PERFORM pgkv._list_repack(v_id, v_head, v_pos - 1, v_head - 1);
        v_head := v_head - 1;
        v_pos := v_pos - 1;
    END IF;
    INSERT INTO pgkv.list_items (id, pos, value) VALUES (v_id, v_pos, p_value);
    PERFORM pgkv._save(p_key, 'list', v_id, v_card + 1, v_head, v_expires_at);
    RETURN (v_card + 1)::integer;
END;
$$;

-- _LPOS: indexes of the elements equal to p_value. p_rank picks the first match to return
-- (2 = the second; negative searches from the tail), p_count how many (0 = all), p_maxlen
-- how many elements to compare (0 = all).
CREATE OR REPLACE FUNCTION pgkv._lpos(p_key TEXT, p_value TEXT, p_rank BIGINT, p_count BIGINT, p_maxlen BIGINT)
RETURNS BIGINT[]
LANGUAGE plpgsql
STABLE
AS $$
DECLARE
    v_peeked RECORD;
    v_id BIGINT;
    v_card BIGINT;
    v_head BIGINT;
BEGIN
    IF p_rank = 0 THEN
        PERFORM pgkv._err('ERR RANK can''t be zero: use 1 to start from the first match, 2 from the second ... or use negative to start from the end of the list');
    END IF;
    IF p_count < 0 THEN
        PERFORM pgkv._err('ERR COUNT can''t be negative');
    END IF;
    IF p_maxlen < 0 THEN
        PERFORM pgkv._err('ERR MAXLEN can''t be negative');
    END IF;

    v_peeked := pgkv._peek(p_key, 'list');
    v_id := v_peeked.o_id;
    v_card := v_peeked.o_card;
    v_head := v_peeked.o_head;
    IF v_id IS NULL THEN
        RETURN NULL;
    END IF;

    IF p_rank > 0 THEN
        RETURN ARRAY(
            SELECT i.pos - v_head FROM pgkv.list_items i
            WHERE i.id = v_id AND i.value = p_value
              AND (p_maxlen = 0 OR i.pos < v_head + p_maxlen)
            ORDER BY i.pos OFFSET p_rank - 1 LIMIT NULLIF(p_count, 0));
    END IF;
    RETURN ARRAY(
        SELECT i.pos - v_head FROM pgkv.list_items i
        WHERE i.id = v_id AND i.value = p_value
          AND (p_maxlen = 0 OR i.pos >= v_head + v_card - p_maxlen)
        ORDER BY i.pos DESC OFFSET -p_rank - 1 LIMIT NULLIF(p_count, 0));
END;
$$;

-- LPOS: Index of the first element equal to p_value (NULL if there is none).
-- p_rank 2 gives the second match, -1 the last.
CREATE OR REPLACE FUNCTION pgkv.lpos(p_key TEXT, p_value TEXT, p_rank BIGINT DEFAULT 1)
RETURNS INTEGER
LANGUAGE plpgsql
STABLE
SECURITY DEFINER
SET search_path = pgkv, pg_temp
AS $$
BEGIN
    RETURN (pgkv._lpos(p_key, p_value, p_rank, 1, 0))[1]::integer;
END;
$$;

-- LMOVE: Atomically move an element from one end of a list to an end of another (or the
-- same) list. p_from_side and p_to_side are LEFT or RIGHT. Returns the element, NULL if the
-- source does not exist.
CREATE OR REPLACE FUNCTION pgkv.lmove(p_source TEXT, p_destination TEXT, p_from_side TEXT, p_to_side TEXT)
RETURNS JSONB
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pgkv, pg_temp
AS $$
DECLARE
    v_from TEXT := upper(p_from_side);
    v_to TEXT := upper(p_to_side);
    v_value TEXT;
BEGIN
    IF v_from NOT IN ('LEFT', 'RIGHT') OR v_to NOT IN ('LEFT', 'RIGHT') THEN
        PERFORM pgkv._err('ERR syntax error');
    END IF;

    -- Lock both keys in key order, and reject a destination of the wrong type before
    -- anything is popped.
    PERFORM pgkv._lock_keys(ARRAY[p_source, p_destination]);
    IF (SELECT p.o_id FROM pgkv._peek(p_source, 'list') p) IS NULL THEN
        RETURN NULL;
    END IF;
    PERFORM pgkv._peek(p_destination, 'list');

    IF p_source = p_destination AND (SELECT p.o_card FROM pgkv._peek(p_source, 'list') p) = 1 THEN
        -- Rotating a one-element list changes nothing; popping it would drop the key's expiry.
        RETURN to_jsonb((pgkv._lrange(p_source, 0, 0))[1]);
    END IF;

    v_value := (pgkv._pop(p_source, 1, v_from = 'LEFT'))[1];
    IF v_value IS NULL THEN
        RETURN NULL;
    END IF;
    PERFORM pgkv._push(p_destination, ARRAY[v_value], v_to = 'LEFT', FALSE);
    RETURN to_jsonb(v_value);
END;
$$;

-- RPOPLPUSH: Move the last element of a list to the head of another
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

