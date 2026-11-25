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
        PERFORM pgkv._wrongtype();
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
STABLE
SECURITY DEFINER
SET search_path = pgkv, pg_temp
AS $$
DECLARE
    v_value JSONB;
    v_type TEXT;
BEGIN
    SELECT s.value, s.type INTO v_value, v_type FROM pgkv.store s
    WHERE s.key = p_key AND (s.expires_at IS NULL OR s.expires_at >= clock_timestamp());
    IF v_type IS NULL THEN RETURN NULL; END IF;
    IF v_type != 'json' THEN PERFORM pgkv._wrongtype(); END IF;
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
    IF v_type != 'json' THEN PERFORM pgkv._wrongtype(); END IF;
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
STABLE
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
STABLE
SECURITY DEFINER
SET search_path = pgkv, pg_temp
AS $$
DECLARE
    v_value JSONB;
BEGIN
    v_value := pgkv.json_get(p_key, p_path);
    IF v_value IS NULL THEN RETURN; END IF;
    IF jsonb_typeof(v_value) != 'object' THEN RAISE EXCEPTION 'WRONGTYPE JSON value is not an object' USING ERRCODE = 'KV001'; END IF;
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
        IF v_type != 'json' THEN PERFORM pgkv._wrongtype(); END IF;
        v_array := CASE WHEN v_root THEN v_value ELSE v_value #> p_path END;
        IF v_array IS NULL THEN v_array := '[]'::jsonb; END IF;
    END IF;
    IF jsonb_typeof(v_array) != 'array' THEN RAISE EXCEPTION 'WRONGTYPE JSON value is not an array' USING ERRCODE = 'KV001'; END IF;
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

