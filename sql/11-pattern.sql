-- REDIS_TO_PG_PATTERN: Convert Redis glob pattern to PostgreSQL LIKE pattern.
-- Returns NULL if the pattern contains character classes (pgkv matches those as a regular expression).
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

