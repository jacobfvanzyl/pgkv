-- ============================================================================
-- Sorted sets: one row per member in pgkv.zset_members. Scores are doubles, as in Redis;
-- members with equal scores are ordered by byte.
-- ============================================================================

-- ZADD_OPTS: ZADD with Redis options. p_pairs is score, member, score, member, ...
--   p_nx: only add new members       p_xx: only update existing members
--   p_gt / p_lt: update an existing member only if the new score is greater / less
--   p_ch: return the number of members added or changed, not just added
--   p_incr: add the score to the member's score (one pair only); returns the new score
-- Returns a JSON number: the count, or with p_incr the new score as a string, or JSON
-- null if p_incr was stopped by another option.
CREATE OR REPLACE FUNCTION pgkv.zadd_opts(
    p_key TEXT,
    p_pairs TEXT[],
    p_nx BOOLEAN DEFAULT FALSE,
    p_xx BOOLEAN DEFAULT FALSE,
    p_gt BOOLEAN DEFAULT FALSE,
    p_lt BOOLEAN DEFAULT FALSE,
    p_ch BOOLEAN DEFAULT FALSE,
    p_incr BOOLEAN DEFAULT FALSE
)
RETURNS JSONB
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pgkv, pg_temp
AS $$
DECLARE
    v_opened RECORD;
    v_n INTEGER := COALESCE(array_length(p_pairs, 1), 0);
    v_scores DOUBLE PRECISION[] := '{}';
    v_id BIGINT;
    v_card BIGINT;
    v_expires_at TIMESTAMPTZ;
    v_new BOOLEAN;
    v_added INTEGER := 0;
    v_changed INTEGER := 0;
    v_current DOUBLE PRECISION;
    v_score DOUBLE PRECISION;
    v_i INTEGER;
BEGIN
    IF p_nx AND p_xx THEN
        PERFORM pgkv._err('ERR XX and NX options at the same time are not compatible');
    END IF;
    IF (p_gt AND p_lt) OR (p_nx AND (p_gt OR p_lt)) THEN
        PERFORM pgkv._err('ERR GT, LT, and/or NX options at the same time are not compatible');
    END IF;
    IF v_n = 0 OR v_n % 2 <> 0 THEN
        PERFORM pgkv._err('ERR syntax error');
    END IF;
    IF p_incr AND v_n > 2 THEN
        PERFORM pgkv._err('ERR INCR option supports a single increment-element pair');
    END IF;
    -- Every score is checked before anything is written.
    FOR v_i IN 1..v_n BY 2 LOOP
        v_scores := v_scores || pgkv._float(p_pairs[v_i]);
    END LOOP;

    v_opened := pgkv._open(p_key, 'zset', NOT p_xx);
    v_id := v_opened.o_id;
    v_card := v_opened.o_card;
    v_expires_at := v_opened.o_expires_at;
    v_new := v_opened.o_new;
    IF v_new AND p_xx THEN
        RETURN CASE WHEN p_incr THEN 'null'::jsonb ELSE '0'::jsonb END;
    END IF;

    FOR v_i IN 1..v_n / 2 LOOP
        v_score := v_scores[v_i];
        SELECT z.score INTO v_current FROM pgkv.zset_members z WHERE z.id = v_id AND z.member = p_pairs[v_i * 2];
        IF NOT FOUND THEN
            IF p_xx THEN
                IF p_incr THEN RETURN 'null'::jsonb; END IF;
                CONTINUE;
            END IF;
            INSERT INTO pgkv.zset_members (id, member, score) VALUES (v_id, p_pairs[v_i * 2], v_score + 0);
            v_added := v_added + 1;
        ELSE
            IF p_incr THEN
                v_score := pgkv._fadd(v_current, v_score);
                IF v_score = 'NaN' THEN
                    PERFORM pgkv._err('ERR resulting score is not a number (NaN)');
                END IF;
            END IF;
            IF p_nx OR (p_gt AND v_score <= v_current) OR (p_lt AND v_score >= v_current) THEN
                IF p_incr THEN RETURN 'null'::jsonb; END IF;
                CONTINUE;
            END IF;
            IF v_score <> v_current THEN
                UPDATE pgkv.zset_members z SET score = v_score WHERE z.id = v_id AND z.member = p_pairs[v_i * 2];
                v_changed := v_changed + 1;
            END IF;
        END IF;
    END LOOP;

    IF v_added > 0 OR v_new THEN
        PERFORM pgkv._save(p_key, 'zset', v_id, v_card + v_added, NULL, v_expires_at);
    END IF;
    IF p_incr THEN
        RETURN to_jsonb(pgkv._fmt(v_score));
    END IF;
    RETURN to_jsonb(CASE WHEN p_ch THEN v_added + v_changed ELSE v_added END);
END;
$$;

-- ZADD: Add member(s) with scores to a sorted set, or update their scores.
-- Arguments are score, member, score, member, ... Returns the number of members added.
CREATE OR REPLACE FUNCTION pgkv.zadd(p_key TEXT, VARIADIC p_pairs TEXT[])
RETURNS INTEGER
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pgkv, pg_temp
AS $$
DECLARE
    v_opened RECORD;
    v_n INTEGER := COALESCE(array_length(p_pairs, 1), 0);
    v_id BIGINT;
    v_card BIGINT;
    v_expires_at TIMESTAMPTZ;
    v_new BOOLEAN;
    v_added INTEGER;
    v_scores DOUBLE PRECISION[] := '{}';
    v_i INTEGER;
BEGIN
    IF v_n = 0 OR v_n % 2 <> 0 THEN
        PERFORM pgkv._err('ERR syntax error');
    END IF;
    -- Every score is checked before the key is looked at, as in Redis. A score of -0 is stored as 0.
    FOR v_i IN 1..v_n BY 2 LOOP
        v_scores := v_scores || (pgkv._float(p_pairs[v_i]) + 0);
    END LOOP;

    v_opened := pgkv._open(p_key, 'zset', TRUE);
    v_id := v_opened.o_id;
    v_card := v_opened.o_card;
    v_expires_at := v_opened.o_expires_at;
    v_new := v_opened.o_new;

    IF v_n = 2 THEN
        INSERT INTO pgkv.zset_members AS z (id, member, score)
        VALUES (v_id, p_pairs[2], v_scores[1])
        ON CONFLICT (id, member) DO UPDATE SET score = EXCLUDED.score
        WHERE z.score <> EXCLUDED.score
        RETURNING (z.xmax = 0)::integer INTO v_added;
        v_added := COALESCE(v_added, 0);
    ELSE
        -- A member given twice takes its last score. Unchanged scores are not rewritten.
        WITH written AS (
            INSERT INTO pgkv.zset_members AS z (id, member, score)
            SELECT v_id, d.m, d.s
            FROM (
                SELECT DISTINCT ON (p_pairs[i + 1]) p_pairs[i + 1] AS m, v_scores[(i + 1) / 2] AS s
                FROM generate_series(1, v_n, 2) AS i
                ORDER BY p_pairs[i + 1], i DESC
            ) d
            ON CONFLICT (id, member) DO UPDATE SET score = EXCLUDED.score
            WHERE z.score <> EXCLUDED.score
            RETURNING (z.xmax = 0) AS inserted
        )
        SELECT (COUNT(*) FILTER (WHERE inserted))::integer INTO v_added FROM written;
    END IF;

    IF v_added > 0 THEN
        PERFORM pgkv._save(p_key, 'zset', v_id, v_card + v_added, NULL, v_expires_at);
    END IF;
    RETURN v_added;
END;
$$;

-- ZINCRBY: Add p_increment to the score of a member, adding the member if needed. Returns the new score.
CREATE OR REPLACE FUNCTION pgkv.zincrby(p_key TEXT, p_increment DOUBLE PRECISION, p_member TEXT)
RETURNS DOUBLE PRECISION
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pgkv, pg_temp
AS $$
DECLARE
    v_opened RECORD;
    v_id BIGINT;
    v_card BIGINT;
    v_expires_at TIMESTAMPTZ;
    v_current DOUBLE PRECISION;
    v_score DOUBLE PRECISION;
BEGIN
    IF p_increment IS NULL OR p_increment = 'NaN' THEN
        PERFORM pgkv._err('ERR value is not a valid float');
    END IF;
    v_opened := pgkv._open(p_key, 'zset', TRUE);
    v_id := v_opened.o_id;
    v_card := v_opened.o_card;
    v_expires_at := v_opened.o_expires_at;

    SELECT z.score INTO v_current FROM pgkv.zset_members z WHERE z.id = v_id AND z.member = p_member;
    IF NOT FOUND THEN
        INSERT INTO pgkv.zset_members (id, member, score) VALUES (v_id, p_member, p_increment + 0);
        PERFORM pgkv._save(p_key, 'zset', v_id, v_card + 1, NULL, v_expires_at);
        RETURN p_increment;
    END IF;

    v_score := pgkv._fadd(v_current, p_increment);
    IF v_score = 'NaN' THEN
        PERFORM pgkv._err('ERR resulting score is not a number (NaN)');
    END IF;
    UPDATE pgkv.zset_members z SET score = v_score WHERE z.id = v_id AND z.member = p_member;
    RETURN v_score;
END;
$$;

-- ZREM: Remove member(s) from a sorted set. Returns the number removed.
CREATE OR REPLACE FUNCTION pgkv.zrem(p_key TEXT, VARIADIC p_members TEXT[])
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
    v_opened := pgkv._open(p_key, 'zset', FALSE);
    v_id := v_opened.o_id;
    v_card := v_opened.o_card;
    v_expires_at := v_opened.o_expires_at;
    v_new := v_opened.o_new;
    IF v_new THEN
        RETURN 0;
    END IF;

    DELETE FROM pgkv.zset_members z WHERE z.id = v_id AND z.member = ANY(p_members);
    GET DIAGNOSTICS v_removed = ROW_COUNT;
    IF v_removed > 0 THEN
        PERFORM pgkv._save(p_key, 'zset', v_id, v_card - v_removed, NULL, v_expires_at);
    END IF;
    RETURN v_removed;
END;
$$;

-- ZSCORE: Score of a member (NULL if the member or the key does not exist)
CREATE OR REPLACE FUNCTION pgkv.zscore(p_key TEXT, p_member TEXT)
RETURNS DOUBLE PRECISION
LANGUAGE plpgsql
STABLE
SECURITY DEFINER
SET search_path = pgkv, pg_temp
AS $$
DECLARE
    v_type TEXT;
    v_score DOUBLE PRECISION;
BEGIN
    SELECT s.type, z.score INTO v_type, v_score
    FROM pgkv.store s
    LEFT JOIN pgkv.zset_members z ON z.id = s.id AND z.member = p_member
    WHERE s.key = p_key AND (s.expires_at IS NULL OR s.expires_at >= clock_timestamp());

    IF v_type <> 'zset' THEN
        PERFORM pgkv._wrongtype();
    END IF;
    RETURN v_score;
END;
$$;

-- ZMSCORE: Scores of several members, in order (NULL for a missing member)
CREATE OR REPLACE FUNCTION pgkv.zmscore(p_key TEXT, VARIADIC p_members TEXT[])
RETURNS TABLE(member TEXT, score DOUBLE PRECISION)
LANGUAGE plpgsql
STABLE
SECURITY DEFINER
SET search_path = pgkv, pg_temp
AS $$
DECLARE
    v_peeked RECORD;
    v_id BIGINT;
BEGIN
    v_peeked := pgkv._peek(p_key, 'zset');
    v_id := v_peeked.o_id;
    RETURN QUERY
    SELECT u.m, z.score
    FROM unnest(p_members) WITH ORDINALITY AS u(m, ord)
    LEFT JOIN pgkv.zset_members z ON z.id = v_id AND z.member = u.m
    ORDER BY u.ord;
END;
$$;

-- ZCARD: Number of members in a sorted set
CREATE OR REPLACE FUNCTION pgkv.zcard(p_key TEXT)
RETURNS INTEGER
LANGUAGE plpgsql
STABLE
SECURITY DEFINER
SET search_path = pgkv, pg_temp
AS $$
BEGIN
    RETURN COALESCE((SELECT p.o_card FROM pgkv._peek(p_key, 'zset') p), 0);
END;
$$;

-- _ZRANK: zero-based rank of a member, from the lowest score or (p_rev) the highest.
-- Counts the members before it, so it costs in proportion to the rank.
CREATE OR REPLACE FUNCTION pgkv._zrank(p_key TEXT, p_member TEXT, p_rev BOOLEAN, OUT o_rank BIGINT, OUT o_score DOUBLE PRECISION)
LANGUAGE plpgsql
STABLE
AS $$
DECLARE
    v_peeked RECORD;
    v_id BIGINT;
    v_card BIGINT;
BEGIN
    v_peeked := pgkv._peek(p_key, 'zset');
    v_id := v_peeked.o_id;
    v_card := v_peeked.o_card;
    SELECT z.score INTO o_score FROM pgkv.zset_members z WHERE z.id = v_id AND z.member = p_member;
    IF NOT FOUND THEN
        RETURN;
    END IF;
    IF p_rev THEN
        SELECT COUNT(*) INTO o_rank FROM pgkv.zset_members z
        WHERE z.id = v_id AND (z.id, z.score, z.member) > (v_id, o_score, p_member);
    ELSE
        SELECT COUNT(*) INTO o_rank FROM pgkv.zset_members z
        WHERE z.id = v_id AND (z.id, z.score, z.member) < (v_id, o_score, p_member);
    END IF;
END;
$$;

-- ZRANK: Zero-based rank of a member, lowest score first (NULL if it does not exist)
CREATE OR REPLACE FUNCTION pgkv.zrank(p_key TEXT, p_member TEXT)
RETURNS INTEGER
LANGUAGE plpgsql
STABLE
SECURITY DEFINER
SET search_path = pgkv, pg_temp
AS $$
BEGIN
    RETURN (SELECT r.o_rank FROM pgkv._zrank(p_key, p_member, FALSE) r)::integer;
END;
$$;

-- ZREVRANK: Zero-based rank of a member, highest score first (NULL if it does not exist)
CREATE OR REPLACE FUNCTION pgkv.zrevrank(p_key TEXT, p_member TEXT)
RETURNS INTEGER
LANGUAGE plpgsql
STABLE
SECURITY DEFINER
SET search_path = pgkv, pg_temp
AS $$
BEGIN
    RETURN (SELECT r.o_rank FROM pgkv._zrank(p_key, p_member, TRUE) r)::integer;
END;
$$;

-- _ZSCORE_BOUND: parse a Redis score bound: a float, optionally preceded by ( for exclusive.
-- Bounds go through C strtod in Redis, which is laxer than score parsing: leading spaces,
-- an empty string (0) and hexadecimal integers are accepted.
CREATE OR REPLACE FUNCTION pgkv._zscore_bound(p_text TEXT, OUT o_value DOUBLE PRECISION, OUT o_exclusive BOOLEAN)
LANGUAGE plpgsql
IMMUTABLE
AS $$
DECLARE
    v_text TEXT;
BEGIN
    o_exclusive := left(p_text, 1) = '(';
    v_text := ltrim(CASE WHEN o_exclusive THEN substr(p_text, 2) ELSE p_text END, E' \t\n\r\f\v');
    IF v_text = '' THEN
        o_value := 0;
    ELSIF v_text ~* '^[+-]?0x[0-9a-f]{1,13}$' THEN
        o_value := (CASE WHEN left(v_text, 1) = '-' THEN -1 ELSE 1 END)
            * ('x' || lpad(regexp_replace(v_text, '^[+-]?0[xX]', ''), 16, '0'))::bit(64)::bigint;
    ELSE
        o_value := pgkv._float(v_text, 'ERR min or max is not a float');
    END IF;
END;
$$;

-- _ZLEX_BOUND: parse a Redis lexicographic bound: [x inclusive, (x exclusive, - lowest, + highest.
-- o_infinite is -1 for -, 1 for +, 0 otherwise.
CREATE OR REPLACE FUNCTION pgkv._zlex_bound(p_text TEXT, OUT o_value TEXT, OUT o_exclusive BOOLEAN, OUT o_infinite INTEGER)
LANGUAGE plpgsql
IMMUTABLE
AS $$
BEGIN
    o_exclusive := FALSE;
    o_infinite := 0;
    IF p_text = '-' THEN
        o_infinite := -1;
    ELSIF p_text = '+' THEN
        o_infinite := 1;
    ELSIF left(p_text, 1) IN ('[', '(') THEN
        o_exclusive := left(p_text, 1) = '(';
        o_value := substr(p_text, 2);
    ELSE
        RAISE EXCEPTION 'ERR min or max not valid string range item' USING ERRCODE = 'KV000';
    END IF;
END;
$$;

-- _ZRANGE: the members of a sorted set in a range, in order, with their scores.
--   p_by 'RANK':  p_min and p_max are start and stop indexes
--   p_by 'SCORE': score bounds ("1.5", "(1.5", "-inf", "+inf")
--   p_by 'LEX':   member bounds ("[a", "(a", "-", "+"), for sets whose scores are all equal
-- p_rev walks from the highest; p_min is then the upper bound, as in ZRANGE ... REV.
-- p_offset and p_count are LIMIT (not for RANK); a negative count means no limit.
CREATE OR REPLACE FUNCTION pgkv._zrange(
    p_key TEXT, p_by TEXT, p_min TEXT, p_max TEXT, p_rev BOOLEAN, p_offset BIGINT, p_count BIGINT
)
RETURNS TABLE(member TEXT, score DOUBLE PRECISION)
LANGUAGE plpgsql
STABLE
AS $$
DECLARE
    v_peeked RECORD;
    v_id BIGINT;
    v_card BIGINT;
    v_off BIGINT;
    v_len BIGINT;
    v_lo DOUBLE PRECISION;
    v_hi DOUBLE PRECISION;
    v_lo_ex BOOLEAN;
    v_hi_ex BOOLEAN;
    v_llo TEXT;
    v_lhi TEXT;
    v_lo_inf INTEGER;
    v_hi_inf INTEGER;
    v_limit BIGINT := CASE WHEN p_count < 0 THEN NULL ELSE p_count END;
BEGIN
    IF p_by = 'RANK' THEN
        v_off := pgkv._int(p_min);
        v_len := pgkv._int(p_max);
    ELSIF p_by = 'SCORE' THEN
        SELECT b.o_value, b.o_exclusive INTO v_lo, v_lo_ex FROM pgkv._zscore_bound(CASE WHEN p_rev THEN p_max ELSE p_min END) b;
        SELECT b.o_value, b.o_exclusive INTO v_hi, v_hi_ex FROM pgkv._zscore_bound(CASE WHEN p_rev THEN p_min ELSE p_max END) b;
    ELSE
        SELECT b.o_value, b.o_exclusive, b.o_infinite INTO v_llo, v_lo_ex, v_lo_inf FROM pgkv._zlex_bound(CASE WHEN p_rev THEN p_max ELSE p_min END) b;
        SELECT b.o_value, b.o_exclusive, b.o_infinite INTO v_lhi, v_hi_ex, v_hi_inf FROM pgkv._zlex_bound(CASE WHEN p_rev THEN p_min ELSE p_max END) b;
    END IF;

    v_peeked := pgkv._peek(p_key, 'zset');
    v_id := v_peeked.o_id;
    v_card := v_peeked.o_card;
    IF v_id IS NULL OR p_offset < 0 THEN
        RETURN;
    END IF;

    IF p_by = 'RANK' THEN
        SELECT r.o_off, r.o_len INTO v_off, v_len FROM pgkv._range(v_off, v_len, v_card) r;
        IF v_len = 0 THEN
            RETURN;
        ELSIF p_rev THEN
            RETURN QUERY SELECT z.member::text, z.score FROM pgkv.zset_members z WHERE z.id = v_id
                ORDER BY z.score DESC, z.member DESC OFFSET v_off LIMIT v_len;
        ELSE
            RETURN QUERY SELECT z.member::text, z.score FROM pgkv.zset_members z WHERE z.id = v_id
                ORDER BY z.score, z.member OFFSET v_off LIMIT v_len;
        END IF;
    ELSIF p_by = 'SCORE' THEN
        IF p_rev THEN
            RETURN QUERY SELECT z.member::text, z.score FROM pgkv.zset_members z
                WHERE z.id = v_id AND z.score >= v_lo AND z.score <= v_hi
                  AND (NOT v_lo_ex OR z.score > v_lo) AND (NOT v_hi_ex OR z.score < v_hi)
                ORDER BY z.score DESC, z.member DESC OFFSET p_offset LIMIT v_limit;
        ELSE
            RETURN QUERY SELECT z.member::text, z.score FROM pgkv.zset_members z
                WHERE z.id = v_id AND z.score >= v_lo AND z.score <= v_hi
                  AND (NOT v_lo_ex OR z.score > v_lo) AND (NOT v_hi_ex OR z.score < v_hi)
                ORDER BY z.score, z.member OFFSET p_offset LIMIT v_limit;
        END IF;
    ELSE
        IF v_lo_inf = 1 OR v_hi_inf = -1 THEN
            RETURN;
        ELSIF p_rev THEN
            RETURN QUERY SELECT z.member::text, z.score FROM pgkv.zset_members z
                WHERE z.id = v_id
                  AND (v_lo_inf = -1 OR z.member > v_llo OR (NOT v_lo_ex AND z.member = v_llo))
                  AND (v_hi_inf = 1 OR z.member < v_lhi OR (NOT v_hi_ex AND z.member = v_lhi))
                ORDER BY z.member DESC OFFSET p_offset LIMIT v_limit;
        ELSE
            RETURN QUERY SELECT z.member::text, z.score FROM pgkv.zset_members z
                WHERE z.id = v_id
                  AND (v_lo_inf = -1 OR z.member > v_llo OR (NOT v_lo_ex AND z.member = v_llo))
                  AND (v_hi_inf = 1 OR z.member < v_lhi OR (NOT v_hi_ex AND z.member = v_lhi))
                ORDER BY z.member OFFSET p_offset LIMIT v_limit;
        END IF;
    END IF;
END;
$$;

-- ZRANGE_BY: ZRANGE with every Redis option. See pgkv._zrange for the arguments, e.g.
--   pgkv.zrange_by('board', '(10', '+inf', 'SCORE', p_offset => 0, p_count => 20)
CREATE OR REPLACE FUNCTION pgkv.zrange_by(
    p_key TEXT,
    p_min TEXT,
    p_max TEXT,
    p_by TEXT DEFAULT 'RANK',
    p_rev BOOLEAN DEFAULT FALSE,
    p_offset BIGINT DEFAULT 0,
    p_count BIGINT DEFAULT -1
)
RETURNS TABLE(member TEXT, score DOUBLE PRECISION)
LANGUAGE plpgsql
STABLE
SECURITY DEFINER
SET search_path = pgkv, pg_temp
AS $$
BEGIN
    IF upper(p_by) NOT IN ('RANK', 'SCORE', 'LEX') THEN
        PERFORM pgkv._err('ERR syntax error');
    END IF;
    RETURN QUERY SELECT r.member, r.score FROM pgkv._zrange(p_key, upper(p_by), p_min, p_max, p_rev, p_offset, p_count) r;
END;
$$;

-- ZRANGE: Members between two ranks, lowest score first. score is NULL unless p_withscores.
CREATE OR REPLACE FUNCTION pgkv.zrange(p_key TEXT, p_start BIGINT, p_stop BIGINT, p_withscores BOOLEAN DEFAULT FALSE)
RETURNS TABLE(member TEXT, score DOUBLE PRECISION)
LANGUAGE plpgsql
STABLE
SECURITY DEFINER
SET search_path = pgkv, pg_temp
AS $$
BEGIN
    RETURN QUERY SELECT r.member, CASE WHEN p_withscores THEN r.score END
    FROM pgkv._zrange(p_key, 'RANK', p_start::text, p_stop::text, FALSE, 0, -1) r;
END;
$$;

-- ZREVRANGE: Members between two ranks, highest score first. score is NULL unless p_withscores.
CREATE OR REPLACE FUNCTION pgkv.zrevrange(p_key TEXT, p_start BIGINT, p_stop BIGINT, p_withscores BOOLEAN DEFAULT FALSE)
RETURNS TABLE(member TEXT, score DOUBLE PRECISION)
LANGUAGE plpgsql
STABLE
SECURITY DEFINER
SET search_path = pgkv, pg_temp
AS $$
BEGIN
    RETURN QUERY SELECT r.member, CASE WHEN p_withscores THEN r.score END
    FROM pgkv._zrange(p_key, 'RANK', p_start::text, p_stop::text, TRUE, 0, -1) r;
END;
$$;

-- ZRANGEBYSCORE: Members with scores between p_min and p_max (inclusive), lowest first.
-- score is NULL unless p_withscores. For exclusive bounds or LIMIT use pgkv.zrange_by.
CREATE OR REPLACE FUNCTION pgkv.zrangebyscore(p_key TEXT, p_min DOUBLE PRECISION, p_max DOUBLE PRECISION, p_withscores BOOLEAN DEFAULT FALSE)
RETURNS TABLE(member TEXT, score DOUBLE PRECISION)
LANGUAGE plpgsql
STABLE
SECURITY DEFINER
SET search_path = pgkv, pg_temp
AS $$
BEGIN
    RETURN QUERY SELECT r.member, CASE WHEN p_withscores THEN r.score END
    FROM pgkv._zrange(p_key, 'SCORE', p_min::text, p_max::text, FALSE, 0, -1) r;
END;
$$;

-- _ZCOUNT: number of members in a score or lexicographic range (bounds as for pgkv._zrange).
CREATE OR REPLACE FUNCTION pgkv._zcount(p_key TEXT, p_by TEXT, p_min TEXT, p_max TEXT)
RETURNS INTEGER
LANGUAGE plpgsql
STABLE
AS $$
BEGIN
    RETURN (SELECT COUNT(*) FROM pgkv._zrange(p_key, p_by, p_min, p_max, FALSE, 0, -1))::integer;
END;
$$;

-- ZCOUNT: Number of members with scores between p_min and p_max (inclusive)
CREATE OR REPLACE FUNCTION pgkv.zcount(p_key TEXT, p_min DOUBLE PRECISION, p_max DOUBLE PRECISION)
RETURNS INTEGER
LANGUAGE plpgsql
STABLE
SECURITY DEFINER
SET search_path = pgkv, pg_temp
AS $$
BEGIN
    RETURN pgkv._zcount(p_key, 'SCORE', p_min::text, p_max::text);
END;
$$;

-- ZLEXCOUNT: Number of members between two lexicographic bounds ("[a", "(a", "-", "+")
CREATE OR REPLACE FUNCTION pgkv.zlexcount(p_key TEXT, p_min TEXT, p_max TEXT)
RETURNS INTEGER
LANGUAGE plpgsql
STABLE
SECURITY DEFINER
SET search_path = pgkv, pg_temp
AS $$
BEGIN
    RETURN pgkv._zcount(p_key, 'LEX', p_min, p_max);
END;
$$;

-- _ZREMRANGE: remove the members in a range (arguments as for pgkv._zrange). Returns them.
CREATE OR REPLACE FUNCTION pgkv._zremrange(p_key TEXT, p_by TEXT, p_min TEXT, p_max TEXT, p_rev BOOLEAN)
RETURNS TABLE(member TEXT, score DOUBLE PRECISION)
LANGUAGE plpgsql
AS $$
DECLARE
    v_opened RECORD;
    v_id BIGINT;
    v_card BIGINT;
    v_expires_at TIMESTAMPTZ;
    v_new BOOLEAN;
    v_members TEXT[];
    v_scores DOUBLE PRECISION[];
BEGIN
    v_opened := pgkv._open(p_key, 'zset', FALSE);
    v_id := v_opened.o_id;
    v_card := v_opened.o_card;
    v_expires_at := v_opened.o_expires_at;
    v_new := v_opened.o_new;

    SELECT array_agg(r.member ORDER BY r.ord), array_agg(r.score ORDER BY r.ord) INTO v_members, v_scores
    FROM pgkv._zrange(p_key, p_by, p_min, p_max, p_rev, 0, -1) WITH ORDINALITY AS r(member, score, ord);
    IF v_members IS NULL THEN
        RETURN;
    END IF;

    DELETE FROM pgkv.zset_members z WHERE z.id = v_id AND z.member = ANY(v_members);
    PERFORM pgkv._save(p_key, 'zset', v_id, v_card - cardinality(v_members), NULL, v_expires_at);
    RETURN QUERY SELECT u.m, u.s FROM unnest(v_members, v_scores) WITH ORDINALITY AS u(m, s, ord) ORDER BY u.ord;
END;
$$;

-- ZREMRANGEBYRANK: Remove the members between two ranks. Returns the number removed.
CREATE OR REPLACE FUNCTION pgkv.zremrangebyrank(p_key TEXT, p_start BIGINT, p_stop BIGINT)
RETURNS INTEGER
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pgkv, pg_temp
AS $$
BEGIN
    RETURN (SELECT COUNT(*) FROM pgkv._zremrange(p_key, 'RANK', p_start::text, p_stop::text, FALSE))::integer;
END;
$$;

-- ZREMRANGEBYSCORE: Remove the members with scores between p_min and p_max (inclusive)
CREATE OR REPLACE FUNCTION pgkv.zremrangebyscore(p_key TEXT, p_min DOUBLE PRECISION, p_max DOUBLE PRECISION)
RETURNS INTEGER
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pgkv, pg_temp
AS $$
BEGIN
    RETURN (SELECT COUNT(*) FROM pgkv._zremrange(p_key, 'SCORE', p_min::text, p_max::text, FALSE))::integer;
END;
$$;

-- ZREMRANGEBYLEX: Remove the members between two lexicographic bounds ("[a", "(a", "-", "+")
CREATE OR REPLACE FUNCTION pgkv.zremrangebylex(p_key TEXT, p_min TEXT, p_max TEXT)
RETURNS INTEGER
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pgkv, pg_temp
AS $$
BEGIN
    RETURN (SELECT COUNT(*) FROM pgkv._zremrange(p_key, 'LEX', p_min, p_max, FALSE))::integer;
END;
$$;

-- ZPOPMIN: Remove and return the p_count members with the lowest scores
CREATE OR REPLACE FUNCTION pgkv.zpopmin(p_key TEXT, p_count BIGINT DEFAULT 1)
RETURNS TABLE(member TEXT, score DOUBLE PRECISION)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pgkv, pg_temp
AS $$
BEGIN
    IF p_count < 0 THEN
        PERFORM pgkv._err('ERR value is out of range, must be positive');
    ELSIF p_count = 0 THEN
        PERFORM pgkv._peek(p_key, 'zset');
        RETURN;
    END IF;
    RETURN QUERY SELECT r.member, r.score FROM pgkv._zremrange(p_key, 'RANK', '0', (p_count - 1)::text, FALSE) r;
END;
$$;

-- ZPOPMAX: Remove and return the p_count members with the highest scores
CREATE OR REPLACE FUNCTION pgkv.zpopmax(p_key TEXT, p_count BIGINT DEFAULT 1)
RETURNS TABLE(member TEXT, score DOUBLE PRECISION)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pgkv, pg_temp
AS $$
BEGIN
    IF p_count < 0 THEN
        PERFORM pgkv._err('ERR value is out of range, must be positive');
    ELSIF p_count = 0 THEN
        PERFORM pgkv._peek(p_key, 'zset');
        RETURN;
    END IF;
    RETURN QUERY SELECT r.member, r.score FROM pgkv._zremrange(p_key, 'RANK', '0', (p_count - 1)::text, TRUE) r;
END;
$$;

-- _STORE_ZSET: replace p_destination with a sorted set (the key is deleted if there are no members).
CREATE OR REPLACE FUNCTION pgkv._store_zset(p_destination TEXT, p_members TEXT[], p_scores DOUBLE PRECISION[])
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
        -- A score of -0 is stored as 0.
        INSERT INTO pgkv.zset_members (id, member, score) SELECT v_id, u.m, u.s + 0 FROM unnest(p_members, p_scores) AS u(m, s);
    END IF;
    PERFORM pgkv._save(p_destination, 'zset', v_id, v_n, NULL, NULL);
    RETURN v_n;
END;
$$;

-- _ZRANGESTORE: store a range of p_source (arguments as for pgkv._zrange) in p_destination.
CREATE OR REPLACE FUNCTION pgkv._zrangestore(
    p_destination TEXT, p_source TEXT, p_by TEXT, p_min TEXT, p_max TEXT, p_rev BOOLEAN, p_offset BIGINT, p_count BIGINT
)
RETURNS INTEGER
LANGUAGE plpgsql
AS $$
DECLARE
    v_members TEXT[];
    v_scores DOUBLE PRECISION[];
BEGIN
    PERFORM pgkv._lock_keys(ARRAY[p_source], ARRAY[p_destination]);
    SELECT array_agg(r.member), array_agg(r.score) INTO v_members, v_scores
    FROM pgkv._zrange(p_source, p_by, p_min, p_max, p_rev, p_offset, p_count) r;
    RETURN pgkv._store_zset(p_destination, v_members, v_scores);
END;
$$;

-- ZRANGESTORE: Store the members of p_source between two ranks in p_destination. Returns how many.
-- p_rev counts ranks from the highest score.
CREATE OR REPLACE FUNCTION pgkv.zrangestore(p_destination TEXT, p_source TEXT, p_start BIGINT, p_stop BIGINT, p_rev BOOLEAN DEFAULT FALSE)
RETURNS INTEGER
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pgkv, pg_temp
AS $$
BEGIN
    RETURN pgkv._zrangestore(p_destination, p_source, 'RANK', p_start::text, p_stop::text, p_rev, 0, -1);
END;
$$;

-- _ZCHECK: raise WRONGTYPE unless every key that exists holds a sorted set or a set.
CREATE OR REPLACE FUNCTION pgkv._zcheck(p_keys TEXT[])
RETURNS VOID
LANGUAGE plpgsql
STABLE
AS $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM pgkv.store s
        WHERE s.key = ANY(p_keys) AND s.type NOT IN ('zset', 'set')
          AND (s.expires_at IS NULL OR s.expires_at >= clock_timestamp())
    ) THEN
        PERFORM pgkv._wrongtype();
    END IF;
END;
$$;

-- _ZCOMBINE: ZUNION, ZINTER or ZDIFF over p_keys, lowest score first. A plain set counts as
-- a sorted set whose scores are all 1. Scores are multiplied by the key's weight (default 1)
-- and combined with p_aggregate (SUM, MIN or MAX); a result that is not a number becomes 0.
CREATE OR REPLACE FUNCTION pgkv._zcombine(p_op TEXT, p_keys TEXT[], p_weights DOUBLE PRECISION[], p_aggregate TEXT)
RETURNS TABLE(member TEXT, score DOUBLE PRECISION)
LANGUAGE plpgsql
STABLE
AS $$
DECLARE
    v_aggregate TEXT := upper(COALESCE(p_aggregate, 'SUM'));
    v_n INTEGER := COALESCE(cardinality(p_keys), 0);
    v_key TEXT;
    v_type TEXT;
    v_id BIGINT;
    v_zsets BIGINT[] := '{}';
    v_sets BIGINT[] := '{}';
    v_ords INTEGER[] := '{}';
    v_i INTEGER := 0;
BEGIN
    IF v_aggregate NOT IN ('SUM', 'MIN', 'MAX') THEN
        PERFORM pgkv._err('ERR syntax error');
    END IF;
    IF v_n = 0 THEN
        PERFORM pgkv._err('ERR at least 1 input key is needed for ''' || p_op || ''' command');
    END IF;
    IF p_weights IS NOT NULL AND cardinality(p_weights) <> v_n THEN
        PERFORM pgkv._err('ERR syntax error');
    END IF;
    IF 'NaN' = ANY(p_weights) THEN
        PERFORM pgkv._err('ERR weight value is not a float');
    END IF;

    -- v_zsets[i] / v_sets[i] hold the id of key i if it is a sorted set / a set, else 0.
    FOREACH v_key IN ARRAY p_keys LOOP
        v_i := v_i + 1;
        SELECT s.type, s.id INTO v_type, v_id FROM pgkv.store s
        WHERE s.key = v_key AND (s.expires_at IS NULL OR s.expires_at >= clock_timestamp());
        IF v_type IS NOT NULL AND v_type NOT IN ('zset', 'set') THEN
            PERFORM pgkv._wrongtype();
        END IF;
        v_zsets := v_zsets || CASE WHEN v_type = 'zset' THEN v_id ELSE 0 END;
        v_sets := v_sets || CASE WHEN v_type = 'set' THEN v_id ELSE 0 END;
        v_ords := v_ords || v_i;
    END LOOP;

    RETURN QUERY
    WITH inputs AS (
        SELECT k.ord, z.member AS m, z.score AS s
        FROM unnest(v_ords, v_zsets) AS k(ord, id) JOIN pgkv.zset_members z ON z.id = k.id
        UNION ALL
        SELECT k.ord, sm.member, 1
        FROM unnest(v_ords, v_sets) AS k(ord, id) JOIN pgkv.set_members sm ON sm.id = k.id
    ), weighted AS (
        SELECT i.ord, i.m, CASE WHEN w.s = 'NaN' THEN 0 ELSE w.s END AS s
        FROM inputs i CROSS JOIN LATERAL (SELECT pgkv._fmul(i.s, COALESCE(p_weights[i.ord], 1)) AS s) w
    ), combined AS (
        SELECT w.m,
               CASE v_aggregate WHEN 'MIN' THEN MIN(w.s) WHEN 'MAX' THEN MAX(w.s) ELSE pgkv._fsum(w.s) END AS s,
               COUNT(*) AS n,
               MIN(w.ord) AS first_ord
        FROM weighted w
        GROUP BY w.m
    )
    SELECT c.m::text, CASE WHEN c.s = 'NaN' THEN 0 ELSE c.s END
    FROM combined c
    WHERE CASE p_op
        WHEN 'zinter' THEN c.n = v_n
        WHEN 'zdiff' THEN c.n = 1 AND c.first_ord = 1
        ELSE TRUE END
    ORDER BY 2, c.m;
END;
$$;

-- ZUNION: Union of sorted sets (see pgkv._zcombine), lowest score first
CREATE OR REPLACE FUNCTION pgkv.zunion(p_keys TEXT[], p_weights DOUBLE PRECISION[] DEFAULT NULL, p_aggregate TEXT DEFAULT 'SUM')
RETURNS TABLE(member TEXT, score DOUBLE PRECISION)
LANGUAGE plpgsql
STABLE
SECURITY DEFINER
SET search_path = pgkv, pg_temp
AS $$
BEGIN
    RETURN QUERY SELECT r.member, r.score FROM pgkv._zcombine('zunion', p_keys, p_weights, p_aggregate) r;
END;
$$;

-- ZINTER: Intersection of sorted sets, lowest score first
CREATE OR REPLACE FUNCTION pgkv.zinter(p_keys TEXT[], p_weights DOUBLE PRECISION[] DEFAULT NULL, p_aggregate TEXT DEFAULT 'SUM')
RETURNS TABLE(member TEXT, score DOUBLE PRECISION)
LANGUAGE plpgsql
STABLE
SECURITY DEFINER
SET search_path = pgkv, pg_temp
AS $$
BEGIN
    RETURN QUERY SELECT r.member, r.score FROM pgkv._zcombine('zinter', p_keys, p_weights, p_aggregate) r;
END;
$$;

-- ZDIFF: Members of the first sorted set that are in none of the others, lowest score first
CREATE OR REPLACE FUNCTION pgkv.zdiff(p_keys TEXT[])
RETURNS TABLE(member TEXT, score DOUBLE PRECISION)
LANGUAGE plpgsql
STABLE
SECURITY DEFINER
SET search_path = pgkv, pg_temp
AS $$
BEGIN
    RETURN QUERY SELECT r.member, r.score FROM pgkv._zcombine('zdiff', p_keys, NULL, 'SUM') r;
END;
$$;

-- ZINTERCARD: Size of the intersection of sorted sets, counting no further than p_limit (0 = no limit)
CREATE OR REPLACE FUNCTION pgkv.zintercard(p_keys TEXT[], p_limit BIGINT DEFAULT 0)
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
    SELECT COUNT(*) INTO v_n FROM pgkv._zcombine('zinter', p_keys, NULL, 'SUM');
    RETURN CASE WHEN p_limit > 0 THEN LEAST(v_n, p_limit) ELSE v_n END;
END;
$$;

-- _ZCOMBINE_STORE: ZUNIONSTORE, ZINTERSTORE, ZDIFFSTORE.
CREATE OR REPLACE FUNCTION pgkv._zcombine_store(p_op TEXT, p_destination TEXT, p_keys TEXT[], p_weights DOUBLE PRECISION[], p_aggregate TEXT)
RETURNS INTEGER
LANGUAGE plpgsql
AS $$
DECLARE
    v_members TEXT[];
    v_scores DOUBLE PRECISION[];
BEGIN
    PERFORM pgkv._lock_keys(p_keys, ARRAY[p_destination]);
    SELECT array_agg(r.member), array_agg(r.score) INTO v_members, v_scores
    FROM pgkv._zcombine(p_op, p_keys, p_weights, p_aggregate) r;
    RETURN pgkv._store_zset(p_destination, v_members, v_scores);
END;
$$;

-- ZUNIONSTORE: Store the union of sorted sets in p_destination. Returns its size.
CREATE OR REPLACE FUNCTION pgkv.zunionstore(p_destination TEXT, p_keys TEXT[], p_weights DOUBLE PRECISION[] DEFAULT NULL, p_aggregate TEXT DEFAULT 'SUM')
RETURNS INTEGER
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pgkv, pg_temp
AS $$
BEGIN
    RETURN pgkv._zcombine_store('zunion', p_destination, p_keys, p_weights, p_aggregate);
END;
$$;

-- ZINTERSTORE: Store the intersection of sorted sets in p_destination. Returns its size.
CREATE OR REPLACE FUNCTION pgkv.zinterstore(p_destination TEXT, p_keys TEXT[], p_weights DOUBLE PRECISION[] DEFAULT NULL, p_aggregate TEXT DEFAULT 'SUM')
RETURNS INTEGER
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pgkv, pg_temp
AS $$
BEGIN
    RETURN pgkv._zcombine_store('zinter', p_destination, p_keys, p_weights, p_aggregate);
END;
$$;

-- ZDIFFSTORE: Store the difference of sorted sets in p_destination. Returns its size.
CREATE OR REPLACE FUNCTION pgkv.zdiffstore(p_destination TEXT, p_keys TEXT[])
RETURNS INTEGER
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pgkv, pg_temp
AS $$
BEGIN
    RETURN pgkv._zcombine_store('zdiff', p_destination, p_keys, NULL, 'SUM');
END;
$$;

-- ZRANDMEMBER: Random members of a sorted set. A positive p_count returns up to that many
-- distinct members; a negative one returns exactly that many, possibly repeating.
CREATE OR REPLACE FUNCTION pgkv.zrandmember(p_key TEXT, p_count BIGINT DEFAULT 1)
RETURNS TABLE(member TEXT, score DOUBLE PRECISION)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pgkv, pg_temp
AS $$
DECLARE
    v_peeked RECORD;
    v_id BIGINT;
    v_card BIGINT;
BEGIN
    v_peeked := pgkv._peek(p_key, 'zset');
    v_id := v_peeked.o_id;
    v_card := v_peeked.o_card;
    IF v_id IS NULL OR p_count = 0 THEN
        RETURN;
    END IF;
    IF p_count > 0 THEN
        RETURN QUERY SELECT z.member::text, z.score FROM pgkv.zset_members z WHERE z.id = v_id ORDER BY random() LIMIT p_count;
    ELSE
        RETURN QUERY
        SELECT r.member::text, r.score
        FROM generate_series(1, -p_count) AS g
        CROSS JOIN LATERAL (
            SELECT z.member, z.score FROM pgkv.zset_members z
            WHERE z.id = v_id ORDER BY z.member OFFSET floor(random() * v_card + g * 0)::bigint LIMIT 1
        ) r;
    END IF;
END;
$$;

-- ZSCAN: Iterate the members of a sorted set matching a pattern, p_count at a time, in byte
-- order of the member. Each row carries the cursor for the next call; 0 means complete.
CREATE OR REPLACE FUNCTION pgkv.zscan(p_key TEXT, p_cursor BIGINT DEFAULT 0, p_pattern TEXT DEFAULT '*', p_count INTEGER DEFAULT 10)
RETURNS TABLE(cursor BIGINT, member TEXT, score DOUBLE PRECISION)
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
    v_peeked := pgkv._peek(p_key, 'zset');
    v_id := v_peeked.o_id;
    RETURN QUERY
    WITH page AS (
        SELECT z.member AS mem, z.score AS sc, row_number() OVER (ORDER BY z.member) AS rn
        FROM pgkv.zset_members z
        WHERE z.id = v_id AND (v_re IS NULL OR z.member ~ v_re)
        ORDER BY z.member
        OFFSET p_cursor LIMIT p_count + 1
    )
    SELECT CASE WHEN (SELECT COUNT(*) FROM page) > p_count THEN p_cursor + p_count ELSE 0 END,
           page.mem::text, page.sc
    FROM page WHERE page.rn <= p_cursor + p_count ORDER BY page.mem;
END;
$$;

