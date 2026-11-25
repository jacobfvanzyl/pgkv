-- ============================================================================
-- The Redis command interface: pgkv.call, pgkv.try_call, pgkv.pipeline
--
--   SELECT pgkv.call('SET', 'k', 'v', 'EX', '10');   -- {"status": "OK"}
--   SELECT pgkv.call('LRANGE', 'jobs', '0', '-1');   -- ["a", "b"]
--
-- Arguments are the words of a Redis command, as text. The reply is what a Redis server
-- answers over RESP2, as JSONB:
--   integer        -> number          bulk string -> string
--   nil            -> null            array       -> array
--   simple string  -> {"status": "OK"} (so a client can tell +OK from the string "OK")
-- No command's reply is itself a JSON object, so an object at the top level or as an element
-- of a pipeline's result is always one of the markers: {"status": ...} or {"error": ...}.
--
-- The three functions differ in what happens to a Redis error reply:
--   pgkv.call(...)       raises it: SQLSTATE class KV, message starting with the Redis
--                        error code. Like any PostgreSQL error it aborts the transaction it
--                        happens in.
--                          KV000 ERR ...        KV001 WRONGTYPE ...
--                          KV002 ERR unknown command / wrong number of arguments (what
--                                Redis reports when a command is queued, before it runs)
--   pgkv.try_call(...)   returns it as {"error": "WRONGTYPE ..."} and undoes only that
--                        command, so the caller's transaction carries on.
--   pgkv.pipeline(commands, atomic)
--                        returns an array of replies with errors in place: one round trip
--                        for many commands, and MULTI/EXEC when atomic. A discarded
--                        transaction is {"error": "EXECABORT ...", "previous": [...]}.
-- A database error (deadlock, serialization failure) is raised by all three.
-- The client libraries in clients/ are thin wrappers over try_call and pipeline.
-- ============================================================================

-- _ARITY: the wrong-number-of-arguments error. Redis rejects most of these when a command is
-- queued in MULTI, from its command table (KV002); a few it only finds when the command
-- runs, such as an odd number of MSET arguments (p_at_run, KV000). The message is the same.
CREATE OR REPLACE FUNCTION pgkv._arity(p_command TEXT, p_at_run BOOLEAN DEFAULT FALSE)
RETURNS VOID
LANGUAGE plpgsql
AS $$
BEGIN
    IF p_at_run THEN
        RAISE EXCEPTION 'ERR wrong number of arguments for ''%'' command', lower(p_command) USING ERRCODE = 'KV000';
    END IF;
    RAISE EXCEPTION 'ERR wrong number of arguments for ''%'' command', lower(p_command) USING ERRCODE = 'KV002';
END;
$$;

CREATE OR REPLACE FUNCTION pgkv._unknown(argv TEXT[])
RETURNS JSONB
LANGUAGE plpgsql
AS $$
BEGIN
    IF cardinality(argv) < 2 THEN
        RAISE EXCEPTION 'ERR unknown command ''%''', argv[1] USING ERRCODE = 'KV002';
    END IF;
    RAISE EXCEPTION 'ERR unknown command ''%'', with args beginning with: %', argv[1],
        (SELECT string_agg('''' || a || ''' ', '') FROM unnest(argv[2:]) AS a)
        USING ERRCODE = 'KV002';
END;
$$;

-- _R: an array of text as a reply (NULL elements become nil).
CREATE OR REPLACE FUNCTION pgkv._r(p_values TEXT[])
RETURNS JSONB
LANGUAGE sql
IMMUTABLE
AS $$
    SELECT CASE WHEN p_values IS NULL OR cardinality(p_values) = 0 THEN '[]'::jsonb ELSE to_jsonb(p_values) END
$$;

-- _RZ: members, with their scores interleaved if p_withscores, as a reply.
CREATE OR REPLACE FUNCTION pgkv._rz(p_members TEXT[], p_scores DOUBLE PRECISION[], p_withscores BOOLEAN)
RETURNS JSONB
LANGUAGE sql
IMMUTABLE
AS $$
    SELECT COALESCE(jsonb_agg(v.e ORDER BY u.ord, v.k), '[]'::jsonb)
    FROM unnest(p_members, p_scores) WITH ORDINALITY AS u(m, s, ord)
    CROSS JOIN LATERAL (VALUES (1, to_jsonb(u.m)), (2, to_jsonb(pgkv._fmt(u.s)))) AS v(k, e)
    WHERE p_withscores OR v.k = 1
$$;

-- _SCAN_OPTS: parse the options of SCAN, HSCAN, SSCAN and ZSCAN from argv[p_from].
CREATE OR REPLACE FUNCTION pgkv._scan_opts(
    argv TEXT[], p_from INTEGER, p_cursor TEXT,
    OUT o_cursor BIGINT, OUT o_pattern TEXT, OUT o_count BIGINT, OUT o_type TEXT, OUT o_novalues BOOLEAN
)
LANGUAGE plpgsql
AS $$
DECLARE
    i INTEGER := p_from;
    n INTEGER := COALESCE(array_length(argv, 1), 0);
    v_opt TEXT;
BEGIN
    o_cursor := pgkv._int(p_cursor, 'ERR invalid cursor');
    IF o_cursor < 0 THEN
        PERFORM pgkv._err('ERR invalid cursor');
    END IF;
    o_pattern := '*';
    o_count := 10;
    o_novalues := FALSE;
    WHILE i <= n LOOP
        v_opt := upper(argv[i]);
        IF v_opt = 'NOVALUES' THEN
            o_novalues := TRUE;
            i := i + 1;
        ELSIF i = n THEN
            PERFORM pgkv._err('ERR syntax error');
        ELSIF v_opt = 'MATCH' THEN
            o_pattern := argv[i + 1];
            i := i + 2;
        ELSIF v_opt = 'COUNT' THEN
            o_count := pgkv._int(argv[i + 1]);
            IF o_count < 1 THEN
                PERFORM pgkv._err('ERR syntax error');
            END IF;
            i := i + 2;
        ELSIF v_opt = 'TYPE' THEN
            o_type := argv[i + 1];
            i := i + 2;
        ELSE
            PERFORM pgkv._err('ERR syntax error');
        END IF;
    END LOOP;
END;
$$;

-- _NUMKEYS: read "numkeys key [key ...]" from argv[p_at]. o_next is the index after the keys.
CREATE OR REPLACE FUNCTION pgkv._numkeys(argv TEXT[], p_at INTEGER, p_command TEXT, OUT o_keys TEXT[], OUT o_next INTEGER)
LANGUAGE plpgsql
AS $$
DECLARE
    v_num BIGINT := pgkv._int(argv[p_at]);
    n INTEGER := COALESCE(array_length(argv, 1), 0);
BEGIN
    IF v_num < 1 THEN
        IF p_command IN ('lmpop', 'zmpop', 'sintercard') THEN
            PERFORM pgkv._err('ERR numkeys should be greater than 0');
        END IF;
        PERFORM pgkv._err('ERR at least 1 input key is needed for ''' || p_command || ''' command');
    END IF;
    IF p_at + v_num > n THEN
        IF p_command = 'sintercard' THEN
            PERFORM pgkv._err('ERR Number of keys can''t be greater than number of args');
        END IF;
        PERFORM pgkv._err('ERR syntax error');
    END IF;
    o_keys := argv[p_at + 1 : p_at + v_num];
    o_next := p_at + v_num + 1;
END;
$$;

-- ---------------------------------------------------------------------------
-- Strings
-- ---------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION pgkv._call_string(c TEXT, argv TEXT[], n INTEGER)
RETURNS JSONB
LANGUAGE plpgsql
AS $$
DECLARE
    i INTEGER;
    v_opt TEXT;
    v_nx BOOLEAN := FALSE;
    v_xx BOOLEAN := FALSE;
    v_get BOOLEAN := FALSE;
    v_keepttl BOOLEAN := FALSE;
    v_persist BOOLEAN := FALSE;
    v_ex BIGINT;
    v_px BIGINT;
    v_exat BIGINT;
    v_pxat BIGINT;
    v_ifeq TEXT;
    v_has_ifeq BOOLEAN := FALSE;
    v_unit TEXT;
    v_amount TEXT;
    v_reply JSONB;
    v_text TEXT;
BEGIN
    CASE c
    WHEN 'SET' THEN
        IF n < 3 THEN PERFORM pgkv._arity(c); END IF;
        i := 4;
        WHILE i <= n LOOP
            v_opt := upper(argv[i]);
            IF v_opt = 'NX' THEN v_nx := TRUE;
            ELSIF v_opt = 'XX' THEN v_xx := TRUE;
            ELSIF v_opt = 'GET' THEN v_get := TRUE;
            ELSIF v_opt = 'KEEPTTL' THEN v_keepttl := TRUE;
            ELSIF v_opt IN ('EX', 'PX', 'EXAT', 'PXAT', 'IFEQ') AND i < n THEN
                i := i + 1;
                IF v_opt = 'IFEQ' THEN
                    v_ifeq := argv[i];
                    v_has_ifeq := TRUE;
                ELSIF v_unit IS NOT NULL THEN
                    PERFORM pgkv._err('ERR syntax error');
                ELSE
                    v_unit := v_opt;
                    v_amount := argv[i];
                END IF;
            ELSE
                PERFORM pgkv._err('ERR syntax error');
            END IF;
            i := i + 1;
        END LOOP;
        -- Redis checks the whole option list for syntax before it reads the expiry value.
        IF (v_nx AND v_xx) OR (v_keepttl AND v_unit IS NOT NULL) THEN
            PERFORM pgkv._err('ERR syntax error');
        END IF;
        IF v_unit = 'EX' THEN v_ex := pgkv._int(v_amount);
        ELSIF v_unit = 'PX' THEN v_px := pgkv._int(v_amount);
        ELSIF v_unit = 'EXAT' THEN v_exat := pgkv._int(v_amount);
        ELSIF v_unit = 'PXAT' THEN v_pxat := pgkv._int(v_amount);
        END IF;
        IF v_has_ifeq THEN
            IF v_nx OR v_xx THEN PERFORM pgkv._err('ERR syntax error'); END IF;
            IF v_get THEN
                v_text := pgkv.get(argv[2]) #>> '{}';
            END IF;
            i := pgkv.cas(argv[2], v_ifeq, argv[3], v_ex, v_px, v_exat, v_pxat, v_keepttl);
            IF v_get THEN RETURN COALESCE(to_jsonb(v_text), 'null'::jsonb); END IF;
            RETURN CASE WHEN i = 1 THEN '{"status":"OK"}'::jsonb ELSE 'null'::jsonb END;
        END IF;
        v_reply := pgkv.set_opts(argv[2], argv[3], v_nx, v_xx, v_get, v_ex, v_px, v_exat, v_pxat, v_keepttl);
        IF v_get THEN
            RETURN COALESCE(to_jsonb(v_reply #>> '{}'), 'null'::jsonb);
        END IF;
        RETURN CASE WHEN v_reply IS NULL THEN 'null'::jsonb ELSE '{"status":"OK"}'::jsonb END;
    WHEN 'SETNX' THEN
        IF n <> 3 THEN PERFORM pgkv._arity(c); END IF;
        RETURN to_jsonb(pgkv.setnx(argv[2], argv[3]));
    WHEN 'SETEX', 'PSETEX' THEN
        IF n <> 4 THEN PERFORM pgkv._arity(c); END IF;
        IF c = 'SETEX' THEN
            PERFORM pgkv._set(argv[2], to_jsonb(argv[4]), pgkv._expires_at(pgkv._int(argv[3]), NULL, NULL, NULL, FALSE, 'setex'), FALSE);
        ELSE
            PERFORM pgkv._set(argv[2], to_jsonb(argv[4]), pgkv._expires_at(NULL, pgkv._int(argv[3]), NULL, NULL, FALSE, 'psetex'), FALSE);
        END IF;
        RETURN '{"status":"OK"}'::jsonb;
    WHEN 'GETSET' THEN
        IF n <> 3 THEN PERFORM pgkv._arity(c); END IF;
        RETURN COALESCE(to_jsonb(pgkv.getset(argv[2], argv[3]) #>> '{}'), 'null'::jsonb);
    WHEN 'GETDEL' THEN
        IF n <> 2 THEN PERFORM pgkv._arity(c); END IF;
        RETURN COALESCE(to_jsonb(pgkv.getdel(argv[2]) #>> '{}'), 'null'::jsonb);
    WHEN 'GETEX' THEN
        IF n < 2 THEN PERFORM pgkv._arity(c); END IF;
        IF n > 2 THEN
            v_opt := upper(argv[3]);
            IF NOT ((v_opt = 'PERSIST' AND n = 3) OR (v_opt IN ('EX', 'PX', 'EXAT', 'PXAT') AND n = 4)) THEN
                PERFORM pgkv._err('ERR syntax error');
            END IF;
            -- As in Redis, a missing key answers nil before the expiry value is looked at.
            IF pgkv.get(argv[2]) IS NULL THEN
                RETURN 'null'::jsonb;
            END IF;
            IF v_opt = 'PERSIST' AND n = 3 THEN v_persist := TRUE;
            ELSIF n <> 4 THEN PERFORM pgkv._err('ERR syntax error');
            ELSIF v_opt = 'EX' THEN v_ex := pgkv._int(argv[4]);
            ELSIF v_opt = 'PX' THEN v_px := pgkv._int(argv[4]);
            ELSIF v_opt = 'EXAT' THEN v_exat := pgkv._int(argv[4]);
            ELSIF v_opt = 'PXAT' THEN v_pxat := pgkv._int(argv[4]);
            ELSE PERFORM pgkv._err('ERR syntax error');
            END IF;
        END IF;
        RETURN COALESCE(to_jsonb(pgkv.getex(argv[2], v_ex, v_px, v_exat, v_pxat, v_persist) #>> '{}'), 'null'::jsonb);
    WHEN 'INCR', 'DECR' THEN
        IF n <> 2 THEN PERFORM pgkv._arity(c); END IF;
        RETURN to_jsonb(pgkv.incrby(argv[2], CASE WHEN c = 'INCR' THEN 1 ELSE -1 END));
    WHEN 'INCRBY' THEN
        IF n <> 3 THEN PERFORM pgkv._arity(c); END IF;
        RETURN to_jsonb(pgkv.incrby(argv[2], pgkv._int(argv[3])));
    WHEN 'DECRBY' THEN
        IF n <> 3 THEN PERFORM pgkv._arity(c); END IF;
        RETURN to_jsonb(pgkv.decrby(argv[2], pgkv._int(argv[3])));
    WHEN 'INCRBYFLOAT' THEN
        IF n <> 3 THEN PERFORM pgkv._arity(c); END IF;
        RETURN to_jsonb(pgkv._num_text(pgkv._incrbyfloat(argv[2], argv[3])));
    WHEN 'APPEND' THEN
        IF n <> 3 THEN PERFORM pgkv._arity(c); END IF;
        RETURN to_jsonb(pgkv.append(argv[2], argv[3]));
    WHEN 'STRLEN' THEN
        IF n <> 2 THEN PERFORM pgkv._arity(c); END IF;
        RETURN to_jsonb(pgkv.strlen(argv[2]));
    WHEN 'GETRANGE', 'SUBSTR' THEN
        IF n <> 4 THEN PERFORM pgkv._arity(c); END IF;
        RETURN to_jsonb(pgkv.getrange(argv[2], pgkv._int(argv[3]), pgkv._int(argv[4])));
    WHEN 'SETRANGE' THEN
        IF n <> 4 THEN PERFORM pgkv._arity(c); END IF;
        RETURN to_jsonb(pgkv.setrange(argv[2], pgkv._int(argv[3]), argv[4]));
    WHEN 'MGET' THEN
        IF n < 2 THEN PERFORM pgkv._arity(c); END IF;
        RETURN (SELECT jsonb_agg(m.value #>> '{}' ORDER BY m.ord)
                FROM pgkv.mget(VARIADIC argv[2:]) WITH ORDINALITY AS m(key, value, ord));
    WHEN 'MSET' THEN
        IF n < 3 THEN PERFORM pgkv._arity(c); END IF;
        IF n % 2 = 0 THEN PERFORM pgkv._arity(c, TRUE); END IF;
        PERFORM pgkv.mset(VARIADIC argv[2:]);
        RETURN '{"status":"OK"}'::jsonb;
    WHEN 'MSETNX' THEN
        IF n < 3 THEN PERFORM pgkv._arity(c); END IF;
        IF n % 2 = 0 THEN PERFORM pgkv._arity(c, TRUE); END IF;
        RETURN to_jsonb(pgkv.msetnx(VARIADIC argv[2:]));
    WHEN 'DELIFEQ' THEN
        IF n <> 3 THEN PERFORM pgkv._arity(c); END IF;
        RETURN to_jsonb(pgkv.cad(argv[2], argv[3]));
    WHEN 'DELEX' THEN
        IF n = 2 THEN RETURN to_jsonb(pgkv.del(argv[2])); END IF;
        IF n <> 4 THEN PERFORM pgkv._arity(c); END IF;
        IF upper(argv[3]) <> 'IFEQ' THEN PERFORM pgkv._err('ERR syntax error'); END IF;
        RETURN to_jsonb(pgkv.cad(argv[2], argv[4]));
    ELSE
        RETURN pgkv._unknown(argv);
    END CASE;
END;
$$;

-- ---------------------------------------------------------------------------
-- Keys, expiry, connection
-- ---------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION pgkv._call_keys(c TEXT, argv TEXT[], n INTEGER)
RETURNS JSONB
LANGUAGE plpgsql
AS $$
DECLARE
    v_cursor BIGINT;
    v_pattern TEXT;
    v_count BIGINT;
    v_type TEXT;
    v_next BIGINT;
    v_keys TEXT[];
    v_t BIGINT;
    v_replace BOOLEAN := FALSE;
    i INTEGER;
BEGIN
    CASE c
    WHEN 'DEL', 'UNLINK' THEN
        IF n < 2 THEN PERFORM pgkv._arity(c); END IF;
        RETURN to_jsonb(pgkv.del(VARIADIC argv[2:]));
    WHEN 'EXISTS', 'TOUCH' THEN
        IF n < 2 THEN PERFORM pgkv._arity(c); END IF;
        RETURN to_jsonb(pgkv.exists(VARIADIC argv[2:]));
    WHEN 'TYPE' THEN
        IF n <> 2 THEN PERFORM pgkv._arity(c); END IF;
        RETURN jsonb_build_object('status', pgkv.type(argv[2]));
    WHEN 'EXPIRE', 'PEXPIRE', 'EXPIREAT', 'PEXPIREAT' THEN
        IF n < 3 THEN PERFORM pgkv._arity(c); END IF;
        -- The options are checked before the time, as in Redis.
        PERFORM pgkv._expire_mode(argv[4:]);
        RETURN to_jsonb(pgkv._expire(
            argv[2],
            pgkv._when(pgkv._int(argv[3]), CASE WHEN left(c, 1) = 'P' THEN 'ms' ELSE 's' END, c LIKE '%AT', lower(c)),
            array_to_string(argv[4:], ' ')));
    WHEN 'TTL' THEN
        IF n <> 2 THEN PERFORM pgkv._arity(c); END IF;
        v_t := pgkv.pttl(argv[2]);
        RETURN to_jsonb(CASE WHEN v_t < 0 THEN v_t ELSE (v_t + 500) / 1000 END);
    WHEN 'PTTL' THEN
        IF n <> 2 THEN PERFORM pgkv._arity(c); END IF;
        RETURN to_jsonb(pgkv.pttl(argv[2]));
    WHEN 'EXPIRETIME' THEN
        IF n <> 2 THEN PERFORM pgkv._arity(c); END IF;
        RETURN to_jsonb(pgkv.expiretime(argv[2]));
    WHEN 'PEXPIRETIME' THEN
        IF n <> 2 THEN PERFORM pgkv._arity(c); END IF;
        RETURN to_jsonb(pgkv.pexpiretime(argv[2]));
    WHEN 'PERSIST' THEN
        IF n <> 2 THEN PERFORM pgkv._arity(c); END IF;
        RETURN to_jsonb(pgkv.persist(argv[2]));
    WHEN 'KEYS' THEN
        IF n <> 2 THEN PERFORM pgkv._arity(c); END IF;
        RETURN pgkv._r(ARRAY(SELECT k FROM pgkv.keys(argv[2]) AS k));
    WHEN 'SCAN' THEN
        IF n < 2 THEN PERFORM pgkv._arity(c); END IF;
        SELECT o.o_cursor, o.o_pattern, o.o_count, o.o_type INTO v_cursor, v_pattern, v_count, v_type
        FROM pgkv._scan_opts(argv, 3, argv[2]) o;
        SELECT s.o_next, s.o_keys INTO v_next, v_keys FROM pgkv._scan(v_cursor, v_pattern, v_count, v_type) s;
        RETURN jsonb_build_array(v_next::text, pgkv._r(v_keys));
    WHEN 'DBSIZE' THEN
        IF n <> 1 THEN PERFORM pgkv._arity(c); END IF;
        RETURN to_jsonb(pgkv.dbsize());
    WHEN 'FLUSHDB', 'FLUSHALL' THEN
        IF n > 2 OR (n = 2 AND upper(argv[2]) NOT IN ('ASYNC', 'SYNC')) THEN PERFORM pgkv._err('ERR syntax error'); END IF;
        PERFORM pgkv.flushall();
        RETURN '{"status":"OK"}'::jsonb;
    WHEN 'RANDOMKEY' THEN
        IF n <> 1 THEN PERFORM pgkv._arity(c); END IF;
        RETURN COALESCE(to_jsonb(pgkv.randomkey()), 'null'::jsonb);
    WHEN 'RENAME' THEN
        IF n <> 3 THEN PERFORM pgkv._arity(c); END IF;
        PERFORM pgkv.rename(argv[2], argv[3]);
        RETURN '{"status":"OK"}'::jsonb;
    WHEN 'RENAMENX' THEN
        IF n <> 3 THEN PERFORM pgkv._arity(c); END IF;
        RETURN to_jsonb(pgkv.renamenx(argv[2], argv[3]));
    WHEN 'COPY' THEN
        IF n < 3 THEN PERFORM pgkv._arity(c); END IF;
        i := 4;
        WHILE i <= n LOOP
            IF upper(argv[i]) = 'REPLACE' THEN
                v_replace := TRUE;
            ELSIF upper(argv[i]) = 'DB' AND i < n THEN
                i := i + 1;
                IF pgkv._int(argv[i]) <> 0 THEN PERFORM pgkv._err('ERR DB index is out of range'); END IF;
            ELSE
                PERFORM pgkv._err('ERR syntax error');
            END IF;
            i := i + 1;
        END LOOP;
        RETURN to_jsonb(pgkv.copy(argv[2], argv[3], v_replace));
    WHEN 'PING' THEN
        IF n > 2 THEN PERFORM pgkv._arity(c, TRUE); END IF;
        RETURN CASE WHEN n = 2 THEN to_jsonb(argv[2]) ELSE '{"status":"PONG"}'::jsonb END;
    WHEN 'ECHO' THEN
        IF n <> 2 THEN PERFORM pgkv._arity(c); END IF;
        RETURN to_jsonb(argv[2]);
    WHEN 'TIME' THEN
        IF n <> 1 THEN PERFORM pgkv._arity(c); END IF;
        v_t := (extract(epoch FROM clock_timestamp())::numeric * 1000000)::bigint;
        RETURN jsonb_build_array((v_t / 1000000)::text, (v_t % 1000000)::text);
    WHEN 'SELECT' THEN
        IF n <> 2 THEN PERFORM pgkv._arity(c); END IF;
        IF pgkv._int(argv[2], 'ERR invalid DB index') <> 0 THEN PERFORM pgkv._err('ERR DB index is out of range'); END IF;
        RETURN '{"status":"OK"}'::jsonb;
    ELSE
        RETURN pgkv._unknown(argv);
    END CASE;
END;
$$;

-- ---------------------------------------------------------------------------
-- Hashes
-- ---------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION pgkv._call_hash(c TEXT, argv TEXT[], n INTEGER)
RETURNS JSONB
LANGUAGE plpgsql
AS $$
DECLARE
    v_cursor BIGINT;
    v_pattern TEXT;
    v_count BIGINT;
    v_novalues BOOLEAN;
    v_with BOOLEAN := FALSE;
    v_fields TEXT[];
    v_values TEXT[];
BEGIN
    CASE c
    WHEN 'HGET' THEN
        IF n <> 3 THEN PERFORM pgkv._arity(c); END IF;
        RETURN COALESCE(to_jsonb(pgkv.hget(argv[2], argv[3]) #>> '{}'), 'null'::jsonb);
    WHEN 'HSET', 'HMSET' THEN
        IF n < 4 THEN PERFORM pgkv._arity(c); END IF;
        IF n % 2 <> 0 THEN PERFORM pgkv._arity(c, TRUE); END IF;
        IF c = 'HMSET' THEN
            PERFORM pgkv.hset(argv[2], VARIADIC argv[3:]);
            RETURN '{"status":"OK"}'::jsonb;
        END IF;
        RETURN to_jsonb(pgkv.hset(argv[2], VARIADIC argv[3:]));
    WHEN 'HSETNX' THEN
        IF n <> 4 THEN PERFORM pgkv._arity(c); END IF;
        RETURN to_jsonb(pgkv.hsetnx(argv[2], argv[3], argv[4]));
    WHEN 'HMGET' THEN
        IF n < 3 THEN PERFORM pgkv._arity(c); END IF;
        RETURN (SELECT jsonb_agg(h.value #>> '{}' ORDER BY h.ord)
                FROM pgkv.hmget(argv[2], VARIADIC argv[3:]) WITH ORDINALITY AS h(field, value, ord));
    WHEN 'HGETALL' THEN
        IF n <> 2 THEN PERFORM pgkv._arity(c); END IF;
        RETURN (SELECT COALESCE(jsonb_agg(v.e ORDER BY h.ord, v.k), '[]'::jsonb)
                FROM pgkv.hgetall(argv[2]) WITH ORDINALITY AS h(field, value, ord)
                CROSS JOIN LATERAL (VALUES (1, to_jsonb(h.field)), (2, to_jsonb(h.value #>> '{}'))) AS v(k, e));
    WHEN 'HDEL' THEN
        IF n < 3 THEN PERFORM pgkv._arity(c); END IF;
        RETURN to_jsonb(pgkv.hdel(argv[2], VARIADIC argv[3:]));
    WHEN 'HEXISTS' THEN
        IF n <> 3 THEN PERFORM pgkv._arity(c); END IF;
        RETURN to_jsonb(pgkv.hexists(argv[2], argv[3]));
    WHEN 'HLEN' THEN
        IF n <> 2 THEN PERFORM pgkv._arity(c); END IF;
        RETURN to_jsonb(pgkv.hlen(argv[2]));
    WHEN 'HSTRLEN' THEN
        IF n <> 3 THEN PERFORM pgkv._arity(c); END IF;
        RETURN to_jsonb(pgkv.hstrlen(argv[2], argv[3]));
    WHEN 'HKEYS' THEN
        IF n <> 2 THEN PERFORM pgkv._arity(c); END IF;
        RETURN pgkv._r(ARRAY(SELECT h.field FROM pgkv.hgetall(argv[2]) h));
    WHEN 'HVALS' THEN
        IF n <> 2 THEN PERFORM pgkv._arity(c); END IF;
        RETURN pgkv._r(ARRAY(SELECT h.value #>> '{}' FROM pgkv.hgetall(argv[2]) h));
    WHEN 'HINCRBY' THEN
        IF n <> 4 THEN PERFORM pgkv._arity(c); END IF;
        RETURN to_jsonb(pgkv.hincrby(argv[2], argv[3], pgkv._int(argv[4])));
    WHEN 'HINCRBYFLOAT' THEN
        IF n <> 4 THEN PERFORM pgkv._arity(c); END IF;
        RETURN to_jsonb(pgkv._num_text(pgkv._hincrbyfloat(argv[2], argv[3], argv[4])));
    WHEN 'HRANDFIELD' THEN
        IF n < 2 THEN PERFORM pgkv._arity(c); END IF;
        IF n = 2 THEN
            RETURN COALESCE((SELECT to_jsonb(h.field) FROM pgkv.hrandfield(argv[2], 1) h), 'null'::jsonb);
        END IF;
        IF n = 4 AND upper(argv[4]) = 'WITHVALUES' THEN
            v_with := TRUE;
        ELSIF n <> 3 THEN
            PERFORM pgkv._err('ERR syntax error');
        END IF;
        SELECT array_agg(h.field), array_agg(h.value #>> '{}') INTO v_fields, v_values
        FROM pgkv.hrandfield(argv[2], pgkv._int(argv[3])) h;
        RETURN (SELECT COALESCE(jsonb_agg(v.e ORDER BY u.ord, v.k), '[]'::jsonb)
                FROM unnest(v_fields, v_values) WITH ORDINALITY AS u(f, val, ord)
                CROSS JOIN LATERAL (VALUES (1, to_jsonb(u.f)), (2, to_jsonb(u.val))) AS v(k, e)
                WHERE v_with OR v.k = 1);
    WHEN 'HSCAN' THEN
        IF n < 3 THEN PERFORM pgkv._arity(c); END IF;
        SELECT o.o_cursor, o.o_pattern, o.o_count, o.o_novalues INTO v_cursor, v_pattern, v_count, v_novalues
        FROM pgkv._scan_opts(argv, 4, argv[3]) o;
        SELECT array_agg(h.field ORDER BY h.ord), array_agg(h.value #>> '{}' ORDER BY h.ord) INTO v_fields, v_values
        FROM pgkv.hscan(argv[2], v_cursor, v_pattern, (v_count + 1)::integer) WITH ORDINALITY AS h(cursor, field, value, ord);
        RETURN jsonb_build_array(
            CASE WHEN COALESCE(cardinality(v_fields), 0) > v_count THEN (v_cursor + v_count)::text ELSE '0' END,
            (SELECT COALESCE(jsonb_agg(v.e ORDER BY u.ord, v.k), '[]'::jsonb)
             FROM unnest(v_fields[1:v_count], v_values[1:v_count]) WITH ORDINALITY AS u(f, val, ord)
             CROSS JOIN LATERAL (VALUES (1, to_jsonb(u.f)), (2, to_jsonb(u.val))) AS v(k, e)
             WHERE NOT v_novalues OR v.k = 1));
    ELSE
        RETURN pgkv._unknown(argv);
    END CASE;
END;
$$;

-- ---------------------------------------------------------------------------
-- Lists
-- ---------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION pgkv._call_list(c TEXT, argv TEXT[], n INTEGER)
RETURNS JSONB
LANGUAGE plpgsql
AS $$
DECLARE
    v_values TEXT[];
    v_keys TEXT[];
    v_key TEXT;
    v_next INTEGER;
    v_rank BIGINT := 1;
    v_count BIGINT;
    v_maxlen BIGINT := 0;
    v_positions BIGINT[];
    v_opt TEXT;
    i INTEGER;
BEGIN
    CASE c
    WHEN 'LPUSH', 'RPUSH', 'LPUSHX', 'RPUSHX' THEN
        IF n < 3 THEN PERFORM pgkv._arity(c); END IF;
        RETURN to_jsonb(pgkv._push(argv[2], argv[3:], left(c, 1) = 'L', right(c, 1) = 'X'));
    WHEN 'LPOP', 'RPOP' THEN
        IF n < 2 THEN PERFORM pgkv._arity(c); END IF;
        IF n > 3 THEN PERFORM pgkv._arity(c, TRUE); END IF;
        IF n = 2 THEN
            RETURN COALESCE(to_jsonb((pgkv._pop(argv[2], 1, c = 'LPOP'))[1]), 'null'::jsonb);
        END IF;
        v_values := pgkv._pop(argv[2], pgkv._int(argv[3], 'ERR value is out of range, must be positive'), c = 'LPOP');
        RETURN CASE WHEN v_values IS NULL THEN 'null'::jsonb ELSE pgkv._r(v_values) END;
    WHEN 'LLEN' THEN
        IF n <> 2 THEN PERFORM pgkv._arity(c); END IF;
        RETURN to_jsonb(pgkv.llen(argv[2]));
    WHEN 'LRANGE' THEN
        IF n <> 4 THEN PERFORM pgkv._arity(c); END IF;
        RETURN pgkv._r(pgkv._lrange(argv[2], pgkv._int(argv[3]), pgkv._int(argv[4])));
    WHEN 'LINDEX' THEN
        IF n <> 3 THEN PERFORM pgkv._arity(c); END IF;
        RETURN COALESCE(pgkv.lindex(argv[2], pgkv._int(argv[3])), 'null'::jsonb);
    WHEN 'LSET' THEN
        IF n <> 4 THEN PERFORM pgkv._arity(c); END IF;
        PERFORM pgkv.lset(argv[2], pgkv._int(argv[3]), argv[4]);
        RETURN '{"status":"OK"}'::jsonb;
    WHEN 'LTRIM' THEN
        IF n <> 4 THEN PERFORM pgkv._arity(c); END IF;
        PERFORM pgkv.ltrim(argv[2], pgkv._int(argv[3]), pgkv._int(argv[4]));
        RETURN '{"status":"OK"}'::jsonb;
    WHEN 'LREM' THEN
        IF n <> 4 THEN PERFORM pgkv._arity(c); END IF;
        RETURN to_jsonb(pgkv.lrem(argv[2], pgkv._int(argv[3]), argv[4]));
    WHEN 'LINSERT' THEN
        IF n <> 5 THEN PERFORM pgkv._arity(c); END IF;
        RETURN to_jsonb(pgkv.linsert(argv[2], argv[3], argv[4], argv[5]));
    WHEN 'LPOS' THEN
        IF n < 3 THEN PERFORM pgkv._arity(c); END IF;
        i := 4;
        WHILE i <= n LOOP
            v_opt := upper(argv[i]);
            IF i = n OR v_opt NOT IN ('RANK', 'COUNT', 'MAXLEN') THEN
                PERFORM pgkv._err('ERR syntax error');
            ELSIF v_opt = 'RANK' THEN
                v_rank := pgkv._int(argv[i + 1]);
                IF v_rank = 0 THEN
                    PERFORM pgkv._err('ERR RANK can''t be zero: use 1 to start from the first match, 2 from the second ... or use negative to start from the end of the list');
                END IF;
            ELSIF v_opt = 'COUNT' THEN
                v_count := pgkv._int(argv[i + 1]);
                IF v_count < 0 THEN PERFORM pgkv._err('ERR COUNT can''t be negative'); END IF;
            ELSE
                v_maxlen := pgkv._int(argv[i + 1]);
                IF v_maxlen < 0 THEN PERFORM pgkv._err('ERR MAXLEN can''t be negative'); END IF;
            END IF;
            i := i + 2;
        END LOOP;
        v_positions := pgkv._lpos(argv[2], argv[3], v_rank, COALESCE(v_count, 1), v_maxlen);
        IF v_count IS NULL THEN
            RETURN COALESCE(to_jsonb(v_positions[1]), 'null'::jsonb);
        END IF;
        RETURN CASE WHEN COALESCE(cardinality(v_positions), 0) = 0 THEN '[]'::jsonb ELSE to_jsonb(v_positions) END;
    WHEN 'LMOVE' THEN
        IF n <> 5 THEN PERFORM pgkv._arity(c); END IF;
        RETURN COALESCE(pgkv.lmove(argv[2], argv[3], argv[4], argv[5]), 'null'::jsonb);
    WHEN 'RPOPLPUSH' THEN
        IF n <> 3 THEN PERFORM pgkv._arity(c); END IF;
        RETURN COALESCE(pgkv.lmove(argv[2], argv[3], 'RIGHT', 'LEFT'), 'null'::jsonb);
    WHEN 'LMPOP' THEN
        IF n < 4 THEN PERFORM pgkv._arity(c); END IF;
        SELECT k.o_keys, k.o_next INTO v_keys, v_next FROM pgkv._numkeys(argv, 2, 'lmpop') k;
        v_opt := upper(argv[v_next]);
        v_count := 1;
        IF v_opt IS NULL OR v_opt NOT IN ('LEFT', 'RIGHT') THEN
            PERFORM pgkv._err('ERR syntax error');
        END IF;
        IF n = v_next + 2 AND upper(argv[v_next + 1]) = 'COUNT' THEN
            v_count := pgkv._int(argv[v_next + 2], 'ERR count should be greater than 0');
            IF v_count < 1 THEN PERFORM pgkv._err('ERR count should be greater than 0'); END IF;
        ELSIF n <> v_next THEN
            PERFORM pgkv._err('ERR syntax error');
        END IF;
        FOREACH v_key IN ARRAY v_keys LOOP
            v_values := pgkv._pop(v_key, v_count, v_opt = 'LEFT');
            IF v_values IS NOT NULL THEN
                RETURN jsonb_build_array(v_key, pgkv._r(v_values));
            END IF;
        END LOOP;
        RETURN 'null'::jsonb;
    ELSE
        RETURN pgkv._unknown(argv);
    END CASE;
END;
$$;

-- ---------------------------------------------------------------------------
-- Sets
-- ---------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION pgkv._call_set(c TEXT, argv TEXT[], n INTEGER)
RETURNS JSONB
LANGUAGE plpgsql
AS $$
DECLARE
    v_cursor BIGINT;
    v_pattern TEXT;
    v_count BIGINT;
    v_keys TEXT[];
    v_next INTEGER;
    v_members TEXT[];
    v_limit BIGINT := 0;
BEGIN
    CASE c
    WHEN 'SADD' THEN
        IF n < 3 THEN PERFORM pgkv._arity(c); END IF;
        RETURN to_jsonb(pgkv.sadd(argv[2], VARIADIC argv[3:]));
    WHEN 'SREM' THEN
        IF n < 3 THEN PERFORM pgkv._arity(c); END IF;
        RETURN to_jsonb(pgkv.srem(argv[2], VARIADIC argv[3:]));
    WHEN 'SMEMBERS' THEN
        IF n <> 2 THEN PERFORM pgkv._arity(c); END IF;
        RETURN pgkv._r(pgkv._smembers(argv[2]));
    WHEN 'SISMEMBER' THEN
        IF n <> 3 THEN PERFORM pgkv._arity(c); END IF;
        RETURN to_jsonb(pgkv.sismember(argv[2], argv[3]));
    WHEN 'SMISMEMBER' THEN
        IF n < 3 THEN PERFORM pgkv._arity(c); END IF;
        RETURN (SELECT jsonb_agg(s.v ORDER BY s.ord) FROM pgkv.smismember(argv[2], VARIADIC argv[3:]) WITH ORDINALITY AS s(v, ord));
    WHEN 'SCARD' THEN
        IF n <> 2 THEN PERFORM pgkv._arity(c); END IF;
        RETURN to_jsonb(pgkv.scard(argv[2]));
    WHEN 'SINTER', 'SUNION', 'SDIFF' THEN
        IF n < 2 THEN PERFORM pgkv._arity(c); END IF;
        RETURN pgkv._r(pgkv._setop(lower(substr(c, 2)), argv[2:]));
    WHEN 'SINTERSTORE', 'SUNIONSTORE', 'SDIFFSTORE' THEN
        IF n < 3 THEN PERFORM pgkv._arity(c); END IF;
        RETURN to_jsonb(pgkv._setop_store(lower(substr(c, 2, length(c) - 6)), argv[2], argv[3:]));
    WHEN 'SINTERCARD' THEN
        IF n < 3 THEN PERFORM pgkv._arity(c); END IF;
        SELECT k.o_keys, k.o_next INTO v_keys, v_next FROM pgkv._numkeys(argv, 2, 'sintercard') k;
        IF n = v_next + 1 AND upper(argv[v_next]) = 'LIMIT' THEN
            v_limit := pgkv._int(argv[v_next + 1], 'ERR LIMIT can''t be negative');
        ELSIF n >= v_next THEN
            PERFORM pgkv._err('ERR syntax error');
        END IF;
        RETURN to_jsonb(pgkv.sintercard(v_keys, v_limit));
    WHEN 'SMOVE' THEN
        IF n <> 4 THEN PERFORM pgkv._arity(c); END IF;
        RETURN to_jsonb(pgkv.smove(argv[2], argv[3], argv[4]));
    WHEN 'SPOP' THEN
        IF n < 2 THEN PERFORM pgkv._arity(c); END IF;
        IF n > 3 THEN PERFORM pgkv._err('ERR syntax error'); END IF;
        IF n = 2 THEN
            RETURN COALESCE((SELECT to_jsonb(m) FROM pgkv.spop(argv[2], 1) AS m), 'null'::jsonb);
        END IF;
        RETURN pgkv._r(ARRAY(SELECT m FROM pgkv.spop(argv[2], pgkv._int(argv[3], 'ERR value is out of range, must be positive')) AS m));
    WHEN 'SRANDMEMBER' THEN
        IF n < 2 THEN PERFORM pgkv._arity(c); END IF;
        IF n > 3 THEN PERFORM pgkv._err('ERR syntax error'); END IF;
        IF n = 2 THEN
            RETURN COALESCE((SELECT to_jsonb(m) FROM pgkv.srandmember(argv[2], 1) AS m), 'null'::jsonb);
        END IF;
        RETURN pgkv._r(ARRAY(SELECT m FROM pgkv.srandmember(argv[2], pgkv._int(argv[3])) AS m));
    WHEN 'SSCAN' THEN
        IF n < 3 THEN PERFORM pgkv._arity(c); END IF;
        SELECT o.o_cursor, o.o_pattern, o.o_count INTO v_cursor, v_pattern, v_count FROM pgkv._scan_opts(argv, 4, argv[3]) o;
        v_members := ARRAY(SELECT s.member #>> '{}' FROM pgkv.sscan(argv[2], v_cursor, v_pattern, (v_count + 1)::integer) s);
        RETURN jsonb_build_array(
            CASE WHEN cardinality(v_members) > v_count THEN (v_cursor + v_count)::text ELSE '0' END,
            pgkv._r(v_members[1:v_count]));
    ELSE
        RETURN pgkv._unknown(argv);
    END CASE;
END;
$$;

-- ---------------------------------------------------------------------------
-- Sorted sets
-- ---------------------------------------------------------------------------

-- _ZARGS: parse "numkeys key [key ...] [WEIGHTS w ...] [AGGREGATE SUM|MIN|MAX] [WITHSCORES]".
CREATE OR REPLACE FUNCTION pgkv._zargs(
    argv TEXT[], p_at INTEGER, p_command TEXT, p_allow_withscores BOOLEAN,
    OUT o_keys TEXT[], OUT o_weights DOUBLE PRECISION[], OUT o_aggregate TEXT, OUT o_withscores BOOLEAN
)
LANGUAGE plpgsql
AS $$
DECLARE
    n INTEGER := COALESCE(array_length(argv, 1), 0);
    i INTEGER;
    j INTEGER;
    v_opt TEXT;
BEGIN
    SELECT k.o_keys, k.o_next INTO o_keys, i FROM pgkv._numkeys(argv, p_at, p_command) k;
    -- Redis looks the keys up, and rejects a wrong type, before it reads the options.
    PERFORM pgkv._zcheck(o_keys);
    o_aggregate := 'SUM';
    o_withscores := FALSE;
    WHILE i <= n LOOP
        v_opt := upper(argv[i]);
        IF v_opt = 'WEIGHTS' AND p_command NOT LIKE 'zdiff%' AND i + cardinality(o_keys) <= n THEN
            o_weights := '{}';
            FOR j IN 1..cardinality(o_keys) LOOP
                o_weights := o_weights || pgkv._float(argv[i + j], 'ERR weight value is not a float');
            END LOOP;
            i := i + cardinality(o_keys) + 1;
        ELSIF v_opt = 'AGGREGATE' AND p_command NOT LIKE 'zdiff%' AND i < n AND upper(argv[i + 1]) IN ('SUM', 'MIN', 'MAX') THEN
            o_aggregate := upper(argv[i + 1]);
            i := i + 2;
        ELSIF v_opt = 'WITHSCORES' AND p_allow_withscores THEN
            o_withscores := TRUE;
            i := i + 1;
        ELSE
            PERFORM pgkv._err('ERR syntax error');
        END IF;
    END LOOP;
END;
$$;

CREATE OR REPLACE FUNCTION pgkv._call_zset(c TEXT, argv TEXT[], n INTEGER)
RETURNS JSONB
LANGUAGE plpgsql
AS $$
DECLARE
    i INTEGER;
    v_opt TEXT;
    v_nx BOOLEAN := FALSE;
    v_xx BOOLEAN := FALSE;
    v_gt BOOLEAN := FALSE;
    v_lt BOOLEAN := FALSE;
    v_ch BOOLEAN := FALSE;
    v_incr BOOLEAN := FALSE;
    v_with BOOLEAN := FALSE;
    v_rev BOOLEAN := FALSE;
    v_by TEXT := 'RANK';
    v_offset BIGINT := 0;
    v_count BIGINT := -1;
    v_has_limit BOOLEAN := FALSE;
    v_members TEXT[];
    v_scores DOUBLE PRECISION[];
    v_keys TEXT[];
    v_key TEXT;
    v_weights DOUBLE PRECISION[];
    v_aggregate TEXT;
    v_next INTEGER;
    v_rank BIGINT;
    v_score DOUBLE PRECISION;
    v_cursor BIGINT;
    v_pattern TEXT;
    v_base INTEGER;
BEGIN
    CASE c
    WHEN 'ZADD' THEN
        IF n < 4 THEN PERFORM pgkv._arity(c); END IF;
        i := 3;
        WHILE i <= n LOOP
            v_opt := upper(argv[i]);
            IF v_opt = 'NX' THEN v_nx := TRUE;
            ELSIF v_opt = 'XX' THEN v_xx := TRUE;
            ELSIF v_opt = 'GT' THEN v_gt := TRUE;
            ELSIF v_opt = 'LT' THEN v_lt := TRUE;
            ELSIF v_opt = 'CH' THEN v_ch := TRUE;
            ELSIF v_opt = 'INCR' THEN v_incr := TRUE;
            ELSE EXIT;
            END IF;
            i := i + 1;
        END LOOP;
        IF i > n OR (n - i + 1) % 2 <> 0 THEN
            PERFORM pgkv._err('ERR syntax error');
        END IF;
        IF i = 3 THEN
            RETURN to_jsonb(pgkv.zadd(argv[2], VARIADIC argv[3:]));
        END IF;
        RETURN pgkv.zadd_opts(argv[2], argv[i:], v_nx, v_xx, v_gt, v_lt, v_ch, v_incr);
    WHEN 'ZINCRBY' THEN
        IF n <> 4 THEN PERFORM pgkv._arity(c); END IF;
        RETURN to_jsonb(pgkv._fmt(pgkv.zincrby(argv[2], pgkv._float(argv[3]), argv[4])));
    WHEN 'ZREM' THEN
        IF n < 3 THEN PERFORM pgkv._arity(c); END IF;
        RETURN to_jsonb(pgkv.zrem(argv[2], VARIADIC argv[3:]));
    WHEN 'ZSCORE' THEN
        IF n <> 3 THEN PERFORM pgkv._arity(c); END IF;
        RETURN COALESCE(to_jsonb(pgkv._fmt(pgkv.zscore(argv[2], argv[3]))), 'null'::jsonb);
    WHEN 'ZMSCORE' THEN
        IF n < 3 THEN PERFORM pgkv._arity(c); END IF;
        RETURN (SELECT jsonb_agg(pgkv._fmt(z.score) ORDER BY z.ord)
                FROM pgkv.zmscore(argv[2], VARIADIC argv[3:]) WITH ORDINALITY AS z(member, score, ord));
    WHEN 'ZCARD' THEN
        IF n <> 2 THEN PERFORM pgkv._arity(c); END IF;
        RETURN to_jsonb(pgkv.zcard(argv[2]));
    WHEN 'ZRANK', 'ZREVRANK' THEN
        IF n < 3 THEN PERFORM pgkv._arity(c); END IF;
        IF n > 4 THEN PERFORM pgkv._arity(c, TRUE); END IF;
        IF n = 4 AND upper(argv[4]) <> 'WITHSCORE' THEN PERFORM pgkv._err('ERR syntax error'); END IF;
        SELECT r.o_rank, r.o_score INTO v_rank, v_score FROM pgkv._zrank(argv[2], argv[3], c = 'ZREVRANK') r;
        IF v_rank IS NULL THEN
            RETURN 'null'::jsonb;
        END IF;
        RETURN CASE WHEN n = 4 THEN jsonb_build_array(v_rank, pgkv._fmt(v_score)) ELSE to_jsonb(v_rank) END;
    WHEN 'ZCOUNT' THEN
        IF n <> 4 THEN PERFORM pgkv._arity(c); END IF;
        RETURN to_jsonb(pgkv._zcount(argv[2], 'SCORE', argv[3], argv[4]));
    WHEN 'ZLEXCOUNT' THEN
        IF n <> 4 THEN PERFORM pgkv._arity(c); END IF;
        RETURN to_jsonb(pgkv._zcount(argv[2], 'LEX', argv[3], argv[4]));
    WHEN 'ZRANGE', 'ZREVRANGE', 'ZRANGEBYSCORE', 'ZREVRANGEBYSCORE', 'ZRANGEBYLEX', 'ZREVRANGEBYLEX', 'ZRANGESTORE' THEN
        -- v_base: index of the key; min and max follow it.
        v_base := CASE WHEN c = 'ZRANGESTORE' THEN 3 ELSE 2 END;
        IF n < v_base + 2 THEN PERFORM pgkv._arity(c); END IF;
        v_rev := c LIKE 'ZREV%';
        IF c LIKE '%BYSCORE' THEN v_by := 'SCORE'; ELSIF c LIKE '%BYLEX' THEN v_by := 'LEX'; END IF;
        i := v_base + 3;
        WHILE i <= n LOOP
            v_opt := upper(argv[i]);
            IF v_opt = 'WITHSCORES' AND c NOT IN ('ZRANGEBYLEX', 'ZREVRANGEBYLEX', 'ZRANGESTORE') THEN
                v_with := TRUE;
            ELSIF v_opt = 'LIMIT' AND i + 2 <= n AND c NOT IN ('ZREVRANGE') THEN
                v_offset := pgkv._int(argv[i + 1]);
                v_count := pgkv._int(argv[i + 2]);
                v_has_limit := TRUE;
                i := i + 2;
            ELSIF v_opt = 'BYSCORE' AND c IN ('ZRANGE', 'ZRANGESTORE') THEN
                v_by := 'SCORE';
            ELSIF v_opt = 'BYLEX' AND c IN ('ZRANGE', 'ZRANGESTORE') THEN
                v_by := 'LEX';
            ELSIF v_opt = 'REV' AND c IN ('ZRANGE', 'ZRANGESTORE') THEN
                v_rev := TRUE;
            ELSE
                PERFORM pgkv._err('ERR syntax error');
            END IF;
            i := i + 1;
        END LOOP;
        IF v_has_limit AND v_by = 'RANK' THEN
            PERFORM pgkv._err('ERR syntax error, LIMIT is only supported in combination with either BYSCORE or BYLEX');
        END IF;
        IF v_with AND v_by = 'LEX' THEN
            PERFORM pgkv._err('ERR syntax error, WITHSCORES not supported in combination with BYLEX');
        END IF;
        IF c = 'ZRANGESTORE' THEN
            RETURN to_jsonb(pgkv._zrangestore(argv[2], argv[3], v_by, argv[4], argv[5], v_rev, v_offset, v_count));
        END IF;
        SELECT array_agg(r.member ORDER BY r.ord), array_agg(r.score ORDER BY r.ord) INTO v_members, v_scores
        FROM pgkv._zrange(argv[2], v_by, argv[3], argv[4], v_rev, v_offset, v_count) WITH ORDINALITY AS r(member, score, ord);
        RETURN pgkv._rz(v_members, v_scores, v_with);
    WHEN 'ZREMRANGEBYRANK', 'ZREMRANGEBYSCORE', 'ZREMRANGEBYLEX' THEN
        IF n <> 4 THEN PERFORM pgkv._arity(c); END IF;
        -- Validate the bounds even when the key does not exist.
        PERFORM 1 FROM pgkv._zrange('', substr(c, 12), argv[3], argv[4], FALSE, 0, 0);
        RETURN to_jsonb((SELECT COUNT(*) FROM pgkv._zremrange(argv[2], substr(c, 12), argv[3], argv[4], FALSE)));
    WHEN 'ZPOPMIN', 'ZPOPMAX' THEN
        IF n < 2 THEN PERFORM pgkv._arity(c); END IF;
        IF n > 3 THEN PERFORM pgkv._err('ERR syntax error'); END IF;
        v_count := CASE WHEN n = 3 THEN pgkv._int(argv[3], 'ERR value is out of range, must be positive') ELSE 1 END;
        IF v_count < 0 THEN PERFORM pgkv._err('ERR value is out of range, must be positive'); END IF;
        IF v_count = 0 THEN
            PERFORM pgkv._peek(argv[2], 'zset');
            RETURN '[]'::jsonb;
        END IF;
        SELECT array_agg(r.member ORDER BY r.ord), array_agg(r.score ORDER BY r.ord) INTO v_members, v_scores
        FROM pgkv._zremrange(argv[2], 'RANK', '0', (v_count - 1)::text, c = 'ZPOPMAX') WITH ORDINALITY AS r(member, score, ord);
        RETURN pgkv._rz(v_members, v_scores, TRUE);
    WHEN 'ZMPOP' THEN
        IF n < 4 THEN PERFORM pgkv._arity(c); END IF;
        SELECT k.o_keys, k.o_next INTO v_keys, v_next FROM pgkv._numkeys(argv, 2, 'zmpop') k;
        v_opt := upper(argv[v_next]);
        v_count := 1;
        IF v_opt IS NULL OR v_opt NOT IN ('MIN', 'MAX') THEN
            PERFORM pgkv._err('ERR syntax error');
        END IF;
        IF n = v_next + 2 AND upper(argv[v_next + 1]) = 'COUNT' THEN
            v_count := pgkv._int(argv[v_next + 2], 'ERR count should be greater than 0');
            IF v_count < 1 THEN PERFORM pgkv._err('ERR count should be greater than 0'); END IF;
        ELSIF n <> v_next THEN
            PERFORM pgkv._err('ERR syntax error');
        END IF;
        FOREACH v_key IN ARRAY v_keys LOOP
            SELECT array_agg(r.member ORDER BY r.ord), array_agg(r.score ORDER BY r.ord) INTO v_members, v_scores
            FROM pgkv._zremrange(v_key, 'RANK', '0', (v_count - 1)::text, v_opt = 'MAX') WITH ORDINALITY AS r(member, score, ord);
            IF v_members IS NOT NULL THEN
                RETURN jsonb_build_array(v_key, (
                    SELECT jsonb_agg(jsonb_build_array(u.m, pgkv._fmt(u.s)) ORDER BY u.ord)
                    FROM unnest(v_members, v_scores) WITH ORDINALITY AS u(m, s, ord)));
            END IF;
        END LOOP;
        RETURN 'null'::jsonb;
    WHEN 'ZUNIONSTORE', 'ZINTERSTORE', 'ZDIFFSTORE' THEN
        IF n < 4 THEN PERFORM pgkv._arity(c); END IF;
        SELECT a.o_keys, a.o_weights, a.o_aggregate INTO v_keys, v_weights, v_aggregate
        FROM pgkv._zargs(argv, 3, lower(c), FALSE) a;
        RETURN to_jsonb(pgkv._zcombine_store(lower(substr(c, 1, length(c) - 5)), argv[2], v_keys, v_weights, v_aggregate));
    WHEN 'ZUNION', 'ZINTER', 'ZDIFF' THEN
        IF n < 3 THEN PERFORM pgkv._arity(c); END IF;
        SELECT a.o_keys, a.o_weights, a.o_aggregate, a.o_withscores INTO v_keys, v_weights, v_aggregate, v_with
        FROM pgkv._zargs(argv, 2, lower(c), TRUE) a;
        SELECT array_agg(r.member ORDER BY r.ord), array_agg(r.score ORDER BY r.ord) INTO v_members, v_scores
        FROM pgkv._zcombine(lower(c), v_keys, v_weights, v_aggregate) WITH ORDINALITY AS r(member, score, ord);
        RETURN pgkv._rz(v_members, v_scores, v_with);
    WHEN 'ZINTERCARD' THEN
        IF n < 3 THEN PERFORM pgkv._arity(c); END IF;
        SELECT k.o_keys, k.o_next INTO v_keys, v_next FROM pgkv._numkeys(argv, 2, 'zintercard') k;
        PERFORM pgkv._zcheck(v_keys);
        v_count := 0;
        IF n = v_next + 1 AND upper(argv[v_next]) = 'LIMIT' THEN
            v_count := pgkv._int(argv[v_next + 1], 'ERR LIMIT can''t be negative');
        ELSIF n >= v_next THEN
            PERFORM pgkv._err('ERR syntax error');
        END IF;
        RETURN to_jsonb(pgkv.zintercard(v_keys, v_count));
    WHEN 'ZRANDMEMBER' THEN
        IF n < 2 THEN PERFORM pgkv._arity(c); END IF;
        IF n = 2 THEN
            RETURN COALESCE((SELECT to_jsonb(z.member) FROM pgkv.zrandmember(argv[2], 1) z), 'null'::jsonb);
        END IF;
        IF n = 4 AND upper(argv[4]) = 'WITHSCORES' THEN
            v_with := TRUE;
        ELSIF n <> 3 THEN
            PERFORM pgkv._err('ERR syntax error');
        END IF;
        SELECT array_agg(z.member), array_agg(z.score) INTO v_members, v_scores
        FROM pgkv.zrandmember(argv[2], pgkv._int(argv[3])) z;
        RETURN pgkv._rz(v_members, v_scores, v_with);
    WHEN 'ZSCAN' THEN
        IF n < 3 THEN PERFORM pgkv._arity(c); END IF;
        SELECT o.o_cursor, o.o_pattern, o.o_count INTO v_cursor, v_pattern, v_count FROM pgkv._scan_opts(argv, 4, argv[3]) o;
        SELECT array_agg(z.member ORDER BY z.ord), array_agg(z.score ORDER BY z.ord) INTO v_members, v_scores
        FROM pgkv.zscan(argv[2], v_cursor, v_pattern, (v_count + 1)::integer) WITH ORDINALITY AS z(cursor, member, score, ord);
        RETURN jsonb_build_array(
            CASE WHEN COALESCE(cardinality(v_members), 0) > v_count THEN (v_cursor + v_count)::text ELSE '0' END,
            pgkv._rz(v_members[1:v_count], v_scores[1:v_count], TRUE));
    ELSE
        RETURN pgkv._unknown(argv);
    END CASE;
END;
$$;

-- _DISPATCH: run one Redis command given as an array of words.
CREATE OR REPLACE FUNCTION pgkv._dispatch(argv TEXT[])
RETURNS JSONB
LANGUAGE plpgsql
AS $$
DECLARE
    n INTEGER := COALESCE(array_length(argv, 1), 0);
    c TEXT;
BEGIN
    IF n = 0 OR array_position(argv, NULL) IS NOT NULL THEN
        PERFORM pgkv._err('ERR pgkv.call takes a command name followed by non-null arguments');
    END IF;
    c := upper(argv[1]);

    -- The commonest commands first, then by family.
    IF c = 'GET' THEN
        IF n <> 2 THEN PERFORM pgkv._arity(c); END IF;
        RETURN COALESCE(to_jsonb(pgkv.get(argv[2]) #>> '{}'), 'null'::jsonb);
    ELSIF c = 'SET' AND n = 3 THEN
        PERFORM pgkv._set(argv[2], to_jsonb(argv[3]), NULL, FALSE);
        RETURN '{"status":"OK"}'::jsonb;
    END IF;

    CASE left(c, 1)
    WHEN 'H' THEN
        RETURN pgkv._call_hash(c, argv, n);
    WHEN 'Z' THEN
        RETURN pgkv._call_zset(c, argv, n);
    WHEN 'L' THEN
        RETURN pgkv._call_list(c, argv, n);
    WHEN 'S' THEN
        IF c IN ('SET', 'SETEX', 'SETNX', 'SETRANGE', 'STRLEN', 'SUBSTR') THEN
            RETURN pgkv._call_string(c, argv, n);
        ELSIF c IN ('SCAN', 'SELECT') THEN
            RETURN pgkv._call_keys(c, argv, n);
        END IF;
        RETURN pgkv._call_set(c, argv, n);
    WHEN 'R' THEN
        IF c IN ('RPUSH', 'RPOP', 'RPUSHX', 'RPOPLPUSH') THEN
            RETURN pgkv._call_list(c, argv, n);
        END IF;
        RETURN pgkv._call_keys(c, argv, n);
    WHEN 'G', 'I', 'A', 'M' THEN
        RETURN pgkv._call_string(c, argv, n);
    ELSE
        IF c IN ('DECR', 'DECRBY', 'DELIFEQ', 'DELEX', 'PSETEX') THEN
            RETURN pgkv._call_string(c, argv, n);
        END IF;
        RETURN pgkv._call_keys(c, argv, n);
    END CASE;
END;
$$;

-- CALL: Run a Redis command. See the top of this section.
CREATE OR REPLACE FUNCTION pgkv.call(VARIADIC argv TEXT[])
RETURNS JSONB
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pgkv, pg_temp
AS $$
DECLARE
    v_value TEXT;
    v_type TEXT;
BEGIN
    -- GET is by far the commonest command: answer it here, without the dispatcher.
    IF argv[1] IN ('GET', 'get') AND array_length(argv, 1) = 2 AND argv[2] IS NOT NULL THEN
        SELECT s.value #>> '{}', s.type INTO v_value, v_type
        FROM pgkv.store s
        WHERE s.key = argv[2] AND (s.expires_at IS NULL OR s.expires_at >= clock_timestamp());
        IF v_type <> 'string' THEN
            PERFORM pgkv._wrongtype();
        END IF;
        RETURN COALESCE(to_jsonb(v_value), 'null'::jsonb);
    END IF;
    RETURN pgkv._dispatch(argv);
END;
$$;

-- TRY_CALL: Run a Redis command; an error reply is returned as {"error": "<message>"}
-- rather than raised, and undoes only what that command did. Use it inside a transaction
-- you want to survive a WRONGTYPE or a bad argument. It costs a subtransaction per call.
CREATE OR REPLACE FUNCTION pgkv.try_call(VARIADIC argv TEXT[])
RETURNS JSONB
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pgkv, pg_temp
AS $$
DECLARE
    v_value TEXT;
    v_type TEXT;
BEGIN
    -- GET, as in pgkv.call: it cannot write, so it needs no subtransaction either.
    IF argv[1] IN ('GET', 'get') AND array_length(argv, 1) = 2 AND argv[2] IS NOT NULL THEN
        SELECT s.value #>> '{}', s.type INTO v_value, v_type
        FROM pgkv.store s
        WHERE s.key = argv[2] AND (s.expires_at IS NULL OR s.expires_at >= clock_timestamp());
        IF v_type <> 'string' THEN
            RETURN jsonb_build_object('error', 'WRONGTYPE Operation against a key holding the wrong kind of value');
        END IF;
        RETURN COALESCE(to_jsonb(v_value), 'null'::jsonb);
    END IF;

    BEGIN
        RETURN pgkv._dispatch(argv);
    EXCEPTION WHEN SQLSTATE 'KV000' OR SQLSTATE 'KV001' OR SQLSTATE 'KV002' THEN
        RETURN jsonb_build_object('error', SQLERRM);
    END;
END;
$$;

-- PIPELINE: Run several Redis commands in one call. p_commands is a JSON array of commands,
-- each an array of strings: '[["SET","k","v"],["INCR","n"]]'. Returns the array of replies;
-- a command that fails contributes {"error": "<message>"} and undoes only itself, the others
-- still apply, as in Redis.
-- The commands run in one statement, so other sessions see all of their effects or none.
-- p_atomic makes it MULTI/EXEC: if a command is unknown or has the wrong number of
-- arguments (the errors Redis detects while queueing), nothing is applied and the result is
-- not an array but the object
--   {"error": "EXECABORT Transaction discarded because of previous errors.",
--    "previous": [{"index": 1, "error": "ERR unknown command ..."}]}
-- listing every refused command by its zero-based position.
-- A Redis error never raises here, so it never aborts the caller's transaction. A database
-- error (deadlock, serialization failure) is raised and undoes everything.
CREATE OR REPLACE FUNCTION pgkv.pipeline(p_commands JSONB, p_atomic BOOLEAN DEFAULT FALSE)
RETURNS JSONB
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pgkv, pg_temp
AS $$
DECLARE
    v_words TEXT[];
    v_lengths INTEGER[];
    v_length INTEGER;
    v_at INTEGER := 1;
    v_index INTEGER := 0;
    v_replies JSONB[] := '{}';
    v_refused JSONB[] := '{}';
BEGIN
    IF p_commands IS NULL OR jsonb_typeof(p_commands) <> 'array'
        OR EXISTS (SELECT 1 FROM jsonb_array_elements(p_commands) AS c(command) WHERE jsonb_typeof(c.command) <> 'array') THEN
        PERFORM pgkv._err('ERR pgkv.pipeline takes a JSON array of commands, each an array of strings');
    END IF;

    -- Every command's words in one array, and how many words each command has.
    SELECT array_agg(jsonb_array_length(c.command) ORDER BY c.ord) INTO v_lengths
    FROM jsonb_array_elements(p_commands) WITH ORDINALITY AS c(command, ord);
    SELECT array_agg(w.word ORDER BY c.ord, w.ord) INTO v_words
    FROM jsonb_array_elements(p_commands) WITH ORDINALITY AS c(command, ord)
    CROSS JOIN LATERAL jsonb_array_elements_text(c.command) WITH ORDINALITY AS w(word, ord);

    -- The inner block is a subtransaction per command, so a failure undoes that command only.
    -- The outer block is what a discarded transaction rolls back to.
    BEGIN
        FOREACH v_length IN ARRAY COALESCE(v_lengths, '{}') LOOP
            BEGIN
                v_replies := v_replies || pgkv._dispatch(COALESCE(v_words[v_at : v_at + v_length - 1], '{}'));
            EXCEPTION
                WHEN SQLSTATE 'KV002' THEN
                    v_replies := v_replies || jsonb_build_object('error', SQLERRM);
                    IF p_atomic THEN
                        v_refused := v_refused || jsonb_build_object('index', v_index, 'error', SQLERRM);
                    END IF;
                WHEN SQLSTATE 'KV000' OR SQLSTATE 'KV001' THEN
                    v_replies := v_replies || jsonb_build_object('error', SQLERRM);
            END;
            v_at := v_at + v_length;
            v_index := v_index + 1;
        END LOOP;

        IF cardinality(v_refused) > 0 THEN
            RAISE EXCEPTION 'EXECABORT Transaction discarded because of previous errors.' USING ERRCODE = 'KV003';
        END IF;
    EXCEPTION WHEN SQLSTATE 'KV003' THEN
        RETURN jsonb_build_object('error', SQLERRM, 'previous', to_jsonb(v_refused));
    END;

    RETURN to_jsonb(v_replies);
END;
$$;

-- COMMANDS: The Redis commands pgkv.call understands
CREATE OR REPLACE FUNCTION pgkv.commands()
RETURNS SETOF TEXT
LANGUAGE sql
IMMUTABLE
SECURITY DEFINER
SET search_path = pgkv, pg_temp
AS $$
    SELECT unnest(ARRAY[
        'APPEND', 'COPY', 'DBSIZE', 'DECR', 'DECRBY', 'DEL', 'DELEX', 'DELIFEQ', 'ECHO', 'EXISTS', 'EXPIRE',
        'EXPIREAT', 'EXPIRETIME', 'FLUSHALL', 'FLUSHDB', 'GET', 'GETDEL', 'GETEX', 'GETRANGE', 'GETSET',
        'HDEL', 'HEXISTS', 'HGET', 'HGETALL', 'HINCRBY', 'HINCRBYFLOAT', 'HKEYS', 'HLEN', 'HMGET', 'HMSET',
        'HRANDFIELD', 'HSCAN', 'HSET', 'HSETNX', 'HSTRLEN', 'HVALS', 'INCR', 'INCRBY', 'INCRBYFLOAT', 'KEYS',
        'LINDEX', 'LINSERT', 'LLEN', 'LMOVE', 'LMPOP', 'LPOP', 'LPOS', 'LPUSH', 'LPUSHX', 'LRANGE', 'LREM',
        'LSET', 'LTRIM', 'MGET', 'MSET', 'MSETNX', 'PERSIST', 'PEXPIRE', 'PEXPIREAT', 'PEXPIRETIME', 'PING',
        'PSETEX', 'PTTL', 'RANDOMKEY', 'RENAME', 'RENAMENX', 'RPOP', 'RPOPLPUSH', 'RPUSH', 'RPUSHX', 'SADD',
        'SCAN', 'SCARD', 'SDIFF', 'SDIFFSTORE', 'SELECT', 'SET', 'SETEX', 'SETNX', 'SETRANGE', 'SINTER',
        'SINTERCARD', 'SINTERSTORE', 'SISMEMBER', 'SMEMBERS', 'SMISMEMBER', 'SMOVE', 'SPOP', 'SRANDMEMBER',
        'SREM', 'SSCAN', 'STRLEN', 'SUBSTR', 'SUNION', 'SUNIONSTORE', 'TIME', 'TOUCH', 'TTL', 'TYPE',
        'UNLINK', 'ZADD', 'ZCARD', 'ZCOUNT', 'ZDIFF', 'ZDIFFSTORE', 'ZINCRBY', 'ZINTER', 'ZINTERCARD',
        'ZINTERSTORE', 'ZLEXCOUNT', 'ZMPOP', 'ZMSCORE', 'ZPOPMAX', 'ZPOPMIN', 'ZRANDMEMBER', 'ZRANGE',
        'ZRANGEBYLEX', 'ZRANGEBYSCORE', 'ZRANGESTORE', 'ZRANK', 'ZREM', 'ZREMRANGEBYLEX', 'ZREMRANGEBYRANK',
        'ZREMRANGEBYSCORE', 'ZREVRANGE', 'ZREVRANGEBYLEX', 'ZREVRANGEBYSCORE', 'ZREVRANK', 'ZSCAN', 'ZSCORE',
        'ZUNION', 'ZUNIONSTORE'
    ])
$$;

