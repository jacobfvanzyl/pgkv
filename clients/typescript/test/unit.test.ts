import { describe, expect, test } from "vitest";
import { toArgv } from "../src/args.ts";
import { isRetryable, Pgkv, ReplyError, sqlState } from "../src/index.ts";
import { shapeReply } from "../src/reply.ts";
import { errorOf, mapError, PIPELINE } from "../src/run.ts";

describe("arguments to argv", () => {
  test("strings and numbers become text", () => {
    expect(toArgv("set", ["k", "v", "EX", 10])).toEqual(["set", "k", "v", "EX", "10"]);
    expect(toArgv("incrbyfloat", ["k", 1.5])).toEqual(["incrbyfloat", "k", "1.5"]);
    expect(toArgv("zadd", ["z", Number.POSITIVE_INFINITY, "m"])).toEqual(["zadd", "z", "Infinity", "m"]);
    expect(toArgv("set", ["k", 10n])).toEqual(["set", "k", "10"]);
    expect(toArgv("set", ["k", true])).toEqual(["set", "k", "true"]);
  });

  test("an integer too large to be exact is sent as digits, as ioredis does", () => {
    expect(toArgv("set", ["k", 1e21])).toEqual(["set", "k", "1000000000000000000000"]);
  });

  test("null and undefined become the empty string, as ioredis does", () => {
    expect(toArgv("set", ["k", null])).toEqual(["set", "k", ""]);
    expect(toArgv("set", ["k", undefined])).toEqual(["set", "k", ""]);
  });

  test("arrays are flattened one level", () => {
    expect(toArgv("del", [["a", "b"]])).toEqual(["del", "a", "b"]);
    expect(toArgv("rpush", ["l", ["a", "b"], "c"])).toEqual(["rpush", "l", "a", "b", "c"]);
    expect(toArgv("zadd", ["z", [1, "a", 2, "b"]])).toEqual(["zadd", "z", "1", "a", "2", "b"]);
    expect(toArgv("call", [["a", ["b", "c"]]])).toEqual(["call", "a", "b,c"]);
  });

  test("mset and msetnx take an object or a Map", () => {
    expect(toArgv("mset", [{ a: "1", b: 2 }])).toEqual(["mset", "a", "1", "b", "2"]);
    expect(
      toArgv("msetnx", [
        new Map<string, unknown>([
          ["a", "1"],
          ["b", 2],
        ]),
      ]),
    ).toEqual(["msetnx", "a", "1", "b", "2"]);
    expect(toArgv("mset", ["a", "1", "b", "2"])).toEqual(["mset", "a", "1", "b", "2"]);
    expect(toArgv("mset", [new Map([[1, 2]])])).toEqual(["mset", "1", "2"]);
  });

  test("hset and hmset take an object or a Map after the key", () => {
    expect(toArgv("hset", ["h", { a: "1", b: 2 }])).toEqual(["hset", "h", "a", "1", "b", "2"]);
    expect(toArgv("hmset", ["h", new Map([["a", "1"]])])).toEqual(["hmset", "h", "a", "1"]);
    expect(toArgv("hset", ["h", "a", "1"])).toEqual(["hset", "h", "a", "1"]);
    expect(toArgv("hset", ["h", { a: null }])).toEqual(["hset", "h", "a", ""]);
  });

  test("the object forms belong to the lowercase method names only, as in ioredis", () => {
    expect(toArgv("HSET", ["h", { a: "1" }])).toEqual(["HSET", "h", "[object Object]"]);
    expect(toArgv("sadd", ["s", { a: "1" }])).toEqual(["sadd", "s", "[object Object]"]);
  });

  test("bytes are accepted when they are UTF-8", () => {
    const bytes = new TextEncoder().encode("héllo ✓");
    expect(toArgv("set", [bytes, Buffer.from("wörld")])).toEqual(["set", "héllo ✓", "wörld"]);
    expect(toArgv("set", ["k", new Uint8Array(0)])).toEqual(["set", "k", ""]);
  });

  test("bytes that are not UTF-8 are rejected", () => {
    expect(() => toArgv("set", ["k", Buffer.from([0xff, 0xfe])])).toThrow(
      new TypeError("pgkv: argument 2 of 'set' is not valid UTF-8: pgkv stores text, not arbitrary bytes"),
    );
    expect(() => toArgv("set", ["k", new Uint8Array([0xc3])])).toThrow(/not valid UTF-8/);
  });

  test("text PostgreSQL cannot hold is rejected", () => {
    expect(() => toArgv("set", ["k", "a\u0000b"])).toThrow(/argument 2 of 'set' contains a NUL character/);
    expect(() => toArgv("set", ["k", Buffer.from([0x61, 0x00])])).toThrow(/NUL character/);
    expect(() => toArgv("set", ["\ud800", "v"])).toThrow(/argument 1 of 'set' is not well-formed Unicode/);
    expect(() => toArgv("set", ["k", "x\udc00"])).toThrow(/not well-formed Unicode/);
    expect(toArgv("set", ["k", "😀"])).toEqual(["set", "k", "😀"]);
  });

  test("a callback is rejected instead of being stored as text", () => {
    expect(() => toArgv("get", ["k", () => {}])).toThrow(
      /argument 2 of 'get' is a function: callbacks are not supported/,
    );
  });

  test("the command name must be a non-empty string", () => {
    expect(() => toArgv("", [])).toThrow(TypeError);
    expect(() => toArgv(undefined as unknown as string, [])).toThrow(TypeError);
  });
});

describe("reply shaping", () => {
  test("a status reply is a plain string", () => {
    expect(shapeReply("set", { status: "OK" })).toBe("OK");
    expect(shapeReply("ping", { status: "PONG" })).toBe("PONG");
    expect(shapeReply("type", { status: "zset" })).toBe("zset");
  });

  test("a bulk string that says OK stays a string", () => {
    expect(shapeReply("get", "OK")).toBe("OK");
  });

  test("integers, nil, strings and arrays pass through", () => {
    expect(shapeReply("incr", 5)).toBe(5);
    expect(shapeReply("get", null)).toBeNull();
    expect(shapeReply("zscore", "1.5")).toBe("1.5");
    expect(shapeReply("mget", ["a", null, "b"])).toEqual(["a", null, "b"]);
    expect(shapeReply("scan", ["0", ["k1", "k2"]])).toEqual(["0", ["k1", "k2"]]);
    expect(shapeReply("zmpop", ["z", [["m", "1"]]])).toEqual(["z", [["m", "1"]]]);
    expect(shapeReply("zrange", ["a", "1", "b", "2"])).toEqual(["a", "1", "b", "2"]);
    expect(shapeReply("hrandfield", ["f", "v"])).toEqual(["f", "v"]);
  });

  test("a status nested in an array is unwrapped too", () => {
    expect(shapeReply("x", [{ status: "OK" }, ["a", { status: "QUEUED" }]])).toEqual(["OK", ["a", "QUEUED"]]);
  });

  test("hgetall becomes an object", () => {
    expect(shapeReply("hgetall", ["a", "1", "b", "2"])).toEqual({ a: "1", b: "2" });
    expect(shapeReply("hgetall", [])).toEqual({});
  });

  test("hgetall keeps fields named like Object.prototype members", () => {
    const shaped = shapeReply("hgetall", ["__proto__", "p", "constructor", "c", "a", "1"]) as Record<string, unknown>;
    expect(Object.keys(shaped)).toEqual(["__proto__", "constructor", "a"]);
    expect(Object.getOwnPropertyDescriptor(shaped, "__proto__")?.value).toBe("p");
    expect(shaped.constructor).toBe("c");
    expect(({} as Record<string, unknown>).p).toBeUndefined();
  });

  test("only the lowercase name is transformed, as in ioredis", () => {
    expect(shapeReply("HGETALL", ["a", "1"])).toEqual(["a", "1"]);
  });
});

describe("errors", () => {
  const driverError = (code: string, message: string) => Object.assign(new Error(message), { code });
  const EXECABORT = "EXECABORT Transaction discarded because of previous errors.";

  test("a ReplyError looks like the one ioredis rejects with", () => {
    const error = new ReplyError("WRONGTYPE Operation against a key holding the wrong kind of value", {
      command: { name: "lpush", args: ["k", "v"] },
    });
    expect(error).toBeInstanceOf(Error);
    expect(error.name).toBe("ReplyError");
    expect(String(error)).toBe("ReplyError: WRONGTYPE Operation against a key holding the wrong kind of value");
    expect(Object.keys(error)).toEqual(["command"]);
    expect(Object.getOwnPropertyNames(error).sort()).toEqual(["command", "message", "stack"]);
    expect(Object.getOwnPropertyNames(new ReplyError("ERR syntax error")).sort()).toEqual(["message", "stack"]);
  });

  test("an error reply is an object with an error message, in place of a whole reply", () => {
    expect(errorOf({ error: "ERR syntax error" })).toBe("ERR syntax error");
    for (const reply of [{ status: "OK" }, "error", null, 1, ["error"], [{ error: "x" }], { error: 1 }]) {
      expect(errorOf(reply)).toBeUndefined();
    }
  });

  test("a raised SQLSTATE of class KV is a ReplyError with the same message", () => {
    for (const [code, message] of [
      ["KV000", "ERR pgkv.pipeline takes a JSON array of commands"],
      ["KV003", EXECABORT], // pgkv.pipeline returns this one now; raised, it is a reply like the others
    ] as const) {
      const cause = driverError(code, message);
      const error = mapError(cause) as ReplyError;
      expect(error).toBeInstanceOf(ReplyError);
      expect(error.message).toBe(message);
      expect(error.cause).toBe(cause);
      expect(error.command).toBeUndefined();
      expect(error.previousErrors).toBeUndefined();
    }
  });

  test("any other database error is returned unchanged", () => {
    for (const code of ["40001", "40P01", "23505", "57014", "08006", "25P02"]) {
      const cause = driverError(code, "boom");
      expect(mapError(cause)).toBe(cause);
    }
    const plain = new Error("socket hang up");
    expect(mapError(plain)).toBe(plain);
    expect(mapError("not an error")).toBe("not an error");
    // A Node.js system error code is not a SQLSTATE.
    const system = driverError("ECONNRESET", "read ECONNRESET");
    expect(mapError(system)).toBe(system);
  });

  test("a SQLSTATE is found through wrappers and in errno", () => {
    const wrapped = new Error("Failed query", { cause: driverError("KV000", "ERR syntax error") });
    expect(sqlState(wrapped)).toBe("KV000");
    expect((mapError(wrapped) as ReplyError).message).toBe("ERR syntax error");
    expect((mapError(wrapped) as ReplyError).cause).toBe(wrapped);
    const bun = Object.assign(new Error("ERR syntax error"), { code: "ERR_POSTGRES_SERVER_ERROR", errno: "KV000" });
    expect(sqlState(bun)).toBe("KV000");
    expect(mapError(bun)).toBeInstanceOf(ReplyError);
    expect(sqlState(new Error("x"))).toBeUndefined();
    expect(sqlState(undefined)).toBeUndefined();
  });

  test("isRetryable recognises serialization failures and deadlocks", () => {
    expect(isRetryable(driverError("40001", "could not serialize access"))).toBe(true);
    expect(isRetryable(driverError("40P01", "deadlock detected"))).toBe(true);
    expect(isRetryable(new Error("wrapped", { cause: driverError("40001", "x") }))).toBe(true);
    expect(isRetryable(driverError("KV000", "ERR syntax error"))).toBe(false);
    expect(isRetryable(driverError("23505", "duplicate key"))).toBe(false);
    expect(isRetryable(new Error("x"))).toBe(false);
    expect(isRetryable(null)).toBe(false);
  });
});

describe("the client over a fake executor", () => {
  const EXECABORT = "EXECABORT Transaction discarded because of previous errors.";

  /**
   * An executor that records every statement it is asked to run and answers each command
   * from a script, as pgkv.pipeline would. An Error for an answer fails the whole statement.
   */
  function fake(answer: (argv: string[]) => unknown = () => ({ status: "OK" })) {
    const log: string[] = [];
    const query = async (text: string, values: string[]) => {
      expect(text).toBe(PIPELINE);
      expect(values).toHaveLength(2);
      const commands = JSON.parse(values[0] as string) as string[][];
      log.push(`${values[1] === "true" ? "atomic" : "each"}: ${commands.map((argv) => argv.join(" ")).join(" | ")}`);
      const replies = commands.map(answer);
      const failure = replies.find((reply) => reply instanceof Error);
      if (failure) throw failure;
      return { rows: [{ reply: JSON.stringify(replies) }] };
    };
    return { log, query };
  }

  test("a command is one statement: a pipeline of one, as JSON text, not atomic", async () => {
    const calls: [string, unknown[]][] = [];
    const kv = new Pgkv({
      query: async (text, values) => {
        calls.push([text, values]);
        return { rows: [{ reply: '[{"status": "OK"}]' }] };
      },
    });
    expect(await kv.set("k", "v", "EX", 10)).toBe("OK");
    expect(calls).toEqual([[PIPELINE, ['[["set","k","v","EX","10"]]', "false"]]]);
    expect(PIPELINE).toBe("SELECT pgkv.pipeline($1::text::jsonb, $2::text::boolean)::text AS reply");
  });

  test("an error reply rejects with a ReplyError that names the command", async () => {
    const kv = new Pgkv(fake(() => ({ error: "ERR value is not an integer or out of range" })));
    const error = await kv.incr("k").catch((e: unknown) => e);
    expect(error).toBeInstanceOf(ReplyError);
    expect(error).toMatchObject({
      name: "ReplyError",
      message: "ERR value is not an integer or out of range",
      command: { name: "incr", args: ["k"] },
    });
    expect((error as ReplyError).cause).toBeUndefined();
  });

  test("a database error rejects a single command as the driver threw it", async () => {
    const lost = Object.assign(new Error("connection lost"), { code: "08006" });
    await expect(new Pgkv(fake(() => lost)).get("k")).rejects.toBe(lost);
  });

  test("has a method for every command and none for what pgkv lacks", () => {
    const kv = new Pgkv(fake()) as unknown as Record<string, unknown>;
    for (const name of new Pgkv(fake()).getBuiltinCommands()) expect(typeof kv[name], name).toBe("function");
    for (const name of [
      "subscribe",
      "publish",
      "blpop",
      "eval",
      "xadd",
      "quit",
      "disconnect",
      "duplicate",
      "connect",
      "watch",
      "exec",
    ]) {
      expect(kv[name], name).toBeUndefined();
    }
    expect(Object.keys(kv)).toEqual([]);
  });

  test("bad arguments reject the promise instead of throwing", async () => {
    const executor = fake();
    const kv = new Pgkv(executor);
    const promise = kv.set("k", "a\u0000");
    expect(promise).toBeInstanceOf(Promise);
    await expect(promise).rejects.toThrow(TypeError);
    expect(executor.log).toEqual([]);
  });

  test("accepts rows as arrays as well as objects", async () => {
    const kv = new Pgkv({ query: async () => ({ rows: [['["v"]']] }) });
    expect(await kv.get("k")).toBe("v");
  });

  test("an executor that does not return the replies as text is reported", async () => {
    await expect(new Pgkv({ query: async () => ({ rows: [] }) }).get("k")).rejects.toThrow(
      /did not return the reply column/,
    );
    // A driver that parsed the jsonb although it was cast to text.
    await expect(new Pgkv({ query: async () => ({ rows: [{ reply: ["v"] }] }) }).get("k")).rejects.toThrow(
      /did not return the reply column/,
    );
    await expect(new Pgkv({ query: async () => ({ rows: [{ reply: "[]" }] }) }).get("k")).rejects.toThrow(
      /one reply per command/,
    );
  });

  test("rejects something that is not an executor", () => {
    expect(() => new Pgkv({} as never)).toThrow(TypeError);
    expect(() => new Pgkv(null as never)).toThrow(TypeError);
    expect(() => new Pgkv("postgres://localhost" as never)).toThrow(TypeError);
  });

  test("an empty pipeline or multi resolves to [] without a query", async () => {
    const executor = fake();
    const kv = new Pgkv(executor);
    expect(await kv.pipeline().exec()).toEqual([]);
    expect(await kv.multi().exec()).toEqual([]);
    expect(executor.log).toEqual([]);
  });

  test("a pipeline queues, counts, chains, takes a batch and is sent as one statement", async () => {
    const executor = fake((argv) => argv[0]);
    const kv = new Pgkv(executor);
    const pipeline = kv.pipeline([
      ["set", "a", 1],
      ["get", "a"],
    ]);
    expect(pipeline.del("a").call("echo", "x")).toBe(pipeline);
    expect(pipeline.length).toBe(4);
    expect(executor.log).toEqual([]);
    expect(await pipeline.exec()).toEqual([
      [null, "set"],
      [null, "get"],
      [null, "del"],
      [null, "echo"],
    ]);
    expect(executor.log).toEqual(["each: set a 1 | get a | del a | echo x"]);
  });

  test("a multi is the same statement with the atomic flag", async () => {
    const executor = fake();
    expect(await new Pgkv(executor).multi().set("a", "1").set("b", "2").exec()).toEqual([
      [null, "OK"],
      [null, "OK"],
    ]);
    expect(executor.log).toEqual(["atomic: set a 1 | set b 2"]);
  });

  test("replies are shaped per command, and an error reply fills its pair alone", async () => {
    const answers: Record<string, unknown> = {
      hgetall: ["f", "v"],
      HGETALL: ["f", "v"],
      incr: { error: "ERR value is not an integer or out of range" },
      set: { status: "OK" },
      get: null,
    };
    for (const atomic of [false, true]) {
      const kv = new Pgkv(fake((argv) => answers[argv[0] as string]));
      const queued = (atomic ? kv.multi() : kv.pipeline()).set("a", "x").incr("a").hgetall("h").call("HGETALL", "h");
      const results = await queued.get("none").exec();
      expect(results).toEqual([
        [null, "OK"],
        [expect.any(ReplyError)],
        [null, { f: "v" }],
        [null, ["f", "v"]],
        [null, null],
      ]);
      expect(results[1]).toHaveLength(1);
      const error = results[1]?.[0] as ReplyError;
      expect(error.message).toBe("ERR value is not an integer or out of range");
      // ioredis names the command on a pipeline's errors and not on a transaction's.
      expect(error.command).toEqual(atomic ? undefined : { name: "incr", args: ["a"] });
    }
  });

  test("a discarded multi rejects with EXECABORT and the errors of the refused commands", async () => {
    const statements: string[][] = [];
    const kv = new Pgkv({
      query: async (_text, values) => {
        statements.push(values as string[]);
        const reply = {
          error: EXECABORT,
          previous: [
            { index: 1, error: "ERR unknown command 'nosuch', with args beginning with: 'x' '2' " },
            { index: 3, error: "ERR wrong number of arguments for 'get' command" },
          ],
        };
        return { rows: [{ reply: JSON.stringify(reply) }] };
      },
    });
    const error = (await kv
      .multi()
      .set("a", "1")
      .call("nosuch", "x", 2)
      .incr("a")
      .call("get")
      .exec()
      .catch((e: unknown) => e)) as ReplyError;
    expect(statements).toHaveLength(1);
    expect(error).toBeInstanceOf(ReplyError);
    expect(error.message).toBe(EXECABORT);
    expect(error.command).toEqual({ name: "exec", args: [] });
    expect(error.cause).toBeUndefined();
    expect(Object.getOwnPropertyNames(error).sort()).toEqual(["command", "message", "previousErrors", "stack"]);
    expect(error.previousErrors).toHaveLength(2);
    for (const previous of error.previousErrors ?? []) {
      expect(previous).toBeInstanceOf(ReplyError);
      expect(Object.getOwnPropertyNames(previous).sort()).toEqual(["command", "message", "stack"]);
    }
    expect(error.previousErrors?.map((previous) => [previous.message, previous.command])).toEqual([
      ["ERR unknown command 'nosuch', with args beginning with: 'x' '2' ", { name: "nosuch", args: ["x", "2"] }],
      ["ERR wrong number of arguments for 'get' command", { name: "get", args: [] }],
    ]);
  });

  test("an EXECABORT answer without a list of refused commands still rejects", async () => {
    const kv = new Pgkv({ query: async () => ({ rows: [{ reply: JSON.stringify({ error: EXECABORT }) }] }) });
    await expect(kv.multi().set("a", "1").exec()).rejects.toMatchObject({
      name: "ReplyError",
      message: EXECABORT,
      previousErrors: [],
    });
  });

  test("a database error rejects exec() of either kind as the driver threw it", async () => {
    const deadlock = Object.assign(new Error("deadlock detected"), { code: "40P01" });
    for (const atomic of [false, true]) {
      const kv = new Pgkv(fake((argv) => (argv[0] === "get" ? deadlock : { status: "OK" })));
      const queued = (atomic ? kv.multi() : kv.pipeline()).set("a", "1").get("a").set("b", "2");
      const error = await queued.exec().catch((e: unknown) => e);
      expect(error).toBe(deadlock);
      expect(isRetryable(error)).toBe(true);
    }
  });

  test("exec() runs once and a pipeline cannot be extended afterwards", async () => {
    const executor = fake();
    const pipeline = new Pgkv(executor).pipeline().set("a", "1");
    const first = pipeline.exec();
    expect(pipeline.exec()).toBe(first);
    await first;
    expect(executor.log).toEqual(["each: set a 1"]);
    expect(() => pipeline.get("a")).toThrow(/exec\(\) has already been called/);
  });

  test("a pool is only asked to query: nothing is checked out, begun or reserved", async () => {
    const refuse = (what: string) => () => {
      throw new Error(`${what} must not be called`);
    };
    const pool = { ...fake(), connect: refuse("connect"), release: refuse("release") };
    const kv = new Pgkv(pool);
    await kv.set("a", "1");
    await kv.pipeline().set("b", "1").exec();
    await kv.multi().set("c", "1").exec();
    expect(pool.log).toEqual(["each: set a 1", "each: set b 1", "atomic: set c 1"]);
  });

  test("postgres.js: sql, a transaction and a reserved connection are only asked for unsafe()", async () => {
    const log: string[] = [];
    const refuse = (what: string) => () => {
      throw new Error(`${what} must not be called`);
    };
    const sql = {
      unsafe: async (text: string, values: string[]) => {
        expect(text).toBe(PIPELINE);
        const commands = JSON.parse(values[0] as string) as string[][];
        log.push(`${values[1]}: ${commands.map((argv) => argv.join(" ")).join(" | ")}`);
        return [{ reply: JSON.stringify(commands.map(() => 1)) }];
      },
      begin: refuse("begin"),
      reserve: refuse("reserve"),
      savepoint: refuse("savepoint"),
      release: refuse("release"),
      // postgres.js's sql is a function; an object with both shapes is taken as postgres.js.
      query: refuse("query"),
    };
    const kv = new Pgkv(sql);
    expect(await kv.incr("a")).toBe(1);
    expect(await kv.pipeline().incr("b").incr("c").exec()).toEqual([
      [null, 1],
      [null, 1],
    ]);
    expect(await kv.multi().incr("d").exec()).toEqual([[null, 1]]);
    expect(log).toEqual(["false: incr a", "false: incr b | incr c", "true: incr d"]);
  });
});
