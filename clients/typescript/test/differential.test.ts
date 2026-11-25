/**
 * The proof that swapping the constructor is enough: every scenario in
 * conformance/scenarios.jsonl is replayed through real ioredis against real Redis and
 * through this client against pgkv, calling the same method with the same arguments on
 * both, and the two outcomes (reply, or thrown error name and message) are compared
 * command by command. Then again with each scenario queued and sent at once, as a
 * pipeline() and as a multi(); and every way a command can fail (alone, in a pipeline, in
 * a transaction, or by discarding one) is compared in full, with the keys it leaves behind.
 *
 * What pgkv answers is verified by the Python harness in conformance/; this test checks
 * the mapping on top of it, so it compares replies and applies the same allowances as
 * conformance/harness.py for what Redis leaves unspecified.
 */
import { readFileSync } from "node:fs";
import { Redis } from "ioredis";
import pg from "pg";
import { afterAll, beforeAll, describe, expect, test } from "vitest";
import { Pgkv } from "../src/index.ts";
import { DSN, REDIS_URL } from "./env.ts";

interface Scenario {
  name: string;
  source?: string;
  cmds: string[][];
}
interface Expected {
  command?: string;
  match: string;
  why: string;
}
type Outcome = { reply: unknown } | { error: { name: string; message: string } };

const read = (file: string) => readFileSync(new URL(`../../../conformance/${file}`, import.meta.url), "utf8");
const scenarios: Scenario[] = read("scenarios.jsonl")
  .split("\n")
  .filter((line) => line.trim())
  .map((line) => JSON.parse(line) as Scenario);
const expected = JSON.parse(read("expected.json")) as Expected[];

// The allowances of conformance/harness.py.
const UNORDERED = new Set(["KEYS", "HKEYS", "HVALS", "SMEMBERS", "SINTER", "SUNION", "SDIFF"]);
const RANDOM = new Set(["SPOP", "SRANDMEMBER", "HRANDFIELD", "ZRANDMEMBER", "RANDOMKEY"]);
const CLOCK: Record<string, number> = { TTL: 1, PTTL: 1500, EXPIRETIME: 1, PEXPIRETIME: 1500 };
const SCANS = new Set(["SCAN", "HSCAN", "SSCAN", "ZSCAN"]);
const LEX = new Set(["ZRANGEBYLEX", "ZREVRANGEBYLEX", "ZLEXCOUNT", "ZREMRANGEBYLEX", "ZRANGE", "ZRANGESTORE"]);
const NUMBER = /^-?[0-9]+(\.[0-9]+)?$/;

let inexactIntegers = 0;

/**
 * Equal, or two long decimal strings for the same number to 15 digits (INCRBYFLOAT), or
 * two approximations of an integer reply too large for a JavaScript number.
 */
function same(a: unknown, b: unknown): boolean {
  if (Array.isArray(a) && Array.isArray(b)) return a.length === b.length && a.every((x, i) => same(x, b[i]));
  if (a === b) return true;
  if (typeof a === "number" && typeof b === "number" && !Number.isSafeInteger(a) && !Number.isSafeInteger(b)) {
    // Beyond 2^53 neither client is exact. This one returns the nearest double (JSON.parse);
    // ioredis's parser adds the digits up one by one and lands a few thousand away.
    inexactIntegers++;
    return Math.abs(a - b) / Math.abs(a) < 1e-15;
  }
  if (
    typeof a === "string" &&
    typeof b === "string" &&
    Math.max(a.length, b.length) >= 15 &&
    NUMBER.test(a) &&
    NUMBER.test(b)
  ) {
    const [fa, fb] = [Number(a), Number(b)];
    if (fa === fb || Math.abs(fa - fb) <= 2e-17) return true;
    return fa !== 0 && Number.isFinite(fa) && Math.abs(fa - fb) / Math.abs(fa) < 1e-15;
  }
  return false;
}

function sameUnordered(a: unknown[], b: unknown[]): boolean {
  if (a.length !== b.length) return false;
  const rest = [...b];
  return a.every((x) => {
    const at = rest.findIndex((y) => same(x, y));
    if (at >= 0) rest.splice(at, 1);
    return at >= 0;
  });
}

const pairs = (flat: unknown[]) => flat.flatMap((x, i) => (i % 2 === 0 ? [[x, flat[i + 1]]] : []));
const isObject = (x: unknown): x is Record<string, unknown> => typeof x === "object" && x !== null && !Array.isArray(x);

/** Whether the oracle's outcome a and the client's outcome b are the same answer to argv. */
function equal(argv: string[], a: Outcome, b: Outcome, unspecified: boolean): boolean {
  const name = (argv[0] as string).toUpperCase();
  if ("error" in a || "error" in b) {
    return "error" in a && "error" in b && a.error.name === b.error.name && a.error.message === b.error.message;
  }
  const [x, y] = [a.reply, b.reply];
  if (name === "TIME") return Array.isArray(x) && Array.isArray(y) && x.length === y.length;
  if (RANDOM.has(name))
    return (
      typeof x === typeof y &&
      Array.isArray(x) === Array.isArray(y) &&
      (!Array.isArray(x) || x.length === (y as unknown[]).length)
    );
  if (name in CLOCK) {
    if (typeof x !== "number" || typeof y !== "number" || x < 0 || y < 0) return x === y;
    return Math.abs(x - y) <= (CLOCK[name] as number);
  }
  if (SCANS.has(name)) {
    if (!(Array.isArray(x) && Array.isArray(y) && x.length === 2 && y.length === 2)) return same(x, y);
    if (x[0] !== "0" || y[0] !== "0") return true; // cursors are opaque
    const withValues = (name === "HSCAN" || name === "ZSCAN") && !argv.some((arg) => arg.toUpperCase() === "NOVALUES");
    return withValues
      ? sameUnordered(pairs(x[1] as unknown[]), pairs(y[1] as unknown[]))
      : sameUnordered(x[1] as unknown[], y[1] as unknown[]);
  }
  if (LEX.has(name) && unspecified) return true;
  if (name === "HGETALL") {
    // Both clients return an object here; field order is unspecified.
    return isObject(x) && isObject(y) && sameUnordered(Object.entries(x), Object.entries(y));
  }
  if (Array.isArray(x) && Array.isArray(y) && UNORDERED.has(name)) return sameUnordered(x, y);
  return same(x, y);
}

/** The outcome as conformance/harness.py prints it, which expected.json's patterns match. */
function describeOutcome(outcome: Outcome): string {
  if ("reply" in outcome) return JSON.stringify(outcome.reply);
  const message = outcome.error.message.trim().replace(/^ERR /, "");
  return message.includes("'") && !message.includes('"')
    ? `Err("${message}")`
    : `Err('${message.replaceAll("'", "\\'")}')`;
}

/** exec() results with each error reduced to its name and message, so that two clients' can be compared. */
function pairsOf(results: [Error | null, unknown][] | null): unknown {
  return results?.map((pair) => (pair[0] ? [{ name: pair[0].name, message: pair[0].message }] : pair));
}

function documented(argv: string[], a: Outcome, b: Outcome): string | undefined {
  const text = `oracle=${describeOutcome(a)} pgkv=${describeOutcome(b)} state=None`;
  const name = (argv[0] as string).toUpperCase();
  return expected.find(
    (e) => (e.command === undefined || e.command === "*" || e.command === name) && new RegExp(e.match).test(text),
  )?.why;
}

describe.skipIf(!DSN || !REDIS_URL)("the same calls on ioredis against Redis and on Pgkv against PostgreSQL", () => {
  const pool = new pg.Pool({ connectionString: DSN, max: 1 });
  const kv = new Pgkv(pool);
  const redis = new Redis(REDIS_URL, { lazyConnect: true, maxRetriesPerRequest: 1 });
  const totals = {
    disturbed: 0,
    scenarios: 0,
    commands: 0,
    full: 0,
    random: 0,
    digits: 0,
    unspecified: 0,
    documented: new Map<string, number>(),
  };

  beforeAll(() => redis.connect());
  afterAll(async () => {
    await redis.flushdb();
    await redis.quit();
    await kv.flushdb();
    await pool.end();
  });

  /** Call the method named like the command, with the command's arguments, and record what happens. */
  async function outcome(client: Redis | Pgkv, argv: string[]): Promise<Outcome> {
    const [command, ...args] = argv as [string, ...string[]];
    // Never FLUSHALL the shared Redis: the oracle only owns the database it is connected to.
    const method = client === redis && command.toLowerCase() === "flushall" ? "flushdb" : command.toLowerCase();
    const target = client as unknown as Record<string, (...args: string[]) => Promise<unknown>>;
    expect(typeof target[method], `${method} is a method of both clients`).toBe("function");
    try {
      return { reply: await (target[method] as (...args: string[]) => Promise<unknown>)(...args) };
    } catch (error) {
      const { name, message } = error as Error;
      return { error: { name, message } };
    }
  }

  /** Lexicographic range commands are only defined when every member has the same score. */
  async function lexOverMixedScores(argv: string[]): Promise<boolean> {
    const name = (argv[0] as string).toUpperCase();
    if (!LEX.has(name) || argv.length < 3) return false;
    if ((name === "ZRANGE" || name === "ZRANGESTORE") && !argv.some((arg) => arg.toUpperCase() === "BYLEX"))
      return false;
    const key = (name === "ZRANGESTORE" ? argv[2] : argv[1]) as string;
    const scores = await redis.zrange(key, 0, -1, "WITHSCORES").catch(() => []);
    return new Set(scores.filter((_, i) => i % 2 === 1)).size > 1;
  }

  type End = "full" | "random" | "unspecified" | "digits" | { documented: string };

  /** Replay one scenario on both sides. Resolves to how it ended and how many commands were compared. */
  async function replay(scenario: Scenario): Promise<{ end: End; commands: number }> {
    await redis.flushdb();
    await kv.flushdb();
    let commands = 0;
    for (const [step, argv] of scenario.cmds.entries()) {
      const name = (argv[0] as string).toUpperCase();
      if (name === "SLEEP_MS") {
        await new Promise((resolve) => setTimeout(resolve, Number(argv[1])));
        continue;
      }
      const unspecified = await lexOverMixedScores(argv);
      const a = await outcome(redis, argv);
      const b = await outcome(kv, argv);
      commands++;
      if (!equal(argv, a, b, unspecified)) {
        const why = documented(argv, a, b);
        if (why) return { end: { documented: why }, commands };
        throw new Error(
          `step ${step} of "${scenario.name}" (${scenario.source ?? "?"})\n  ${JSON.stringify(argv)}\n` +
            `  ioredis on Redis: ${JSON.stringify(a)}\n  Pgkv on pgkv:     ${JSON.stringify(b)}`,
        );
      }
      // After these the two servers hold different data, so the rest cannot be compared.
      if ("reply" in a && RANDOM.has(name)) return { end: "random", commands };
      if ("reply" in a && unspecified) return { end: "unspecified", commands };
      const float = name === "INCRBYFLOAT" || name === "HINCRBYFLOAT";
      if (float && "reply" in a && "reply" in b && a.reply !== b.reply) return { end: "digits", commands };
    }
    return { end: "full", commands };
  }

  /** How often FLUSHALL has run on the Redis server, by anyone. It empties every database, ours included. */
  async function flushallCount(): Promise<number> {
    return Number(/cmdstat_flushall:calls=(\d+)/.exec(await redis.info("commandstats"))?.[1] ?? 0);
  }

  /**
   * The Redis server may be shared. If someone else's FLUSHALL emptied the oracle while fn
   * ran, what fn compared means nothing either way, so it is run again.
   */
  async function undisturbed<T>(fn: () => Promise<T>): Promise<T> {
    for (let attempt = 1; ; attempt++) {
      const before = await flushallCount();
      const inexactBefore = inexactIntegers;
      const result = await fn().then(
        (value) => ({ value }),
        (error: unknown) => ({ error }),
      );
      if ((await flushallCount()) === before) {
        if ("error" in result) throw result.error;
        return result.value;
      }
      totals.disturbed++;
      inexactIntegers = inexactBefore;
      if (attempt === 40)
        expect.fail("another client ran FLUSHALL on the Redis server during every one of 40 attempts");
      await new Promise((resolve) => setTimeout(resolve, 100 * attempt));
    }
  }

  test.each(scenarios.map((scenario, index) => [`${index + 1}: ${scenario.name}`, scenario] as const))(
    "%s",
    async (_title, scenario) => {
      const result = await undisturbed(() => replay(scenario));
      totals.scenarios++;
      totals.commands += result.commands;
      if (typeof result.end === "string") totals[result.end]++;
      else totals.documented.set(result.end.documented, (totals.documented.get(result.end.documented) ?? 0) + 1);
    },
    120_000,
  );

  /** What a call does on each client: [ioredis on Redis, Pgkv on pgkv]. */
  async function both<T>(use: (client: Redis | Pgkv) => Promise<T>): Promise<[Outcome, Outcome]> {
    const run = (client: Redis | Pgkv) =>
      use(client).then(
        (reply): Outcome => ({ reply }),
        (error: Error): Outcome => ({ error: { name: error.name, message: error.message } }),
      );
    return [await run(redis), await run(kv)];
  }

  // The scenarios only pass strings. These are the argument and reply shapes ioredis adds on top.
  test("argument and reply shaping is the same as ioredis's", async () => {
    await undisturbed(async () => {
      await redis.flushdb();
      await kv.flushdb();
      // Typed against Pgkv: at run time `client` is each of the two in turn.
      const shapes: ((client: Pgkv) => Promise<unknown>)[] = [
        (c) => c.mset({ a: "1", b: 2 }),
        (c) =>
          c.mset(
            new Map<string, string | number>([
              ["c", "3"],
              ["d", 4],
            ]),
          ),
        (c) => c.msetnx({ a: "x", z: "y" }),
        (c) => c.mget("a", "b", "c", "d", "z"),
        (c) => c.mget(["a", "b"]),
        (c) => c.hset("h", { name: "Ada", born: 1815 }),
        (c) => c.hset("h", new Map([["died", "1852"]])),
        (c) => c.hmset("h", { field: "value" }),
        (c) => c.hgetall("h").then((hash) => Object.entries(hash).sort()),
        (c) => c.hgetall("missing"),
        (c) => c.call("hgetall", "missing"),
        (c) => c.call("HGETALL", "missing"),
        (c) => c.call("hgetall", "h").then((hash) => Object.entries(hash as object).sort()),
        (c) => c.call("HLEN", "h"),
        (c) => c.hset("proto", "__proto__", "p", "constructor", "c"),
        (c) => c.hgetall("proto").then((hash) => Object.keys(hash).sort()),
        (c) => c.del(["a", "b"]),
        // ioredis flattens nested arrays at run time although its types do not offer this form.
        (c) => c.rpush("l", ["x", "y"] as unknown as string, "z"),
        (c) => c.lrange("l", 0, -1),
        (c) => c.lpop("l", 2),
        (c) => c.set("n", 1e21),
        (c) => c.get("n"),
        (c) => c.set("f", 1.5),
        (c) => c.incrbyfloat("f", 0.25),
        (c) => c.set("empty", null as unknown as string),
        (c) => c.get("empty"),
        (c) => c.set("bytes", Buffer.from("héllo ✓")),
        (c) => c.get("bytes"),
        (c) => c.set(Buffer.from("bytekey"), "v", "EX", 100),
        (c) => c.ttl("bytekey"),
        (c) => c.set("ok", "OK"),
        (c) => c.get("ok"),
        (c) => c.set("ok", "v", "NX"),
        (c) => c.set("ok", "v2", "GET"),
        (c) => c.type("h"),
        (c) => c.type("missing"),
        (c) => c.ping(),
        (c) => c.ping("hello"),
        (c) => c.echo("😀"),
        (c) => c.zadd("z", 1, "a", 2.5, "b", "+inf", "c"),
        (c) => c.zrange("z", 0, -1, "WITHSCORES"),
        (c) => c.zscore("z", "b"),
        (c) => c.zincrby("z", 1, "a"),
        (c) => c.zrank("z", "b", "WITHSCORE"),
        (c) => c.zpopmin("z", 2),
        (c) => c.zmpop(1, "z", "MAX"),
        (c) => c.zmpop(1, "z", "MAX"),
        (c) => c.lmpop(1, "l", "LEFT"),
        (c) => c.exists("a", "c", "d"),
        (c) => c.scan(0, "MATCH", "nothing*", "COUNT", 1000),
        (c) => c.sadd("s", 1, 2, 3),
        (c) => c.smismember("s", 1, 9),
        (c) => c.spop("missing"),
        (c) => c.srandmember("missing", 2),
        (c) => c.getex("ok", "PERSIST"),
        (c) => c.expire("ok", 100, "NX"),
        // Errors: name and message.
        (c) => c.lpush("ok", "x"),
        (c) => c.incr("ok"),
        (c) => c.call("get"),
        (c) => c.set("k", "v", "EX", "soon"),
        (c) => c.call("set", "k", "v", "BOGUS"),
        (c) => c.zadd("z", "nan", "m"),
        (c) => c.hincrby("h", "name", 1),
        (c) => c.lset("missing", 0, "v"),
        (c) => c.rename("missing", "other"),
        // Pipelines: the pairs, with an error in the middle.
        (c) => c.pipeline().set("p", "1").lpush("p", "x").incr("p").hgetall("h").exec().then(pairsOf),
        (c) =>
          c
            .pipeline([
              ["set", "q", "1"],
              ["get", "q"],
            ])
            .exec()
            .then(pairsOf),
        (c) => c.pipeline().exec(),
        // Transactions.
        (c) => c.multi().set("m", "1").incr("m").hgetall("h").exec().then(pairsOf),
        (c) =>
          c
            .multi([
              ["set", "m2", "1"],
              ["get", "m2"],
            ])
            .exec()
            .then(pairsOf),
        (c) => c.multi().exec(),
      ];
      for (const [index, shape] of shapes.entries()) {
        const [a, b] = await both(shape as (client: Redis | Pgkv) => Promise<unknown>);
        expect(b, `shape ${index}: ${shape}`).toEqual(a);
      }
    });
  });

  /** Everything observable about an error: what it is, what it says, and the properties ioredis attaches. */
  function errorDetail(error: unknown): unknown {
    if (!(error instanceof Error)) return error;
    const { command, previousErrors } = error as Error & { command?: unknown; previousErrors?: unknown[] };
    return {
      isError: true,
      name: error.name,
      message: error.message,
      text: String(error),
      properties: Object.getOwnPropertyNames(error)
        .filter((property) => property !== "stack" && property !== "cause")
        .sort(),
      command,
      previousErrors: previousErrors?.map(errorDetail),
    };
  }

  /** An exec() result, or the error exec() rejected with, in full: also how long each pair is. */
  const execDetail = (exec: Promise<[Error | null, unknown][] | null>) =>
    exec.then(
      (results) => ({
        resolved: results?.map((pair) => ({ length: pair.length, error: errorDetail(pair[0]), result: pair[1] })),
      }),
      (error: unknown) => ({ rejected: errorDetail(error) }),
    );

  /** Every key with its type and contents: what the commands left behind. */
  async function keyState(client: Redis | Pgkv): Promise<unknown> {
    const c = client as Pgkv;
    const state: Record<string, unknown> = {};
    for (const key of (await c.keys("*")).sort()) {
      const type = await c.type(key);
      if (type === "string") state[key] = await c.get(key);
      else if (type === "list") state[key] = await c.lrange(key, 0, -1);
      else if (type === "hash") state[key] = Object.entries(await c.hgetall(key)).sort();
      else if (type === "set") state[key] = (await c.smembers(key)).sort();
      else state[key] = await c.zrange(key, 0, -1, "WITHSCORES");
    }
    return state;
  }

  type Queued = ReturnType<Pgkv["pipeline"]>;
  /** Run the same use of a client on both, from the same starting keys, and return [ioredis's, Pgkv's] outcome and keys. */
  async function sideBySide(use: (client: Pgkv) => Promise<unknown>) {
    const sides: { outcome: unknown; keys: unknown }[] = [];
    for (const client of [redis, kv]) {
      const c = client as Pgkv;
      await c.flushdb();
      await c.set("string", "text");
      await c.set("counter", "10");
      await c.hset("hash", { field: "value", other: "1" });
      await c.rpush("list", "a", "b");
      sides.push({ outcome: await use(c), keys: await keyState(client) });
    }
    return sides as [{ outcome: unknown; keys: unknown }, { outcome: unknown; keys: unknown }];
  }

  const WRONGTYPE = "WRONGTYPE Operation against a key holding the wrong kind of value";
  const EXECABORT = "EXECABORT Transaction discarded because of previous errors.";

  describe("errors, pipelines and transactions, mode by mode", () => {
    test("a single command that fails rejects with the same ReplyError and changes nothing", async () => {
      await undisturbed(async () => {
        const failing: ((c: Pgkv) => Promise<unknown>)[] = [
          (c) => c.lpush("string", "x"),
          (c) => c.incr("string"),
          (c) => c.hincrby("hash", "field", 1),
          (c) => c.call("get"),
          (c) => c.call("nosuchcommand", "a", "b"),
          (c) => c.set("string", "v", "EX", "soon"),
          (c) => c.hset("list", { a: "1" }),
        ];
        for (const [index, fail] of failing.entries()) {
          const [a, b] = await sideBySide((c) =>
            fail(c).then(
              (reply) => ({ reply }),
              (error: unknown) => ({ rejected: errorDetail(error) }),
            ),
          );
          expect(a.outcome, `${index}: ${fail}`).toHaveProperty("rejected.name", "ReplyError");
          expect(b, `${index}: ${fail}`).toEqual(a);
        }
      });
    });

    test("pipeline(): a failing command in the middle, an unknown one, and the reply transformers", async () => {
      await undisturbed(async () => {
        const [a, b] = await sideBySide((c) =>
          execDetail(
            c
              .pipeline()
              .incr("counter")
              .lpush("string", "x") // WRONGTYPE
              .incr("counter")
              .hgetall("hash") // transformed to an object
              .call("hgetall", "hash") // transformed too
              .call("HGETALL", "hash") // not transformed: the transformer belongs to the lowercase name
              .call("nosuchcommand", "x") // unknown
              .call("get") // wrong number of arguments
              .hset("fresh", { a: "1", b: 2 }) // argument transformer
              .hgetall("missing")
              .rpush("list", "c")
              .get("counter")
              .exec(),
          ),
        );
        expect(b).toEqual(a);
        // And it is what it should be, so that two equal mistakes do not pass.
        const resolved = (b.outcome as { resolved: { length: number; error: unknown; result: unknown }[] }).resolved;
        expect(resolved.map((pair) => pair.length)).toEqual([2, 1, 2, 2, 2, 2, 1, 1, 2, 2, 2, 2]);
        expect(resolved[1]).toEqual({
          length: 1,
          error: {
            isError: true,
            name: "ReplyError",
            message: WRONGTYPE,
            text: `ReplyError: ${WRONGTYPE}`,
            properties: ["command", "message"],
            command: { name: "lpush", args: ["string", "x"] },
            previousErrors: undefined,
          },
          result: undefined,
        });
        expect(resolved[3]?.result).toEqual({ field: "value", other: "1" });
        expect(resolved[5]?.result).toHaveLength(4);
        expect(resolved[11]).toEqual({ length: 2, error: null, result: "12" });
        expect(b.keys).toMatchObject({ counter: "12", list: ["a", "b", "c"], string: "text" });
      });
    });

    test("multi(): a command that fails while running is reported in its slot and the others apply", async () => {
      await undisturbed(async () => {
        const [a, b] = await sideBySide((c) =>
          execDetail(
            c
              .multi()
              .incr("counter")
              .lpush("string", "x") // WRONGTYPE
              .incr("string") // not an integer
              .set("after", "1")
              .hgetall("hash")
              .call("HGETALL", "hash")
              .hset("fresh", new Map([["a", "1"]]))
              .lset("list", 99, "x") // index out of range
              .rpush("list", "c")
              .incr("counter")
              .exec(),
          ),
        );
        expect(b).toEqual(a);
        const resolved = (b.outcome as { resolved: { length: number; error: unknown; result: unknown }[] }).resolved;
        expect(resolved.map((pair) => pair.length)).toEqual([2, 1, 1, 2, 2, 2, 2, 1, 2, 2]);
        // ioredis does not attach the command to an error from inside a transaction.
        expect(resolved[1]?.error).toMatchObject({ message: WRONGTYPE, properties: ["message"], command: undefined });
        expect(b.keys).toMatchObject({ after: "1", counter: "12", list: ["a", "b", "c"], fresh: [["a", "1"]] });
      });
    });

    test("multi(): a command Redis refuses to queue discards the transaction and exec() rejects", async () => {
      await undisturbed(async () => {
        const refused: ((queued: Queued) => Queued)[] = [
          (m) => m.call("nosuchcommand", "x"),
          (m) => m.call("get"),
          (m) => m.call("set", "only-a-key"),
          (m) => m.call("get").lpush("string", "x").call("nosuchcommand"), // two refused, and a runtime error
          (m) => m.call("nosuchcommand", "a", "b").call("NoSuchCommand").call("GET", "a", "b").call("hset", "hash"),
        ];
        const expected = [
          [{ name: "nosuchcommand", args: ["x"] }],
          [{ name: "get", args: [] }],
          [{ name: "set", args: ["only-a-key"] }],
          [
            { name: "get", args: [] },
            { name: "nosuchcommand", args: [] },
          ],
          [
            { name: "nosuchcommand", args: ["a", "b"] },
            { name: "NoSuchCommand", args: [] },
            { name: "GET", args: ["a", "b"] },
            { name: "hset", args: ["hash"] },
          ],
        ];
        for (const [index, refuse] of refused.entries()) {
          const [a, b] = await sideBySide((c) =>
            execDetail(refuse(c.multi().incr("counter").set("after", "1")).rpush("list", "c").exec()),
          );
          expect(a.outcome, `${index}`).toMatchObject({
            rejected: {
              name: "ReplyError",
              message: EXECABORT,
              text: `ReplyError: ${EXECABORT}`,
              properties: ["command", "message", "previousErrors"],
              command: { name: "exec", args: [] },
            },
          });
          // The errors of the refused commands, each a ReplyError naming its command; the
          // command that would only have failed while running is not among them.
          const { previousErrors } = (a.outcome as { rejected: { previousErrors: Record<string, unknown>[] } })
            .rejected;
          expect(previousErrors.map((previous) => previous.command)).toEqual(expected[index]);
          for (const previous of previousErrors) {
            expect(previous).toMatchObject({ isError: true, name: "ReplyError", properties: ["command", "message"] });
          }
          // Nothing was applied, on either side.
          expect(a.keys).toEqual({ counter: "10", hash: expect.anything(), list: ["a", "b"], string: "text" });
          // And Pgkv's rejection and keys are ioredis's, in full.
          expect(b, `${index}`).toEqual(a);
        }
      });
    });

    test("multi(): an argument count Redis only rejects when the command runs does not discard", async () => {
      await undisturbed(async () => {
        const [a, b] = await sideBySide((c) =>
          execDetail(
            c
              .multi()
              .incr("counter")
              .call("mset", "a", "1", "b") // odd
              .call("msetnx", "a", "1", "b")
              .call("hset", "hash", "f", "v", "g")
              .call("hmset", "hash", "f", "v", "g")
              .call("lpop", "list", "1", "2")
              .call("rpop", "list", "1", "2")
              .call("zrank", "z", "m", "withscore", "x")
              .call("zrevrank", "z", "m", "withscore", "x")
              .call("ping", "a", "b")
              .rpush("list", "c")
              .exec(),
          ),
        );
        expect(b).toEqual(a);
        const resolved = (b.outcome as { resolved: { length: number }[] }).resolved;
        expect(resolved.map((pair) => pair.length)).toEqual([2, 1, 1, 1, 1, 1, 1, 1, 1, 1, 2]);
        expect(b.keys).toMatchObject({ counter: "11", list: ["a", "b", "c"] });
      });
    });

    test("empty and array forms", async () => {
      await undisturbed(async () => {
        const forms: ((c: Pgkv) => ReturnType<Queued["exec"]>)[] = [
          (c) => c.pipeline().exec(),
          (c) => c.multi().exec(),
          (c) => c.pipeline([]).exec(),
          (c) => c.multi([]).exec(),
          (c) =>
            c
              .pipeline([
                ["incr", "counter"],
                ["lpush", "string", "x"],
                ["hgetall", "hash"],
              ])
              .exec(),
          (c) =>
            c
              .multi([
                ["incr", "counter"],
                ["lpush", "string", "x"],
                ["hgetall", "hash"],
              ])
              .exec(),
          (c) => c.pipeline().ping().echo("x").type("hash").exec(),
        ];
        for (const [index, form] of forms.entries()) {
          const [a, b] = await sideBySide((c) => execDetail(form(c)));
          expect(b, `${index}: ${form}`).toEqual(a);
        }
      });
    });
  });

  /**
   * Every scenario again, this time with all of its commands queued and sent at once: as a
   * pipeline() and as a multi(), on both clients. The pairs are compared one by one with
   * the same allowances as above, up to the first command after which the two servers may
   * legitimately hold different data.
   */
  describe("each scenario as one pipeline() and as one multi()", () => {
    const batchable = scenarios.filter((scenario) =>
      scenario.cmds.every((argv) => argv[0]?.toUpperCase() !== "SLEEP_MS"),
    );
    const batchTotals = { scenarios: 0, pairs: 0, discarded: 0 };

    async function runBatch(client: Redis | Pgkv, scenario: Scenario, atomic: boolean) {
      const c = client as Pgkv;
      await c.flushdb();
      const queued = atomic ? c.multi() : c.pipeline();
      const target = queued as unknown as Record<string, (...args: string[]) => unknown>;
      for (const [command, ...args] of scenario.cmds as [string, ...string[]][]) {
        // Never FLUSHALL the shared Redis: the oracle only owns the database it is connected to.
        const method = client === redis && command.toLowerCase() === "flushall" ? "flushdb" : command.toLowerCase();
        (target[method] as (...args: string[]) => unknown)(...args);
      }
      return queued.exec().then(
        (results) => ({ results }),
        (error: Error & { previousErrors?: (Error & { command?: unknown })[] }) => ({
          rejected: {
            name: error.name,
            message: error.message,
            previousErrors: error.previousErrors?.map((previous) => ({
              name: previous.name,
              message: previous.message,
              command: previous.command,
            })),
          },
        }),
      );
    }

    const outcomeOf = (pair: [Error | null, unknown]): Outcome =>
      pair[0] ? { error: { name: pair[0].name, message: pair[0].message } } : { reply: pair[1] };

    test.each(batchable.map((scenario, index) => [`${index + 1}: ${scenario.name}`, scenario] as const))(
      "%s",
      async (_title, scenario) => {
        await undisturbed(async () => {
          const inexactBefore = inexactIntegers;
          let pairs = 0;
          for (const atomic of [false, true]) {
            const a = await runBatch(redis, scenario, atomic);
            const b = await runBatch(kv, scenario, atomic);
            const mode = atomic ? "multi" : "pipeline";
            if ("rejected" in a || "rejected" in b) {
              expect(atomic, `${mode} of "${scenario.name}" rejected: ${JSON.stringify([a, b])}`).toBe(true);
              // Discarded on both sides or on neither, for the same refused commands.
              expect(a, `${mode} of "${scenario.name}"`).toMatchObject({ rejected: { message: EXECABORT } });
              expect(b, `${mode} of "${scenario.name}"`).toEqual(a);
              batchTotals.discarded++;
              continue;
            }
            expect(b.results, `${mode} of "${scenario.name}"`).toHaveLength(a.results?.length ?? -1);
            for (const [step, argv] of scenario.cmds.entries()) {
              const name = (argv[0] as string).toUpperCase();
              const [x, y] = [a.results?.[step], b.results[step]] as [[Error | null, unknown], [Error | null, unknown]];
              expect(y.length, `${mode} step ${step} of "${scenario.name}": pair length`).toBe(x.length);
              const [oa, ob] = [outcomeOf(x), outcomeOf(y)];
              // Whether a lex range ran over mixed scores cannot be asked in the middle of a
              // batch, so such a reply is taken as unspecified, and the comparison ends there.
              const lex = LEX.has(name) && (argv.some((arg) => arg.toUpperCase() === "BYLEX") || name.includes("LEX"));
              if (!equal(argv, oa, ob, lex)) {
                if (documented(argv, oa, ob)) break;
                throw new Error(
                  `${mode} step ${step} of "${scenario.name}" (${scenario.source ?? "?"})\n  ${JSON.stringify(argv)}\n` +
                    `  ioredis on Redis: ${JSON.stringify(oa)}\n  Pgkv on pgkv:     ${JSON.stringify(ob)}`,
                );
              }
              pairs++;
              if ("reply" in oa && (RANDOM.has(name) || lex)) break;
              const float = name === "INCRBYFLOAT" || name === "HINCRBYFLOAT";
              if (float && "reply" in oa && "reply" in ob && oa.reply !== ob.reply) break;
            }
          }
          inexactIntegers = inexactBefore; // counted once, by the command-by-command replay
          batchTotals.scenarios++;
          batchTotals.pairs += pairs;
        });
      },
      120_000,
    );

    test("summary", () => {
      console.log(
        `differential, batched: ${batchTotals.scenarios} scenarios each sent as one pipeline() and as one multi(), ` +
          `${batchTotals.pairs} [error, result] pairs compared, ${batchTotals.discarded} transactions discarded (EXECABORT) on both sides ` +
          "with the same previousErrors, 0 discarded on one side only",
      );
      expect(batchTotals.scenarios).toBe(batchable.length);
    });
  });

  test("summary", () => {
    const documentedCount = [...totals.documented.values()].reduce((sum, n) => sum + n, 0);
    console.log(
      `differential: ${totals.scenarios} scenarios, ${totals.commands} commands compared: ${totals.full} scenarios match to the end, ` +
        `${totals.random + totals.digits + totals.unspecified} stop early after a matching reply ` +
        `(${totals.random} random command, ${totals.digits} INCRBYFLOAT digits, ${totals.unspecified} unspecified lex order), ` +
        `${documentedCount} stop at a documented difference` +
        [...totals.documented].map(([why, n]) => `\n  documented (${n}): ${why}`).join("") +
        `\n  ${inexactIntegers} integer replies beyond 2^53 compared as approximations` +
        `\n  ${totals.disturbed} runs repeated because another client ran FLUSHALL on the Redis server meanwhile`,
    );
    expect(totals.scenarios).toBe(scenarios.length);
  });
});
