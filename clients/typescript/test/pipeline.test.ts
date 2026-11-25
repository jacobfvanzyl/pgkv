import pg from "pg";
import postgres from "postgres";
import { afterAll, beforeEach, describe, expect, test } from "vitest";
import { isRetryable, Pgkv, ReplyError, sqlState } from "../src/index.ts";
import { PIPELINE } from "../src/run.ts";
import { DSN } from "./env.ts";

const WRONGTYPE = "WRONGTYPE Operation against a key holding the wrong kind of value";
const NOT_AN_INTEGER = "ERR value is not an integer or out of range";
const EXECABORT = "EXECABORT Transaction discarded because of previous errors.";

describe.skipIf(!DSN)("pipeline and multi", () => {
  // One connection, and a short wait for it: a leaked connection fails the next test at once.
  const pool = new pg.Pool({ connectionString: DSN, max: 1, connectionTimeoutMillis: 3000 });
  const sql = postgres(DSN, { max: 1, onnotice: () => {} });
  const kv = new Pgkv(pool);
  const idle = () => expect([pool.totalCount, pool.idleCount, pool.waitingCount]).toEqual([1, 1, 0]);

  /** A client on the pool that records every statement it sends. */
  function counting() {
    const statements: string[] = [];
    const client = new Pgkv({
      query: (text: string, values: string[]) => {
        statements.push(text);
        return pool.query(text, values);
      },
    });
    return { statements, client };
  }

  beforeEach(async () => void (await kv.flushdb()));
  afterAll(async () => {
    await pool.end();
    await sql.end();
  });

  describe("pipeline on a pool", () => {
    test("runs the commands in order and shapes each reply", async () => {
      const results = await kv
        .pipeline()
        .set("n", "1")
        .incr("n")
        .append("n", "0")
        .get("n")
        .hset("h", { a: "1" })
        .hgetall("h")
        .get("missing")
        .exec();
      expect(results).toEqual([
        [null, "OK"],
        [null, 2],
        [null, 2],
        [null, "20"],
        [null, 1],
        [null, { a: "1" }],
        [null, null],
      ]);
      idle();
    });

    test("is one statement, however many commands", async () => {
      const { statements, client } = counting();
      const pipeline = client.pipeline();
      for (let i = 0; i < 500; i++) pipeline.rpush("list", `item ${i}`);
      const results = await pipeline.llen("list").exec();
      expect(results).toHaveLength(501);
      expect(results[500]).toEqual([null, 500]);
      expect(statements).toEqual([PIPELINE]);
    });

    test("a failing command has its error in its slot and the others still apply", async () => {
      const results = await kv.pipeline().set("s", "v").lpush("s", "x").incr("s").set("t", "after").exec();
      expect(results).toHaveLength(4);
      expect(results[0]).toEqual([null, "OK"]);
      expect(results[1]).toHaveLength(1); // [error], as ioredis
      expect(results[1]?.[0]).toBeInstanceOf(ReplyError);
      expect(results[1]?.[0]).toMatchObject({
        name: "ReplyError",
        message: WRONGTYPE,
        command: { name: "lpush", args: ["s", "x"] },
      });
      expect(results[2]?.[0]?.message).toBe(NOT_AN_INTEGER);
      expect(results[3]).toEqual([null, "OK"]);
      expect(await kv.mget("s", "t")).toEqual(["v", "after"]);
      idle();
    });

    test("a failing command undoes only what it did itself", async () => {
      await kv.rpush("list", "a");
      // MSETNX-style partial work is not at stake here; a command that fails half way is.
      const results = await kv.pipeline().set("a", "1").call("lpush", "a", "x").rpush("list", "b").exec();
      expect(results).toMatchObject([[null, "OK"], [{ message: WRONGTYPE }], [null, 2]]);
      expect(await kv.get("a")).toBe("1");
      expect(await kv.lrange("list", 0, -1)).toEqual(["a", "b"]);
    });

    test("an unsupported command or a wrong number of arguments is an error in its slot", async () => {
      const results = await kv.pipeline().call("publish", "ch", "m").call("get").ping().exec();
      expect(results[0]?.[0]?.message).toBe("ERR unknown command 'publish', with args beginning with: 'ch' 'm' ");
      expect(results[1]?.[0]?.message).toBe("ERR wrong number of arguments for 'get' command");
      expect(results[2]).toEqual([null, "PONG"]);
    });

    test("a database error rejects exec as the driver threw it, and no command was applied", async () => {
      // Another session holds the row lock on "locked"; this one gives up waiting for it.
      // (Two clients of their own: pg's Pool.query discards a connection whose query failed,
      // and the lock_timeout setting with it.)
      const [holder, waiter] = [new pg.Client({ connectionString: DSN }), new pg.Client({ connectionString: DSN })];
      await Promise.all([holder.connect(), waiter.connect()]);
      try {
        await kv.set("locked", "1");
        await holder.query("BEGIN");
        await new Pgkv(holder).incr("locked");
        await waiter.query("SET lock_timeout = '200ms'");
        const waiting = new Pgkv(waiter);
        for (const queued of [waiting.pipeline(), waiting.multi()]) {
          const error = await queued
            .set("before", "1")
            .incr("locked")
            .set("after", "1")
            .exec()
            .catch((e: unknown) => e);
          expect(error).not.toBeInstanceOf(ReplyError);
          expect(sqlState(error)).toBe("55P03"); // lock_not_available
          expect(isRetryable(error)).toBe(false);
          // The statement was undone as a whole: not even the command before the failure stays.
          expect(await kv.exists("before", "after")).toBe(0);
        }
        const single = await waiting.incr("locked").catch((e: unknown) => e);
        expect(single).not.toBeInstanceOf(ReplyError);
        expect(sqlState(single)).toBe("55P03");
        await holder.query("ROLLBACK");
        expect(await waiting.incr("locked")).toBe(2);
      } finally {
        await Promise.all([holder.end(), waiter.end()]);
      }
    });

    test("a serialization failure is retryable and undoes the whole pipeline", async () => {
      const other = new pg.Client({ connectionString: DSN });
      await other.connect();
      try {
        await kv.set("counter", "1");
        await other.query("BEGIN ISOLATION LEVEL REPEATABLE READ");
        const tx = new Pgkv(other);
        expect(await tx.get("counter")).toBe("1"); // takes the snapshot
        await kv.incr("counter"); // committed by someone else meanwhile
        const error = await tx
          .pipeline()
          .set("mine", "1")
          .incr("counter")
          .exec()
          .catch((e: unknown) => e);
        expect(error).not.toBeInstanceOf(ReplyError);
        expect(sqlState(error)).toBe("40001");
        expect(isRetryable(error)).toBe(true);
        // Unlike a Redis error reply, a database error aborts the enclosing transaction.
        await expect(tx.get("counter")).rejects.toMatchObject({ code: "25P02" });
        await other.query("ROLLBACK");
        // Run again from the start, as isRetryable says.
        await other.query("BEGIN ISOLATION LEVEL REPEATABLE READ");
        expect(await tx.pipeline().set("mine", "1").incr("counter").exec()).toEqual([
          [null, "OK"],
          [null, 3],
        ]);
        await other.query("COMMIT");
      } finally {
        await other.end();
      }
      expect(await kv.mget("mine", "counter")).toEqual(["1", "3"]);
    });
  });

  describe("multi on a pool", () => {
    test("applies all the commands and resolves like ioredis", async () => {
      expect(await kv.multi().set("a", "1").incr("a").hset("h", "f", "v").hgetall("h").exec()).toEqual([
        [null, "OK"],
        [null, 2],
        [null, 1],
        [null, { f: "v" }],
      ]);
      expect(await kv.get("a")).toBe("2");
      idle();
    });

    test("is one statement, with no BEGIN or COMMIT around it", async () => {
      const { statements, client } = counting();
      expect(await client.multi().set("a", "1").incr("a").get("a").exec()).toEqual([
        [null, "OK"],
        [null, 2],
        [null, "2"],
      ]);
      expect(statements).toEqual([PIPELINE]);
    });

    test("a command that fails while running has its error in its slot and the others still apply", async () => {
      await kv.set("counter", "10");
      const results = await kv.multi().incr("counter").set("s", "v").lpush("s", "x").set("after", "1").exec();
      expect(results).toEqual([[null, 11], [null, "OK"], [expect.any(ReplyError)], [null, "OK"]]);
      expect(results[2]).toHaveLength(1);
      expect(results[2]?.[0]).toMatchObject({ name: "ReplyError", message: WRONGTYPE });
      // ioredis does not name the command on an error from inside a transaction.
      expect((results[2]?.[0] as ReplyError | undefined)?.command).toBeUndefined();
      expect(await kv.mget("counter", "s", "after")).toEqual(["11", "v", "1"]);
      idle();
    });

    test("a command Redis would refuse to queue discards them all and rejects with EXECABORT", async () => {
      await kv.set("counter", "10");
      for (const refused of [["nosuchcommand", "x"], ["get"], ["set", "only-a-key"]]) {
        const error = await kv
          .multi()
          .incr("counter")
          .set("s", "v")
          .call(refused[0] as string, ...refused.slice(1))
          .set("after", "1")
          .exec()
          .catch((e: unknown) => e);
        expect(error, refused.join(" ")).toBeInstanceOf(ReplyError);
        expect(error).toMatchObject({
          name: "ReplyError",
          message: EXECABORT,
          command: { name: "exec", args: [] },
          previousErrors: [{ name: "ReplyError", command: { name: refused[0], args: refused.slice(1) } }],
        });
        // It is an answer, not an error of the database's.
        expect(sqlState(error)).toBeUndefined();
        expect((error as ReplyError).cause).toBeUndefined();
        expect(await kv.mget("counter", "s", "after")).toEqual(["10", null, null]);
        idle();
      }
    });

    test("commands from an array, as ioredis accepts them", async () => {
      expect(
        await kv
          .multi([
            ["set", "a", "1"],
            ["incrby", "a", 5],
          ])
          .exec(),
      ).toEqual([
        [null, "OK"],
        [null, 6],
      ]);
      expect(await kv.pipeline([["get", "a"]]).exec()).toEqual([[null, "6"]]);
    });

    test("needs nothing from the executor but query()", async () => {
      const bare = new Pgkv({ query: (text, values) => pool.query(text, values) });
      expect(await bare.multi().set("m", "1").incr("m").lpush("m", "x").get("m").exec()).toMatchObject([
        [null, "OK"],
        [null, 2],
        [{ message: WRONGTYPE }],
        [null, "2"],
      ]);
      await expect(bare.multi().set("m", "9").call("get").exec()).rejects.toMatchObject({ message: EXECABORT });
      expect(await kv.get("m")).toBe("2");
      idle();
    });
  });

  describe("inside the caller's transaction (node-postgres)", () => {
    async function inTransaction(fn: (client: pg.PoolClient, tx: Pgkv) => Promise<void>, end = "COMMIT") {
      const client = await pool.connect();
      try {
        await client.query("BEGIN");
        await fn(client, new Pgkv(client));
        await client.query(end);
      } catch (error) {
        await client.query("ROLLBACK");
        throw error;
      } finally {
        client.release();
      }
    }

    test("a failing command rejects and leaves the transaction usable", async () => {
      await inTransaction(async (client, tx) => {
        await tx.set("before", "1");
        await expect(tx.lpush("before", "x")).rejects.toMatchObject({ name: "ReplyError", message: WRONGTYPE });
        await expect(tx.call("nosuchcommand")).rejects.toBeInstanceOf(ReplyError);
        expect(client.getTransactionStatus()).toBe("T");
        expect(await tx.incr("before")).toBe(2);
      });
      expect(await kv.get("before")).toBe("2");
      idle();
    });

    test("a failing pipeline command does not abort the transaction or the rest of the pipeline", async () => {
      await inTransaction(async (client, tx) => {
        await tx.set("before", "1");
        const results = await tx.pipeline().set("s", "v").lpush("s", "x").set("t", "after").exec();
        expect(results).toMatchObject([[null, "OK"], [{ message: WRONGTYPE }], [null, "OK"]]);
        expect((await client.query("SELECT 1 AS alive")).rows).toEqual([{ alive: 1 }]);
        expect(client.getTransactionStatus()).toBe("T");
        await tx.set("later", "1");
      });
      expect(await kv.mget("before", "s", "t", "later")).toEqual(["1", "v", "after", "1"]);
      idle();
    });

    test("a pipeline is rolled back with the transaction", async () => {
      await inTransaction(async (_client, tx) => {
        expect(await tx.pipeline().set("a", "1").set("b", "2").exec()).toEqual([
          [null, "OK"],
          [null, "OK"],
        ]);
      }, "ROLLBACK");
      expect(await kv.exists("a", "b")).toBe(0);
    });

    test("a multi with a failing command applies the others and leaves the transaction usable", async () => {
      await inTransaction(async (client, tx) => {
        await tx.set("before", "1");
        const results = await tx.multi().set("m", "1").incr("before").lpush("m", "x").exec();
        expect(results).toMatchObject([[null, "OK"], [null, 2], [{ message: WRONGTYPE }]]);
        expect(client.getTransactionStatus()).toBe("T");
        expect(await tx.multi().incr("before").exec()).toEqual([[null, 3]]);
      });
      expect(await kv.mget("before", "m")).toEqual(["3", "1"]);
      idle();
    });

    test("a multi neither commits nor outlives the caller's transaction", async () => {
      const other = new pg.Client({ connectionString: DSN });
      await other.connect();
      try {
        await inTransaction(async (_client, tx) => {
          expect(await tx.multi().set("m", "1").exec()).toEqual([[null, "OK"]]);
          expect(await new Pgkv(other).exists("m")).toBe(0);
        }, "ROLLBACK");
      } finally {
        await other.end();
      }
      expect(await kv.exists("m")).toBe(0);
    });

    test("a BEGIN the caller did not await is honoured: nothing is committed behind its back", async () => {
      const client = await pool.connect();
      try {
        // node-postgres queues statements, so code that does not await BEGIN is common.
        void client.query("BEGIN");
        expect(await new Pgkv(client).multi().set("m", "1").exec()).toEqual([[null, "OK"]]);
        expect(await new Pgkv(client).pipeline().set("s", "v").lpush("s", "x").set("p", "1").exec()).toMatchObject([
          [null, "OK"],
          [{ message: WRONGTYPE }],
          [null, "OK"],
        ]);
        await client.query("ROLLBACK");
      } finally {
        client.release();
      }
      expect(await kv.exists("m", "s", "p")).toBe(0);
    });

    test("a discarded multi applies nothing and leaves the transaction it runs in usable", async () => {
      await inTransaction(async (client, tx) => {
        await tx.set("before", "1");
        const error = await tx
          .multi()
          .set("m", "1")
          .call("get")
          .incr("before")
          .call("nosuchcommand")
          .exec()
          .catch((e: unknown) => e);
        expect(error).toMatchObject({
          message: EXECABORT,
          previousErrors: [
            { message: "ERR wrong number of arguments for 'get' command", command: { name: "get", args: [] } },
            { message: "ERR unknown command 'nosuchcommand'", command: { name: "nosuchcommand", args: [] } },
          ],
        });
        expect(client.getTransactionStatus()).toBe("T");
        expect(await tx.mget("before", "m")).toEqual(["1", null]);
        await tx.set("after", "1");
      });
      expect(await kv.mget("before", "m", "after")).toEqual(["1", null, "1"]);
      idle();
    });

    test("in a transaction that has already failed, everything rejects with PostgreSQL's error", async () => {
      await inTransaction(async (client, tx) => {
        await client.query("SELECT 1/0").catch(() => {});
        for (const attempt of [tx.pipeline().set("a", "1").exec(), tx.multi().set("a", "1").exec(), tx.get("a")]) {
          const error = await attempt.catch((e: unknown) => e);
          expect(error).toMatchObject({ code: "25P02" });
          expect(error).not.toBeInstanceOf(ReplyError);
        }
      }, "ROLLBACK");
      idle();
    });
  });

  describe("postgres.js", () => {
    const pjs = new Pgkv(sql);

    test("pipeline and multi on sql", async () => {
      const results = await pjs.pipeline().set("s", "v").lpush("s", "x").set("t", "after").exec();
      expect(results).toMatchObject([[null, "OK"], [{ name: "ReplyError", message: WRONGTYPE }], [null, "OK"]]);
      expect(await pjs.multi().set("m", "1").lpush("s", "x").incr("m").exec()).toMatchObject([
        [null, "OK"],
        [{ name: "ReplyError", message: WRONGTYPE }],
        [null, 2],
      ]);
      const error = await pjs
        .multi()
        .set("m", "9")
        .call("get")
        .exec()
        .catch((e: unknown) => e);
      expect(error).toBeInstanceOf(ReplyError);
      expect(error).toMatchObject({
        message: EXECABORT,
        command: { name: "exec", args: [] },
        previousErrors: [{ message: "ERR wrong number of arguments for 'get' command" }],
      });
      // max is 1: this only answers if no connection is held.
      expect(await pjs.mget("s", "t", "m")).toEqual(["v", "after", "2"]);
    });

    test("inside sql.begin a failing command, pipeline command or multi command does not roll the transaction back", async () => {
      await sql.begin(async (tx) => {
        const inner = new Pgkv(tx);
        await inner.set("before", "1");
        await expect(inner.lpush("before", "x")).rejects.toMatchObject({ name: "ReplyError", message: WRONGTYPE });
        const results = await inner.pipeline().set("s", "v").lpush("s", "x").set("t", "after").exec();
        expect(results).toMatchObject([[null, "OK"], [{ message: WRONGTYPE }], [null, "OK"]]);
        expect(await inner.multi().set("m", "1").lpush("m", "x").incr("before").exec()).toMatchObject([
          [null, "OK"],
          [{ message: WRONGTYPE }],
          [null, 2],
        ]);
        await inner.set("later", "1");
      });
      expect(await pjs.mget("before", "s", "t", "m", "later")).toEqual(["2", "v", "after", "1", "1"]);
    });

    test("a discarded multi inside sql.begin applies nothing and the transaction commits its other work", async () => {
      const caught = await sql.begin(async (tx) => {
        const inner = new Pgkv(tx);
        await inner.set("before", "1");
        const error = await inner
          .multi()
          .set("m", "1")
          .call("get")
          .exec()
          .catch((e: unknown) => e);
        expect(await inner.mget("before", "m")).toEqual(["1", null]);
        await inner.set("after", "1");
        return error;
      });
      expect(caught).toBeInstanceOf(ReplyError);
      expect(caught).toMatchObject({ message: EXECABORT, previousErrors: [{ command: { name: "get", args: [] } }] });
      expect(await pjs.mget("before", "m", "after")).toEqual(["1", null, "1"]);
      // Left uncaught, it is an error like any other thrown in the callback: sql.begin rolls back.
      await expect(
        sql.begin(async (tx) => {
          await new Pgkv(tx).set("uncaught", "1");
          await new Pgkv(tx).multi().set("m", "1").call("get").exec();
        }),
      ).rejects.toMatchObject({ name: "ReplyError", message: EXECABORT });
      expect(await pjs.exists("uncaught", "m")).toBe(0);
    });
  });
});
