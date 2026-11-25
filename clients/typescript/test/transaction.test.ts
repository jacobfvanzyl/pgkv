import pg from "pg";
import postgres from "postgres";
import { afterAll, beforeAll, describe, expect, test } from "vitest";
import { Pgkv, ReplyError } from "../src/index.ts";
import { DSN } from "./env.ts";

/**
 * What Redis cannot do: a key written through the client is part of the caller's
 * transaction, together with the rows of ordinary tables.
 */
describe.skipIf(!DSN)("key-value writes in the application's transaction", () => {
  const pool = new pg.Pool({ connectionString: DSN, max: 2 });
  const sql = postgres(DSN, { max: 2, onnotice: () => {} });
  const kv = new Pgkv(pool);
  const orderCount = async (id: string) =>
    (await pool.query("SELECT count(*)::int AS n FROM client_ts_orders WHERE id = $1", [id])).rows[0].n as number;

  beforeAll(async () => {
    await pool.query("CREATE TABLE IF NOT EXISTS client_ts_orders (id text PRIMARY KEY, total numeric NOT NULL)");
    await pool.query("TRUNCATE client_ts_orders");
    await kv.flushdb();
  });
  afterAll(async () => {
    await pool.query("DROP TABLE IF EXISTS client_ts_orders");
    await pool.end();
    await sql.end();
  });

  test("node-postgres: roll back and both are gone", async () => {
    const client = await pool.connect();
    try {
      await client.query("BEGIN");
      await client.query("INSERT INTO client_ts_orders VALUES ($1, $2)", ["pg-1", 10]);
      const tx = new Pgkv(client);
      await tx.set("order:pg-1:status", "paid");
      await tx.rpush("outbox", "pg-1");
      expect(await tx.get("order:pg-1:status")).toBe("paid");
      // Another connection sees neither until the transaction commits.
      expect(await kv.get("order:pg-1:status")).toBeNull();
      expect(await orderCount("pg-1")).toBe(0);
      await client.query("ROLLBACK");
    } finally {
      client.release();
    }
    expect(await orderCount("pg-1")).toBe(0);
    expect(await kv.exists("order:pg-1:status", "outbox")).toBe(0);
  });

  test("node-postgres: commit and both are there", async () => {
    const client = await pool.connect();
    try {
      await client.query("BEGIN");
      await client.query("INSERT INTO client_ts_orders VALUES ($1, $2)", ["pg-2", 20]);
      const tx = new Pgkv(client);
      await tx.set("order:pg-2:status", "paid");
      await tx.rpush("outbox", "pg-2");
      await client.query("COMMIT");
    } finally {
      client.release();
    }
    expect(await orderCount("pg-2")).toBe(1);
    expect(await kv.get("order:pg-2:status")).toBe("paid");
    expect(await kv.lrange("outbox", 0, -1)).toEqual(["pg-2"]);
  });

  test("postgres.js: roll back and both are gone", async () => {
    const failure = new Error("payment declined");
    await expect(
      sql.begin(async (tx) => {
        await tx`INSERT INTO client_ts_orders VALUES (${"pjs-1"}, ${10})`;
        await new Pgkv(tx).set("order:pjs-1:status", "paid");
        expect(await new Pgkv(tx).get("order:pjs-1:status")).toBe("paid");
        throw failure;
      }),
    ).rejects.toBe(failure);
    expect(await orderCount("pjs-1")).toBe(0);
    expect(await kv.exists("order:pjs-1:status")).toBe(0);
  });

  test("postgres.js: commit and both are there", async () => {
    await sql.begin(async (tx) => {
      await tx`INSERT INTO client_ts_orders VALUES (${"pjs-2"}, ${20})`;
      await new Pgkv(tx).set("order:pjs-2:status", "paid");
    });
    expect(await orderCount("pjs-2")).toBe(1);
    expect(await kv.get("order:pjs-2:status")).toBe("paid");
  });

  test("node-postgres: a failing command is only a rejected promise; the transaction commits its other work", async () => {
    const client = await pool.connect();
    try {
      await client.query("BEGIN");
      await client.query("INSERT INTO client_ts_orders VALUES ($1, $2)", ["pg-3", 30]);
      const tx = new Pgkv(client);
      await tx.set("order:pg-3:status", "paid");
      await expect(tx.incr("order:pg-3:status")).rejects.toMatchObject({
        name: "ReplyError",
        message: "ERR value is not an integer or out of range",
      });
      // As in Redis, the error was a reply and nothing more: no savepoint, no rollback.
      expect(client.getTransactionStatus()).toBe("T");
      expect(await tx.get("order:pg-3:status")).toBe("paid");
      await tx.rpush("outbox:3", "pg-3");
      expect((await client.query("COMMIT")).command).toBe("COMMIT");
    } finally {
      client.release();
    }
    expect(await orderCount("pg-3")).toBe(1);
    expect(await kv.get("order:pg-3:status")).toBe("paid");
    expect(await kv.lrange("outbox:3", 0, -1)).toEqual(["pg-3"]);
  });

  test("postgres.js: a failing command is only a rejected promise; the transaction commits its other work", async () => {
    await sql.begin(async (tx) => {
      await tx`INSERT INTO client_ts_orders VALUES (${"pjs-3"}, ${30})`;
      const inner = new Pgkv(tx);
      await inner.set("order:pjs-3:status", "paid");
      await expect(inner.incr("order:pjs-3:status")).rejects.toMatchObject({ name: "ReplyError" });
      await inner.rpush("outbox:pjs-3", "pjs-3");
    });
    expect(await orderCount("pjs-3")).toBe(1);
    expect(await kv.get("order:pjs-3:status")).toBe("paid");
    expect(await kv.lrange("outbox:pjs-3", 0, -1)).toEqual(["pjs-3"]);
  });

  test("a database error, unlike a Redis error reply, aborts the transaction, keys included", async () => {
    const client = await pool.connect();
    try {
      await client.query("BEGIN");
      await client.query("INSERT INTO client_ts_orders VALUES ($1, $2)", ["pg-4", 40]);
      const tx = new Pgkv(client);
      await tx.set("order:pg-4:status", "paid");
      await expect(client.query("INSERT INTO client_ts_orders VALUES ($1, $2)", ["pg-4", 41])).rejects.toMatchObject({
        code: "23505",
      });
      // PostgreSQL refuses everything until the transaction ends; that is not a Redis reply.
      const refused = await tx.get("order:pg-4:status").catch((e: unknown) => e);
      expect(refused).toMatchObject({ code: "25P02" });
      expect(refused).not.toBeInstanceOf(ReplyError);
      expect((await client.query("COMMIT")).command).toBe("ROLLBACK");
    } finally {
      client.release();
    }
    expect(await orderCount("pg-4")).toBe(0);
    expect(await kv.exists("order:pg-4:status")).toBe(0);
  });
});
