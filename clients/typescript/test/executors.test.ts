import pg from "pg";
import postgres from "postgres";
import { afterAll, beforeAll, describe, expect, test } from "vitest";
import { Pgkv, ReplyError } from "../src/index.ts";
import { DSN } from "./env.ts";

/** The same use of the client, whatever it runs on. `p` keeps the keys of each run apart. */
async function smoke(kv: Pgkv, p: string): Promise<void> {
  expect(await kv.set(`${p}:s`, "v", "EX", 60)).toBe("OK");
  expect(await kv.get(`${p}:s`)).toBe("v");
  expect(await kv.get(`${p}:none`)).toBeNull();
  expect(await kv.ttl(`${p}:s`)).toBeGreaterThan(50);
  expect(await kv.incrby(`${p}:n`, 41)).toBe(41);
  expect(await kv.incr(`${p}:n`)).toBe(42);
  expect(await kv.mset({ [`${p}:a`]: "1", [`${p}:b`]: "2" })).toBe("OK");
  expect(await kv.mget(`${p}:a`, `${p}:none`, `${p}:b`)).toEqual(["1", null, "2"]);
  expect(await kv.hset(`${p}:h`, { name: "Ada", born: 1815 })).toBe(2);
  expect(await kv.hgetall(`${p}:h`)).toEqual({ name: "Ada", born: "1815" });
  expect(await kv.hgetall(`${p}:none`)).toEqual({});
  expect(await kv.rpush(`${p}:l`, "a", "b", "c")).toBe(3);
  expect(await kv.lrange(`${p}:l`, 0, -1)).toEqual(["a", "b", "c"]);
  expect(await kv.sadd(`${p}:set`, ["x", "y"])).toBe(2);
  expect((await kv.smembers(`${p}:set`)).sort()).toEqual(["x", "y"]);
  expect(await kv.zadd(`${p}:z`, 1.5, "a", 2, "b")).toBe(2);
  expect(await kv.zrange(`${p}:z`, 0, -1, "WITHSCORES")).toEqual(["a", "1.5", "b", "2"]);
  expect(await kv.zscore(`${p}:z`, "a")).toBe("1.5");
  expect(await kv.type(`${p}:z`)).toBe("zset");
  expect(await kv.ping()).toBe("PONG");
  expect(await kv.echo("héllo ✓ 😀")).toBe("héllo ✓ 😀");
  expect(await kv.set(`${p}:bytes`, Buffer.from("from a buffer"))).toBe("OK");
  expect(await kv.get(`${p}:bytes`)).toBe("from a buffer");
  expect(await kv.call("strlen", `${p}:s`)).toBe(1);
  const [cursor, keys] = await kv.scan(0, "MATCH", `${p}:*`, "COUNT", 1000);
  expect(cursor).toBe("0");
  expect(keys).toHaveLength(9);

  expect(await kv.pipeline().set(`${p}:p`, "1").incr(`${p}:p`).hgetall(`${p}:h`).exec()).toEqual([
    [null, "OK"],
    [null, 2],
    [null, { name: "Ada", born: "1815" }],
  ]);
  expect(await kv.multi().incr(`${p}:p`).get(`${p}:p`).exec()).toEqual([
    [null, 3],
    [null, "3"],
  ]);

  expect(await kv.del(keys)).toBe(9);
  expect(await kv.del(`${p}:p`)).toBe(1);
  expect(await kv.exists(`${p}:s`, `${p}:h`)).toBe(0);
}

/** A Redis error reply is a ReplyError. It needs nothing but one statement. */
async function errors(kv: Pgkv, p: string): Promise<void> {
  await kv.set(`${p}:s`, "v");
  const wrongType = await kv.lpush(`${p}:s`, "x").catch((e: unknown) => e);
  expect(wrongType).toBeInstanceOf(ReplyError);
  expect(wrongType).toMatchObject({
    name: "ReplyError",
    message: "WRONGTYPE Operation against a key holding the wrong kind of value",
    command: { name: "lpush", args: [`${p}:s`, "x"] },
  });
  expect((wrongType as ReplyError).cause).toBeUndefined();
  await expect(kv.incr(`${p}:s`)).rejects.toMatchObject({
    name: "ReplyError",
    message: "ERR value is not an integer or out of range",
  });
  await expect(kv.call("get")).rejects.toThrow("ERR wrong number of arguments for 'get' command");
  const results = await kv.multi().incr(`${p}:m`).lpush(`${p}:s`, "x").incr(`${p}:m`).exec();
  expect(results).toMatchObject([[null, 1], [{ name: "ReplyError" }], [null, 2]]);
  expect(results[1]).toHaveLength(1);
  expect(await kv.del(`${p}:s`, `${p}:m`)).toBe(2);
}

/** A discarded multi is an answer too: nothing applied, nothing raised, the connection as it was. */
async function discarded(kv: Pgkv, p: string): Promise<void> {
  const error = await kv
    .multi()
    .set(`${p}:d`, "1")
    .call("get")
    .exec()
    .catch((e: unknown) => e);
  expect(error).toBeInstanceOf(ReplyError);
  expect(error).toMatchObject({
    message: "EXECABORT Transaction discarded because of previous errors.",
    command: { name: "exec", args: [] },
    previousErrors: [
      { message: "ERR wrong number of arguments for 'get' command", command: { name: "get", args: [] } },
    ],
  });
  expect((error as ReplyError).cause).toBeUndefined();
  expect(await kv.exists(`${p}:d`)).toBe(0);
}

describe.skipIf(!DSN)("executors", () => {
  const pool = new pg.Pool({ connectionString: DSN, max: 2 });
  const sql = postgres(DSN, { max: 2, onnotice: () => {} });
  beforeAll(async () => void (await new Pgkv(pool).flushdb()));
  afterAll(async () => {
    await pool.end();
    await sql.end();
  });

  test("node-postgres Pool", async () => {
    await smoke(new Pgkv(pool), "pool");
    await errors(new Pgkv(pool), "pool");
    await discarded(new Pgkv(pool), "pool");
    expect(pool.idleCount).toBe(pool.totalCount);
  });

  test("node-postgres Client", async () => {
    const client = new pg.Client({ connectionString: DSN });
    await client.connect();
    try {
      await smoke(new Pgkv(client), "client");
      await errors(new Pgkv(client), "client");
      await discarded(new Pgkv(client), "client");
    } finally {
      await client.end();
    }
  });

  test("node-postgres PoolClient inside BEGIN / COMMIT", async () => {
    const client = await pool.connect();
    try {
      await client.query("BEGIN");
      await smoke(new Pgkv(client), "pooltx");
      await errors(new Pgkv(client), "pooltx");
      await new Pgkv(client).set("pooltx:kept", "yes");
      expect(client.getTransactionStatus()).toBe("T");
      await client.query("COMMIT");
    } finally {
      client.release();
    }
    expect(await new Pgkv(pool).getdel("pooltx:kept")).toBe("yes");
  });

  test("node-postgres PoolClient outside a transaction", async () => {
    const client = await pool.connect();
    try {
      await smoke(new Pgkv(client), "poolclient");
      await errors(new Pgkv(client), "poolclient");
      await discarded(new Pgkv(client), "poolclient");
    } finally {
      client.release();
    }
  });

  test("postgres.js sql", async () => {
    await smoke(new Pgkv(sql), "pjs");
    await errors(new Pgkv(sql), "pjs");
    await discarded(new Pgkv(sql), "pjs");
  });

  test("postgres.js inside sql.begin", async () => {
    await sql.begin(async (tx) => {
      await smoke(new Pgkv(tx), "pjstx");
      await errors(new Pgkv(tx), "pjstx");
      await new Pgkv(tx).set("pjstx:kept", "yes");
    });
    expect(await new Pgkv(sql).getdel("pjstx:kept")).toBe("yes");
  });

  test("postgres.js reserved connection, with and without a hand-written BEGIN", async () => {
    const reserved = await sql.reserve();
    try {
      await smoke(new Pgkv(reserved), "pjsres");
      await errors(new Pgkv(reserved), "pjsres");
      await discarded(new Pgkv(reserved), "pjsres");
      await reserved.unsafe("BEGIN");
      await smoke(new Pgkv(reserved), "pjsresx");
      await errors(new Pgkv(reserved), "pjsresx");
      await new Pgkv(reserved).set("pjsres:gone", "yes");
      await reserved.unsafe("ROLLBACK");
    } finally {
      reserved.release();
    }
    expect(await new Pgkv(sql).exists("pjsres:gone")).toBe(0);
  });

  test("an object with only query(), over a pool", async () => {
    const kv = new Pgkv({ query: (text, values) => pool.query(text, values) });
    await smoke(kv, "generic");
    await errors(kv, "generic");
    await discarded(kv, "generic");
    expect(pool.idleCount).toBe(pool.totalCount);
  });

  test("an object with only query(), over a transaction", async () => {
    const client = await pool.connect();
    try {
      await client.query("BEGIN");
      await smoke(new Pgkv({ query: (text, values) => client.query(text, values) }), "generictx");
      await errors(new Pgkv({ query: (text, values) => client.query(text, values) }), "generictx");
      await client.query("COMMIT");
    } finally {
      client.release();
    }
  });

  test("the two-line adapter from the README, over postgres.js", async () => {
    const kv = new Pgkv({ query: async (text, values) => ({ rows: await sql.unsafe(text, values) }) });
    await smoke(kv, "adapter");
    await errors(kv, "adapter");
    await discarded(kv, "adapter");
  });
});
