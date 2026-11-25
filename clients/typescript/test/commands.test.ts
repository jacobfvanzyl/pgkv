import pg from "pg";
import { afterAll, describe, expect, test } from "vitest";
import { COMMANDS, Pgkv } from "../src/index.ts";
import { DSN } from "./env.ts";

describe.skipIf(!DSN)("the command list", () => {
  const pool = new pg.Pool({ connectionString: DSN, max: 1 });
  afterAll(() => pool.end());

  test("is exactly what SELECT pgkv.commands() returns", async () => {
    const { rows } = await pool.query<{ name: string }>("SELECT lower(c) AS name FROM pgkv.commands() AS c");
    expect([...COMMANDS].sort()).toEqual(rows.map((row) => row.name).sort());
  });

  test("has no duplicates and is lowercase", () => {
    expect(new Set(COMMANDS).size).toBe(COMMANDS.length);
    for (const name of COMMANDS) expect(name).toBe(name.toLowerCase());
  });

  test("every listed command reaches the dispatcher, and an unlisted one is an unknown command", async () => {
    const kv = new Pgkv(pool);
    for (const name of COMMANDS) {
      // With no arguments most commands fail on arity; what matters is that none is unknown.
      const outcome = await kv.call(name).then(
        () => "ok",
        (error: Error) => error.message,
      );
      expect(outcome, name).not.toMatch(/unknown command/);
    }
    await expect(kv.call("subscribe", "ch")).rejects.toMatchObject({
      name: "ReplyError",
      message: "ERR unknown command 'subscribe', with args beginning with: 'ch' ",
    });
    await kv.flushdb();
  });
});
