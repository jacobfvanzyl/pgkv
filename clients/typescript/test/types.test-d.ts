/**
 * Compile-time checks, run by `pnpm typecheck` (tsc). Nothing here executes.
 *
 * The standard is ioredis's own declaration of `Redis`. For every command pgkv supports,
 * the method on `Pgkv` is compared with the method of the same name on `Redis`.
 */
import type { Redis } from "ioredis";
import type pg from "pg";
import type postgres from "postgres";
import type { NOT_IN_IOREDIS } from "../src/commands.ts";
import type { CommandName, Commands, ExecResult, Pipeline } from "../src/index.ts";
import { Pgkv } from "../src/index.ts";

type Equal<A, B> = (<T>() => T extends A ? 1 : 2) extends <T>() => T extends B ? 1 : 2 ? true : false;
type Expect<T extends true> = T;

/** The commands whose signatures come from ioredis, and `call`. */
type Checked = Exclude<CommandName, (typeof NOT_IN_IOREDIS)[number]> | "call";

// 1. There is a method for every command in the list, and no method that is not in it.
export type MethodsAreTheCommandList = Expect<Equal<keyof Commands, CommandName | "call">>;
export type TheHandWrittenOnesAreMissingFromIoredis = Expect<
  Equal<Extract<(typeof NOT_IN_IOREDIS)[number], keyof Redis>, never>
>;

// 2. Pgkv declares nothing ioredis does not: ioredis's method is assignable to ours, so
//    every overload we have is one ioredis has, with a return type that is not narrower.
//    Checked for every command.
type DeclaresMoreThanIoredis = { [K in Checked]: Redis[K] extends Pgkv[K] ? never : K }[Checked];
export type NothingIoredisLacks = Expect<Equal<DeclaresMoreThanIoredis, never>>;

// 3. The other direction: our method is assignable to ioredis's, so every call ioredis
//    accepts we accept, with a return type that is not wider. This cannot hold for the
//    commands below: they take a variable number of arguments, and for those ioredis
//    declares separate overloads ending in a callback, which pgkv does not have. The list
//    is exact: a command that starts or stops passing fails the build. For these, checks 2
//    and 4 still hold, and `pnpm typecheck` also verifies that commands.generated.ts is what
//    the generator derives from the installed ioredis declarations.
type MissingAnIoredisOverload = { [K in Checked]: Pgkv[K] extends Redis[K] ? never : K }[Checked];
type VariadicWithCallbackOverloads =
  | "call"
  | "del"
  | "exists"
  | "hdel"
  | "hmget"
  | "hmset"
  | "hset"
  | "lmpop"
  | "lpush"
  | "lpushx"
  | "mget"
  | "mset"
  | "msetnx"
  | "rpush"
  | "rpushx"
  | "sadd"
  | "sdiff"
  | "sdiffstore"
  | "sinter"
  | "sintercard"
  | "sinterstore"
  | "smismember"
  | "srem"
  | "sunion"
  | "sunionstore"
  | "touch"
  | "unlink"
  | "zadd"
  | "zdiff"
  | "zdiffstore"
  | "zinter"
  | "zintercard"
  | "zinterstore"
  | "zmpop"
  | "zmscore"
  | "zrem"
  | "zunion"
  | "zunionstore";
export type EveryCallIoredisAccepts = Expect<Equal<MissingAnIoredisOverload, VariadicWithCallbackOverloads>>;

// 4. For every command, the return type of the last overload (in ioredis the promise form
//    without a callback) is identical.
type ReturnsDiffer = {
  [K in Checked]: Equal<ReturnType<Pgkv[K]>, ReturnType<Redis[K]>> extends true ? never : K;
}[Checked];
export type SameReturnTypes = Expect<Equal<ReturnsDiffer, never>>;

// Spot checks of what a caller sees.
declare const kv: Pgkv;
export const checks = async () => {
  const value: string | null = await kv.get("k");
  const ok: "OK" = await kv.set("k", "v");
  const maybe: "OK" | null = await kv.set("k", "v", "EX", 10, "NX");
  const count: number = await kv.del("a", "b");
  const hash: Record<string, string> = await kv.hgetall("h");
  const [cursor, keys]: [string, string[]] = await kv.scan(0, "MATCH", "user:*");
  const any: unknown = await kv.call("anything", "a", 1);
  await kv.mset({ a: "1" });
  await kv.hset("h", new Map([["f", "v"]]));
  await kv.set(new Uint8Array(), Buffer.from("v"));
  const results: ExecResult = await kv.pipeline().set("k", "v").get("k").exec();
  const chained: Pipeline = kv.multi().incr("n").call("get", "n");
  // @ts-expect-error a key is required
  await kv.get();
  // @ts-expect-error there is no callback API
  await kv.get("k", () => {});
  // @ts-expect-error no pub/sub
  await kv.subscribe("channel");
  // @ts-expect-error no blocking commands
  await kv.blpop("list", 0);
  // @ts-expect-error no scripting
  await kv.eval("return 1", 0);
  // @ts-expect-error the client owns no connection to close
  await kv.quit();
  // @ts-expect-error a pipeline's commands return the pipeline, not a promise
  await kv.pipeline().get("k").then;
  return [value, ok, maybe, count, hash, cursor, keys, any, results, chained];
};

// What the constructor accepts, against the drivers' own types.
declare const pool: pg.Pool;
declare const client: pg.Client;
declare const poolClient: pg.PoolClient;
declare const sql: postgres.Sql;
declare const transaction: postgres.TransactionSql;
declare const reserved: postgres.ReservedSql;
export const executors = () => [
  new Pgkv(pool),
  new Pgkv(client),
  new Pgkv(poolClient),
  new Pgkv(sql),
  new Pgkv(transaction),
  new Pgkv(reserved),
  new Pgkv({ query: async (text, values) => ({ rows: await sql.unsafe(text, values) }) }),
  // @ts-expect-error not an executor
  new Pgkv({}),
  // @ts-expect-error the client does not open connections
  new Pgkv("postgres://localhost/db"),
];
