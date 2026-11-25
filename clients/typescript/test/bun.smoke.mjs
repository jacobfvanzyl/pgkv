// Not part of `pnpm test`: run with `pnpm build && bun test/bun.smoke.mjs` to check the
// built package on Bun's own SQL client, which has the shape of postgres.js.
import { SQL } from "bun";
import { Pgkv, ReplyError } from "../dist/index.js";

const dsn = process.env.PGKV_TEST_DSN;
if (!dsn) {
  console.log("PGKV_TEST_DSN is not set: skipped");
  process.exit(0);
}
const sql = new SQL(dsn, { max: 2 });
const kv = new Pgkv(sql);
const same = (actual, expected, what) => {
  if (JSON.stringify(actual) !== JSON.stringify(expected)) {
    throw new Error(`${what}: expected ${JSON.stringify(expected)}, got ${JSON.stringify(actual)}`);
  }
};
const messages = (results) => results.map(([error, value]) => (error ? error.name : value));

await kv.del("bun:a", "bun:h", "bun:s", "bun:m", "bun:kept");
same(await kv.set("bun:a", "1"), "OK", "set");
same(await kv.incr("bun:a"), 2, "incr");
same([await kv.hset("bun:h", { f: "v" }), await kv.hgetall("bun:h")], [1, { f: "v" }], "hash");
const wrongType = await kv.lpush("bun:a", "x").catch((error) => error);
same([wrongType instanceof ReplyError, wrongType.message.split(" ")[0]], [true, "WRONGTYPE"], "error");
same(
  messages(await kv.pipeline().set("bun:s", "v").lpush("bun:s", "x").get("bun:s").exec()),
  ["OK", "ReplyError", "v"],
  "pipeline",
);
same(
  messages(await kv.multi().set("bun:m", "1").lpush("bun:m", "x").incr("bun:m").exec()),
  ["OK", "ReplyError", 2],
  "multi with a command that fails while running",
);
const aborted = await kv
  .multi()
  .set("bun:m", "9")
  .call("get")
  .exec()
  .catch((error) => error);
same(
  [
    aborted instanceof ReplyError,
    aborted.message.split(" ")[0],
    aborted.command,
    aborted.previousErrors.map((previous) => [previous.message, previous.command]),
    await kv.get("bun:m"),
  ],
  [
    true,
    "EXECABORT",
    { name: "exec", args: [] },
    [["ERR wrong number of arguments for 'get' command", { name: "get", args: [] }]],
    "2",
  ],
  "multi discarded",
);
await sql.begin(async (tx) => {
  const inner = new Pgkv(tx);
  const failed = await inner.lpush("bun:a", "x").catch((error) => error);
  same(failed instanceof ReplyError, true, "error in a transaction");
  same(
    messages(await inner.pipeline().set("bun:s", "w").lpush("bun:s", "x").get("bun:s").exec()),
    ["OK", "ReplyError", "w"],
    "pipeline in a transaction",
  );
  same(
    messages(await inner.multi().incr("bun:m").lpush("bun:m", "x").exec()),
    [3, "ReplyError"],
    "multi in a transaction",
  );
  const discarded = await inner
    .multi()
    .set("bun:m", "9")
    .call("nosuchcommand")
    .exec()
    .catch((error) => error);
  same(
    discarded.previousErrors?.[0]?.message,
    "ERR unknown command 'nosuchcommand'",
    "multi discarded in a transaction",
  );
  await inner.set("bun:kept", "1");
});
same(await kv.mget("bun:s", "bun:m", "bun:kept"), ["w", "3", "1"], "after the transaction");
await kv.del("bun:a", "bun:h", "bun:s", "bun:m", "bun:kept");
await sql.close();
console.log(`ok: Bun ${Bun.version} SQL`);
