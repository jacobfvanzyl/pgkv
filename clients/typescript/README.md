# pgkv for TypeScript

A client for [pgkv](../../README.md), the PostgreSQL extension that implements Redis
commands in SQL. It has the API of [ioredis](https://github.com/redis/ioredis): code
written against ioredis runs against PostgreSQL when the client is constructed differently.

```ts
// before
import { Redis } from "ioredis";
const redis = new Redis(process.env.REDIS_URL);

// after
import { Pgkv } from "pgkv";
const redis = new Pgkv(pool); // the pg Pool or postgres.js sql the application already has
```

The package is a thin layer. Everything it does is one SQL statement,

```sql
SELECT pgkv.pipeline($1::text::jsonb, $2::text::boolean)::text AS reply
```

where `$1` is a JSON array of commands and `$2` says whether they are a transaction. A
single command is a pipeline of one. The function holds all the behaviour; the client turns
method calls into the words of the commands, shapes the replies the way ioredis does, and
turns error replies into `ReplyError`s. It never opens a transaction, takes a connection
out of a pool or sends a second statement. It has no runtime dependencies and uses no
Node.js APIs, so it runs wherever a PostgreSQL driver does (Node.js 20+, Bun, Deno, edge
runtimes).

It needs the pgkv extension, version 0.4.0 or later, in the database. The role the connection uses must own the
extension or have been given access with `SELECT pgkv.grant_access('<role>')`.

## Use

The client does not open connections. Give it what the application already uses.

With [node-postgres](https://node-postgres.com) (a `Pool`, a `Client` or a `PoolClient`):

```ts
import pg from "pg";
import { Pgkv } from "pgkv";

const pool = new pg.Pool({ connectionString: process.env.DATABASE_URL });
const kv = new Pgkv(pool);

await kv.set("session:42", "alice", "EX", 3600); // "OK"
await kv.get("session:42"); // "alice"
await kv.hset("user:1", { name: "Ada", born: 1815 }); // 2
await kv.hgetall("user:1"); // { name: "Ada", born: "1815" }
await kv.zadd("scores", 10, "a", 20, "b");
await kv.zrange("scores", 0, -1, "WITHSCORES"); // ["a", "10", "b", "20"]
```

With [postgres.js](https://github.com/porsager/postgres) (`sql`, a transaction from
`sql.begin`, or a reserved connection):

```ts
import postgres from "postgres";
import { Pgkv } from "pgkv";

const sql = postgres(process.env.DATABASE_URL);
const kv = new Pgkv(sql);

await kv.incr("visits"); // 1
```

Bun's built-in `SQL` has the same shape as postgres.js and is accepted the same way.

With anything else, pass an object with one method, `query(text, values)`, that runs the
statement with `values` as the parameters `$1, $2, ...` and resolves to `{ rows }`, where
each row is an object keyed by column name or an array of columns. Only strings cross this
boundary, in both directions. For Drizzle on node-postgres, for example:

```ts
const kv = new Pgkv({ query: (text, values) => db.$client.query(text, values) });
```

and for anything that can run raw SQL with parameters and return rows:

```ts
const kv = new Pgkv({ query: async (text, values) => ({ rows: await runRaw(text, values) }) });
```

The object can be a pool, a single connection or a transaction: the client only ever asks
it to run that one statement, so it makes no difference, and `pipeline()` and `multi()`
work on all of them.

The client holds no state. Constructing one costs nothing, so make one wherever you have a
connection or transaction to run on.

### Commands

There is a method for each of the commands pgkv supports (strings, keys and expiry,
hashes, lists, sets, sorted sets), with the names, arguments and return types ioredis
gives them; `kv.getBuiltinCommands()` lists them. `kv.call("set", "k", "v")` runs any
command by name.

Arguments are strings or numbers, as in ioredis. A `Buffer` or `Uint8Array` is accepted if
it is valid UTF-8 and rejected with a `TypeError` otherwise: pgkv stores text. Replies are
what ioredis returns: status replies are strings (`"OK"`), integers are numbers, nil is
`null`, `hgetall` returns an object, and `mset`, `msetnx`, `hset` and `hmset` accept an
object or a `Map`.

### Errors

A Redis error reply rejects with a `ReplyError`, exported by this package, whose `name`
(`"ReplyError"`) and `message` (`"WRONGTYPE Operation against a key holding the wrong kind
of value"`, `"ERR value is not an integer or out of range"`, ...) are the ones ioredis
produces for the same error, and whose `command` is `{ name, args }` as ioredis attaches it.

An error reply is an answer, not a database error: the statement that carried it
succeeded. It undoes what the failing command did and nothing else, and a transaction it
happens in carries on.

Any other database error (a lost connection, a serialization failure, a deadlock, a lock
timeout) is not a Redis reply. The promise rejects with the error exactly as the driver
threw it, the database has undone the whole statement, and, as with any failed statement
in PostgreSQL, an enclosing transaction is aborted. `isRetryable(error)` is true for the
two a transaction should be retried on, SQLSTATE `40001` and `40P01`, and
`sqlState(error)` returns the SQLSTATE of any driver's error.

## Transactions with the rest of your data

This is what Redis cannot do. Pass the client the transaction your application is already
in, and the keys it writes commit or roll back together with your rows.

```ts
// node-postgres
const client = await pool.connect();
try {
  await client.query("BEGIN");
  await client.query("INSERT INTO orders (id, total) VALUES ($1, $2)", [id, total]);
  const kv = new Pgkv(client);
  await kv.set(`order:${id}:status`, "paid");
  await kv.rpush("outbox", id);
  await client.query("COMMIT"); // the row, the key and the list entry appear together
} catch (error) {
  await client.query("ROLLBACK"); // or none of them does
  throw error;
} finally {
  client.release();
}
```

```ts
// postgres.js
await sql.begin(async (tx) => {
  await tx`INSERT INTO orders (id, total) VALUES (${id}, ${total})`;
  const kv = new Pgkv(tx);
  await kv.set(`order:${id}:status`, "paid");
  await kv.rpush("outbox", id);
});
```

A command that fails with a Redis error does not disturb the transaction, exactly as in
Redis: the promise rejects with a `ReplyError`, the command has changed nothing, and the
transaction goes on and can commit its other work.

```ts
await client.query("BEGIN");
await kv.set("order:1:status", "paid");
await kv.incr("order:1:status").catch(() => {}); // ReplyError: ERR value is not an integer or out of range
await kv.rpush("outbox", "1");
await client.query("COMMIT"); // the key and the list entry are there
```

A database error is different, here as everywhere in PostgreSQL: after a deadlock, a
serialization failure or a failed statement of your own, the transaction is aborted and
has to be rolled back.

## pipeline() and multi()

Both queue commands and send them with `exec()`, which resolves to one `[error, result]`
pair per command, as in ioredis.

```ts
const results = await kv.pipeline().set("a", "1").incr("a").get("a").exec();
// [[null, "OK"], [null, 2], [null, "2"]]
```

**A pipeline is one statement and one round trip**, however many commands it holds, on
whatever executor the client was given. Because it is one statement, other sessions see
all of its commands' effects or none of them, which is more than Redis promises for a
pipeline and the same as it promises for `MULTI`.

**`pipeline()`** runs the commands in order, each on its own. A command that fails has its
`ReplyError` alone in its pair (`[error]`, as ioredis shapes it), has changed nothing
itself, and the commands around it still apply.

**`multi()`** is Redis `MULTI`/`EXEC`, with the same two kinds of failure:

- A command that fails while it runs (`WRONGTYPE`, not an integer, ...) has its error in
  its pair; the others still apply and `exec()` resolves.
- A command Redis would refuse to queue (an unknown command, or a number of arguments its
  command table rules out) discards the transaction: nothing is applied and `exec()`
  rejects with the `ReplyError` `"EXECABORT Transaction discarded because of previous
  errors."`. Its `previousErrors` are the errors of the refused commands, each with its
  `command`, as in ioredis. The database raises nothing for it, so inside your own
  transaction you can catch it and carry on.

```ts
await kv.multi().decrby("stock:42", 1).rpush("orders", "42").exec();
```

Neither needs a connection of its own, `BEGIN` or a savepoint. Inside your transaction
they are part of it and commit or roll back with it. There is no `WATCH`; use `SET ... NX`,
`SET ... IFEQ` or your own transaction instead.

A database error that is not a Redis reply rejects `exec()` of either kind with the
driver's error, unchanged, so `isRetryable(error)` applies to it. The database has undone
the whole statement: every command failed, including those queued before the one that hit
the error, and nothing needs cleaning up before trying again. (ioredis in the comparable
case, a lost connection, may have applied some commands and not others.)

## What is not there

- No pub/sub, no blocking commands (`BLPOP`, ...), no Lua scripts, no streams, no cluster.
  pgkv does not have them, so the client has no such methods; calling one through `call()`
  rejects with `ERR unknown command`.
- No `connect`, `quit`, `disconnect` or `duplicate`, and no events: the client owns no
  connection. Close the pool you gave it.
- No callbacks and no `...Buffer` methods (`getBuffer`). Await the promise; values are text.
- Values are UTF-8 text without NUL characters, not arbitrary bytes. `SETRANGE` past the
  end of a string and byte ranges that split a multi-byte character are errors where Redis
  would produce raw bytes.
- Integer replies beyond 2^53 are not exact, as in ioredis. (The two round differently:
  this client returns the nearest number, ioredis one a few thousand away.)
- A database error (deadlock, serialization failure, lost connection) rejects the whole
  `exec()` and, inside a transaction, aborts it. Redis has no such errors.
- A `ReplyError` is this package's class, not ioredis's: `name`, `message`, `command` and
  `String(error)` are the same, `instanceof` against ioredis's class is not.
- `multi().length` counts the queued commands; ioredis also counts `MULTI` itself. A
  pipeline cannot be added to after `exec()`.
- `TIME` returns strings, as it does from ioredis at run time, although the type inherited
  from ioredis says `number[]`.

## Development

```sh
pnpm install
pnpm test        # generated types up to date, tsc (including the type tests), then vitest
pnpm lint
pnpm build       # dist/: ES modules and type declarations
```

The tests that need a database read two environment variables and are skipped, with a
notice, when they are not set. Both databases are flushed (`FLUSHDB`), so give the tests
their own.

```sh
export PGKV_TEST_DSN=postgres://postgres:postgres@127.0.0.1:5432/pgkv_test   # has CREATE EXTENSION pgkv
export PGKV_TEST_REDIS_URL=redis://127.0.0.1:6379/6                          # a real Redis to compare with
```

- `test/differential.test.ts` replays [`conformance/scenarios.jsonl`](../../conformance/scenarios.jsonl)
  through ioredis against Redis and through this client against pgkv, calling the same
  method with the same arguments on both, and compares every reply and every error:
  command by command, then each scenario as one `pipeline()` and as one `multi()`. It
  also compares every way a command can fail (alone, in a pipeline, in a transaction, by
  discarding one, `previousErrors` included) in full, with the keys left behind.
- `test/commands.test.ts` checks that the command list in `src/commands.ts` is what
  `SELECT pgkv.commands()` returns.
- `test/types.test-d.ts` compares the method types with ioredis's `Redis` type.

`src/commands.generated.ts` holds the method signatures. `pnpm generate` derives it from
the ioredis declarations installed as a dev dependency, for the commands in
`src/commands.ts`; `pnpm typecheck` fails if it is out of date.

## License

MIT. The generated method signatures are derived from ioredis, also MIT; see `LICENSE`.
