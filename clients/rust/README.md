# pgkv (Rust client)

Run [redis-rs](https://crates.io/crates/redis) code against PostgreSQL through the
[pgkv](https://github.com/jacobfvanzyl/pgkv) extension.

`pgkv::Connection` implements `redis::aio::ConnectionLike`, so `redis::AsyncCommands`,
`redis::cmd(..).query_async(..)` and `redis::pipe()` work on it as they do on a Redis
connection. Swap the connection object; the rest of the code stays.

The crate is a thin layer. A command is one statement, `SELECT pgkv.try_call(...)`, and a whole
pipeline is one statement, `SELECT pgkv.pipeline(...)`. The JSON reply becomes the `redis::Value`
a Redis server would have sent over RESP2, and an error reply becomes the `RedisError` redis-rs
would have produced. All command behaviour lives in the extension's SQL; the crate has no
per-command code.

Requires the pgkv extension 0.4.0 or later (`CREATE EXTENSION pgkv`).

## Example

```rust,no_run
use redis::AsyncCommands;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let (client, connection) =
        tokio_postgres::connect("postgresql://localhost/app", tokio_postgres::NoTls).await?;
    tokio::spawn(connection);

    let mut con = pgkv::Connection::new(client);
    let _: () = con.set_ex("session:42", "alice", 3600).await?;
    let user: Option<String> = con.get("session:42").await?;
    let visits: i64 = con.hincr("stats", "visits", 1).await?;
    let _: i64 = con.zadd("scores", "alice", 12.5).await?;
    let top: Vec<(String, f64)> = con.zrange_withscores("scores", 0, 9).await?;
    println!("{user:?} {visits} {top:?}");
    Ok(())
}
```

`Connection::new` takes what the commands run on. It does not open, pool or close anything.

| You have | Pass |
| --- | --- |
| `tokio_postgres::Client` | the client, `&mut client`, or `&client` (shared between tasks) |
| `tokio_postgres::Transaction` | the transaction, `&mut transaction` or `&transaction` |
| `deadpool_postgres::Object` | `&mut **object` (it dereferences to `tokio_postgres::Client`) |
| `sqlx::PoolConnection<Postgres>`, `sqlx::Transaction<Postgres>` | `&mut *connection`, `&mut *transaction` (feature `sqlx`) |
| another driver | your implementation of `pgkv::Executor` (one method) |

There are three statements (a command, a pipeline, an atomic pipeline). Each is prepared on
first use and the connection keeps it for later (sqlx caches it itself). `get_ref()`, `get_mut()` and `into_inner()` give the executor back.

## Key-value writes in your own transaction

What PostgreSQL adds over Redis is that a key and a row can be written in one transaction:

```rust,no_run
# use redis::AsyncCommands;
# async fn example(client: &mut tokio_postgres::Client) -> Result<(), Box<dyn std::error::Error>> {
let transaction = client.transaction().await?;
let mut con = pgkv::Connection::new(transaction);

con.get_ref()
    .execute("INSERT INTO orders (id, total) VALUES ($1, $2)", &[&"o-1", &100_i32])
    .await?;
let _: () = con.set("order:o-1:status", "pending").await?;
let _: i64 = con.rpush("orders:queue", "o-1").await?;

// Both the row and the keys become visible here, or neither does.
con.into_inner().commit().await?;
# Ok(())
# }
```

A Redis error reply does not abort the transaction. `try_call` returns it instead of raising it,
and undoes only what that command did:

```rust,no_run
# use redis::AsyncCommands;
# async fn example(mut con: pgkv::Connection<tokio_postgres::Transaction<'_>>) -> Result<(), Box<dyn std::error::Error>> {
let _: () = con.set("name", "alice").await?;
let err = con.lpush::<_, _, i64>("name", "x").await.unwrap_err();
assert_eq!(err.code(), Some("WRONGTYPE"));
// The transaction is still usable and commits everything else it did.
let _: () = con.set("other", "value").await?;
con.into_inner().commit().await?;
# Ok(())
# }
```

A database error is different: a deadlock, a serialization failure, a failed constraint in your
own SQL, or a lost connection aborts the enclosing transaction as it always does in PostgreSQL,
and every later statement fails with SQLSTATE 25P02 until you roll back.

## Pipelines

`redis::pipe()` works, and a pipeline is one statement: one round trip however many commands it
holds, and other sessions see all of its effects or none of them.

A plain pipeline behaves as on Redis. A command that fails with a Redis error takes its slot,
undoes only itself, and the commands after it still run. `query_async` then returns the pipeline
error redis-rs builds from the failed slots, or every command's own result with
`.ignore_errors()`.

An atomic pipeline (`.atomic()`, MULTI/EXEC on Redis) behaves as on Redis too:

- A command that fails while the transaction runs does not undo the others. They are applied,
  and the error names the failed command's index, exactly as redis-rs reports it for Redis.
- A command Redis would refuse to queue (an unknown command, fewer arguments than the command
  takes) discards the transaction: nothing is applied, with or without `.ignore_errors()`. The
  error has the kind and code it has on Redis, `ErrorKind::Server(ServerErrorKind::ExecAbort)`
  and `EXECABORT`, and its detail names each refused command by its index in the pipeline.

```rust,no_run
# async fn example(mut con: pgkv::Connection<tokio_postgres::Client>) {
let err = redis::pipe()
    .atomic()
    .set("a", 1)
    .cmd("NOSUCHCOMMAND")
    .exec_async(&mut con)
    .await
    .unwrap_err();
assert_eq!(err.code(), Some("EXECABORT"));
// Transaction discarded because of previous errors. Refused: command 1: ERR unknown command ...
println!("{}", err.detail().unwrap());
# }
```

Inside your own PostgreSQL transaction a pipeline is still one statement of that transaction: its
effects become visible to other sessions when you commit. A discarded atomic pipeline applies
nothing and leaves the transaction usable, like any other Redis error. A database error in any
command fails the whole pipeline, undoes all of it and aborts the enclosing transaction.

Any executor runs atomic pipelines, a shared `&Client` included. `MULTI`, `EXEC` and `WATCH` sent
as individual commands are unknown commands; use `.atomic()` or a PostgreSQL transaction.

## Errors

- A Redis error reply (`ERR ...`, `WRONGTYPE ...`) is the `RedisError` redis-rs builds for the
  same line from a Redis server: it is produced by redis-rs's own parser, so `kind()`, `code()`
  and `detail()` are the same. It is returned by the statement, not raised, so it leaves an
  enclosing PostgreSQL transaction usable.
- A discarded atomic pipeline is the server's `EXECABORT` reply as a `RedisError`, with the
  refused commands appended to its detail:
  `Transaction discarded because of previous errors. Refused: command 1: ERR unknown command 'NOSUCHCOMMAND'`
  (several are separated by `; `). Like any Redis error reply it leaves an enclosing PostgreSQL
  transaction usable.
- A database error is a `RedisError` of kind `ErrorKind::Io` wrapping a `pgkv::DbError`. It
  aborts the enclosing PostgreSQL transaction.
  `pgkv::DbError::from_redis_error(&err)` returns it, with `sqlstate()`, `message()` and
  `is_retryable()` (true for serialization failures, 40001, and deadlocks, 40P01). A closed
  connection reports `is_connection_dropped()`.
- An argument that is not valid UTF-8 or contains a NUL byte is rejected before anything is sent,
  with kind `ErrorKind::Client`. In a pipeline, no command runs.

## Differences from a Redis connection

`tests/parity.rs` runs the same code against a Redis server and against pgkv and compares
everything a caller can observe: values, errors, and the keys left behind, for single commands,
plain pipelines, atomic pipelines (those Redis discards included) and `.ignore_errors()`. What
remains different:

- **The error of a discarded atomic pipeline has another text.** Kind and code are the same
  (`ExecAbort`, `EXECABORT`). On Redis, redis-rs prints it as
  `Transaction aborted: [(Index 1, error: ...)]`, with no detail and with the refused commands
  in `into_server_errors()`. redis-rs has no public way to build that error, so here the refused
  commands are in `detail()` (see Errors) and `into_server_errors()` holds the one `EXECABORT`
  error.

- **Values are text.** Keys, values, fields and members must be valid UTF-8 without NUL bytes.
  Binary values are rejected, not converted.
- **No pub/sub, no blocking commands, no scripts.** `SUBSCRIBE`, `BLPOP`, `EVAL`, `CLIENT`,
  streams and the rest of what pgkv does not implement return
  `ERR unknown command`. `SELECT pgkv.commands()` lists what it has. `SELECT` only accepts
  database 0 and `get_db()` returns 0: there is one keyspace per PostgreSQL database.
- **RESP2 only.** Replies have the RESP2 shape (for example `HGETALL` is a flat array). The typed
  conversions of redis-rs handle both shapes, so this only shows when you ask for a raw `Value`.
- **Database errors exist.** A deadlock, a serialization failure or a lost connection is not a
  Redis error reply. It fails the command or the whole pipeline, undoes all of it, and aborts the
  enclosing PostgreSQL transaction (see Errors). No Redis error does that.
- **A pipeline inside your transaction is not yet visible to others.** On Redis EXEC makes the
  effects visible at once; here they become visible when the enclosing transaction commits.
- Sets, hash fields and `KEYS` come back in byte order where Redis returns its internal order.
  A few commands differ in documented corner cases (see `conformance/expected.json` in the
  repository).

## Versions

- redis-rs 1.x (`redis = "1"`), tested with 1.8.3 and 1.0.0. The `redis` dependency is built with its
  `aio` and `tokio-comp` features.
- tokio-postgres 0.7 (default feature `tokio-postgres`), tested with 0.7.18.
- sqlx 0.9 (optional feature `sqlx`), tested with 0.9.1.
- Minimum Rust version: 1.88, the minimum of redis 1.8. The `sqlx` feature needs what sqlx needs
  (1.94 for sqlx 0.9.1).

## Tests

The unit tests need nothing. The integration tests need a PostgreSQL database with pgkv installed
that they may flush, and a Redis server to compare against; they skip when the variables are
unset.

```sh
PGKV_TEST_DSN=postgresql://postgres:postgres@127.0.0.1:5432/pgkv_test \
PGKV_TEST_REDIS_URL=redis://127.0.0.1:6379/5 \
cargo test --all-features
```

`tests/conformance.rs` replays `conformance/scenarios.jsonl` from the repository (it reads
`../../conformance`) through the same code path against Redis and against pgkv and compares the
`redis::Value`s, once command by command and once with each scenario as a single atomic pipeline;
every scenario matches in both. It runs `FLUSHDB` on the Redis database the URL selects and on
the PostgreSQL database.

The other checks need no services:

```sh
cargo fmt --check
cargo clippy --all-targets --all-features -- -D warnings
cargo clippy --no-default-features -- -D warnings
RUSTDOCFLAGS="-D warnings" cargo doc --no-deps --all-features
```

## License

MIT
