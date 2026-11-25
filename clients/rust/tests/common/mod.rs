//! Shared setup for the integration tests.
//!
//! `PGKV_TEST_DSN` is a PostgreSQL URL for a database with the pgkv extension installed that the
//! tests may fill and flush. `PGKV_TEST_REDIS_URL` is a Redis server used as the oracle; the
//! tests run FLUSHDB on the database the URL selects.
#![allow(dead_code)]

use redis::AsyncCommands;

fn env(name: &str) -> Option<String> {
    match std::env::var(name) {
        Ok(value) if !value.is_empty() => Some(value),
        _ => {
            eprintln!("skipping: {name} is not set");
            None
        }
    }
}

/// A new PostgreSQL session, or `None` (after saying so) when `PGKV_TEST_DSN` is unset.
pub async fn pg() -> Option<tokio_postgres::Client> {
    Some(pg_with_task().await?.0)
}

/// A new PostgreSQL session and the task that drives its socket.
pub async fn pg_with_task() -> Option<(tokio_postgres::Client, tokio::task::JoinHandle<()>)> {
    let dsn = env("PGKV_TEST_DSN")?;
    let (client, connection) = tokio_postgres::connect(&dsn, tokio_postgres::NoTls)
        .await
        .expect("connect to PGKV_TEST_DSN");
    let task = tokio::spawn(async move {
        let _ = connection.await;
    });
    Some((client, task))
}

/// A RESP2 connection to the oracle, or `None` when `PGKV_TEST_REDIS_URL` is unset.
pub async fn oracle() -> Option<redis::aio::MultiplexedConnection> {
    let url = env("PGKV_TEST_REDIS_URL")?;
    let client = redis::Client::open(url).expect("PGKV_TEST_REDIS_URL");
    Some(
        client
            .get_multiplexed_async_connection()
            .await
            .expect("connect to PGKV_TEST_REDIS_URL"),
    )
}

/// Deletes the keys starting with `prefix` and returns the prefix.
pub async fn fresh<C: AsyncCommands>(con: &mut C, prefix: &str) -> String {
    let keys: Vec<String> = con.keys(format!("{prefix}*")).await.expect("KEYS");
    if !keys.is_empty() {
        let _: i64 = con.del(keys).await.expect("DEL");
    }
    prefix.to_owned()
}
