//! The sqlx executor (feature `sqlx`): pooled connections and transactions.

use pgkv::{Connection, DbError};
use redis::{AsyncCommands, ErrorKind, Value};
use sqlx::Connection as _;

async fn pool() -> Option<sqlx::PgPool> {
    let Ok(dsn) = std::env::var("PGKV_TEST_DSN") else {
        eprintln!("skipping: PGKV_TEST_DSN is not set");
        return None;
    };
    Some(
        sqlx::PgPool::connect(&dsn)
            .await
            .expect("connect to PGKV_TEST_DSN"),
    )
}

#[tokio::test]
async fn commands_and_pipelines_on_a_pooled_connection() {
    let Some(pool) = pool().await else { return };
    let mut pooled = pool.acquire().await.unwrap();
    let mut con = Connection::new(&mut *pooled);
    let (k, l) = ("rs:sqlx:k", "rs:sqlx:l");
    let _: i64 = con.del(&[k, l]).await.unwrap();

    let _: () = con.set(k, "héllo").await.unwrap();
    assert_eq!(con.get::<_, String>(k).await.unwrap(), "héllo");
    assert_eq!(
        con.get::<_, Option<String>>("rs:sqlx:none").await.unwrap(),
        None
    );
    let pong: Value = redis::cmd("PING").query_async(&mut con).await.unwrap();
    assert_eq!(pong, Value::SimpleString("PONG".into()));

    let err = con.lpush::<_, _, i64>(k, "x").await.unwrap_err();
    assert_eq!(
        (err.kind(), err.code()),
        (ErrorKind::Extension, Some("WRONGTYPE"))
    );
    let err = redis::cmd("EVAL")
        .arg("x")
        .query_async::<Value>(&mut con)
        .await
        .unwrap_err();
    assert_eq!(err.code(), Some("ERR"));

    let (len, items): (i64, Vec<String>) = redis::pipe()
        .atomic()
        .rpush(l, &["a", "b"])
        .lrange(l, 0, -1)
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!((len, items), (2, vec!["a".to_owned(), "b".to_owned()]));

    // As in MULTI/EXEC, a command that fails does not undo the others.
    let err = redis::pipe()
        .atomic()
        .set(k, "changed")
        .lpop(k, None)
        .rpush(l, "c")
        .exec_async(&mut con)
        .await
        .unwrap_err();
    assert_eq!(err.into_server_errors().unwrap()[0].1.code(), "WRONGTYPE");
    assert_eq!(con.get::<_, String>(k).await.unwrap(), "changed");
    assert_eq!(con.llen::<_, i64>(l).await.unwrap(), 3);

    // A plain pipeline reports each command's result; a discarded transaction applies nothing.
    let results: Vec<redis::RedisResult<Value>> = redis::pipe()
        .get(k)
        .llen(k)
        .llen(l)
        .ignore_errors()
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(results[0], Ok(Value::BulkString(b"changed".to_vec())));
    assert_eq!(results[1].as_ref().unwrap_err().code(), Some("WRONGTYPE"));
    assert_eq!(results[2], Ok(Value::Int(3)));
    let err = redis::pipe()
        .atomic()
        .del(l)
        .cmd("GET")
        .exec_async(&mut con)
        .await
        .unwrap_err();
    assert_eq!(
        err.kind(),
        ErrorKind::Server(redis::ServerErrorKind::ExecAbort)
    );
    assert!(err.detail().unwrap().contains("Refused: command 1: ERR "));
    assert_eq!(con.llen::<_, i64>(l).await.unwrap(), 3);
    let _: i64 = con.del(&[k, l]).await.unwrap();
}

#[tokio::test]
async fn key_and_row_in_one_sqlx_transaction() {
    let Some(pool) = pool().await else { return };
    sqlx::query("CREATE TABLE IF NOT EXISTS pgkv_rust_sqlx_orders (id text PRIMARY KEY)")
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query("DELETE FROM pgkv_rust_sqlx_orders")
        .execute(&pool)
        .await
        .unwrap();
    let key = "rs:sqlxtx:order";
    let mut pooled = pool.acquire().await.unwrap();
    let _: i64 = Connection::new(&mut *pooled).del(key).await.unwrap();

    for commit in [false, true] {
        let mut transaction = pooled.begin().await.unwrap();
        sqlx::query("INSERT INTO pgkv_rust_sqlx_orders VALUES ('o1')")
            .execute(&mut *transaction)
            .await
            .unwrap();
        let mut con = Connection::new(&mut *transaction);
        let _: () = con.set(key, "pending").await.unwrap();
        // Error replies, alone or in a pipeline, leave the transaction usable.
        let err = con.lpush::<_, _, i64>(key, "x").await.unwrap_err();
        assert_eq!(err.code(), Some("WRONGTYPE"));
        let err = redis::pipe()
            .atomic()
            .set(key, "paid")
            .lpush(key, "x")
            .exec_async(&mut con)
            .await
            .unwrap_err();
        // redis-rs gives a pipeline error the kind of its first failure; an extension code such
        // as WRONGTYPE is only in the per-command errors.
        assert_eq!(err.kind(), ErrorKind::Extension, "{err}");
        assert_eq!(err.into_server_errors().unwrap()[0].1.code(), "WRONGTYPE");
        assert_eq!(con.get::<_, String>(key).await.unwrap(), "paid");
        if commit {
            transaction.commit().await.unwrap();
        } else {
            transaction.rollback().await.unwrap();
        }

        let rows: i64 = sqlx::query_scalar("SELECT count(*) FROM pgkv_rust_sqlx_orders")
            .fetch_one(&mut *pooled)
            .await
            .unwrap();
        let value: Option<String> = Connection::new(&mut *pooled).get(key).await.unwrap();
        if commit {
            assert_eq!((rows, value.as_deref()), (1, Some("paid")));
        } else {
            assert_eq!((rows, value), (0, None));
        }
    }
    let _: i64 = Connection::new(&mut *pooled).del(key).await.unwrap();
}

#[tokio::test]
async fn database_errors_keep_their_sqlstate() {
    let Some(pool) = pool().await else { return };
    let mut pooled = pool.acquire().await.unwrap();
    let mut transaction = pooled.begin().await.unwrap();
    let mut con = Connection::new(&mut *transaction);
    let _: () = con.set("rs:sqlxerr:k", "text").await.unwrap();
    // An error reply is not a database error.
    let err = con.incr::<_, _, i64>("rs:sqlxerr:k", 1).await.unwrap_err();
    assert!(DbError::from_redis_error(&err).is_none());
    assert_eq!(con.get::<_, String>("rs:sqlxerr:k").await.unwrap(), "text");
    // A database error aborts the transaction; the next command is refused by PostgreSQL.
    let failed = sqlx::query("SELECT 1 / 0")
        .execute(&mut **con.get_mut())
        .await;
    assert!(failed.is_err());
    let err = con.get::<_, String>("rs:sqlxerr:k").await.unwrap_err();
    assert_eq!(err.kind(), ErrorKind::Io);
    let db = DbError::from_redis_error(&err).expect("a database error");
    assert_eq!(db.sqlstate(), Some("25P02"));
    transaction.rollback().await.unwrap();
}
