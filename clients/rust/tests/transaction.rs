//! Key-value writes and relational writes in one PostgreSQL transaction.

mod common;

use pgkv::Connection;
use redis::{AsyncCommands, ErrorKind, Value};

async fn order_count(client: &impl tokio_postgres::GenericClient, id: &str) -> i64 {
    client
        .query_one(
            "SELECT count(*) FROM pgkv_rust_orders WHERE id = $1",
            &[&id],
        )
        .await
        .unwrap()
        .get(0)
}

#[tokio::test]
async fn key_and_row_commit_or_roll_back_together() {
    let Some(mut client) = common::pg().await else {
        return;
    };
    client
        .batch_execute(
            "CREATE TABLE IF NOT EXISTS pgkv_rust_orders (id text PRIMARY KEY, total int NOT NULL);
             DELETE FROM pgkv_rust_orders WHERE id LIKE 'tx:%'",
        )
        .await
        .unwrap();
    let key = "rs:tx:order:1";
    let _: i64 = Connection::new(&mut client).del(key).await.unwrap();

    // Roll back: neither the row nor the key exists afterwards.
    let transaction = client.transaction().await.unwrap();
    let mut con = Connection::new(transaction);
    con.get_ref()
        .execute("INSERT INTO pgkv_rust_orders VALUES ('tx:1', 100)", &[])
        .await
        .unwrap();
    let _: () = con.set(key, "pending").await.unwrap();
    let _: i64 = con
        .hset("rs:tx:order:1:meta", "source", "web")
        .await
        .unwrap();
    assert_eq!(con.get::<_, String>(key).await.unwrap(), "pending");
    assert_eq!(order_count(con.get_ref(), "tx:1").await, 1);
    con.into_inner().rollback().await.unwrap();

    let mut con = Connection::new(&mut client);
    assert_eq!(con.get::<_, Option<String>>(key).await.unwrap(), None);
    assert!(!con.exists::<_, bool>("rs:tx:order:1:meta").await.unwrap());
    assert_eq!(order_count(&**con.get_ref(), "tx:1").await, 0);

    // Commit: both are there.
    let transaction = client.transaction().await.unwrap();
    let mut con = Connection::new(transaction);
    con.get_ref()
        .execute("INSERT INTO pgkv_rust_orders VALUES ('tx:1', 100)", &[])
        .await
        .unwrap();
    let _: () = con.set(key, "paid").await.unwrap();
    con.into_inner().commit().await.unwrap();

    let mut con = Connection::new(&mut client);
    assert_eq!(con.get::<_, String>(key).await.unwrap(), "paid");
    assert_eq!(order_count(&**con.get_ref(), "tx:1").await, 1);
    let _: i64 = con.del(key).await.unwrap();
}

#[tokio::test]
async fn an_error_reply_does_not_abort_the_transaction() {
    let Some(mut client) = common::pg().await else {
        return;
    };
    client
        .batch_execute(
            "CREATE TABLE IF NOT EXISTS pgkv_rust_orders (id text PRIMARY KEY, total int NOT NULL);
             DELETE FROM pgkv_rust_orders WHERE id LIKE 'txerr:%'",
        )
        .await
        .unwrap();
    let (text, list, after) = ("rs:txerr:text", "rs:txerr:list", "rs:txerr:after");
    let _: i64 = Connection::new(&mut client)
        .del(&[text, list, after])
        .await
        .unwrap();

    let transaction = client.transaction().await.unwrap();
    let mut con = Connection::new(transaction);
    con.get_ref()
        .execute("INSERT INTO pgkv_rust_orders VALUES ('txerr:1', 100)", &[])
        .await
        .unwrap();
    let _: () = con.set(text, "a string").await.unwrap();

    // WRONGTYPE is an ordinary RedisError, the same one a Redis server gives.
    let err = con.lpush::<_, _, i64>(text, "x").await.unwrap_err();
    assert_eq!(err.kind(), ErrorKind::Extension);
    assert_eq!(err.code(), Some("WRONGTYPE"));
    assert_eq!(
        err.detail(),
        Some("Operation against a key holding the wrong kind of value")
    );
    assert!(pgkv::DbError::from_redis_error(&err).is_none());
    // So are a bad argument and an unknown command.
    let err = con.incr::<_, _, i64>(text, 1).await.unwrap_err();
    assert_eq!(err.code(), Some("ERR"));
    let err = redis::cmd("NOSUCHCOMMAND")
        .query_async::<Value>(&mut con)
        .await
        .unwrap_err();
    assert_eq!(err.code(), Some("ERR"));

    // The transaction goes on: more keys, more rows, and the failed commands changed nothing.
    assert_eq!(con.get::<_, String>(text).await.unwrap(), "a string");
    let _: i64 = con.rpush(list, &["a", "b"]).await.unwrap();
    let _: () = con.set(after, "written after the errors").await.unwrap();
    con.get_ref()
        .execute("INSERT INTO pgkv_rust_orders VALUES ('txerr:2', 200)", &[])
        .await
        .unwrap();
    con.into_inner().commit().await.unwrap();

    let mut con = Connection::new(&mut client);
    assert_eq!(order_count(&**con.get_ref(), "txerr:1").await, 1);
    assert_eq!(order_count(&**con.get_ref(), "txerr:2").await, 1);
    assert_eq!(con.get::<_, String>(text).await.unwrap(), "a string");
    assert_eq!(
        con.lrange::<_, Vec<String>>(list, 0, -1).await.unwrap(),
        ["a", "b"]
    );
    assert_eq!(
        con.get::<_, String>(after).await.unwrap(),
        "written after the errors"
    );
    let _: i64 = con.del(&[text, list, after]).await.unwrap();
}

#[tokio::test]
async fn pipelines_inside_a_transaction() {
    let Some(mut client) = common::pg().await else {
        return;
    };
    let (kept, a, b) = ("rs:txsp:kept", "rs:txsp:a", "rs:txsp:b");
    let _: i64 = Connection::new(&mut client)
        .del(&[kept, a, b])
        .await
        .unwrap();

    let mut transaction = client.transaction().await.unwrap();
    // Borrowed this time, so the transaction can be used directly afterwards.
    let mut con = Connection::new(&mut transaction);
    let _: () = con.set(kept, "1").await.unwrap();

    // As in MULTI/EXEC, the command that fails undoes only itself, and the PostgreSQL
    // transaction survives it.
    let err = redis::pipe()
        .atomic()
        .set(a, "x")
        .incr(a, 1) // not an integer
        .set(b, "y")
        .exec_async(&mut con)
        .await
        .unwrap_err();
    assert_eq!(err.code(), Some("ERR"));
    assert_eq!(con.get::<_, String>(kept).await.unwrap(), "1");
    assert_eq!(con.get::<_, String>(a).await.unwrap(), "x");
    assert_eq!(con.get::<_, String>(b).await.unwrap(), "y");

    let (n,): (i64,) = redis::pipe()
        .atomic()
        .set(a, 1)
        .ignore()
        .incr(a, 1)
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(n, 2);

    // Not visible to another session until the transaction commits.
    let Some(observer) = common::pg().await else {
        return;
    };
    let mut outside = Connection::new(&observer);
    assert_eq!(outside.get::<_, Value>(kept).await.unwrap(), Value::Nil);
    drop(con);
    transaction.commit().await.unwrap();
    assert_eq!(outside.get::<_, String>(kept).await.unwrap(), "1");
    assert_eq!(outside.get::<_, i64>(a).await.unwrap(), 2);
    assert_eq!(outside.get::<_, String>(b).await.unwrap(), "y");
    let _: i64 = outside.del(&[kept, a, b]).await.unwrap();
}

#[tokio::test]
async fn a_discarded_pipeline_does_not_abort_the_enclosing_transaction() {
    let Some(mut client) = common::pg().await else {
        return;
    };
    client
        .batch_execute(
            "CREATE TABLE IF NOT EXISTS pgkv_rust_orders (id text PRIMARY KEY, total int NOT NULL);
             DELETE FROM pgkv_rust_orders WHERE id LIKE 'txabort:%'",
        )
        .await
        .unwrap();
    let (kept, a, after) = ("rs:txabort:kept", "rs:txabort:a", "rs:txabort:after");
    let _: i64 = Connection::new(&mut client)
        .del(&[kept, a, after])
        .await
        .unwrap();

    // Outside a transaction a discarded pipeline applies nothing and that is all.
    let mut con = Connection::new(&mut client);
    let mut discarded = redis::pipe();
    discarded.atomic().set(a, "discarded").cmd("NOSUCHCOMMAND");
    let err = discarded.exec_async(&mut con).await.unwrap_err();
    assert_eq!(err.code(), Some("EXECABORT"));
    assert!(!con.exists::<_, bool>(a).await.unwrap());
    let _: () = con.set(kept, "before").await.unwrap();

    // Inside one it is the same: pgkv.pipeline returns EXECABORT, it does not raise it. The
    // pipeline applies nothing, and the PostgreSQL transaction around it goes on.
    let transaction = client.transaction().await.unwrap();
    let mut con = Connection::new(transaction);
    con.get_ref()
        .execute("INSERT INTO pgkv_rust_orders VALUES ('txabort:1', 1)", &[])
        .await
        .unwrap();
    let _: () = con.set(kept, "inside").await.unwrap();
    for ignore_errors in [false, true] {
        if ignore_errors {
            discarded.ignore_errors();
        }
        let err = discarded.exec_async(&mut con).await.unwrap_err();
        assert_eq!(err.code(), Some("EXECABORT"));
        assert!(pgkv::DbError::from_redis_error(&err).is_none());
        assert_eq!(
            err.detail(),
            Some(concat!(
                "Transaction discarded because of previous errors. ",
                "Refused: command 1: ERR unknown command 'NOSUCHCOMMAND'"
            ))
        );
    }
    assert_eq!(con.get::<_, String>(kept).await.unwrap(), "inside");
    assert!(!con.exists::<_, bool>(a).await.unwrap());
    let _: () = con.set(after, "written after the discard").await.unwrap();
    let (n,): (i64,) = redis::pipe()
        .atomic()
        .set(a, 1)
        .ignore()
        .incr(a, 1)
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(n, 2);
    con.get_ref()
        .execute("INSERT INTO pgkv_rust_orders VALUES ('txabort:2', 2)", &[])
        .await
        .unwrap();
    con.into_inner().commit().await.unwrap();

    let mut con = Connection::new(&mut client);
    assert_eq!(con.get::<_, String>(kept).await.unwrap(), "inside");
    assert_eq!(con.get::<_, i64>(a).await.unwrap(), 2);
    assert_eq!(
        con.get::<_, String>(after).await.unwrap(),
        "written after the discard"
    );
    assert_eq!(order_count(&**con.get_ref(), "txabort:1").await, 1);
    assert_eq!(order_count(&**con.get_ref(), "txabort:2").await, 1);
    let _: i64 = con.del(&[kept, a, after]).await.unwrap();
}
