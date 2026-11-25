//! The redis-rs typed API (`AsyncCommands`, `Cmd`, `Pipeline`) over a pgkv connection.

mod common;

use std::collections::{HashMap, HashSet};
use std::num::NonZeroUsize;
use std::time::Duration;

use pgkv::{Connection, DbError};
use redis::aio::ConnectionLike;
use redis::{AsyncCommands, ErrorKind, RedisError, RedisResult, ServerErrorKind, Value};

const WRONGTYPE: &str = "Operation against a key holding the wrong kind of value";

#[tokio::test]
async fn strings() {
    let Some(mut client) = common::pg().await else {
        return;
    };
    let mut con = Connection::new(&mut client);
    let p = common::fresh(&mut con, "rs:strings:").await;
    let k = format!("{p}k");

    let _: () = con.set(&k, "hello").await.unwrap();
    assert_eq!(con.get::<_, String>(&k).await.unwrap(), "hello");
    assert_eq!(
        con.get::<_, Option<String>>(format!("{p}none"))
            .await
            .unwrap(),
        None
    );
    assert_eq!(con.append::<_, _, i64>(&k, " wörld 😀").await.unwrap(), 17);
    assert_eq!(con.get::<_, String>(&k).await.unwrap(), "hello wörld 😀");
    assert_eq!(con.strlen::<_, i64>(&k).await.unwrap(), 17);

    let n = format!("{p}n");
    assert_eq!(con.incr::<_, _, i64>(&n, 5).await.unwrap(), 5);
    assert_eq!(con.decr::<_, _, i64>(&n, 2).await.unwrap(), 3);
    assert_eq!(con.get::<_, i64>(&n).await.unwrap(), 3);
    assert_eq!(con.incr::<_, _, f64>(&n, 0.5).await.unwrap(), 3.5);

    let (a, b) = (format!("{p}a"), format!("{p}b"));
    let _: () = con.mset(&[(&a, "1"), (&b, "2")]).await.unwrap();
    let got: Vec<Option<String>> = con.mget(&[&a, &b, &format!("{p}c")]).await.unwrap();
    assert_eq!(got, [Some("1".into()), Some("2".into()), None]);

    assert!(con
        .set_nx::<_, _, bool>(format!("{p}nx"), "v")
        .await
        .unwrap());
    assert!(!con
        .set_nx::<_, _, bool>(format!("{p}nx"), "w")
        .await
        .unwrap());
    assert!(con.exists::<_, bool>(&k).await.unwrap());
    assert_eq!(con.del::<_, i64>(&[&k, &n]).await.unwrap(), 2);
    assert!(!con.exists::<_, bool>(&k).await.unwrap());

    // +OK and +PONG are statuses, the bulk string "OK" is not.
    let raw: Value = redis::cmd("SET")
        .arg(&k)
        .arg("OK")
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(raw, Value::Okay);
    let raw: Value = redis::cmd("GET")
        .arg(&k)
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(raw, Value::BulkString(b"OK".to_vec()));
    let raw: Value = redis::cmd("PING").query_async(&mut con).await.unwrap();
    assert_eq!(raw, Value::SimpleString("PONG".into()));
    let pong: String = redis::cmd("PING").query_async(&mut con).await.unwrap();
    assert_eq!(pong, "PONG");
    let kind: String = redis::cmd("TYPE")
        .arg(&k)
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(kind, "string");
    let old: Option<String> = redis::cmd("SET")
        .arg(&k)
        .arg("new")
        .arg("GET")
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(old.as_deref(), Some("OK"));
    assert_eq!(con.get_db(), 0);
}

#[tokio::test]
async fn expiry() {
    let Some(mut client) = common::pg().await else {
        return;
    };
    let mut con = Connection::new(&mut client);
    let p = common::fresh(&mut con, "rs:expiry:").await;
    let k = format!("{p}k");

    let _: () = con.set_ex(&k, "v", 100).await.unwrap();
    let ttl: i64 = con.ttl(&k).await.unwrap();
    assert!((98..=100).contains(&ttl), "ttl {ttl}");
    let pttl: i64 = con.pttl(&k).await.unwrap();
    assert!((98_000..=100_000).contains(&pttl), "pttl {pttl}");
    assert!(con.persist::<_, bool>(&k).await.unwrap());
    assert_eq!(con.ttl::<_, i64>(&k).await.unwrap(), -1);
    assert!(con.expire::<_, bool>(&k, 50).await.unwrap());
    assert!(!con.expire::<_, bool>(format!("{p}none"), 50).await.unwrap());
    assert_eq!(con.ttl::<_, i64>(format!("{p}none")).await.unwrap(), -2);

    assert!(con.pexpire::<_, bool>(&k, 60).await.unwrap());
    tokio::time::sleep(Duration::from_millis(150)).await;
    assert!(!con.exists::<_, bool>(&k).await.unwrap());
    assert_eq!(con.get::<_, Option<String>>(&k).await.unwrap(), None);
}

#[tokio::test]
async fn hashes() {
    let Some(mut client) = common::pg().await else {
        return;
    };
    let mut con = Connection::new(&mut client);
    let p = common::fresh(&mut con, "rs:hashes:").await;
    let k = format!("{p}h");

    assert_eq!(
        con.hset::<_, _, _, i64>(&k, "name", "Ada").await.unwrap(),
        1
    );
    let _: () = con
        .hset_multiple(&k, &[("lang", "en"), ("visits", "41")])
        .await
        .unwrap();
    let all: HashMap<String, String> = con.hgetall(&k).await.unwrap();
    let expected: HashMap<String, String> = [("name", "Ada"), ("lang", "en"), ("visits", "41")]
        .into_iter()
        .map(|(f, v)| (f.to_owned(), v.to_owned()))
        .collect();
    assert_eq!(all, expected);
    assert_eq!(con.hget::<_, _, String>(&k, "name").await.unwrap(), "Ada");
    assert_eq!(
        con.hget::<_, _, Option<String>>(&k, "none").await.unwrap(),
        None
    );
    assert_eq!(
        con.hincr::<_, _, _, i64>(&k, "visits", 1).await.unwrap(),
        42
    );
    assert_eq!(con.hlen::<_, i64>(&k).await.unwrap(), 3);
    assert!(con.hexists::<_, _, bool>(&k, "lang").await.unwrap());
    let fields: HashSet<String> = con.hkeys(&k).await.unwrap();
    assert_eq!(fields.len(), 3);
    let some: Vec<Option<String>> = con.hmget(&k, &["lang", "none"]).await.unwrap();
    assert_eq!(some, [Some("en".into()), None]);
    assert_eq!(con.hdel::<_, _, i64>(&k, "lang").await.unwrap(), 1);
    let empty: HashMap<String, String> = con.hgetall(format!("{p}none")).await.unwrap();
    assert!(empty.is_empty());
}

#[tokio::test]
async fn lists() {
    let Some(mut client) = common::pg().await else {
        return;
    };
    let mut con = Connection::new(&mut client);
    let p = common::fresh(&mut con, "rs:lists:").await;
    let k = format!("{p}l");

    assert_eq!(
        con.rpush::<_, _, i64>(&k, &["b", "c", "d"]).await.unwrap(),
        3
    );
    assert_eq!(con.lpush::<_, _, i64>(&k, "a").await.unwrap(), 4);
    let all: Vec<String> = con.lrange(&k, 0, -1).await.unwrap();
    assert_eq!(all, ["a", "b", "c", "d"]);
    assert_eq!(con.llen::<_, i64>(&k).await.unwrap(), 4);
    assert_eq!(con.lindex::<_, String>(&k, -1).await.unwrap(), "d");
    assert_eq!(con.lpop::<_, String>(&k, None).await.unwrap(), "a");
    let two: Vec<String> = con.rpop(&k, NonZeroUsize::new(2)).await.unwrap();
    assert_eq!(two, ["d", "c"]);
    assert_eq!(
        con.lpop::<_, Option<String>>(format!("{p}none"), None)
            .await
            .unwrap(),
        None
    );
    let empty: Vec<String> = con.lrange(format!("{p}none"), 0, -1).await.unwrap();
    assert!(empty.is_empty());
}

#[tokio::test]
async fn sets() {
    let Some(mut client) = common::pg().await else {
        return;
    };
    let mut con = Connection::new(&mut client);
    let p = common::fresh(&mut con, "rs:sets:").await;
    let (a, b) = (format!("{p}a"), format!("{p}b"));

    assert_eq!(
        con.sadd::<_, _, i64>(&a, &["x", "y", "z"]).await.unwrap(),
        3
    );
    assert_eq!(con.sadd::<_, _, i64>(&a, "x").await.unwrap(), 0);
    assert_eq!(
        con.sadd::<_, _, i64>(&b, &["y", "z", "w"]).await.unwrap(),
        3
    );
    let members: HashSet<String> = con.smembers(&a).await.unwrap();
    assert_eq!(members, HashSet::from(["x".into(), "y".into(), "z".into()]));
    assert!(con.sismember::<_, _, bool>(&a, "x").await.unwrap());
    assert!(!con.sismember::<_, _, bool>(&a, "w").await.unwrap());
    assert_eq!(con.scard::<_, i64>(&a).await.unwrap(), 3);
    let both: HashSet<String> = con.sinter(&[&a, &b]).await.unwrap();
    assert_eq!(both, HashSet::from(["y".into(), "z".into()]));
    assert_eq!(con.srem::<_, _, i64>(&a, "x").await.unwrap(), 1);
}

#[tokio::test]
async fn sorted_sets() {
    let Some(mut client) = common::pg().await else {
        return;
    };
    let mut con = Connection::new(&mut client);
    let p = common::fresh(&mut con, "rs:zsets:").await;
    let k = format!("{p}z");

    assert_eq!(con.zadd::<_, _, _, i64>(&k, "one", 1).await.unwrap(), 1);
    let added: i64 = con
        .zadd_multiple(&k, &[(2.5, "two"), (-3.0, "minus"), (1e10, "big")])
        .await
        .unwrap();
    assert_eq!(added, 3);
    let scored: Vec<(String, f64)> = con.zrange_withscores(&k, 0, -1).await.unwrap();
    assert_eq!(
        scored,
        [
            ("minus".to_owned(), -3.0),
            ("one".to_owned(), 1.0),
            ("two".to_owned(), 2.5),
            ("big".to_owned(), 1e10)
        ]
    );
    assert_eq!(con.zscore::<_, _, f64>(&k, "two").await.unwrap(), 2.5);
    assert_eq!(
        con.zscore::<_, _, Option<f64>>(&k, "none").await.unwrap(),
        None
    );
    assert_eq!(
        con.zincr::<_, _, _, f64>(&k, "two", 0.25).await.unwrap(),
        2.75
    );
    assert_eq!(
        con.zrank::<_, _, Option<i64>>(&k, "one").await.unwrap(),
        Some(1)
    );
    assert_eq!(con.zcard::<_, i64>(&k).await.unwrap(), 4);
    let names: Vec<String> = con.zrangebyscore(&k, 0, 3).await.unwrap();
    assert_eq!(names, ["one", "two"]);

    let _: i64 = con.zadd(&k, "top", "inf").await.unwrap();
    let _: i64 = con.zadd(&k, "bottom", "-inf").await.unwrap();
    assert_eq!(
        con.zscore::<_, _, f64>(&k, "top").await.unwrap(),
        f64::INFINITY
    );
    assert_eq!(
        con.zscore::<_, _, f64>(&k, "bottom").await.unwrap(),
        f64::NEG_INFINITY
    );
    let popped: Vec<(String, f64)> = con.zpopmin(&k, 1).await.unwrap();
    assert_eq!(popped, [("bottom".to_owned(), f64::NEG_INFINITY)]);
}

#[tokio::test]
async fn scan_iterators() {
    let Some(mut client) = common::pg().await else {
        return;
    };
    let mut con = Connection::new(&mut client);
    let p = common::fresh(&mut con, "rs:scan:").await;

    // More keys than one SCAN call returns, so the iterator has to follow the cursor.
    let mut pipe = redis::pipe();
    for i in 0..250 {
        pipe.set(format!("{p}key:{i}"), i).ignore();
    }
    let _: () = pipe.query_async(&mut con).await.unwrap();

    let mut seen = HashSet::new();
    let mut iter = con
        .scan_match::<_, String>(format!("{p}key:*"))
        .await
        .unwrap();
    while let Some(key) = iter.next_item().await {
        assert!(seen.insert(key.unwrap()), "SCAN returned a key twice");
    }
    drop(iter);
    let expected: HashSet<String> = (0..250).map(|i| format!("{p}key:{i}")).collect();
    assert_eq!(seen, expected);

    let h = format!("{p}hash");
    let fields: Vec<(String, String)> = (0..120)
        .map(|i| (format!("f{i}"), format!("v{i}")))
        .collect();
    let _: () = con.hset_multiple(&h, &fields).await.unwrap();
    let mut scanned = HashMap::new();
    let mut iter = con.hscan::<_, (String, String)>(&h).await.unwrap();
    while let Some(pair) = iter.next_item().await {
        let (field, value) = pair.unwrap();
        scanned.insert(field, value);
    }
    drop(iter);
    assert_eq!(scanned, fields.into_iter().collect::<HashMap<_, _>>());

    let s = format!("{p}set");
    let _: i64 = con.sadd(&s, (0..120).collect::<Vec<i32>>()).await.unwrap();
    let mut members = HashSet::new();
    let mut iter = con.sscan::<_, i32>(&s).await.unwrap();
    while let Some(member) = iter.next_item().await {
        members.insert(member.unwrap());
    }
    drop(iter);
    assert_eq!(members, (0..120).collect::<HashSet<i32>>());

    let z = format!("{p}zset");
    let scores: Vec<(f64, String)> = (0..120)
        .map(|i| (i as f64 / 2.0, format!("m{i}")))
        .collect();
    let _: i64 = con.zadd_multiple(&z, &scores).await.unwrap();
    let mut scored = HashMap::new();
    let mut iter = con.zscan::<_, (String, f64)>(&z).await.unwrap();
    while let Some(pair) = iter.next_item().await {
        let (member, score) = pair.unwrap();
        scored.insert(member, score);
    }
    drop(iter);
    assert_eq!(scored.len(), 120);
    assert_eq!(scored["m7"], 3.5);
}

#[tokio::test]
async fn pipeline() {
    let Some(mut client) = common::pg().await else {
        return;
    };
    let mut con = Connection::new(&mut client);
    let p = common::fresh(&mut con, "rs:pipe:").await;
    let (a, l) = (format!("{p}a"), format!("{p}l"));

    let (n, got, items): (i64, String, Vec<String>) = redis::pipe()
        .set(&a, 41)
        .ignore()
        .incr(&a, 1)
        .get(&a)
        .rpush(&l, &["x", "y"])
        .ignore()
        .lrange(&l, 0, -1)
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!((n, got.as_str()), (42, "42"));
    assert_eq!(items, ["x", "y"]);

    let empty: Vec<Value> = redis::pipe().query_async(&mut con).await.unwrap();
    assert!(empty.is_empty());

    // An error reply takes its slot; the commands around it still run, as in Redis.
    let b = format!("{p}b");
    let err = redis::pipe()
        .set(&b, "1")
        .lpush(&b, "x")
        .set(format!("{p}c"), "3")
        .query_async::<Vec<Value>>(&mut con)
        .await
        .unwrap_err();
    assert_eq!(err.kind(), ErrorKind::Extension);
    assert_eq!(
        err.to_string(),
        format!("Pipeline failure: [(Index 1, error: \"WRONGTYPE\": {WRONGTYPE})]")
    );
    let errors = err.into_server_errors().unwrap();
    assert_eq!(
        (errors.len(), errors[0].0, errors[0].1.code()),
        (1, 1, "WRONGTYPE")
    );
    assert_eq!(con.get::<_, String>(&b).await.unwrap(), "1");
    assert_eq!(con.get::<_, String>(format!("{p}c")).await.unwrap(), "3");

    let results: Vec<RedisResult<Value>> = redis::pipe()
        .get(&b)
        .lpush(&b, "x")
        .get(&b)
        .ignore_errors()
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(results[0], Ok(Value::BulkString(b"1".to_vec())));
    assert_eq!(results[1].as_ref().unwrap_err().code(), Some("WRONGTYPE"));
    assert_eq!(results[2], Ok(Value::BulkString(b"1".to_vec())));
}

#[tokio::test]
async fn atomic_pipeline() {
    let Some(mut client) = common::pg().await else {
        return;
    };
    let mut con = Connection::new(&mut client);
    let p = common::fresh(&mut con, "rs:atomic:").await;
    let (a, b) = (format!("{p}a"), format!("{p}b"));

    let (n, got): (i64, String) = redis::pipe()
        .atomic()
        .set(&a, 1)
        .ignore()
        .incr(&a, 9)
        .set(&b, "two")
        .ignore()
        .get(&b)
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!((n, got.as_str()), (10, "two"));
    assert_eq!(con.get::<_, i64>(&a).await.unwrap(), 10);

    let empty: Vec<Value> = redis::pipe().atomic().query_async(&mut con).await.unwrap();
    assert!(empty.is_empty());
    let _: () = redis::pipe()
        .atomic()
        .del(&b)
        .exec_async(&mut con)
        .await
        .unwrap();
    assert!(!con.exists::<_, bool>(&b).await.unwrap());
}

#[tokio::test]
async fn atomic_pipeline_errors_are_those_of_multi_exec() {
    let Some(mut client) = common::pg().await else {
        return;
    };
    let mut con = Connection::new(&mut client);
    let p = common::fresh(&mut con, "rs:atomicfail:").await;
    let (a, b, c) = (format!("{p}a"), format!("{p}b"), format!("{p}c"));
    let _: () = con.set(&a, "before").await.unwrap();

    // A command that fails while the transaction runs does not undo the others.
    let mut pipe = redis::pipe();
    pipe.atomic()
        .set(&a, "after")
        .set(&b, "new")
        .lpush(&b, "x") // WRONGTYPE
        .set(&c, "still runs");
    let err = pipe.query_async::<Vec<Value>>(&mut con).await.unwrap_err();
    assert_eq!(err.kind(), ErrorKind::Extension);
    assert_eq!(
        err.to_string(),
        format!("Pipeline failure: [(Index 2, error: \"WRONGTYPE\": {WRONGTYPE})]")
    );
    let state: Vec<Option<String>> = con.mget(&[&a, &b, &c]).await.unwrap();
    assert_eq!(
        state,
        [Some("after"), Some("new"), Some("still runs")].map(|v| v.map(str::to_owned))
    );

    // With ignore_errors() every command reports its own result.
    let results: Vec<RedisResult<Value>> =
        pipe.ignore_errors().query_async(&mut con).await.unwrap();
    assert_eq!(results.len(), 4);
    assert_eq!(results[0], Ok(Value::Okay));
    assert_eq!(results[1], Ok(Value::Okay));
    assert_eq!(results[2].as_ref().unwrap_err().code(), Some("WRONGTYPE"));
    assert_eq!(results[3], Ok(Value::Okay));

    // A command Redis would refuse to queue (unknown, or wrong number of arguments) discards
    // the transaction: EXECABORT, and nothing is applied.
    let _: i64 = con.del(&[&a, &b, &c]).await.unwrap();
    for refused in [
        redis::cmd("NOSUCHCOMMAND").arg("x").clone(),
        redis::cmd("GET").clone(),
    ] {
        let mut pipe = redis::pipe();
        pipe.atomic().set(&a, "1").add_command(refused).set(&b, "2");
        for ignore_errors in [false, true] {
            if ignore_errors {
                pipe.ignore_errors();
            }
            let err = pipe.query_async::<Value>(&mut con).await.unwrap_err();
            assert_eq!(err.kind(), ErrorKind::Server(ServerErrorKind::ExecAbort));
            assert_eq!(err.code(), Some("EXECABORT"));
            // The refused command, by its index in the pipeline.
            let detail = err.detail().unwrap();
            assert!(
                detail.starts_with(
                    "Transaction discarded because of previous errors. Refused: command 1: ERR "
                ),
                "{detail}"
            );
            assert_eq!(con.exists::<_, i64>(&[&a, &b]).await.unwrap(), 0);
        }
    }

    // An argument-count error Redis only finds when the command runs (MSET with an odd number
    // of words) fails that command alone, as any other error inside the transaction does.
    let results: Vec<RedisResult<Value>> = redis::pipe()
        .atomic()
        .set(&a, "1")
        .cmd("MSET")
        .arg(&a)
        .arg("x")
        .arg(&b)
        .set(&b, "2")
        .ignore_errors()
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(results[0], Ok(Value::Okay));
    assert_response_error(
        results[1].as_ref().unwrap_err(),
        "wrong number of arguments for 'mset' command",
    );
    assert_eq!(results[2], Ok(Value::Okay));
    assert_eq!(con.exists::<_, i64>(&[&a, &b]).await.unwrap(), 2);
    let _: i64 = con.del(&[&a, &b]).await.unwrap();

    // In a plain pipeline the same command is only an error in its own slot.
    let results: Vec<RedisResult<Value>> = redis::pipe()
        .set(&a, "1")
        .cmd("NOSUCHCOMMAND")
        .set(&b, "2")
        .ignore_errors()
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(results[0], Ok(Value::Okay));
    assert_response_error(results[1].as_ref().unwrap_err(), "unknown command");
    assert_eq!(results[2], Ok(Value::Okay));
    assert_eq!(con.exists::<_, i64>(&[&a, &b]).await.unwrap(), 2);
}

/// An executor that records the statements it is asked to run.
struct Recording<'a> {
    client: &'a tokio_postgres::Client,
    statements: Vec<&'static str>,
}

impl<'a> pgkv::Executor for Recording<'a> {
    type State = <&'a tokio_postgres::Client as pgkv::Executor>::State;

    fn query<'b>(
        &'b mut self,
        state: &'b mut Self::State,
        sql: &'static str,
        param: &'b str,
    ) -> pgkv::BoxFuture<'b, Result<String, DbError>> {
        self.statements.push(sql);
        pgkv::Executor::query(&mut self.client, state, sql, param)
    }
}

#[tokio::test]
async fn a_pipeline_is_one_statement() {
    let Some(client) = common::pg().await else {
        return;
    };
    let mut con = Connection::new(Recording {
        client: &client,
        statements: Vec::new(),
    });
    let p = common::fresh(&mut con, "rs:onestmt:").await;

    // Words that need quoting in a JSON string or in an array literal.
    let words = [
        "a,b",
        "{c}",
        "\"quoted\"",
        "back\\slash",
        "",
        "NULL",
        "né 😀\n\ttab\u{1}",
        "'",
    ];
    for atomic in [false, true] {
        con.get_mut().statements.clear();
        let mut pipe = redis::pipe();
        if atomic {
            pipe.atomic();
        }
        for (index, word) in words.iter().enumerate() {
            pipe.set(format!("{p}{atomic}{index}"), word).ignore();
        }
        for index in 0..words.len() {
            pipe.get(format!("{p}{atomic}{index}"));
        }
        let got: Vec<String> = pipe.query_async(&mut con).await.unwrap();
        assert_eq!(got, words);
        let statements = &con.get_ref().statements;
        assert_eq!(statements.len(), 1, "{statements:?}");
        assert!(statements[0].contains("pgkv.pipeline("), "{statements:?}");
    }

    // The same words through single commands, one statement each.
    con.get_mut().statements.clear();
    for (index, word) in words.iter().enumerate() {
        let key = format!("{p}single{index}");
        let _: () = con.set(&key, word).await.unwrap();
        assert_eq!(con.get::<_, String>(&key).await.unwrap(), *word);
    }
    assert_eq!(con.get_ref().statements.len(), 2 * words.len());

    // An empty pipeline sends nothing.
    con.get_mut().statements.clear();
    let empty: Vec<Value> = redis::pipe().query_async(&mut con).await.unwrap();
    assert!(empty.is_empty());
    let empty: Vec<Value> = redis::pipe().atomic().query_async(&mut con).await.unwrap();
    assert!(empty.is_empty());
    assert!(con.get_ref().statements.is_empty());
}

fn assert_response_error(err: &RedisError, detail_prefix: &str) {
    assert_eq!(
        err.kind(),
        ErrorKind::Server(ServerErrorKind::ResponseError),
        "{err}"
    );
    assert_eq!(err.code(), Some("ERR"));
    let detail = err.detail().unwrap_or_default();
    assert!(detail.starts_with(detail_prefix), "{detail}");
    assert!(DbError::from_redis_error(err).is_none());
}

#[tokio::test]
async fn error_replies() {
    let Some(mut client) = common::pg().await else {
        return;
    };
    let mut con = Connection::new(&mut client);
    let p = common::fresh(&mut con, "rs:errors:").await;
    let k = format!("{p}k");
    let _: () = con.set(&k, "text").await.unwrap();

    let err = con.lpush::<_, _, i64>(&k, "x").await.unwrap_err();
    assert_eq!(err.kind(), ErrorKind::Extension);
    assert_eq!(err.code(), Some("WRONGTYPE"));
    assert_eq!(err.detail(), Some(WRONGTYPE));
    assert_eq!(err.to_string(), format!("\"WRONGTYPE\": {WRONGTYPE}"));
    assert!(err.into_server_errors().is_some());

    let err = redis::cmd("SET")
        .arg(&k)
        .arg("v")
        .arg("BADOPTION")
        .query_async::<Value>(&mut con)
        .await
        .unwrap_err();
    assert_response_error(&err, "syntax error");
    assert_eq!(err.to_string(), "ResponseError: syntax error");

    let err = con.incr::<_, _, i64>(&k, 1).await.unwrap_err();
    assert_response_error(&err, "value is not an integer or out of range");

    let err = redis::cmd("GET")
        .query_async::<Value>(&mut con)
        .await
        .unwrap_err();
    assert_response_error(&err, "wrong number of arguments for 'get' command");

    // Commands pgkv does not have are unknown commands, as they would be on a Redis without them.
    for command in ["EVAL", "SUBSCRIBE", "BLPOP", "CLIENT", "MULTI", "XADD"] {
        let err = redis::cmd(command)
            .arg("x")
            .arg("0")
            .query_async::<Value>(&mut con)
            .await
            .unwrap_err();
        assert_response_error(
            &err,
            &format!("unknown command '{command}', with args beginning with: 'x' '0' "),
        );
        // Without arguments there is no list of them, as on Redis 8.
        let err = redis::cmd(command)
            .query_async::<Value>(&mut con)
            .await
            .unwrap_err();
        assert_eq!(
            err.detail(),
            Some(format!("unknown command '{command}'").as_str())
        );
    }

    // The raw trait method hands the error back as a value, as redis-rs connections do.
    let raw = con
        .req_packed_command(redis::cmd("LLEN").arg(&k))
        .await
        .unwrap();
    assert!(matches!(raw, Value::ServerError(_)), "{raw:?}");

    // A reply of the wrong type is a redis-rs parse error, not a server error.
    let err = con.get::<_, i64>(&k).await.unwrap_err();
    assert_eq!(err.kind(), ErrorKind::Parse);
}

#[tokio::test]
async fn arguments_that_are_not_text_are_rejected_before_anything_runs() {
    let Some(mut client) = common::pg().await else {
        return;
    };
    let mut con = Connection::new(&mut client);
    let p = common::fresh(&mut con, "rs:args:").await;
    let k = format!("{p}k");

    let err = con
        .set::<_, _, ()>(&k, &[0xff_u8, 0x00, 0x80][..])
        .await
        .unwrap_err();
    assert_eq!(err.kind(), ErrorKind::Client);
    assert!(err.to_string().contains("not valid UTF-8"), "{err}");
    let err = con.set::<_, _, ()>(&k, "a\0b").await.unwrap_err();
    assert_eq!(err.kind(), ErrorKind::Client);
    assert!(err.to_string().contains("NUL"), "{err}");
    assert!(!con.exists::<_, bool>(&k).await.unwrap());

    for atomic in [false, true] {
        let mut pipe = redis::pipe();
        if atomic {
            pipe.atomic();
        }
        let err = pipe
            .set(&k, "fine")
            .set(format!("{p}other"), &b"\xc3\x28"[..])
            .exec_async(&mut con)
            .await
            .unwrap_err();
        assert_eq!(err.kind(), ErrorKind::Client);
        assert_eq!(err.detail(), Some("argument 2 of SET"));
        assert!(!con.exists::<_, bool>(&k).await.unwrap());
    }
}

#[tokio::test]
async fn owned_and_shared_clients() {
    let Some(client) = common::pg().await else {
        return;
    };

    // A shared client runs everything: a command or a pipeline is a single statement.
    let mut shared = Connection::new(&client);
    let p = common::fresh(&mut shared, "rs:shared:").await;
    let k = format!("{p}k");
    let _: () = shared.set(&k, 1).await.unwrap();
    let (n,): (i64,) = redis::pipe()
        .incr(&k, 1)
        .query_async(&mut shared)
        .await
        .unwrap();
    assert_eq!(n, 2);
    let (n,): (i64,) = redis::pipe()
        .atomic()
        .incr(&k, 1)
        .query_async(&mut shared)
        .await
        .unwrap();
    assert_eq!(n, 3);

    // Two connections over the same shared client can be used concurrently, atomic pipelines
    // included: neither sees the other's commands between its own.
    let mut other = Connection::new(&client);
    let mut tens = redis::pipe();
    tens.atomic().incr(&k, 10).incr(&k, 10).get(&k);
    let mut hundreds = redis::pipe();
    hundreds.atomic().incr(&k, 100).incr(&k, 100).get(&k);
    let (a, b) = tokio::join!(
        tens.query_async::<(i64, i64, i64)>(&mut shared),
        hundreds.query_async::<(i64, i64, i64)>(&mut other)
    );
    let (a, b) = (a.unwrap(), b.unwrap());
    assert_eq!((a.1 - a.0, b.1 - b.0), (10, 100));
    assert_eq!(a.2.max(b.2), 223);
    let (a, b) = tokio::join!(
        shared.incr::<_, _, i64>(&k, 1),
        other.incr::<_, _, i64>(&k, 1)
    );
    assert_eq!(a.unwrap().max(b.unwrap()), 225);

    // An owned client does everything.
    let mut owned = Connection::new(client);
    let (n,): (i64,) = redis::pipe()
        .atomic()
        .incr(&k, 1)
        .query_async(&mut owned)
        .await
        .unwrap();
    assert_eq!(n, 226);
    let client = owned.into_inner();
    assert_eq!(
        client
            .query_one("SELECT 1", &[])
            .await
            .unwrap()
            .get::<_, i32>(0),
        1
    );
}

#[tokio::test]
async fn database_errors_keep_their_sqlstate() {
    let Some(mut client) = common::pg().await else {
        return;
    };
    let mut setup = Connection::new(&mut client);
    let p = common::fresh(&mut setup, "rs:dberr:").await;
    let k = format!("{p}k");
    let _: () = setup.set(&k, "text").await.unwrap();

    // A Redis error reply is not a database error and leaves the transaction usable.
    let transaction = client.transaction().await.unwrap();
    let mut con = Connection::new(transaction);
    let err = con.lpush::<_, _, i64>(&k, "x").await.unwrap_err();
    assert_eq!(err.code(), Some("WRONGTYPE"));
    assert!(DbError::from_redis_error(&err).is_none());
    assert_eq!(con.get::<_, String>(&k).await.unwrap(), "text");

    // A database error aborts it. What follows is refused by PostgreSQL, which is a database
    // error too, for a command and for a pipeline.
    let failed = con.get_ref().batch_execute("SELECT 1 / 0").await;
    assert!(failed.is_err());
    let err = con.get::<_, String>(&k).await.unwrap_err();
    assert_eq!(err.kind(), ErrorKind::Io);
    assert!(err.into_server_errors().is_none());
    for atomic in [false, true] {
        let mut pipe = redis::pipe();
        if atomic {
            pipe.atomic();
        }
        let err = pipe.get(&k).exec_async(&mut con).await.unwrap_err();
        assert_eq!(err.kind(), ErrorKind::Io);
        assert_eq!(
            DbError::from_redis_error(&err).unwrap().sqlstate(),
            Some("25P02")
        );
    }
    let err = con.get::<_, String>(&k).await.unwrap_err();
    let db = DbError::from_redis_error(&err).expect("a database error");
    assert_eq!(db.sqlstate(), Some("25P02"));
    assert!(
        db.message().contains("current transaction is aborted"),
        "{db}"
    );
    assert!(!db.is_retryable());
    assert!(!err.is_connection_dropped());
    assert_eq!(err.to_string(), format!("SQLSTATE 25P02: {}", db.message()));
    con.into_inner().rollback().await.unwrap();
}

#[tokio::test]
async fn serialization_failure_is_retryable() {
    let (Some(mut first), Some(second)) = (common::pg().await, common::pg().await) else {
        return;
    };
    let mut other = Connection::new(&second);
    let p = common::fresh(&mut other, "rs:retry:").await;
    let k = format!("{p}k");
    let _: () = other.set(&k, 1).await.unwrap();

    let transaction = first
        .build_transaction()
        .isolation_level(tokio_postgres::IsolationLevel::RepeatableRead)
        .start()
        .await
        .unwrap();
    let mut con = Connection::new(transaction);
    assert_eq!(con.get::<_, i64>(&k).await.unwrap(), 1);
    let _: () = other.set(&k, 2).await.unwrap();
    let err = con.incr::<_, _, i64>(&k, 1).await.unwrap_err();
    assert_eq!(err.kind(), ErrorKind::Io);
    let db = DbError::from_redis_error(&err).expect("a database error");
    assert_eq!(db.sqlstate(), Some("40001"), "{db}");
    assert!(db.is_retryable());
    assert!(matches!(
        err.retry_method(),
        redis::RetryMethod::RetryImmediately
    ));
    con.into_inner().rollback().await.unwrap();
    assert_eq!(other.get::<_, i64>(&k).await.unwrap(), 2);

    // The same inside a pipeline: the database error fails the whole pipeline and undoes all
    // of it, including the commands that had already run.
    let transaction = first
        .build_transaction()
        .isolation_level(tokio_postgres::IsolationLevel::RepeatableRead)
        .start()
        .await
        .unwrap();
    let mut con = Connection::new(transaction);
    assert_eq!(con.get::<_, i64>(&k).await.unwrap(), 2);
    let _: () = other.set(&k, 3).await.unwrap();
    let before = format!("{p}before");
    let err = redis::pipe()
        .set(&before, "x")
        .incr(&k, 1)
        .exec_async(&mut con)
        .await
        .unwrap_err();
    let db = DbError::from_redis_error(&err).expect("a database error");
    assert_eq!(db.sqlstate(), Some("40001"), "{db}");
    con.into_inner().rollback().await.unwrap();
    assert_eq!(other.get::<_, i64>(&k).await.unwrap(), 3);
    assert!(!other.exists::<_, bool>(&before).await.unwrap());
}

#[tokio::test]
async fn closed_connection_is_reported_as_dropped() {
    let Some((client, task)) = common::pg_with_task().await else {
        return;
    };
    let mut con = Connection::new(&client);
    let _: String = redis::cmd("PING").query_async(&mut con).await.unwrap();
    task.abort();
    let _ = task.await;
    let err = redis::cmd("PING")
        .query_async::<String>(&mut con)
        .await
        .unwrap_err();
    assert_eq!(err.kind(), ErrorKind::Io);
    assert!(err.is_connection_dropped(), "{err}");
    assert!(DbError::from_redis_error(&err)
        .unwrap()
        .is_connection_closed());
}
