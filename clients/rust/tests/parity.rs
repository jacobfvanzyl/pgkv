//! Errors and pipelines behave the same through redis-rs whether the connection is a Redis
//! connection or a pgkv connection: each check runs the same generic code against both. The one
//! difference, the text of the error for a discarded atomic pipeline, is asserted as such.

mod common;

use pgkv::Connection;
use redis::aio::ConnectionLike;
use redis::{AsyncCommands, RedisError, RedisResult, Value};

/// Index in the pipeline, kind, code and detail of a server error.
type ServerErrorAt = (
    usize,
    Option<redis::ServerErrorKind>,
    String,
    Option<String>,
);

/// Everything a caller can observe about an error.
#[derive(Debug, PartialEq)]
struct Observed {
    kind: redis::ErrorKind,
    code: Option<String>,
    detail: Option<String>,
    display: String,
    category: String,
    retry: std::mem::Discriminant<redis::RetryMethod>,
    server_errors: Option<Vec<ServerErrorAt>>,
}

fn observe(error: RedisError) -> Observed {
    Observed {
        kind: error.kind(),
        code: error.code().map(str::to_owned),
        detail: error.detail().map(str::to_owned),
        display: error.to_string(),
        category: error.category().to_owned(),
        retry: std::mem::discriminant(&error.retry_method()),
        server_errors: error.into_server_errors().map(|errors| {
            errors
                .iter()
                .map(|(index, e)| {
                    (
                        *index,
                        e.kind(),
                        e.code().to_owned(),
                        e.details().map(str::to_owned),
                    )
                })
                .collect()
        }),
    }
}

async fn single_command_errors<C: AsyncCommands>(con: &mut C, p: &str) -> Vec<Observed> {
    let k = format!("{p}string");
    let _: () = con.set(&k, "text").await.unwrap();
    let mut seen = Vec::new();
    let mut commands = vec![
        redis::cmd("LPUSH").arg(&k).arg("x").clone(),
        redis::cmd("HGETALL").arg(&k).clone(),
        redis::cmd("INCR").arg(&k).clone(),
        redis::cmd("SET")
            .arg(&k)
            .arg("v")
            .arg("EX")
            .arg("0")
            .clone(),
        redis::cmd("SET").arg(&k).arg("v").arg("EX").clone(),
        redis::cmd("EXPIRE").arg(&k).arg("soon").clone(),
        redis::cmd("GET").clone(),
        redis::cmd("ZADD")
            .arg(format!("{p}z"))
            .arg("nan")
            .arg("m")
            .clone(),
        redis::cmd("LSET")
            .arg(format!("{p}nolist"))
            .arg(0)
            .arg("x")
            .clone(),
        redis::cmd("ZRANGEBYSCORE")
            .arg(format!("{p}z"))
            .arg("a")
            .arg("b")
            .clone(),
    ];
    commands.push(redis::cmd("SCAN").arg("notacursor").clone());
    for cmd in commands {
        let error = cmd.query_async::<Value>(con).await.unwrap_err();
        seen.push(observe(error));
    }
    // The typed layer on top of a reply of another type.
    seen.push(observe(con.get::<_, i64>(&k).await.unwrap_err()));
    seen
}

async fn pipeline_with_error<C: AsyncCommands>(
    con: &mut C,
    p: &str,
) -> (Observed, Vec<Value>, Vec<Option<String>>) {
    let (a, b, c) = (format!("{p}pa"), format!("{p}pb"), format!("{p}pc"));
    let mut pipe = redis::pipe();
    pipe.set(&a, "1")
        .lpush(&a, "x")
        .set(&b, "2")
        .incr(&b, 1)
        .hset(&b, "f", "v")
        .set(&c, "3");
    let error = observe(pipe.query_async::<Vec<Value>>(con).await.unwrap_err());
    let results: Vec<RedisResult<Value>> = pipe.ignore_errors().query_async(con).await.unwrap();
    let results = results
        .into_iter()
        .map(|r| r.unwrap_or_else(|e| Value::SimpleString(format!("{:?}", observe(e)))))
        .collect();
    let state = con.mget(&[&a, &b, &c]).await.unwrap();
    (error, results, state)
}

async fn atomic_pipeline<C: AsyncCommands>(con: &mut C, p: &str) -> (Value, Vec<Option<String>>) {
    let (a, l) = (format!("{p}aa"), format!("{p}al"));
    let reply = redis::pipe()
        .atomic()
        .set(&a, 1)
        .incr(&a, 41)
        .rpush(&l, &["x", "y"])
        .lrange(&l, 0, -1)
        .get(format!("{p}missing"))
        .cmd("TYPE")
        .arg(&l)
        .cmd("PING")
        .query_async::<Value>(con)
        .await
        .unwrap();
    let ignored = redis::pipe()
        .atomic()
        .set(&a, 5)
        .ignore()
        .get(&a)
        .query_async::<Value>(con)
        .await
        .unwrap();
    assert_eq!(
        ignored,
        Value::Array(vec![Value::BulkString(b"5".to_vec())])
    );
    let empty = redis::pipe()
        .atomic()
        .query_async::<Value>(con)
        .await
        .unwrap();
    assert_eq!(empty, Value::Array(vec![]));
    (
        reply,
        con.mget(&[&a, &format!("{p}missing")]).await.unwrap(),
    )
}

/// The error of the pipeline as built, each command's result with `ignore_errors()`, and the
/// values of `keys` afterwards.
type PipelineOutcome = (Observed, Result<Vec<Value>, Observed>, Vec<Option<String>>);

async fn outcome<C: AsyncCommands>(
    con: &mut C,
    mut pipe: redis::Pipeline,
    keys: &[String],
) -> PipelineOutcome {
    let error = observe(pipe.query_async::<Vec<Value>>(con).await.unwrap_err());
    let after_first: Vec<Option<String>> = con.mget(keys).await.unwrap();
    let _: i64 = con.del(keys).await.unwrap();
    let results = pipe
        .ignore_errors()
        .query_async::<Vec<RedisResult<Value>>>(con)
        .await
        .map(|results| {
            results
                .into_iter()
                .map(|r| r.unwrap_or_else(|e| Value::SimpleString(format!("{:?}", observe(e)))))
                .collect()
        })
        .map_err(observe);
    let state: Vec<Option<String>> = con.mget(keys).await.unwrap();
    assert_eq!(
        state, after_first,
        "ignore_errors() changes what is applied"
    );
    let _: i64 = con.del(keys).await.unwrap();
    (error, results, state)
}

/// A command that fails while a MULTI/EXEC block runs: the others are applied.
async fn atomic_runtime_error<C: AsyncCommands>(con: &mut C, p: &str) -> PipelineOutcome {
    let keys = [format!("{p}ra"), format!("{p}rb"), format!("{p}rc")];
    let mut pipe = redis::pipe();
    pipe.atomic()
        .set(&keys[0], "1")
        .lpush(&keys[0], "x")
        .set(&keys[1], "2")
        .incr(&keys[1], 1)
        .hset(&keys[1], "f", "v")
        .cmd("SET")
        .arg(&keys[2])
        .arg("v")
        .arg("EX")
        .arg("0")
        .set(&keys[2], "3");
    outcome(con, pipe, &keys).await
}

/// Commands Redis refuses to queue, between two it accepts: EXEC answers EXECABORT and nothing
/// is applied.
async fn atomic_queue_error<C: AsyncCommands>(
    con: &mut C,
    p: &str,
    refused: &[redis::Cmd],
) -> PipelineOutcome {
    let keys = [format!("{p}qa"), format!("{p}qb")];
    let mut pipe = redis::pipe();
    pipe.atomic().set(&keys[0], "1");
    for cmd in refused {
        pipe.add_command(cmd.clone());
    }
    pipe.set(&keys[1], "2");
    outcome(con, pipe, &keys).await
}

async fn raw_trait_calls<C: ConnectionLike>(con: &mut C, p: &str) -> Vec<Value> {
    let k = format!("{p}raw");
    let mut seen = Vec::new();
    for cmd in [
        redis::cmd("SET").arg(&k).arg("v").clone(),
        redis::cmd("LLEN").arg(&k).clone(),
        redis::cmd("GET").arg(&k).clone(),
    ] {
        seen.push(con.req_packed_command(&cmd).await.unwrap());
    }
    let mut pipe = redis::pipe();
    pipe.get(&k).llen(&k).del(&k);
    seen.extend(con.req_packed_commands(&pipe, 0, 3).await.unwrap());
    // The way redis-rs asks for MULTI, three commands, EXEC: only the reply to EXEC.
    let mut pipe = redis::pipe();
    pipe.atomic().set(&k, "v").llen(&k).del(&k);
    seen.extend(con.req_packed_commands(&pipe, 4, 1).await.unwrap());
    let mut pipe = redis::pipe();
    pipe.set(&k, "v").cmd("NOSUCHCOMMAND").arg("x").del(&k);
    seen.extend(con.req_packed_commands(&pipe, 0, 3).await.unwrap());
    seen
}

#[tokio::test]
async fn same_observable_behaviour_as_a_redis_connection() {
    let (Some(mut client), Some(mut oracle)) = (common::pg().await, common::oracle().await) else {
        return;
    };
    let mut pgkv = Connection::new(&mut client);
    let p = common::fresh(&mut pgkv, "rs:parity:").await;
    common::fresh(&mut oracle, &p).await;

    let theirs = single_command_errors(&mut oracle, &p).await;
    let ours = single_command_errors(&mut pgkv, &p).await;
    assert_eq!(theirs.len(), ours.len());
    for (theirs, ours) in theirs.iter().zip(&ours) {
        assert_eq!(ours, theirs);
    }

    assert_eq!(
        pipeline_with_error(&mut pgkv, &p).await,
        pipeline_with_error(&mut oracle, &p).await
    );
    assert_eq!(
        atomic_pipeline(&mut pgkv, &p).await,
        atomic_pipeline(&mut oracle, &p).await
    );
    assert_eq!(
        raw_trait_calls(&mut pgkv, &p).await,
        raw_trait_calls(&mut oracle, &p).await
    );

    // A failing command inside an atomic pipeline: the same error, the same per-command
    // results, and the other commands applied in both.
    let theirs = atomic_runtime_error(&mut oracle, &p).await;
    assert_eq!(atomic_runtime_error(&mut pgkv, &p).await, theirs);
    let applied = [Some("1"), Some("3"), Some("3")].map(|v| v.map(str::to_owned));
    assert_eq!(theirs.2, applied);
    assert_eq!(theirs.0.server_errors.as_ref().map(Vec::len), Some(3));

    // Commands refused at queue time: nothing applied in either, with or without
    // ignore_errors(), and an error of the same kind, code, category and retry method. The
    // documented difference: redis-rs lists the refused commands in into_server_errors(), pgkv
    // names them in the detail of the EXECABORT error.
    let unknown = redis::cmd("NOSUCHCOMMAND").arg("x").clone();
    let odd_mset = redis::cmd("MSET").arg("k").arg("v").arg("k2").clone();
    // The commands, and the indices in the pipeline of those that are refused.
    let refusals: [(&[redis::Cmd], &[usize]); 6] = [
        (std::slice::from_ref(&unknown), &[1]),
        (&[redis::cmd("NOSUCHCOMMAND").clone()], &[1]),
        (&[redis::cmd("GET").clone()], &[1]),
        (&[redis::cmd("SET").arg("only-a-key").clone()], &[1]),
        (&[redis::cmd("LPOP").clone(), unknown.clone()], &[1, 2]),
        // After one that is queued and would only have failed when run.
        (&[odd_mset, unknown.clone()], &[2]),
    ];
    for (refused, at) in refusals {
        let (their_error, their_results, their_state) =
            atomic_queue_error(&mut oracle, &p, refused).await;
        let (our_error, our_results, our_state) = atomic_queue_error(&mut pgkv, &p, refused).await;
        assert_eq!(their_state, [None, None]);
        assert_eq!(our_state, their_state);
        let exec_abort = redis::ServerErrorKind::ExecAbort;
        assert_eq!(their_error.kind, redis::ErrorKind::Server(exec_abort));
        assert_eq!(their_error.code.as_deref(), Some("EXECABORT"));
        assert_eq!(our_error.kind, their_error.kind);
        assert_eq!(our_error.code, their_error.code);
        assert_eq!(our_error.category, their_error.category);
        assert_eq!(our_error.retry, their_error.retry);

        // Redis through redis-rs: no detail, the refused commands as server errors.
        assert_eq!(their_error.detail, None);
        let first = format!("Transaction aborted: [(Index {}, error: ", at[0]);
        assert!(their_error.display.starts_with(&first));
        let queued = their_error.server_errors.as_ref().unwrap();
        let indices: Vec<usize> = queued.iter().map(|(index, ..)| *index).collect();
        assert_eq!(indices, at);

        // pgkv: the reply to EXEC, then each refused command's index and the error Redis
        // gave it, in the detail. The one server error is the EXECABORT itself.
        let mut detail = "Transaction discarded because of previous errors.".to_owned();
        for (n, (index, _, code, message)) in queued.iter().enumerate() {
            detail += if n == 0 { " Refused: " } else { "; " };
            detail += &format!("command {index}: {code} {}", message.as_deref().unwrap());
        }
        assert_eq!(our_error.detail.as_deref(), Some(detail.as_str()));
        assert_eq!(our_error.display, format!("ExecAbort: {detail}"));
        assert_eq!(
            our_error.server_errors,
            Some(vec![(
                0,
                Some(exec_abort),
                "EXECABORT".to_owned(),
                Some(detail)
            )])
        );

        // ignore_errors() does not turn a discarded transaction into per-command results.
        assert_eq!(their_results.as_ref().unwrap_err(), &their_error);
        assert_eq!(our_results.as_ref().unwrap_err(), &our_error);
    }

    // An argument-count error Redis only finds when the command runs fails that command
    // alone: the transaction is not discarded and the other commands are applied.
    for at_run in [
        redis::cmd("MSET")
            .arg(format!("{p}odd"))
            .arg("v")
            .arg(format!("{p}odd2"))
            .clone(),
        redis::cmd("LPOP")
            .arg(format!("{p}l"))
            .arg(1)
            .arg(1)
            .clone(),
        redis::cmd("PING").arg("a").arg("b").clone(),
    ] {
        let theirs = atomic_queue_error(&mut oracle, &p, std::slice::from_ref(&at_run)).await;
        let ours = atomic_queue_error(&mut pgkv, &p, &[at_run]).await;
        assert_eq!(ours, theirs);
        assert_eq!(theirs.2, [Some("1".to_owned()), Some("2".to_owned())]);
        let failed = theirs.0.server_errors.as_ref().unwrap();
        assert_eq!((failed.len(), failed[0].0), (1, 1));
        let detail = failed[0].3.as_deref().unwrap();
        assert!(detail.starts_with("wrong number of arguments"), "{detail}");
        assert!(theirs.1.is_ok());
    }

    // An unknown command, with and without arguments.
    for cmd in [redis::cmd("NOSUCHCOMMAND").clone(), unknown] {
        let theirs = observe(cmd.query_async::<Value>(&mut oracle).await.unwrap_err());
        let ours = observe(cmd.query_async::<Value>(&mut pgkv).await.unwrap_err());
        assert_eq!(ours, theirs);
    }
    let bare = redis::cmd("NOSUCHCOMMAND")
        .query_async::<Value>(&mut pgkv)
        .await
        .unwrap_err();
    assert_eq!(bare.detail(), Some("unknown command 'NOSUCHCOMMAND'"));

    common::fresh(&mut pgkv, &p).await;
    common::fresh(&mut oracle, &p).await;
}
