//! The redis-rs connection backed by `pgkv.try_call` and `pgkv.pipeline`.

use redis::aio::ConnectionLike;
use redis::{Cmd, Pipeline, RedisFuture, RedisResult, Value};

use crate::error::{self, DbError};
use crate::executor::Executor;
use crate::{args, reply};

// The parameter is declared as text and cast in SQL, so every driver can bind a plain string.
// The reply is fetched as JSON text.

/// One command; `$1` is its words as a PostgreSQL array literal.
const CALL_SQL: &str = "SELECT pgkv.try_call(VARIADIC $1::text::text[])::text";
/// A pipeline; `$1` is its commands as a JSON array of arrays of strings.
const PIPELINE_SQL: &str = "SELECT pgkv.pipeline($1::text::jsonb, false)::text";
/// The same as MULTI/EXEC.
const TRANSACTION_SQL: &str = "SELECT pgkv.pipeline($1::text::jsonb, true)::text";

/// A redis-rs async connection that runs its commands on PostgreSQL through pgkv.
///
/// It implements [`redis::aio::ConnectionLike`], so [`redis::AsyncCommands`], [`redis::Cmd`] and
/// [`redis::Pipeline`] work on it as they do on a Redis connection.
///
/// `E` is what the commands run on: a `tokio_postgres::Client`, a `tokio_postgres::Transaction`,
/// a reference to either, or any other [`Executor`]. The connection does not open, pool or close
/// anything.
///
/// A command is one statement and so is a whole pipeline. A Redis error reply (`WRONGTYPE ...`,
/// `ERR ...`) is returned by that statement, not raised, so it leaves an enclosing PostgreSQL
/// transaction usable. So does an atomic pipeline that is discarded because a command could not
/// be queued: it applies nothing and fails with an `EXECABORT` error whose detail names each
/// refused command by its index in the pipeline.
#[derive(Debug)]
pub struct Connection<E: Executor> {
    executor: E,
    state: E::State,
}

impl<E: Executor> Connection<E> {
    /// Wraps an executor. Nothing is sent until the first command.
    pub fn new(executor: E) -> Self {
        Self {
            executor,
            state: E::State::default(),
        }
    }

    /// The executor, for running other SQL on the same session or transaction.
    pub fn get_ref(&self) -> &E {
        &self.executor
    }

    /// The executor, mutably.
    pub fn get_mut(&mut self) -> &mut E {
        &mut self.executor
    }

    /// Returns the executor, for example to commit a transaction.
    pub fn into_inner(self) -> E {
        self.executor
    }

    async fn query(&mut self, sql: &'static str, param: &str) -> RedisResult<String> {
        self.executor
            .query(&mut self.state, sql, param)
            .await
            .map_err(DbError::into_redis_error)
    }

    async fn command(&mut self, cmd: &Cmd) -> RedisResult<Value> {
        let words = args::array_literal(&args::argv(cmd)?);
        // An error reply comes back as `Value::ServerError`, as on a Redis connection.
        reply::parse(&self.query(CALL_SQL, &words).await?)
    }

    async fn pipeline(&mut self, pipeline: &Pipeline) -> RedisResult<Vec<Value>> {
        // Validate every command before running any.
        let commands = pipeline
            .cmd_iter()
            .map(args::argv)
            .collect::<RedisResult<Vec<_>>>()?;
        let atomic = pipeline.is_transaction();
        let replies = if commands.is_empty() {
            Vec::new()
        } else {
            let sql = if atomic {
                TRANSACTION_SQL
            } else {
                PIPELINE_SQL
            };
            let json = self.query(sql, &args::json(&commands)).await?;
            match reply::parse_pipeline(&json, commands.len())? {
                reply::Pipeline::Replies(replies) => replies,
                // Nothing was applied.
                reply::Pipeline::Discarded(discarded) => return Err(error::discarded(&discarded)),
            }
        };
        // A plain pipeline answers with one reply per command. For MULTI ... EXEC redis-rs asks
        // for the reply to EXEC alone: an array of the commands' replies.
        Ok(if atomic {
            vec![Value::Array(replies)]
        } else {
            replies
        })
    }
}

impl<E: Executor> ConnectionLike for Connection<E> {
    fn req_packed_command<'a>(&'a mut self, cmd: &'a Cmd) -> RedisFuture<'a, Value> {
        Box::pin(self.command(cmd))
    }

    fn req_packed_commands<'a>(
        &'a mut self,
        pipeline: &'a Pipeline,
        _offset: usize,
        _count: usize,
    ) -> RedisFuture<'a, Vec<Value>> {
        Box::pin(self.pipeline(pipeline))
    }

    /// pgkv has one keyspace per PostgreSQL database.
    fn get_db(&self) -> i64 {
        0
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::executor::BoxFuture;
    use crate::DbError;
    use redis::{ErrorKind, ServerErrorKind};

    /// Answers every statement with the same thing and records what it was asked.
    struct Canned {
        answer: fn() -> Result<String, DbError>,
        asked: Vec<(&'static str, String)>,
    }

    impl Executor for Canned {
        type State = ();

        fn query<'a>(
            &'a mut self,
            _state: &'a mut (),
            sql: &'static str,
            param: &'a str,
        ) -> BoxFuture<'a, Result<String, DbError>> {
            self.asked.push((sql, param.to_owned()));
            let answer = (self.answer)();
            Box::pin(async move { answer })
        }
    }

    fn canned(answer: fn() -> Result<String, DbError>) -> Connection<Canned> {
        Connection::new(Canned {
            answer,
            asked: Vec::new(),
        })
    }

    fn pipe(atomic: bool) -> Pipeline {
        let mut pipe = redis::pipe();
        if atomic {
            pipe.atomic();
        }
        pipe.cmd("SET").arg("k").arg("a \"b\"").cmd("GET").arg("k");
        pipe
    }

    #[tokio::test]
    async fn one_statement_per_command_and_per_pipeline() {
        let mut con = canned(|| Ok(r#"{"status": "OK"}"#.to_owned()));
        let cmd = redis::cmd("SET").arg("k").arg("a \"b\"").clone();
        assert_eq!(con.req_packed_command(&cmd).await, Ok(Value::Okay));
        assert_eq!(
            con.get_ref().asked,
            [(CALL_SQL, r#"{"SET","k","a \"b\""}"#.to_owned())]
        );

        for (atomic, sql) in [(false, PIPELINE_SQL), (true, TRANSACTION_SQL)] {
            let mut con = canned(|| Ok(r#"[{"status": "OK"}, {"error": "ERR x"}]"#.to_owned()));
            let replies = con.req_packed_commands(&pipe(atomic), 0, 2).await.unwrap();
            let error = redis::parse_redis_value(b"-ERR x\r\n").unwrap();
            let expected = vec![Value::Okay, error];
            if atomic {
                assert_eq!(replies, [Value::Array(expected)]);
            } else {
                assert_eq!(replies, expected);
            }
            assert_eq!(
                con.get_ref().asked,
                [(sql, r#"[["SET","k","a \"b\""],["GET","k"]]"#.to_owned())]
            );
        }
    }

    #[tokio::test]
    async fn an_error_reply_to_a_command_is_a_value() {
        let mut con = canned(|| Ok(r#"{"error": "ERR syntax error"}"#.to_owned()));
        let reply = con.req_packed_command(&redis::cmd("SET")).await.unwrap();
        assert_eq!(
            reply,
            redis::parse_redis_value(b"-ERR syntax error\r\n").unwrap()
        );
    }

    #[tokio::test]
    async fn a_discarded_transaction_is_an_execabort_error() {
        let mut con = canned(|| {
            Ok(concat!(
                r#"{"error": "EXECABORT Transaction discarded because of previous errors.", "#,
                r#""previous": [{"index": 1, "error": "ERR unknown command 'GEX'"}]}"#
            )
            .to_owned())
        });
        let error = con
            .req_packed_commands(&pipe(true), 3, 1)
            .await
            .unwrap_err();
        assert_eq!(error.kind(), ErrorKind::Server(ServerErrorKind::ExecAbort));
        assert_eq!(error.code(), Some("EXECABORT"));
        assert!(DbError::from_redis_error(&error).is_none());
        assert_eq!(
            error.detail(),
            Some(concat!(
                "Transaction discarded because of previous errors. ",
                "Refused: command 1: ERR unknown command 'GEX'"
            ))
        );
    }

    #[tokio::test]
    async fn database_errors_and_malformed_replies_fail_the_request() {
        let mut con = canned(|| Err(DbError::new(Some("40P01"), "deadlock detected")));
        for atomic in [false, true] {
            let error = con
                .req_packed_commands(&pipe(atomic), 0, 2)
                .await
                .unwrap_err();
            let db = DbError::from_redis_error(&error).expect("a database error");
            assert_eq!(db.sqlstate(), Some("40P01"));
        }
        let error = con
            .req_packed_command(&redis::cmd("PING"))
            .await
            .unwrap_err();
        assert!(DbError::from_redis_error(&error).unwrap().is_retryable());

        for answer in [
            (|| Ok("[1]".to_owned())) as fn() -> Result<String, DbError>,
            || Ok("[1, 2, 3]".to_owned()),
            || Ok("1".to_owned()),
            || Ok("not json".to_owned()),
            || Ok(r#"{"error": "ERR x"}"#.to_owned()),
        ] {
            let mut con = canned(answer);
            let error = con
                .req_packed_commands(&pipe(false), 0, 2)
                .await
                .unwrap_err();
            assert_eq!(error.kind(), ErrorKind::Parse);
        }
    }

    #[tokio::test]
    async fn bad_arguments_and_empty_pipelines_send_nothing() {
        let mut con = canned(|| Ok("[]".to_owned()));
        let mut bad = redis::pipe();
        bad.cmd("SET").arg("k").arg(&b"\xff"[..]);
        assert!(con.req_packed_commands(&bad, 0, 1).await.is_err());
        assert_eq!(
            con.req_packed_commands(&redis::pipe(), 0, 0).await,
            Ok(vec![])
        );
        let mut empty = redis::pipe();
        empty.atomic();
        assert_eq!(
            con.req_packed_commands(&empty, 1, 1).await,
            Ok(vec![Value::Array(vec![])])
        );
        assert!(con.get_ref().asked.is_empty());
    }
}
