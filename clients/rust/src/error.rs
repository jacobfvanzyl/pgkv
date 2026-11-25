//! Errors from the database that are not Redis replies.

use std::error::Error as StdError;
use std::{fmt, io, sync::Arc};

use redis::{ErrorKind, RedisError, ServerErrorKind, Value};

use crate::reply::Discarded;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Kind {
    Database,
    Closed,
}

/// A failure reported by an [`Executor`](crate::Executor).
///
/// It is either an error raised by PostgreSQL (with its SQLSTATE) or a closed connection.
///
/// It is never a Redis error reply: pgkv returns those (a discarded atomic pipeline included)
/// instead of raising them. A `DbError` reaches the caller as a [`RedisError`] of kind
/// [`redis::ErrorKind::Io`] that carries it; get it back with [`DbError::from_redis_error`].
#[derive(Debug)]
pub struct DbError {
    kind: Kind,
    sqlstate: Option<String>,
    message: String,
    source: Option<Box<dyn StdError + Send + Sync>>,
}

impl DbError {
    /// An error raised by the database. `sqlstate` is the five-character SQLSTATE, if the
    /// driver reported one; `message` is the primary message without the severity prefix.
    pub fn new(sqlstate: Option<&str>, message: impl Into<String>) -> Self {
        Self {
            kind: Kind::Database,
            sqlstate: sqlstate.map(str::to_owned),
            message: message.into(),
            source: None,
        }
    }

    /// The connection to the database is closed.
    pub fn closed(message: impl Into<String>) -> Self {
        Self {
            kind: Kind::Closed,
            ..Self::new(None, message)
        }
    }

    /// Attaches the driver's own error as the [`source`](StdError::source).
    #[must_use]
    pub fn with_source(mut self, source: impl Into<Box<dyn StdError + Send + Sync>>) -> Self {
        self.source = Some(source.into());
        self
    }

    /// The SQLSTATE PostgreSQL reported, e.g. `"40001"`.
    pub fn sqlstate(&self) -> Option<&str> {
        self.sqlstate.as_deref()
    }

    /// The error message.
    pub fn message(&self) -> &str {
        &self.message
    }

    /// Whether the connection to the database is closed.
    pub fn is_connection_closed(&self) -> bool {
        self.kind == Kind::Closed
    }

    /// Whether the transaction failed for a reason that goes away when it is run again:
    /// `serialization_failure` (40001) or `deadlock_detected` (40P01).
    pub fn is_retryable(&self) -> bool {
        matches!(self.sqlstate(), Some("40001" | "40P01"))
    }

    /// The `DbError` inside a [`RedisError`] returned by a [`Connection`](crate::Connection),
    /// or `None` if the error is something else (a Redis error reply, a client-side error).
    pub fn from_redis_error(error: &RedisError) -> Option<&DbError> {
        // redis-rs keeps a wrapped io::Error as Arc<dyn Error> and returns that Arc as the source.
        let source = error.source()?;
        let io_error = match source.downcast_ref::<Arc<dyn StdError + Send + Sync>>() {
            Some(inner) => inner.downcast_ref::<io::Error>(),
            None => source.downcast_ref::<io::Error>(),
        }?;
        io_error.get_ref()?.downcast_ref::<DbError>()
    }

    /// The error a [`Connection`](crate::Connection) returns for this failure.
    pub(crate) fn into_redis_error(self) -> RedisError {
        let kind = match self.kind {
            Kind::Closed => io::ErrorKind::BrokenPipe,
            Kind::Database if self.is_retryable() => io::ErrorKind::Interrupted,
            Kind::Database => io::ErrorKind::Other,
        };
        RedisError::from(io::Error::new(kind, self))
    }
}

impl fmt::Display for DbError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.sqlstate {
            Some(state) => write!(f, "SQLSTATE {state}: {}", self.message),
            None => f.write_str(&self.message),
        }
    }
}

impl StdError for DbError {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        self.source
            .as_deref()
            .map(|source| source as &(dyn StdError + 'static))
    }
}

/// A RESP error is a single line.
pub(crate) fn one_line(line: &str) -> String {
    line.replace(['\r', '\n'], " ")
}

/// Builds the value redis-rs produces for the RESP error line `-<line>`, using its own parser so
/// that kind, code and detail are the same as for an error sent by a Redis server.
pub(crate) fn server_error(line: &str) -> Value {
    let line = one_line(line);
    match redis::parse_redis_value(format!("-{line}\r\n").as_bytes()) {
        Ok(value @ Value::ServerError(_)) => value,
        _ => {
            // Not reachable for a single line, but never panic on server data.
            let (code, detail) = match line.split_once(' ') {
                Some((code, detail)) => (code.to_owned(), Some(detail.to_owned())),
                None => (line, None),
            };
            match redis::make_extension_error(code, detail).try_into() {
                Ok(error) => Value::ServerError(error),
                Err(_) => Value::Nil,
            }
        }
    }
}

/// The error for an atomic pipeline pgkv discarded: the reply to EXEC, followed by every refused
/// command's index in the pipeline and its error.
///
/// redis-rs reports the refused commands of a transaction Redis discarded in an error only its
/// own connections can build, so they are named in the detail instead.
pub(crate) fn discarded(discarded: &Discarded) -> RedisError {
    let mut line = discarded.line.clone();
    for (n, (index, error)) in discarded.refused.iter().enumerate() {
        line += if n == 0 { " Refused: " } else { "; " };
        line += &format!("command {index}: {error}");
    }
    match server_error(&line).extract_error() {
        Err(error) => error,
        // Not reachable: `server_error` returns an error value for any line.
        Ok(_) => RedisError::from((
            ErrorKind::Server(ServerErrorKind::ExecAbort),
            "pgkv: transaction discarded",
            line,
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use redis::RetryMethod;

    fn redis_error(line: &str) -> RedisError {
        server_error(line).extract_error().unwrap_err()
    }

    fn wire_error(line: &str) -> RedisError {
        redis::parse_redis_value(format!("-{line}\r\n").as_bytes())
            .unwrap()
            .extract_error()
            .unwrap_err()
    }

    #[test]
    fn redis_replies_match_what_redis_rs_parses() {
        for line in [
            "ERR syntax error",
            "ERR unknown command 'EVAL', with args beginning with: 'return 1' '0' ",
            "ERR unknown command 'EVAL'",
            "WRONGTYPE Operation against a key holding the wrong kind of value",
            "ERR",
            "EXECABORT Transaction discarded because of previous errors.",
        ] {
            let ours = redis_error(line);
            let theirs = wire_error(line);
            assert_eq!(ours.kind(), theirs.kind(), "{line}");
            assert_eq!(ours.code(), theirs.code(), "{line}");
            assert_eq!(ours.detail(), theirs.detail(), "{line}");
            assert_eq!(ours.to_string(), theirs.to_string(), "{line}");
            assert_eq!(ours, theirs, "{line}");
        }
    }

    #[test]
    fn error_kinds() {
        let e = redis_error("ERR syntax error");
        assert_eq!(e.kind(), ErrorKind::Server(ServerErrorKind::ResponseError));
        assert_eq!(e.code(), Some("ERR"));
        assert_eq!(e.detail(), Some("syntax error"));

        let e = redis_error("WRONGTYPE Operation against a key holding the wrong kind of value");
        assert_eq!(e.kind(), ErrorKind::Extension);
        assert_eq!(e.code(), Some("WRONGTYPE"));
    }

    #[test]
    fn multi_line_message_stays_one_error() {
        let e = redis_error("ERR first\r\nsecond");
        assert_eq!(e.detail(), Some("first  second"));
    }

    #[test]
    fn a_discarded_pipeline_is_execabort_and_names_the_refused_commands() {
        let exec = "EXECABORT Transaction discarded because of previous errors.";
        let error = discarded(&Discarded {
            line: exec.to_owned(),
            refused: vec![
                (1, "ERR unknown command 'NOPE'".to_owned()),
                (3, "ERR wrong number\r\nof arguments".to_owned()),
            ],
        });
        // Kind, code and what follows from them are those of redis-rs's own error.
        assert_eq!(error.kind(), ErrorKind::Server(ServerErrorKind::ExecAbort));
        assert_eq!(error.code(), Some("EXECABORT"));
        assert_eq!(error.category(), wire_error(exec).category());
        assert!(matches!(error.retry_method(), RetryMethod::NoRetry));
        assert!(DbError::from_redis_error(&error).is_none());
        assert_eq!(
            error.detail(),
            Some(concat!(
                "Transaction discarded because of previous errors. Refused: ",
                "command 1: ERR unknown command 'NOPE'; command 3: ERR wrong number  of arguments"
            ))
        );
        assert_eq!(
            error.to_string(),
            format!("ExecAbort: {}", error.detail().unwrap())
        );
        // The one server error is the EXECABORT itself, not the refused commands.
        let errors = error.into_server_errors().unwrap();
        assert_eq!(errors.len(), 1);
        assert_eq!(errors[0].0, 0);
        assert_eq!(errors[0].1.kind(), Some(ServerErrorKind::ExecAbort));

        // Without refused commands it is the reply to EXEC as redis-rs parses it.
        let bare = discarded(&Discarded {
            line: exec.to_owned(),
            refused: Vec::new(),
        });
        assert_eq!(bare, wire_error(exec));
    }

    #[test]
    fn classification() {
        // pgkv does not raise Redis error replies to this crate; one that is raised anyway is
        // a database error like any other, since it aborted the enclosing transaction.
        let raised = DbError::new(Some("KV000"), "ERR x").into_redis_error();
        assert_eq!(raised.kind(), ErrorKind::Io);
        let db = DbError::from_redis_error(&raised).unwrap();
        assert_eq!(db.sqlstate(), Some("KV000"));
        assert!(DbError::new(Some("40001"), "x").is_retryable());
        assert!(DbError::new(Some("40P01"), "x").is_retryable());
        assert!(!DbError::new(Some("23505"), "x").is_retryable());
        assert!(!DbError::closed("x").is_retryable());
    }

    #[test]
    fn database_errors_keep_sqlstate_and_message() {
        let source = io::Error::other("driver detail");
        let e = DbError::new(Some("40001"), "could not serialize access")
            .with_source(source)
            .into_redis_error();
        assert_eq!(e.kind(), ErrorKind::Io);
        assert!(e.is_io_error());
        assert!(!e.is_connection_dropped());
        assert_eq!(e.to_string(), "SQLSTATE 40001: could not serialize access");
        let db = DbError::from_redis_error(&e).expect("DbError is recoverable");
        assert_eq!(db.sqlstate(), Some("40001"));
        assert_eq!(db.message(), "could not serialize access");
        assert!(db.is_retryable());
        assert_eq!(db.source().unwrap().to_string(), "driver detail");

        let e = DbError::new(Some("23505"), "duplicate key").into_redis_error();
        let db = DbError::from_redis_error(&e).unwrap();
        assert_eq!(db.sqlstate(), Some("23505"));
        assert!(!db.is_retryable());
    }

    #[test]
    fn closed_connection_is_a_dropped_connection() {
        let e = DbError::closed("connection closed").into_redis_error();
        assert!(e.is_connection_dropped());
        assert!(matches!(e.retry_method(), RetryMethod::Reconnect));
        assert!(DbError::from_redis_error(&e)
            .unwrap()
            .is_connection_closed());
    }

    #[test]
    fn redis_errors_carry_no_db_error() {
        assert!(DbError::from_redis_error(&redis_error("ERR syntax error")).is_none());
    }
}
