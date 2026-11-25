//! [`Executor`] for sqlx PostgreSQL connections.

use sqlx::PgConnection;

use crate::executor::{BoxFuture, Executor};
use crate::DbError;

impl From<sqlx::Error> for DbError {
    fn from(error: sqlx::Error) -> Self {
        let converted = match &error {
            sqlx::Error::Database(db) => DbError::new(db.code().as_deref(), db.message()),
            sqlx::Error::Io(_) | sqlx::Error::PoolClosed => DbError::closed(error.to_string()),
            _ => DbError::new(None, error.to_string()),
        };
        converted.with_source(error)
    }
}

/// `&mut PgConnection` is what a pooled connection (`&mut *connection`) and a transaction
/// (`&mut *transaction`) dereference to, so this covers both. sqlx prepares and caches the
/// statements on the connection itself.
impl Executor for PgConnection {
    type State = ();

    fn query<'a>(
        &'a mut self,
        _state: &'a mut (),
        sql: &'static str,
        param: &'a str,
    ) -> BoxFuture<'a, Result<String, DbError>> {
        Box::pin(async move { Ok(sqlx::query_scalar(sql).bind(param).fetch_one(self).await?) })
    }
}
