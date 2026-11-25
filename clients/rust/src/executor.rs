//! The seam between this crate and a PostgreSQL driver.

use std::future::Future;
use std::pin::Pin;

use crate::DbError;

/// A boxed, sendable future, as returned by [`Executor::query`].
pub type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// Something that can run a statement on one PostgreSQL session.
///
/// Implemented for `tokio_postgres::Client` and `tokio_postgres::Transaction`, owned or behind
/// `&` (feature `tokio-postgres`, on by default), for `sqlx::PgConnection` (feature `sqlx`), and
/// for `&mut E` of any executor `E`. Implement its one method to use another driver.
pub trait Executor: Send {
    /// State a [`Connection`](crate::Connection) keeps for the executor between calls, such as
    /// prepared statements. Use `()` if the driver caches statements itself.
    ///
    /// [`redis::AsyncCommands`] is implemented for connections that are `Send + Sync`, so an
    /// executor has to be `Sync` as well for the typed command methods to be available.
    type State: Default + Send + Sync;

    /// Runs `sql` with `param` bound to `$1` as `text`, and returns the single `text` column of
    /// the single row it produces.
    ///
    /// `sql` is one of a few fixed statements, so it can be prepared once and cached under its
    /// address or text. Report a failure as a [`DbError`] with the SQLSTATE PostgreSQL sent.
    fn query<'a>(
        &'a mut self,
        state: &'a mut Self::State,
        sql: &'static str,
        param: &'a str,
    ) -> BoxFuture<'a, Result<String, DbError>>;
}

impl<E: Executor + ?Sized> Executor for &mut E {
    type State = E::State;

    fn query<'a>(
        &'a mut self,
        state: &'a mut Self::State,
        sql: &'static str,
        param: &'a str,
    ) -> BoxFuture<'a, Result<String, DbError>> {
        (**self).query(state, sql, param)
    }
}
