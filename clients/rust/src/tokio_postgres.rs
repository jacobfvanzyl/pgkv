//! [`Executor`] for tokio-postgres clients and transactions.

use tokio_postgres::{Client, GenericClient, Statement, Transaction};

use crate::executor::{BoxFuture, Executor};
use crate::DbError;

impl From<tokio_postgres::Error> for DbError {
    fn from(error: tokio_postgres::Error) -> Self {
        let converted = match error.as_db_error() {
            Some(db) => DbError::new(Some(db.code().code()), db.message()),
            None if error.is_closed() => DbError::closed(error.to_string()),
            None => DbError::new(None, error.to_string()),
        };
        converted.with_source(error)
    }
}

/// The statements prepared so far, by their text.
type Prepared = Vec<(&'static str, Statement)>;

/// Runs the statement, preparing it on first use.
async fn query<C: GenericClient + Sync>(
    client: &C,
    prepared: &mut Prepared,
    sql: &'static str,
    param: &str,
) -> Result<String, DbError> {
    let statement = match prepared.iter().find(|(text, _)| *text == sql) {
        Some((_, statement)) => statement.clone(),
        None => {
            let statement = client.prepare(sql).await?;
            prepared.push((sql, statement.clone()));
            statement
        }
    };
    let row = client.query_one(&statement, &[&param]).await?;
    Ok(row.try_get(0)?)
}

/// Every statement is a single query, so a client or transaction shared with other tasks
/// (`&Client`) can run everything an exclusive one can.
macro_rules! executor {
    ($($ty:ty => $client:ty),*) => {$(
        impl Executor for $ty {
            type State = Prepared;

            fn query<'a>(
                &'a mut self,
                state: &'a mut Self::State,
                sql: &'static str,
                param: &'a str,
            ) -> BoxFuture<'a, Result<String, DbError>> {
                let client: &$client = self;
                Box::pin(query(client, state, sql, param))
            }
        }
    )*};
}

executor!(
    Client => Client,
    Transaction<'_> => Transaction<'_>,
    &Client => Client,
    &Transaction<'_> => Transaction<'_>
);
