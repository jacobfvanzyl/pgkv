#![doc = include_str!("../README.md")]
#![forbid(unsafe_code)]
#![warn(missing_docs)]

mod args;
mod connection;
mod error;
mod executor;
mod reply;
#[cfg(feature = "sqlx")]
mod sqlx;
#[cfg(feature = "tokio-postgres")]
mod tokio_postgres;

pub use connection::Connection;
pub use error::DbError;
pub use executor::{BoxFuture, Executor};
