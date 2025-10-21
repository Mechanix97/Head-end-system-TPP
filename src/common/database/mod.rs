pub mod api;
pub mod in_memory;
pub mod postgres;

use clap::ValueEnum;

#[derive(Debug, Clone, PartialEq, Eq, Copy, ValueEnum)]
pub enum DatabaseType {
    InMemory,
    Postgres,
}

#[derive(Debug, thiserror::Error)]
pub enum DatabaseError {
    #[error("io error: {0}")]
    TcpError(#[from] std::io::Error),
    #[error("QueryError error: {0}")]
    QueryError(String),
}
