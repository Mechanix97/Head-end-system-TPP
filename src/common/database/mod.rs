pub mod api;
pub mod in_memory;
pub mod postgres;

use clap::ValueEnum;

#[derive(Debug, Clone, PartialEq, Eq, Copy, ValueEnum)]
pub enum DatabaseType {
    InMemory,
    Postgres,
}

/// All parameters needed to initialize a database connection.
///
/// Use `DatabaseConfig::in_memory()` for tests, or construct the struct
/// directly with postgres fields for production.
#[derive(Debug)]
pub struct DatabaseConfig {
    pub db_type: DatabaseType,
    pub user: String,
    pub password: String,
    pub url: String,
    pub port: String,
}

impl DatabaseConfig {
    /// Shortcut for tests: creates an in-memory config (postgres fields are ignored).
    pub fn in_memory() -> Self {
        Self {
            db_type: DatabaseType::InMemory,
            user: String::new(),
            password: String::new(),
            url: String::new(),
            port: String::new(),
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum DatabaseError {
    #[error("io error: {0}")]
    TcpError(#[from] std::io::Error),
    #[error("QueryError error: {0}")]
    QueryError(String),
    #[error("Error: No data found")]
    NoDataFound,
    #[error("Error: Too many rows")]
    TooManyRows,
    #[error("Error: Invalid initialization arguments")]
    InvalidInitilizationArguments,
    #[error("Sqlx error: {0}")]
    SqlxError(#[from] sqlx::error::Error),
    #[error("Error during registration")]
    RegistrationError,
    #[error("Error ACK timeout")]
    AckTimeout,
}
