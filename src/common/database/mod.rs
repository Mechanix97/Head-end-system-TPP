pub mod api;
pub mod in_memory;
pub mod postgres;

use clap::ValueEnum;

#[derive(Debug, Clone, PartialEq, Eq, Copy, ValueEnum)]
pub enum DatabaseType {
    InMemory,
    Postgres,
}
