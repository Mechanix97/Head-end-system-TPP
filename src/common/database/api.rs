use std::sync::Arc;

use crate::database::{DatabaseType, in_memory::InMemoryDB, postgres::PostgresDB};

pub struct Database {
    pub engine: Arc<dyn Engine>,
}

impl Database {
    pub fn new(database_type: DatabaseType) -> Self {
        match database_type {
            DatabaseType::InMemory => Self {
                engine: Arc::new(InMemoryDB {}),
            },
            DatabaseType::Postgres => Self {
                engine: Arc::new(PostgresDB {}),
            },
        }
    }
}

pub trait Engine {}
