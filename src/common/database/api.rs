use std::sync::Arc;

use crate::connection::Connection;
use crate::database::{DatabaseType, in_memory::InMemoryDB, postgres::PostgresDB};

pub struct Database {
    pub engine: Arc<dyn Engine>,
}

impl Database {
    pub async fn new(database_type: DatabaseType) -> Self {
        match database_type {
            DatabaseType::InMemory => Self {
                engine: Arc::new(InMemoryDB::default()),
            },
            DatabaseType::Postgres => Self {
                engine: Arc::new(PostgresDB::new().await),
            },
        }
    }

    pub fn get_active_connections(&self) -> Vec<Connection> {
        self.engine.get_active_connections()
    }
}

pub trait Engine {
    fn get_active_connections(&self) -> Vec<Connection>;
}
