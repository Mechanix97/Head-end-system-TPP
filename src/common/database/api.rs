use std::fmt::Debug;
use std::sync::Arc;

use crate::connection::Connection;
use crate::database::DatabaseError;
use crate::database::{DatabaseType, in_memory::InMemoryDB, postgres::PostgresDB};

#[derive(Debug, Clone)]
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

    pub async fn get_active_connections(&self) -> Vec<Connection> {
        self.engine.get_active_connections().await
    }

    pub async fn add_new_connection(&self, connection: Connection) -> Result<(), DatabaseError> {
        self.engine.add_new_connection(connection).await
    }
}

#[async_trait::async_trait]
pub trait Engine: Debug + Send + Sync {
    async fn get_active_connections(&self) -> Vec<Connection>;
    async fn add_new_connection(&self, connection: Connection) -> Result<(), DatabaseError>;
}
