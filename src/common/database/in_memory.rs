use crate::connection::Connection;
use crate::database::DatabaseError;
use crate::database::api::Engine;

use std::sync::Arc;
use tokio::sync::Mutex;

#[derive(Default)]
pub struct InMemoryDB {
    pub inner: Arc<Mutex<InnerDB>>,
}

#[derive(Default)]
pub struct InnerDB {
    pub active_connections: Vec<Connection>, //todo remove this pub
}

#[async_trait::async_trait]
impl Engine for InMemoryDB {
    async fn get_active_connections(&self) -> Vec<Connection> {
        self.inner.lock().await.active_connections.clone()
    }

    async fn add_new_connection(&self, connection: Connection) -> Result<(), DatabaseError> {
        Ok(())
    }
}
