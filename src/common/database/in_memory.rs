use crate::connection::Connection;
use crate::database::DatabaseError;
use crate::database::api::Engine;

use std::sync::Arc;
use tokio::sync::Mutex;
use uuid::Uuid;

#[derive(Default, Debug)]
pub struct InMemoryDB {
    pub inner: Arc<Mutex<InnerDB>>,
}

#[derive(Default, Debug)]
pub struct InnerDB {
    pub active_connections: Vec<Connection>, //todo remove this pub
}

#[async_trait::async_trait]
impl Engine for InMemoryDB {
    async fn get_active_connections(&self) -> Vec<Connection> {
        self.inner.lock().await.active_connections.clone()
    }

    async fn add_new_connection(&self, connection: &Connection) -> Result<(), DatabaseError> {
        self.inner
            .lock()
            .await
            .active_connections
            .push(connection.clone());
        Ok(())
    }

    async fn get_connection_data(&self, device_id: Uuid) -> Result<Connection, DatabaseError> {
        let lock = self.inner.lock().await;

        let results: Vec<&Connection> = lock
            .active_connections
            .iter()
            .filter(|c| c.device_id == device_id)
            .collect();

        if results.len() < 1 {
            return Err(DatabaseError::NoDataFound);
        } else if results.len() > 1 {
            return Err(DatabaseError::TooManyRows);
        }

        Ok(results[0].clone())
    }
}
