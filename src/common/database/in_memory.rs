use crate::connection::{Connection, ConnectionStatus};
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
    connections: Vec<Connection>,
}

#[async_trait::async_trait]
impl Engine for InMemoryDB {
    async fn get_active_connections(&self) -> Result<Vec<Connection>, DatabaseError> {
        Ok(self
            .inner
            .lock()
            .await
            .connections
            .iter()
            .filter(|&c| c.status == ConnectionStatus::Active)
            .cloned()
            .collect())
    }

    async fn add_new_connection(&self, connection: &Connection) -> Result<(), DatabaseError> {
        self.inner.lock().await.connections.push(connection.clone());
        Ok(())
    }

    async fn get_connection_data(&self, device_id: Uuid) -> Result<Connection, DatabaseError> {
        let lock = self.inner.lock().await;

        let results: Vec<&Connection> = lock
            .connections
            .iter()
            .filter(|c| c.device_id == device_id)
            .collect();

        if results.is_empty() {
            return Err(DatabaseError::NoDataFound);
        } else if results.len() > 1 {
            return Err(DatabaseError::TooManyRows);
        }

        Ok(results[0].clone())
    }

    async fn update_connection(&self, connection: &Connection) -> Result<(), DatabaseError> {
        let mut guard = self.inner.lock().await;

        if let Some(pos) = guard
            .connections
            .iter()
            .position(|c| c.device_id == connection.device_id)
        {
            guard.connections[pos] = connection.clone();
        } else {
            return Err(DatabaseError::NoDataFound);
        }

        Ok(())
    }
}
