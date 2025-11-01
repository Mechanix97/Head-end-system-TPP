use crate::connection::{Connection, ConnectionStatus};
use crate::database::DatabaseError;
use crate::database::api::Engine;
use crate::device::Device;

use std::collections::HashMap;
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
    devices: HashMap<Uuid, Device>,
}

#[async_trait::async_trait]
impl Engine for InMemoryDB {
    // Device fns
    async fn add_device(&self, device: &Device) -> Result<(), DatabaseError> {
        self.inner
            .lock()
            .await
            .devices
            .insert(device.id, device.clone());
        Ok(())
    }

    async fn get_device(&self, device_id: Uuid) -> Result<Device, DatabaseError> {
        self.inner
            .lock()
            .await
            .devices
            .get(&device_id)
            .cloned()
            .ok_or(DatabaseError::NoDataFound)
    }

    async fn modify_device(&self, device: &Device) -> Result<(), DatabaseError> {
        let mut lock = self.inner.lock().await;

        let element = lock
            .devices
            .get_mut(&device.id)
            .ok_or(DatabaseError::NoDataFound)?;

        *element = device.clone();

        Ok(())
    }

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

#[cfg(test)]
mod test {
    use crate::database::DatabaseType;
    use crate::database::api::Database;
    use crate::device::Device;

    use std::net::{IpAddr, Ipv4Addr, SocketAddr};

    // use super::*;
    #[tokio::test]
    async fn test_devices() {
        let db = Database::new(DatabaseType::InMemory, None).await.unwrap();

        let socket = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 8080);

        let mut device: Device = Device::new(socket, None, None, None);

        db.add_device(&device).await.unwrap();

        let device2 = db.get_device(device.id).await.unwrap();

        assert_eq!(device, device2);

        device.batch_id = Some(123);

        db.modify_device(&device).await.unwrap();

        let device3 = db.get_device(device.id).await.unwrap();

        assert_eq!(device, device3);
    }
}
