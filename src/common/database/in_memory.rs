use chrono::NaiveDateTime;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;
use uuid::Uuid;

use crate::database::DatabaseError;
use crate::database::api::Engine;
use crate::device::Device;
use crate::registration_status::DeviceRegistration;
use crate::registration_status::RegistrationStatus;
use crate::scheduled_connection::{ScheduledConnection, ScheduledStatus};

#[derive(Default, Debug)]
pub struct InMemoryDB {
    pub inner: Arc<Mutex<InnerDB>>,
}

#[derive(Default, Debug)]
pub struct InnerDB {
    devices: HashMap<Uuid, Device>,
    device_registration: HashMap<Uuid, (RegistrationStatus, NaiveDateTime)>,
    buckets: HashMap<Uuid, (i32, Uuid)>, // device_id -> (bucket_number, node_id)
    scheduled_connections: HashMap<Uuid, ScheduledConnection>,
}

#[async_trait::async_trait]
impl Engine for InMemoryDB {
    // Device
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

    // Device registration
    async fn register_device(
        &self,
        device_id: Uuid,
        timestamp: NaiveDateTime,
    ) -> Result<(), DatabaseError> {
        self.inner
            .lock()
            .await
            .device_registration
            .insert(device_id, (RegistrationStatus::PendingAck, timestamp));

        Ok(())
    }

    async fn registration_timeout(
        &self,
        device_id: Uuid,
        timestamp: NaiveDateTime,
    ) -> Result<bool, DatabaseError> {
        let mut lock = self.inner.lock().await;

        let element = lock
            .device_registration
            .get_mut(&device_id)
            .ok_or(DatabaseError::NoDataFound)?;
        if element.0 == RegistrationStatus::PendingAck {
            *element = (RegistrationStatus::AckTimeout, timestamp);
            return Ok(true);
        }

        Ok(false)
    }

    // buckets
    async fn get_bucket_with_less_devices(&self, total_buckets: i32) -> Result<i32, DatabaseError> {
        let inner = self.inner.lock().await;

        let mut counts = vec![0usize; total_buckets as usize];

        for &(bucket, _) in inner.buckets.values() {
            let bucket_usize = bucket as usize;
            if bucket >= 0 && bucket_usize < counts.len() {
                counts[bucket_usize] += 1;
            }
        }

        let min_count = *counts.iter().min().unwrap_or(&0);

        let bucket = counts.iter().position(|&c| c == min_count).unwrap_or(0) as i32;
        Ok(bucket)
    }

    async fn add_device_to_bucket(
        &self,
        device_id: Uuid,
        bucket_number: i32,
        node_id: Uuid,
    ) -> Result<(), DatabaseError> {
        self.inner
            .lock()
            .await
            .buckets
            .insert(device_id, (bucket_number, node_id));
        Ok(())
    }

    async fn get_bucket_number(&self, device_id: Uuid) -> Result<i32, DatabaseError> {
        self.inner
            .lock()
            .await
            .buckets
            .get(&device_id)
            .map(|&(bucket, _)| bucket)
            .ok_or(DatabaseError::NoDataFound)
    }

    // schedule connection
    async fn schedule_connection(
        &self,
        device_id: Uuid,
        timestamp: NaiveDateTime,
        job_id: Uuid,
    ) -> Result<(), DatabaseError> {
        let mut lock = self.inner.lock().await;
        lock.scheduled_connections.remove(&device_id);
        lock.scheduled_connections.insert(
            device_id,
            ScheduledConnection {
                fk_device: device_id,
                schedule_time: timestamp,
                connection_time: None,
                status: ScheduledStatus::Awaiting,
                job_id: Some(job_id),
                renewable: true,
            },
        );
        Ok(())
    }

    async fn get_scheduled_connections(&self) -> Result<Vec<ScheduledConnection>, DatabaseError> {
        Ok(self
            .inner
            .lock()
            .await
            .scheduled_connections
            .values()
            .cloned()
            .collect())
    }

    async fn get_scheduled_connection(
        &self,
        device_id: Uuid,
    ) -> Result<ScheduledConnection, DatabaseError> {
        let lock = self.inner.lock().await;
        lock.scheduled_connections
            .get(&device_id)
            .cloned()
            .ok_or(DatabaseError::NoDataFound)
    }

    async fn update_scheduled_connection(
        &self,
        connection: &ScheduledConnection,
    ) -> Result<(), DatabaseError> {
        let mut lock = self.inner.lock().await;
        lock.scheduled_connections
            .insert(connection.fk_device, connection.clone());
        Ok(())
    }

    async fn get_device_registration(
        &self,
        device_id: Uuid,
    ) -> Result<DeviceRegistration, DatabaseError> {
        let lock = self.inner.lock().await;

        let (status, time) = lock
            .device_registration
            .get(&device_id)
            .cloned()
            .ok_or(DatabaseError::NoDataFound)?;

        Ok(DeviceRegistration {
            fk_device: device_id,
            registration_status: status,
            registration_time: time,
        })
    }

    async fn update_device_registration(
        &self,
        device_id: Uuid,
        status: Option<RegistrationStatus>,
        timestamp: Option<NaiveDateTime>,
    ) -> Result<(), DatabaseError> {
        let mut lock = self.inner.lock().await;

        let element = lock
            .device_registration
            .get_mut(&device_id)
            .ok_or(DatabaseError::NoDataFound)?;

        if let Some(new_status) = status {
            element.0 = new_status;
        }

        if let Some(new_time) = timestamp {
            element.1 = new_time;
        }

        Ok(())
    }

    // ========== Cluster management ==========

    async fn register_cluster_node(
        &self,
        _node_id: Uuid,
        _node_name: String,
        _cluster_ip: String,
        _cluster_port: i32,
        _backdoor_port: i32,
    ) -> Result<(), DatabaseError> {
        Err(DatabaseError::QueryError(
            "Cluster operations are not supported in in-memory database. Use PostgreSQL for cluster mode.".to_string()
        ))
    }

    async fn get_active_cluster_nodes(&self) -> Result<Vec<(Uuid, String, String, i32, i32)>, DatabaseError> {
        Err(DatabaseError::QueryError(
            "Cluster operations are not supported in in-memory database. Use PostgreSQL for cluster mode.".to_string()
        ))
    }

    async fn update_cluster_node_status(&self, _node_id: Uuid, _status: &str) -> Result<(), DatabaseError> {
        Err(DatabaseError::QueryError(
            "Cluster operations are not supported in in-memory database. Use PostgreSQL for cluster mode.".to_string()
        ))
    }

    async fn update_cluster_node_last_seen(&self, _node_id: Uuid) -> Result<(), DatabaseError> {
        Err(DatabaseError::QueryError(
            "Cluster operations are not supported in in-memory database. Use PostgreSQL for cluster mode.".to_string()
        ))
    }

        // ========== Device ownership ==========

    async fn get_devices_by_owner(&self, node_id: Uuid) -> Result<Vec<Uuid>, DatabaseError> {
        let lock = self.inner.lock().await;
        Ok(lock
            .buckets
            .iter()
            .filter(|(_, (_, owner))| *owner == node_id)
            .map(|(device_id, _)| *device_id)
            .collect())
    }

    async fn set_device_owner(&self, device_id: Uuid, node_id: Uuid) -> Result<(), DatabaseError> {
        let mut lock = self.inner.lock().await;
        if let Some(entry) = lock.buckets.get_mut(&device_id) {
            entry.1 = node_id;
        }
        Ok(())
    }

    async fn get_all_device_ids(&self) -> Result<Vec<Uuid>, DatabaseError> {
        let inner = self.inner.lock().await;
        let device_ids: Vec<Uuid> = inner.devices.keys().copied().collect();
        Ok(device_ids)
    }
}

#[cfg(test)]
mod test {
    use crate::database::DatabaseType;
    use crate::database::api::Database;
    use crate::device::Device;

    use std::net::{IpAddr, Ipv4Addr, SocketAddr};

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
