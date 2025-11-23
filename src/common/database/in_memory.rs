use chrono::NaiveDateTime;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;
use uuid::Uuid;

use crate::database::DatabaseError;
use crate::database::api::Engine;
use crate::device::Device;
use crate::device::RegistrationStatus;

#[derive(Default, Debug)]
pub struct InMemoryDB {
    pub inner: Arc<Mutex<InnerDB>>,
}

#[derive(Default, Debug)]
pub struct InnerDB {
    devices: HashMap<Uuid, Device>,
    device_registration: HashMap<Uuid, (RegistrationStatus, NaiveDateTime)>,
    buckets: HashMap<Uuid, i32>,
    scheduled_connections: HashMap<Uuid, (NaiveDateTime, Uuid)>,
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

    async fn registration_ack(
        &self,
        device_id: Uuid,
        timestamp: NaiveDateTime,
    ) -> Result<f64, DatabaseError> {
        let mut lock = self.inner.lock().await;

        let element = lock
            .device_registration
            .get_mut(&device_id)
            .ok_or(DatabaseError::NoDataFound)?;
        if element.0 != RegistrationStatus::PendingAck {
            return Err(DatabaseError::RegistrationError);
        }

        // Calculate duration in seconds before updating
        let registration_time = element.1;
        let duration = timestamp.signed_duration_since(registration_time);
        let duration_seconds = duration.num_milliseconds() as f64 / 1000.0;

        *element = (RegistrationStatus::Registered, timestamp);
        Ok(duration_seconds)
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

        for &bucket in inner.buckets.values() {
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
    ) -> Result<(), DatabaseError> {
        self.inner
            .lock()
            .await
            .buckets
            .insert(device_id, bucket_number);
        Ok(())
    }

    async fn get_bucket_number(&self, device_id: Uuid) -> Result<i32, DatabaseError> {
        self.inner
            .lock()
            .await
            .buckets
            .get(&device_id)
            .cloned()
            .ok_or(DatabaseError::NoDataFound)
    }

    async fn remove_device_from_bucket(&self, device_id: Uuid) -> Result<(), DatabaseError> {
        self.inner.lock().await.buckets.remove(&device_id);
        Ok(())
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
        lock.scheduled_connections
            .insert(device_id, (timestamp, job_id));
        Ok(())
    }

    async fn get_scheduled_connections(&self) -> Result<Vec<(Uuid, NaiveDateTime)>, DatabaseError> {
        Ok(self
            .inner
            .lock()
            .await
            .scheduled_connections
            .iter()
            .map(|(&device_id, &(time, _job_id))| (device_id, time))
            .collect())
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
