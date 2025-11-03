use chrono::NaiveDateTime;
use std::fmt::Debug;
use std::sync::Arc;
use uuid::Uuid;

use crate::connection::Connection;
use crate::database::DatabaseError;
use crate::database::postgres::PostgresConnectionArgs;
use crate::database::{DatabaseType, in_memory::InMemoryDB, postgres::PostgresDB};
use crate::device::Device;

#[derive(Debug, Clone)]
pub struct Database {
    pub engine: Arc<dyn Engine>,
}

impl Database {
    pub async fn new(
        database_type: DatabaseType,
        postgres_args: Option<PostgresConnectionArgs>,
    ) -> Result<Self, DatabaseError> {
        match database_type {
            DatabaseType::InMemory => Ok(Self {
                engine: Arc::new(InMemoryDB::default()),
            }),
            DatabaseType::Postgres => {
                let args = postgres_args.ok_or(DatabaseError::InvalidInitilizationArguments)?;

                Ok(Self {
                    engine: Arc::new(PostgresDB::new(args).await?),
                })
            }
        }
    }

    // Device
    pub async fn add_device(&self, device: &Device) -> Result<(), DatabaseError> {
        self.engine.add_device(device).await
    }

    pub async fn get_device(&self, device_id: Uuid) -> Result<Device, DatabaseError> {
        self.engine.get_device(device_id).await
    }

    pub async fn modify_device(&self, device: &Device) -> Result<(), DatabaseError> {
        self.engine.modify_device(device).await
    }

    // Device registration
    pub async fn register_device(
        &self,
        device_id: Uuid,
        timestamp: NaiveDateTime,
    ) -> Result<(), DatabaseError> {
        self.engine.register_device(device_id, timestamp).await
    }

    pub async fn registration_ack(
        &self,
        device_id: Uuid,
        timestamp: NaiveDateTime,
    ) -> Result<(), DatabaseError> {
        self.engine.registration_ack(device_id, timestamp).await
    }

    pub async fn registration_timeout(
        &self,
        device_id: Uuid,
        timestamp: NaiveDateTime,
    ) -> Result<bool, DatabaseError> {
        self.engine.registration_timeout(device_id, timestamp).await
    }

    // buckets
    pub async fn get_bucket_with_less_devices(
        &self,
        total_buckets: i32,
    ) -> Result<i32, DatabaseError> {
        self.engine
            .get_bucket_with_less_devices(total_buckets)
            .await
    }

    pub async fn add_device_to_bucket(
        &self,
        device_id: Uuid,
        bucket_number: i32,
    ) -> Result<(), DatabaseError> {
        self.engine
            .add_device_to_bucket(device_id, bucket_number)
            .await
    }

    pub async fn get_bucket_number(&self, device_id: Uuid) -> Result<i32, DatabaseError> {
        self.engine.get_bucket_number(device_id).await
    }

    pub async fn remove_device_from_bucket(&self, device_id: Uuid) -> Result<(), DatabaseError> {
        self.engine.remove_device_from_bucket(device_id).await
    }

    // schedule connection
    pub async fn schedule_connection(
        &self,
        device_id: Uuid,
        timestamp: NaiveDateTime,
    ) -> Result<(), DatabaseError> {
        self.engine.schedule_connection(device_id, timestamp).await
    }

    // Others
    pub async fn get_active_connections(&self) -> Result<Vec<Connection>, DatabaseError> {
        self.engine.get_active_connections().await
    }

    pub async fn add_new_connection(&self, connection: &Connection) -> Result<(), DatabaseError> {
        self.engine.add_new_connection(connection).await
    }

    pub async fn get_connection_data(&self, device_id: Uuid) -> Result<Connection, DatabaseError> {
        self.engine.get_connection_data(device_id).await
    }

    pub async fn update_connection(&self, connection: &Connection) -> Result<(), DatabaseError> {
        self.engine.update_connection(connection).await
    }
}

#[async_trait::async_trait]
pub trait Engine: Debug + Send + Sync {
    async fn get_active_connections(&self) -> Result<Vec<Connection>, DatabaseError>;
    async fn add_new_connection(&self, connection: &Connection) -> Result<(), DatabaseError>;
    async fn get_connection_data(&self, device_id: Uuid) -> Result<Connection, DatabaseError>;
    async fn update_connection(&self, connection: &Connection) -> Result<(), DatabaseError>;

    // Device
    async fn add_device(&self, device: &Device) -> Result<(), DatabaseError>;
    async fn get_device(&self, device_id: Uuid) -> Result<Device, DatabaseError>;
    async fn modify_device(&self, device: &Device) -> Result<(), DatabaseError>;

    // Device registration
    async fn register_device(
        &self,
        device_id: Uuid,
        timestamp: NaiveDateTime,
    ) -> Result<(), DatabaseError>;
    async fn registration_ack(
        &self,
        device_id: Uuid,
        timestamp: NaiveDateTime,
    ) -> Result<(), DatabaseError>;
    /// This fn checks if the registration has been completed,
    /// if not returns true and updates the db status to timeout
    async fn registration_timeout(
        &self,
        device_id: Uuid,
        timestamp: NaiveDateTime,
    ) -> Result<bool, DatabaseError>;

    // buckets
    async fn get_bucket_with_less_devices(&self, total_buckets: i32) -> Result<i32, DatabaseError>;
    async fn add_device_to_bucket(
        &self,
        device_id: Uuid,
        bucket_number: i32,
    ) -> Result<(), DatabaseError>;
    async fn get_bucket_number(&self, device_id: Uuid) -> Result<i32, DatabaseError>;
    async fn remove_device_from_bucket(&self, device_id: Uuid) -> Result<(), DatabaseError>;

    // schedule connection
    async fn schedule_connection(
        &self,
        device_id: Uuid,
        timestamp: NaiveDateTime,
    ) -> Result<(), DatabaseError>;
}
