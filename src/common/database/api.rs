use chrono::NaiveDateTime;
use std::fmt::Debug;
use std::sync::Arc;
use uuid::Uuid;

use crate::database::DatabaseError;
use crate::database::postgres::PostgresConnectionArgs;
use crate::database::{DatabaseType, in_memory::InMemoryDB, postgres::PostgresDB};
use crate::device::Device;
use crate::registration_status::DeviceRegistration;

/// Database abstraction layer that supports multiple backend implementations.
///
/// This provides a unified interface for database operations regardless of whether
/// we're using PostgreSQL (production) or an in-memory database (testing).
///
/// The actual database logic is delegated to an implementation of the `Engine` trait,
/// which is stored as an `Arc<dyn Engine>` for thread-safe sharing across async tasks.
#[derive(Debug, Clone)]
pub struct Database {
    pub engine: Arc<dyn Engine>,
}

impl Database {
    /// Creates a new database instance with the specified backend.
    ///
    /// For `DatabaseType::InMemory`, creates an in-memory database (useful for testing).
    /// For `DatabaseType::Postgres`, connects to PostgreSQL using the provided connection args.
    ///
    /// Returns an error if Postgres is selected but no connection args are provided.
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

    // ========== Device management ==========

    /// Adds a new device to the database.
    pub async fn add_device(&self, device: &Device) -> Result<(), DatabaseError> {
        self.engine.add_device(device).await
    }

    /// Retrieves device information by UUID.
    pub async fn get_device(&self, device_id: Uuid) -> Result<Device, DatabaseError> {
        self.engine.get_device(device_id).await
    }

    /// Updates an existing device's information.
    pub async fn modify_device(&self, device: &Device) -> Result<(), DatabaseError> {
        self.engine.modify_device(device).await
    }

    // ========== Device registration flow ==========

    /// Registers a new device in the system.
    ///
    /// This is the first step when a device connects to the backdoor server.
    pub async fn register_device(
        &self,
        device_id: Uuid,
        timestamp: NaiveDateTime,
    ) -> Result<(), DatabaseError> {
        self.engine.register_device(device_id, timestamp).await
    }

    /// Checks if registration has timed out and updates status accordingly.
    ///
    /// Returns `true` if the registration timed out (no ACK received),
    /// `false` if it completed successfully.
    pub async fn registration_timeout(
        &self,
        device_id: Uuid,
        timestamp: NaiveDateTime,
    ) -> Result<bool, DatabaseError> {
        self.engine.registration_timeout(device_id, timestamp).await
    }

    /// Retrieves the registration status and details for a specific device.
    ///
    /// Returns the complete DeviceRegistration struct containing:
    /// - fk_device: The device UUID
    /// - registration_status: Current registration state (Registered, PendingAck, AckTimeout)
    /// - registration_time: Timestamp of when registration started
    pub async fn get_device_registration(
        &self,
        device_id: Uuid,
    ) -> Result<DeviceRegistration, DatabaseError> {
        self.engine.get_device_registration(device_id).await
    }

    /// Updates the registration status and/or timestamp for a device.
    ///
    /// Allows partial updates - you can update just the status, just the timestamp,
    /// or both depending on your use case.
    pub async fn update_device_registration(
        &self,
        device_id: Uuid,
        status: Option<crate::registration_status::RegistrationStatus>,
        timestamp: Option<NaiveDateTime>,
    ) -> Result<(), DatabaseError> {
        self.engine
            .update_device_registration(device_id, status, timestamp)
            .await
    }

    // ========== Time-bucket scheduling ==========

    /// Finds the bucket with the fewest devices for load balancing.
    ///
    /// The scheduler divides the 24-hour day into `total_buckets` time slots
    /// and distributes devices evenly across them to avoid network congestion.
    pub async fn get_bucket_with_less_devices(
        &self,
        total_buckets: i32,
    ) -> Result<i32, DatabaseError> {
        self.engine
            .get_bucket_with_less_devices(total_buckets)
            .await
    }

    /// Assigns a device to a specific time bucket.
    pub async fn add_device_to_bucket(
        &self,
        device_id: Uuid,
        bucket_number: i32,
    ) -> Result<(), DatabaseError> {
        self.engine
            .add_device_to_bucket(device_id, bucket_number)
            .await
    }

    /// Gets the bucket number assigned to a device.
    pub async fn get_bucket_number(&self, device_id: Uuid) -> Result<i32, DatabaseError> {
        self.engine.get_bucket_number(device_id).await
    }

    /// Removes a device from its assigned bucket.
    pub async fn remove_device_from_bucket(&self, device_id: Uuid) -> Result<(), DatabaseError> {
        self.engine.remove_device_from_bucket(device_id).await
    }

    // ========== Connection scheduling ==========

    /// Schedules a periodic connection for a device.
    ///
    /// The HES will connect to the device at the scheduled timestamp to collect
    /// consumption data. The job_id links to the tokio-cron-scheduler job.
    pub async fn schedule_connection(
        &self,
        device_id: Uuid,
        timestamp: NaiveDateTime,
        job_id: Uuid,
    ) -> Result<(), DatabaseError> {
        self.engine
            .schedule_connection(device_id, timestamp, job_id)
            .await
    }

    /// Retrieves all scheduled connections for state restoration on HES restart.
    pub async fn get_scheduled_connections(
        &self,
    ) -> Result<Vec<(Uuid, NaiveDateTime)>, DatabaseError> {
        self.engine.get_scheduled_connections().await
    }
}

/// Database engine trait that defines the interface for database operations.
///
/// This trait is implemented by both `PostgresDB` (production) and `InMemoryDB` (testing).
/// Using a trait allows the rest of the HES code to work with any database backend
/// without needing to know which one is in use.
///
/// The `async_trait` macro is used because Rust doesn't natively support async methods
/// in traits yet. It works by transforming async trait methods into regular methods that
/// return `Pin<Box<dyn Future>>`.
#[async_trait::async_trait]
pub trait Engine: Debug + Send + Sync {
    // ========== Device management ==========
    async fn add_device(&self, device: &Device) -> Result<(), DatabaseError>;
    async fn get_device(&self, device_id: Uuid) -> Result<Device, DatabaseError>;
    async fn modify_device(&self, device: &Device) -> Result<(), DatabaseError>;

    // ========== Device registration flow ==========
    async fn register_device(
        &self,
        device_id: Uuid,
        timestamp: NaiveDateTime,
    ) -> Result<(), DatabaseError>;

    /// Checks if registration has timed out and updates status if needed.
    ///
    /// Returns `true` if the registration timed out (no ACK received).
    /// Returns `false` if registration completed successfully.
    /// If timed out, updates the database status to reflect the timeout.
    async fn registration_timeout(
        &self,
        device_id: Uuid,
        timestamp: NaiveDateTime,
    ) -> Result<bool, DatabaseError>;

    /// Retrieves the complete registration information for a device.
    async fn get_device_registration(
        &self,
        device_id: Uuid,
    ) -> Result<DeviceRegistration, DatabaseError>;

    /// Updates the registration status and/or timestamp for a device.
    ///
    /// Allows flexible updates:
    /// - If `status` is Some, updates the registration_status field
    /// - If `timestamp` is Some, updates the registration_time field
    /// - If both are Some, updates both fields
    async fn update_device_registration(
        &self,
        device_id: Uuid,
        status: Option<crate::registration_status::RegistrationStatus>,
        timestamp: Option<NaiveDateTime>,
    ) -> Result<(), DatabaseError>;

    // ========== Time-bucket scheduling ==========
    async fn get_bucket_with_less_devices(&self, total_buckets: i32) -> Result<i32, DatabaseError>;
    async fn add_device_to_bucket(
        &self,
        device_id: Uuid,
        bucket_number: i32,
    ) -> Result<(), DatabaseError>;
    async fn get_bucket_number(&self, device_id: Uuid) -> Result<i32, DatabaseError>;
    async fn remove_device_from_bucket(&self, device_id: Uuid) -> Result<(), DatabaseError>;

    // ========== Connection scheduling ==========
    async fn schedule_connection(
        &self,
        device_id: Uuid,
        timestamp: NaiveDateTime,
        job_id: Uuid,
    ) -> Result<(), DatabaseError>;
    async fn get_scheduled_connections(&self) -> Result<Vec<(Uuid, NaiveDateTime)>, DatabaseError>;
}
