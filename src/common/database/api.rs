use chrono::NaiveDateTime;
use std::fmt::Debug;
use std::sync::Arc;
use uuid::Uuid;

use crate::database::DatabaseError;
use crate::database::postgres::PostgresConnectionArgs;
use crate::database::{DatabaseConfig, DatabaseType, in_memory::InMemoryDB, postgres::PostgresDB};
use crate::device::Device;
use crate::registration_status::DeviceRegistration;
use crate::scheduled_connection::ScheduledConnection;

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
    /// Creates a new database instance from a [`DatabaseConfig`].
    ///
    /// For `InMemory`, the postgres fields are ignored.
    /// For `Postgres`, opens a connection pool using the provided credentials.
    pub async fn new(config: DatabaseConfig) -> Result<Self, DatabaseError> {
        match config.db_type {
            DatabaseType::InMemory => Ok(Self {
                engine: Arc::new(InMemoryDB::default()),
            }),
            DatabaseType::Postgres => {
                let args = PostgresConnectionArgs {
                    user: config.user,
                    password: config.password,
                    url: config.url,
                    port: config.port,
                };
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
        node_id: Uuid,
    ) -> Result<(), DatabaseError> {
        self.engine
            .add_device_to_bucket(device_id, bucket_number, node_id)
            .await
    }

    /// Gets the bucket number assigned to a device.
    pub async fn get_bucket_number(&self, device_id: Uuid) -> Result<i32, DatabaseError> {
        self.engine.get_bucket_number(device_id).await
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
    ) -> Result<Vec<ScheduledConnection>, DatabaseError> {
        self.engine.get_scheduled_connections().await
    }

    /// Retrieves a single scheduled connection by device ID.
    pub async fn get_scheduled_connection(
        &self,
        device_id: Uuid,
    ) -> Result<ScheduledConnection, DatabaseError> {
        self.engine.get_scheduled_connection(device_id).await
    }

    /// Updates an existing scheduled connection with new information.
    pub async fn update_scheduled_connection(
        &self,
        connection: &ScheduledConnection,
    ) -> Result<(), DatabaseError> {
        self.engine.update_scheduled_connection(connection).await
    }

    // ========== Cluster management ==========

    /// Registers a new node in the cluster.
    pub async fn register_cluster_node(
        &self,
        node_id: Uuid,
        node_name: String,
        cluster_ip: String,
        cluster_port: i32,
        backdoor_port: i32,
    ) -> Result<(), DatabaseError> {
        self.engine
            .register_cluster_node(node_id, node_name, cluster_ip, cluster_port, backdoor_port)
            .await
    }

    /// Gets all active nodes in the cluster.
    pub async fn get_active_cluster_nodes(&self) -> Result<Vec<(Uuid, String, String, i32, i32)>, DatabaseError> {
        self.engine.get_active_cluster_nodes().await
    }

    /// Updates a cluster node's status.
    pub async fn update_cluster_node_status(&self, node_id: Uuid, status: &str) -> Result<(), DatabaseError> {
        self.engine.update_cluster_node_status(node_id, status).await
    }

    /// Updates a cluster node's last_seen timestamp.
    pub async fn update_cluster_node_last_seen(&self, node_id: Uuid) -> Result<(), DatabaseError> {
        self.engine.update_cluster_node_last_seen(node_id).await
    }

    // ========== Device ownership (for cluster delegation) ==========

    /// Gets all device UUIDs owned by a specific node.
    pub async fn get_devices_by_owner(&self, node_id: Uuid) -> Result<Vec<Uuid>, DatabaseError> {
        self.engine.get_devices_by_owner(node_id).await
    }

    /// Sets the owner node for a device.
    pub async fn set_device_owner(&self, device_id: Uuid, node_id: Uuid) -> Result<(), DatabaseError> {
        self.engine.set_device_owner(device_id, node_id).await
    }

    // ========== Device queries ==========

    /// Returns a list of all device UUIDs in the database.
    pub async fn get_all_device_ids(&self) -> Result<Vec<Uuid>, DatabaseError> {
        self.engine.get_all_device_ids().await
    }

    // ========== Scheduler queries ==========

    /// Returns upcoming connections (Awaiting), sorted by schedule_time ASC, paginated.
    /// Pass `device_id = Some(uuid)` to filter to a single device.
    pub async fn get_upcoming_connections(
        &self,
        limit: i64,
        offset: i64,
        device_id: Option<Uuid>,
    ) -> Result<Vec<ScheduledConnection>, DatabaseError> {
        self.engine.get_upcoming_connections(limit, offset, device_id).await
    }

    /// Returns past connections (Done or Lost), sorted by schedule_time DESC, paginated.
    /// Pass `device_id = Some(uuid)` to filter to a single device.
    pub async fn get_connection_history(
        &self,
        limit: i64,
        offset: i64,
        device_id: Option<Uuid>,
    ) -> Result<Vec<ScheduledConnection>, DatabaseError> {
        self.engine
            .get_connection_history(limit, offset, device_id)
            .await
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
        node_id: Uuid,
    ) -> Result<(), DatabaseError>;
    async fn get_bucket_number(&self, device_id: Uuid) -> Result<i32, DatabaseError>;

    // ========== Connection scheduling ==========
    async fn schedule_connection(
        &self,
        device_id: Uuid,
        timestamp: NaiveDateTime,
        job_id: Uuid,
    ) -> Result<(), DatabaseError>;
    async fn get_scheduled_connections(&self) -> Result<Vec<ScheduledConnection>, DatabaseError>;
    async fn get_scheduled_connection(
        &self,
        device_id: Uuid,
    ) -> Result<ScheduledConnection, DatabaseError>;
    async fn update_scheduled_connection(
        &self,
        connection: &ScheduledConnection,
    ) -> Result<(), DatabaseError>;

    // ========== Cluster management ==========
    /// Registers a new node in the cluster
    async fn register_cluster_node(
        &self,
        node_id: Uuid,
        node_name: String,
        cluster_ip: String,
        cluster_port: i32,
        backdoor_port: i32,
    ) -> Result<(), DatabaseError>;

    /// Gets all active nodes in the cluster
    async fn get_active_cluster_nodes(&self) -> Result<Vec<(Uuid, String, String, i32, i32)>, DatabaseError>;

    /// Updates a cluster node's status
    async fn update_cluster_node_status(&self, node_id: Uuid, status: &str) -> Result<(), DatabaseError>;

    /// Updates a cluster node's last_seen timestamp
    async fn update_cluster_node_last_seen(&self, node_id: Uuid) -> Result<(), DatabaseError>;

    // ========== Device ownership (for cluster delegation) ==========

    /// Gets all device UUIDs owned by a specific node
    async fn get_devices_by_owner(&self, node_id: Uuid) -> Result<Vec<Uuid>, DatabaseError>;

    /// Sets the owner node for a device
    async fn set_device_owner(&self, device_id: Uuid, node_id: Uuid) -> Result<(), DatabaseError>;

    // ========== Device queries ==========
    async fn get_all_device_ids(&self) -> Result<Vec<Uuid>, DatabaseError>;

    // ========== Scheduler queries ==========

    /// Returns upcoming scheduled connections (status = Awaiting), sorted by schedule_time ASC.
    /// Optionally filtered to a single device.
    async fn get_upcoming_connections(
        &self,
        limit: i64,
        offset: i64,
        device_id: Option<Uuid>,
    ) -> Result<Vec<ScheduledConnection>, DatabaseError>;

    /// Returns past connections (status = Done or Lost), sorted by schedule_time DESC.
    /// Optionally filtered to a single device.
    async fn get_connection_history(
        &self,
        limit: i64,
        offset: i64,
        device_id: Option<Uuid>,
    ) -> Result<Vec<ScheduledConnection>, DatabaseError>;
}
