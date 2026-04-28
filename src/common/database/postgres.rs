use std::collections::HashMap;

use chrono::NaiveDateTime;
use sqlx::Pool;
use sqlx::Postgres;
use sqlx::Row;
use sqlx::postgres::PgPoolOptions;
use sqlx::query_as;
use sqlx::query_scalar;
use tracing::error;
use uuid::Uuid;

use crate::database::DatabaseError;
use crate::database::api::Engine;
use crate::device::Device;
use crate::registration_status::DeviceRegistration;
use crate::registration_status::RegistrationStatus;
use crate::scheduled_connection::ScheduledConnection;

fn map_scheduled_row(row: &sqlx::postgres::PgRow) -> Result<ScheduledConnection, DatabaseError> {
    let fk_device: Uuid = row
        .try_get("fk_device")
        .map_err(|e| DatabaseError::QueryError(e.to_string()))?;
    let schedule_time: NaiveDateTime = row
        .try_get("schedule_time")
        .map_err(|e| DatabaseError::QueryError(e.to_string()))?;
    let connection_time: Option<NaiveDateTime> = row
        .try_get("connection_time")
        .map_err(|e| DatabaseError::QueryError(e.to_string()))?;
    let status = row
        .try_get("status")
        .map_err(|e| DatabaseError::QueryError(e.to_string()))?;
    let job_id: Option<Uuid> = row
        .try_get("job_id")
        .map_err(|e| DatabaseError::QueryError(e.to_string()))?;
    let renewable: bool = row
        .try_get("renewable")
        .map_err(|e| DatabaseError::QueryError(e.to_string()))?;
    Ok(ScheduledConnection {
        fk_device,
        schedule_time,
        connection_time,
        status,
        job_id,
        renewable,
    })
}

/// PostgreSQL database implementation of the `Engine` trait.
///
/// Uses sqlx for async database operations with connection pooling.
/// The database schema is defined in `init-db.sql` with tables for devices,
/// registrations, buckets, and active connections.
#[derive(Debug)]
pub struct PostgresDB {
    pool: Pool<Postgres>,
}

/// Connection parameters for PostgreSQL database.
///
/// Used to build the connection string: `postgres://user:password@url:port/hes`
pub struct PostgresConnectionArgs {
    pub user: String,
    pub password: String,
    pub url: String,
    pub port: String,
}

impl PostgresDB {
    /// Creates a new PostgreSQL database connection with a connection pool.
    ///
    /// The pool is configured with a maximum of 5 concurrent connections.
    /// Connects to the `hes` database on the PostgreSQL server.
    ///
    /// # Errors
    /// Returns an error if the database connection fails (wrong credentials, server down, etc.)
    pub async fn new(args: PostgresConnectionArgs) -> Result<Self, DatabaseError> {
        Ok(Self {
            pool: PgPoolOptions::new()
                // TODO: Make max_connections configurable instead of hardcoding 5
                .max_connections(5)
                .connect(
                    format!(
                        "postgres://{}:{}@{}:{}/hes",
                        args.user, args.password, args.url, args.port
                    )
                    .as_str(),
                )
                .await?,
        })
    }
}

#[async_trait::async_trait]
impl Engine for PostgresDB {
    // Device
    async fn add_device(&self, device: &Device) -> Result<(), DatabaseError> {
        let query = "INSERT INTO T_DEVICES (id, IPv4, IPv6, imei) VALUES ($1, $2, $3, $4)";

        sqlx::query(query)
            .bind(device.id)
            .bind(device.ipv4.clone())
            .bind(device.ipv6.clone())
            .bind(device.imei.clone())
            .execute(&self.pool)
            .await
            .map_err(|e| DatabaseError::QueryError(e.to_string()))?;

        Ok(())
    }

    async fn get_device(&self, device_id: Uuid) -> Result<Device, DatabaseError> {
        // TODO: Optimize this - we're doing COUNT + SELECT which is inefficient.
        // Instead, we could just try to fetch the device and handle the error cases.
        let query_count = r#"
            SELECT COUNT(*)
            FROM T_DEVICES
            WHERE id = $1
        "#;

        let count: i64 = query_scalar(query_count)
            .bind(device_id)
            .fetch_one(&self.pool)
            .await
            .map_err(|e| {
                error!("Error counting connections for device {}: {}", device_id, e);
                DatabaseError::QueryError(e.to_string())
            })?;

        if count < 1 {
            return Err(DatabaseError::NoDataFound);
        } else if count > 1 {
            return Err(DatabaseError::TooManyRows);
        }

        let query = r#"
            SELECT id, IPv4, IPv6, imei
            FROM T_DEVICES
            WHERE id = $1
        "#;

        let device = query_as::<_, Device>(query)
            .bind(device_id)
            .fetch_one(&self.pool)
            .await
            .map_err(|e| {
                error!(
                    "Error fetching connection data for device {}: {}",
                    device_id, e
                );
                DatabaseError::QueryError(e.to_string())
            })?;

        Ok(device)
    }

    async fn modify_device(&self, device: &Device) -> Result<(), DatabaseError> {
        let query = "UPDATE T_DEVICES SET IPv4 = $2, IPv6 = $3, imei = $4 WHERE id = $1";

        sqlx::query(query)
            .bind(device.id)
            .bind(device.ipv4.clone())
            .bind(device.ipv6.clone())
            .bind(device.imei.clone())
            .execute(&self.pool)
            .await
            .map_err(|e| DatabaseError::QueryError(e.to_string()))?;

        Ok(())
    }

    // Device registration
    async fn register_device(
        &self,
        device_id: Uuid,
        timestamp: NaiveDateTime,
    ) -> Result<(), DatabaseError> {
        let query = "DELETE FROM T_DEVICE_REGISTRATION WHERE FK_DEVICE = $1";

        sqlx::query(query)
            .bind(device_id)
            .execute(&self.pool)
            .await
            .map_err(|e| DatabaseError::QueryError(e.to_string()))?;

        let query = "INSERT INTO T_DEVICE_REGISTRATION
                    (fk_device, registration_status, registration_time) 
                    VALUES ($1, $2, $3)";

        sqlx::query(query)
            .bind(device_id)
            .bind(RegistrationStatus::PendingAck)
            .bind(timestamp)
            .execute(&self.pool)
            .await
            .map_err(|e| DatabaseError::QueryError(e.to_string()))?;

        Ok(())
    }

    async fn registration_timeout(
        &self,
        device_id: Uuid,
        timestamp: NaiveDateTime,
    ) -> Result<bool, DatabaseError> {
        let query_count = r#"
            SELECT COUNT(*) 
            FROM T_DEVICE_REGISTRATION 
            WHERE fk_device = $1 
            AND registration_status = 'pending_ack'
        "#;

        let count: i64 = query_scalar(query_count)
            .bind(device_id)
            .fetch_one(&self.pool)
            .await
            .map_err(|e| {
                error!("Error counting connections for device {}: {}", device_id, e);
                DatabaseError::QueryError(e.to_string())
            })?;

        if count < 1 {
            return Ok(false);
        } else if count > 1 {
            return Err(DatabaseError::TooManyRows);
        }

        let query = r#"UPDATE T_DEVICE_REGISTRATION 
        SET registration_status = 'ack_timeout', 
        registration_time = $2
         WHERE fk_device = $1 
            AND registration_status = 'pending_ack'"#;

        sqlx::query(query)
            .bind(device_id)
            .bind(timestamp)
            .execute(&self.pool)
            .await
            .map_err(|e| DatabaseError::QueryError(e.to_string()))?;

        Ok(true)
    }

    // ========== Bucket management ==========

    /// Finds the bucket with the fewest assigned devices for load balancing.
    ///
    /// This implements the time-bucket load balancing algorithm:
    /// 1. Query the database to count devices in each bucket
    /// 2. Find the bucket with the minimum device count
    /// 3. If multiple buckets have the same minimum count, pick the lowest-numbered one
    /// 4. If no buckets exist yet, return bucket 0
    ///
    /// The goal is to evenly distribute devices across time slots throughout the day
    /// to avoid network congestion when many devices wake up simultaneously.
    async fn get_bucket_with_less_devices(&self, total_buckets: i32) -> Result<i32, DatabaseError> {
        let query = r#"
            SELECT bucket, COUNT(*) as count FROM T_BUCKETS GROUP BY bucket
        "#;

        let rows = sqlx::query(query)
            .fetch_all(&self.pool)
            .await
            .map_err(|e| DatabaseError::QueryError(e.to_string()))?;

        // Build a map of bucket number → device count
        let bucket_counts: HashMap<i32, i64> = rows
            .into_iter()
            .map(|row| {
                let bucket: i32 = row
                    .try_get("bucket")
                    .map_err(|e| DatabaseError::QueryError(e.to_string()))?;
                let count: i64 = row
                    .try_get("count")
                    .map_err(|e| DatabaseError::QueryError(e.to_string()))?;
                Ok((bucket, count))
            })
            .collect::<Result<HashMap<_, _>, DatabaseError>>()?;

        // Find the bucket with the minimum count
        let mut min_count = i64::MAX;
        let mut min_bucket = 1;
        for bucket in 0..total_buckets {
            let count = *bucket_counts.get(&bucket).unwrap_or(&0);
            if count < min_count {
                min_count = count;
                min_bucket = bucket;
            } else if count == min_count && bucket < min_bucket {
                // Tie-breaker: prefer lower bucket number
                min_bucket = bucket;
            }
        }

        // If no buckets have been created yet, start with bucket 0
        if min_count == i64::MAX {
            return Ok(0);
        }

        Ok(min_bucket)
    }

    async fn add_device_to_bucket(
        &self,
        device_id: Uuid,
        bucket_number: i32,
        node_id: Uuid,
    ) -> Result<(), DatabaseError> {
        let query = "DELETE FROM T_BUCKETS WHERE FK_DEVICE = $1";

        sqlx::query(query)
            .bind(device_id)
            .execute(&self.pool)
            .await
            .map_err(|e| DatabaseError::QueryError(e.to_string()))?;

        let query = "INSERT INTO T_BUCKETS
                    (fk_device, fk_node, bucket)
                    VALUES ($1, $2, $3)";

        sqlx::query(query)
            .bind(device_id)
            .bind(node_id)
            .bind(bucket_number)
            .execute(&self.pool)
            .await
            .map_err(|e| DatabaseError::QueryError(e.to_string()))?;

        Ok(())
    }

    async fn get_bucket_number(&self, device_id: Uuid) -> Result<i32, DatabaseError> {
        let query = "SELECT bucket FROM T_BUCKETS
            WHERE fk_device = $1";

        let bucket = sqlx::query_scalar(query)
            .bind(device_id)
            .fetch_one(&self.pool)
            .await
            .map_err(|e| DatabaseError::QueryError(e.to_string()))?;

        Ok(bucket)
    }

    // schedule connection
    async fn schedule_connection(
        &self,
        device_id: Uuid,
        timestamp: NaiveDateTime,
        job_id: Uuid,
    ) -> Result<(), DatabaseError> {
        let query = "INSERT INTO T_SCHEDULED_CONNECTIONS
                    (fk_device, schedule_time, connection_time, status, job_id, renewable)
                    VALUES ($1, $2, NULL, 'awaiting', $3, true)";

        sqlx::query(query)
            .bind(device_id)
            .bind(timestamp)
            .bind(job_id)
            .execute(&self.pool)
            .await
            .map_err(|e| DatabaseError::QueryError(e.to_string()))?;

        Ok(())
    }

    async fn get_scheduled_connections(&self) -> Result<Vec<ScheduledConnection>, DatabaseError> {
        let query = r#"
            SELECT fk_device, schedule_time, connection_time, status, job_id, renewable
            FROM T_SCHEDULED_CONNECTIONS
            WHERE status = 'awaiting'
            ORDER BY schedule_time ASC
        "#;

        let rows = sqlx::query(query)
            .fetch_all(&self.pool)
            .await
            .map_err(|e| DatabaseError::QueryError(e.to_string()))?;

        rows.into_iter()
            .map(|row| map_scheduled_row(&row))
            .collect()
    }

    async fn get_scheduled_connection(
        &self,
        device_id: Uuid,
    ) -> Result<ScheduledConnection, DatabaseError> {
        let query = r#"
            SELECT fk_device, schedule_time, connection_time, status, job_id, renewable
            FROM T_SCHEDULED_CONNECTIONS
            WHERE fk_device = $1
        "#;

        let row = sqlx::query(query)
            .bind(device_id)
            .fetch_one(&self.pool)
            .await
            .map_err(|e| DatabaseError::QueryError(e.to_string()))?;

        map_scheduled_row(&row)
    }

    async fn update_scheduled_connection(
        &self,
        connection: &ScheduledConnection,
    ) -> Result<(), DatabaseError> {
        let query = r#"
            UPDATE T_SCHEDULED_CONNECTIONS
            SET schedule_time = $1,
                connection_time = $2,
                status = $3,
                job_id = $4,
                renewable = $5
            WHERE fk_device = $6
        "#;

        sqlx::query(query)
            .bind(connection.schedule_time)
            .bind(connection.connection_time)
            .bind(&connection.status)
            .bind(connection.job_id)
            .bind(connection.renewable)
            .bind(connection.fk_device)
            .execute(&self.pool)
            .await
            .map_err(|e| DatabaseError::QueryError(e.to_string()))?;

        Ok(())
    }

    async fn get_upcoming_connections(
        &self,
        limit: i64,
        offset: i64,
        device_id: Option<Uuid>,
    ) -> Result<Vec<ScheduledConnection>, DatabaseError> {
        let rows = if let Some(did) = device_id {
            let query = r#"
                SELECT fk_device, schedule_time, connection_time, status, job_id, renewable
                FROM T_SCHEDULED_CONNECTIONS
                WHERE status = 'awaiting' AND fk_device = $3
                ORDER BY schedule_time ASC
                LIMIT $1 OFFSET $2
            "#;
            sqlx::query(query)
                .bind(limit)
                .bind(offset)
                .bind(did)
                .fetch_all(&self.pool)
                .await
                .map_err(|e| DatabaseError::QueryError(e.to_string()))?
        } else {
            let query = r#"
                SELECT fk_device, schedule_time, connection_time, status, job_id, renewable
                FROM T_SCHEDULED_CONNECTIONS
                WHERE status = 'awaiting'
                ORDER BY schedule_time ASC
                LIMIT $1 OFFSET $2
            "#;
            sqlx::query(query)
                .bind(limit)
                .bind(offset)
                .fetch_all(&self.pool)
                .await
                .map_err(|e| DatabaseError::QueryError(e.to_string()))?
        };

        rows.into_iter()
            .map(|row| map_scheduled_row(&row))
            .collect()
    }

    async fn get_connection_history(
        &self,
        limit: i64,
        offset: i64,
        device_id: Option<Uuid>,
    ) -> Result<Vec<ScheduledConnection>, DatabaseError> {
        let rows = if let Some(did) = device_id {
            let query = r#"
                SELECT fk_device, schedule_time, connection_time, status, job_id, renewable
                FROM T_SCHEDULED_CONNECTIONS
                WHERE status IN ('done', 'lost') AND fk_device = $3
                ORDER BY schedule_time DESC
                LIMIT $1 OFFSET $2
            "#;
            sqlx::query(query)
                .bind(limit)
                .bind(offset)
                .bind(did)
                .fetch_all(&self.pool)
                .await
                .map_err(|e| DatabaseError::QueryError(e.to_string()))?
        } else {
            let query = r#"
                SELECT fk_device, schedule_time, connection_time, status, job_id, renewable
                FROM T_SCHEDULED_CONNECTIONS
                WHERE status IN ('done', 'lost')
                ORDER BY schedule_time DESC
                LIMIT $1 OFFSET $2
            "#;
            sqlx::query(query)
                .bind(limit)
                .bind(offset)
                .fetch_all(&self.pool)
                .await
                .map_err(|e| DatabaseError::QueryError(e.to_string()))?
        };

        rows.into_iter()
            .map(|row| map_scheduled_row(&row))
            .collect()
    }

    async fn get_device_registration(
        &self,
        device_id: Uuid,
    ) -> Result<DeviceRegistration, DatabaseError> {
        let query = r#"
            SELECT fk_device, registration_status, registration_time
            FROM T_DEVICE_REGISTRATION
            WHERE fk_device = $1
        "#;

        let device_reg = query_as::<_, DeviceRegistration>(query)
            .bind(device_id)
            .fetch_one(&self.pool)
            .await
            .map_err(|e| {
                error!(
                    "Error fetching device registration for device {}: {}",
                    device_id, e
                );
                DatabaseError::QueryError(e.to_string())
            })?;

        Ok(device_reg)
    }

    async fn update_device_registration(
        &self,
        device_id: Uuid,
        status: Option<RegistrationStatus>,
        timestamp: Option<NaiveDateTime>,
    ) -> Result<(), DatabaseError> {
        // Check if the device registration exists
        let query_count = r#"
            SELECT COUNT(*)
            FROM T_DEVICE_REGISTRATION
            WHERE fk_device = $1
        "#;

        let count: i64 = query_scalar(query_count)
            .bind(device_id)
            .fetch_one(&self.pool)
            .await
            .map_err(|e| DatabaseError::QueryError(e.to_string()))?;

        if count < 1 {
            return Err(DatabaseError::NoDataFound);
        }

        // Build dynamic UPDATE query based on what needs updating
        match (status, timestamp) {
            (Some(s), Some(t)) => {
                let query = "UPDATE T_DEVICE_REGISTRATION
                            SET registration_status = $1, registration_time = $2
                            WHERE fk_device = $3";
                sqlx::query(query)
                    .bind(s)
                    .bind(t)
                    .bind(device_id)
                    .execute(&self.pool)
                    .await
                    .map_err(|e| DatabaseError::QueryError(e.to_string()))?;
            }
            (Some(s), None) => {
                let query = "UPDATE T_DEVICE_REGISTRATION
                            SET registration_status = $1
                            WHERE fk_device = $2";
                sqlx::query(query)
                    .bind(s)
                    .bind(device_id)
                    .execute(&self.pool)
                    .await
                    .map_err(|e| DatabaseError::QueryError(e.to_string()))?;
            }
            (None, Some(t)) => {
                let query = "UPDATE T_DEVICE_REGISTRATION
                            SET registration_time = $1
                            WHERE fk_device = $2";
                sqlx::query(query)
                    .bind(t)
                    .bind(device_id)
                    .execute(&self.pool)
                    .await
                    .map_err(|e| DatabaseError::QueryError(e.to_string()))?;
            }
            (None, None) => {
                // Nothing to update
                return Ok(());
            }
        }

        Ok(())
    }

    // ========== Cluster management ==========

    async fn register_cluster_node(
        &self,
        node_id: Uuid,
        node_name: String,
        cluster_ip: String,
        cluster_port: i32,
        backdoor_port: i32,
    ) -> Result<(), DatabaseError> {
        let query = r#"
            INSERT INTO T_NODES (id, node_name, cluster_ip, cluster_port, backdoor_port, status, started_at, last_seen)
            VALUES ($1, $2, $3, $4, $5, 'active', NOW(), NOW())
            ON CONFLICT (id)
            DO UPDATE SET
                status = 'active',
                cluster_ip = EXCLUDED.cluster_ip,
                cluster_port = EXCLUDED.cluster_port,
                backdoor_port = EXCLUDED.backdoor_port,
                last_seen = NOW()
        "#;

        sqlx::query(query)
            .bind(node_id)
            .bind(node_name)
            .bind(cluster_ip)
            .bind(cluster_port)
            .bind(backdoor_port)
            .execute(&self.pool)
            .await
            .map_err(|e| DatabaseError::QueryError(e.to_string()))?;

        Ok(())
    }

    async fn get_active_cluster_nodes(
        &self,
    ) -> Result<Vec<(Uuid, String, String, i32, i32)>, DatabaseError> {
        let query = r#"
            SELECT id, node_name, cluster_ip, cluster_port, backdoor_port
            FROM T_NODES
            WHERE status IN ('active', 'starting')
            ORDER BY node_name
        "#;

        let rows = sqlx::query(query)
            .fetch_all(&self.pool)
            .await
            .map_err(|e| DatabaseError::QueryError(e.to_string()))?;

        let nodes = rows
            .into_iter()
            .map(|row| {
                let node_id: Uuid = row
                    .try_get("id")
                    .map_err(|e| DatabaseError::QueryError(e.to_string()))?;
                let node_name: String = row
                    .try_get("node_name")
                    .map_err(|e| DatabaseError::QueryError(e.to_string()))?;
                let cluster_ip: String = row
                    .try_get("cluster_ip")
                    .map_err(|e| DatabaseError::QueryError(e.to_string()))?;
                let cluster_port: i32 = row
                    .try_get("cluster_port")
                    .map_err(|e| DatabaseError::QueryError(e.to_string()))?;
                let backdoor_port: i32 = row
                    .try_get("backdoor_port")
                    .map_err(|e| DatabaseError::QueryError(e.to_string()))?;
                Ok((node_id, node_name, cluster_ip, cluster_port, backdoor_port))
            })
            .collect::<Result<Vec<_>, DatabaseError>>()?;

        Ok(nodes)
    }

    async fn update_cluster_node_status(
        &self,
        node_id: Uuid,
        status: &str,
    ) -> Result<(), DatabaseError> {
        let query = "UPDATE T_NODES SET status = $1, last_seen = NOW() WHERE id = $2";

        sqlx::query(query)
            .bind(status)
            .bind(node_id)
            .execute(&self.pool)
            .await
            .map_err(|e| DatabaseError::QueryError(e.to_string()))?;

        Ok(())
    }

    async fn update_cluster_node_last_seen(&self, node_id: Uuid) -> Result<(), DatabaseError> {
        let query = "UPDATE T_NODES SET last_seen = NOW() WHERE id = $1";

        sqlx::query(query)
            .bind(node_id)
            .execute(&self.pool)
            .await
            .map_err(|e| DatabaseError::QueryError(e.to_string()))?;

        Ok(())
    }

    // ========== Device ownership (for cluster delegation) ==========

    async fn get_devices_by_owner(&self, node_id: Uuid) -> Result<Vec<Uuid>, DatabaseError> {
        let query = "SELECT FK_DEVICE FROM T_BUCKETS WHERE FK_NODE = $1";

        let devices: Vec<Uuid> = sqlx::query_scalar(query)
            .bind(node_id)
            .fetch_all(&self.pool)
            .await
            .map_err(|e| DatabaseError::QueryError(e.to_string()))?;

        Ok(devices)
    }

    async fn set_device_owner(&self, device_id: Uuid, node_id: Uuid) -> Result<(), DatabaseError> {
        let query = "UPDATE T_BUCKETS SET FK_NODE = $1 WHERE FK_DEVICE = $2";

        sqlx::query(query)
            .bind(node_id)
            .bind(device_id)
            .execute(&self.pool)
            .await
            .map_err(|e| DatabaseError::QueryError(e.to_string()))?;

        Ok(())
    }

    async fn get_all_device_ids(&self) -> Result<Vec<Uuid>, DatabaseError> {
        let query = "SELECT id FROM T_DEVICES ORDER BY id";

        let rows = sqlx::query(query)
            .fetch_all(&self.pool)
            .await
            .map_err(|e| DatabaseError::QueryError(e.to_string()))?;

        let device_ids: Vec<Uuid> = rows.iter().map(|row| row.get("id")).collect();

        Ok(device_ids)
    }
}
