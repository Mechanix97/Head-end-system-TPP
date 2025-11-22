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
use crate::device::RegistrationStatus;

#[derive(Debug)]
pub struct PostgresDB {
    pool: Pool<Postgres>,
}

pub struct PostgresConnectionArgs {
    pub user: String,
    pub password: String,
    pub url: String,
    pub port: String,
}

impl PostgresDB {
    pub async fn new(args: PostgresConnectionArgs) -> Result<Self, DatabaseError> {
        Ok(Self {
            pool: PgPoolOptions::new()
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
        let query = "INSERT INTO T_DEVICES
                    (id, IPv4, IPv6, MAC, factory_id, batch_id) 
                    VALUES ($1, $2, $3, $4, $5, $6)";

        sqlx::query(query)
            .bind(device.id)
            .bind(device.ipv4.clone())
            .bind(device.ipv6.clone())
            .bind(device.mac.clone())
            .bind(device.factory_id)
            .bind(device.batch_id)
            .execute(&self.pool)
            .await
            .map_err(|e| DatabaseError::QueryError(e.to_string()))?;

        Ok(())
    }

    async fn get_device(&self, device_id: Uuid) -> Result<Device, DatabaseError> {
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
            SELECT id, IPv4, IPv6, MAC, factory_id, batch_id
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
        let query = "UPDATE T_DEVICES
        SET IPv4 = $2, IPv6 = $3, MAC = $4, factory_id = $5, batch_id = $6
        WHERE id = $1";

        sqlx::query(query)
            .bind(device.id)
            .bind(device.ipv4.clone())
            .bind(device.ipv6.clone())
            .bind(device.mac.clone())
            .bind(device.factory_id)
            .bind(device.batch_id)
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

    async fn registration_ack(
        &self,
        device_id: Uuid,
        timestamp: NaiveDateTime,
    ) -> Result<(), DatabaseError> {
        let query_count = r#"
            SELECT COUNT(*) 
            FROM T_DEVICE_REGISTRATION 
            WHERE fk_device = $1 
            AND "registration_status" = 'pending_ack'
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

        let query = r#"UPDATE T_DEVICE_REGISTRATION 
        SET registration_status = 'registered', 
        registration_time = $2
        WHERE fk_device = $1 
            AND registration_status = 'pending_ack'"#;

        sqlx::query(query)
            .bind(device_id)
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

    // buckets
    async fn get_bucket_with_less_devices(&self, total_buckets: i32) -> Result<i32, DatabaseError> {
        let query = r#"
            SELECT bucket, COUNT(*) as count FROM T_BUCKETS GROUP BY bucket
        "#;

        let rows = sqlx::query(query)
            .fetch_all(&self.pool)
            .await
            .map_err(|e| DatabaseError::QueryError(e.to_string()))?;

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

        let mut min_count = i64::MAX;
        let mut min_bucket = 1;
        for bucket in 0..total_buckets {
            let count = *bucket_counts.get(&bucket).unwrap_or(&0);
            if count < min_count {
                min_count = count;
                min_bucket = bucket;
            } else if count == min_count && bucket < min_bucket {
                min_bucket = bucket;
            }
        }

        if min_count == i64::MAX {
            return Ok(0);
        }

        Ok(min_bucket)
    }

    async fn add_device_to_bucket(
        &self,
        device_id: Uuid,
        bucket_number: i32,
    ) -> Result<(), DatabaseError> {
        let query = "DELETE FROM T_BUCKETS WHERE FK_DEVICE = $1";

        sqlx::query(query)
            .bind(device_id)
            .execute(&self.pool)
            .await
            .map_err(|e| DatabaseError::QueryError(e.to_string()))?;

        let query = "INSERT INTO T_BUCKETS
                    (fk_device, bucket) 
                    VALUES ($1, $2)";

        sqlx::query(query)
            .bind(device_id)
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

    async fn remove_device_from_bucket(&self, device_id: Uuid) -> Result<(), DatabaseError> {
        let query = "DELETE FROM T_BUCKETS WHERE FK_DEVICE = $1";

        sqlx::query(query)
            .bind(device_id)
            .execute(&self.pool)
            .await
            .map_err(|e| DatabaseError::QueryError(e.to_string()))?;
        Ok(())
    }

    // schedule connection
    async fn schedule_connection(
        &self,
        device_id: Uuid,
        timestamp: NaiveDateTime,
        job_id: Uuid,
    ) -> Result<(), DatabaseError> {
        let query = "INSERT INTO T_SCHEDULED_CONNECTIONS
                    (fk_device, schedule_time, connection_time, status, job_id) 
                    VALUES ($1, $2, NULL, 'awaiting', $3)";

        sqlx::query(query)
            .bind(device_id)
            .bind(timestamp)
            .bind(job_id)
            .execute(&self.pool)
            .await
            .map_err(|e| DatabaseError::QueryError(e.to_string()))?;

        Ok(())
    }

    async fn get_scheduled_connections(&self) -> Result<Vec<(Uuid, NaiveDateTime)>, DatabaseError> {
        let query = r#"
            SELECT fk_device, schedule_time
            FROM T_SCHEDULED_CONNECTIONS
            WHERE status = 'awaiting'
            ORDER BY schedule_time ASC
        "#;

        let rows = sqlx::query(query)
            .fetch_all(&self.pool)
            .await
            .map_err(|e| DatabaseError::QueryError(e.to_string()))?;

        let scheduled = rows
            .into_iter()
            .map(|row| {
                let device_id: Uuid = row
                    .try_get("fk_device")
                    .map_err(|e| DatabaseError::QueryError(e.to_string()))?;
                let schedule_time: NaiveDateTime = row
                    .try_get("schedule_time")
                    .map_err(|e| DatabaseError::QueryError(e.to_string()))?;
                Ok((device_id, schedule_time))
            })
            .collect::<Result<Vec<_>, DatabaseError>>()?;

        Ok(scheduled)
    }
}
