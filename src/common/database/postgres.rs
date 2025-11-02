use chrono::NaiveDateTime;
use sqlx::Pool;
use sqlx::Postgres;
use sqlx::postgres::PgPoolOptions;
use sqlx::query_as;
use sqlx::query_scalar;
use tracing::error;
use uuid::Uuid;

use crate::connection::Connection;
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
        let query = "UPDATE T_DEVICE 
        SET IPv4 = $2, IPv6 = $3, MAC = $4, factory_id = $5, batch_id = $6
        WHERE device_id = $1";

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
                    (FK_DEVICE, registration_status, registration_time) 
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
            WHERE "FK_DEVICE" = $1 
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
        registration_time = $2, 
         WHERE FK_DEVICE = $1 
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
            WHERE FK_DEVICE = $1 
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
        registration_time = $2, 
         WHERE FK_DEVICE = $1 
            AND registration_status = 'pending_ack'"#;

        sqlx::query(query)
            .bind(device_id)
            .bind(timestamp)
            .execute(&self.pool)
            .await
            .map_err(|e| DatabaseError::QueryError(e.to_string()))?;

        Ok(true)
    }

    async fn get_active_connections(&self) -> Result<Vec<Connection>, DatabaseError> {
        let query = "SELECT device_id, ip, bucket, last_connection, next_wakeup, status 
                     FROM T_ACTIVE_CONNECTIONS 
                     WHERE status = 'active'";

        Ok(sqlx::query_as::<_, Connection>(query)
            .fetch_all(&self.pool)
            .await
            .unwrap_or_else(|e| {
                error!("Error fetching active connections: {e}");
                vec![]
            }))
    }

    async fn add_new_connection(&self, connection: &Connection) -> Result<(), DatabaseError> {
        let query = "INSERT INTO T_ACTIVE_CONNECTIONS (device_id, ip, bucket, last_connection, next_wakeup, status) 
                     VALUES ($1, $2, $3, $4, $5, $6)";

        sqlx::query(query)
            .bind(connection.device_id)
            .bind(connection.ip.clone())
            .bind(connection.bucket)
            .bind(connection.last_connection)
            .bind(connection.next_wakeup)
            .bind(connection.status.clone())
            .execute(&self.pool)
            .await
            .map_err(|e| DatabaseError::QueryError(e.to_string()))?;

        Ok(())
    }

    async fn get_connection_data(&self, device_id: Uuid) -> Result<Connection, DatabaseError> {
        let query_count = r#"
            SELECT COUNT(*) 
            FROM T_ACTIVE_CONNECTIONS 
            WHERE device_id = $1
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
            SELECT device_id, ip, bucket, last_connection, next_wakeup, status 
            FROM T_ACTIVE_CONNECTIONS 
            WHERE device_id = $1
        "#;

        let connection = query_as::<_, Connection>(query)
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

        Ok(connection)
    }

    async fn update_connection(&self, connection: &Connection) -> Result<(), DatabaseError> {
        let query = "UPDATE T_ACTIVE_CONNECTIONS 
        SET ip = $2, bucket = $3, last_connection = $4, next_wakeup = $5, status = $6
        WHERE device_id = $1";

        sqlx::query(query)
            .bind(connection.device_id)
            .bind(connection.ip.clone())
            .bind(connection.bucket)
            .bind(connection.last_connection)
            .bind(connection.next_wakeup)
            .bind(connection.status.clone())
            .execute(&self.pool)
            .await
            .map_err(|e| DatabaseError::QueryError(e.to_string()))?;

        Ok(())
    }
}
