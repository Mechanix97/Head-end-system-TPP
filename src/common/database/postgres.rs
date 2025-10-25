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

#[derive(Debug)]
pub struct PostgresDB {
    pub pool: Pool<Postgres>, // remove this pub
}

impl PostgresDB {
    pub async fn new() -> Self {
        Self {
            pool: PgPoolOptions::new()
                .max_connections(5)
                .connect("postgres://postgres:password@127.0.0.1:5432/hes") // TODO fix this
                .await
                .expect("Error connecting to DB"), // TODO remove this expect
        }
    }
}

#[async_trait::async_trait]
impl Engine for PostgresDB {
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
