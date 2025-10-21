use sqlx::Pool;
use sqlx::Postgres;
use sqlx::postgres::PgPoolOptions;
use tracing::error;

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
    async fn get_active_connections(&self) -> Vec<Connection> {
        let query = "SELECT device_id, ip, last_connection, next_wakeup, status 
                     FROM T_ACTIVE_CONNECTIONS 
                     WHERE status = 'active'";

        sqlx::query_as::<_, Connection>(query)
            .fetch_all(&self.pool)
            .await
            .unwrap_or_else(|e| {
                error!("Error fetching active connections: {e}");
                vec![]
            })
    }

    async fn add_new_connection(&self, connection: Connection) -> Result<(), DatabaseError> {
        let query = "INSERT INTO T_ACTIVE_CONNECTIONS (device_id, ip, last_connection, next_wakeup, status) 
                     VALUES ($1, $2, $3, $4, $5)";

        sqlx::query(query)
            .bind(connection.device_id)
            .bind(connection.ip)
            .bind(connection.last_connection)
            .bind(connection.next_wakeup)
            .bind(connection.status)
            .execute(&self.pool)
            .await
            .map_err(|e| DatabaseError::QueryError(e.to_string()))?;

        Ok(())
    }
}
