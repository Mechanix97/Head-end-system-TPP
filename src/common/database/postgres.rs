use sqlx::postgres::PgPoolOptions;

use crate::connection::Connection;
use crate::database::api::Engine;

pub struct PostgresDB {}

impl PostgresDB {
    pub async fn new() -> Self {
        let _pool = PgPoolOptions::new()
            .max_connections(5)
            .connect("postgres://postgres:password@postgres:5432/hes")
            .await
            .expect("Error connecting to DB");

        Self {}
    }
}

impl Engine for PostgresDB {
    fn get_active_connections(&self) -> Vec<Connection> {
        vec![]
    }
}
