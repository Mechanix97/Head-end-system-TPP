use sqlx::Pool;
use sqlx::Postgres;
use sqlx::postgres::PgPoolOptions;

use crate::connection::Connection;
use crate::database::api::Engine;

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

impl Engine for PostgresDB {
    fn get_active_connections(&self) -> Vec<Connection> {
        vec![]
    }
}
