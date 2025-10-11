use crate::connection::Connection;
use crate::database::api::Engine;

pub struct PostgresDB {}

impl PostgresDB {
    pub fn new() -> Self {
        Self {}
    }
}

impl Engine for PostgresDB {
    fn get_active_connections(&self) -> Vec<Connection> {
        vec![]
    }
}
