use crate::connection::Connection;
use crate::database::api::Engine;

pub struct InMemoryDB {}

impl InMemoryDB {
    pub fn new() -> Self {
        Self {}
    }
}

impl Engine for InMemoryDB {
    fn get_active_connections(&self) -> Vec<Connection> {
        vec![]
    }
}
