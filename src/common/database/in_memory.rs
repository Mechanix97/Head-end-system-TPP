use crate::connection::Connection;
use crate::database::api::Engine;
use std::sync::Arc;
use tokio::sync::Mutex;

#[derive(Default)]
pub struct InMemoryDB {
    pub inner: Arc<Mutex<InnerDB>>,
}

#[derive(Default)]
pub struct InnerDB {
    pub active_connections: Vec<Connection>, //todo remove this pub
}

impl Engine for InMemoryDB {
    fn get_active_connections(&self) -> Vec<Connection> {
        vec![]
    }
}
