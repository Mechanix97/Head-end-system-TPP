use chrono::{DateTime, Utc};
use sqlx::prelude::FromRow;
use uuid::Uuid;

#[derive(Clone, Eq, PartialEq, Hash, FromRow)]
pub struct Connection {
    pub device_id: Uuid,
    pub ip: Option<String>,
    pub connection_time: DateTime<Utc>,
    pub next_wakeup: Option<DateTime<Utc>>,
    pub status: String,
}

impl Connection {
    pub fn new(device_id: Uuid, ip: Option<String>) -> Self {
        Connection {
            device_id,
            ip,
            connection_time: Utc::now(),
            next_wakeup: None,
            status: "active".to_string(),
        }
    }
}
