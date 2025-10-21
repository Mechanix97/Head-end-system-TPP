use chrono::{DateTime, Utc};
use sqlx::prelude::FromRow;
use uuid::Uuid;

#[derive(Clone, Debug, Eq, PartialEq, Hash, FromRow)]
pub struct Connection {
    pub device_id: Uuid,
    pub ip: Option<String>,
    pub bucket: Option<i32>,
    pub last_connection: DateTime<Utc>,
    pub next_wakeup: Option<DateTime<Utc>>,
    pub status: ConnectionStatus,
}

impl Connection {
    pub fn new(
        device_id: Uuid,
        ip: Option<String>,
        bucket: Option<i32>,
        status: ConnectionStatus,
    ) -> Self {
        Connection {
            device_id,
            ip,
            bucket,
            last_connection: Utc::now(),
            next_wakeup: None,
            status,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Hash, sqlx::Type)]
pub enum ConnectionStatus {
    #[sqlx(rename = "active")]
    Active,
    #[sqlx(rename = "pending_ack")]
    PendingAck,
    #[sqlx(rename = "lost")]
    Lost,
}
