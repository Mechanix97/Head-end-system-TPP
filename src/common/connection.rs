use chrono::NaiveDateTime;
use sqlx::prelude::FromRow;
use uuid::Uuid;

#[derive(Clone, Debug, Eq, PartialEq, Hash, FromRow)]
pub struct Connection {
    pub device_id: Uuid,
    pub ip: Option<String>,
    pub bucket: Option<i64>,
    pub last_connection: NaiveDateTime,
    pub next_wakeup: Option<NaiveDateTime>,
    pub status: ConnectionStatus,
}

impl Connection {
    pub fn new(
        device_id: Uuid,
        ip: Option<String>,
        bucket: Option<i64>,
        status: ConnectionStatus,
    ) -> Self {
        Connection {
            device_id,
            ip,
            bucket,
            last_connection: chrono::Local::now().naive_local(),
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
