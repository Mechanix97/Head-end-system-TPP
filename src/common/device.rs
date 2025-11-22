use sqlx::prelude::FromRow;
use std::net::SocketAddr;
use uuid::Uuid;

#[derive(Clone, Debug, Eq, PartialEq, Hash, FromRow)]
pub struct Device {
    pub id: Uuid,
    pub ipv4: Option<String>,
    pub ipv6: Option<String>,
    pub mac: Option<String>,
    pub factory_id: Option<i64>,
    pub batch_id: Option<i64>,
}

impl Device {
    pub fn new(
        socket: SocketAddr,
        mac: Option<String>,
        factory_id: Option<i64>,
        batch_id: Option<i64>,
    ) -> Self {
        let (ipv4, ipv6) = match socket {
            SocketAddr::V4(_) => (Some(socket.ip().to_string()), None),
            SocketAddr::V6(_) => (None, Some(socket.ip().to_string())),
        };

        Device {
            id: Uuid::new_v4(),
            ipv4,
            ipv6,
            mac,
            factory_id,
            batch_id,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Hash, sqlx::Type)]
pub enum RegistrationStatus {
    #[sqlx(rename = "registered")]
    Registered,
    #[sqlx(rename = "pending_ack")]
    PendingAck,
    #[sqlx(rename = "ack_timeout")]
    AckTimeout,
}
