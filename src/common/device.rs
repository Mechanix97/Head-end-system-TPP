use sqlx::prelude::FromRow;
use std::net::SocketAddr;
use uuid::Uuid;

#[derive(Clone, Debug, Eq, PartialEq, Hash, FromRow)]
pub struct Device {
    pub id: Uuid,
    pub ipv4: Option<String>,
    pub ipv6: Option<String>,
    pub imei: Option<String>,
}

impl Device {
    pub fn new(socket: SocketAddr, imei: Option<String>) -> Self {
        let (ipv4, ipv6) = match socket {
            SocketAddr::V4(_) => (Some(socket.ip().to_string()), None),
            SocketAddr::V6(_) => (None, Some(socket.ip().to_string())),
        };

        Device {
            id: Uuid::new_v4(),
            ipv4,
            ipv6,
            imei,
        }
    }
}
