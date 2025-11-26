use chrono::NaiveDateTime;
use sqlx::prelude::FromRow;
use uuid::Uuid;

#[derive(Clone, Debug, Eq, PartialEq, Hash, sqlx::Type)]
pub enum RegistrationStatus {
    #[sqlx(rename = "registered")]
    Registered,
    #[sqlx(rename = "pending_ack")]
    PendingAck,
    #[sqlx(rename = "ack_timeout")]
    AckTimeout,
}

#[derive(Clone, Debug, Eq, PartialEq, Hash, FromRow)]
pub struct DeviceRegistration {
    pub fk_device: Uuid,
    pub registration_status: RegistrationStatus,
    pub registration_time: NaiveDateTime,
}
