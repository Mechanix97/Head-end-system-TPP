use chrono::NaiveDateTime;
use uuid::Uuid;

pub struct ScheduledConnection {
    pub fk_device: Uuid,
    pub schedule_time: NaiveDateTime,
}
