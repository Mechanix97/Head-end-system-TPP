use chrono::NaiveDateTime;
use uuid::Uuid;

#[derive(Clone, Debug, Eq, PartialEq, Hash, sqlx::Type)]
pub enum ScheduledStatus {
    #[sqlx(rename = "awaiting")]
    Awaiting,
    #[sqlx(rename = "lost")]
    Lost,
    #[sqlx(rename = "done")]
    Done,
}

#[derive(Clone, Debug)]
pub struct ScheduledConnection {
    pub fk_device: Uuid,
    pub schedule_time: NaiveDateTime,
    pub connection_time: Option<NaiveDateTime>,
    pub status: ScheduledStatus,
    pub job_id: Option<Uuid>,
    pub renewable: bool,
}
