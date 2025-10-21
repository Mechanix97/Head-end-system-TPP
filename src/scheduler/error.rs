use common::database::DatabaseError;
use thiserror::Error;
use tokio_cron_scheduler::JobSchedulerError;

#[derive(Error, Debug)]
pub enum SchedulerError {
    #[error("Error in job scheduler: {0}")]
    DiscordError(#[from] JobSchedulerError),

    #[error("Error in database: {0}")]
    DatabaseError(#[from] DatabaseError),
}
