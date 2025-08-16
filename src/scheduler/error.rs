use thiserror::Error;
use tokio_cron_scheduler::JobSchedulerError;

#[derive(Error, Debug)]
pub enum SchedulerError {
    #[error("Error in job scheduler: {0}")]
    DiscordError(#[from] JobSchedulerError),
}
