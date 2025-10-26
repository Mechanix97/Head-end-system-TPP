use common::database::DatabaseError;
use thiserror::Error;
use tokio_cron_scheduler::JobSchedulerError;

#[derive(Error, Debug)]
pub enum SchedulerError {
    #[error("Error in job scheduler: {0}")]
    JobSchedulerError(#[from] JobSchedulerError),
    #[error("Error in database: {0}")]
    DatabaseError(#[from] DatabaseError),
    #[error("Parse Error: {0}")]
    ParseError(String),
    #[error("No schedule defined Error")]
    NoScheduleDefined,
    #[error("No bucket defined")]
    NoBucketDefined,
}
