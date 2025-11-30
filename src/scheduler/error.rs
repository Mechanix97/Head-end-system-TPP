use common::database::DatabaseError;
use common::messages::{MessageError, MsgCodecError};
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

#[derive(Error, Debug)]
pub enum TaskError {
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
    #[error("Device has no IP address")]
    DeviceWithNoIP,
    #[error("IO error: {0}")]
    IoError(#[from] std::io::Error),
    #[error("Max connection retries exceeded")]
    MaxRetriesExceeded,
    #[error("Message error: {0}")]
    MessageError(#[from] MessageError),
    #[error("Message codec error: {0}")]
    MsgCodecError(#[from] MsgCodecError),
}
