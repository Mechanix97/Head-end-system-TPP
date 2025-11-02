pub mod backdoor;

use common::{database::DatabaseError, messages::MessageError};
use scheduler::error::SchedulerError;

#[derive(Debug, thiserror::Error)]
pub enum BackdoorError {
    #[error("Scheduler Error: {0}")]
    SchedulerErr(#[from] SchedulerError),
    #[error("io error: {0}")]
    TcpError(#[from] std::io::Error),
    #[error("Message error: {0}")]
    MessageError(#[from] MessageError),
    #[error("Error: register request device id not zero")]
    RegisterRequestInvalidId,
    #[error("Database Error: {0}")]
    DatabaseError(#[from] DatabaseError),
    #[error("Parse Error: {0}")]
    ParseError(String),
    #[error("Error invalid IP")]
    InvalidIp,
    #[error("Error invalid Timestamp")]
    InvalidTimeStamp,
}
