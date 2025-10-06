pub mod backdoor;

use common::messages::MessageError;
use scheduler::error::SchedulerError;

#[derive(Debug, thiserror::Error)]
pub enum BackdoorError {
    #[error("Scheduler Error: {0}")]
    SChedulerErr(#[from] SchedulerError),
    #[error("io error: {0}")]
    TcpError(#[from] std::io::Error),
    #[error("Message error: {0}")]
    MessageError(#[from] MessageError),
    #[error("Error: register request device id not zero")]
    RegisterRequestInvalidId,
}
