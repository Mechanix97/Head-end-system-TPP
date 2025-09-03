pub mod backdoor;

use scheduler::error::SchedulerError;

#[derive(Debug, thiserror::Error)]
pub enum BackdoorError {
    #[error("Scheduler Error: {0}")]
    SChedulerErr(#[from] SchedulerError),
    #[error("io error: {0}")]
    TcpError(#[from] std::io::Error),
}
