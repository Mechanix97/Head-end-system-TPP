pub mod backdoor;

use common::{database::DatabaseError, messages::MessageError};
use device_manager::DeviceManagerError;

#[derive(Debug, thiserror::Error)]
pub enum BackdoorError {
    #[error("Device Manager Error: {0}")]
    DeviceManagerErr(#[from] DeviceManagerError),
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
