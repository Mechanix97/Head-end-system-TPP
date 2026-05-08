pub mod action;
pub mod codec;
pub mod execute;
pub mod handshake;
pub mod message;
pub mod nack;
pub mod read;
pub mod registry;
pub mod write;

#[derive(Debug, thiserror::Error)]
pub enum MsgCodecError {
    #[error("Error: Invalid msg length")]
    InvalidLength,
    #[error("Error: Unknown msg type")]
    UnknownMsgType,
    #[error("io error: {0}")]
    IoError(#[from] std::io::Error),
    #[error("Payload decode error: {0}")]
    PayloadDecodeError(String),
}

impl MsgCodecError {
    /// Returns a short snake_case label suitable for Prometheus metric tags.
    pub fn as_metric_label(&self) -> &'static str {
        match self {
            MsgCodecError::InvalidLength => "invalid_length",
            MsgCodecError::UnknownMsgType => "unknown_msg_type",
            MsgCodecError::PayloadDecodeError(_) => "payload_decode",
            MsgCodecError::IoError(_) => "io",
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum MessageError {
    #[error("Error: Invalid msg length")]
    InvalidLength,
    #[error("Error: Unknown msg type")]
    UnknownMsgType,
    #[error("io error: {0}")]
    IoError(#[from] std::io::Error),
    #[error("SystemTimeError: {0}")]
    SystemTimeError(#[from] std::time::SystemTimeError),
    #[error("Error: Invalid TimeStamp")]
    InvalidTimeStamp,
}
