pub mod action;
pub mod codec;
pub mod execute;
pub mod handshake;
pub mod message;
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
}
