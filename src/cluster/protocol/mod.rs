//! Inter-node communication protocol for the HES cluster.
//!
//! This module defines the message types and encoding/decoding logic for
//! communication between HES nodes in a cluster.

pub mod codec;
pub mod message;
pub mod payload;

pub use codec::ClusterMessageCodec;
pub use message::{ClusterMessage, ClusterMessageType};
pub use payload::*;

/// Errors that can occur during message encoding/decoding.
#[derive(Debug, thiserror::Error)]
pub enum ClusterCodecError {
    #[error("Invalid message length")]
    InvalidLength,
    #[error("Unknown message type: {0}")]
    UnknownMessageType(u8),
    #[error("Invalid payload: {0}")]
    InvalidPayload(String),
    #[error("IO error: {0}")]
    IoError(#[from] std::io::Error),
}
