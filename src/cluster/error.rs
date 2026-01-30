//! Error types for the cluster module.

use common::database::DatabaseError;

/// Errors that can occur in cluster operations.
#[derive(Debug, thiserror::Error)]
pub enum ClusterError {
    /// Error communicating with another node
    #[error("Communication error: {0}")]
    CommunicationError(String),

    /// Error encoding/decoding cluster messages
    #[error("Codec error: {0}")]
    CodecError(String),

    /// Database error
    #[error("Database error: {0}")]
    DatabaseError(#[from] DatabaseError),

    /// Node not found in the cluster
    #[error("Node not found: {0}")]
    NodeNotFound(uuid::Uuid),

    /// Bucket not assigned to this node
    #[error("Bucket {0} not owned by this node")]
    BucketNotOwned(i32),

    /// No nodes available for delegation
    #[error("No nodes available for delegation")]
    NoNodesAvailable,

    /// Delegation was rejected by the target node
    #[error("Delegation rejected: {0}")]
    DelegationRejected(String),

    /// IO error
    #[error("IO error: {0}")]
    IoError(#[from] std::io::Error),

    /// Invalid configuration
    #[error("Invalid configuration: {0}")]
    InvalidConfig(String),

    /// Node already exists in cluster
    #[error("Node already registered: {0}")]
    NodeAlreadyExists(uuid::Uuid),

    /// Timeout waiting for response
    #[error("Timeout waiting for response from node {0}")]
    Timeout(uuid::Uuid),

    /// Invalid message received
    #[error("Invalid message: {0}")]
    InvalidMessage(String),

    /// Cluster not initialized
    #[error("Cluster not initialized")]
    NotInitialized,
}
