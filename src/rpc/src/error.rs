use thiserror::Error;

/// JSON-RPC 2.0 error codes.
pub const CODE_PARSE_ERROR: i64 = -32700;
pub const CODE_INVALID_REQUEST: i64 = -32600;
pub const CODE_METHOD_NOT_FOUND: i64 = -32601;
pub const CODE_INVALID_PARAMS: i64 = -32602;
pub const CODE_INTERNAL_ERROR: i64 = -32603;
// Server-defined errors: -32000 to -32099
pub const CODE_CONFIG_ERROR: i64 = -32000;
pub const CODE_CLUSTER_NOT_ENABLED: i64 = -32001;
pub const CODE_DEVICE_NOT_FOUND: i64 = -32002;
pub const CODE_DATABASE_ERROR: i64 = -32003;

#[derive(Debug, Error)]
pub enum RpcError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),

    #[error("Method not found: {0}")]
    MethodNotFound(String),

    #[error("Invalid params: {0}")]
    InvalidParams(String),

    #[error("Internal error: {0}")]
    Internal(String),

    #[error("Config error: {0}")]
    Config(String),

    #[error("Cluster not enabled")]
    ClusterNotEnabled,

    #[error("Device not found: {0}")]
    DeviceNotFound(String),

    #[error("Database error: {0}")]
    Database(String),
}

impl RpcError {
    /// Returns the JSON-RPC error code for this error.
    pub fn code(&self) -> i64 {
        match self {
            RpcError::Io(_) => CODE_INTERNAL_ERROR,
            RpcError::Json(_) => CODE_PARSE_ERROR,
            RpcError::MethodNotFound(_) => CODE_METHOD_NOT_FOUND,
            RpcError::InvalidParams(_) => CODE_INVALID_PARAMS,
            RpcError::Internal(_) => CODE_INTERNAL_ERROR,
            RpcError::Config(_) => CODE_CONFIG_ERROR,
            RpcError::ClusterNotEnabled => CODE_CLUSTER_NOT_ENABLED,
            RpcError::DeviceNotFound(_) => CODE_DEVICE_NOT_FOUND,
            RpcError::Database(_) => CODE_DATABASE_ERROR,
        }
    }
}
