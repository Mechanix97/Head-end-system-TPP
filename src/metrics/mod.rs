pub mod api;
pub mod metrics_connections;

#[derive(Debug, thiserror::Error)]
pub enum MetricsError {
    #[error("Prometheus Error: {0}")]
    PrometheusErr(String),
    #[error("io error: {0}")]
    TcpError(#[from] std::io::Error),
    #[error("MetricsL2Error {0}")]
    FromUtf8Error(#[from] std::string::FromUtf8Error),
    #[error("MetricsL2Error {0}")]
    TryInto(#[from] std::num::TryFromIntError),
}
