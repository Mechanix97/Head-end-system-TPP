pub mod api;
pub mod metrics_cluster;
pub mod metrics_connections;
pub mod metrics_protocol;

use std::sync::OnceLock;

static METRICS_NODE_ID: OnceLock<String> = OnceLock::new();

/// Initialize the metrics subsystem with the local node ID.
///
/// Must be called before metrics are first accessed. Subsequent calls are no-ops.
/// The node_id is attached as a const label to every metric so multi-node scrapes
/// can be filtered by node in Prometheus/Grafana.
pub fn init(node_id: &str) {
    METRICS_NODE_ID.get_or_init(|| node_id.to_string());
}

pub(crate) fn node_id() -> &'static str {
    METRICS_NODE_ID.get().map(|s| s.as_str()).unwrap_or("unknown")
}

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
