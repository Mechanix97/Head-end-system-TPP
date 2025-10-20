use prometheus::{Encoder, IntCounterVec, Opts, Registry, TextEncoder};
use std::sync::LazyLock;

use crate::MetricsError;

pub static METRICS_CONNECTIONS: LazyLock<MetricsConns> = LazyLock::new(MetricsConns::default);

#[derive(Debug, Clone)]
pub struct MetricsConns {
    pub connections_tracker: IntCounterVec,
}

impl Default for MetricsConns {
    fn default() -> Self {
        Self::new()
    }
}

impl MetricsConns {
    pub fn new() -> Self {
        let connections_tracker = IntCounterVec::new(
            Opts::new("connections_tracker", "Keeps track of all connections"),
            &["type"],
        )
        .expect("Invalid Prometheus counter");

        connections_tracker
            .with_label_values(&["new_connection"])
            .inc_by(0);
        MetricsConns {
            connections_tracker,
        }
    }
    pub fn gather_metrics(&self) -> Result<String, MetricsError> {
        let r = Registry::new();

        r.register(Box::new(self.connections_tracker.clone()))
            .map_err(|e| MetricsError::PrometheusErr(e.to_string()))?;

        let encoder = TextEncoder::new();
        let metric_families = r.gather();

        let mut buffer = Vec::new();
        encoder
            .encode(&metric_families, &mut buffer)
            .map_err(|e| MetricsError::PrometheusErr(e.to_string()))?;

        let res = String::from_utf8(buffer)?;

        Ok(res)
    }
}
