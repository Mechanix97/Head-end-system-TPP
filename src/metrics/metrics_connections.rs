use prometheus::{Encoder, IntCounterVec, Opts, Registry, TextEncoder};
use std::sync::LazyLock;

use crate::MetricsError;

/// Global singleton instance of connection metrics.
///
/// This uses `LazyLock` to initialize the metrics registry lazily on first access.
/// The metrics can be accessed from anywhere in the codebase without needing to
/// pass around a metrics handle.
pub static METRICS_CONNECTIONS: LazyLock<MetricsConns> = LazyLock::new(MetricsConns::default);

/// Prometheus metrics for tracking device connections.
///
/// Currently tracks a single counter: `connections_tracker` with label "type"
/// to differentiate between different connection events (e.g., "new_connection").
#[derive(Debug, Clone)]
pub struct MetricsConns {
    /// Counter for tracking connection events, labeled by event type
    pub connections_tracker: IntCounterVec,
}

impl Default for MetricsConns {
    fn default() -> Self {
        Self::new()
    }
}

impl MetricsConns {
    /// Creates a new metrics collector with default counters initialized.
    ///
    /// Registers the "connections_tracker" metric and initializes it with
    /// a "new_connection" label set to 0.
    pub fn new() -> Self {
        let connections_tracker = IntCounterVec::new(
            Opts::new("connections_tracker", "Keeps track of all connections"),
            &["type"],
        )
        .expect("Invalid Prometheus counter");

        // Initialize the counter with a "new_connection" label
        connections_tracker
            .with_label_values(&["new_connection"])
            .inc_by(0);

        MetricsConns {
            connections_tracker,
        }
    }

    /// Gathers all metrics and encodes them in Prometheus text format.
    ///
    /// This is called by the `/metrics` HTTP endpoint to return metrics
    /// in the OpenMetrics format that Prometheus can scrape.
    ///
    /// Returns a string containing metrics in the format:
    /// ```text
    /// # HELP connections_tracker Keeps track of all connections
    /// # TYPE connections_tracker counter
    /// connections_tracker{type="new_connection"} 42
    /// ```
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
