use prometheus::{Encoder, HistogramVec, IntCounterVec, Opts, Registry, TextEncoder};
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
/// Tracks:
/// - `connections_tracker`: Counter for connection events (e.g., "new_connection")
/// - `registration_ack_duration_seconds`: Histogram of time between REGISTER_REQUEST and ACK
#[derive(Debug, Clone)]
pub struct MetricsConns {
    /// Counter for tracking connection events, labeled by event type
    pub connections_tracker: IntCounterVec,
    /// Histogram for tracking registration ACK response time in seconds
    pub registration_ack_duration_seconds: HistogramVec,
}

impl Default for MetricsConns {
    fn default() -> Self {
        Self::new()
    }
}

impl MetricsConns {
    /// Creates a new metrics collector with default counters and histograms initialized.
    ///
    /// Registers:
    /// - "connections_tracker" counter with "new_connection" label
    /// - "registration_ack_duration_seconds" histogram with buckets optimized for registration latency
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

        // Create histogram for registration ACK duration
        // Buckets: 10ms, 50ms, 100ms, 250ms, 500ms, 1s, 2.5s, 5s, 10s
        // Optimized for typical registration latency (expected: 50-300ms range)
        let registration_ack_duration_seconds = HistogramVec::new(
            prometheus::HistogramOpts::new(
                "registration_ack_duration_seconds",
                "Time between REGISTER_REQUEST and ACK in seconds"
            )
            .buckets(vec![0.01, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0]),
            &["status"], // Label: "success" or "timeout"
        )
        .expect("Invalid Prometheus histogram");

        MetricsConns {
            connections_tracker,
            registration_ack_duration_seconds,
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

        r.register(Box::new(self.registration_ack_duration_seconds.clone()))
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
