use prometheus::{Encoder, Histogram, HistogramOpts, IntCounter, IntCounterVec, Opts, Registry, TextEncoder};
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
/// - `ack_response_time_ms`: Histogram for ACK response time distribution in milliseconds
/// - `ack_timeout_count`: Counter for ACKs that exceeded the timeout threshold
#[derive(Debug)]
pub struct MetricsConns {
    /// Prometheus registry - created once and reused
    registry: Registry,
    /// Counter for tracking connection events, labeled by event type
    pub connections_tracker: IntCounterVec,
    /// Histogram for ACK response time distribution in milliseconds
    /// Provides percentiles (p50, p95, p99), count, and sum
    pub ack_response_time_ms: Histogram,
    /// Counter for ACKs that exceeded the configured timeout
    pub ack_timeout_count: IntCounter,
}

impl Default for MetricsConns {
    fn default() -> Self {
        Self::new()
    }
}

impl MetricsConns {
    /// Creates a new metrics collector with a global registry.
    ///
    /// Registers:
    /// - "connections_tracker" counter with "new_connection" label
    /// - "ack_response_time_ms" histogram for ACK response time distribution
    /// - "ack_timeout_count" counter for timeouts
    ///
    /// The registry is created once and reused for all gather operations,
    /// avoiding the overhead of re-registering metrics on every scrape.
    pub fn new() -> Self {
        let registry = Registry::new();

        let connections_tracker = IntCounterVec::new(
            Opts::new("connections_tracker", "Keeps track of all connections"),
            &["type"],
        )
        .expect("Invalid Prometheus counter");

        // Initialize the counter with a "new_connection" label
        connections_tracker
            .with_label_values(&["new_connection"])
            .inc_by(0);

        // Histogram buckets for latency in milliseconds
        // Covers range from 10ms to 10s with meaningful breakpoints
        let latency_buckets = vec![10.0, 25.0, 50.0, 100.0, 250.0, 500.0, 1000.0, 2500.0, 5000.0, 10000.0];

        let ack_response_time_ms = Histogram::with_opts(
            HistogramOpts::new(
                "ack_response_time_ms",
                "ACK response time distribution in milliseconds",
            )
            .buckets(latency_buckets),
        )
        .expect("Invalid Prometheus histogram");

        let ack_timeout_count = IntCounter::new(
            "ack_timeout_count",
            "Count of ACKs that exceeded the timeout threshold",
        )
        .expect("Invalid Prometheus counter");

        // Register all metrics once at creation time
        registry
            .register(Box::new(connections_tracker.clone()))
            .expect("Failed to register connections_tracker");
        registry
            .register(Box::new(ack_response_time_ms.clone()))
            .expect("Failed to register ack_response_time_ms");
        registry
            .register(Box::new(ack_timeout_count.clone()))
            .expect("Failed to register ack_timeout_count");

        MetricsConns {
            registry,
            connections_tracker,
            ack_response_time_ms,
            ack_timeout_count,
        }
    }

    /// Gathers all metrics and encodes them in Prometheus text format.
    ///
    /// This is called by the `/metrics` HTTP endpoint to return metrics
    /// in the OpenMetrics format that Prometheus can scrape.
    ///
    /// Uses the pre-registered global registry for efficient gathering.
    ///
    /// Returns a string containing metrics in the format:
    /// ```text
    /// # HELP connections_tracker Keeps track of all connections
    /// # TYPE connections_tracker counter
    /// connections_tracker{type="new_connection"} 42
    /// # HELP ack_response_time_ms ACK response time distribution in milliseconds
    /// # TYPE ack_response_time_ms histogram
    /// ack_response_time_ms_bucket{le="10"} 5
    /// ack_response_time_ms_bucket{le="25"} 12
    /// ...
    /// ```
    pub fn gather_metrics(&self) -> Result<String, MetricsError> {
        let encoder = TextEncoder::new();
        let metric_families = self.registry.gather();

        let mut buffer = Vec::new();
        encoder
            .encode(&metric_families, &mut buffer)
            .map_err(|e| MetricsError::PrometheusErr(e.to_string()))?;

        let res = String::from_utf8(buffer)?;

        Ok(res)
    }
}
