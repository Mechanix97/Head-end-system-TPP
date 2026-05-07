use std::collections::HashMap;
use std::sync::LazyLock;

use prometheus::{Encoder, Histogram, HistogramOpts, IntCounter, IntCounterVec, IntGauge, IntGaugeVec, Opts, Registry, TextEncoder};

use crate::MetricsError;

/// Global singleton instance of connection metrics.
///
/// This uses `LazyLock` to initialize the metrics registry lazily on first access.
/// The metrics can be accessed from anywhere in the codebase without needing to
/// pass around a metrics handle.
pub static METRICS_CONNECTIONS: LazyLock<MetricsConns> =
    LazyLock::new(|| MetricsConns::try_new().unwrap_or_else(|e| panic!("Failed to initialize connection metrics: {e}")));

/// Prometheus metrics for tracking HES operations.
///
/// Tracks:
/// - `connections_tracker`: Counter for connection events (e.g., "new_connection")
/// - `ack_response_time_ms`: Histogram for ACK response time distribution in milliseconds
/// - `ack_timeout_count`: Counter for ACKs that exceeded the timeout threshold
/// - `scheduled_devices_total`: Gauge for total number of scheduled devices
/// - `devices_per_bucket`: Gauge for devices in each scheduler bucket
/// - `errors_total`: Counter for errors by component and type
/// - `messages_total`: Counter for messages by type and direction
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

    // Scheduler metrics
    /// Total number of devices with scheduled connections
    pub scheduled_devices_total: IntGauge,
    /// Number of devices assigned to each bucket (label: bucket)
    pub devices_per_bucket: IntGaugeVec,

    // Error metrics
    /// Total errors by component and error type (labels: component, error_type)
    pub errors_total: IntCounterVec,

    // Message metrics
    /// Total messages by type and direction (labels: msg_type, direction)
    pub messages_total: IntCounterVec,
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
    pub fn try_new() -> Result<Self, MetricsError> {
        let mut const_labels = HashMap::new();
        const_labels.insert("node_id".to_string(), crate::node_id().to_string());
        let registry = Registry::new_custom(None, Some(const_labels))
            .map_err(|e| MetricsError::PrometheusErr(e.to_string()))?;

        let connections_tracker = IntCounterVec::new(
            Opts::new("connections_tracker", "Keeps track of all connections"),
            &["type"],
        )
        .map_err(|e| MetricsError::PrometheusErr(e.to_string()))?;

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
        .map_err(|e| MetricsError::PrometheusErr(e.to_string()))?;

        let ack_timeout_count = IntCounter::new(
            "ack_timeout_count",
            "Count of ACKs that exceeded the timeout threshold",
        )
        .map_err(|e| MetricsError::PrometheusErr(e.to_string()))?;

        // Scheduler metrics
        let scheduled_devices_total = IntGauge::new(
            "scheduled_devices_total",
            "Total number of devices with scheduled connections",
        )
        .map_err(|e| MetricsError::PrometheusErr(e.to_string()))?;

        let devices_per_bucket = IntGaugeVec::new(
            Opts::new("devices_per_bucket", "Number of devices assigned to each scheduler bucket"),
            &["bucket"],
        )
        .map_err(|e| MetricsError::PrometheusErr(e.to_string()))?;

        // Error metrics
        let errors_total = IntCounterVec::new(
            Opts::new("errors_total", "Total errors by component and type"),
            &["component", "error_type"],
        )
        .map_err(|e| MetricsError::PrometheusErr(e.to_string()))?;

        // Message metrics
        let messages_total = IntCounterVec::new(
            Opts::new("messages_total", "Total messages by type and direction"),
            &["msg_type", "direction"],
        )
        .map_err(|e| MetricsError::PrometheusErr(e.to_string()))?;

        // Register all metrics once at creation time
        registry
            .register(Box::new(connections_tracker.clone()))
            .map_err(|e| MetricsError::PrometheusErr(e.to_string()))?;
        registry
            .register(Box::new(ack_response_time_ms.clone()))
            .map_err(|e| MetricsError::PrometheusErr(e.to_string()))?;
        registry
            .register(Box::new(ack_timeout_count.clone()))
            .map_err(|e| MetricsError::PrometheusErr(e.to_string()))?;
        registry
            .register(Box::new(scheduled_devices_total.clone()))
            .map_err(|e| MetricsError::PrometheusErr(e.to_string()))?;
        registry
            .register(Box::new(devices_per_bucket.clone()))
            .map_err(|e| MetricsError::PrometheusErr(e.to_string()))?;
        registry
            .register(Box::new(errors_total.clone()))
            .map_err(|e| MetricsError::PrometheusErr(e.to_string()))?;
        registry
            .register(Box::new(messages_total.clone()))
            .map_err(|e| MetricsError::PrometheusErr(e.to_string()))?;

        Ok(MetricsConns {
            registry,
            connections_tracker,
            ack_response_time_ms,
            ack_timeout_count,
            scheduled_devices_total,
            devices_per_bucket,
            errors_total,
            messages_total,
        })
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
