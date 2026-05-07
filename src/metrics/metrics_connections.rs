use std::collections::HashMap;
use std::sync::LazyLock;

use prometheus::{
    Encoder, Histogram, HistogramOpts, HistogramVec, IntCounter, IntCounterVec, IntGauge,
    IntGaugeVec, Opts, Registry, TextEncoder,
};

use crate::MetricsError;

pub static METRICS_CONNECTIONS: LazyLock<MetricsConns> =
    LazyLock::new(|| MetricsConns::try_new().unwrap_or_else(|e| panic!("Failed to initialize connection metrics: {e}")));

/// Prometheus metrics for tracking HES operations.
///
/// Phase 0 metrics (baseline):
/// - `connections_tracker`: Counter for connection events by type
/// - `ack_response_time_ms`: Histogram for registration ACK round-trip time
/// - `ack_timeout_count`: Counter for registration timeouts
/// - `scheduled_devices_total`: Gauge for total scheduled devices
/// - `devices_per_bucket`: Gauge per scheduler bucket
/// - `errors_total`: Counter for errors by component and type
/// - `messages_total`: Counter for messages by type and direction
///
/// Phase 1 metrics (domain A — periodic sessions):
/// - `hes_device_connection_outcome_total`: Session outcome counter by outcome label
/// - `hes_device_retry_count_total`: Total session retries across all devices
/// - `hes_device_session_active`: Gauge of concurrently running sessions
///
/// Phase 1 metrics (domain D — scheduler):
/// - `hes_scheduler_job_execution_duration_ms`: Histogram of full job execution time
/// - `hes_scheduler_wake_drift_ms`: Histogram of (actual_wake − scheduled_wake)
/// - `hes_scheduler_overdue_devices_total`: Gauge of devices found past-due at reload
///
/// Phase 1 metrics (domain E — backdoor/registration):
/// - `hes_registration_total`: Registration outcome counter (success/ack_timeout/db_error)
/// - `hes_registration_duration_ms`: Histogram from RegisterRequest to ACK received
///
/// Phase 1 metrics (domain F — database):
/// - `hes_db_query_duration_ms`: Histogram of query latency by (query_name, result)
/// - `hes_db_errors_total`: Counter of DB errors by query_name
#[derive(Debug)]
pub struct MetricsConns {
    registry: Registry,

    // ── Phase 0 ──────────────────────────────────────────────────────────────
    pub connections_tracker: IntCounterVec,
    pub ack_response_time_ms: Histogram,
    pub ack_timeout_count: IntCounter,
    pub scheduled_devices_total: IntGauge,
    pub devices_per_bucket: IntGaugeVec,
    pub errors_total: IntCounterVec,
    pub messages_total: IntCounterVec,

    // ── Phase 1 — domain A (periodic sessions) ────────────────────────────────
    /// Outcome of each periodic device session. Labels: outcome ∈
    /// {success, max_retries, no_ip}.
    pub hes_device_connection_outcome_total: IntCounterVec,
    /// Total session-level retries (incremented once per failed attempt).
    pub hes_device_retry_count_total: IntCounter,
    /// Gauge of sessions currently in-flight.
    pub hes_device_session_active: IntGauge,

    // ── Phase 1 — domain D (scheduler internals) ─────────────────────────────
    /// Wall-clock time from job fire to wake_up_device return, in milliseconds.
    pub hes_scheduler_job_execution_duration_ms: Histogram,
    /// Positive difference (actual_fire_time − scheduled_time) in milliseconds.
    pub hes_scheduler_wake_drift_ms: Histogram,
    /// Devices found past their next_wake time at reload_active_connections.
    pub hes_scheduler_overdue_devices_total: IntGauge,

    // ── Phase 1 — domain E (backdoor / registration) ─────────────────────────
    /// Count of completed registrations by outcome. Labels: outcome ∈
    /// {success, ack_timeout, db_error}.
    pub hes_registration_total: IntCounterVec,
    /// Histogram of full registration flow (RegisterRequest → ACK received).
    pub hes_registration_duration_ms: Histogram,

    // ── Phase 1 — domain F (database) ────────────────────────────────────────
    /// Query latency in milliseconds. Labels: (query_name, result ∈ {ok, error}).
    pub hes_db_query_duration_ms: HistogramVec,
    /// Count of DB errors by query_name.
    pub hes_db_errors_total: IntCounterVec,
}

impl MetricsConns {
    pub fn try_new() -> Result<Self, MetricsError> {
        let mut const_labels = HashMap::new();
        const_labels.insert("node_id".to_string(), crate::node_id().to_string());
        let registry = Registry::new_custom(None, Some(const_labels))
            .map_err(|e| MetricsError::PrometheusErr(e.to_string()))?;

        // ── Phase 0 ──────────────────────────────────────────────────────────
        let connections_tracker = IntCounterVec::new(
            Opts::new("connections_tracker", "Keeps track of all connections"),
            &["type"],
        )
        .map_err(|e| MetricsError::PrometheusErr(e.to_string()))?;
        connections_tracker
            .with_label_values(&["new_connection"])
            .inc_by(0);

        let latency_buckets = vec![10.0, 25.0, 50.0, 100.0, 250.0, 500.0, 1000.0, 2500.0, 5000.0, 10000.0];

        let ack_response_time_ms = Histogram::with_opts(
            HistogramOpts::new("ack_response_time_ms", "ACK response time in milliseconds")
                .buckets(latency_buckets.clone()),
        )
        .map_err(|e| MetricsError::PrometheusErr(e.to_string()))?;

        let ack_timeout_count =
            IntCounter::new("ack_timeout_count", "ACKs that exceeded the timeout threshold")
                .map_err(|e| MetricsError::PrometheusErr(e.to_string()))?;

        let scheduled_devices_total =
            IntGauge::new("scheduled_devices_total", "Total devices with scheduled connections")
                .map_err(|e| MetricsError::PrometheusErr(e.to_string()))?;

        let devices_per_bucket = IntGaugeVec::new(
            Opts::new("devices_per_bucket", "Devices assigned to each scheduler bucket"),
            &["bucket"],
        )
        .map_err(|e| MetricsError::PrometheusErr(e.to_string()))?;

        let errors_total = IntCounterVec::new(
            Opts::new("errors_total", "Total errors by component and type"),
            &["component", "error_type"],
        )
        .map_err(|e| MetricsError::PrometheusErr(e.to_string()))?;

        let messages_total = IntCounterVec::new(
            Opts::new("messages_total", "Total messages by type and direction"),
            &["msg_type", "direction"],
        )
        .map_err(|e| MetricsError::PrometheusErr(e.to_string()))?;

        // ── Phase 1 — domain A ────────────────────────────────────────────────
        let hes_device_connection_outcome_total = IntCounterVec::new(
            Opts::new(
                "hes_device_connection_outcome_total",
                "Periodic session outcomes by result type",
            ),
            &["outcome"],
        )
        .map_err(|e| MetricsError::PrometheusErr(e.to_string()))?;

        let hes_device_retry_count_total = IntCounter::new(
            "hes_device_retry_count_total",
            "Total session retries across all devices",
        )
        .map_err(|e| MetricsError::PrometheusErr(e.to_string()))?;

        let hes_device_session_active =
            IntGauge::new("hes_device_session_active", "Currently active periodic sessions")
                .map_err(|e| MetricsError::PrometheusErr(e.to_string()))?;

        // ── Phase 1 — domain D ────────────────────────────────────────────────
        let job_exec_buckets = vec![
            500.0, 1_000.0, 5_000.0, 10_000.0, 30_000.0, 60_000.0, 120_000.0, 300_000.0,
        ];

        let hes_scheduler_job_execution_duration_ms = Histogram::with_opts(
            HistogramOpts::new(
                "hes_scheduler_job_execution_duration_ms",
                "Wall-clock time to execute a full wake_up_device job in milliseconds",
            )
            .buckets(job_exec_buckets),
        )
        .map_err(|e| MetricsError::PrometheusErr(e.to_string()))?;

        let wake_drift_buckets = vec![100.0, 500.0, 1_000.0, 2_000.0, 5_000.0, 10_000.0, 30_000.0];

        let hes_scheduler_wake_drift_ms = Histogram::with_opts(
            HistogramOpts::new(
                "hes_scheduler_wake_drift_ms",
                "Positive difference between actual job fire time and scheduled time in milliseconds",
            )
            .buckets(wake_drift_buckets),
        )
        .map_err(|e| MetricsError::PrometheusErr(e.to_string()))?;

        let hes_scheduler_overdue_devices_total = IntGauge::new(
            "hes_scheduler_overdue_devices_total",
            "Devices found past their scheduled wake time at startup reload",
        )
        .map_err(|e| MetricsError::PrometheusErr(e.to_string()))?;

        // ── Phase 1 — domain E ────────────────────────────────────────────────
        let hes_registration_total = IntCounterVec::new(
            Opts::new(
                "hes_registration_total",
                "Device registration outcomes (success/ack_timeout/db_error)",
            ),
            &["outcome"],
        )
        .map_err(|e| MetricsError::PrometheusErr(e.to_string()))?;

        let hes_registration_duration_ms = Histogram::with_opts(
            HistogramOpts::new(
                "hes_registration_duration_ms",
                "Full registration round-trip (RegisterRequest to ACK received) in milliseconds",
            )
            .buckets(latency_buckets),
        )
        .map_err(|e| MetricsError::PrometheusErr(e.to_string()))?;

        // ── Phase 1 — domain F ────────────────────────────────────────────────
        let db_query_buckets = vec![1.0, 5.0, 10.0, 25.0, 50.0, 100.0, 250.0, 500.0, 1_000.0];

        let hes_db_query_duration_ms = HistogramVec::new(
            HistogramOpts::new(
                "hes_db_query_duration_ms",
                "Database query latency in milliseconds",
            )
            .buckets(db_query_buckets),
            &["query_name", "result"],
        )
        .map_err(|e| MetricsError::PrometheusErr(e.to_string()))?;

        let hes_db_errors_total = IntCounterVec::new(
            Opts::new("hes_db_errors_total", "Database errors by query name"),
            &["query_name"],
        )
        .map_err(|e| MetricsError::PrometheusErr(e.to_string()))?;

        // ── Register all metrics ──────────────────────────────────────────────
        for m in [
            Box::new(connections_tracker.clone()) as Box<dyn prometheus::core::Collector>,
            Box::new(ack_response_time_ms.clone()),
            Box::new(ack_timeout_count.clone()),
            Box::new(scheduled_devices_total.clone()),
            Box::new(devices_per_bucket.clone()),
            Box::new(errors_total.clone()),
            Box::new(messages_total.clone()),
            Box::new(hes_device_connection_outcome_total.clone()),
            Box::new(hes_device_retry_count_total.clone()),
            Box::new(hes_device_session_active.clone()),
            Box::new(hes_scheduler_job_execution_duration_ms.clone()),
            Box::new(hes_scheduler_wake_drift_ms.clone()),
            Box::new(hes_scheduler_overdue_devices_total.clone()),
            Box::new(hes_registration_total.clone()),
            Box::new(hes_registration_duration_ms.clone()),
            Box::new(hes_db_query_duration_ms.clone()),
            Box::new(hes_db_errors_total.clone()),
        ] {
            registry
                .register(m)
                .map_err(|e| MetricsError::PrometheusErr(e.to_string()))?;
        }

        Ok(MetricsConns {
            registry,
            connections_tracker,
            ack_response_time_ms,
            ack_timeout_count,
            scheduled_devices_total,
            devices_per_bucket,
            errors_total,
            messages_total,
            hes_device_connection_outcome_total,
            hes_device_retry_count_total,
            hes_device_session_active,
            hes_scheduler_job_execution_duration_ms,
            hes_scheduler_wake_drift_ms,
            hes_scheduler_overdue_devices_total,
            hes_registration_total,
            hes_registration_duration_ms,
            hes_db_query_duration_ms,
            hes_db_errors_total,
        })
    }

    pub fn gather_metrics(&self) -> Result<String, MetricsError> {
        let encoder = TextEncoder::new();
        let metric_families = self.registry.gather();

        let mut buffer = Vec::new();
        encoder
            .encode(&metric_families, &mut buffer)
            .map_err(|e| MetricsError::PrometheusErr(e.to_string()))?;

        Ok(String::from_utf8(buffer)?)
    }
}
