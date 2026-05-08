//! Prometheus metrics for the wire protocol layer (Phase 3 — Domain B).

use std::collections::HashMap;
use std::sync::LazyLock;

use prometheus::{
    Encoder, HistogramOpts, HistogramVec, IntCounter, IntCounterVec, Opts, Registry, TextEncoder,
};

use crate::MetricsError;

/// Global singleton for protocol-level metrics.
pub static METRICS_PROTOCOL: LazyLock<MetricsProtocol> = LazyLock::new(|| {
    MetricsProtocol::try_new()
        .unwrap_or_else(|e| panic!("Failed to initialize protocol metrics: {e}"))
});

/// Prometheus metrics for wire-protocol operations.
///
/// Phase 3 metrics (domain B — protocol/messaging):
/// - `hes_message_size_bytes`: Histogram of message wire sizes by msg_type
/// - `hes_message_decode_errors_total`: Decode failure counter by error_kind
/// - `hes_mac_verification_total`: MAC check outcome counter (valid/invalid/skipped)
/// - `hes_sequence_replay_detected_total`: Replay-attack detection counter
/// - `hes_obis_read_total`: OBIS code counter from ReadResponse payloads
#[derive(Debug)]
pub struct MetricsProtocol {
    registry: Registry,

    /// Wire size of each message in bytes. Label: msg_type.
    ///
    /// Recorded on inbound messages after a successful decode and on outbound
    /// messages after a successful encode (using the encoded buffer length).
    pub hes_message_size_bytes: HistogramVec,

    /// Decode failures. Label: error_kind ∈ {invalid_length, unknown_msg_type,
    /// payload_decode, io}.
    pub hes_message_decode_errors_total: IntCounterVec,

    /// MAC tag outcomes. Label: result ∈ {valid, invalid, skipped}.
    ///
    /// Currently always "skipped" — HMAC-SHA256 verification is not yet
    /// implemented (issue #9).
    pub hes_mac_verification_total: IntCounterVec,

    /// Sequence-number replay attacks detected (seq ≤ last_seen within a session).
    ///
    /// Currently always zero — per-session sequence tracking is not yet implemented.
    pub hes_sequence_replay_detected_total: IntCounter,

    /// OBIS codes read from device ReadResponse payloads. Label: obis_code
    /// (e.g. "1.0.1", "C.6.1", "0.9.4").
    ///
    /// Incremented once per ObisValue in a successfully decoded ReadResponse.
    pub hes_obis_read_total: IntCounterVec,
}

impl MetricsProtocol {
    pub fn try_new() -> Result<Self, MetricsError> {
        let mut const_labels = HashMap::new();
        const_labels.insert("node_id".to_string(), crate::node_id().to_string());
        let registry = Registry::new_custom(None, Some(const_labels))
            .map_err(|e| MetricsError::PrometheusErr(e.to_string()))?;

        // Protocol messages are 46 bytes minimum (header + MAC, no payload).
        // Typical payloads add 10-100 bytes; 1 KB is a hard upper bound for UDP.
        let msg_size_buckets = vec![46.0, 64.0, 96.0, 128.0, 256.0, 512.0, 1024.0];
        let hes_message_size_bytes = HistogramVec::new(
            HistogramOpts::new(
                "hes_message_size_bytes",
                "Wire size of protocol messages in bytes",
            )
            .buckets(msg_size_buckets),
            &["msg_type"],
        )
        .map_err(|e| MetricsError::PrometheusErr(e.to_string()))?;

        let hes_message_decode_errors_total = IntCounterVec::new(
            Opts::new(
                "hes_message_decode_errors_total",
                "Protocol message decode failures by error kind",
            ),
            &["error_kind"],
        )
        .map_err(|e| MetricsError::PrometheusErr(e.to_string()))?;

        let hes_mac_verification_total = IntCounterVec::new(
            Opts::new(
                "hes_mac_verification_total",
                "MAC tag verification outcomes (valid / invalid / skipped)",
            ),
            &["result"],
        )
        .map_err(|e| MetricsError::PrometheusErr(e.to_string()))?;

        let hes_sequence_replay_detected_total = IntCounter::new(
            "hes_sequence_replay_detected_total",
            "Sequence-number replay attacks detected",
        )
        .map_err(|e| MetricsError::PrometheusErr(e.to_string()))?;

        let hes_obis_read_total = IntCounterVec::new(
            Opts::new(
                "hes_obis_read_total",
                "OBIS codes read from device ReadResponse payloads",
            ),
            &["obis_code"],
        )
        .map_err(|e| MetricsError::PrometheusErr(e.to_string()))?;

        for m in [
            Box::new(hes_message_size_bytes.clone()) as Box<dyn prometheus::core::Collector>,
            Box::new(hes_message_decode_errors_total.clone()),
            Box::new(hes_mac_verification_total.clone()),
            Box::new(hes_sequence_replay_detected_total.clone()),
            Box::new(hes_obis_read_total.clone()),
        ] {
            registry
                .register(m)
                .map_err(|e| MetricsError::PrometheusErr(e.to_string()))?;
        }

        Ok(MetricsProtocol {
            registry,
            hes_message_size_bytes,
            hes_message_decode_errors_total,
            hes_mac_verification_total,
            hes_sequence_replay_detected_total,
            hes_obis_read_total,
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
