//! Prometheus metrics for cluster operations.

use prometheus::{Encoder, IntCounter, IntCounterVec, IntGauge, IntGaugeVec, Opts, Registry};
use std::sync::LazyLock;

use crate::MetricsError;

/// Global singleton instance of cluster metrics.
pub static METRICS_CLUSTER: LazyLock<MetricsCluster> =
    LazyLock::new(|| MetricsCluster::try_new().unwrap_or_else(|e| panic!("Failed to initialize cluster metrics: {e}")));

/// Prometheus metrics for tracking cluster operations.
///
/// Tracks:
/// - `cluster_nodes_total`: Total number of nodes in the cluster
/// - `cluster_nodes_active`: Number of active nodes
/// - `cluster_buckets_owned`: Number of buckets owned by this node
/// - `cluster_devices_owned`: Number of devices managed by this node
/// - `cluster_heartbeat_sent_total`: Total heartbeats sent
/// - `cluster_heartbeat_received_total`: Total heartbeats received
/// - `cluster_heartbeat_timeout_total`: Total heartbeat timeouts
/// - `cluster_failovers_total`: Total node failovers
/// - `cluster_delegations_total`: Total bucket delegations
/// - `cluster_rebalances_total`: Total cluster rebalances
/// - `cluster_messages_total`: Total cluster messages by type
#[derive(Debug)]
pub struct MetricsCluster {
    /// Prometheus registry
    registry: Registry,

    // Node metrics
    /// Total number of nodes in the cluster
    pub cluster_nodes_total: IntGauge,
    /// Number of active nodes
    pub cluster_nodes_active: IntGauge,
    /// Number of buckets owned by this node
    pub cluster_buckets_owned: IntGauge,
    /// Number of devices managed by this node
    pub cluster_devices_owned: IntGauge,

    // Heartbeat metrics
    /// Total heartbeats sent
    pub cluster_heartbeat_sent_total: IntCounter,
    /// Total heartbeats received
    pub cluster_heartbeat_received_total: IntCounter,
    /// Total heartbeat timeouts
    pub cluster_heartbeat_timeout_total: IntCounter,

    // Failover metrics
    /// Total node failovers
    pub cluster_failovers_total: IntCounter,
    /// Total bucket delegations
    pub cluster_delegations_total: IntCounter,
    /// Total cluster rebalances
    pub cluster_rebalances_total: IntCounter,

    // Message metrics
    /// Total cluster messages by type (label: msg_type, direction)
    pub cluster_messages_total: IntCounterVec,

    // Per-node metrics
    /// Buckets per node (label: node_id)
    pub cluster_node_buckets: IntGaugeVec,
    /// Devices per node (label: node_id)
    pub cluster_node_devices: IntGaugeVec,
    /// Load percentage per node (label: node_id)
    pub cluster_node_load_percent: IntGaugeVec,
}

impl MetricsCluster {
    /// Creates a new cluster metrics collector.
    pub fn try_new() -> Result<Self, MetricsError> {
        let registry = Registry::new();

        // Node metrics
        let cluster_nodes_total = IntGauge::new(
            "hes_cluster_nodes_total",
            "Total number of nodes in the cluster",
        )
        .map_err(|e| MetricsError::PrometheusErr(e.to_string()))?;

        let cluster_nodes_active = IntGauge::new(
            "hes_cluster_nodes_active",
            "Number of active nodes in the cluster",
        )
        .map_err(|e| MetricsError::PrometheusErr(e.to_string()))?;

        let cluster_buckets_owned = IntGauge::new(
            "hes_cluster_buckets_owned",
            "Number of buckets owned by this node",
        )
        .map_err(|e| MetricsError::PrometheusErr(e.to_string()))?;

        let cluster_devices_owned = IntGauge::new(
            "hes_cluster_devices_owned",
            "Number of devices managed by this node",
        )
        .map_err(|e| MetricsError::PrometheusErr(e.to_string()))?;

        // Heartbeat metrics
        let cluster_heartbeat_sent_total = IntCounter::new(
            "hes_cluster_heartbeat_sent_total",
            "Total heartbeats sent by this node",
        )
        .map_err(|e| MetricsError::PrometheusErr(e.to_string()))?;

        let cluster_heartbeat_received_total = IntCounter::new(
            "hes_cluster_heartbeat_received_total",
            "Total heartbeats received by this node",
        )
        .map_err(|e| MetricsError::PrometheusErr(e.to_string()))?;

        let cluster_heartbeat_timeout_total = IntCounter::new(
            "hes_cluster_heartbeat_timeout_total",
            "Total heartbeat timeouts detected",
        )
        .map_err(|e| MetricsError::PrometheusErr(e.to_string()))?;

        // Failover metrics
        let cluster_failovers_total = IntCounter::new(
            "hes_cluster_failovers_total",
            "Total node failovers handled",
        )
        .map_err(|e| MetricsError::PrometheusErr(e.to_string()))?;

        let cluster_delegations_total = IntCounter::new(
            "hes_cluster_delegations_total",
            "Total bucket delegations performed",
        )
        .map_err(|e| MetricsError::PrometheusErr(e.to_string()))?;

        let cluster_rebalances_total = IntCounter::new(
            "hes_cluster_rebalances_total",
            "Total cluster rebalances performed",
        )
        .map_err(|e| MetricsError::PrometheusErr(e.to_string()))?;

        // Message metrics
        let cluster_messages_total = IntCounterVec::new(
            Opts::new(
                "hes_cluster_messages_total",
                "Total cluster messages by type and direction",
            ),
            &["msg_type", "direction"],
        )
        .map_err(|e| MetricsError::PrometheusErr(e.to_string()))?;

        // Per-node metrics
        let cluster_node_buckets = IntGaugeVec::new(
            Opts::new(
                "hes_cluster_node_buckets",
                "Number of buckets per node",
            ),
            &["node_id"],
        )
        .map_err(|e| MetricsError::PrometheusErr(e.to_string()))?;

        let cluster_node_devices = IntGaugeVec::new(
            Opts::new(
                "hes_cluster_node_devices",
                "Number of devices per node",
            ),
            &["node_id"],
        )
        .map_err(|e| MetricsError::PrometheusErr(e.to_string()))?;

        let cluster_node_load_percent = IntGaugeVec::new(
            Opts::new(
                "hes_cluster_node_load_percent",
                "Load percentage per node (0-100)",
            ),
            &["node_id"],
        )
        .map_err(|e| MetricsError::PrometheusErr(e.to_string()))?;

        // Register all metrics
        registry
            .register(Box::new(cluster_nodes_total.clone()))
            .map_err(|e| MetricsError::PrometheusErr(e.to_string()))?;
        registry
            .register(Box::new(cluster_nodes_active.clone()))
            .map_err(|e| MetricsError::PrometheusErr(e.to_string()))?;
        registry
            .register(Box::new(cluster_buckets_owned.clone()))
            .map_err(|e| MetricsError::PrometheusErr(e.to_string()))?;
        registry
            .register(Box::new(cluster_devices_owned.clone()))
            .map_err(|e| MetricsError::PrometheusErr(e.to_string()))?;
        registry
            .register(Box::new(cluster_heartbeat_sent_total.clone()))
            .map_err(|e| MetricsError::PrometheusErr(e.to_string()))?;
        registry
            .register(Box::new(cluster_heartbeat_received_total.clone()))
            .map_err(|e| MetricsError::PrometheusErr(e.to_string()))?;
        registry
            .register(Box::new(cluster_heartbeat_timeout_total.clone()))
            .map_err(|e| MetricsError::PrometheusErr(e.to_string()))?;
        registry
            .register(Box::new(cluster_failovers_total.clone()))
            .map_err(|e| MetricsError::PrometheusErr(e.to_string()))?;
        registry
            .register(Box::new(cluster_delegations_total.clone()))
            .map_err(|e| MetricsError::PrometheusErr(e.to_string()))?;
        registry
            .register(Box::new(cluster_rebalances_total.clone()))
            .map_err(|e| MetricsError::PrometheusErr(e.to_string()))?;
        registry
            .register(Box::new(cluster_messages_total.clone()))
            .map_err(|e| MetricsError::PrometheusErr(e.to_string()))?;
        registry
            .register(Box::new(cluster_node_buckets.clone()))
            .map_err(|e| MetricsError::PrometheusErr(e.to_string()))?;
        registry
            .register(Box::new(cluster_node_devices.clone()))
            .map_err(|e| MetricsError::PrometheusErr(e.to_string()))?;
        registry
            .register(Box::new(cluster_node_load_percent.clone()))
            .map_err(|e| MetricsError::PrometheusErr(e.to_string()))?;

        Ok(MetricsCluster {
            registry,
            cluster_nodes_total,
            cluster_nodes_active,
            cluster_buckets_owned,
            cluster_devices_owned,
            cluster_heartbeat_sent_total,
            cluster_heartbeat_received_total,
            cluster_heartbeat_timeout_total,
            cluster_failovers_total,
            cluster_delegations_total,
            cluster_rebalances_total,
            cluster_messages_total,
            cluster_node_buckets,
            cluster_node_devices,
            cluster_node_load_percent,
        })
    }

    /// Gathers all cluster metrics and encodes them in Prometheus text format.
    pub fn gather_metrics(&self) -> Result<String, MetricsError> {
        let encoder = prometheus::TextEncoder::new();
        let metric_families = self.registry.gather();

        let mut buffer = Vec::new();
        encoder
            .encode(&metric_families, &mut buffer)
            .map_err(|e| MetricsError::PrometheusErr(e.to_string()))?;

        let res = String::from_utf8(buffer)?;

        Ok(res)
    }
}
