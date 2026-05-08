//! Heartbeat sender task for the cluster.

use std::sync::Arc;
use std::time::Duration;

use tokio::net::UdpSocket;
use tokio::sync::RwLock;
use tracing::{info, warn};

use metrics::metrics_cluster::METRICS_CLUSTER;

use crate::membership::{MembershipList, broadcast_message};
use crate::protocol::ClusterMessage;

/// Heartbeat sender task.
///
/// Runs in the background and sends heartbeats to all known nodes periodically.
pub async fn heartbeat_loop(
    membership: Arc<RwLock<MembershipList>>,
    socket: Arc<UdpSocket>,
    interval: Duration,
) {
    let mut ticker = tokio::time::interval(interval);

    loop {
        ticker.tick().await;
        info!("Sending heartbeat to peers");

        let seq = membership.write().await.next_seq();
        let (node_id, payload) = {
            let m = membership.read().await;
            (m.local_node_id(), m.create_heartbeat_payload())
        };

        let msg = ClusterMessage::heartbeat(node_id, seq, payload);

        if let Err(e) = broadcast_message(&membership, &socket, msg).await {
            warn!("Failed to broadcast heartbeat: {}", e);
        } else {
            METRICS_CLUSTER.cluster_heartbeat_sent_total.inc();
            METRICS_CLUSTER
                .cluster_messages_total
                .with_label_values(&["Heartbeat", "outbound"])
                .inc();
        }
    }
}
