//! Broadcasts config updates to all cluster peers via UDP.

use bytes::BytesMut;
use tokio_util::codec::Encoder;
use uuid::Uuid;

use cluster::protocol::{
    ClusterMessage, ClusterMessageCodec,
    payload::ConfigUpdatePayload,
};

use crate::state::ClusterManagerHandle;

/// Broadcasts a ConfigUpdate message to all reachable cluster peers.
///
/// Returns the number of peers that were sent the message successfully.
pub async fn broadcast_config_update(
    cluster: &ClusterManagerHandle,
    sender_node_id: Uuid,
    key: &str,
    value: &str,
) -> Result<usize, std::io::Error> {
    let peers: Vec<std::net::SocketAddr> = {
        let m = cluster.membership.read().await;
        m.reachable_nodes()
            .iter()
            .filter(|n| n.node_id != cluster.node_id)
            .map(|n| n.cluster_addr)
            .collect()
    };

    if peers.is_empty() {
        return Ok(0);
    }

    let update_id = Uuid::new_v4();
    let payload = ConfigUpdatePayload {
        update_id,
        key: key.to_string(),
        value: value.to_string(),
    };

    let msg = ClusterMessage::config_update(sender_node_id, 0, payload);

    let mut sent = 0usize;
    let mut codec = ClusterMessageCodec;
    for peer_addr in &peers {
        let mut buf = BytesMut::new();
        if codec.encode(msg.clone(), &mut buf).is_err() {
            continue;
        }
        if cluster.socket.send_to(&buf, peer_addr).await.is_ok() {
            sent += 1;
        }
    }

    Ok(sent)
}
