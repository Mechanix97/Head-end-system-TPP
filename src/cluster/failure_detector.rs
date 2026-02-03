//! SWIM-like failure detection for the cluster.
//!
//! Detects failed nodes using:
//! 1. Direct heartbeat monitoring (60s timeout)
//! 2. Indirect probing through other nodes
//! 3. Broadcast confirmation before declaring dead

use std::sync::Arc;
use std::time::Duration;

use tokio::net::UdpSocket;
use tokio::sync::RwLock;
use tracing::{debug, info, warn};
use uuid::Uuid;

use crate::device_manager::DeviceManager;
use crate::membership::{broadcast_message, send_message, MembershipList};
use crate::node::ClusterConfig;
use crate::protocol::ClusterMessage;

/// Runs the failure detection loop.
///
/// Periodically checks for nodes that have exceeded timeout thresholds and
/// initiates the failure detection protocol.
pub async fn failure_detector_loop(
    membership: Arc<RwLock<MembershipList>>,
    device_manager: Arc<RwLock<DeviceManager>>,
    socket: Arc<UdpSocket>,
    config: ClusterConfig,
) {
    // Check more frequently than the suspect timeout to catch failures quickly
    let check_interval = Duration::from_secs(10);
    let mut ticker = tokio::time::interval(check_interval);

    loop {
        ticker.tick().await;

        // Check for suspect candidates (nodes without heartbeat for suspect_timeout)
        let suspect_candidates = {
            let membership = membership.read().await;
            membership.get_suspect_candidates()
        };

        for node_id in suspect_candidates {
            handle_suspect_node(
                node_id,
                &membership,
                &device_manager,
                &socket,
                &config,
            )
            .await;
        }

        // Check for dead candidates (suspect nodes that exceeded dead_timeout)
        let dead_candidates = {
            let membership = membership.read().await;
            membership.get_dead_candidates()
        };

        for node_id in dead_candidates {
            handle_dead_node(node_id, &membership, &device_manager, &socket).await;
        }
    }
}

/// Handles a node that has become suspect.
async fn handle_suspect_node(
    node_id: Uuid,
    membership: &RwLock<MembershipList>,
    _device_manager: &RwLock<DeviceManager>,
    socket: &UdpSocket,
    config: &ClusterConfig,
) {
    // Get node info and check if still suspect-worthy
    let node_name = {
        let mut membership = membership.write().await;
        if let Some(node) = membership.get_node(node_id) {
            if node.time_since_heartbeat() <= config.suspect_timeout {
                return; // Received heartbeat, no longer suspect
            }
            let name = node.node_name.clone();
            membership.mark_suspect(node_id);
            name
        } else {
            return;
        }
    };

    warn!("Node {} is suspected to be down, initiating probe", node_name);

    // Broadcast NODE_SUSPECT
    let (local_id, seq) = {
        let mut membership = membership.write().await;
        let local_id = membership.local_node_id();
        let seq = membership.next_seq();
        (local_id, seq)
    };

    let suspect_msg = ClusterMessage::node_suspect(local_id, seq, node_id);
    if let Err(e) = broadcast_message(membership, socket, suspect_msg).await {
        warn!("Failed to broadcast NODE_SUSPECT: {}", e);
    }

    // Try indirect probe through another node
    let probe_result = indirect_probe(node_id, membership, socket, config).await;

    if probe_result {
        // Node is alive, update heartbeat
        info!("Node {} responded to indirect probe, marking as active", node_name);
        let mut membership = membership.write().await;
        if let Some(node) = membership.get_node_mut(node_id) {
            node.update_heartbeat();
            node.status = crate::node::NodeStatus::Active;
        }
    } else {
        debug!(
            "Node {} failed indirect probe, will be marked dead after timeout",
            node_name
        );
    }
}

/// Handles a node that has been confirmed dead.
async fn handle_dead_node(
    node_id: Uuid,
    membership: &RwLock<MembershipList>,
    device_manager: &RwLock<DeviceManager>,
    socket: &UdpSocket,
) {
    let node_name = {
        let mut membership = membership.write().await;
        if let Some(node) = membership.get_node_mut(node_id) {
            let name = node.node_name.clone();
            membership.mark_dead(node_id);
            name
        } else {
            return;
        }
    };

    warn!("Node {} confirmed dead, initiating redistribution", node_name);

    // Broadcast NODE_DEAD
    let (local_id, seq) = {
        let mut membership = membership.write().await;
        let local_id = membership.local_node_id();
        let seq = membership.next_seq();
        (local_id, seq)
    };

    let dead_msg = ClusterMessage::node_dead(local_id, seq, node_id);
    if let Err(e) = broadcast_message(membership, socket, dead_msg).await {
        warn!("Failed to broadcast NODE_DEAD: {}", e);
    }

    // Trigger device redistribution
    let redistribute_result = {
        let mut device_manager = device_manager.write().await;
        device_manager.redistribute_from_failed(node_id).await
    };

    if let Err(e) = redistribute_result {
        warn!("Failed to redistribute devices from dead node: {}", e);
    }

    // Remove node from membership list
    let removed = {
        let mut membership = membership.write().await;
        membership.remove_node(node_id)
    };

    if let Some(node) = removed {
        info!(
            "Removed dead node {} from membership (had {} devices)",
            node.node_name, node.device_count
        );
    }
}

/// Performs an indirect probe through another node.
///
/// Asks a random healthy node to probe the suspect node and report back.
async fn indirect_probe(
    target_node_id: Uuid,
    membership: &RwLock<MembershipList>,
    socket: &UdpSocket,
    config: &ClusterConfig,
) -> bool {
    // Select a random active node to do the probe
    let probe_helper = {
        let membership = membership.read().await;
        let active_nodes: Vec<_> = membership
            .active_nodes()
            .into_iter()
            .filter(|n| n.node_id != target_node_id)
            .collect();

        if active_nodes.is_empty() {
            return false;
        }

        // Pick a random node
        let idx = rand::random::<usize>() % active_nodes.len();
        active_nodes[idx].cluster_addr
    };

    // Send probe request
    let (local_id, seq) = {
        let mut membership = membership.write().await;
        let local_id = membership.local_node_id();
        let seq = membership.next_seq();
        (local_id, seq)
    };

    let probe_msg = ClusterMessage::probe_request(local_id, seq, target_node_id);

    if let Err(e) = send_message(socket, probe_helper, probe_msg).await {
        warn!("Failed to send probe request: {}", e);
        return false;
    }

    // TODO: Track pending probes and match responses properly
    // Wait for response (with timeout)
    // In a real implementation, we'd need to track pending probes and match responses
    // For now, we just wait a short time and check if the target node's heartbeat was updated
    tokio::time::sleep(Duration::from_secs(5)).await;

    

    {
        let membership = membership.read().await;
        if let Some(node) = membership.get_node(target_node_id) {
            node.time_since_heartbeat() < config.suspect_timeout
        } else {
            false
        }
    }
}

/// Handles a received probe request.
///
/// Probes the target node and sends a response.
pub async fn handle_probe_request(
    target_node_id: Uuid,
    requester_addr: std::net::SocketAddr,
    membership: &RwLock<MembershipList>,
    socket: &UdpSocket,
) {
    // Check if we know the target node
    let target_addr = {
        let membership = membership.read().await;
        membership.get_node(target_node_id).map(|n| n.cluster_addr)
    };

    let is_alive = if let Some(addr) = target_addr {
        // Try to ping the target directly
        let (local_id, seq) = {
            let mut membership = membership.write().await;
            let local_id = membership.local_node_id();
            let seq = membership.next_seq();
            (local_id, seq)
        };

        let ping_msg = ClusterMessage::status_request(local_id, seq);

        // Send ping and wait for response
        if send_message(socket, addr, ping_msg).await.is_ok() {
            // Wait briefly for response
            tokio::time::sleep(Duration::from_secs(2)).await;

            // Check if heartbeat was updated
            let membership = membership.read().await;
            if let Some(node) = membership.get_node(target_node_id) {
                node.time_since_heartbeat() < Duration::from_secs(5)
            } else {
                false
            }
        } else {
            false
        }
    } else {
        false
    };

    // Send probe response
    let (local_id, seq) = {
        let mut membership = membership.write().await;
        let local_id = membership.local_node_id();
        let seq = membership.next_seq();
        (local_id, seq)
    };

    let response = ClusterMessage::probe_response(local_id, seq, target_node_id, is_alive);
    if let Err(e) = send_message(socket, requester_addr, response).await {
        warn!("Failed to send probe response: {}", e);
    }
}
