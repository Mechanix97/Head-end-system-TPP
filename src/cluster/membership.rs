//! Membership list management for the cluster.
//!
//! Tracks all known nodes in the cluster and handles heartbeat logic.

use std::collections::HashMap;
use std::net::SocketAddr;

use tokio::net::UdpSocket;
use tokio::sync::RwLock;
use tracing::{info, warn};
use uuid::Uuid;

use crate::error::ClusterError;
use crate::node::{ClusterConfig, NodeInfo, NodeStatus};
use crate::protocol::{ClusterMessage, ClusterMessageCodec, HeartbeatPayload, NodeJoinPayload};

use bytes::BytesMut;
use tokio_util::codec::Encoder;

/// Manages the list of nodes in the cluster.
#[derive(Debug)]
pub struct MembershipList {
    /// This node's information
    local_node: NodeInfo,
    /// Map of node ID to node info for all known nodes (excluding self)
    nodes: HashMap<Uuid, NodeInfo>,
    /// Sequence counter for outgoing messages
    seq_counter: u32,
    /// Configuration
    config: ClusterConfig,
}

impl MembershipList {
    /// Creates a new membership list with just the local node.
    pub fn new(config: ClusterConfig) -> Result<Self, ClusterError> {
        let cluster_addr = config.cluster_addr()?;
        let local_node = NodeInfo::new_local(
            config.node_id,
            config.node_name.clone(),
            cluster_addr,
            config.backdoor_port,
        );

        Ok(Self {
            local_node,
            nodes: HashMap::new(),
            seq_counter: 0,
            config,
        })
    }

    /// Gets the next sequence number.
    pub fn next_seq(&mut self) -> u32 {
        self.seq_counter = self.seq_counter.wrapping_add(1);
        self.seq_counter
    }

    /// Gets this node's ID.
    pub fn local_node_id(&self) -> Uuid {
        self.local_node.node_id
    }

    /// Gets this node's info.
    pub fn local_node(&self) -> &NodeInfo {
        &self.local_node
    }

    /// Gets mutable reference to local node info.
    pub fn local_node_mut(&mut self) -> &mut NodeInfo {
        &mut self.local_node
    }

    /// Gets all known nodes (excluding self).
    pub fn nodes(&self) -> &HashMap<Uuid, NodeInfo> {
        &self.nodes
    }

    /// Gets all active nodes (excluding self).
    pub fn active_nodes(&self) -> Vec<&NodeInfo> {
        self.nodes.values().filter(|n| n.is_available()).collect()
    }

    /// Gets all reachable nodes (excluding self).
    pub fn reachable_nodes(&self) -> Vec<&NodeInfo> {
        self.nodes.values().filter(|n| n.is_reachable()).collect()
    }

    /// Gets a node by ID.
    pub fn get_node(&self, node_id: Uuid) -> Option<&NodeInfo> {
        if node_id == self.local_node.node_id {
            Some(&self.local_node)
        } else {
            self.nodes.get(&node_id)
        }
    }

    /// Gets a mutable reference to a node by ID.
    pub fn get_node_mut(&mut self, node_id: Uuid) -> Option<&mut NodeInfo> {
        if node_id == self.local_node.node_id {
            Some(&mut self.local_node)
        } else {
            self.nodes.get_mut(&node_id)
        }
    }

    /// Adds or updates a node in the membership list.
    pub fn add_or_update_node(&mut self, node: NodeInfo) {
        if node.node_id == self.local_node.node_id {
            return; // Don't add self
        }

        if let Some(existing) = self.nodes.get_mut(&node.node_id) {
            // Update existing node
            existing.status = node.status;
            existing.bucket_count = node.bucket_count;
            existing.device_count = node.device_count;
            existing.load_percent = node.load_percent;
            existing.max_device_suggested = node.max_device_suggested;
            existing.update_heartbeat();
        } else {
            // Add new node
            info!(
                "Adding new node to membership: {} ({})",
                node.node_name, node.node_id
            );
            self.nodes.insert(node.node_id, node);
        }
    }

    /// Updates a node's heartbeat timestamp.
    pub fn update_heartbeat(&mut self, node_id: Uuid) {
        if let Some(node) = self.nodes.get_mut(&node_id) {
            node.update_heartbeat();
        }
    }

    /// Marks a node as suspect.
    pub fn mark_suspect(&mut self, node_id: Uuid) {
        if let Some(node) = self.nodes.get_mut(&node_id) {
            if node.status == NodeStatus::Active {
                warn!("Marking node {} as suspect", node.node_name);
                node.status = NodeStatus::Suspect;
            }
        }
    }

    /// Marks a node as dead.
    pub fn mark_dead(&mut self, node_id: Uuid) {
        if let Some(node) = self.nodes.get_mut(&node_id) {
            warn!("Marking node {} as dead", node.node_name);
            node.status = NodeStatus::Dead;
        }
    }

    /// Removes a dead node from the list.
    pub fn remove_node(&mut self, node_id: Uuid) -> Option<NodeInfo> {
        self.nodes.remove(&node_id)
    }

    /// Gets nodes that have exceeded the suspect timeout.
    pub fn get_suspect_candidates(&self) -> Vec<Uuid> {
        self.nodes
            .values()
            .filter(|n| {
                n.status == NodeStatus::Active
                    && n.time_since_heartbeat() > self.config.suspect_timeout
            })
            .map(|n| n.node_id)
            .collect()
    }

    /// Gets nodes that have exceeded the dead timeout while in suspect state.
    pub fn get_dead_candidates(&self) -> Vec<Uuid> {
        self.nodes
            .values()
            .filter(|n| {
                n.status == NodeStatus::Suspect
                    && n.time_since_heartbeat()
                        > self.config.suspect_timeout + self.config.dead_timeout
            })
            .map(|n| n.node_id)
            .collect()
    }

    /// Gets all known node IDs (excluding self).
    pub fn known_node_ids(&self) -> Vec<Uuid> {
        self.nodes.keys().copied().collect()
    }

    /// Creates a heartbeat payload for this node.
    pub fn create_heartbeat_payload(&self) -> HeartbeatPayload {
        HeartbeatPayload {
            status: self.local_node.status,
            bucket_count: self.local_node.bucket_count as u16,
            device_count: self.local_node.device_count,
            load_percent: self.local_node.load_percent as u8,
            max_device_suggested: self.local_node.max_device_suggested,
            known_nodes: self.known_node_ids(),
        }
    }

    /// Creates a node join payload for this node.
    pub fn create_node_join_payload(&self) -> NodeJoinPayload {
        NodeJoinPayload {
            node_name: self.local_node.node_name.clone(),
            cluster_addr: self.local_node.cluster_addr,
            backdoor_port: self.local_node.backdoor_port,
        }
    }

    /// Sets the local node status.
    pub fn set_local_status(&mut self, status: NodeStatus) {
        self.local_node.status = status;
    }

    /// Updates local node stats. Recalculates load_percent as device_count / max_device_suggested * 100.
    pub fn update_local_stats(&mut self, bucket_count: u32, device_count: u32) {
        self.local_node.bucket_count = bucket_count;
        self.local_node.device_count = device_count;
        if self.local_node.max_device_suggested > 0 {
            self.local_node.load_percent =
                (device_count as f32 / self.local_node.max_device_suggested as f32) * 100.0;
        } else {
            self.local_node.load_percent = 0.0;
        }
    }

    /// Gets the total number of nodes (including self).
    pub fn node_count(&self) -> usize {
        self.nodes.len() + 1
    }

    /// Gets the number of active nodes (including self if active).
    pub fn active_node_count(&self) -> usize {
        let self_active = if self.local_node.is_available() { 1 } else { 0 };
        self.active_nodes().len() + self_active
    }
}


/// Sends a single message to a specific node.
pub async fn send_message(
    socket: &UdpSocket,
    target: SocketAddr,
    msg: ClusterMessage,
) -> Result<(), ClusterError> {
    let mut codec = ClusterMessageCodec;
    let mut buf = BytesMut::new();

    codec
        .encode(msg, &mut buf)
        .map_err(|e| ClusterError::CodecError(e.to_string()))?;

    socket
        .send_to(&buf, target)
        .await
        .map_err(|e| ClusterError::CommunicationError(e.to_string()))?;

    Ok(())
}

/// Broadcasts a message to all reachable nodes.
pub async fn broadcast_message(
    membership: &RwLock<MembershipList>,
    socket: &UdpSocket,
    msg: ClusterMessage,
) -> Result<(), ClusterError> {
    let targets: Vec<SocketAddr> = {
        let membership = membership.read().await;
        membership
            .reachable_nodes()
            .iter()
            .map(|n| n.cluster_addr)
            .collect()
    };

    let mut codec = ClusterMessageCodec;
    let mut buf = BytesMut::new();

    codec
        .encode(msg, &mut buf)
        .map_err(|e| ClusterError::CodecError(e.to_string()))?;

    for target in targets {
        if let Err(e) = socket.send_to(&buf, target).await {
            warn!("Failed to broadcast to {}: {}", target, e);
        }
    }

    Ok(())
}
