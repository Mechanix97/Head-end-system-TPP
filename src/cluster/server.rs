//! UDP server for inter-node cluster communication.

use std::net::SocketAddr;
use std::sync::Arc;

use bytes::BytesMut;
use tokio::net::UdpSocket;
use tokio::sync::RwLock;
use tokio_util::codec::Decoder;
use tracing::{debug, info, warn};
use uuid::Uuid;

use crate::bucket_manager::BucketManager;
use crate::delegation::DelegationHandler;
use crate::error::ClusterError;
use crate::failure_detector::handle_probe_request;
use crate::membership::MembershipList;
use crate::node::{NodeInfo, NodeStatus};
use crate::protocol::{
    ClusterMessage, ClusterMessageCodec, ClusterMessageType, ClusterPayload, StatusResponsePayload,
};

use common::database::api::Database;

/// Maximum UDP packet size.
const MAX_PACKET_SIZE: usize = 65535;

/// Runs the cluster server that handles incoming messages from other nodes.
pub async fn run_cluster_server(
    socket: Arc<UdpSocket>,
    membership: Arc<RwLock<MembershipList>>,
    bucket_manager: Arc<RwLock<BucketManager>>,
    database: Database,
) -> Result<(), ClusterError> {
    let local_node_id = {
        let m = membership.read().await;
        m.local_node_id()
    };

    let delegation_handler = DelegationHandler::new(
        local_node_id,
        bucket_manager.clone(),
        membership.clone(),
        socket.clone(),
        database.clone(),
    );

    let mut buf = [0u8; MAX_PACKET_SIZE];
    let mut codec = ClusterMessageCodec;

    info!("Cluster server listening on {:?}", socket.local_addr());

    loop {
        let (len, from_addr) = match socket.recv_from(&mut buf).await {
            Ok(result) => result,
            Err(e) => {
                warn!("Failed to receive packet: {}", e);
                continue;
            }
        };

        let mut bytes = BytesMut::from(&buf[..len]);

        let msg = match codec.decode(&mut bytes) {
            Ok(Some(msg)) => msg,
            Ok(None) => {
                debug!("Incomplete message from {}", from_addr);
                continue;
            }
            Err(e) => {
                warn!("Failed to decode message from {}: {}", from_addr, e);
                continue;
            }
        };

        // Handle the message
        if let Err(e) = handle_message(
            msg,
            from_addr,
            &membership,
            &bucket_manager,
            &delegation_handler,
            &socket,
        )
        .await
        {
            warn!("Failed to handle message from {}: {}", from_addr, e);
        }
    }
}

/// Handles an incoming cluster message.
async fn handle_message(
    msg: ClusterMessage,
    from_addr: SocketAddr,
    membership: &RwLock<MembershipList>,
    bucket_manager: &RwLock<BucketManager>,
    delegation_handler: &DelegationHandler,
    socket: &UdpSocket,
) -> Result<(), ClusterError> {
    debug!(
        "Received {:?} from node {} at {}",
        msg.msg_type, msg.node_id, from_addr
    );

    match msg.msg_type {
        ClusterMessageType::Heartbeat => {
            handle_heartbeat(msg.node_id, from_addr, msg.payload, membership).await
        }
        ClusterMessageType::HeartbeatAck => {
            handle_heartbeat_ack(msg.node_id, membership).await
        }
        ClusterMessageType::StatusRequest => {
            handle_status_request(msg.node_id, from_addr, membership, bucket_manager, socket).await
        }
        ClusterMessageType::StatusResponse => {
            handle_status_response(msg.node_id, msg.payload, membership).await
        }
        ClusterMessageType::DelegateRequest => {
            if let ClusterPayload::DelegateRequest(payload) = msg.payload {
                delegation_handler
                    .handle_delegation_request(msg.node_id, from_addr, payload)
                    .await
            } else {
                Err(ClusterError::InvalidMessage("Expected DelegateRequest payload".to_string()))
            }
        }
        ClusterMessageType::DelegateAccept => {
            if let ClusterPayload::DelegateAccept(payload) = msg.payload {
                delegation_handler
                    .handle_delegation_accept(msg.node_id, payload)
                    .await
            } else {
                Err(ClusterError::InvalidMessage("Expected DelegateAccept payload".to_string()))
            }
        }
        ClusterMessageType::DelegateReject => {
            if let ClusterPayload::DelegateReject(payload) = msg.payload {
                delegation_handler
                    .handle_delegation_reject(msg.node_id, payload.reason)
                    .await
            } else {
                Err(ClusterError::InvalidMessage("Expected DelegateReject payload".to_string()))
            }
        }
        ClusterMessageType::NodeJoin => {
            handle_node_join(msg.node_id, from_addr, msg.payload, membership, bucket_manager, socket).await
        }
        ClusterMessageType::NodeLeave => {
            handle_node_leave(msg.node_id, membership).await
        }
        ClusterMessageType::NodeSuspect => {
            handle_node_suspect(msg.payload, membership).await
        }
        ClusterMessageType::NodeDead => {
            handle_node_dead(msg.payload, membership, bucket_manager).await
        }
        ClusterMessageType::ProbeRequest => {
            if let ClusterPayload::ProbeRequest(payload) = msg.payload {
                handle_probe_request(payload.target_node_id, from_addr, membership, socket).await;
                Ok(())
            } else {
                Err(ClusterError::InvalidMessage("Expected ProbeRequest payload".to_string()))
            }
        }
        ClusterMessageType::ProbeResponse => {
            // Probe responses update the target node's heartbeat if alive
            if let ClusterPayload::ProbeResponse(payload) = msg.payload {
                if payload.is_alive {
                    let mut m = membership.write().await;
                    m.update_heartbeat(payload.target_node_id);
                }
            }
            Ok(())
        }
    }
}

/// Handles a heartbeat message.
async fn handle_heartbeat(
    node_id: Uuid,
    from_addr: SocketAddr,
    payload: ClusterPayload,
    membership: &RwLock<MembershipList>,
) -> Result<(), ClusterError> {
    let heartbeat = match payload {
        ClusterPayload::Heartbeat(h) => h,
        _ => return Err(ClusterError::InvalidMessage("Expected Heartbeat payload".to_string())),
    };

    let mut m = membership.write().await;

    // Update or add the node
    if let Some(node) = m.get_node_mut(node_id) {
        node.status = heartbeat.status;
        node.bucket_count = heartbeat.bucket_count as u32;
        node.device_count = heartbeat.device_count;
        node.load_percent = heartbeat.load_percent;
        node.update_heartbeat();
    } else {
        // New node - add to membership
        let node = NodeInfo {
            node_id,
            node_name: format!("node-{}", &node_id.to_string()[..8]),
            cluster_addr: from_addr,
            backdoor_port: 6565, // Will be updated when we receive NodeJoin
            status: heartbeat.status,
            started_at: chrono::Utc::now(),
            last_heartbeat: chrono::Utc::now(),
            bucket_count: heartbeat.bucket_count as u32,
            device_count: heartbeat.device_count,
            load_percent: heartbeat.load_percent,
        };
        m.add_or_update_node(node);
    }

    // Check for nodes we don't know about
    for known_id in heartbeat.known_nodes {
        if m.get_node(known_id).is_none() && known_id != m.local_node_id() {
            debug!("Discovered new node {} from heartbeat gossip", known_id);
            // We'll learn about this node when we receive their heartbeat
        }
    }

    Ok(())
}

/// Handles a heartbeat acknowledgment.
async fn handle_heartbeat_ack(
    node_id: Uuid,
    membership: &RwLock<MembershipList>,
) -> Result<(), ClusterError> {
    let mut m = membership.write().await;
    m.update_heartbeat(node_id);
    Ok(())
}

/// Handles a status request.
async fn handle_status_request(
    _from_node_id: Uuid,
    from_addr: SocketAddr,
    membership: &RwLock<MembershipList>,
    bucket_manager: &RwLock<BucketManager>,
    socket: &UdpSocket,
) -> Result<(), ClusterError> {
    let (local_id, seq, payload) = {
        let mut m = membership.write().await;
        let local = m.local_node();

        let bm = bucket_manager.read().await;
        let owned_buckets: Vec<i32> = bm.owned_buckets().iter().copied().collect();

        let payload = StatusResponsePayload {
            node_name: local.node_name.clone(),
            status: local.status,
            bucket_count: owned_buckets.len() as u16,
            device_count: local.device_count,
            load_percent: local.load_percent,
            owned_buckets,
        };

        let seq = m.next_seq();
        (m.local_node_id(), seq, payload)
    };

    let msg = ClusterMessage::status_response(local_id, seq, payload);
    crate::membership::send_message(socket, from_addr, msg).await?;

    Ok(())
}

/// Handles a status response.
async fn handle_status_response(
    node_id: Uuid,
    payload: ClusterPayload,
    membership: &RwLock<MembershipList>,
) -> Result<(), ClusterError> {
    let status = match payload {
        ClusterPayload::StatusResponse(s) => s,
        _ => return Err(ClusterError::InvalidMessage("Expected StatusResponse payload".to_string())),
    };

    let mut m = membership.write().await;
    if let Some(node) = m.get_node_mut(node_id) {
        node.node_name = status.node_name;
        node.status = status.status;
        node.bucket_count = status.bucket_count as u32;
        node.device_count = status.device_count;
        node.load_percent = status.load_percent;
        node.update_heartbeat();
    }

    Ok(())
}

/// Handles a node join announcement.
async fn handle_node_join(
    node_id: Uuid,
    from_addr: SocketAddr,
    payload: ClusterPayload,
    membership: &RwLock<MembershipList>,
    bucket_manager: &RwLock<BucketManager>,
    socket: &UdpSocket,
) -> Result<(), ClusterError> {
    let join = match payload {
        ClusterPayload::NodeJoin(j) => j,
        _ => return Err(ClusterError::InvalidMessage("Expected NodeJoin payload".to_string())),
    };

    info!("Node {} ({}) is joining the cluster", join.node_name, node_id);

    // Add to membership
    {
        let mut m = membership.write().await;
        let node = NodeInfo {
            node_id,
            node_name: join.node_name,
            cluster_addr: join.cluster_addr,
            backdoor_port: join.backdoor_port,
            status: NodeStatus::Starting,
            started_at: chrono::Utc::now(),
            last_heartbeat: chrono::Utc::now(),
            bucket_count: 0,
            device_count: 0,
            load_percent: 0,
        };
        m.add_or_update_node(node);
    }

    // Assign buckets to the new node
    let buckets_given = {
        let mut bm = bucket_manager.write().await;
        bm.assign_buckets_on_join(node_id).await?
    };

    if !buckets_given.is_empty() {
        info!("Gave {} buckets to new node {}", buckets_given.len(), node_id);
    }

    // Send our status back
    handle_status_request(node_id, from_addr, membership, bucket_manager, socket).await?;

    Ok(())
}

/// Handles a node leave announcement.
async fn handle_node_leave(
    node_id: Uuid,
    membership: &RwLock<MembershipList>,
) -> Result<(), ClusterError> {
    let mut m = membership.write().await;

    if let Some(node) = m.get_node_mut(node_id) {
        info!("Node {} is leaving the cluster gracefully", node.node_name);
        node.status = NodeStatus::Draining;
    }

    Ok(())
}

/// Handles a node suspect announcement.
async fn handle_node_suspect(
    payload: ClusterPayload,
    membership: &RwLock<MembershipList>,
) -> Result<(), ClusterError> {
    let suspect = match payload {
        ClusterPayload::NodeSuspect(s) => s,
        _ => return Err(ClusterError::InvalidMessage("Expected NodeSuspect payload".to_string())),
    };

    let mut m = membership.write().await;
    m.mark_suspect(suspect.suspect_node_id);

    Ok(())
}

/// Handles a node dead announcement.
async fn handle_node_dead(
    payload: ClusterPayload,
    membership: &RwLock<MembershipList>,
    bucket_manager: &RwLock<BucketManager>,
) -> Result<(), ClusterError> {
    let dead = match payload {
        ClusterPayload::NodeDead(d) => d,
        _ => return Err(ClusterError::InvalidMessage("Expected NodeDead payload".to_string())),
    };

    let node_name = {
        let mut m = membership.write().await;
        let name = m
            .get_node(dead.dead_node_id)
            .map(|n| n.node_name.clone())
            .unwrap_or_else(|| "unknown".to_string());
        m.mark_dead(dead.dead_node_id);
        name
    };

    info!("Node {} ({}) confirmed dead by cluster", node_name, dead.dead_node_id);

    // Participate in redistribution
    let mut bm = bucket_manager.write().await;
    bm.redistribute_from_failed(dead.dead_node_id).await?;

    // Remove from membership
    let mut m = membership.write().await;
    m.remove_node(dead.dead_node_id);

    Ok(())
}
