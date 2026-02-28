//! UDP server for inter-node cluster communication.

use std::net::SocketAddr;
use std::sync::Arc;

use bytes::BytesMut;
use tokio::net::UdpSocket;
use tokio::sync::RwLock;
use tokio_util::codec::Decoder;
use tracing::{debug, info, warn};
use uuid::Uuid;

use crate::delegation::DelegationHandler;
use device_manager::DeviceManager;
use crate::error::ClusterError;
use crate::failure_detector::handle_probe_request;
use crate::membership::MembershipList;
use crate::node::{NodeInfo, NodeStatus};
use crate::protocol::{
    ClusterMessage, ClusterMessageCodec, ClusterMessageType, ClusterPayload, KnownNodeInfo,
    NodeJoinPayload, StatusResponsePayload,
};

use common::config_store::ConfigStore;
use common::database::api::Database;

/// Maximum UDP packet size.
const MAX_PACKET_SIZE: usize = 65535;

/// Runs the cluster server that handles incoming messages from other nodes.
pub async fn run_cluster_server(
    socket: Arc<UdpSocket>,
    membership: Arc<RwLock<MembershipList>>,
    device_manager: Arc<RwLock<DeviceManager>>,
    database: Database,
    config_store: Arc<dyn ConfigStore>,
) -> Result<(), ClusterError> {
    let local_node_id = {
        let m = membership.read().await;
        m.local_node_id()
    };

    let delegation_handler = DelegationHandler::new(
        local_node_id,
        device_manager.clone(),
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
                // WSAECONNRESET (10054) is expected when sending to a dead node
                // Log as debug instead of warning to reduce noise
                if e.kind() == std::io::ErrorKind::ConnectionReset {
                    debug!("Connection reset by peer (node likely offline): {}", e);
                } else {
                    warn!("Failed to receive packet: {}", e);
                }
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
            &device_manager,
            &delegation_handler,
            &socket,
            &database,
            &config_store,
        )
        .await
        {
            warn!("Failed to handle message from {}: {}", from_addr, e);
        }
    }
}

/// Handles an incoming cluster message.
#[allow(clippy::too_many_arguments)]
async fn handle_message(
    msg: ClusterMessage,
    from_addr: SocketAddr,
    membership: &RwLock<MembershipList>,
    device_manager: &RwLock<DeviceManager>,
    delegation_handler: &DelegationHandler,
    socket: &UdpSocket,
    database: &Database,
    config_store: &Arc<dyn ConfigStore>,
) -> Result<(), ClusterError> {
    info!(
        "Received {:?} from node {} at {}",
        msg.msg_type, msg.node_id, from_addr
    );

    match msg.msg_type {
        ClusterMessageType::Heartbeat => {
            handle_heartbeat(msg.node_id, from_addr, msg.payload, membership).await
        }
        ClusterMessageType::HeartbeatAck => handle_heartbeat_ack(msg.node_id, membership).await,
        ClusterMessageType::StatusRequest => {
            handle_status_request(msg.node_id, from_addr, membership, device_manager, socket).await
        }
        ClusterMessageType::StatusResponse => {
            handle_status_response(msg.node_id, from_addr, msg.payload, membership, socket).await
        }
        ClusterMessageType::DelegateRequest => {
            if let ClusterPayload::DelegateRequest(payload) = msg.payload {
                delegation_handler
                    .handle_delegation_request(msg.node_id, from_addr, payload)
                    .await
            } else {
                Err(ClusterError::InvalidMessage(
                    "Expected DelegateRequest payload".to_string(),
                ))
            }
        }
        ClusterMessageType::DelegateAccept => {
            if let ClusterPayload::DelegateAccept(payload) = msg.payload {
                delegation_handler
                    .handle_delegation_accept(msg.node_id, payload)
                    .await
            } else {
                Err(ClusterError::InvalidMessage(
                    "Expected DelegateAccept payload".to_string(),
                ))
            }
        }
        ClusterMessageType::DelegateReject => {
            if let ClusterPayload::DelegateReject(payload) = msg.payload {
                delegation_handler
                    .handle_delegation_reject(msg.node_id, payload.reason)
                    .await
            } else {
                Err(ClusterError::InvalidMessage(
                    "Expected DelegateReject payload".to_string(),
                ))
            }
        }
        ClusterMessageType::NodeJoin => {
            handle_node_join(
                msg.node_id,
                from_addr,
                msg.payload,
                membership,
                device_manager,
                socket,
                config_store,
            )
            .await
        }
        ClusterMessageType::NodeLeave => handle_node_leave(msg.node_id, membership).await,
        ClusterMessageType::NodeSuspect => handle_node_suspect(msg.payload, membership).await,
        ClusterMessageType::NodeDead => {
            handle_node_dead(msg.payload, membership, device_manager, database).await
        }
        ClusterMessageType::ProbeRequest => {
            if let ClusterPayload::ProbeRequest(payload) = msg.payload {
                handle_probe_request(payload.target_node_id, from_addr, membership, socket).await;
                Ok(())
            } else {
                Err(ClusterError::InvalidMessage(
                    "Expected ProbeRequest payload".to_string(),
                ))
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
        _ => {
            return Err(ClusterError::InvalidMessage(
                "Expected Heartbeat payload".to_string(),
            ));
        }
    };
    info!("Received Heartbeat from {}", from_addr);
    let mut m = membership.write().await;

    // Update or add the node
    if let Some(node) = m.get_node_mut(node_id) {
        node.status = heartbeat.status;
        node.bucket_count = heartbeat.bucket_count as u32;
        node.device_count = heartbeat.device_count;
        node.load_percent = heartbeat.load_percent.into();
        node.max_device_suggested = heartbeat.max_device_suggested;
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
            max_device_suggested: heartbeat.max_device_suggested,
            load_percent: f32::from(heartbeat.load_percent),
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
    from_node_id: Uuid,
    from_addr: SocketAddr,
    membership: &RwLock<MembershipList>,
    device_manager: &RwLock<DeviceManager>,
    socket: &UdpSocket,
) -> Result<(), ClusterError> {
    let (local_id, seq, payload) = {
        let mut m = membership.write().await;
        let local = m.local_node();

        let dm = device_manager.read().await;
        let bucket_count = dm.local_bucket_count() as u16;
        let device_count = dm.device_count() as u32;

        let known_nodes: Vec<KnownNodeInfo> = m
            .reachable_nodes()
            .iter()
            .filter(|n| n.node_id != from_node_id)
            .map(|n| KnownNodeInfo {
                node_id: n.node_id,
                node_name: n.node_name.clone(),
                cluster_addr: n.cluster_addr,
                backdoor_port: n.backdoor_port,
                status: n.status,
            })
            .collect();

        let payload = StatusResponsePayload {
            node_name: local.node_name.clone(),
            status: local.status,
            bucket_count,
            device_count,
            load_percent: local.load_percent as u8,
            max_device_suggested: local.max_device_suggested,
            backdoor_port: local.backdoor_port,
            known_nodes,
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
    from_addr: SocketAddr,
    payload: ClusterPayload,
    membership: &RwLock<MembershipList>,
    socket: &UdpSocket,
) -> Result<(), ClusterError> {
    let status = match payload {
        ClusterPayload::StatusResponse(s) => s,
        _ => {
            return Err(ClusterError::InvalidMessage(
                "Expected StatusResponse payload".to_string(),
            ));
        }
    };

    let mut m = membership.write().await;

    // New node responding to our join - add to membership
    let node = NodeInfo {
        node_id,
        node_name: status.node_name.clone(),
        // Use actual source IP from UDP packet
        cluster_addr: std::net::SocketAddr::new(from_addr.ip(), from_addr.port()),
        backdoor_port: status.backdoor_port,
        status: status.status,
        started_at: chrono::Utc::now(),
        last_heartbeat: chrono::Utc::now(),
        bucket_count: status.bucket_count as u32,
        device_count: status.device_count,
        max_device_suggested: status.max_device_suggested,
        load_percent: f32::from(status.load_percent),
    };
    m.add_or_update_node(node);

    // Send NODE_JOIN to unknown nodes discovered via the status response
    let local = m.local_node();
    let local_id = local.node_id;
    let join_payload = NodeJoinPayload {
        node_name: local.node_name.clone(),
        cluster_addr: local.cluster_addr,
        backdoor_port: local.backdoor_port,
    };
    let seq = m.next_seq();

    let unknown_addrs: Vec<SocketAddr> = status
        .known_nodes
        .iter()
        .filter(|known| m.get_node(known.node_id).is_none())
        .map(|known| known.cluster_addr)
        .collect();

    if !unknown_addrs.is_empty() {
        info!("Unknown addresses received: {:?}", unknown_addrs);
    }

    // Release the lock before sending
    drop(m);

    for addr in unknown_addrs {
        let msg = ClusterMessage::node_join(local_id, seq, join_payload.clone());
        if let Err(e) = crate::membership::send_message(socket, addr, msg).await {
            warn!(
                "Failed to send NODE_JOIN to discovered node {}: {}",
                addr, e
            );
        }
    }

    Ok(())
}

/// Handles a node join announcement.
async fn handle_node_join(
    node_id: Uuid,
    from_addr: SocketAddr,
    payload: ClusterPayload,
    membership: &RwLock<MembershipList>,
    device_manager: &RwLock<DeviceManager>,
    socket: &UdpSocket,
    config_store: &Arc<dyn ConfigStore>,
) -> Result<(), ClusterError> {
    let join = match payload {
        ClusterPayload::NodeJoin(j) => j,
        _ => {
            return Err(ClusterError::InvalidMessage(
                "Expected NodeJoin payload".to_string(),
            ));
        }
    };

    info!(
        "Node {} ({}) is joining the cluster",
        join.node_name, node_id
    );

    // Add to membership and collect updated seed list
    let seeds = {
        let mut m = membership.write().await;
        let node = NodeInfo {
            node_id,
            node_name: join.node_name,
            cluster_addr: std::net::SocketAddr::new(from_addr.ip(), join.cluster_addr.port()),
            backdoor_port: join.backdoor_port,
            status: NodeStatus::Starting,
            started_at: chrono::Utc::now(),
            last_heartbeat: chrono::Utc::now(),
            bucket_count: 0,
            device_count: 0,
            max_device_suggested: NodeInfo::DEFAULT_MAX_DEVICES,
            load_percent: 0.0,
        };
        m.add_or_update_node(node);

        // All reachable peers (excluding ourselves) as seeds
        let local_id = m.local_node_id();
        m.reachable_nodes()
            .iter()
            .filter(|n| n.node_id != local_id)
            .map(|n| n.cluster_addr.to_string())
            .collect::<Vec<_>>()
            .join(",")
    };

    if let Err(e) = config_store.set_cluster_seeds(Some(seeds)).await {
        warn!("Failed to persist cluster seeds: {}", e);
    }

    // Send our status back to the joining node
    // Use the node's cluster_addr (not from_addr) to handle relayed NODE_JOIN correctly
    let target_addr = {
        let m = membership.read().await;
        m.get_node(node_id).map(|n| n.cluster_addr)
    };

    if let Some(addr) = target_addr {
        handle_status_request(node_id, addr, membership, device_manager, socket).await?;
    }

    Ok(())
}

/// Handles a node leave announcement.
///
/// The leaving node has already delegated its devices before sending NODE_LEAVE,
/// so we remove it from the membership list immediately.
async fn handle_node_leave(
    node_id: Uuid,
    membership: &RwLock<MembershipList>,
) -> Result<(), ClusterError> {
    let mut m = membership.write().await;

    if let Some(node) = m.remove_node(node_id) {
        info!("Node {} left the cluster gracefully", node.node_name);
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
        _ => {
            return Err(ClusterError::InvalidMessage(
                "Expected NodeSuspect payload".to_string(),
            ));
        }
    };

    let mut m = membership.write().await;
    m.mark_suspect(suspect.suspect_node_id);

    Ok(())
}

/// Handles a node dead announcement.
async fn handle_node_dead(
    payload: ClusterPayload,
    membership: &RwLock<MembershipList>,
    device_manager: &RwLock<DeviceManager>,
    database: &Database,
) -> Result<(), ClusterError> {
    let dead = match payload {
        ClusterPayload::NodeDead(d) => d,
        _ => {
            return Err(ClusterError::InvalidMessage(
                "Expected NodeDead payload".to_string(),
            ));
        }
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

    info!(
        "Node {} ({}) confirmed dead by cluster",
        node_name, dead.dead_node_id
    );

    // Update node status in database to "dead"
    database
        .update_cluster_node_status(dead.dead_node_id, "dead")
        .await?;

    // Extract active nodes from membership for redistribution
    let other_active_nodes: Vec<(uuid::Uuid, usize)> = {
        let m = membership.read().await;
        m.active_nodes()
            .iter()
            .map(|n| (n.node_id, n.device_count as usize))
            .collect()
    };

    // Participate in redistribution of devices
    let mut dm = device_manager.write().await;
    dm.redistribute_from_failed(dead.dead_node_id, other_active_nodes).await?;

    // Remove from membership (in-memory only)
    let mut m = membership.write().await;
    m.remove_node(dead.dead_node_id);

    Ok(())
}
