//! Device delegation between nodes.
//!
//! Handles the transfer of device ownership and associated connections
//! between nodes during rebalancing or shutdown.

use std::net::SocketAddr;
use std::sync::Arc;

use tokio::net::UdpSocket;
use tokio::sync::RwLock;
use tracing::{info, warn};
use uuid::Uuid;

use common::database::api::Database;
use common::delegated_device::DelegatedDevice;

use crate::device_manager::DeviceManager;
use crate::error::ClusterError;
use crate::membership::{send_message, MembershipList};
use crate::protocol::{
    ClusterMessage, DelegateAcceptPayload, DelegatedDevicePayload, DelegateRequestPayload,
    DelegationReason,
};

/// Handles outgoing delegation requests.
pub struct DelegationHandler {
    /// This node's ID
    local_node_id: Uuid,
    /// Device manager
    device_manager: Arc<RwLock<DeviceManager>>,
    /// Membership list
    membership: Arc<RwLock<MembershipList>>,
    /// UDP socket for sending messages
    socket: Arc<UdpSocket>,
    /// Database handle
    database: Database,
}

impl DelegationHandler {
    /// Creates a new delegation handler.
    pub fn new(
        local_node_id: Uuid,
        device_manager: Arc<RwLock<DeviceManager>>,
        membership: Arc<RwLock<MembershipList>>,
        socket: Arc<UdpSocket>,
        database: Database,
    ) -> Self {
        Self {
            local_node_id,
            device_manager,
            membership,
            socket,
            database,
        }
    }

    /// Requests delegation of specific devices to another node.
    pub async fn request_delegation(
        &self,
        devices: Vec<DelegatedDevice>,
        reason: DelegationReason,
        target_node_id: Option<Uuid>,
    ) -> Result<Vec<Uuid>, ClusterError> {
        if devices.is_empty() {
            return Ok(Vec::new());
        }

        // Find a target node if not specified
        let (target_id, target_addr) = if let Some(id) = target_node_id {
            let membership = self.membership.read().await;
            let node = membership
                .get_node(id)
                .ok_or(ClusterError::NodeNotFound(id))?;
            (id, node.cluster_addr)
        } else {
            // Find the least loaded active node (by device count)
            let membership = self.membership.read().await;
            let node = membership
                .active_nodes()
                .into_iter()
                .min_by_key(|n| n.device_count)
                .ok_or(ClusterError::NoNodesAvailable)?;
            (node.node_id, node.cluster_addr)
        };

        // Send delegation request
        let seq = {
            let mut membership = self.membership.write().await;
            membership.next_seq()
        };

        // Convert DelegatedDevice to DelegatedDevicePayload
        let device_payloads: Vec<DelegatedDevicePayload> = devices
            .iter()
            .map(|d| DelegatedDevicePayload {
                device_id: d.device_id,
                ipv4: d.ipv4.clone(),
                ipv6: d.ipv6.clone(),
                schedule_time: d.schedule_time.and_utc().timestamp(),
            })
            .collect();

        let payload = DelegateRequestPayload {
            devices: device_payloads,
            reason,
        };

        let msg = ClusterMessage::delegate_request(self.local_node_id, seq, payload);

        info!(
            "Requesting delegation of {} devices to node {}",
            devices.len(),
            target_id
        );

        send_message(&self.socket, target_addr, msg).await?;

        // TODO: Wait for DELEGATE_ACCEPT/REJECT response with timeout before returning success

        Ok(devices.iter().map(|d| d.device_id).collect())
    }

    /// Handles an incoming delegation request.
    pub async fn handle_delegation_request(
        &self,
        from_node_id: Uuid,
        from_addr: SocketAddr,
        payload: DelegateRequestPayload,
    ) -> Result<(), ClusterError> {
        info!(
            "Received delegation request for {} devices from node {} (reason: {:?})",
            payload.devices.len(),
            from_node_id,
            payload.reason
        );

        // Check if we can accept the delegation
        let (can_accept, is_overloaded) = {
            let membership = self.membership.read().await;
            let local = membership.local_node();
            let overloaded = local.load_percent >= 90.0;

            // Always accept Shutdown delegations (node is going down, must rescue devices)
            // For other reasons, only accept if we're active and not overloaded
            let accept = if payload.reason == DelegationReason::Shutdown {
                local.is_available()
            } else {
                local.is_available() && !overloaded
            };

            (accept, overloaded)
        };

        let seq = {
            let mut membership = self.membership.write().await;
            membership.next_seq()
        };

        if !can_accept {
            let msg = ClusterMessage::delegate_reject(
                self.local_node_id,
                seq,
                "Node is not available or overloaded".to_string(),
            );
            send_message(&self.socket, from_addr, msg).await?;
            return Ok(());
        }

        // Convert payload to DelegatedDevice
        let delegated_devices: Vec<DelegatedDevice> = payload
            .devices
            .iter()
            .map(|p| {
                DelegatedDevice::new(
                    p.device_id,
                    p.ipv4.clone(),
                    p.ipv6.clone(),
                    chrono::DateTime::from_timestamp(p.schedule_time, 0)
                        .unwrap_or_default()
                        .naive_utc(),
                )
            })
            .collect();

        // Accept the delegation: claim ownership in DB and schedule each device in one step
        let accepted_device_ids = {
            let mut dm = self.device_manager.write().await;
            dm.accept_delegation(delegated_devices).await?
        };

        // Send accept response
        let accept_payload = DelegateAcceptPayload {
            accepted_device_ids: accepted_device_ids.clone(),
        };
        let msg = ClusterMessage::delegate_accept(self.local_node_id, seq, accept_payload);
        send_message(&self.socket, from_addr, msg).await?;

        info!("Accepted delegation of {} devices", accepted_device_ids.len());

        // If we accepted a Shutdown delegation while overloaded, redistribute some devices
        if is_overloaded && payload.reason == DelegationReason::Shutdown {
            warn!(
                "Accepted {} devices from shutting down node while overloaded ({}% load), will redistribute",
                accepted_device_ids.len(),
                {
                    let membership = self.membership.read().await;
                    membership.local_node().load_percent
                }
            );

            // TODO: Trigger rebalancing to redistribute excess load
            // - Select target node(s) with lower load
            // - Calculate how many devices to delegate to get below 90% load
            // - Call request_delegation() with DelegationReason::Rebalance
            // This should be done asynchronously to not block the shutdown response
        }

        Ok(())
    }

    /// Handles an incoming delegation accept response.
    pub async fn handle_delegation_accept(
        &self,
        from_node_id: Uuid,
        payload: DelegateAcceptPayload,
    ) -> Result<(), ClusterError> {
        info!(
            "Node {} accepted delegation of {} devices",
            from_node_id,
            payload.accepted_device_ids.len()
        );

        // Update our device manager to release the devices and remove from scheduler
        {
            let mut dm = self.device_manager.write().await;
            dm.release_devices(&payload.accepted_device_ids);
            for device_id in &payload.accepted_device_ids {
                dm.remove_scheduled_device(*device_id);
            }
        }

        Ok(())
    }

    /// Handles an incoming delegation reject response.
    pub async fn handle_delegation_reject(
        &self,
        from_node_id: Uuid,
        reason: String,
    ) -> Result<(), ClusterError> {
        warn!(
            "Node {} rejected delegation: {}",
            from_node_id, reason
        );

        // In a production system, we'd try another node
        Err(ClusterError::DelegationRejected(reason))
    }

    /// Initiates graceful shutdown delegation.
    ///
    /// Transfers all owned devices to other nodes before shutting down.
    pub async fn shutdown_delegation(&self) -> Result<(), ClusterError> {
        let device_ids = {
            let device_manager = self.device_manager.read().await;
            device_manager.devices_for_shutdown()
        };

        if device_ids.is_empty() {
            info!("No devices to delegate on shutdown");
            return Ok(());
        }

        info!("Initiating shutdown delegation of {} devices", device_ids.len());

        // Build DelegatedDevice structs with device info
        let mut delegated_devices = Vec::new();
        for device_id in &device_ids {
            let device = self.database.get_device(*device_id).await?;
            let schedule_time = match self.database.get_scheduled_connection(*device_id).await {
                Ok(conn) => conn.schedule_time,
                Err(_) => chrono::Utc::now().naive_utc() + chrono::Duration::days(1),
            };

            delegated_devices.push(DelegatedDevice::new(
                device.id,
                device.ipv4,
                device.ipv6,
                schedule_time,
            ));
        }

        // Get active nodes sorted by load (device count)
        let target_nodes: Vec<(Uuid, SocketAddr)> = {
            let membership = self.membership.read().await;
            let mut nodes: Vec<_> = membership
                .active_nodes()
                .iter()
                .map(|n| (n.node_id, n.cluster_addr, n.device_count))
                .collect();
            nodes.sort_by_key(|(_, _, count)| *count);
            nodes.into_iter().map(|(id, addr, _)| (id, addr)).collect()
        };

        if target_nodes.is_empty() {
            warn!("No nodes available for shutdown delegation!");
            return Err(ClusterError::NoNodesAvailable);
        }

        // Distribute devices among available nodes
        let devices_per_node = delegated_devices.len().div_ceil(target_nodes.len());

        for (i, chunk) in delegated_devices.chunks(devices_per_node).enumerate() {
            let target_idx = i % target_nodes.len();
            let (target_id, _) = target_nodes[target_idx];

            self.request_delegation(
                chunk.to_vec(),
                DelegationReason::Shutdown,
                Some(target_id),
            )
            .await?;
        }

        info!("Shutdown delegation complete");
        Ok(())
    }
}
