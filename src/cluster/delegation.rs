//! Bucket delegation between nodes.
//!
//! Handles the transfer of bucket ownership and associated connections
//! between nodes during rebalancing or shutdown.

use std::net::SocketAddr;
use std::sync::Arc;

use tokio::net::UdpSocket;
use tokio::sync::RwLock;
use tracing::{info, warn};
use uuid::Uuid;

use common::database::api::Database;

use crate::bucket_manager::BucketManager;
use crate::error::ClusterError;
use crate::membership::{send_message, MembershipList};
use crate::protocol::{
    ClusterMessage, DelegateAcceptPayload, DelegateRequestPayload, DelegationReason,
};

/// Handles outgoing delegation requests.
pub struct DelegationHandler {
    /// This node's ID
    local_node_id: Uuid,
    /// Bucket manager
    bucket_manager: Arc<RwLock<BucketManager>>,
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
        bucket_manager: Arc<RwLock<BucketManager>>,
        membership: Arc<RwLock<MembershipList>>,
        socket: Arc<UdpSocket>,
        database: Database,
    ) -> Self {
        Self {
            local_node_id,
            bucket_manager,
            membership,
            socket,
            database,
        }
    }

    /// Requests delegation of specific buckets to another node.
    pub async fn request_delegation(
        &self,
        buckets: Vec<i32>,
        reason: DelegationReason,
        target_node_id: Option<Uuid>,
    ) -> Result<Vec<i32>, ClusterError> {
        if buckets.is_empty() {
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
            // Find the least loaded active node
            let membership = self.membership.read().await;
            let node = membership
                .active_nodes()
                .into_iter()
                .min_by_key(|n| n.bucket_count)
                .ok_or(ClusterError::NoNodesAvailable)?;
            (node.node_id, node.cluster_addr)
        };

        // Get device count for the buckets
        let device_count = self.get_device_count_for_buckets(&buckets).await?;

        // Send delegation request
        let seq = {
            let mut membership = self.membership.write().await;
            membership.next_seq()
        };

        let payload = DelegateRequestPayload {
            buckets: buckets.clone(),
            reason,
            device_count,
        };

        let msg = ClusterMessage::delegate_request(self.local_node_id, seq, payload);

        info!(
            "Requesting delegation of {} buckets to node {}",
            buckets.len(),
            target_id
        );

        send_message(&self.socket, target_addr, msg).await?;

        // For now, we assume the delegation will be accepted
        // In a production system, we'd wait for a response and handle rejection
        // The actual transfer happens when we receive DELEGATE_ACCEPT

        Ok(buckets)
    }

    /// Handles an incoming delegation request.
    pub async fn handle_delegation_request(
        &self,
        from_node_id: Uuid,
        from_addr: SocketAddr,
        payload: DelegateRequestPayload,
    ) -> Result<(), ClusterError> {
        info!(
            "Received delegation request for {} buckets from node {} (reason: {:?})",
            payload.buckets.len(),
            from_node_id,
            payload.reason
        );

        // Check if we can accept the delegation
        let can_accept = {
            let membership = self.membership.read().await;
            let local = membership.local_node();
            // Accept if we're active and not overloaded
            local.is_available() && local.load_percent < 90
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

        // Accept the delegation
        let accepted_buckets = payload.buckets.clone();

        // Update our bucket manager
        {
            let mut bucket_manager = self.bucket_manager.write().await;
            bucket_manager.accept_delegation(accepted_buckets.clone()).await?;
        }

        // Send accept response
        let accept_payload = DelegateAcceptPayload {
            buckets: accepted_buckets.clone(),
        };
        let msg = ClusterMessage::delegate_accept(self.local_node_id, seq, accept_payload);
        send_message(&self.socket, from_addr, msg).await?;

        info!("Accepted delegation of {} buckets", accepted_buckets.len());

        Ok(())
    }

    /// Handles an incoming delegation accept response.
    pub async fn handle_delegation_accept(
        &self,
        from_node_id: Uuid,
        payload: DelegateAcceptPayload,
    ) -> Result<(), ClusterError> {
        info!(
            "Node {} accepted delegation of {} buckets",
            from_node_id,
            payload.buckets.len()
        );

        // Update our bucket manager to release the buckets
        {
            let mut bucket_manager = self.bucket_manager.write().await;
            bucket_manager.release_buckets(&payload.buckets);
        }

        // Update the database to reflect new ownership
        for bucket in &payload.buckets {
            self.database.assign_bucket(*bucket, from_node_id).await?;
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

    /// Gets the device count for a set of buckets.
    async fn get_device_count_for_buckets(&self, buckets: &[i32]) -> Result<u32, ClusterError> {
        let mut count = 0u32;
        for &bucket in buckets {
            let devices = self.database.get_devices_in_bucket(bucket).await?;
            count += devices.len() as u32;
        }
        Ok(count)
    }

    /// Initiates graceful shutdown delegation.
    ///
    /// Transfers all owned buckets to other nodes before shutting down.
    pub async fn shutdown_delegation(&self) -> Result<(), ClusterError> {
        let buckets = {
            let bucket_manager = self.bucket_manager.read().await;
            bucket_manager.buckets_for_shutdown()
        };

        if buckets.is_empty() {
            info!("No buckets to delegate on shutdown");
            return Ok(());
        }

        info!("Initiating shutdown delegation of {} buckets", buckets.len());

        // Get active nodes sorted by load
        let target_nodes: Vec<(Uuid, SocketAddr)> = {
            let membership = self.membership.read().await;
            let mut nodes: Vec<_> = membership
                .active_nodes()
                .iter()
                .map(|n| (n.node_id, n.cluster_addr, n.bucket_count))
                .collect();
            nodes.sort_by_key(|(_, _, count)| *count);
            nodes.into_iter().map(|(id, addr, _)| (id, addr)).collect()
        };

        if target_nodes.is_empty() {
            warn!("No nodes available for shutdown delegation!");
            return Err(ClusterError::NoNodesAvailable);
        }

        // Distribute buckets among available nodes
        let buckets_per_node = buckets.len().div_ceil(target_nodes.len());

        for (i, chunk) in buckets.chunks(buckets_per_node).enumerate() {
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
