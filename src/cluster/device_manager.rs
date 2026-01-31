//! Device ownership and delegation management.
//!
//! Handles the distribution of devices across cluster nodes.

use std::collections::HashSet;
use std::sync::Arc;

use chrono::Utc;
use tokio::sync::RwLock;
use tracing::{debug, info, warn};
use uuid::Uuid;

use common::database::api::Database;
use common::delegated_device::DelegatedDevice;

use crate::error::ClusterError;
use crate::membership::MembershipList;

/// Manages device ownership across cluster nodes.
#[derive(Debug)]
pub struct DeviceManager {
    /// This node's ID
    local_node_id: Uuid,
    /// Total number of local buckets for this node (fixed)
    local_bucket_count: i32,
    /// Device UUIDs owned by this node
    owned_devices: HashSet<Uuid>,
    /// Database handle
    database: Database,
    /// Reference to membership list for node info
    membership: Arc<RwLock<MembershipList>>,
}

impl DeviceManager {
    /// Creates a new device manager.
    pub fn new(
        local_node_id: Uuid,
        local_bucket_count: i32,
        database: Database,
        membership: Arc<RwLock<MembershipList>>,
    ) -> Self {
        Self {
            local_node_id,
            local_bucket_count,
            owned_devices: HashSet::new(),
            database,
            membership,
        }
    }

    /// Gets the local node ID.
    pub fn local_node_id(&self) -> Uuid {
        self.local_node_id
    }

    /// Gets the devices owned by this node.
    pub fn owned_devices(&self) -> &HashSet<Uuid> {
        &self.owned_devices
    }

    /// Gets the number of devices owned by this node (metric for balancing).
    pub fn device_count(&self) -> usize {
        self.owned_devices.len()
    }

    /// Checks if this node owns a specific device.
    pub fn owns_device(&self, device_id: Uuid) -> bool {
        self.owned_devices.contains(&device_id)
    }

    /// Loads device ownership from the database on startup.
    pub async fn load_from_database(&mut self) -> Result<(), ClusterError> {
        let devices = self.database.get_devices_by_owner(self.local_node_id).await?;

        self.owned_devices.clear();
        for device_id in devices {
            self.owned_devices.insert(device_id);
        }

        info!(
            "Loaded {} device ownerships from database",
            self.owned_devices.len()
        );
        Ok(())
    }

    /// Claims unassigned devices when joining the cluster.
    ///
    /// This is called when a new node joins. It will claim any devices that
    /// don't have an owner (orphaned from a dead node or never assigned).
    pub async fn claim_unassigned_devices(&mut self) -> Result<Vec<Uuid>, ClusterError> {
        // Query all devices
        let all_devices = self.database.get_devices_by_owner(Uuid::nil()).await?;

        if all_devices.is_empty() {
            debug!("No unassigned devices to claim");
            return Ok(Vec::new());
        }

        info!("Claiming {} unassigned devices", all_devices.len());

        // Claim all unassigned devices
        for device_id in &all_devices {
            self.database
                .set_device_owner(*device_id, self.local_node_id)
                .await?;
            self.owned_devices.insert(*device_id);
        }

        Ok(all_devices)
    }

    /// Gets devices to delegate when a new node joins for load balancing.
    ///
    /// This is called when a new node joins to give it a fair share of devices.
    /// Returns the devices that should be delegated.
    pub async fn get_devices_for_delegation(
        &mut self,
        target_node_id: Uuid,
    ) -> Result<Vec<DelegatedDevice>, ClusterError> {
        // Calculate fair share
        let active_node_count = {
            let membership = self.membership.read().await;
            membership.active_node_count() as usize
        };

        if active_node_count <= 1 {
            return Ok(Vec::new());
        }

        let fair_share = self.owned_devices.len() / active_node_count;
        let current_count = self.owned_devices.len();
        let to_give = current_count.saturating_sub(fair_share);

        if to_give == 0 {
            debug!("No devices to give away (have {}, fair share is {})", current_count, fair_share);
            return Ok(Vec::new());
        }

        // Select devices to give away and get their scheduled connections
        let device_ids_to_give: Vec<Uuid> = self.owned_devices.iter().copied().take(to_give).collect();

        let mut delegated_devices = Vec::new();

        for device_id in &device_ids_to_give {
            // Get device info
            let device = self.database.get_device(*device_id).await?;

            // Get scheduled connection for this device
            let schedule_time = match self.database.get_scheduled_connection(*device_id).await {
                Ok(conn) => conn.schedule_time,
                Err(_) => {
                    // No scheduled connection, use current time + 1 day as default
                    Utc::now().naive_utc() + chrono::Duration::days(1)
                }
            };

            delegated_devices.push(DelegatedDevice::new(
                device.id,
                device.ipv4,
                device.ipv6,
                schedule_time,
            ));
        }

        info!(
            "Prepared {} devices for delegation to node {} (had {}, fair share is {})",
            delegated_devices.len(),
            target_node_id,
            current_count,
            fair_share
        );

        Ok(delegated_devices)
    }

    /// Accepts delegated devices from another node.
    ///
    /// Returns the list of device IDs that were successfully accepted.
    pub async fn accept_delegation(&mut self, devices: Vec<DelegatedDevice>) -> Result<Vec<Uuid>, ClusterError> {
        info!("Accepting delegation of {} devices", devices.len());

        let mut accepted_ids = Vec::new();

        for device in devices {
            // Set ownership in database
            self.database
                .set_device_owner(device.device_id, self.local_node_id)
                .await?;

            // Add to owned set
            self.owned_devices.insert(device.device_id);
            accepted_ids.push(device.device_id);
        }

        info!("Successfully accepted {} devices", accepted_ids.len());

        Ok(accepted_ids)
    }

    /// Releases devices for delegation to another node.
    pub fn release_devices(&mut self, device_ids: &[Uuid]) {
        for device_id in device_ids {
            self.owned_devices.remove(device_id);
        }
        info!("Released {} devices from ownership", device_ids.len());
    }

    /// Gets devices to delegate when shutting down.
    pub fn devices_for_shutdown(&self) -> Vec<Uuid> {
        self.owned_devices.iter().copied().collect()
    }

    /// Redistributes devices from a failed node.
    ///
    /// This distributes the failed node's devices among remaining active nodes.
    pub async fn redistribute_from_failed(&mut self, failed_node_id: Uuid) -> Result<(), ClusterError> {
        // Get devices owned by failed node
        let orphaned = self.database.get_devices_by_owner(failed_node_id).await?;

        if orphaned.is_empty() {
            debug!("Failed node {} had no devices to redistribute", failed_node_id);
            return Ok(());
        }

        info!(
            "Redistributing {} devices from failed node {}",
            orphaned.len(),
            failed_node_id
        );

        // Get active nodes sorted by device count (least loaded first)
        let active_nodes: Vec<(Uuid, usize)> = {
            let membership = self.membership.read().await;
            let mut nodes: Vec<_> = membership
                .active_nodes()
                .iter()
                .map(|n| (n.node_id, n.device_count as usize))
                .collect();
            // Include self
            nodes.push((self.local_node_id, self.owned_devices.len()));
            nodes.sort_by_key(|(_, count)| *count);
            nodes
        };

        if active_nodes.is_empty() {
            warn!("No active nodes to redistribute devices to!");
            return Err(ClusterError::NoNodesAvailable);
        }

        // Distribute devices round-robin among active nodes, least loaded first
        for (i, device_id) in orphaned.iter().enumerate() {
            let (target_node_id, _) = active_nodes[i % active_nodes.len()];

            self.database.set_device_owner(*device_id, target_node_id).await?;

            if target_node_id == self.local_node_id {
                self.owned_devices.insert(*device_id);
                debug!("Claimed orphaned device {:?}", device_id);
            }
        }

        info!(
            "Redistribution complete, this node now owns {} devices",
            self.owned_devices.len()
        );

        Ok(())
    }

    /// Rebalances devices across the cluster.
    ///
    /// Called when nodes have significantly uneven device counts.
    pub async fn rebalance_cluster(&mut self) -> Result<(), ClusterError> {
        let (active_nodes, my_count) = {
            let membership = self.membership.read().await;
            let nodes: Vec<_> = membership
                .active_nodes()
                .iter()
                .map(|n| (n.node_id, n.device_count as usize))
                .collect();
            let my_count = self.owned_devices.len();
            (nodes, my_count)
        };

        let total_nodes = active_nodes.len() + 1; // +1 for self
        let fair_share = my_count / total_nodes;
        let excess = my_count.saturating_sub(fair_share);

        if excess == 0 {
            debug!("No excess devices to redistribute (have {}, fair share is {})", my_count, fair_share);
            return Ok(());
        }

        // Find underloaded nodes
        let underloaded: Vec<_> = active_nodes
            .into_iter()
            .filter(|(_, count)| *count < fair_share)
            .collect();

        if underloaded.is_empty() {
            debug!("No underloaded nodes to give devices to");
            return Ok(());
        }

        // Give devices to underloaded nodes
        let devices_to_give: Vec<Uuid> = self.owned_devices.iter().copied().take(excess).collect();

        let mut device_idx = 0;
        for (node_id, count) in underloaded {
            let needed = fair_share.saturating_sub(count);
            for _ in 0..needed {
                if device_idx >= devices_to_give.len() {
                    break;
                }
                let device_id = devices_to_give[device_idx];
                self.database.set_device_owner(device_id, node_id).await?;
                self.owned_devices.remove(&device_id);
                device_idx += 1;
            }
        }

        info!(
            "Rebalance complete, gave away {} devices, now own {}",
            device_idx,
            self.owned_devices.len()
        );

        Ok(())
    }

    /// Test-only method to directly add a device without database operations.
    #[cfg(test)]
    pub fn test_add_device(&mut self, device_id: Uuid) {
        self.owned_devices.insert(device_id);
    }

    /// Gets the local bucket count for this node.
    pub fn local_bucket_count(&self) -> i32 {
        self.local_bucket_count
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use common::database::{DatabaseType, api::Database};

    #[tokio::test]
    async fn test_device_manager_creation() {
        let db = Database::new(DatabaseType::InMemory, None).await.unwrap();
        let node_id = Uuid::new_v4();
        let membership = Arc::new(RwLock::new(
            MembershipList::new(crate::node::ClusterConfig {
                node_id,
                node_name: "test".to_string(),
                cluster_ip: "127.0.0.1".to_string(),
                cluster_port: 6570,
                backdoor_port: 6565,
                total_buckets: 48,
                heartbeat_interval: std::time::Duration::from_secs(15),
                suspect_timeout: std::time::Duration::from_secs(30),
                dead_timeout: std::time::Duration::from_secs(60),
                cluster_seeds: vec![],
            }).unwrap()
        ));

        let manager = DeviceManager::new(node_id, 48, db, membership);

        assert_eq!(manager.device_count(), 0);
        assert_eq!(manager.local_bucket_count(), 48);
    }
}
