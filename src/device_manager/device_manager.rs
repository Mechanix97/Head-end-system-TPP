use std::collections::HashSet;

use tokio::sync::mpsc;

use chrono::{NaiveDateTime, Utc};
use tracing::{debug, info, warn};
use uuid::Uuid;

use common::database::DatabaseError;
use common::database::api::Database;
use common::delegated_device::DelegatedDevice;
use common::device::Device;
use common::scheduled_connection::ScheduledConnection;
use scheduler::error::SchedulerError;
use scheduler::scheduler::Scheduler;

/// Errors that can occur in device manager operations.
#[derive(Debug, thiserror::Error)]
pub enum DeviceManagerError {
    #[error("Database error: {0}")]
    Database(#[from] DatabaseError),
    #[error("Scheduler error: {0}")]
    Scheduler(#[from] SchedulerError),
    #[error("No active nodes available for redistribution")]
    NoNodesAvailable,
}

/// Manages device ownership for a single node with integrated scheduling.
pub struct DeviceManager {
    /// This node's ID
    local_node_id: Uuid,
    /// Total number of local buckets for this node
    local_bucket_count: i32,
    /// Device UUIDs owned by this node
    owned_devices: HashSet<Uuid>,
    /// Database handle
    database: Database,
    /// Scheduler
    scheduler: Scheduler,
}

impl DeviceManager {
    /// Creates a new device manager with an owned scheduler.
    pub fn new(
        local_node_id: Uuid,
        local_bucket_count: i32,
        database: Database,
        scheduler: Scheduler,
    ) -> Self {
        Self {
            local_node_id,
            local_bucket_count,
            owned_devices: HashSet::new(),
            database,
            scheduler,
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

    /// Gets the local bucket count for this node.
    pub fn local_bucket_count(&self) -> i32 {
        self.local_bucket_count
    }

    // --- Scheduler wrapper methods ---

    /// Registers a device in the scheduler (assigns to a time bucket) and adds to owned set.
    pub async fn register_device(&mut self, device: &Device) -> Result<NaiveDateTime, DeviceManagerError> {
        let next_wake = self.scheduler.register_device(device).await?;
        self.owned_devices.insert(device.id);
        Ok(next_wake)
    }

    /// Schedules the next wakeup job for a device.
    pub async fn schedule_next_wakeup(
        &mut self,
        device_id: Uuid,
    ) -> Result<(), DeviceManagerError> {
        self.scheduler.schedule_next_wakeup_job(device_id).await?;
        Ok(())
    }

    /// Schedules a delegated device at its original schedule time.
    pub async fn schedule_delegated_device(
        &mut self,
        device_id: Uuid,
        schedule_time: NaiveDateTime,
    ) -> Result<(), DeviceManagerError> {
        self.scheduler
            .schedule_delegated_device(device_id, schedule_time)
            .await?;
        Ok(())
    }

    /// Removes a device from the scheduler's owned set.
    pub fn remove_scheduled_device(&mut self, device_id: Uuid) {
        self.scheduler.remove_owned_device(device_id);
    }

    /// Enables cluster mode on the scheduler with the given owned devices.
    pub fn enable_cluster_mode(&mut self, owned_devices: HashSet<Uuid>) {
        self.scheduler.enable_cluster_mode(owned_devices);
    }

    /// Sets the reschedule channel sender on the scheduler.
    pub fn set_reschedule_sender(&mut self, tx: mpsc::Sender<Uuid>) {
        self.scheduler.set_reschedule_sender(tx);
    }

    /// Restores scheduled connections from the database.
    ///
    /// Must be called after `enable_cluster_mode()` in cluster mode so that the
    /// scheduler knows which devices it owns before loading connections.
    pub async fn reload_active_connections(&mut self) -> Result<(), DeviceManagerError> {
        self.scheduler
            .reload_active_connections()
            .await
            .map_err(DeviceManagerError::Scheduler)
    }

    /// Gets scheduled connections (delegates to scheduler).
    pub async fn get_scheduled_connections(
        &self,
    ) -> Result<Vec<ScheduledConnection>, DeviceManagerError> {
        self.scheduler
            .get_scheduled_connections()
            .await
            .map_err(DeviceManagerError::Scheduler)
    }

    // --- Device ownership methods ---

    /// Adds a newly registered device to the owned set.
    ///
    /// Called when a device registers at runtime via the backdoor server.
    pub fn add_owned_device(&mut self, device_id: Uuid) {
        self.owned_devices.insert(device_id);
    }

    /// Loads device ownership from the database on startup.
    pub async fn load_from_database(&mut self) -> Result<(), DeviceManagerError> {
        let devices = self
            .database
            .get_devices_by_owner(self.local_node_id)
            .await?;

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
    pub async fn claim_unassigned_devices(&mut self) -> Result<Vec<Uuid>, DeviceManagerError> {
        let all_devices = self.database.get_devices_by_owner(Uuid::nil()).await?;

        if all_devices.is_empty() {
            debug!("No unassigned devices to claim");
            return Ok(Vec::new());
        }

        info!("Claiming {} unassigned devices", all_devices.len());

        for device_id in &all_devices {
            self.database
                .set_device_owner(*device_id, self.local_node_id)
                .await?;
            self.owned_devices.insert(*device_id);
        }

        Ok(all_devices)
    }

    /// Claims all devices when creating a new cluster (first node).
    pub async fn claim_all_devices(&mut self) -> Result<Vec<Uuid>, DeviceManagerError> {
        let all_devices = self.database.get_all_device_ids().await?;

        if all_devices.is_empty() {
            debug!("No devices to claim");
            return Ok(all_devices);
        }

        info!("Claiming {} devices", all_devices.len());

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
    /// `active_node_count` is the total number of active nodes in the cluster
    /// (including this node), provided by the caller from the membership list.
    pub async fn get_devices_for_delegation(
        &mut self,
        target_node_id: Uuid,
        active_node_count: usize,
    ) -> Result<Vec<DelegatedDevice>, DeviceManagerError> {
        if active_node_count <= 1 {
            return Ok(Vec::new());
        }

        let fair_share = self.owned_devices.len() / active_node_count;
        let current_count = self.owned_devices.len();
        let to_give = current_count.saturating_sub(fair_share);

        if to_give == 0 {
            debug!(
                "No devices to give away (have {}, fair share is {})",
                current_count, fair_share
            );
            return Ok(Vec::new());
        }

        let device_ids_to_give: Vec<Uuid> =
            self.owned_devices.iter().copied().take(to_give).collect();

        let mut delegated_devices = Vec::new();

        for device_id in &device_ids_to_give {
            let device = self.database.get_device(*device_id).await?;

            let schedule_time = match self.database.get_scheduled_connection(*device_id).await {
                Ok(conn) => conn.schedule_time,
                Err(_) => Utc::now().naive_utc() + chrono::Duration::days(1),
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

    /// Accepts delegated devices: claims ownership in DB and schedules them at their original times.
    pub async fn accept_delegation(
        &mut self,
        devices: Vec<DelegatedDevice>,
    ) -> Result<Vec<Uuid>, DeviceManagerError> {
        info!("Accepting delegation of {} devices", devices.len());

        let mut accepted_ids = Vec::new();

        for device in devices {
            self.database
                .set_device_owner(device.device_id, self.local_node_id)
                .await?;
            self.owned_devices.insert(device.device_id);
            self.scheduler
                .schedule_delegated_device(device.device_id, device.schedule_time)
                .await?;
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

    /// Gets all owned device IDs for shutdown delegation.
    pub fn devices_for_shutdown(&self) -> Vec<Uuid> {
        self.owned_devices.iter().copied().collect()
    }

    /// Redistributes devices from a failed node across the remaining nodes.
    ///
    /// `other_active_nodes` is `Vec<(node_id, device_count)>` for nodes OTHER than self,
    /// provided by the caller from the membership list. Self is added internally.
    pub async fn redistribute_from_failed(
        &mut self,
        failed_node_id: Uuid,
        other_active_nodes: Vec<(Uuid, usize)>,
    ) -> Result<(), DeviceManagerError> {
        let orphaned = self.database.get_devices_by_owner(failed_node_id).await?;

        if orphaned.is_empty() {
            debug!(
                "Failed node {} had no devices to redistribute",
                failed_node_id
            );
            return Ok(());
        }

        info!(
            "Redistributing {} devices from failed node {}",
            orphaned.len(),
            failed_node_id
        );

        // Build node list: other active nodes + self
        let mut all_nodes = other_active_nodes;
        all_nodes.push((self.local_node_id, self.owned_devices.len()));
        all_nodes.sort_by_key(|(_, count)| *count);

        if all_nodes.is_empty() {
            warn!("No active nodes to redistribute devices to!");
            return Err(DeviceManagerError::NoNodesAvailable);
        }

        for (i, device_id) in orphaned.iter().enumerate() {
            let (target_node_id, _) = all_nodes[i % all_nodes.len()];

            self.database
                .set_device_owner(*device_id, target_node_id)
                .await?;

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

    /// Rebalances devices across the cluster based on load percentage.
    ///
    /// `other_active_nodes` is `Vec<(node_id, device_count, load_percent, max_device_suggested)>`
    /// for nodes OTHER than self, provided by the caller from the membership list.
    /// `local_load` and `local_max` are this node's current load metrics.
    pub async fn rebalance_cluster(
        &mut self,
        other_active_nodes: Vec<(Uuid, u32, f32, u32)>,
        local_load: f32,
        local_max: u32,
    ) -> Result<(), DeviceManagerError> {
        if other_active_nodes.is_empty() {
            debug!("No active nodes to rebalance with");
            return Ok(());
        }

        let my_count = self.owned_devices.len();

        let total_load: f32 = other_active_nodes.iter().map(|(_, _, load, _)| load).sum();
        let avg_load = (total_load + local_load) / (other_active_nodes.len() + 1) as f32;

        let mut underloaded: Vec<_> = other_active_nodes
            .into_iter()
            .filter(|(_, _, load, _)| *load < avg_load)
            .collect();

        if underloaded.is_empty() {
            debug!(
                "No underloaded nodes found (all at or above {:.1}% average)",
                avg_load
            );
            return Ok(());
        }

        underloaded.sort_by(|a, b| a.2.partial_cmp(&b.2).unwrap_or(std::cmp::Ordering::Equal));

        let target_load = avg_load;
        let devices_to_shed = if local_max > 0 {
            let target_count = (target_load / 100.0 * local_max as f32) as usize;
            my_count.saturating_sub(target_count)
        } else {
            0
        };

        if devices_to_shed == 0 {
            debug!(
                "No devices to shed (local load {:.1}% is at target {:.1}%)",
                local_load, target_load
            );
            return Ok(());
        }

        info!(
            "Rebalancing: local load {:.1}%, cluster avg {:.1}%, shedding {} devices to {} underloaded nodes",
            local_load,
            avg_load,
            devices_to_shed,
            underloaded.len()
        );

        let devices_to_give: Vec<Uuid> = self
            .owned_devices
            .iter()
            .copied()
            .take(devices_to_shed)
            .collect();

        let mut device_idx = 0;
        let devices_per_node = devices_to_shed.div_ceil(underloaded.len());

        for (node_id, current_count, current_load, max_suggested) in underloaded {
            let max_acceptable_count = if max_suggested > 0 {
                (avg_load / 100.0 * max_suggested as f32) as u32
            } else {
                current_count + devices_per_node as u32
            };

            let can_take =
                (max_acceptable_count.saturating_sub(current_count) as usize).min(devices_per_node);

            for _ in 0..can_take {
                if device_idx >= devices_to_give.len() {
                    break;
                }
                let device_id = devices_to_give[device_idx];
                self.database.set_device_owner(device_id, node_id).await?;
                self.owned_devices.remove(&device_id);
                device_idx += 1;

                debug!(
                    "Delegated device {} to node {} (was at {:.1}% load)",
                    device_id, node_id, current_load
                );
            }

            if device_idx >= devices_to_give.len() {
                break;
            }
        }

        info!(
            "Rebalance complete: redistributed {} devices, now own {} ({:.1}% load)",
            device_idx,
            self.owned_devices.len(),
            (self.owned_devices.len() as f32 / local_max as f32) * 100.0
        );

        Ok(())
    }

    /// Test-only method to directly add a device without database operations.
    #[cfg(test)]
    pub fn test_add_device(&mut self, device_id: Uuid) {
        self.owned_devices.insert(device_id);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use common::database::{DatabaseConfig, api::Database};

    async fn create_test_scheduler(db: Database, node_id: Uuid) -> Scheduler {
        Scheduler::new(1, db, node_id).await.unwrap()
    }

    #[tokio::test]
    async fn test_device_manager_creation() {
        let db = Database::new(DatabaseConfig::in_memory()).await.unwrap();
        let node_id = Uuid::new_v4();
        let scheduler = create_test_scheduler(db.clone(), node_id).await;
        let manager = DeviceManager::new(node_id, 48, db, scheduler);
        assert_eq!(manager.device_count(), 0);
        assert_eq!(manager.local_bucket_count(), 48);
    }

    #[tokio::test]
    async fn test_add_owned_device() {
        let db = Database::new(DatabaseConfig::in_memory()).await.unwrap();
        let node_id = Uuid::new_v4();
        let scheduler = create_test_scheduler(db.clone(), node_id).await;
        let mut manager = DeviceManager::new(node_id, 48, db, scheduler);
        let device_id = Uuid::new_v4();
        manager.add_owned_device(device_id);
        assert!(manager.owns_device(device_id));
        assert_eq!(manager.device_count(), 1);
    }
}
