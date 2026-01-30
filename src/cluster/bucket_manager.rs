//! Bucket assignment and redistribution management.
//!
//! Handles the distribution of time buckets across cluster nodes.

use std::collections::HashSet;
use std::sync::Arc;

use tokio::sync::RwLock;
use tracing::{debug, info, warn};
use uuid::Uuid;

use common::database::api::Database;

use crate::error::ClusterError;
use crate::membership::MembershipList;

/// Manages bucket assignment across cluster nodes.
#[derive(Debug)]
pub struct BucketManager {
    /// This node's ID
    local_node_id: Uuid,
    /// Total number of buckets in the system
    total_buckets: i32,
    /// Buckets owned by this node
    owned_buckets: HashSet<i32>,
    /// Database handle
    database: Database,
    /// Reference to membership list for node info
    membership: Arc<RwLock<MembershipList>>,
}

impl BucketManager {
    /// Creates a new bucket manager.
    pub fn new(
        local_node_id: Uuid,
        total_buckets: i32,
        database: Database,
        membership: Arc<RwLock<MembershipList>>,
    ) -> Self {
        Self {
            local_node_id,
            total_buckets,
            owned_buckets: HashSet::new(),
            database,
            membership,
        }
    }

    /// Gets the local node ID.
    pub fn local_node_id(&self) -> Uuid {
        self.local_node_id
    }

    /// Gets the buckets owned by this node.
    pub fn owned_buckets(&self) -> &HashSet<i32> {
        &self.owned_buckets
    }

    /// Gets the number of buckets owned by this node.
    pub fn bucket_count(&self) -> usize {
        self.owned_buckets.len()
    }

    /// Checks if this node owns a specific bucket.
    pub fn owns_bucket(&self, bucket: i32) -> bool {
        self.owned_buckets.contains(&bucket)
    }

    /// Loads bucket assignments from the database on startup.
    pub async fn load_from_database(&mut self) -> Result<(), ClusterError> {
        let assignments = self.database.get_bucket_assignments().await?;

        self.owned_buckets.clear();
        for (bucket, owner_id) in assignments {
            if owner_id == self.local_node_id {
                self.owned_buckets.insert(bucket);
            }
        }

        info!(
            "Loaded {} bucket assignments from database",
            self.owned_buckets.len()
        );
        Ok(())
    }

    /// Claims unassigned buckets when joining the cluster.
    ///
    /// This is called when a new node joins. It will claim any buckets that
    /// don't have an owner (orphaned from a dead node or never assigned).
    pub async fn claim_unassigned_buckets(&mut self) -> Result<Vec<i32>, ClusterError> {
        let assignments = self.database.get_bucket_assignments().await?;

        // Find buckets without an owner
        let assigned: HashSet<i32> = assignments.into_iter().map(|(b, _)| b).collect();
        let unassigned: Vec<i32> = (0..self.total_buckets)
            .filter(|b| !assigned.contains(b))
            .collect();

        if unassigned.is_empty() {
            debug!("No unassigned buckets to claim");
            return Ok(Vec::new());
        }

        info!("Claiming {} unassigned buckets", unassigned.len());

        // Claim all unassigned buckets
        for &bucket in &unassigned {
            self.database
                .assign_bucket(bucket, self.local_node_id)
                .await?;
            self.owned_buckets.insert(bucket);
        }

        Ok(unassigned)
    }

    /// Assigns buckets to a new node for load balancing.
    ///
    /// This is called when a new node joins to give it a fair share of buckets.
    /// Returns the buckets that were given away.
    pub async fn assign_buckets_on_join(&mut self, new_node_id: Uuid) -> Result<Vec<i32>, ClusterError> {
        // Calculate fair share
        let active_node_count = {
            let membership = self.membership.read().await;
            membership.active_node_count() as i32
        };

        if active_node_count <= 1 {
            return Ok(Vec::new());
        }

        let fair_share = self.total_buckets / active_node_count;
        let current_count = self.owned_buckets.len() as i32;
        let to_give = current_count - fair_share;

        if to_give <= 0 {
            debug!("No buckets to give away (have {}, fair share is {})", current_count, fair_share);
            return Ok(Vec::new());
        }

        // Select buckets to give away (arbitrary selection - just take first N)
        let buckets_to_give: Vec<i32> = self.owned_buckets.iter().copied().take(to_give as usize).collect();

        info!(
            "Giving {} buckets to new node {} (had {}, fair share is {})",
            buckets_to_give.len(),
            new_node_id,
            current_count,
            fair_share
        );

        // Transfer buckets
        for &bucket in &buckets_to_give {
            self.database.assign_bucket(bucket, new_node_id).await?;
            self.owned_buckets.remove(&bucket);
        }

        Ok(buckets_to_give)
    }

    /// Redistributes buckets from a failed node.
    ///
    /// This distributes the failed node's buckets among remaining active nodes.
    pub async fn redistribute_from_failed(&mut self, failed_node_id: Uuid) -> Result<(), ClusterError> {
        // Get buckets owned by failed node
        let assignments = self.database.get_bucket_assignments().await?;
        let orphaned: Vec<i32> = assignments
            .into_iter()
            .filter(|(_, owner)| *owner == failed_node_id)
            .map(|(bucket, _)| bucket)
            .collect();

        if orphaned.is_empty() {
            debug!("Failed node {} had no buckets to redistribute", failed_node_id);
            return Ok(());
        }

        info!(
            "Redistributing {} buckets from failed node {}",
            orphaned.len(),
            failed_node_id
        );

        // Get active nodes sorted by bucket count (least loaded first)
        let active_nodes: Vec<(Uuid, usize)> = {
            let membership = self.membership.read().await;
            let mut nodes: Vec<_> = membership
                .active_nodes()
                .iter()
                .map(|n| (n.node_id, n.bucket_count as usize))
                .collect();
            // Include self
            nodes.push((self.local_node_id, self.owned_buckets.len()));
            nodes.sort_by_key(|(_, count)| *count);
            nodes
        };

        if active_nodes.is_empty() {
            warn!("No active nodes to redistribute buckets to!");
            return Err(ClusterError::NoNodesAvailable);
        }

        // Distribute buckets round-robin among active nodes, least loaded first
        for (i, bucket) in orphaned.iter().enumerate() {
            let (target_node_id, _) = active_nodes[i % active_nodes.len()];

            self.database.assign_bucket(*bucket, target_node_id).await?;

            if target_node_id == self.local_node_id {
                self.owned_buckets.insert(*bucket);
                debug!("Claimed orphaned bucket {}", bucket);
            }
        }

        info!(
            "Redistribution complete, this node now owns {} buckets",
            self.owned_buckets.len()
        );

        Ok(())
    }

    /// Rebalances buckets across the cluster.
    ///
    /// Called when nodes have significantly uneven bucket counts.
    pub async fn rebalance_cluster(&mut self) -> Result<(), ClusterError> {
        let (active_nodes, my_count) = {
            let membership = self.membership.read().await;
            let nodes: Vec<_> = membership
                .active_nodes()
                .iter()
                .map(|n| (n.node_id, n.bucket_count as i32))
                .collect();
            let my_count = self.owned_buckets.len() as i32;
            (nodes, my_count)
        };

        let total_nodes = active_nodes.len() as i32 + 1; // +1 for self
        let fair_share = self.total_buckets / total_nodes;
        let excess = my_count - fair_share;

        if excess <= 0 {
            debug!("No excess buckets to redistribute (have {}, fair share is {})", my_count, fair_share);
            return Ok(());
        }

        // Find underloaded nodes
        let underloaded: Vec<_> = active_nodes
            .into_iter()
            .filter(|(_, count)| *count < fair_share)
            .collect();

        if underloaded.is_empty() {
            debug!("No underloaded nodes to give buckets to");
            return Ok(());
        }

        // Give buckets to underloaded nodes
        let buckets_to_give: Vec<i32> = self.owned_buckets.iter().copied().take(excess as usize).collect();

        let mut bucket_idx = 0;
        for (node_id, count) in underloaded {
            let needed = fair_share - count;
            for _ in 0..needed {
                if bucket_idx >= buckets_to_give.len() {
                    break;
                }
                let bucket = buckets_to_give[bucket_idx];
                self.database.assign_bucket(bucket, node_id).await?;
                self.owned_buckets.remove(&bucket);
                bucket_idx += 1;
            }
        }

        info!(
            "Rebalance complete, gave away {} buckets, now own {}",
            bucket_idx,
            self.owned_buckets.len()
        );

        Ok(())
    }

    /// Accepts delegated buckets from another node.
    pub async fn accept_delegation(&mut self, buckets: Vec<i32>) -> Result<(), ClusterError> {
        info!("Accepting delegation of {} buckets", buckets.len());

        for bucket in buckets {
            self.database.assign_bucket(bucket, self.local_node_id).await?;
            self.owned_buckets.insert(bucket);
        }

        Ok(())
    }

    /// Releases buckets for delegation to another node.
    pub fn release_buckets(&mut self, buckets: &[i32]) {
        for bucket in buckets {
            self.owned_buckets.remove(bucket);
        }
    }

    /// Gets buckets to delegate when shutting down.
    pub fn buckets_for_shutdown(&self) -> Vec<i32> {
        self.owned_buckets.iter().copied().collect()
    }

    /// Test-only method to directly add a bucket without database operations.
    #[cfg(test)]
    pub fn test_add_bucket(&mut self, bucket: i32) {
        self.owned_buckets.insert(bucket);
    }
}

#[cfg(test)]
mod tests {
    

    // Integration tests would go here, but they require a database mock
}
