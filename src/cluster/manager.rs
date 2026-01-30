//! Main cluster manager that orchestrates all cluster functionality.

use std::collections::HashSet;
use std::sync::Arc;

use tokio::net::UdpSocket;
use tokio::sync::RwLock;
use tokio::task::JoinHandle;
use tracing::{error, info};
use uuid::Uuid;

use common::database::api::Database;

use crate::bucket_manager::BucketManager;
use crate::delegation::DelegationHandler;
use crate::error::ClusterError;
use crate::failure_detector::failure_detector_loop;
use crate::membership::{heartbeat_loop, broadcast_message, MembershipList};
use crate::node::{ClusterConfig, NodeStatus};
use crate::protocol::ClusterMessage;
use crate::server::run_cluster_server;

/// Main cluster manager that coordinates all cluster operations.
pub struct ClusterManager {
    /// Cluster configuration
    config: ClusterConfig,
    /// Membership list
    membership: Arc<RwLock<MembershipList>>,
    /// Bucket manager
    bucket_manager: Arc<RwLock<BucketManager>>,
    /// UDP socket for cluster communication
    socket: Arc<UdpSocket>,
    /// Database handle
    database: Database,
    /// Background task handles
    tasks: Vec<JoinHandle<()>>,
}

impl ClusterManager {
    /// Creates a new cluster manager.
    pub async fn new(config: ClusterConfig, database: Database) -> Result<Self, ClusterError> {
        // Create UDP socket for cluster communication
        let bind_addr = format!("{}:{}", config.cluster_ip, config.cluster_port);
        let socket = UdpSocket::bind(&bind_addr).await?;
        let socket = Arc::new(socket);

        info!("Cluster socket bound to {}", bind_addr);

        // Create membership list
        let membership = Arc::new(RwLock::new(MembershipList::new(config.clone())?));

        // Create bucket manager
        let local_node_id = config.node_id;
        let bucket_manager = Arc::new(RwLock::new(BucketManager::new(
            local_node_id,
            config.total_buckets,
            database.clone(),
            membership.clone(),
        )));

        Ok(Self {
            config,
            membership,
            bucket_manager,
            socket,
            database,
            tasks: Vec::new(),
        })
    }

    /// Starts the cluster manager.
    ///
    /// This initializes the cluster by:
    /// 1. Loading bucket assignments from database
    /// 2. Contacting seed nodes if provided
    /// 3. Starting background tasks (heartbeat, failure detection, server)
    pub async fn start(&mut self) -> Result<(), ClusterError> {
        info!("Starting cluster manager for node {}", self.config.node_id);

        // Load bucket assignments from database
        {
            let mut bm = self.bucket_manager.write().await;
            bm.load_from_database().await?;
        }

        // If this is a new node joining an existing cluster, announce ourselves
        if !self.config.cluster_seeds.is_empty() {
            self.join_cluster().await?;
        } else {
            // First node in cluster - claim unassigned buckets
            info!("First node in cluster, claiming unassigned buckets");
            let mut bm = self.bucket_manager.write().await;
            let claimed = bm.claim_unassigned_buckets().await?;
            info!("Claimed {} unassigned buckets", claimed.len());
        }

        // Register this node in the database
        self.database
            .register_cluster_node(
                self.config.node_id,
                self.config.node_name.clone(),
                self.config.cluster_ip.clone(),
                self.config.cluster_port as i32,
                self.config.backdoor_port as i32,
            )
            .await?;

        // Set local node status to active
        {
            let mut m = self.membership.write().await;
            m.set_local_status(NodeStatus::Active);
        }

        // Start background tasks
        self.start_background_tasks();

        info!("Cluster manager started successfully");

        Ok(())
    }

    /// Joins an existing cluster by contacting seed nodes.
    async fn join_cluster(&self) -> Result<(), ClusterError> {
        info!("Joining cluster via {} seed nodes", self.config.cluster_seeds.len());

        // Send NODE_JOIN to all seed nodes
        let (node_id, seq, payload) = {
            let mut m = self.membership.write().await;
            let seq = m.next_seq();
            let node_id = m.local_node_id();
            let payload = m.create_node_join_payload();
            (node_id, seq, payload)
        };

        let join_msg = ClusterMessage::node_join(node_id, seq, payload);

        for seed_addr in &self.config.cluster_seeds {
            info!("Sending NODE_JOIN to seed {}", seed_addr);
            if let Err(e) = crate::membership::send_message(&self.socket, *seed_addr, join_msg.clone()).await {
                error!("Failed to contact seed {}: {}", seed_addr, e);
            }
        }

        // Wait a moment for responses
        tokio::time::sleep(std::time::Duration::from_secs(2)).await;

        // Check if we received any responses
        let node_count = {
            let m = self.membership.read().await;
            m.node_count()
        };

        if node_count > 1 {
            info!("Successfully joined cluster with {} total nodes", node_count);
        } else {
            info!("No other nodes responded, starting as first node");
        }

        // Claim any unassigned buckets
        let mut bm = self.bucket_manager.write().await;
        let claimed = bm.claim_unassigned_buckets().await?;
        if !claimed.is_empty() {
            info!("Claimed {} unassigned buckets", claimed.len());
        }

        Ok(())
    }

    /// Starts all background tasks.
    fn start_background_tasks(&mut self) {
        // Start heartbeat loop
        let heartbeat_task = {
            let membership = self.membership.clone();
            let socket = self.socket.clone();
            let interval = self.config.heartbeat_interval;

            tokio::spawn(async move {
                heartbeat_loop(membership, socket, interval).await;
            })
        };
        self.tasks.push(heartbeat_task);

        // Start failure detector
        let failure_detector_task = {
            let membership = self.membership.clone();
            let bucket_manager = self.bucket_manager.clone();
            let socket = self.socket.clone();
            let config = self.config.clone();

            tokio::spawn(async move {
                failure_detector_loop(membership, bucket_manager, socket, config).await;
            })
        };
        self.tasks.push(failure_detector_task);

        // Start cluster server
        let server_task = {
            let socket = self.socket.clone();
            let membership = self.membership.clone();
            let bucket_manager = self.bucket_manager.clone();
            let database = self.database.clone();

            tokio::spawn(async move {
                if let Err(e) = run_cluster_server(socket, membership, bucket_manager, database).await {
                    error!("Cluster server error: {}", e);
                }
            })
        };
        self.tasks.push(server_task);

        info!("Started {} background tasks", self.tasks.len());
    }

    /// Updates local node statistics.
    pub async fn update_stats(&self, bucket_count: u32, device_count: u32, load_percent: u8) {
        let mut m = self.membership.write().await;
        m.update_local_stats(bucket_count, device_count, load_percent);
    }

    /// Gets the membership list (for external access).
    pub fn membership(&self) -> &Arc<RwLock<MembershipList>> {
        &self.membership
    }

    /// Gets the bucket manager (for external access).
    pub fn bucket_manager(&self) -> &Arc<RwLock<BucketManager>> {
        &self.bucket_manager
    }

    /// Checks if this node owns a specific bucket.
    pub async fn owns_bucket(&self, bucket: i32) -> bool {
        let bm = self.bucket_manager.read().await;
        bm.owns_bucket(bucket)
    }

    /// Gets the local node ID.
    pub fn local_node_id(&self) -> Uuid {
        self.config.node_id
    }

    /// Initiates graceful shutdown.
    ///
    /// Transfers all buckets to other nodes and announces departure.
    pub async fn shutdown(&self) -> Result<(), ClusterError> {
        info!("Initiating graceful cluster shutdown");

        // Set status to draining
        {
            let mut m = self.membership.write().await;
            m.set_local_status(NodeStatus::Draining);
        }

        // Delegate all buckets
        let delegation_handler = DelegationHandler::new(
            self.config.node_id,
            self.bucket_manager.clone(),
            self.membership.clone(),
            self.socket.clone(),
            self.database.clone(),
        );

        delegation_handler.shutdown_delegation().await?;

        // Wait for delegations to complete
        tokio::time::sleep(std::time::Duration::from_secs(2)).await;

        // Broadcast NODE_LEAVE
        let (node_id, seq) = {
            let mut m = self.membership.write().await;
            let node_id = m.local_node_id();
            let seq = m.next_seq();
            (node_id, seq)
        };

        let leave_msg = ClusterMessage::node_leave(node_id, seq);
        broadcast_message(&self.membership, &self.socket, leave_msg).await?;

        // Update node status in database
        self.database
            .update_cluster_node_status(self.config.node_id, "draining")
            .await?;

        info!("Graceful shutdown complete");

        Ok(())
    }

    /// Aborts all background tasks (for testing or emergency shutdown).
    pub fn abort_tasks(&mut self) {
        for task in &self.tasks {
            task.abort();
        }
        self.tasks.clear();
    }

    /// Gets the owned buckets from this cluster manager.
    ///
    /// Returns a set of bucket numbers currently owned by this node.
    pub async fn get_owned_buckets(&self) -> HashSet<i32> {
        let bucket_manager = self.bucket_manager.read().await;
        bucket_manager.owned_buckets().iter().copied().collect()
    }
}

impl Drop for ClusterManager {
    fn drop(&mut self) {
        self.abort_tasks();
    }
}
