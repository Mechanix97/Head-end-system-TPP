use std::net::SocketAddr;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;

use tokio::net::UdpSocket;
use tokio::sync::{RwLock, watch};
use uuid::Uuid;

use cluster::membership::MembershipList;
use common::config_store::ConfigStore;
use common::database::api::Database;
use device_manager::DeviceManager;

/// Lightweight handle to ClusterManager internals, used by RPC handlers.
///
/// Constructed from `ClusterManager` public getters and stored in `RpcState`.
#[derive(Clone)]
pub struct ClusterManagerHandle {
    pub membership: Arc<RwLock<MembershipList>>,
    pub socket: Arc<UdpSocket>,
    pub node_id: Uuid,
    pub local_addr: SocketAddr,
}

impl ClusterManagerHandle {
    /// Sends a NODE_JOIN message to the given seed address.
    pub async fn join_peer(
        &self,
        seed: SocketAddr,
    ) -> Result<(), cluster::error::ClusterError> {
        cluster::membership::send_join_to_peer(&self.socket, &self.membership, seed).await
    }

    /// Broadcasts a CONFIG_UPDATE to all reachable cluster peers.
    ///
    /// Returns the number of peers successfully notified.
    pub async fn broadcast_config_update(
        &self,
        key: &str,
        value: &str,
    ) -> Result<usize, cluster::error::ClusterError> {
        cluster::membership::broadcast_config_update(
            &self.socket,
            &self.membership,
            self.node_id,
            key,
            value,
        )
        .await
    }
}

/// Shared state injected into every RPC handler.
#[derive(Clone)]
pub struct RpcState {
    /// Node's own ID (for `system.version`)
    pub node_id: Uuid,
    /// Config store for reading and writing config values
    pub config_store: Arc<dyn ConfigStore>,
    /// Handle to device manager (for `device.*` methods)
    pub device_manager: Arc<RwLock<DeviceManager>>,
    /// Handle to cluster (None if cluster disabled)
    pub cluster_handle: Option<ClusterManagerHandle>,
    /// Database handle (for `device.*` queries)
    pub database: Database,
    /// Controls whether the Prometheus /metrics endpoint responds (hot-reloadable).
    pub metrics_enabled: Arc<AtomicBool>,
    /// Notifies the backdoor task to rebind to a new (ip, port) (hot-reloadable).
    pub backdoor_rebind: Arc<watch::Sender<(String, String)>>,
}
