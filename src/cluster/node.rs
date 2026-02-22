//! Node information and cluster configuration types.

use std::net::SocketAddr;
use std::time::Duration;

use chrono::{DateTime, Utc};
use uuid::Uuid;

/// Status of a node in the cluster.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NodeStatus {
    /// Node is starting up and joining the cluster
    Starting,
    /// Node is fully operational and handling devices
    Active,
    /// Node is gracefully shutting down, transferring devices
    Draining,
    /// Node is suspected to be unreachable
    Suspect,
    /// Node has been confirmed dead
    Dead,
}

impl NodeStatus {
    /// Returns the status code for wire encoding.
    pub fn code(&self) -> u8 {
        match self {
            NodeStatus::Starting => 0x01,
            NodeStatus::Active => 0x02,
            NodeStatus::Draining => 0x03,
            NodeStatus::Suspect => 0x04,
            NodeStatus::Dead => 0x05,
        }
    }

    /// Creates a status from its wire code.
    pub fn from_code(code: u8) -> Option<Self> {
        match code {
            0x01 => Some(NodeStatus::Starting),
            0x02 => Some(NodeStatus::Active),
            0x03 => Some(NodeStatus::Draining),
            0x04 => Some(NodeStatus::Suspect),
            0x05 => Some(NodeStatus::Dead),
            _ => None,
        }
    }

    /// Returns the string representation for database storage.
    pub fn as_str(&self) -> &'static str {
        match self {
            NodeStatus::Starting => "starting",
            NodeStatus::Active => "active",
            NodeStatus::Draining => "draining",
            NodeStatus::Suspect => "suspect",
            NodeStatus::Dead => "dead",
        }
    }
}

/// Implement FromStr trait for NodeStatus
impl std::str::FromStr for NodeStatus {
    type Err = ();

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "starting" => Ok(NodeStatus::Starting),
            "active" => Ok(NodeStatus::Active),
            "draining" => Ok(NodeStatus::Draining),
            "suspect" => Ok(NodeStatus::Suspect),
            "dead" => Ok(NodeStatus::Dead),
            _ => Err(()),
        }
    }
}

/// Information about a node in the cluster.
#[derive(Debug, Clone)]
pub struct NodeInfo {
    /// Unique identifier for this node
    pub node_id: Uuid,
    /// Human-readable name for the node
    pub node_name: String,
    /// Address for inter-node cluster communication
    pub cluster_addr: SocketAddr,
    /// Port for the backdoor UDP server (device registration)
    pub backdoor_port: u16,
    /// Current status of the node
    pub status: NodeStatus,
    /// When the node started
    pub started_at: DateTime<Utc>,
    /// Last time we received a heartbeat from this node
    pub last_heartbeat: DateTime<Utc>,
    /// Number of buckets owned by this node
    pub bucket_count: u32,
    /// Number of devices managed by this node
    pub device_count: u32,
    /// Number of max devices targeted to be managed by this node (can be surpassed)
    pub max_device_suggested: u32,
    /// Current load percentage (0-100)
    pub load_percent: f32,
}

impl NodeInfo {
    pub const DEFAULT_MAX_DEVICES: u32 = 1000;

    /// Creates a new NodeInfo for the local node.
    pub fn new_local(
        node_id: Uuid,
        node_name: String,
        cluster_addr: SocketAddr,
        backdoor_port: u16,
    ) -> Self {
        let now = Utc::now();
        Self {
            node_id,
            node_name,
            cluster_addr,
            backdoor_port,
            status: NodeStatus::Starting,
            started_at: now,
            last_heartbeat: now,
            bucket_count: 0,
            device_count: 0,
            max_device_suggested: Self::DEFAULT_MAX_DEVICES,
            load_percent: 0.0,
        }
    }

    /// Checks if this node is available for accepting new devices.
    pub fn is_available(&self) -> bool {
        matches!(self.status, NodeStatus::Active)
    }

    /// Checks if this node is considered reachable.
    pub fn is_reachable(&self) -> bool {
        matches!(
            self.status,
            NodeStatus::Starting | NodeStatus::Active | NodeStatus::Draining
        )
    }

    /// Updates the heartbeat timestamp.
    pub fn update_heartbeat(&mut self) {
        self.last_heartbeat = Utc::now();
    }

    /// Calculates time since last heartbeat.
    pub fn time_since_heartbeat(&self) -> Duration {
        let now = Utc::now();
        let diff = now.signed_duration_since(self.last_heartbeat);
        Duration::from_millis(diff.num_milliseconds().max(0) as u64)
    }
}

/// Configuration for cluster behavior.
#[derive(Debug, Clone)]
pub struct ClusterConfig {
    /// This node's unique identifier
    pub node_id: Uuid,
    /// This node's human-readable name
    pub node_name: String,
    /// Port for cluster communication
    pub cluster_port: u16,
    /// IP address to bind for cluster communication
    pub cluster_ip: String,
    /// Seed nodes to contact on startup (format: "ip:port,ip:port")
    pub cluster_seeds: Vec<SocketAddr>,
    /// Interval between heartbeats
    pub heartbeat_interval: Duration,
    /// Time without heartbeat before suspecting a node
    pub suspect_timeout: Duration,
    /// Time in suspect state before declaring dead
    pub dead_timeout: Duration,
    /// Port for the backdoor UDP server
    pub backdoor_port: u16,
    /// Total number of buckets in the system
    pub total_buckets: i32,
}

impl ClusterConfig {
    /// Default heartbeat interval (60 seconds).
    pub const DEFAULT_HEARTBEAT_INTERVAL: Duration = Duration::from_secs(60);
    /// Default suspect timeout (180 seconds - 3 missed heartbeats).
    pub const DEFAULT_SUSPECT_TIMEOUT: Duration = Duration::from_secs(180);
    /// Default dead timeout after suspect (60 seconds - 1 additional heartbeat).
    pub const DEFAULT_DEAD_TIMEOUT: Duration = Duration::from_secs(60);
    /// Default cluster port.
    pub const DEFAULT_CLUSTER_PORT: u16 = 6570;

    /// Creates a new cluster configuration with default timeouts.
    pub fn new(
        node_name: String,
        cluster_ip: String,
        cluster_port: u16,
        backdoor_port: u16,
        total_buckets: i32,
    ) -> Self {
        Self {
            node_id: Uuid::new_v4(),
            node_name,
            cluster_port,
            cluster_ip,
            cluster_seeds: Vec::new(),
            heartbeat_interval: Self::DEFAULT_HEARTBEAT_INTERVAL,
            suspect_timeout: Self::DEFAULT_SUSPECT_TIMEOUT,
            dead_timeout: Self::DEFAULT_DEAD_TIMEOUT,
            backdoor_port,
            total_buckets,
        }
    }

    /// Sets the seed nodes from a comma-separated string.
    pub fn with_seeds(mut self, seeds: &str) -> Result<Self, crate::ClusterError> {
        if seeds.is_empty() {
            return Ok(self);
        }

        for seed in seeds.split(',') {
            let addr: SocketAddr = seed.trim().parse().map_err(|e| {
                crate::ClusterError::InvalidConfig(format!("Invalid seed address '{seed}': {e}"))
            })?;
            self.cluster_seeds.push(addr);
        }
        Ok(self)
    }

    /// Returns the cluster address for this node.
    pub fn cluster_addr(&self) -> Result<SocketAddr, crate::ClusterError> {
        let addr_str = format!("{}:{}", self.cluster_ip, self.cluster_port);
        addr_str.parse().map_err(|e| {
            crate::ClusterError::InvalidConfig(format!("Invalid cluster address: {e}"))
        })
    }

    /// Creates a ClusterConfig from command-line arguments.
    ///
    /// Automatically determines the node name from hostname if not provided.
    pub fn from_cli_args(
        node_id: Uuid,
        node_name: Option<String>,
        cluster_ip: String,
        cluster_port: u16,
        backdoor_port: u16,
        total_buckets: i32,
        cluster_seeds: Option<String>,
    ) -> Result<Self, crate::ClusterError> {
        let node_name = node_name.unwrap_or_else(|| {
            hostname::get()
                .ok()
                .and_then(|h| h.into_string().ok())
                .unwrap_or_else(|| "hes-node".to_string())
        });

        let mut config = Self::new(
            node_name,
            cluster_ip,
            cluster_port,
            backdoor_port,
            total_buckets,
        );
        config.node_id = node_id; // Use provided node_id instead of generating new one

        if let Some(seeds) = cluster_seeds {
            config = config.with_seeds(&seeds)?;
        }

        Ok(config)
    }
}
