//! Cluster module for horizontal scaling of the HES.
//!
//! This module implements a distributed clustering system that allows multiple HES nodes
//! to work together, distributing device connections across nodes and handling failover
//! when nodes become unavailable.
//!
//! # Architecture
//!
//! The cluster uses a SWIM-like protocol for failure detection with:
//! - Heartbeats every 60 seconds
//! - 180-second suspect timeout + 60-second dead timeout for declaring a node dead
//! - Device-based delegation for load balancing (each node has fixed local buckets)
//!
//! # Components
//!
//! - `node`: Node information and status tracking
//! - `protocol`: Inter-node communication protocol (messages, codec)
//! - `membership`: Membership list management and heartbeat logic
//! - `failure_detector`: SWIM-like failure detection
//! - `device_manager`: Device ownership and delegation management
//! - `delegation`: Device delegation between nodes
//! - `server`: UDP server for inter-node communication
//! - `manager`: Main cluster manager that orchestrates everything

pub mod device_manager;
pub mod delegation;
pub mod error;
pub mod failure_detector;
pub mod manager;
pub mod membership;
pub mod node;
pub mod protocol;
pub mod server;

pub use error::ClusterError;
pub use manager::ClusterManager;
pub use node::{ClusterConfig, NodeInfo, NodeStatus};

#[cfg(test)]
mod node_test;
#[cfg(test)]
mod membership_test;
// #[cfg(test)]
// mod device_manager_test;  // TODO: Create tests for DeviceManager
#[cfg(test)]
mod integration_tests;
