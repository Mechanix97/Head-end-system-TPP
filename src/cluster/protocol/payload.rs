//! Payload types for cluster messages.

use std::net::SocketAddr;

use bytes::{Buf, BufMut};
use uuid::Uuid;

use crate::node::NodeStatus;
use super::ClusterCodecError;

/// Reason for delegation request.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DelegationReason {
    /// Node is overloaded and needs to shed load
    Overload,
    /// Node is shutting down gracefully
    Shutdown,
    /// Cluster is rebalancing after a node joined/left
    Rebalance,
    /// Node that owned these buckets has died
    NodeFailure,
}

impl DelegationReason {
    pub fn code(&self) -> u8 {
        match self {
            DelegationReason::Overload => 0x01,
            DelegationReason::Shutdown => 0x02,
            DelegationReason::Rebalance => 0x03,
            DelegationReason::NodeFailure => 0x04,
        }
    }

    pub fn from_code(code: u8) -> Option<Self> {
        match code {
            0x01 => Some(DelegationReason::Overload),
            0x02 => Some(DelegationReason::Shutdown),
            0x03 => Some(DelegationReason::Rebalance),
            0x04 => Some(DelegationReason::NodeFailure),
            _ => None,
        }
    }
}

/// Payload for HEARTBEAT messages.
#[derive(Debug, Clone)]
pub struct HeartbeatPayload {
    /// Current status of the node
    pub status: NodeStatus,
    /// Number of buckets owned by this node
    pub bucket_count: u16,
    /// Number of devices managed by this node
    pub device_count: u32,
    /// Current load percentage (0-100)
    pub load_percent: u8,
    /// List of known node IDs
    pub known_nodes: Vec<Uuid>,
}

impl HeartbeatPayload {
    /// Minimum payload size: status(1) + bucket_count(2) + device_count(4) + load(1) + node_count(2) = 10
    pub const MIN_SIZE: usize = 10;

    pub fn encode(&self, buf: &mut impl BufMut) {
        buf.put_u8(self.status.code());
        buf.put_u16(self.bucket_count);
        buf.put_u32(self.device_count);
        buf.put_u8(self.load_percent);
        buf.put_u16(self.known_nodes.len() as u16);
        for node_id in &self.known_nodes {
            buf.put_u128(node_id.as_u128());
        }
    }

    pub fn decode(buf: &mut impl Buf) -> Result<Self, ClusterCodecError> {
        if buf.remaining() < Self::MIN_SIZE {
            return Err(ClusterCodecError::InvalidLength);
        }

        let status_code = buf.get_u8();
        let status = NodeStatus::from_code(status_code)
            .ok_or_else(|| ClusterCodecError::InvalidPayload(format!("Invalid status code: {status_code}")))?;
        let bucket_count = buf.get_u16();
        let device_count = buf.get_u32();
        let load_percent = buf.get_u8();
        let node_count = buf.get_u16() as usize;

        if buf.remaining() < node_count * 16 {
            return Err(ClusterCodecError::InvalidLength);
        }

        let mut known_nodes = Vec::with_capacity(node_count);
        for _ in 0..node_count {
            known_nodes.push(Uuid::from_u128(buf.get_u128()));
        }

        Ok(Self {
            status,
            bucket_count,
            device_count,
            load_percent,
            known_nodes,
        })
    }

    pub fn encoded_size(&self) -> usize {
        Self::MIN_SIZE + self.known_nodes.len() * 16
    }
}

/// Payload for NODE_JOIN messages.
#[derive(Debug, Clone)]
pub struct NodeJoinPayload {
    /// Name of the joining node
    pub node_name: String,
    /// Cluster address of the joining node
    pub cluster_addr: SocketAddr,
    /// Backdoor port of the joining node
    pub backdoor_port: u16,
}

impl NodeJoinPayload {
    pub fn encode(&self, buf: &mut impl BufMut) {
        // Encode node name with length prefix
        let name_bytes = self.node_name.as_bytes();
        buf.put_u16(name_bytes.len() as u16);
        buf.put_slice(name_bytes);

        // Encode cluster address as string with length prefix
        let addr_str = self.cluster_addr.to_string();
        let addr_bytes = addr_str.as_bytes();
        buf.put_u16(addr_bytes.len() as u16);
        buf.put_slice(addr_bytes);

        buf.put_u16(self.backdoor_port);
    }

    pub fn decode(buf: &mut impl Buf) -> Result<Self, ClusterCodecError> {
        if buf.remaining() < 6 {
            return Err(ClusterCodecError::InvalidLength);
        }

        // Decode node name
        let name_len = buf.get_u16() as usize;
        if buf.remaining() < name_len {
            return Err(ClusterCodecError::InvalidLength);
        }
        let mut name_bytes = vec![0u8; name_len];
        buf.copy_to_slice(&mut name_bytes);
        let node_name = String::from_utf8(name_bytes)
            .map_err(|e| ClusterCodecError::InvalidPayload(e.to_string()))?;

        if buf.remaining() < 4 {
            return Err(ClusterCodecError::InvalidLength);
        }

        // Decode cluster address
        let addr_len = buf.get_u16() as usize;
        if buf.remaining() < addr_len {
            return Err(ClusterCodecError::InvalidLength);
        }
        let mut addr_bytes = vec![0u8; addr_len];
        buf.copy_to_slice(&mut addr_bytes);
        let addr_str = String::from_utf8(addr_bytes)
            .map_err(|e| ClusterCodecError::InvalidPayload(e.to_string()))?;
        let cluster_addr: SocketAddr = addr_str
            .parse()
            .map_err(|e| ClusterCodecError::InvalidPayload(format!("Invalid address: {e}")))?;

        if buf.remaining() < 2 {
            return Err(ClusterCodecError::InvalidLength);
        }
        let backdoor_port = buf.get_u16();

        Ok(Self {
            node_name,
            cluster_addr,
            backdoor_port,
        })
    }
}

/// Payload for DELEGATE_REQUEST messages.
#[derive(Debug, Clone)]
pub struct DelegateRequestPayload {
    /// Buckets to delegate
    pub buckets: Vec<i32>,
    /// Reason for delegation
    pub reason: DelegationReason,
    /// Number of devices affected
    pub device_count: u32,
}

impl DelegateRequestPayload {
    pub fn encode(&self, buf: &mut impl BufMut) {
        buf.put_u16(self.buckets.len() as u16);
        for bucket in &self.buckets {
            buf.put_i32(*bucket);
        }
        buf.put_u8(self.reason.code());
        buf.put_u32(self.device_count);
    }

    pub fn decode(buf: &mut impl Buf) -> Result<Self, ClusterCodecError> {
        if buf.remaining() < 7 {
            return Err(ClusterCodecError::InvalidLength);
        }

        let bucket_count = buf.get_u16() as usize;
        if buf.remaining() < bucket_count * 4 + 5 {
            return Err(ClusterCodecError::InvalidLength);
        }

        let mut buckets = Vec::with_capacity(bucket_count);
        for _ in 0..bucket_count {
            buckets.push(buf.get_i32());
        }

        let reason_code = buf.get_u8();
        let reason = DelegationReason::from_code(reason_code)
            .ok_or_else(|| ClusterCodecError::InvalidPayload(format!("Invalid reason code: {reason_code}")))?;
        let device_count = buf.get_u32();

        Ok(Self {
            buckets,
            reason,
            device_count,
        })
    }
}

/// Payload for DELEGATE_ACCEPT messages.
#[derive(Debug, Clone)]
pub struct DelegateAcceptPayload {
    /// Buckets that were accepted
    pub buckets: Vec<i32>,
}

impl DelegateAcceptPayload {
    pub fn encode(&self, buf: &mut impl BufMut) {
        buf.put_u16(self.buckets.len() as u16);
        for bucket in &self.buckets {
            buf.put_i32(*bucket);
        }
    }

    pub fn decode(buf: &mut impl Buf) -> Result<Self, ClusterCodecError> {
        if buf.remaining() < 2 {
            return Err(ClusterCodecError::InvalidLength);
        }

        let bucket_count = buf.get_u16() as usize;
        if buf.remaining() < bucket_count * 4 {
            return Err(ClusterCodecError::InvalidLength);
        }

        let mut buckets = Vec::with_capacity(bucket_count);
        for _ in 0..bucket_count {
            buckets.push(buf.get_i32());
        }

        Ok(Self { buckets })
    }
}

/// Payload for DELEGATE_REJECT messages.
#[derive(Debug, Clone)]
pub struct DelegateRejectPayload {
    /// Reason for rejection
    pub reason: String,
}

impl DelegateRejectPayload {
    pub fn encode(&self, buf: &mut impl BufMut) {
        let bytes = self.reason.as_bytes();
        buf.put_u16(bytes.len() as u16);
        buf.put_slice(bytes);
    }

    pub fn decode(buf: &mut impl Buf) -> Result<Self, ClusterCodecError> {
        if buf.remaining() < 2 {
            return Err(ClusterCodecError::InvalidLength);
        }

        let len = buf.get_u16() as usize;
        if buf.remaining() < len {
            return Err(ClusterCodecError::InvalidLength);
        }

        let mut bytes = vec![0u8; len];
        buf.copy_to_slice(&mut bytes);
        let reason = String::from_utf8(bytes)
            .map_err(|e| ClusterCodecError::InvalidPayload(e.to_string()))?;

        Ok(Self { reason })
    }
}

/// Payload for NODE_SUSPECT messages.
#[derive(Debug, Clone)]
pub struct NodeSuspectPayload {
    /// ID of the suspected node
    pub suspect_node_id: Uuid,
}

impl NodeSuspectPayload {
    pub fn encode(&self, buf: &mut impl BufMut) {
        buf.put_u128(self.suspect_node_id.as_u128());
    }

    pub fn decode(buf: &mut impl Buf) -> Result<Self, ClusterCodecError> {
        if buf.remaining() < 16 {
            return Err(ClusterCodecError::InvalidLength);
        }
        Ok(Self {
            suspect_node_id: Uuid::from_u128(buf.get_u128()),
        })
    }
}

/// Payload for NODE_DEAD messages.
#[derive(Debug, Clone)]
pub struct NodeDeadPayload {
    /// ID of the dead node
    pub dead_node_id: Uuid,
}

impl NodeDeadPayload {
    pub fn encode(&self, buf: &mut impl BufMut) {
        buf.put_u128(self.dead_node_id.as_u128());
    }

    pub fn decode(buf: &mut impl Buf) -> Result<Self, ClusterCodecError> {
        if buf.remaining() < 16 {
            return Err(ClusterCodecError::InvalidLength);
        }
        Ok(Self {
            dead_node_id: Uuid::from_u128(buf.get_u128()),
        })
    }
}

/// Payload for STATUS_RESPONSE messages.
#[derive(Debug, Clone)]
pub struct StatusResponsePayload {
    /// Node name
    pub node_name: String,
    /// Current status
    pub status: NodeStatus,
    /// Number of buckets owned
    pub bucket_count: u16,
    /// Number of devices managed
    pub device_count: u32,
    /// Load percentage
    pub load_percent: u8,
    /// List of owned bucket numbers
    pub owned_buckets: Vec<i32>,
}

impl StatusResponsePayload {
    pub fn encode(&self, buf: &mut impl BufMut) {
        let name_bytes = self.node_name.as_bytes();
        buf.put_u16(name_bytes.len() as u16);
        buf.put_slice(name_bytes);
        buf.put_u8(self.status.code());
        buf.put_u16(self.bucket_count);
        buf.put_u32(self.device_count);
        buf.put_u8(self.load_percent);
        buf.put_u16(self.owned_buckets.len() as u16);
        for bucket in &self.owned_buckets {
            buf.put_i32(*bucket);
        }
    }

    pub fn decode(buf: &mut impl Buf) -> Result<Self, ClusterCodecError> {
        if buf.remaining() < 12 {
            return Err(ClusterCodecError::InvalidLength);
        }

        let name_len = buf.get_u16() as usize;
        if buf.remaining() < name_len + 10 {
            return Err(ClusterCodecError::InvalidLength);
        }

        let mut name_bytes = vec![0u8; name_len];
        buf.copy_to_slice(&mut name_bytes);
        let node_name = String::from_utf8(name_bytes)
            .map_err(|e| ClusterCodecError::InvalidPayload(e.to_string()))?;

        let status_code = buf.get_u8();
        let status = NodeStatus::from_code(status_code)
            .ok_or_else(|| ClusterCodecError::InvalidPayload(format!("Invalid status: {status_code}")))?;
        let bucket_count = buf.get_u16();
        let device_count = buf.get_u32();
        let load_percent = buf.get_u8();
        let owned_count = buf.get_u16() as usize;

        if buf.remaining() < owned_count * 4 {
            return Err(ClusterCodecError::InvalidLength);
        }

        let mut owned_buckets = Vec::with_capacity(owned_count);
        for _ in 0..owned_count {
            owned_buckets.push(buf.get_i32());
        }

        Ok(Self {
            node_name,
            status,
            bucket_count,
            device_count,
            load_percent,
            owned_buckets,
        })
    }
}

/// Payload for PROBE_REQUEST messages.
#[derive(Debug, Clone)]
pub struct ProbeRequestPayload {
    /// ID of the node to probe
    pub target_node_id: Uuid,
}

impl ProbeRequestPayload {
    pub fn encode(&self, buf: &mut impl BufMut) {
        buf.put_u128(self.target_node_id.as_u128());
    }

    pub fn decode(buf: &mut impl Buf) -> Result<Self, ClusterCodecError> {
        if buf.remaining() < 16 {
            return Err(ClusterCodecError::InvalidLength);
        }
        Ok(Self {
            target_node_id: Uuid::from_u128(buf.get_u128()),
        })
    }
}

/// Payload for PROBE_RESPONSE messages.
#[derive(Debug, Clone)]
pub struct ProbeResponsePayload {
    /// ID of the probed node
    pub target_node_id: Uuid,
    /// Whether the node responded
    pub is_alive: bool,
}

impl ProbeResponsePayload {
    pub fn encode(&self, buf: &mut impl BufMut) {
        buf.put_u128(self.target_node_id.as_u128());
        buf.put_u8(if self.is_alive { 1 } else { 0 });
    }

    pub fn decode(buf: &mut impl Buf) -> Result<Self, ClusterCodecError> {
        if buf.remaining() < 17 {
            return Err(ClusterCodecError::InvalidLength);
        }
        Ok(Self {
            target_node_id: Uuid::from_u128(buf.get_u128()),
            is_alive: buf.get_u8() != 0,
        })
    }
}

/// Union of all possible payloads.
#[derive(Debug, Clone)]
pub enum ClusterPayload {
    Empty,
    Heartbeat(HeartbeatPayload),
    NodeJoin(NodeJoinPayload),
    NodeSuspect(NodeSuspectPayload),
    NodeDead(NodeDeadPayload),
    DelegateRequest(DelegateRequestPayload),
    DelegateAccept(DelegateAcceptPayload),
    DelegateReject(DelegateRejectPayload),
    StatusResponse(StatusResponsePayload),
    ProbeRequest(ProbeRequestPayload),
    ProbeResponse(ProbeResponsePayload),
}

impl ClusterPayload {
    pub fn encode(&self, buf: &mut impl BufMut) {
        match self {
            ClusterPayload::Empty => {}
            ClusterPayload::Heartbeat(p) => p.encode(buf),
            ClusterPayload::NodeJoin(p) => p.encode(buf),
            ClusterPayload::NodeSuspect(p) => p.encode(buf),
            ClusterPayload::NodeDead(p) => p.encode(buf),
            ClusterPayload::DelegateRequest(p) => p.encode(buf),
            ClusterPayload::DelegateAccept(p) => p.encode(buf),
            ClusterPayload::DelegateReject(p) => p.encode(buf),
            ClusterPayload::StatusResponse(p) => p.encode(buf),
            ClusterPayload::ProbeRequest(p) => p.encode(buf),
            ClusterPayload::ProbeResponse(p) => p.encode(buf),
        }
    }
}
