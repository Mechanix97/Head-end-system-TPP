//! Payload types for cluster messages.

use std::net::SocketAddr;

use bytes::{Buf, BufMut};
use uuid::Uuid;

use crate::node::NodeStatus;
use super::ClusterCodecError;

// Protocol encoding constants
const OPTION_SOME: u8 = 1;
const OPTION_NONE: u8 = 0;
const BOOL_TRUE: u8 = 1;
const BOOL_FALSE: u8 = 0;

// Size constants for primitive types (in bytes)
const UUID_SIZE: usize = 16;  // u128
const U8_SIZE: usize = 1;
const U16_SIZE: usize = 2;
const U32_SIZE: usize = 4;
const I64_SIZE: usize = 8;

/// Helper: encode optional string
fn encode_optional_string(buf: &mut impl BufMut, opt: &Option<String>) {
    if let Some(s) = opt {
        buf.put_u8(OPTION_SOME);
        let bytes = s.as_bytes();
        buf.put_u16(bytes.len() as u16);
        buf.put_slice(bytes);
    } else {
        buf.put_u8(OPTION_NONE);
    }
}

/// Helper: decode optional string
fn decode_optional_string(buf: &mut impl Buf) -> Result<Option<String>, ClusterCodecError> {
    if buf.get_u8() == OPTION_SOME {
        let len = buf.get_u16() as usize;
        if buf.remaining() < len {
            return Err(ClusterCodecError::InvalidLength);
        }
        let mut bytes = vec![0u8; len];
        buf.copy_to_slice(&mut bytes);
        Ok(Some(String::from_utf8(bytes)
            .map_err(|e| ClusterCodecError::InvalidPayload(e.to_string()))?))
    } else {
        Ok(None)
    }
}

/// Reason for delegation request.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DelegationReason {
    /// Node is overloaded and needs to shed load
    Overload,
    /// Node is shutting down gracefully
    Shutdown,
    /// Cluster is rebalancing after a node joined/left
    Rebalance,
    /// Node that owned these devices has died
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
    /// Suggested maximum number of devices for this node
    pub max_device_suggested: u32,
    /// List of known node IDs
    pub known_nodes: Vec<Uuid>,
}

impl HeartbeatPayload {
    /// Minimum payload size: status + bucket_count + device_count + load + max_device_suggested + node_count
    pub const MIN_SIZE: usize = U8_SIZE + U16_SIZE + U32_SIZE + U8_SIZE + U32_SIZE + U16_SIZE;

    pub fn encode(&self, buf: &mut impl BufMut) {
        buf.put_u8(self.status.code());
        buf.put_u16(self.bucket_count);
        buf.put_u32(self.device_count);
        buf.put_u8(self.load_percent);
        buf.put_u32(self.max_device_suggested);
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
        let max_device_suggested = buf.get_u32();
        let node_count = buf.get_u16() as usize;

        if buf.remaining() < node_count * UUID_SIZE {
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
            max_device_suggested,
            known_nodes,
        })
    }

    pub fn encoded_size(&self) -> usize {
        Self::MIN_SIZE + self.known_nodes.len() * UUID_SIZE
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
    // Minimum size: name_len + addr_len + backdoor_port
    const MIN_SIZE: usize = U16_SIZE + U16_SIZE + U16_SIZE;

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
        if buf.remaining() < Self::MIN_SIZE {
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

        if buf.remaining() < U16_SIZE + U16_SIZE {
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

        if buf.remaining() < U16_SIZE {
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

/// Payload for a single delegated device.
#[derive(Debug, Clone)]
pub struct DelegatedDevicePayload {
    /// Device UUID (16 bytes)
    pub device_id: Uuid,
    /// IPv4 address (optional, length-prefixed)
    pub ipv4: Option<String>,
    /// IPv6 address (optional, length-prefixed)
    pub ipv6: Option<String>,
    /// Scheduled time as Unix timestamp (8 bytes)
    pub schedule_time: i64,
}

impl DelegatedDevicePayload {
    // Minimum size: UUID + ipv4_flag + ipv6_flag + schedule_time
    const MIN_SIZE: usize = UUID_SIZE + U8_SIZE + U8_SIZE + I64_SIZE;

    pub fn encode(&self, buf: &mut impl BufMut) {
        // Device ID (16 bytes)
        buf.put_u128(self.device_id.as_u128());

        // IPv4 (optional)
        encode_optional_string(buf, &self.ipv4);

        // IPv6 (optional)
        encode_optional_string(buf, &self.ipv6);

        // Schedule time (8 bytes)
        buf.put_i64(self.schedule_time);
    }

    pub fn decode(buf: &mut impl Buf) -> Result<Self, ClusterCodecError> {
        if buf.remaining() < Self::MIN_SIZE {
            return Err(ClusterCodecError::InvalidLength);
        }

        // Device ID
        let device_id = Uuid::from_u128(buf.get_u128());

        // IPv4
        let ipv4 = decode_optional_string(buf)?;

        // IPv6
        let ipv6 = decode_optional_string(buf)?;

        // Schedule time
        if buf.remaining() < 8 {
            return Err(ClusterCodecError::InvalidLength);
        }
        let schedule_time = buf.get_i64();

        Ok(Self {
            device_id,
            ipv4,
            ipv6,
            schedule_time,
        })
    }
}

/// Payload for DELEGATE_REQUEST messages.
#[derive(Debug, Clone)]
pub struct DelegateRequestPayload {
    /// Devices to delegate
    pub devices: Vec<DelegatedDevicePayload>,
    /// Reason for delegation
    pub reason: DelegationReason,
}

impl DelegateRequestPayload {
    // Minimum size: device_count + reason (empty device list)
    const MIN_SIZE: usize = U16_SIZE + U8_SIZE;

    pub fn encode(&self, buf: &mut impl BufMut) {
        buf.put_u16(self.devices.len() as u16);
        for device in &self.devices {
            device.encode(buf);
        }
        buf.put_u8(self.reason.code());
    }

    pub fn decode(buf: &mut impl Buf) -> Result<Self, ClusterCodecError> {
        if buf.remaining() < Self::MIN_SIZE {
            return Err(ClusterCodecError::InvalidLength);
        }

        let device_count = buf.get_u16() as usize;
        let mut devices = Vec::with_capacity(device_count);
        for _ in 0..device_count {
            devices.push(DelegatedDevicePayload::decode(buf)?);
        }

        if buf.remaining() < 1 {
            return Err(ClusterCodecError::InvalidLength);
        }

        let reason_code = buf.get_u8();
        let reason = DelegationReason::from_code(reason_code)
            .ok_or_else(|| ClusterCodecError::InvalidPayload(format!("Invalid reason code: {reason_code}")))?;

        Ok(Self {
            devices,
            reason,
        })
    }
}

/// Payload for DELEGATE_ACCEPT messages.
#[derive(Debug, Clone)]
pub struct DelegateAcceptPayload {
    /// Device IDs that were accepted
    pub accepted_device_ids: Vec<Uuid>,
}

impl DelegateAcceptPayload {
    // Minimum size: device_count (empty device list)
    const MIN_SIZE: usize = U16_SIZE;

    pub fn encode(&self, buf: &mut impl BufMut) {
        buf.put_u16(self.accepted_device_ids.len() as u16);
        for device_id in &self.accepted_device_ids {
            buf.put_u128(device_id.as_u128());
        }
    }

    pub fn decode(buf: &mut impl Buf) -> Result<Self, ClusterCodecError> {
        if buf.remaining() < Self::MIN_SIZE {
            return Err(ClusterCodecError::InvalidLength);
        }

        let device_count = buf.get_u16() as usize;
        if buf.remaining() < device_count * 16 {
            return Err(ClusterCodecError::InvalidLength);
        }

        let mut accepted_device_ids = Vec::with_capacity(device_count);
        for _ in 0..device_count {
            accepted_device_ids.push(Uuid::from_u128(buf.get_u128()));
        }

        Ok(Self { accepted_device_ids })
    }
}

/// Payload for DELEGATE_REJECT messages.
#[derive(Debug, Clone)]
pub struct DelegateRejectPayload {
    /// Reason for rejection
    pub reason: String,
}

impl DelegateRejectPayload {
    // Minimum size: string length prefix
    const MIN_SIZE: usize = U16_SIZE;

    pub fn encode(&self, buf: &mut impl BufMut) {
        let bytes = self.reason.as_bytes();
        buf.put_u16(bytes.len() as u16);
        buf.put_slice(bytes);
    }

    pub fn decode(buf: &mut impl Buf) -> Result<Self, ClusterCodecError> {
        if buf.remaining() < Self::MIN_SIZE {
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
    const MIN_SIZE: usize = UUID_SIZE;

    pub fn encode(&self, buf: &mut impl BufMut) {
        buf.put_u128(self.suspect_node_id.as_u128());
    }

    pub fn decode(buf: &mut impl Buf) -> Result<Self, ClusterCodecError> {
        if buf.remaining() < Self::MIN_SIZE {
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
    const MIN_SIZE: usize = UUID_SIZE;

    pub fn encode(&self, buf: &mut impl BufMut) {
        buf.put_u128(self.dead_node_id.as_u128());
    }

    pub fn decode(buf: &mut impl Buf) -> Result<Self, ClusterCodecError> {
        if buf.remaining() < Self::MIN_SIZE {
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
    /// Suggested maximum number of devices for this node
    pub max_device_suggested: u32,
}

impl StatusResponsePayload {
    // Minimum size: name_len + status + bucket_count + device_count + load + max_device_suggested
    const MIN_SIZE: usize = U16_SIZE + U8_SIZE + U16_SIZE + U32_SIZE + U8_SIZE + U32_SIZE;
    // Size after name_len: status + bucket_count + device_count + load + max_device_suggested
    const FIELDS_AFTER_NAME: usize = U8_SIZE + U16_SIZE + U32_SIZE + U8_SIZE + U32_SIZE;

    pub fn encode(&self, buf: &mut impl BufMut) {
        let name_bytes = self.node_name.as_bytes();
        buf.put_u16(name_bytes.len() as u16);
        buf.put_slice(name_bytes);
        buf.put_u8(self.status.code());
        buf.put_u16(self.bucket_count);
        buf.put_u32(self.device_count);
        buf.put_u8(self.load_percent);
        buf.put_u32(self.max_device_suggested);
    }

    pub fn decode(buf: &mut impl Buf) -> Result<Self, ClusterCodecError> {
        if buf.remaining() < Self::MIN_SIZE {
            return Err(ClusterCodecError::InvalidLength);
        }

        let name_len = buf.get_u16() as usize;
        if buf.remaining() < name_len + Self::FIELDS_AFTER_NAME {
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
        let max_device_suggested = buf.get_u32();

        Ok(Self {
            node_name,
            status,
            bucket_count,
            device_count,
            load_percent,
            max_device_suggested,
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
    const MIN_SIZE: usize = UUID_SIZE;

    pub fn encode(&self, buf: &mut impl BufMut) {
        buf.put_u128(self.target_node_id.as_u128());
    }

    pub fn decode(buf: &mut impl Buf) -> Result<Self, ClusterCodecError> {
        if buf.remaining() < Self::MIN_SIZE {
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
    const MIN_SIZE: usize = UUID_SIZE + U8_SIZE;

    pub fn encode(&self, buf: &mut impl BufMut) {
        buf.put_u128(self.target_node_id.as_u128());
        buf.put_u8(if self.is_alive { BOOL_TRUE } else { BOOL_FALSE });
    }

    pub fn decode(buf: &mut impl Buf) -> Result<Self, ClusterCodecError> {
        if buf.remaining() < Self::MIN_SIZE {
            return Err(ClusterCodecError::InvalidLength);
        }
        Ok(Self {
            target_node_id: Uuid::from_u128(buf.get_u128()),
            is_alive: buf.get_u8() == BOOL_TRUE,
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
