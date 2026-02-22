//! Cluster message types and structure.

use std::time::{SystemTime, UNIX_EPOCH};

use bytes::{Buf, BufMut};
use uuid::Uuid;

use super::payload::ClusterPayload;
use super::ClusterCodecError;

/// Protocol version for cluster communication.
pub const CLUSTER_PROTOCOL_VERSION: u8 = 1;

/// Types of messages exchanged between cluster nodes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ClusterMessageType {
    /// Periodic heartbeat to indicate node is alive (broadcast)
    Heartbeat,
    /// Acknowledgment of received heartbeat (unicast)
    HeartbeatAck,
    /// Request full status from a node (unicast)
    StatusRequest,
    /// Response with node status (unicast)
    StatusResponse,
    /// Request to delegate devices to another node (unicast)
    DelegateRequest,
    /// Accept device delegation (unicast)
    DelegateAccept,
    /// Reject device delegation (unicast)
    DelegateReject,
    /// Announce new node joining the cluster (broadcast)
    NodeJoin,
    /// Announce graceful node departure (broadcast)
    NodeLeave,
    /// Report a suspected failed node (broadcast)
    NodeSuspect,
    /// Confirm a node is dead (broadcast)
    NodeDead,
    /// Request indirect probe of a node (unicast)
    ProbeRequest,
    /// Response to probe request (unicast)
    ProbeResponse,
}

impl ClusterMessageType {
    /// Returns the wire code for this message type.
    pub fn code(&self) -> u8 {
        match self {
            ClusterMessageType::Heartbeat => 0x01,
            ClusterMessageType::HeartbeatAck => 0x02,
            ClusterMessageType::StatusRequest => 0x10,
            ClusterMessageType::StatusResponse => 0x11,
            ClusterMessageType::DelegateRequest => 0x20,
            ClusterMessageType::DelegateAccept => 0x21,
            ClusterMessageType::DelegateReject => 0x22,
            ClusterMessageType::NodeJoin => 0x30,
            ClusterMessageType::NodeLeave => 0x31,
            ClusterMessageType::NodeSuspect => 0x40,
            ClusterMessageType::NodeDead => 0x41,
            ClusterMessageType::ProbeRequest => 0x50,
            ClusterMessageType::ProbeResponse => 0x51,
        }
    }

    /// Creates a message type from its wire code.
    pub fn from_code(code: u8) -> Result<Self, ClusterCodecError> {
        match code {
            0x01 => Ok(ClusterMessageType::Heartbeat),
            0x02 => Ok(ClusterMessageType::HeartbeatAck),
            0x10 => Ok(ClusterMessageType::StatusRequest),
            0x11 => Ok(ClusterMessageType::StatusResponse),
            0x20 => Ok(ClusterMessageType::DelegateRequest),
            0x21 => Ok(ClusterMessageType::DelegateAccept),
            0x22 => Ok(ClusterMessageType::DelegateReject),
            0x30 => Ok(ClusterMessageType::NodeJoin),
            0x31 => Ok(ClusterMessageType::NodeLeave),
            0x40 => Ok(ClusterMessageType::NodeSuspect),
            0x41 => Ok(ClusterMessageType::NodeDead),
            0x50 => Ok(ClusterMessageType::ProbeRequest),
            0x51 => Ok(ClusterMessageType::ProbeResponse),
            _ => Err(ClusterCodecError::UnknownMessageType(code)),
        }
    }
}

/// A message exchanged between cluster nodes.
///
/// Message structure:
/// ```text
/// ┌──────────┬─────────┬──────────┬─────┬───────────┬─────────┐
/// │ Version  │ MsgType │ NodeID   │ Seq │ Timestamp │ Payload │
/// │ (1 byte) │ (1 byte)│ (16 bytes)│(4 B)│  (8 B)    │ (var)   │
/// └──────────┴─────────┴──────────┴─────┴───────────┴─────────┘
/// ```
#[derive(Debug, Clone)]
pub struct ClusterMessage {
    /// Protocol version
    pub version: u8,
    /// Type of message
    pub msg_type: ClusterMessageType,
    /// ID of the node that sent this message
    pub node_id: Uuid,
    /// Sequence number for ordering
    pub seq: u32,
    /// Unix timestamp in milliseconds
    pub timestamp: u64,
    /// Message-specific payload
    pub payload: ClusterPayload,
}

/// Header size: version(1) + msg_type(1) + node_id(16) + seq(4) + timestamp(8) = 30 bytes
pub const CLUSTER_HEADER_SIZE: usize = 30;

impl ClusterMessage {
    /// Creates a new cluster message with the current timestamp.
    pub fn new(
        node_id: Uuid,
        seq: u32,
        msg_type: ClusterMessageType,
        payload: ClusterPayload,
    ) -> Self {
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0);

        Self {
            version: CLUSTER_PROTOCOL_VERSION,
            msg_type,
            node_id,
            seq,
            timestamp,
            payload,
        }
    }

    /// Creates a heartbeat message.
    pub fn heartbeat(node_id: Uuid, seq: u32, payload: super::HeartbeatPayload) -> Self {
        Self::new(
            node_id,
            seq,
            ClusterMessageType::Heartbeat,
            ClusterPayload::Heartbeat(payload),
        )
    }

    /// Creates a heartbeat acknowledgment message.
    pub fn heartbeat_ack(node_id: Uuid, seq: u32) -> Self {
        Self::new(
            node_id,
            seq,
            ClusterMessageType::HeartbeatAck,
            ClusterPayload::Empty,
        )
    }

    /// Creates a node join announcement.
    pub fn node_join(node_id: Uuid, seq: u32, payload: super::NodeJoinPayload) -> Self {
        Self::new(
            node_id,
            seq,
            ClusterMessageType::NodeJoin,
            ClusterPayload::NodeJoin(payload),
        )
    }

    /// Creates a node leave announcement.
    pub fn node_leave(node_id: Uuid, seq: u32) -> Self {
        Self::new(
            node_id,
            seq,
            ClusterMessageType::NodeLeave,
            ClusterPayload::Empty,
        )
    }

    /// Creates a node suspect announcement.
    pub fn node_suspect(node_id: Uuid, seq: u32, suspect_node_id: Uuid) -> Self {
        Self::new(
            node_id,
            seq,
            ClusterMessageType::NodeSuspect,
            ClusterPayload::NodeSuspect(super::NodeSuspectPayload { suspect_node_id }),
        )
    }

    /// Creates a node dead announcement.
    pub fn node_dead(node_id: Uuid, seq: u32, dead_node_id: Uuid) -> Self {
        Self::new(
            node_id,
            seq,
            ClusterMessageType::NodeDead,
            ClusterPayload::NodeDead(super::NodeDeadPayload { dead_node_id }),
        )
    }

    /// Creates a delegate request message.
    pub fn delegate_request(node_id: Uuid, seq: u32, payload: super::DelegateRequestPayload) -> Self {
        Self::new(
            node_id,
            seq,
            ClusterMessageType::DelegateRequest,
            ClusterPayload::DelegateRequest(payload),
        )
    }

    /// Creates a delegate accept message.
    pub fn delegate_accept(node_id: Uuid, seq: u32, payload: super::DelegateAcceptPayload) -> Self {
        Self::new(
            node_id,
            seq,
            ClusterMessageType::DelegateAccept,
            ClusterPayload::DelegateAccept(payload),
        )
    }

    /// Creates a delegate reject message.
    pub fn delegate_reject(node_id: Uuid, seq: u32, reason: String) -> Self {
        Self::new(
            node_id,
            seq,
            ClusterMessageType::DelegateReject,
            ClusterPayload::DelegateReject(super::DelegateRejectPayload { reason }),
        )
    }

    /// Creates a status request message.
    pub fn status_request(node_id: Uuid, seq: u32) -> Self {
        Self::new(
            node_id,
            seq,
            ClusterMessageType::StatusRequest,
            ClusterPayload::Empty,
        )
    }

    /// Creates a status response message.
    pub fn status_response(node_id: Uuid, seq: u32, payload: super::StatusResponsePayload) -> Self {
        Self::new(
            node_id,
            seq,
            ClusterMessageType::StatusResponse,
            ClusterPayload::StatusResponse(payload),
        )
    }

    /// Creates a probe request message.
    pub fn probe_request(node_id: Uuid, seq: u32, target_node_id: Uuid) -> Self {
        Self::new(
            node_id,
            seq,
            ClusterMessageType::ProbeRequest,
            ClusterPayload::ProbeRequest(super::ProbeRequestPayload { target_node_id }),
        )
    }

    /// Creates a probe response message.
    pub fn probe_response(node_id: Uuid, seq: u32, target_node_id: Uuid, is_alive: bool) -> Self {
        Self::new(
            node_id,
            seq,
            ClusterMessageType::ProbeResponse,
            ClusterPayload::ProbeResponse(super::ProbeResponsePayload {
                target_node_id,
                is_alive,
            }),
        )
    }

    /// Encodes the message header into a buffer.
    pub fn encode_header(&self, buf: &mut impl BufMut) {
        buf.put_u8(self.version);
        buf.put_u8(self.msg_type.code());
        buf.put_u128(self.node_id.as_u128());
        buf.put_u32(self.seq);
        buf.put_u64(self.timestamp);
    }

    /// Decodes the message header from a buffer.
    pub fn decode_header(buf: &mut impl Buf) -> Result<(u8, ClusterMessageType, Uuid, u32, u64), ClusterCodecError> {
        if buf.remaining() < CLUSTER_HEADER_SIZE {
            return Err(ClusterCodecError::InvalidLength);
        }

        let version = buf.get_u8();
        let msg_type_code = buf.get_u8();
        let node_id = Uuid::from_u128(buf.get_u128());
        let seq = buf.get_u32();
        let timestamp = buf.get_u64();

        let msg_type = ClusterMessageType::from_code(msg_type_code)?;

        Ok((version, msg_type, node_id, seq, timestamp))
    }
}
