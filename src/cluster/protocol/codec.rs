//! Codec for encoding/decoding cluster messages for UDP transport.

use bytes::BytesMut;
use tokio_util::codec::{Decoder, Encoder};

use super::message::{ClusterMessage, ClusterMessageType, CLUSTER_HEADER_SIZE};
use super::payload::*;
use super::ClusterCodecError;

/// Codec for encoding and decoding cluster protocol messages over UDP.
///
/// Implements tokio's Decoder and Encoder traits for use with `UdpFramed`.
#[derive(Debug, Default)]
pub struct ClusterMessageCodec;

impl Decoder for ClusterMessageCodec {
    type Item = ClusterMessage;
    type Error = ClusterCodecError;

    fn decode(&mut self, buf: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        // Need at least the header
        if buf.len() < CLUSTER_HEADER_SIZE {
            if !buf.is_empty() {
                buf.clear();
                return Err(ClusterCodecError::InvalidLength);
            }
            return Ok(None);
        }

        // Decode header
        let (version, msg_type, node_id, seq, timestamp) = ClusterMessage::decode_header(buf)?;

        // Decode payload based on message type
        let payload = decode_payload(msg_type, buf)?;

        Ok(Some(ClusterMessage {
            version,
            msg_type,
            node_id,
            seq,
            timestamp,
            payload,
        }))
    }
}

impl Encoder<ClusterMessage> for ClusterMessageCodec {
    type Error = ClusterCodecError;

    fn encode(&mut self, msg: ClusterMessage, buf: &mut BytesMut) -> Result<(), Self::Error> {
        // Reserve space (header + estimated payload)
        buf.reserve(CLUSTER_HEADER_SIZE + 256);

        // Encode header
        msg.encode_header(buf);

        // Encode payload
        msg.payload.encode(buf);

        Ok(())
    }
}

/// Decodes the payload based on message type.
fn decode_payload(
    msg_type: ClusterMessageType,
    buf: &mut BytesMut,
) -> Result<ClusterPayload, ClusterCodecError> {
    match msg_type {
        ClusterMessageType::Heartbeat => {
            Ok(ClusterPayload::Heartbeat(HeartbeatPayload::decode(buf)?))
        }
        ClusterMessageType::HeartbeatAck => Ok(ClusterPayload::Empty),
        ClusterMessageType::StatusRequest => Ok(ClusterPayload::Empty),
        ClusterMessageType::StatusResponse => {
            Ok(ClusterPayload::StatusResponse(StatusResponsePayload::decode(buf)?))
        }
        ClusterMessageType::DelegateRequest => {
            Ok(ClusterPayload::DelegateRequest(DelegateRequestPayload::decode(buf)?))
        }
        ClusterMessageType::DelegateAccept => {
            Ok(ClusterPayload::DelegateAccept(DelegateAcceptPayload::decode(buf)?))
        }
        ClusterMessageType::DelegateReject => {
            Ok(ClusterPayload::DelegateReject(DelegateRejectPayload::decode(buf)?))
        }
        ClusterMessageType::NodeJoin => {
            Ok(ClusterPayload::NodeJoin(NodeJoinPayload::decode(buf)?))
        }
        ClusterMessageType::NodeLeave => Ok(ClusterPayload::Empty),
        ClusterMessageType::NodeSuspect => {
            Ok(ClusterPayload::NodeSuspect(NodeSuspectPayload::decode(buf)?))
        }
        ClusterMessageType::NodeDead => {
            Ok(ClusterPayload::NodeDead(NodeDeadPayload::decode(buf)?))
        }
        ClusterMessageType::ProbeRequest => {
            Ok(ClusterPayload::ProbeRequest(ProbeRequestPayload::decode(buf)?))
        }
        ClusterMessageType::ProbeResponse => {
            Ok(ClusterPayload::ProbeResponse(ProbeResponsePayload::decode(buf)?))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::node::NodeStatus;
    use uuid::Uuid;

    #[test]
    fn test_heartbeat_roundtrip() {
        let mut codec = ClusterMessageCodec;
        let node_id = Uuid::new_v4();

        let payload = HeartbeatPayload {
            status: NodeStatus::Active,
            bucket_count: 10,
            device_count: 100,
            load_percent: 50,
            known_nodes: vec![Uuid::new_v4(), Uuid::new_v4()],
        };

        let msg = ClusterMessage::heartbeat(node_id, 1, payload);

        let mut buf = BytesMut::new();
        codec.encode(msg.clone(), &mut buf).expect("encode failed");

        let decoded = codec.decode(&mut buf).expect("decode failed").expect("no message");

        assert_eq!(decoded.version, msg.version);
        assert_eq!(decoded.msg_type, ClusterMessageType::Heartbeat);
        assert_eq!(decoded.node_id, node_id);
        assert_eq!(decoded.seq, 1);

        if let ClusterPayload::Heartbeat(p) = decoded.payload {
            assert_eq!(p.status, NodeStatus::Active);
            assert_eq!(p.bucket_count, 10);
            assert_eq!(p.device_count, 100);
            assert_eq!(p.load_percent, 50);
            assert_eq!(p.known_nodes.len(), 2);
        } else {
            panic!("Expected Heartbeat payload");
        }
    }

    #[test]
    fn test_node_join_roundtrip() {
        let mut codec = ClusterMessageCodec;
        let node_id = Uuid::new_v4();

        let payload = NodeJoinPayload {
            node_name: "test-node".to_string(),
            cluster_addr: "127.0.0.1:6570".parse().expect("parse addr"),
            backdoor_port: 6565,
        };

        let msg = ClusterMessage::node_join(node_id, 1, payload);

        let mut buf = BytesMut::new();
        codec.encode(msg, &mut buf).expect("encode failed");

        let decoded = codec.decode(&mut buf).expect("decode failed").expect("no message");

        if let ClusterPayload::NodeJoin(p) = decoded.payload {
            assert_eq!(p.node_name, "test-node");
            assert_eq!(p.backdoor_port, 6565);
        } else {
            panic!("Expected NodeJoin payload");
        }
    }

    #[test]
    fn test_delegate_request_roundtrip() {
        let mut codec = ClusterMessageCodec;
        let node_id = Uuid::new_v4();

        let devices = vec![
            DelegatedDevicePayload {
                device_id: Uuid::new_v4(),
                ipv4: Some("192.168.1.100".to_string()),
                ipv6: None,
                schedule_time: 1234567890,
            },
            DelegatedDevicePayload {
                device_id: Uuid::new_v4(),
                ipv4: Some("192.168.1.101".to_string()),
                ipv6: None,
                schedule_time: 1234567891,
            },
        ];

        let payload = DelegateRequestPayload {
            devices,
            reason: DelegationReason::Rebalance,
        };

        let msg = ClusterMessage::delegate_request(node_id, 1, payload);

        let mut buf = BytesMut::new();
        codec.encode(msg, &mut buf).expect("encode failed");

        let decoded = codec.decode(&mut buf).expect("decode failed").expect("no message");

        if let ClusterPayload::DelegateRequest(p) = decoded.payload {
            assert_eq!(p.devices.len(), 2);
            assert_eq!(p.reason, DelegationReason::Rebalance);
            assert_eq!(p.devices[0].ipv4, Some("192.168.1.100".to_string()));
            assert_eq!(p.devices[1].schedule_time, 1234567891);
        } else {
            panic!("Expected DelegateRequest payload");
        }
    }

    #[test]
    fn test_empty_buffer_returns_none() {
        let mut codec = ClusterMessageCodec;
        let mut buf = BytesMut::new();

        let result = codec.decode(&mut buf).expect("decode failed");
        assert!(result.is_none());
    }

    #[test]
    fn test_short_buffer_returns_error() {
        let mut codec = ClusterMessageCodec;
        let mut buf = BytesMut::from(&[0u8; 10][..]);

        let result = codec.decode(&mut buf);
        assert!(result.is_err());
    }
}
