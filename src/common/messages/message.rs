use std::time::{SystemTime, UNIX_EPOCH};

use crate::messages::MessageError;
use crate::messages::MsgCodecError;

use crate::messages::action::{ActionRequestMessage, ActionResponseMessage};
use crate::messages::execute::{ExecuteRequestMessage, ExecuteResponseMessage};
use crate::messages::handshake::{HandshakeMessage, HandshakeResponseMessage};
use crate::messages::nack::NackMessage;
use crate::messages::read::{ReadRequestMessage, ReadResponseMessage};
use crate::messages::registry::{RegistryRequestMessage, RegistryResponseMessage};
use crate::messages::write::{WriteRequestMessage, WriteResponseMessage};

use bytes::BufMut;
use chrono::DateTime;
use chrono::NaiveDateTime;

const CURRENT_PROTOCOL_VERSION: u8 = 1;

/// Protocol message structure for IoT device communication.
///
/// Each message follows this format:
/// ```text
/// ┌──────────┬─────────┬──────────┬─────┬───────────┬─────────┬─────────┐
/// │ Version  │ MsgType │ DeviceID │ Seq │ Timestamp │ Payload │ MAC tag │
/// │ (1 byte) │ (1 byte)│(16 bytes)│(4 B)│  (8 B)    │ (var)   │ (16 B)  │
/// └──────────┴─────────┴──────────┴─────┴───────────┴─────────┴─────────┘
/// ```
///
/// Messages are secured with HMAC-SHA256 authentication and include sequence numbers
/// for replay protection.
#[derive(Debug, Clone)]
pub struct Message {
    /// Protocol version (currently 1)
    pub version: u8,
    /// Type of message (handshake, register, read, write, etc.)
    pub msg_type: MsgType,
    /// Unique device identifier (UUID as u128)
    pub device_id: u128,
    /// Sequence number for replay protection (increments per session)
    pub seq: u32,
    /// Unix timestamp when the message was created
    pub timestamp: u64,
    /// Variable-length payload specific to message type
    pub payload: MessagePayload,
    /// HMAC-SHA256 tag (16 bytes) for message authentication
    pub mac: u128,
}

impl Message {
    /// Creates a new device registration request message.
    ///
    /// Used when a device first connects to the HES backdoor to register itself.
    /// The device_id and seq are set to 0 since they haven't been assigned yet.
    pub fn new_register_request_message(imei: String, ipv6: String) -> Result<Self, MessageError> {
        let mut msg = Message {
            version: CURRENT_PROTOCOL_VERSION,
            msg_type: MsgType::RegisterRequest,
            device_id: 0,
            seq: 0,
            timestamp: SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis() as u64,
            payload: MessagePayload::RegistryRequest(RegistryRequestMessage::new(imei, ipv6)),
            mac: 0,
        };

        msg.calculate_mac();
        Ok(msg)
    }

    /// Creates a registration response message to send back to a newly registered device.
    ///
    /// The HES sends this after validating the registration request, assigning a UUID
    /// and scheduling the device in a time bucket.
    pub fn new_register_response_message(device_id: u128, seq: u32, flag: u8, next_wake_time: u64) -> Result<Self, MessageError> {
        let mut msg = Message {
            version: CURRENT_PROTOCOL_VERSION,
            msg_type: MsgType::RegisterResponse,
            device_id,
            seq,
            timestamp: SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis() as u64,
            payload: MessagePayload::RegistryResponse(RegistryResponseMessage::new(flag, next_wake_time)),
            mac: 0,
        };

        msg.calculate_mac();
        Ok(msg)
    }

    /// Creates a HANDSHAKE message to initiate a session with a device.
    ///
    /// The HES sends this when connecting to a device at its scheduled wake time.
    /// The device should respond with a HANDSHAKE_RESPONSE.
    pub fn new_handshake_message(device_id: u128, seq: u32, nonce: Vec<u8>) -> Result<Self, MessageError> {
        let mut msg = Message {
            version: CURRENT_PROTOCOL_VERSION,
            msg_type: MsgType::Handshake,
            device_id,
            seq,
            timestamp: SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis() as u64,
            payload: MessagePayload::Handshake(HandshakeMessage::new(nonce)),
            mac: 0,
        };

        msg.calculate_mac();
        Ok(msg)
    }

    /// Creates a NACK message to signal failure to the device.
    ///
    /// Sent when the HES cannot process a request (e.g., unknown device_id,
    /// database error). The device should retry or fall back to full registration.
    pub fn new_nack_message(device_id: u128, seq: u32, error_code: u8) -> Result<Self, MessageError> {
        let mut msg = Message {
            version: CURRENT_PROTOCOL_VERSION,
            msg_type: MsgType::Nack,
            device_id,
            seq,
            timestamp: SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis() as u64,
            payload: MessagePayload::Nack(NackMessage::new(error_code)),
            mac: 0,
        };
        msg.calculate_mac();
        Ok(msg)
    }

    /// Creates an ACK message to confirm successful message reception.
    ///
    /// Used to close sessions and confirm operations. The device uses this to
    /// acknowledge the HES's messages before going back to sleep.
    pub fn new_ack_message(device_id: u128, seq: u32) -> Result<Self, MessageError> {
        let mut msg = Message {
            version: CURRENT_PROTOCOL_VERSION,
            msg_type: MsgType::Ack,
            device_id,
            seq,
            timestamp: SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis() as u64,
            payload: MessagePayload::Ack,
            mac: 0,
        };

        msg.calculate_mac();
        Ok(msg)
    }

    /// Calculates and sets the HMAC-SHA256 authentication tag for this message.
    ///
    /// TODO: Actually implement HMAC calculation (issue #9)
    /// Currently just sets MAC to 0 as a placeholder.
    fn calculate_mac(&mut self) {
        // TODO: calculate mac #9
        self.mac = 0;
    }

    pub fn get_timestamp(&self) -> Result<NaiveDateTime, MessageError> {
        // Convert milliseconds to seconds and nanoseconds
        let secs = (self.timestamp / 1000) as i64;
        let nanos = ((self.timestamp % 1000) * 1_000_000) as u32;

        Ok(DateTime::from_timestamp(secs, nanos)
            .ok_or(MessageError::InvalidTimeStamp)?
            .naive_utc())
    }
}

/// Protocol message types based on IEC 62056 COSEM subset.
///
/// The protocol supports 13 message types organized in request/response pairs.
/// Each type has a unique hex code for wire encoding.
#[derive(Debug, Clone, Copy)]
pub enum MsgType {
    /// Session initiation from HES (0x00)
    Handshake,
    /// Session acknowledgment from device (0x01)
    HandshakeResponse,
    /// New device registration request (0x02)
    RegisterRequest,
    /// Registration confirmation from HES (0x03)
    RegisterResponse,
    /// Batch OBIS data query - water volume, battery, etc. (0x0A)
    ReadRequest,
    /// Response with requested OBIS data (0x0B)
    ReadResponse,
    /// Configuration update - next wake time, clock sync (0x14)
    WriteRequest,
    /// Write operation confirmation (0x15)
    WriteResponse,
    /// Execute command - e.g., firmware OTA update (0x1E)
    ExecuteRequest,
    /// Execute operation result (0x1F)
    ExecuteResponse,
    /// Action request - e.g., diagnostic mode (0x28)
    ActionRequest,
    /// Action operation result (0x29)
    ActionResponse,
    /// Generic success acknowledgment, session close (0xFF)
    Ack,
    /// Generic failure / rejection (0xFE)
    Nack,
}

impl MsgType {
    /// Returns a short lowercase name suitable for logging and metrics labels.
    pub fn as_str(&self) -> &'static str {
        match self {
            MsgType::Handshake => "handshake",
            MsgType::HandshakeResponse => "handshake_response",
            MsgType::RegisterRequest => "register_request",
            MsgType::RegisterResponse => "register_response",
            MsgType::ReadRequest => "read_request",
            MsgType::ReadResponse => "read_response",
            MsgType::WriteRequest => "write_request",
            MsgType::WriteResponse => "write_response",
            MsgType::ExecuteRequest => "execute_request",
            MsgType::ExecuteResponse => "execute_response",
            MsgType::ActionRequest => "action_request",
            MsgType::ActionResponse => "action_response",
            MsgType::Ack => "ack",
            MsgType::Nack => "nack",
        }
    }

    /// Returns the hex code for this message type.
    pub fn code(&self) -> u8 {
        match self {
            MsgType::Handshake => 0x00,
            MsgType::HandshakeResponse => 0x01,
            MsgType::RegisterRequest => 0x02,
            MsgType::RegisterResponse => 0x03,
            MsgType::ReadRequest => 0x0A,
            MsgType::ReadResponse => 0x0B,
            MsgType::WriteRequest => 0x14,
            MsgType::WriteResponse => 0x15,
            MsgType::ExecuteRequest => 0x1E,
            MsgType::ExecuteResponse => 0x1F,
            MsgType::ActionRequest => 0x28,
            MsgType::ActionResponse => 0x29,
            MsgType::Ack => 0xFF,
            MsgType::Nack => 0xFE,
        }
    }

    /// Converts a hex code to the corresponding message type.
    ///
    /// Returns an error if the code doesn't match any known message type.
    pub fn from_code(code: u8) -> Result<Self, MsgCodecError> {
        match code {
            0x00 => Ok(MsgType::Handshake),
            0x01 => Ok(MsgType::HandshakeResponse),
            0x02 => Ok(MsgType::RegisterRequest),
            0x03 => Ok(MsgType::RegisterResponse),
            0x0A => Ok(MsgType::ReadRequest),
            0x0B => Ok(MsgType::ReadResponse),
            0x14 => Ok(MsgType::WriteRequest),
            0x15 => Ok(MsgType::WriteResponse),
            0x1E => Ok(MsgType::ExecuteRequest),
            0x1F => Ok(MsgType::ExecuteResponse),
            0x28 => Ok(MsgType::ActionRequest),
            0x29 => Ok(MsgType::ActionResponse),
            0xFE => Ok(MsgType::Nack),
            0xFF => Ok(MsgType::Ack),
            _ => Err(MsgCodecError::UnknownMsgType),
        }
    }
}

/// Variable-length payload for different message types.
///
/// Each message type has its own payload structure. Some messages like ACK
/// have no payload data.
#[derive(Debug, Clone)]
pub enum MessagePayload {
    Handshake(HandshakeMessage),
    HandshakeResponse(HandshakeResponseMessage),
    RegistryRequest(RegistryRequestMessage),
    RegistryResponse(RegistryResponseMessage),
    ReadRequest(ReadRequestMessage),
    ReadResponse(ReadResponseMessage),
    WriteRequest(WriteRequestMessage),
    WriteResponse(WriteResponseMessage),
    ExecuteRequest(ExecuteRequestMessage),
    ExecuteResponse(ExecuteResponseMessage),
    ActionRequest(ActionRequestMessage),
    ActionResponse(ActionResponseMessage),
    Ack,
    Nack(NackMessage),
}

impl MessagePayload {
    /// Encodes the payload into the provided buffer.
    ///
    /// Delegates to each message type's encode implementation.
    /// ACK messages have no payload so this is a no-op.
    pub(crate) fn encode(&self, buf: &mut dyn BufMut) -> Result<(), MsgCodecError> {
        match self {
            MessagePayload::Handshake(msg) => msg.encode(buf),
            MessagePayload::HandshakeResponse(msg) => msg.encode(buf),
            MessagePayload::RegistryRequest(msg) => msg.encode(buf),
            MessagePayload::RegistryResponse(msg) => msg.encode(buf),
            MessagePayload::ReadRequest(msg) => msg.encode(buf),
            MessagePayload::ReadResponse(msg) => msg.encode(buf),
            MessagePayload::WriteRequest(msg) => msg.encode(buf),
            MessagePayload::WriteResponse(msg) => msg.encode(buf),
            MessagePayload::ExecuteRequest(msg) => msg.encode(buf),
            MessagePayload::ExecuteResponse(msg) => msg.encode(buf),
            MessagePayload::ActionRequest(msg) => msg.encode(buf),
            MessagePayload::ActionResponse(msg) => msg.encode(buf),
            MessagePayload::Ack => Ok(()),
            MessagePayload::Nack(msg) => msg.encode(buf),
        }
    }

    /// Decodes payload data based on the message type code.
    pub(crate) fn decode(code: u8, msg_data: &[u8]) -> Result<Self, MsgCodecError> {
        match code {
            0x00 => Ok(MessagePayload::Handshake(HandshakeMessage::decode(msg_data)?)),
            0x01 => Ok(MessagePayload::HandshakeResponse(HandshakeResponseMessage::decode(msg_data)?)),
            0x02 => Ok(MessagePayload::RegistryRequest(RegistryRequestMessage::decode(msg_data)?)),
            0x03 => Ok(MessagePayload::RegistryResponse(RegistryResponseMessage::decode(msg_data)?)),
            0x0A => Ok(MessagePayload::ReadRequest(ReadRequestMessage::decode(msg_data)?)),
            0x0B => Ok(MessagePayload::ReadResponse(ReadResponseMessage::decode(msg_data)?)),
            0x14 => Ok(MessagePayload::WriteRequest(WriteRequestMessage::decode(msg_data)?)),
            0x15 => Ok(MessagePayload::WriteResponse(WriteResponseMessage::decode(msg_data)?)),
            0x1E => Ok(MessagePayload::ExecuteRequest(ExecuteRequestMessage::decode(msg_data)?)),
            0x1F => Ok(MessagePayload::ExecuteResponse(ExecuteResponseMessage::decode(msg_data)?)),
            0x28 => Ok(MessagePayload::ActionRequest(ActionRequestMessage::decode(msg_data)?)),
            0x29 => Ok(MessagePayload::ActionResponse(ActionResponseMessage::decode(msg_data)?)),
            0xFE => Ok(MessagePayload::Nack(NackMessage::decode(msg_data)?)),
            0xFF => Ok(MessagePayload::Ack),
            _ => Err(MsgCodecError::UnknownMsgType),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::BytesMut;
    use crate::messages::codec::MessageCodec;
    use tokio_util::codec::{Decoder, Encoder};

    fn roundtrip(msg: Message) -> Message {
        let mut buf = BytesMut::new();
        let mut codec = MessageCodec;
        codec.encode(msg, &mut buf).expect("encode failed");
        codec.decode(&mut buf).expect("decode failed").expect("no message decoded")
    }

    #[test]
    fn registry_request_roundtrip() {
        let msg = Message::new_register_request_message("123456789012345".to_string(), "fe80::1".to_string()).unwrap();
        let decoded = roundtrip(msg);
        assert!(matches!(decoded.payload, MessagePayload::RegistryRequest(_)));
    }

    #[test]
    fn handshake_roundtrip() {
        let msg = Message::new_handshake_message(42, 1, vec![0xDE, 0xAD, 0xBE, 0xEF]).unwrap();
        let decoded = roundtrip(msg);
        assert!(matches!(decoded.payload, MessagePayload::Handshake(_)));
    }

    #[test]
    fn ack_roundtrip() {
        let msg = Message::new_ack_message(99, 1).unwrap();
        let decoded = roundtrip(msg);
        assert!(matches!(decoded.payload, MessagePayload::Ack));
    }
}
