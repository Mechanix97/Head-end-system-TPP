use std::time::{SystemTime, UNIX_EPOCH};

use crate::messages::MessageError;
use crate::messages::MsgCodecError;

use crate::messages::action::{ActionRequestMessage, ActionResponseMessage};
use crate::messages::execute::{ExecuteRequestMessage, ExecuteResponseMessage};
use crate::messages::handshake::{HandshakeMessage, HandshakeResponseMessage};
use crate::messages::read::{ReadRequestMessage, ReadResponseMessage};
use crate::messages::registry::{RegistryRequestMessage, RegistryResponseMessage};
use crate::messages::write::{WriteRequestMessage, WriteResponseMessage};

use bytes::BufMut;

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
#[derive(Debug, Clone, Copy)]
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
    pub fn new_register_request_message() -> Result<Self, MessageError> {
        let mut msg = Message {
            version: CURRENT_PROTOCOL_VERSION,
            msg_type: MsgType::RegisterRequest,
            device_id: 0,
            seq: 0,
            timestamp: SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis() as u64,
            payload: MessagePayload::RegistryResponse(RegistryResponseMessage {}),
            mac: 0,
        };

        msg.calculate_mac();
        Ok(msg)
    }

    /// Creates a registration response message to send back to a newly registered device.
    ///
    /// The HES sends this after validating the registration request, assigning a UUID
    /// and scheduling the device in a time bucket.
    pub fn new_register_response_message(device_id: u128, seq: u32) -> Result<Self, MessageError> {
        let mut msg = Message {
            version: CURRENT_PROTOCOL_VERSION,
            msg_type: MsgType::RegisterResponse,
            device_id,
            seq,
            timestamp: SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis() as u64,
            payload: MessagePayload::RegistryResponse(RegistryResponseMessage {}),
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
}

impl MsgType {
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
            0xFF => Ok(MsgType::Ack),
            _ => Err(MsgCodecError::UnknownMsgType),
        }
    }
}

/// Variable-length payload for different message types.
///
/// Each message type has its own payload structure. Some messages like ACK
/// have no payload data.
#[derive(Debug, Clone, Copy)]
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
        }
    }

    /// Decodes payload data based on the message type code.
    ///
    /// CRITICAL BUG: This function is currently broken and always returns Ack regardless
    /// of the message type. All the match arms are empty and msg_data is ignored.
    /// This needs to be fixed to properly decode each message type's payload.
    /// See todo-hes.md issue #1 for details.
    pub(crate) fn decode(code: u8, _msg_data: &[u8]) -> Result<Self, MsgCodecError> {
        // TODO: Fix broken payload decoding - always returns Ack (issue #1)
        match code {
            0x00 => {}
            0x01 => {}
            0x02 => {}
            0x03 => {}
            0x0A => {}
            0x0B => {}
            0x14 => {}
            0x15 => {}
            0x1E => {}
            0x1F => {}
            0x28 => {}
            0x29 => {}
            0xFF => {}
            _ => {}
        }
        Ok(MessagePayload::Ack)
    }
}
