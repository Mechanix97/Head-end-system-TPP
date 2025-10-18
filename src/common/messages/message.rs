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

#[derive(Debug, Clone, Copy)]
pub struct Message {
    pub version: u8,
    pub msg_type: MsgType,
    pub device_id: u128,
    pub seq: u32,
    pub timestamp: u64,
    pub payload: MessagePayload,
    pub mac: u128,
}

impl Message {
    pub fn new_register_response(device_id: u128, seq: u32) -> Result<Self, MessageError> {
        let mut msg = Message {
            version: CURRENT_PROTOCOL_VERSION,
            msg_type: MsgType::RegisterResponse,
            device_id,
            seq,
            timestamp: SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs(),
            payload: MessagePayload::RegistryResponse(RegistryResponseMessage {}),
            mac: 0,
        };

        msg.calculate_mac();
        Ok(msg)
    }

    pub fn new_ack_message(device_id: u128, seq: u32) -> Result<Self, MessageError> {
        let mut msg = Message {
            version: CURRENT_PROTOCOL_VERSION,
            msg_type: MsgType::Ack,
            device_id,
            seq,
            timestamp: SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs(),
            payload: MessagePayload::Ack,
            mac: 0,
        };

        msg.calculate_mac();
        Ok(msg)
    }

    fn calculate_mac(&mut self) {
        // TODO: calculate mac #9
        self.mac = 0;
    }
}

#[derive(Debug, Clone, Copy)]
pub enum MsgType {
    Handshake,
    HandshakeResponse,
    RegisterRequest,
    RegisterResponse,
    ReadRequest,
    ReadResponse,
    WriteRequest,
    WriteResponse,
    ExecuteRequest,
    ExecuteResponse,
    ActionRequest,
    ActionResponse,
    Ack,
}

impl MsgType {
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

    pub(crate) fn decode(code: u8, _msg_data: &[u8]) -> Result<Self, MsgCodecError> {
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
