use crate::messages::action::{ActionRequestMessage, ActionResponseMessage};
use crate::messages::execute::{ExecuteRequestMessage, ExecuteResponseMessage};
use crate::messages::handshake::{HandshakeMessage, HandshakeResponseMessage};
use crate::messages::read::{ReadRequestMessage, ReadResponseMessage};
use crate::messages::registry::{RegistryRequestMessage, RegistryResponseMessage};
use crate::messages::write::{WriteRequestMessage, WriteResponseMessage};

pub struct Message {
    pub version: u8,
    pub msg_type: MsgType,
    pub device_id: u128,
    pub seq: u32,
    pub timestamp: u64,
    pub payload: MessagePayload,
    pub mac: u128,
}

pub enum MsgType {
    Handshake,
    HandshakeRespoonse,
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
            MsgType::HandshakeRespoonse => 0x01,
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
}

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
