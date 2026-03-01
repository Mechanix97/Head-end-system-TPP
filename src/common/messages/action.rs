use crate::messages::MsgCodecError;
use bytes::BufMut;

#[derive(Debug, Clone, Copy)]
pub struct ActionRequestMessage {}

impl ActionRequestMessage {
    pub fn decode(_data: &[u8]) -> Result<Self, MsgCodecError> {
        Ok(Self {})
    }

    pub fn encode(&self, _buf: &mut dyn BufMut) -> Result<(), MsgCodecError> {
        Ok(())
    }
}

#[derive(Debug, Clone, Copy)]
pub struct ActionResponseMessage {}

impl ActionResponseMessage {
    pub fn decode(_data: &[u8]) -> Result<Self, MsgCodecError> {
        Ok(Self {})
    }

    pub fn encode(&self, _buf: &mut dyn BufMut) -> Result<(), MsgCodecError> {
        Ok(())
    }
}
