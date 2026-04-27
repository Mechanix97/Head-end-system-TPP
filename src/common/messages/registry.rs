use crate::messages::MsgCodecError;
use bytes::BufMut;

#[derive(Debug, Clone, Copy)]
pub struct RegistryRequestMessage {}

impl RegistryRequestMessage {
    pub fn decode(_data: &[u8]) -> Result<Self, MsgCodecError> {
        Ok(Self {})
    }

    pub fn encode(&self, _buf: &mut dyn BufMut) -> Result<(), MsgCodecError> {
        Ok(())
    }
}

#[derive(Debug, Clone, Copy)]
pub struct RegistryResponseMessage {}

impl RegistryResponseMessage {
    pub fn decode(_data: &[u8]) -> Result<Self, MsgCodecError> {
        Ok(Self {})
    }

    pub fn encode(&self, _buf: &mut dyn BufMut) -> Result<(), MsgCodecError> {
        Ok(())
    }
}
