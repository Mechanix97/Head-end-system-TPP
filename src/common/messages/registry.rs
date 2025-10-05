use crate::messages::MsgCodecError;
use bytes::BufMut;

#[derive(Debug, Clone, Copy)]
pub struct RegistryRequestMessage {}

impl RegistryRequestMessage {
    pub fn encode(&self, _buf: &mut dyn BufMut) -> Result<(), MsgCodecError> {
        Ok(())
    }
}

#[derive(Debug, Clone, Copy)]
pub struct RegistryResponseMessage {}

impl RegistryResponseMessage {
    pub fn encode(&self, _buf: &mut dyn BufMut) -> Result<(), MsgCodecError> {
        Ok(())
    }
}
