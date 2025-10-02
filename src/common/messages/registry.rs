use crate::messages::MsgCodecError;
use bytes::BufMut;

pub struct RegistryRequestMessage {}

impl RegistryRequestMessage {
    pub fn encode(&self, _buf: &mut dyn BufMut) -> Result<(), MsgCodecError> {
        Ok(())
    }
}
pub struct RegistryResponseMessage {}

impl RegistryResponseMessage {
    pub fn encode(&self, _buf: &mut dyn BufMut) -> Result<(), MsgCodecError> {
        Ok(())
    }
}
