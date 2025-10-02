use crate::messages::MsgCodecError;
use bytes::BufMut;

pub struct ActionRequestMessage {}

impl ActionRequestMessage {
    pub fn encode(&self, _buf: &mut dyn BufMut) -> Result<(), MsgCodecError> {
        Ok(())
    }
}

pub struct ActionResponseMessage {}

impl ActionResponseMessage {
    pub fn encode(&self, _buf: &mut dyn BufMut) -> Result<(), MsgCodecError> {
        Ok(())
    }
}
