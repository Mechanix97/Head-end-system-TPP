use crate::messages::MsgCodecError;
use bytes::BufMut;

#[derive(Debug)]
pub struct ActionRequestMessage {}

impl ActionRequestMessage {
    pub fn encode(&self, _buf: &mut dyn BufMut) -> Result<(), MsgCodecError> {
        Ok(())
    }
}

#[derive(Debug)]
pub struct ActionResponseMessage {}

impl ActionResponseMessage {
    pub fn encode(&self, _buf: &mut dyn BufMut) -> Result<(), MsgCodecError> {
        Ok(())
    }
}
