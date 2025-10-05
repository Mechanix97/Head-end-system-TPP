use crate::messages::MsgCodecError;
use bytes::BufMut;

#[derive(Debug, Clone, Copy)]
pub struct ReadRequestMessage {}

impl ReadRequestMessage {
    pub fn encode(&self, _buf: &mut dyn BufMut) -> Result<(), MsgCodecError> {
        Ok(())
    }
}

#[derive(Debug, Clone, Copy)]
pub struct ReadResponseMessage {}

impl ReadResponseMessage {
    pub fn encode(&self, _buf: &mut dyn BufMut) -> Result<(), MsgCodecError> {
        Ok(())
    }
}
