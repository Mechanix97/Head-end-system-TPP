use crate::messages::MsgCodecError;
use bytes::BufMut;

#[derive(Debug)]
pub struct ReadRequestMessage {}

impl ReadRequestMessage {
    pub fn encode(&self, _buf: &mut dyn BufMut) -> Result<(), MsgCodecError> {
        Ok(())
    }
}

#[derive(Debug)]
pub struct ReadResponseMessage {}

impl ReadResponseMessage {
    pub fn encode(&self, _buf: &mut dyn BufMut) -> Result<(), MsgCodecError> {
        Ok(())
    }
}
