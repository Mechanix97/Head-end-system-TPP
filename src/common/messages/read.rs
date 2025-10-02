use crate::messages::MsgCodecError;
use bytes::BufMut;

pub struct ReadRequestMessage {}

impl ReadRequestMessage {
    pub fn encode(&self, _buf: &mut dyn BufMut) -> Result<(), MsgCodecError> {
        Ok(())
    }
}

pub struct ReadResponseMessage {}

impl ReadResponseMessage {
    pub fn encode(&self, _buf: &mut dyn BufMut) -> Result<(), MsgCodecError> {
        Ok(())
    }
}
