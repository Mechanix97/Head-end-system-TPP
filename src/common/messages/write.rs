use crate::messages::MsgCodecError;
use bytes::BufMut;

pub struct WriteRequestMessage {}

impl WriteRequestMessage {
    pub fn encode(&self, _buf: &mut dyn BufMut) -> Result<(), MsgCodecError> {
        Ok(())
    }
}

pub struct WriteResponseMessage {}

impl WriteResponseMessage {
    pub fn encode(&self, _buf: &mut dyn BufMut) -> Result<(), MsgCodecError> {
        Ok(())
    }
}
