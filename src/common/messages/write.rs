use crate::messages::MsgCodecError;
use bytes::BufMut;

#[derive(Debug)]
pub struct WriteRequestMessage {}

impl WriteRequestMessage {
    pub fn encode(&self, _buf: &mut dyn BufMut) -> Result<(), MsgCodecError> {
        Ok(())
    }
}

#[derive(Debug)]
pub struct WriteResponseMessage {}

impl WriteResponseMessage {
    pub fn encode(&self, _buf: &mut dyn BufMut) -> Result<(), MsgCodecError> {
        Ok(())
    }
}
