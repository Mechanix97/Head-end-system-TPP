use crate::messages::MsgCodecError;
use bytes::BufMut;

#[derive(Debug)]
pub struct HandshakeMessage {}

impl HandshakeMessage {
    pub fn encode(&self, _buf: &mut dyn BufMut) -> Result<(), MsgCodecError> {
        Ok(())
    }
}

#[derive(Debug)]
pub struct HandshakeResponseMessage {}

impl HandshakeResponseMessage {
    pub fn encode(&self, _buf: &mut dyn BufMut) -> Result<(), MsgCodecError> {
        Ok(())
    }
}
