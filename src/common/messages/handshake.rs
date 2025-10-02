use crate::messages::MsgCodecError;
use bytes::BufMut;

pub struct HandshakeMessage {}

impl HandshakeMessage {
    pub fn encode(&self, _buf: &mut dyn BufMut) -> Result<(), MsgCodecError> {
        Ok(())
    }
}

pub struct HandshakeResponseMessage {}

impl HandshakeResponseMessage {
    pub fn encode(&self, _buf: &mut dyn BufMut) -> Result<(), MsgCodecError> {
        Ok(())
    }
}
