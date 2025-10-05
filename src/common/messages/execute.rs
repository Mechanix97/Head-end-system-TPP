use crate::messages::MsgCodecError;
use bytes::BufMut;

#[derive(Debug, Clone, Copy)]
pub struct ExecuteRequestMessage {}

impl ExecuteRequestMessage {
    pub fn encode(&self, _buf: &mut dyn BufMut) -> Result<(), MsgCodecError> {
        Ok(())
    }
}

#[derive(Debug, Clone, Copy)]
pub struct ExecuteResponseMessage {}

impl ExecuteResponseMessage {
    pub fn encode(&self, _buf: &mut dyn BufMut) -> Result<(), MsgCodecError> {
        Ok(())
    }
}
