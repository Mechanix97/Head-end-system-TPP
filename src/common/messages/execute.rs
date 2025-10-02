use crate::messages::MsgCodecError;
use bytes::BufMut;

pub struct ExecuteRequestMessage {}

impl ExecuteRequestMessage {
    pub fn encode(&self, _buf: &mut dyn BufMut) -> Result<(), MsgCodecError> {
        Ok(())
    }
}
pub struct ExecuteResponseMessage {}

impl ExecuteResponseMessage {
    pub fn encode(&self, _buf: &mut dyn BufMut) -> Result<(), MsgCodecError> {
        Ok(())
    }
}
