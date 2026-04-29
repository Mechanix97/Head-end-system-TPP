use crate::messages::MsgCodecError;
use bytes::BufMut;
use rlp::RlpStream;

pub const NACK_DEVICE_NOT_FOUND: u8 = 0x01;
pub const NACK_INTERNAL_ERROR: u8 = 0x02;

#[derive(Debug, Clone)]
pub struct NackMessage {
    pub error_code: u8,
}

impl NackMessage {
    pub fn new(error_code: u8) -> Self {
        Self { error_code }
    }

    pub fn encode(&self, buf: &mut dyn BufMut) -> Result<(), MsgCodecError> {
        let mut stream = RlpStream::new_list(1);
        stream.append(&self.error_code);
        buf.put_slice(&stream.out());
        Ok(())
    }

    pub fn decode(data: &[u8]) -> Result<Self, MsgCodecError> {
        let rlp = rlp::Rlp::new(data);
        let error_code = rlp
            .val_at(0)
            .map_err(|e| MsgCodecError::PayloadDecodeError(e.to_string()))?;
        Ok(Self { error_code })
    }
}
