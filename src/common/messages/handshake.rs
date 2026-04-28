use crate::messages::MsgCodecError;
use bytes::BufMut;
use rlp::RlpStream;

#[derive(Debug, Clone)]
pub struct HandshakeMessage {
    pub nonce: Vec<u8>,
}

impl HandshakeMessage {
    pub fn new(nonce: Vec<u8>) -> Self {
        Self { nonce }
    }

    pub fn encode(&self, buf: &mut dyn BufMut) -> Result<(), MsgCodecError> {
        let mut stream = RlpStream::new_list(1);
        stream.append(&self.nonce);
        buf.put_slice(&stream.out());
        Ok(())
    }

    pub fn decode(data: &[u8]) -> Result<Self, MsgCodecError> {
        let rlp = rlp::Rlp::new(data);
        let nonce = rlp.val_at(0)
            .map_err(|e| MsgCodecError::PayloadDecodeError(e.to_string()))?;
        Ok(Self { nonce })
    }
}

#[derive(Debug, Clone)]
pub struct HandshakeResponseMessage {
    pub status: u8,
}

impl HandshakeResponseMessage {
    pub fn new(status: u8) -> Self {
        Self { status }
    }

    pub fn encode(&self, buf: &mut dyn BufMut) -> Result<(), MsgCodecError> {
        let mut stream = RlpStream::new_list(1);
        stream.append(&self.status);
        buf.put_slice(&stream.out());
        Ok(())
    }

    pub fn decode(data: &[u8]) -> Result<Self, MsgCodecError> {
        let rlp = rlp::Rlp::new(data);
        let status = rlp.val_at(0)
            .map_err(|e| MsgCodecError::PayloadDecodeError(e.to_string()))?;
        Ok(Self { status })
    }
}
