use crate::messages::MsgCodecError;
use bytes::BufMut;
use rlp::RlpStream;

#[derive(Debug, Clone)]
pub struct RegistryRequestMessage {
    pub imei: String,
    pub ipv6: String,
}

impl RegistryRequestMessage {
    pub fn new(imei: String, ipv6: String) -> Self {
        Self { imei, ipv6 }
    }

    pub fn encode(&self, buf: &mut dyn BufMut) -> Result<(), MsgCodecError> {
        let mut stream = RlpStream::new_list(2);
        stream.append(&self.imei.as_bytes());
        stream.append(&self.ipv6.as_bytes());
        buf.put_slice(&stream.out());
        Ok(())
    }

    pub fn decode(data: &[u8]) -> Result<Self, MsgCodecError> {
        let rlp = rlp::Rlp::new(data);
        let imei_bytes: Vec<u8> = rlp
            .val_at(0)
            .map_err(|e| MsgCodecError::PayloadDecodeError(e.to_string()))?;
        let ipv6_bytes: Vec<u8> = rlp
            .val_at(1)
            .map_err(|e| MsgCodecError::PayloadDecodeError(e.to_string()))?;

        let imei = String::from_utf8(imei_bytes)
            .map_err(|e| MsgCodecError::PayloadDecodeError(e.to_string()))?;
        let ipv6 = String::from_utf8(ipv6_bytes)
            .map_err(|e| MsgCodecError::PayloadDecodeError(e.to_string()))?;

        Ok(Self { imei, ipv6 })
    }
}

#[derive(Debug, Clone)]
pub struct RegistryResponseMessage {
    pub flag: u8,
    pub next_wake_time: u64,
}

impl RegistryResponseMessage {
    pub fn new(flag: u8, next_wake_time: u64) -> Self {
        Self { flag, next_wake_time }
    }

    pub fn encode(&self, buf: &mut dyn BufMut) -> Result<(), MsgCodecError> {
        let mut stream = RlpStream::new_list(2);
        stream.append(&self.flag);
        stream.append(&self.next_wake_time);
        buf.put_slice(&stream.out());
        Ok(())
    }

    pub fn decode(data: &[u8]) -> Result<Self, MsgCodecError> {
        let rlp = rlp::Rlp::new(data);
        let flag = rlp
            .val_at(0)
            .map_err(|e| MsgCodecError::PayloadDecodeError(e.to_string()))?;
        let next_wake_time = rlp
            .val_at(1)
            .map_err(|e| MsgCodecError::PayloadDecodeError(e.to_string()))?;
        Ok(Self { flag, next_wake_time })
    }
}
