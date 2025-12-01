use crate::messages::MsgCodecError;
use bytes::BufMut;
use rlp::RlpStream;

#[derive(Debug, Clone)]
pub struct RegistryRequestMessage {
    pub factory_id: u64,
    pub batch_id: u64,
    pub ipv6: String,
    pub mac: String,
}

impl RegistryRequestMessage {
    pub fn new(factory_id: u64, batch_id: u64, ipv6: String, mac: String) -> Self {
        Self {
            factory_id,
            batch_id,
            ipv6,
            mac,
        }
    }

    pub fn encode(&self, buf: &mut dyn BufMut) -> Result<(), MsgCodecError> {
        let mut stream = RlpStream::new_list(4);
        stream.append(&self.factory_id);
        stream.append(&self.batch_id);
        stream.append(&self.ipv6.as_bytes());
        stream.append(&self.mac.as_bytes());
        buf.put_slice(&stream.out());
        Ok(())
    }

    pub fn decode(data: &[u8]) -> Result<Self, MsgCodecError> {
        let rlp = rlp::Rlp::new(data);
        let factory_id = rlp.val_at(0)
            .map_err(|e| MsgCodecError::PayloadDecodeError(e.to_string()))?;
        let batch_id = rlp.val_at(1)
            .map_err(|e| MsgCodecError::PayloadDecodeError(e.to_string()))?;
        let ipv6_bytes: Vec<u8> = rlp.val_at(2)
            .map_err(|e| MsgCodecError::PayloadDecodeError(e.to_string()))?;
        let mac_bytes: Vec<u8> = rlp.val_at(3)
            .map_err(|e| MsgCodecError::PayloadDecodeError(e.to_string()))?;

        let ipv6 = String::from_utf8(ipv6_bytes)
            .map_err(|e| MsgCodecError::PayloadDecodeError(e.to_string()))?;
        let mac = String::from_utf8(mac_bytes)
            .map_err(|e| MsgCodecError::PayloadDecodeError(e.to_string()))?;

        Ok(Self {
            factory_id,
            batch_id,
            ipv6,
            mac,
        })
    }
}

#[derive(Debug, Clone)]
pub struct RegistryResponseMessage {
    pub device_id: u128,
    pub next_wake_time: u64,
}

impl RegistryResponseMessage {
    pub fn new(device_id: u128, next_wake_time: u64) -> Self {
        Self {
            device_id,
            next_wake_time,
        }
    }

    pub fn encode(&self, buf: &mut dyn BufMut) -> Result<(), MsgCodecError> {
        let mut stream = RlpStream::new_list(2);
        stream.append(&self.device_id);
        stream.append(&self.next_wake_time);
        buf.put_slice(&stream.out());
        Ok(())
    }

    pub fn decode(data: &[u8]) -> Result<Self, MsgCodecError> {
        let rlp = rlp::Rlp::new(data);
        let device_id = rlp.val_at(0)
            .map_err(|e| MsgCodecError::PayloadDecodeError(e.to_string()))?;
        let next_wake_time = rlp.val_at(1)
            .map_err(|e| MsgCodecError::PayloadDecodeError(e.to_string()))?;
        Ok(Self {
            device_id,
            next_wake_time,
        })
    }
}
