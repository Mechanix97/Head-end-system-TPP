use crate::messages::MsgCodecError;
use bytes::BufMut;
use rlp::RlpStream;

/// OBIS code value (e.g., "1.0.1" for water volume, "C.6.1" for battery)
#[derive(Debug, Clone)]
pub struct ObisValue {
    pub code: String,
    pub value: Vec<u8>,
}

#[derive(Debug, Clone)]
pub struct ReadRequestMessage {
    pub obis_codes: Vec<String>,
}

impl ReadRequestMessage {
    pub fn new(obis_codes: Vec<String>) -> Self {
        Self { obis_codes }
    }

    pub fn encode(&self, buf: &mut dyn BufMut) -> Result<(), MsgCodecError> {
        let mut stream = RlpStream::new_list(self.obis_codes.len());
        for code in &self.obis_codes {
            stream.append(&code.as_bytes());
        }
        buf.put_slice(&stream.out());
        Ok(())
    }

    pub fn decode(data: &[u8]) -> Result<Self, MsgCodecError> {
        let rlp = rlp::Rlp::new(data);
        let count = rlp.item_count()
            .map_err(|e| MsgCodecError::PayloadDecodeError(e.to_string()))?;

        let mut obis_codes = Vec::new();
        for i in 0..count {
            let code_bytes: Vec<u8> = rlp.val_at(i)
                .map_err(|e| MsgCodecError::PayloadDecodeError(e.to_string()))?;
            let code = String::from_utf8(code_bytes)
                .map_err(|e| MsgCodecError::PayloadDecodeError(e.to_string()))?;
            obis_codes.push(code);
        }

        Ok(Self { obis_codes })
    }
}

#[derive(Debug, Clone)]
pub struct ReadResponseMessage {
    pub values: Vec<ObisValue>,
}

impl ReadResponseMessage {
    pub fn new(values: Vec<ObisValue>) -> Self {
        Self { values }
    }

    pub fn encode(&self, buf: &mut dyn BufMut) -> Result<(), MsgCodecError> {
        let mut stream = RlpStream::new_list(self.values.len());
        for value in &self.values {
            stream.begin_list(2);
            stream.append(&value.code.as_bytes());
            stream.append(&value.value);
        }
        buf.put_slice(&stream.out());
        Ok(())
    }

    pub fn decode(data: &[u8]) -> Result<Self, MsgCodecError> {
        let rlp = rlp::Rlp::new(data);
        let count = rlp.item_count()
            .map_err(|e| MsgCodecError::PayloadDecodeError(e.to_string()))?;

        let mut values = Vec::new();
        for i in 0..count {
            let item = rlp.at(i)
                .map_err(|e| MsgCodecError::PayloadDecodeError(e.to_string()))?;

            let code_bytes: Vec<u8> = item.val_at(0)
                .map_err(|e| MsgCodecError::PayloadDecodeError(e.to_string()))?;
            let code = String::from_utf8(code_bytes)
                .map_err(|e| MsgCodecError::PayloadDecodeError(e.to_string()))?;

            let value: Vec<u8> = item.val_at(1)
                .map_err(|e| MsgCodecError::PayloadDecodeError(e.to_string()))?;

            values.push(ObisValue { code, value });
        }

        Ok(Self { values })
    }
}
