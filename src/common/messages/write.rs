use crate::messages::MsgCodecError;
use bytes::BufMut;
use rlp::RlpStream;

#[derive(Debug, Clone)]
pub struct WriteParameter {
    pub code: String,
    pub value: Vec<u8>,
}

#[derive(Debug, Clone)]
pub struct WriteRequestMessage {
    pub parameters: Vec<WriteParameter>,
}

impl WriteRequestMessage {
    pub fn new(parameters: Vec<WriteParameter>) -> Self {
        Self { parameters }
    }

    pub fn encode(&self, buf: &mut dyn BufMut) -> Result<(), MsgCodecError> {
        let mut stream = RlpStream::new_list(self.parameters.len());
        for param in &self.parameters {
            stream.begin_list(2);
            stream.append(&param.code.as_bytes());
            stream.append(&param.value);
        }
        buf.put_slice(&stream.out());
        Ok(())
    }

    pub fn decode(data: &[u8]) -> Result<Self, MsgCodecError> {
        let rlp = rlp::Rlp::new(data);
        let count = rlp.item_count()
            .map_err(|e| MsgCodecError::PayloadDecodeError(e.to_string()))?;

        let mut parameters = Vec::new();
        for i in 0..count {
            let item = rlp.at(i)
                .map_err(|e| MsgCodecError::PayloadDecodeError(e.to_string()))?;

            let code_bytes: Vec<u8> = item.val_at(0)
                .map_err(|e| MsgCodecError::PayloadDecodeError(e.to_string()))?;
            let code = String::from_utf8(code_bytes)
                .map_err(|e| MsgCodecError::PayloadDecodeError(e.to_string()))?;

            let value: Vec<u8> = item.val_at(1)
                .map_err(|e| MsgCodecError::PayloadDecodeError(e.to_string()))?;

            parameters.push(WriteParameter { code, value });
        }

        Ok(Self { parameters })
    }
}

#[derive(Debug, Clone)]
pub struct WriteResponseMessage {
    pub success: bool,
    pub written_codes: Vec<String>,
}

impl WriteResponseMessage {
    pub fn new(success: bool, written_codes: Vec<String>) -> Self {
        Self {
            success,
            written_codes,
        }
    }

    pub fn encode(&self, buf: &mut dyn BufMut) -> Result<(), MsgCodecError> {
        let mut stream = RlpStream::new_list(2);
        stream.append(&(self.success as u8));
        stream.begin_list(self.written_codes.len());
        for code in &self.written_codes {
            stream.append(&code.as_bytes());
        }
        buf.put_slice(&stream.out());
        Ok(())
    }

    pub fn decode(data: &[u8]) -> Result<Self, MsgCodecError> {
        let rlp = rlp::Rlp::new(data);
        let success_byte: u8 = rlp.val_at(0)
            .map_err(|e| MsgCodecError::PayloadDecodeError(e.to_string()))?;
        let success = success_byte != 0;

        let codes_list = rlp.at(1)
            .map_err(|e| MsgCodecError::PayloadDecodeError(e.to_string()))?;
        let count = codes_list.item_count()
            .map_err(|e| MsgCodecError::PayloadDecodeError(e.to_string()))?;

        let mut written_codes = Vec::new();
        for i in 0..count {
            let code_bytes: Vec<u8> = codes_list.val_at(i)
                .map_err(|e| MsgCodecError::PayloadDecodeError(e.to_string()))?;
            let code = String::from_utf8(code_bytes)
                .map_err(|e| MsgCodecError::PayloadDecodeError(e.to_string()))?;
            written_codes.push(code);
        }

        Ok(Self {
            success,
            written_codes,
        })
    }
}
