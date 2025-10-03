use bytes::{Buf, BufMut, BytesMut};
use tokio_util::codec::{Decoder, Encoder};

use crate::messages::MsgCodecError;
// use crate::messages::action::{ActionRequestMessage, ActionResponseMessage};
// use crate::messages::execute::{ExecuteRequestMessage, ExecuteResponseMessage};
// use crate::messages::handshake::{HandshakeMessage, HandshakeResponseMessage};
use crate::messages::message::{Message, MessagePayload, MsgType};
// use crate::messages::read::{ReadRequestMessage, ReadResponseMessage};
// use crate::messages::registry::{RegistryRequestMessage, RegistryResponseMessage};
// use crate::messages::write::{WriteRequestMessage, WriteResponseMessage};

const HEADER_SIZE: usize = 30;
const MAC_SIZE: usize = 16;
const MIN_MSG_LEN: usize = HEADER_SIZE + MAC_SIZE;

#[derive(Debug)]
pub struct MessageCodec;

impl Decoder for MessageCodec {
    type Item = Message;
    type Error = MsgCodecError;

    fn decode(&mut self, buf: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        if buf.len() < MIN_MSG_LEN {
            return Ok(None);
        }

        // Extract header
        let version = buf.get_u8();
        let msg_type_code = buf.get_u8();
        let device_id = buf.get_u128();
        let seq = buf.get_u32();
        let timestamp = buf.get_u64();

        // Extract payload & mac
        let payload_len = buf.len() - MAC_SIZE;
        let payload_data = buf.split_to(payload_len).freeze();
        let mac = buf.get_u128();

        // Determine msg type from code
        let msg_type = MsgType::from_code(msg_type_code)?;
        let payload = MessagePayload::decode(msg_type_code, &payload_data)?;

        Ok(Some(Message {
            version,
            msg_type,
            device_id,
            seq,
            timestamp,
            payload,
            mac,
        }))
    }
}

impl Encoder<Message> for MessageCodec {
    type Error = MsgCodecError;

    fn encode(&mut self, item: Message, buf: &mut BytesMut) -> Result<(), Self::Error> {
        buf.reserve(MIN_MSG_LEN + 100); // Hardcoded, remove later

        // write header
        buf.put_u8(item.version);
        buf.put_u8(item.msg_type.code());
        buf.put_u128(item.device_id);
        buf.put_u32(item.seq);
        buf.put_u64(item.timestamp);

        // write payload
        item.payload.encode(buf)?;

        // write MAC
        buf.put_u128(item.mac);

        Ok(())
    }
}
