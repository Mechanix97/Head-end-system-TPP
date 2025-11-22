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

/// Message header size in bytes: version(1) + msg_type(1) + device_id(16) + seq(4) + timestamp(8)
const HEADER_SIZE: usize = 30;
/// HMAC-SHA256 MAC tag size in bytes
const MAC_SIZE: usize = 16;
/// Minimum valid message length: header + MAC (no payload like ACK messages)
const MIN_MSG_LEN: usize = HEADER_SIZE + MAC_SIZE;

/// Codec for encoding and decoding protocol messages over UDP.
///
/// Implements tokio's Decoder and Encoder traits to integrate with `UdpFramed`.
/// Handles the binary format of messages: header, variable payload, and MAC tag.
#[derive(Debug)]
pub struct MessageCodec;

impl Decoder for MessageCodec {
    type Item = Message;
    type Error = MsgCodecError;

    /// Decodes a message from the provided buffer.
    ///
    /// Returns `Ok(None)` if the buffer doesn't contain enough data yet.
    /// Returns `Ok(Some(Message))` when a complete message is decoded.
    ///
    /// Message format: [header(30) | payload(variable) | mac(16)]
    fn decode(&mut self, buf: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        // Wait for at least minimum message size
        if buf.len() < MIN_MSG_LEN {
            return Ok(None);
        }

        // Extract header fields (30 bytes total)
        let version = buf.get_u8();
        let msg_type_code = buf.get_u8();
        let device_id = buf.get_u128();
        let seq = buf.get_u32();
        let timestamp = buf.get_u64();

        // Extract payload and MAC
        // BUG: This has an off-by-one error when calculating payload length.
        // After extracting the header (30 bytes), buf.len() now represents only
        // the remaining data (payload + MAC), so we should calculate differently.
        // See todo-hes.md issue #2 for details.
        let payload_len = buf.len() - MAC_SIZE;
        let payload_data = buf.split_to(payload_len).freeze();
        let mac = buf.get_u128();

        // Decode message type and payload
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

    /// Encodes a message into the provided buffer.
    ///
    /// Writes the message in this order:
    /// 1. Header (30 bytes): version, msg_type, device_id, seq, timestamp
    /// 2. Payload (variable length, depends on message type)
    /// 3. MAC tag (16 bytes)
    fn encode(&mut self, item: Message, buf: &mut BytesMut) -> Result<(), Self::Error> {
        // Reserve space for message. The +100 is a rough estimate for payload size.
        // TODO: Calculate exact payload size instead of hardcoding +100
        buf.reserve(MIN_MSG_LEN + 100);

        // Write header (30 bytes total)
        buf.put_u8(item.version);
        buf.put_u8(item.msg_type.code());
        buf.put_u128(item.device_id);
        buf.put_u32(item.seq);
        buf.put_u64(item.timestamp);

        // Write variable-length payload
        item.payload.encode(buf)?;

        // Write HMAC-SHA256 MAC tag (16 bytes)
        buf.put_u128(item.mac);

        Ok(())
    }
}
