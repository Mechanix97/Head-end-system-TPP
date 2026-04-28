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
            // In UDP, each packet is complete. If the message is too short,
            // it's malformed and won't receive more data. Discard it.
            if !buf.is_empty() {
                buf.clear();

                return Err(MsgCodecError::InvalidLength);
            }
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

        // Prevent integer underflow: ensure we have at least MAC_SIZE bytes remaining
        let payload_len = buf.len().checked_sub(MAC_SIZE).ok_or_else(|| {
            buf.clear();
            MsgCodecError::InvalidLength
        })?;

        let payload_data = buf.split_to(payload_len).freeze();
        let mac = buf.get_u128();

        // Decode message type and payload
        let msg_type = MsgType::from_code(msg_type_code).inspect_err(|_| {
            buf.clear();
        })?;
        let payload = MessagePayload::decode(msg_type_code, &payload_data).inspect_err(|_| {
            buf.clear();
        })?;

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

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::BufMut;

    // This test simulates the scenario where encoder skips msg_type byte (line 110)
    // The decoder should discard the message and return an error instead of looping
    #[test]
    fn test_malformed_message_is_discarded() {
        let mut codec = MessageCodec;
        let mut buffer = BytesMut::new();

        // Manually create a malformed message (45 bytes instead of 46)
        // Missing the msg_type byte
        buffer.put_u8(1); // version
        // SKIP msg_type (simulating commented line 110)
        buffer.put_u128(0); // device_id
        buffer.put_u32(0); // seq
        buffer.put_u64(1234567890); // timestamp
        buffer.put_u128(0); // mac

        assert_eq!(buffer.len(), 45); // Should be 45 bytes (missing 1 byte)

        // Try to decode - should return error and clear buffer
        let result = codec.decode(&mut buffer);

        // Should return error
        assert!(result.is_err());

        // Buffer should be cleared to prevent infinite loop
        assert_eq!(
            buffer.len(),
            0,
            "Buffer should be cleared after detecting malformed message"
        );
    }

    // Test that messages causing integer underflow are handled correctly
    #[test]
    fn test_message_with_underflow_is_discarded() {
        let mut codec = MessageCodec;
        let mut buffer = BytesMut::new();

        // Create a message with valid header but not enough bytes for MAC
        buffer.put_u8(1); // version
        buffer.put_u8(0x02); // msg_type (RegisterRequest)
        buffer.put_u128(0); // device_id
        buffer.put_u32(0); // seq
        buffer.put_u64(1234567890); // timestamp
        // SKIP payload
        // SKIP MAC (only put 10 bytes instead of 16)
        buffer.put_u64(0);
        buffer.put_u16(0);

        assert_eq!(buffer.len(), 40); // 30 header + 10 partial MAC

        // Try to decode
        let result = codec.decode(&mut buffer);

        // Should return error due to underflow
        assert!(result.is_err());

        // Buffer should be cleared
        assert_eq!(buffer.len(), 0);
    }

    // Test that valid messages still work correctly
    #[test]
    fn test_valid_message_decodes_successfully() {
        let mut codec = MessageCodec;
        let mut encode_buffer = BytesMut::new();

        // Create a valid message using the encoder
        let msg = crate::messages::message::Message::new_register_request_message(
            "123456789012345".to_string(),
            "fe80::1".to_string(),
        )
        .expect("Failed to create message");

        codec
            .encode(msg, &mut encode_buffer)
            .expect("Failed to encode message");

        let result = codec.decode(&mut encode_buffer);

        assert!(result.is_ok());
        assert!(result.unwrap().is_some());
        assert_eq!(encode_buffer.len(), 0);
    }

    fn roundtrip(msg: Message) -> Message {
        let mut codec = MessageCodec;
        let mut buf = BytesMut::new();
        codec.encode(msg, &mut buf).expect("encode failed");
        codec
            .decode(&mut buf)
            .expect("decode error")
            .expect("decode returned None")
    }

    #[test]
    fn test_roundtrip_register_request() {
        use crate::messages::message::{MessagePayload, MsgType};
        let msg = Message::new_register_request_message("123456789012345".to_string(), "fe80::1".to_string()).unwrap();
        let decoded = roundtrip(msg);
        assert!(matches!(decoded.msg_type, MsgType::RegisterRequest));
        assert!(matches!(decoded.payload, MessagePayload::RegistryRequest(_)));
    }

    #[test]
    fn test_roundtrip_register_response() {
        use crate::messages::message::{MessagePayload, MsgType};
        let msg = Message::new_register_response_message(42, 1, 0, 0).unwrap();
        let decoded = roundtrip(msg);
        assert!(matches!(decoded.msg_type, MsgType::RegisterResponse));
        assert!(matches!(decoded.payload, MessagePayload::RegistryResponse(_)));
    }

    #[test]
    fn test_roundtrip_handshake() {
        use crate::messages::message::{MessagePayload, MsgType};
        let msg = Message::new_handshake_message(1, 0, vec![]).unwrap();
        let decoded = roundtrip(msg);
        assert!(matches!(decoded.msg_type, MsgType::Handshake));
        assert!(matches!(decoded.payload, MessagePayload::Handshake(_)));
    }

    #[test]
    fn test_roundtrip_ack() {
        use crate::messages::message::{MessagePayload, MsgType};
        let msg = Message::new_ack_message(1, 0).unwrap();
        let decoded = roundtrip(msg);
        assert!(matches!(decoded.msg_type, MsgType::Ack));
        assert!(matches!(decoded.payload, MessagePayload::Ack));
    }

    #[test]
    fn test_unknown_msg_type_returns_error() {
        let mut codec = MessageCodec;
        let mut buf = BytesMut::new();

        buf.put_u8(1);    // version
        buf.put_u8(0xAB); // unknown msg_type
        buf.put_u128(0);  // device_id
        buf.put_u32(0);   // seq
        buf.put_u64(0);   // timestamp
        buf.put_u128(0);  // mac

        let result = codec.decode(&mut buf);
        assert!(result.is_err());
    }
}
